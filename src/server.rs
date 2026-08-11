use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::cache::CacheStore;
use crate::snapshot::{Snapshot, unix_now};

const HTTP_READ_LIMIT: usize = 16 * 1024;
const SSE_KEEPALIVE_SECS: u64 = 15;

#[derive(Clone, Debug)]
pub struct ProjectedResponse {
    pub content_type: String,
    pub body: Vec<u8>,
}

impl ProjectedResponse {
    pub fn json<T: serde::Serialize>(value: &T) -> Result<Self> {
        Ok(Self {
            content_type: "application/json".into(),
            body: serde_json::to_vec(value).context("serialize projected snapshot")?,
        })
    }
}

pub type SnapshotProjector<S> =
    Arc<dyn Fn(&S, &str, &str) -> Option<ProjectedResponse> + Send + Sync + 'static>;
pub type RefreshValidator<S> = Arc<dyn Fn(&S, &str) -> bool + Send + Sync + 'static>;
pub type HealthProjector<S> = Arc<dyn Fn(&S) -> ProjectedResponse + Send + Sync + 'static>;
pub type CommandHandler = Arc<
    dyn Fn(&str, serde_json::Value) -> std::result::Result<serde_json::Value, String>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CommandRequest {
    pub operation: String,
    pub input: serde_json::Value,
}

/// Single-writer state shared by a host collector and transport threads.
pub struct SharedSnapshot<S: Snapshot> {
    pub(crate) state: Mutex<S>,
    pub(crate) changed: Condvar,
    store: CacheStore<S>,
    refresh_requests: Mutex<VecDeque<String>>,
}

impl<S: Snapshot> SharedSnapshot<S> {
    #[must_use]
    pub fn new(state: S, store: CacheStore<S>) -> Self {
        Self {
            state: Mutex::new(state),
            changed: Condvar::new(),
            store,
            refresh_requests: Mutex::new(VecDeque::new()),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> S {
        self.state.lock().map_or_else(
            |poisoned| poisoned.into_inner().clone(),
            |state| state.clone(),
        )
    }

    pub fn request_refresh(&self, domain: String) {
        let mut requests = self
            .refresh_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !requests.contains(&domain) {
            requests.push_back(domain);
        }
    }

    pub fn take_refresh_request(&self) -> Option<String> {
        self.refresh_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
    }

    pub fn update(&self, mutate: impl FnOnce(&mut S)) {
        let snapshot = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            mutate(&mut state);
            let revision = state.revision().saturating_add(1);
            state.set_revision(revision);
            state.set_saved_at(Some(unix_now()));
            state.clone()
        };
        if let Err(error) = self.store.save(&snapshot) {
            eprintln!("{} daemon: cache save failed: {error:#}", S::APP_NAME);
        }
        self.changed.notify_all();
    }

    /// Replace a collector-owned payload while allowing a host to preserve
    /// health/notices that changed during an unlocked API request.
    pub fn replace_payload(&self, mut state: S, reconcile: impl FnOnce(&mut S, &S)) {
        let snapshot = {
            let mut current = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reconcile(&mut state, &current);
            state.set_revision(current.revision().saturating_add(1));
            state.set_saved_at(Some(unix_now()));
            *current = state.clone();
            state
        };
        if let Err(error) = self.store.save(&snapshot) {
            eprintln!("{} daemon: cache save failed: {error:#}", S::APP_NAME);
        }
        self.changed.notify_all();
    }
}

pub struct ServerOptions<S: Snapshot> {
    pub shared: Arc<SharedSnapshot<S>>,
    /// Empty/None disables TCP. Use `127.0.0.1:0` in tests.
    pub bind: Option<String>,
    /// Optional owner-local Unix socket served with the same HTTP/SSE contract.
    pub unix_socket: Option<PathBuf>,
    pub token: String,
    pub projector: Option<SnapshotProjector<S>>,
    pub refresh_validator: RefreshValidator<S>,
    pub health_projector: Option<HealthProjector<S>>,
    /// Optional authenticated application-command conduit. Hosts use this to
    /// keep credentials and source mutations inside the single daemon process.
    pub command_handler: Option<CommandHandler>,
    /// When the explicit `bind` is a specific, non-loopback, non-wildcard
    /// address (e.g. a Tailscale IP), the server ALSO binds the matching
    /// loopback alias (`127.0.0.1`/`::1`) on the same port by default, so
    /// local `127.0.0.1` clients keep working without exposing the wildcard
    /// LAN. Set this to opt out of the automatic loopback alias.
    pub disable_default_loopback: bool,
}

impl<S: Snapshot> ServerOptions<S> {
    #[must_use]
    pub fn new(shared: Arc<SharedSnapshot<S>>, token: String) -> Self {
        Self {
            shared,
            bind: Some("127.0.0.1:0".into()),
            unix_socket: None,
            token,
            projector: None,
            refresh_validator: Arc::new(|_, domain| !domain.is_empty()),
            health_projector: None,
            command_handler: None,
            disable_default_loopback: false,
        }
    }
}

pub struct ServerHandle {
    pub http_address: Option<SocketAddr>,
    /// Address of the automatic loopback alias, when one was bound alongside a
    /// non-loopback `http_address` (see `ServerOptions::disable_default_loopback`).
    pub loopback_address: Option<SocketAddr>,
    pub unix_socket: Option<PathBuf>,
    stop: Arc<AtomicBool>,
}

impl ServerHandle {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop();
        if let Some(path) = &self.unix_socket {
            let _ = fs::remove_file(path);
        }
    }
}

pub fn start_server<S: Snapshot>(options: ServerOptions<S>) -> Result<ServerHandle> {
    if options.token.len() < 32 {
        bail!("daemon bearer token is empty or too short");
    }
    let stop = Arc::new(AtomicBool::new(false));
    let shared_options = Arc::new(options);
    let mut http_address = None;
    let mut loopback_address = None;

    if let Some(bind) = shared_options
        .bind
        .as_deref()
        .filter(|bind| !bind.is_empty())
    {
        let listener = TcpListener::bind(bind)
            .with_context(|| format!("bind {} daemon HTTP server at {bind}", S::DISPLAY_NAME))?;
        let address = spawn_tcp_listener(listener, &shared_options, &stop)?;
        http_address = Some(address);

        // Default loopback alias: a specific non-loopback bind (e.g. a Tailscale
        // IP) ALSO serves the matching loopback address on the same port, so
        // `127.0.0.1` local access keeps working without binding the wildcard
        // `0.0.0.0` (which would expose the LAN). Opt out via
        // `disable_default_loopback`.
        if !shared_options.disable_default_loopback {
            if let Some(loop_addr) = default_loopback_alias(address) {
                let listener = TcpListener::bind(loop_addr).with_context(|| {
                    format!(
                        "bind {} daemon loopback alias at {loop_addr}",
                        S::DISPLAY_NAME
                    )
                })?;
                let address = spawn_tcp_listener(listener, &shared_options, &stop)?;
                loopback_address = Some(address);
            }
        }
    }

    #[cfg(unix)]
    let unix_socket = if let Some(path) = &shared_options.unix_socket {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixListener;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create daemon socket directory {}", parent.display()))?;
        }
        if path.exists() {
            // A successful connection proves a live owner; otherwise the path is
            // a stale filesystem entry left by an unclean stop.
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                bail!("daemon Unix socket is already active: {}", path.display());
            }
            fs::remove_file(path)
                .with_context(|| format!("remove stale daemon socket {}", path.display()))?;
        }
        let listener = UnixListener::bind(path)
            .with_context(|| format!("bind daemon Unix socket {}", path.display()))?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let options = Arc::clone(&shared_options);
        let listener_stop = Arc::clone(&stop);
        thread::spawn(move || accept_unix(listener, options, listener_stop));
        Some(path.clone())
    } else {
        None
    };
    #[cfg(not(unix))]
    let unix_socket = None;

    if http_address.is_none() && unix_socket.is_none() {
        bail!("daemon requires at least one HTTP or Unix-socket bind");
    }
    Ok(ServerHandle {
        http_address,
        loopback_address,
        unix_socket,
        stop,
    })
}

/// Bind bookkeeping shared by the explicit HTTP listener and its optional
/// loopback alias: set non-blocking, read the resolved address, and spawn the
/// accept loop.
fn spawn_tcp_listener<S: Snapshot>(
    listener: TcpListener,
    shared_options: &Arc<ServerOptions<S>>,
    stop: &Arc<AtomicBool>,
) -> Result<SocketAddr> {
    listener.set_nonblocking(true)?;
    let address = listener.local_addr().context("read daemon HTTP address")?;
    let options = Arc::clone(shared_options);
    let listener_stop = Arc::clone(stop);
    thread::spawn(move || accept_tcp(listener, options, listener_stop));
    Ok(address)
}

/// Compute the loopback alias to bind alongside an explicit, resolved HTTP
/// address. Returns `None` when the explicit address already covers loopback
/// (loopback or wildcard/unspecified) or uses an ephemeral (`0`) port, so the
/// alias never double-binds a port the primary listener already owns.
fn default_loopback_alias(address: SocketAddr) -> Option<SocketAddr> {
    if address.port() == 0 {
        return None;
    }
    match address.ip() {
        IpAddr::V4(v4) => {
            if v4.is_loopback() || v4.is_unspecified() {
                None
            } else {
                Some(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    address.port(),
                ))
            }
        }
        IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() {
                None
            } else {
                Some(SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::LOCALHOST),
                    address.port(),
                ))
            }
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn accept_tcp<S: Snapshot>(
    listener: TcpListener,
    options: Arc<ServerOptions<S>>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                // macOS inherits O_NONBLOCK from the listener on accepted
                // sockets. `write_all` then returns EAGAIN once a response is
                // larger than the kernel send buffer, truncating snapshots.
                // Connection threads are intentionally blocking and bounded by
                // read/write timeouts, so clear inherited nonblocking mode.
                if let Err(error) = stream.set_nonblocking(false) {
                    eprintln!(
                        "{} daemon: configure client stream failed: {error}",
                        S::APP_NAME
                    );
                    continue;
                }
                let options = Arc::clone(&options);
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
                    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
                    if let Err(error) = serve_connection(stream, &options, &stop) {
                        eprintln!(
                            "{} daemon: client connection failed: {error:#}",
                            S::APP_NAME
                        );
                    }
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                eprintln!("{} daemon: accept failed: {error}", S::APP_NAME);
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

#[cfg(unix)]
#[allow(clippy::needless_pass_by_value)]
fn accept_unix<S: Snapshot>(
    listener: std::os::unix::net::UnixListener,
    options: Arc<ServerOptions<S>>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(error) = stream.set_nonblocking(false) {
                    eprintln!(
                        "{} daemon: configure Unix client stream failed: {error}",
                        S::APP_NAME
                    );
                    continue;
                }
                let options = Arc::clone(&options);
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
                    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
                    if let Err(error) = serve_connection(stream, &options, &stop) {
                        eprintln!("{} daemon: Unix client failed: {error:#}", S::APP_NAME);
                    }
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                eprintln!("{} daemon: Unix accept failed: {error}", S::APP_NAME);
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn serve_connection<S, T>(
    mut stream: T,
    options: &ServerOptions<S>,
    stop: &AtomicBool,
) -> Result<()>
where
    S: Snapshot,
    T: Read + Write,
{
    let request = read_http_request(&mut stream)?;
    if !authorized(&request.headers, &options.token) {
        return write_http_response(
            &mut stream,
            "401 Unauthorized",
            "text/plain",
            b"unauthorized\n",
        );
    }
    let route = request.path.split('?').next().unwrap_or_default();
    match (request.method.as_str(), route) {
        ("GET", "/snapshot") => {
            let body = serde_json::to_vec(&options.shared.snapshot())
                .context("serialize daemon snapshot")?;
            write_http_response(&mut stream, "200 OK", "application/json", &body)
        }
        ("GET", route) if route.starts_with("/snapshot/") => {
            let Some(projector) = &options.projector else {
                return write_http_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain",
                    b"unknown snapshot surface\n",
                );
            };
            let Some(response) = projector(&options.shared.snapshot(), route, &request.path) else {
                return write_http_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain",
                    b"unknown snapshot surface\n",
                );
            };
            write_http_response(
                &mut stream,
                "200 OK",
                &response.content_type,
                &response.body,
            )
        }
        ("GET", "/health") => {
            let response = options.health_projector.as_ref().map_or_else(
                || {
                    ProjectedResponse::json(&serde_json::json!({
                        "revision": options.shared.snapshot().revision(),
                        "saved_at": options.shared.snapshot().saved_at(),
                    }))
                },
                |projector| Ok(projector(&options.shared.snapshot())),
            )?;
            write_http_response(
                &mut stream,
                "200 OK",
                &response.content_type,
                &response.body,
            )
        }
        ("GET", "/events") => serve_sse(stream, &options.shared, stop),
        ("POST", "/command") => {
            let Some(handler) = &options.command_handler else {
                return write_http_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain",
                    b"command conduit disabled\n",
                );
            };
            let command: CommandRequest = match serde_json::from_slice(&request.body) {
                Ok(command) => command,
                Err(error) => {
                    return write_http_response(
                        &mut stream,
                        "400 Bad Request",
                        "application/json",
                        &serde_json::to_vec(&serde_json::json!({"error": error.to_string()}))?,
                    );
                }
            };
            match handler(&command.operation, command.input) {
                Ok(output) => write_http_response(
                    &mut stream,
                    "200 OK",
                    "application/json",
                    &serde_json::to_vec(&output)?,
                ),
                Err(error) => write_http_response(
                    &mut stream,
                    "500 Internal Server Error",
                    "application/json",
                    &serde_json::to_vec(&serde_json::json!({"error": error}))?,
                ),
            }
        }
        ("POST", "/refresh") => {
            let domain = query_parameter(&request.path, "domain").unwrap_or_default();
            let snapshot = options.shared.snapshot();
            if domain.is_empty() || !(options.refresh_validator)(&snapshot, &domain) {
                return write_http_response(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain",
                    b"unknown refresh domain\n",
                );
            }
            options.shared.request_refresh(domain);
            write_http_response(
                &mut stream,
                "202 Accepted",
                "application/json",
                b"{\"queued\":true}\n",
            )
        }
        _ => write_http_response(&mut stream, "404 Not Found", "text/plain", b"not found\n"),
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn read_http_request(stream: &mut impl Read) -> Result<HttpRequest> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        if bytes.len() >= HTTP_READ_LIMIT {
            bail!("HTTP request exceeds {HTTP_READ_LIMIT} bytes");
        }
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            bail!("HTTP request ended before headers");
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
    };
    let header_bytes = &bytes[..header_end];
    let text = std::str::from_utf8(header_bytes).context("HTTP request is not UTF-8")?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().context("missing HTTP request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().context("missing HTTP method")?.to_string();
    if !matches!(method.as_str(), "GET" | "POST") {
        bail!("only GET and POST are supported");
    }
    let path = parts.next().context("missing HTTP path")?.to_string();
    let headers: BTreeMap<String, String> = lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let content_length = headers
        .get("content-length")
        .map_or(Ok(0_usize), |value| value.parse::<usize>())
        .context("invalid Content-Length")?;
    let body_start = header_end + 4;
    let request_length = body_start.saturating_add(content_length);
    if request_length > HTTP_READ_LIMIT {
        bail!("HTTP request exceeds {HTTP_READ_LIMIT} bytes");
    }
    while bytes.len() < request_length {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            bail!("HTTP request body ended early");
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    let body = bytes[body_start..request_length].to_vec();
    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn authorized(headers: &BTreeMap<String, String>, expected: &str) -> bool {
    let Some(candidate) = headers
        .get("authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    constant_time_eq(candidate.as_bytes(), expected.as_bytes())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn query_parameter(path: &str, key: &str) -> Option<String> {
    path.split_once('?').and_then(|(_, query)| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.into_owned())
    })
}

fn write_http_response(
    stream: &mut impl Write,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

fn serve_sse<S: Snapshot, T: Write>(
    mut stream: T,
    shared: &SharedSnapshot<S>,
    stop: &AtomicBool,
) -> Result<()> {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nConnection: keep-alive\r\nX-Accel-Buffering: no\r\n\r\n"
    )?;
    let mut revision = u64::MAX;
    while !stop.load(Ordering::Acquire) {
        let mut guard = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.revision() == revision {
            let waited = shared
                .changed
                .wait_timeout(guard, Duration::from_secs(SSE_KEEPALIVE_SECS))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = waited.0;
            if waited.1.timed_out() {
                drop(guard);
                stream.write_all(b": keepalive\n\n")?;
                stream.flush()?;
                continue;
            }
        }
        let snapshot = guard.clone();
        revision = snapshot.revision();
        drop(guard);
        let data = serde_json::to_string(&snapshot).context("serialize SSE snapshot")?;
        write!(stream, "id: {revision}\nevent: snapshot\ndata: {data}\n\n")?;
        stream.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::net::TcpStream;

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct State {
        revision: u64,
        saved_at: Option<i64>,
        value: String,
    }

    impl Snapshot for State {
        const APP_NAME: &'static str = "remote-cli-test";
        const DISPLAY_NAME: &'static str = "Remote test";
        const CACHE_DIR_ENV: &'static str = "REMOTE_CLI_TEST_CACHE_DIR";
        fn revision(&self) -> u64 {
            self.revision
        }
        fn set_revision(&mut self, value: u64) {
            self.revision = value;
        }
        fn saved_at(&self) -> Option<i64> {
            self.saved_at
        }
        fn set_saved_at(&mut self, value: Option<i64>) {
            self.saved_at = value;
        }
    }

    fn try_raw_http(
        address: SocketAddr,
        token: Option<&str>,
        method: &str,
        path: &str,
    ) -> std::io::Result<String> {
        let mut stream = TcpStream::connect(address)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n{}\r\n",
            token.map_or_else(String::new, |value| format!(
                "Authorization: Bearer {value}\r\n"
            ))
        )?;
        stream.flush()?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    fn raw_http(address: SocketAddr, token: Option<&str>, method: &str, path: &str) -> String {
        try_raw_http(address, token, method, path).unwrap()
    }

    #[test]
    fn authenticated_snapshot_and_refresh_are_generic() {
        let dir = tempfile::tempdir().unwrap();
        let store = CacheStore::new(dir.path().join("state.json"));
        let shared = Arc::new(SharedSnapshot::new(
            State {
                value: "hello".into(),
                ..State::default()
            },
            store,
        ));
        let token = "x".repeat(64);
        let mut options = ServerOptions::new(Arc::clone(&shared), token.clone());
        options.refresh_validator = Arc::new(|_, domain| domain == "mail");
        let handle = start_server(options).unwrap();
        let address = handle.http_address.unwrap();
        assert!(raw_http(address, None, "GET", "/snapshot").starts_with("HTTP/1.1 401"));
        let response = raw_http(address, Some(&token), "GET", "/snapshot");
        assert!(response.contains("hello"));
        assert!(
            raw_http(address, Some(&token), "POST", "/refresh?domain=nope")
                .starts_with("HTTP/1.1 400")
        );
        assert!(
            raw_http(address, Some(&token), "POST", "/refresh?domain=mail")
                .starts_with("HTTP/1.1 202")
        );
        assert_eq!(shared.take_refresh_request().as_deref(), Some("mail"));
    }

    #[test]
    fn large_snapshot_survives_slow_reader_backpressure() {
        const VALUE_BYTES: usize = 4 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let store = CacheStore::new(dir.path().join("state.json"));
        let shared = Arc::new(SharedSnapshot::new(
            State {
                value: "x".repeat(VALUE_BYTES),
                ..State::default()
            },
            store,
        ));
        let token = "l".repeat(64);
        let handle = start_server(ServerOptions::new(shared, token.clone())).unwrap();
        let mut stream = TcpStream::connect(handle.http_address.unwrap()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        write!(
            stream,
            "GET /snapshot HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n\r\n"
        )
        .unwrap();
        stream.flush().unwrap();

        // Let the server fill its kernel send buffer before consuming bytes.
        // On macOS an accepted socket inherited O_NONBLOCK from the listener;
        // before the fix this forced write_all to return EAGAIN and truncated
        // the body at a few hundred KiB.
        thread::sleep(Duration::from_millis(250));
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap();
        let headers = std::str::from_utf8(&response[..header_end]).unwrap();
        assert!(headers.starts_with("HTTP/1.1 200 OK"));
        let expected_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("Content-Length: ")
                    .and_then(|value| value.parse::<usize>().ok())
            })
            .unwrap();
        let body = &response[header_end + 4..];
        assert_eq!(body.len(), expected_length);
        let decoded: State = serde_json::from_slice(body).unwrap();
        assert_eq!(decoded.value.len(), VALUE_BYTES);
    }

    #[test]
    fn shared_updates_revision_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let store = CacheStore::new(dir.path().join("state.json"));
        let shared = SharedSnapshot::new(State::default(), store.clone());
        shared.update(|state| state.value = "new".into());
        assert_eq!(shared.snapshot().revision, 1);
        assert_eq!(store.load().unwrap().value, "new");
    }

    #[cfg(unix)]
    #[test]
    fn unix_socket_is_owner_only_and_serves_same_protocol() {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixStream;
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("daemon.sock");
        let store = CacheStore::new(dir.path().join("state.json"));
        let shared = Arc::new(SharedSnapshot::new(State::default(), store));
        let token = "u".repeat(64);
        let mut options = ServerOptions::new(shared, token.clone());
        options.bind = None;
        options.unix_socket = Some(socket.clone());
        let _handle = start_server(options).unwrap();
        assert_eq!(
            fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let mut stream = UnixStream::connect(socket).unwrap();
        write!(
            stream,
            "GET /snapshot HTTP/1.1\r\nAuthorization: Bearer {token}\r\n\r\n"
        )
        .unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
    }

    /// Discover a routable non-loopback IPv4 for this host without sending any
    /// packets (a connected UDP socket only consults the routing table).
    /// Returns `None` on hosts with no such address so the positive-path tests
    /// skip cleanly instead of flaking.
    fn discover_nonloopback_ipv4() -> Option<IpAddr> {
        let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
        sock.connect("8.8.8.8:80").ok()?;
        let ip = sock.local_addr().ok()?.ip();
        if ip.is_loopback() || ip.is_unspecified() {
            None
        } else {
            Some(ip)
        }
    }

    /// Claim then release an ephemeral port so a concrete (non-zero) port is
    /// available for the dual-bind positive tests (the loopback alias is only
    /// added for a concrete port).
    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[test]
    fn default_loopback_alias_only_for_specific_nonloopback() {
        // Loopback / wildcard already cover 127.0.0.1, so no extra alias.
        assert_eq!(
            default_loopback_alias("127.0.0.1:7634".parse().unwrap()),
            None
        );
        assert_eq!(
            default_loopback_alias("0.0.0.0:7634".parse().unwrap()),
            None
        );
        assert_eq!(default_loopback_alias("[::1]:7634".parse().unwrap()), None);
        assert_eq!(default_loopback_alias("[::]:7634".parse().unwrap()), None);
        // An ephemeral (0) port cannot be matched by a fixed alias.
        assert_eq!(
            default_loopback_alias("100.103.121.27:0".parse().unwrap()),
            None
        );
        // A specific non-loopback address gets a same-port loopback alias.
        assert_eq!(
            default_loopback_alias("100.103.121.27:7634".parse().unwrap()),
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7634))
        );
        assert_eq!(
            default_loopback_alias("[fd7a:1::2]:7634".parse().unwrap()),
            Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 7634))
        );
    }

    #[test]
    fn loopback_primary_bind_has_no_extra_alias() {
        let dir = tempfile::tempdir().unwrap();
        let store = CacheStore::new(dir.path().join("state.json"));
        let shared = Arc::new(SharedSnapshot::new(State::default(), store));
        let token = "x".repeat(64);
        let mut options = ServerOptions::new(Arc::clone(&shared), token);
        options.bind = Some("127.0.0.1:0".into());
        let handle = start_server(options).unwrap();
        assert!(handle.http_address.is_some());
        assert!(handle.loopback_address.is_none());
    }

    #[test]
    fn nonloopback_bind_also_serves_loopback_by_default() {
        let Some(ip) = discover_nonloopback_ipv4() else {
            return;
        };
        let port = free_port();
        let dir = tempfile::tempdir().unwrap();
        let store = CacheStore::new(dir.path().join("state.json"));
        let shared = Arc::new(SharedSnapshot::new(
            State {
                value: "hi".into(),
                ..State::default()
            },
            store,
        ));
        let token = "x".repeat(64);
        let mut options = ServerOptions::new(Arc::clone(&shared), token.clone());
        options.bind = Some(format!("{ip}:{port}"));
        let Ok(handle) = start_server(options) else {
            return; // Host advertised an IP it will not let us bind; skip.
        };
        // Primary bound the explicit non-loopback address ...
        assert_eq!(handle.http_address.map(|a| a.port()), Some(port));
        assert!(!handle.http_address.unwrap().ip().is_loopback());
        // ... and the automatic loopback alias serves the SAME port.
        let loop_addr = handle.loopback_address.expect("loopback alias bound");
        assert!(loop_addr.ip().is_loopback());
        assert_eq!(loop_addr.port(), port);
        // Both endpoints actually serve the authenticated snapshot.
        // Some hosts advertise a non-loopback source address that their local
        // firewall will not hairpin back into. Skip only that primary-endpoint
        // assertion when the host rejects its own address; the loopback alias
        // remains deterministic and must always serve.
        if let Ok(response) = try_raw_http(
            handle.http_address.unwrap(),
            Some(&token),
            "GET",
            "/snapshot",
        ) {
            assert!(response.contains("hi"));
        }
        assert!(raw_http(loop_addr, Some(&token), "GET", "/snapshot").contains("hi"));
    }

    #[test]
    fn disable_default_loopback_skips_the_alias() {
        let Some(ip) = discover_nonloopback_ipv4() else {
            return;
        };
        let port = free_port();
        let dir = tempfile::tempdir().unwrap();
        let store = CacheStore::new(dir.path().join("state.json"));
        let shared = Arc::new(SharedSnapshot::new(State::default(), store));
        let token = "x".repeat(64);
        let mut options = ServerOptions::new(Arc::clone(&shared), token);
        options.bind = Some(format!("{ip}:{port}"));
        options.disable_default_loopback = true;
        let Ok(handle) = start_server(options) else {
            return;
        };
        assert!(handle.http_address.is_some());
        assert!(handle.loopback_address.is_none());
    }
}
