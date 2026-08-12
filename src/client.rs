//! Smart client source orchestration shared by graphical and terse CLI clients.

use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::de::DeserializeOwned;

use crate::auth::read_token;
use crate::cache::CacheStore;
use crate::endpoint::Endpoint;
use crate::snapshot::{Snapshot, unix_now};

const MAX_SSE_LINE_BYTES: usize = 32 * 1024 * 1024;
const FALLBACK_LEASE_TTL: Duration = Duration::from_secs(120);
const FALLBACK_LEASE_RENEW: Duration = Duration::from_secs(30);
const FALLBACK_LEASE_RETRY: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct ClientOptions<S: Snapshot> {
    pub cache_store: CacheStore<S>,
    pub use_cache: bool,
    pub use_daemon: bool,
    pub endpoint: String,
    pub token_path: PathBuf,
    pub fallback: bool,
    pub fallback_timeout: Duration,
    pub fallback_lease_path: PathBuf,
}

impl<S: Snapshot> Clone for ClientOptions<S> {
    fn clone(&self) -> Self {
        Self {
            cache_store: self.cache_store.clone(),
            use_cache: self.use_cache,
            use_daemon: self.use_daemon,
            endpoint: self.endpoint.clone(),
            token_path: self.token_path.clone(),
            fallback: self.fallback,
            fallback_timeout: self.fallback_timeout,
            fallback_lease_path: self.fallback_lease_path.clone(),
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientHealth {
    pub daemon_enabled: bool,
    pub daemon_connected: bool,
    pub daemon_age_secs: Option<u64>,
    pub cache_enabled: bool,
    pub cache_live: bool,
    pub cache_age_secs: Option<u64>,
    pub fallback_active: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub enum ClientUpdate<S> {
    State(Box<S>, String),
    Health(ClientHealth),
    Status(String),
    Error(String),
    FallbackRequired(String),
    DaemonRecovered,
}

pub struct ClientSubscription<S: Snapshot> {
    pub rx: Receiver<ClientUpdate<S>>,
    refresh_tx: Option<Sender<String>>,
    stop: Arc<AtomicBool>,
    marker: PhantomData<fn() -> S>,
}

impl<S: Snapshot> ClientSubscription<S> {
    #[must_use]
    pub fn spawn(options: ClientOptions<S>) -> Self {
        let (updates_tx, updates_rx) = mpsc::channel();
        let (source_tx, source_rx) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));

        if options.use_cache {
            let store = options.cache_store.clone();
            let tx = source_tx.clone();
            let source_stop = Arc::clone(&stop);
            thread::spawn(move || follow_local(&store, &tx, &source_stop));
        }
        let (refresh_tx, refresh_rx) = mpsc::channel();
        let refresh_tx = if options.use_daemon {
            let endpoint = options.endpoint.clone();
            let token_path = options.token_path.clone();
            let tx = source_tx.clone();
            let source_stop = Arc::clone(&stop);
            thread::spawn(move || {
                follow_refresh_commands(&endpoint, &token_path, &refresh_rx, &tx, &source_stop);
            });
            Some(refresh_tx)
        } else {
            drop(refresh_rx);
            None
        };
        if options.use_daemon {
            let endpoint = options.endpoint.clone();
            let token_path = options.token_path.clone();
            let tx = source_tx.clone();
            let source_stop = Arc::clone(&stop);
            thread::spawn(move || follow_remote(&endpoint, &token_path, &tx, &source_stop));
        }
        drop(source_tx);

        let coordinator_stop = Arc::clone(&stop);
        thread::spawn(move || {
            coordinate_sources(&options, &source_rx, &updates_tx, &coordinator_stop);
        });
        Self {
            rx: updates_rx,
            refresh_tx,
            stop,
            marker: PhantomData,
        }
    }

    #[must_use]
    pub fn request_refresh(&self, domain: String) -> bool {
        self.refresh_tx
            .as_ref()
            .is_some_and(|tx| tx.send(domain).is_ok())
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

impl<S: Snapshot> Drop for ClientSubscription<S> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    Cache,
    Daemon,
}

#[derive(Debug)]
enum SourceEvent<S> {
    State(Box<S>, Source),
    DaemonUp,
    DaemonHeartbeat,
    DaemonDown(String),
    CacheError(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SnapshotVersion {
    revision: u64,
    saved_at: Option<i64>,
    latest_refresh: Option<i64>,
}

impl SnapshotVersion {
    fn of(state: &impl Snapshot) -> Self {
        Self {
            revision: state.revision(),
            saved_at: state.saved_at(),
            latest_refresh: state.latest_refresh(),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn coordinate_sources<S: Snapshot>(
    options: &ClientOptions<S>,
    source_rx: &Receiver<SourceEvent<S>>,
    updates: &Sender<ClientUpdate<S>>,
    stop: &AtomicBool,
) {
    let started = Instant::now();
    let timeout = options.fallback_timeout.max(Duration::from_secs(1));
    let mut daemon_alive = false;
    let mut last_daemon_signal = None;
    let mut daemon_outage_since = options.use_daemon.then_some(started);
    let mut last_cache_progress = None;
    let mut last_cache_version = None;
    let mut delivered_version = None;
    let mut fallback_revision = 0_u64;
    let mut fallback_active = false;
    let mut last_error = None;
    let mut last_health = None;
    let mut fallback_disabled_reported = false;
    let mut fallback_lease: Option<FallbackLease> = None;
    let mut last_lease_attempt = None;
    let mut last_lease_renewal = Instant::now();

    while !stop.load(Ordering::Acquire) {
        match source_rx.recv_timeout(Duration::from_millis(250)) {
            Ok(SourceEvent::State(state, source)) => {
                let version = SnapshotVersion::of(&*state);
                match source {
                    Source::Cache => {
                        last_error = None;
                        if last_cache_version != Some(version) {
                            last_cache_version = Some(version);
                            last_cache_progress = Some(progress_instant(&*state, timeout));
                        }
                        if fallback_active && state.revision() > fallback_revision {
                            recover_from_fallback(
                                updates,
                                &mut fallback_active,
                                &mut fallback_lease,
                            );
                            daemon_alive = true;
                            last_daemon_signal = Some(Instant::now());
                            daemon_outage_since = None;
                        }
                    }
                    Source::Daemon => {
                        daemon_alive = true;
                        last_daemon_signal = Some(Instant::now());
                        daemon_outage_since = None;
                        last_error = None;
                        if options.use_cache {
                            if let Err(error) = options.cache_store.save_exact(&state) {
                                let _ = updates.send(ClientUpdate::Error(format!(
                                    "cache write-through failed: {error:#}"
                                )));
                            }
                        }
                        if fallback_active {
                            recover_from_fallback(
                                updates,
                                &mut fallback_active,
                                &mut fallback_lease,
                            );
                        }
                    }
                }
                if delivered_version != Some(version) {
                    delivered_version = Some(version);
                    let status = match source {
                        Source::Cache => "client · cache",
                        Source::Daemon => "client · daemon live",
                    };
                    let _ = updates.send(ClientUpdate::State(state, status.into()));
                }
            }
            Ok(SourceEvent::DaemonUp) => {
                daemon_alive = true;
                last_daemon_signal = Some(Instant::now());
                daemon_outage_since = None;
                last_error = None;
                if fallback_active {
                    recover_from_fallback(updates, &mut fallback_active, &mut fallback_lease);
                }
                let _ = updates.send(ClientUpdate::Status("client · daemon connected".into()));
            }
            Ok(SourceEvent::DaemonHeartbeat) => {
                daemon_alive = true;
                last_daemon_signal = Some(Instant::now());
                daemon_outage_since = None;
                last_error = None;
            }
            Ok(SourceEvent::DaemonDown(error)) => {
                daemon_alive = false;
                daemon_outage_since.get_or_insert_with(Instant::now);
                last_error = Some(error.clone());
                let _ = updates.send(ClientUpdate::Error(format!(
                    "daemon unavailable; monitoring fallback timeout: {error}"
                )));
            }
            Ok(SourceEvent::CacheError(error)) => {
                last_error = Some(error.clone());
                let _ = updates.send(ClientUpdate::Error(error));
            }
            Err(RecvTimeoutError::Disconnected) => thread::sleep(Duration::from_millis(250)),
            Err(RecvTimeoutError::Timeout) => {}
        }

        let now = Instant::now();
        if fallback_active && now.duration_since(last_lease_renewal) >= FALLBACK_LEASE_RENEW {
            if let Some(lease) = &fallback_lease {
                if let Err(error) = lease.renew() {
                    let _ = updates.send(ClientUpdate::Error(format!(
                        "fallback lease renewal failed: {error:#}"
                    )));
                }
            }
            last_lease_renewal = now;
        }
        if daemon_alive
            && last_daemon_signal.is_some_and(|signal| now.duration_since(signal) >= timeout)
        {
            daemon_alive = false;
            daemon_outage_since = last_daemon_signal;
            last_error = Some("daemon heartbeat timed out".into());
            let _ = updates.send(ClientUpdate::Error(
                "daemon heartbeat timed out; evaluating fallback".into(),
            ));
        }
        let both_disabled = !options.use_cache && !options.use_daemon;
        let daemon_timed_out = options.use_daemon
            && !daemon_alive
            && daemon_outage_since.is_some_and(|since| now.duration_since(since) >= timeout);
        let cache_stale = !options.use_cache
            || last_cache_progress.is_none_or(|progress| now.duration_since(progress) >= timeout);
        let fallback_due = both_disabled || (daemon_timed_out && cache_stale);
        let can_retry_lease = last_lease_attempt
            .is_none_or(|attempt| now.duration_since(attempt) >= FALLBACK_LEASE_RETRY);
        if !fallback_due {
            fallback_disabled_reported = false;
        }
        if options.fallback && fallback_due && !fallback_active && can_retry_lease {
            last_lease_attempt = Some(now);
            match FallbackLease::try_acquire(&options.fallback_lease_path) {
                Ok(Some(lease)) => {
                    fallback_revision = delivered_version.map_or(0, |version| version.revision);
                    fallback_active = true;
                    last_lease_renewal = now;
                    fallback_lease = Some(lease);
                    last_error = None;
                    let reason = if both_disabled {
                        "cache and daemon disabled; embedded read-only collector active"
                    } else {
                        "daemon/cache sources stale beyond timeout; embedded read-only collector active"
                    };
                    let _ = updates.send(ClientUpdate::FallbackRequired(reason.into()));
                }
                Ok(None) => {
                    let _ = updates.send(ClientUpdate::Status(format!(
                        "fallback collector already leased by another local {} client",
                        S::DISPLAY_NAME
                    )));
                }
                Err(error) => {
                    let _ = updates.send(ClientUpdate::Error(format!(
                        "cannot acquire fallback lease: {error:#}"
                    )));
                }
            }
        } else if fallback_due && !options.fallback && !fallback_disabled_reported {
            fallback_disabled_reported = true;
            let _ = updates.send(ClientUpdate::Status(
                "all enabled sources unavailable; fallback disabled".into(),
            ));
        }

        let health = ClientHealth {
            daemon_enabled: options.use_daemon,
            daemon_connected: daemon_alive,
            daemon_age_secs: last_daemon_signal
                .map(|signal| age_bucket(now.duration_since(signal))),
            cache_enabled: options.use_cache,
            cache_live: options.use_cache
                && last_cache_progress
                    .is_some_and(|progress| now.duration_since(progress) < timeout),
            cache_age_secs: last_cache_progress
                .map(|progress| age_bucket(now.duration_since(progress))),
            fallback_active,
            error: last_error.clone(),
        };
        if last_health.as_ref() != Some(&health) {
            last_health = Some(health.clone());
            let _ = updates.send(ClientUpdate::Health(health));
        }
    }
}

fn age_bucket(age: Duration) -> u64 {
    let seconds = age.as_secs();
    if seconds < 60 {
        (seconds / 5) * 5
    } else {
        (seconds / 60) * 60
    }
}

fn recover_from_fallback<S>(
    updates: &Sender<ClientUpdate<S>>,
    active: &mut bool,
    lease: &mut Option<FallbackLease>,
) {
    *active = false;
    *lease = None;
    let _ = updates.send(ClientUpdate::DaemonRecovered);
    let _ = updates.send(ClientUpdate::Status(
        "client · daemon recovered; embedded collector stopped".into(),
    ));
}

fn progress_instant(state: &impl Snapshot, timeout: Duration) -> Instant {
    let now = Instant::now();
    let age = state
        .saved_at()
        .map_or(timeout.saturating_add(Duration::from_secs(1)), |saved| {
            Duration::from_secs(
                u64::try_from(unix_now().saturating_sub(saved).max(0)).unwrap_or(u64::MAX),
            )
        });
    now.checked_sub(age.min(timeout.saturating_add(Duration::from_secs(1))))
        .unwrap_or(now)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Fingerprint {
    modified: Option<SystemTime>,
    len: u64,
    file_identity: u64,
}

fn fingerprint<S: Snapshot>(store: &CacheStore<S>) -> Option<Fingerprint> {
    let metadata = fs::metadata(store.path()).ok()?;
    #[cfg(unix)]
    let file_identity = {
        use std::os::unix::fs::MetadataExt;
        metadata.ino()
    };
    #[cfg(not(unix))]
    let file_identity = 0;
    Some(Fingerprint {
        modified: metadata.modified().ok(),
        len: metadata.len(),
        file_identity,
    })
}

fn follow_local<S: Snapshot>(
    store: &CacheStore<S>,
    tx: &Sender<SourceEvent<S>>,
    stop: &AtomicBool,
) {
    if store.path().exists() {
        if let Ok(state) = store.load() {
            let _ = tx.send(SourceEvent::State(Box::new(state), Source::Cache));
        }
    }
    let mut seen = fingerprint(store);
    while !stop.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(350));
        let current = fingerprint(store);
        if current == seen {
            continue;
        }
        seen = current;
        match store.load() {
            Ok(state) => {
                let _ = tx.send(SourceEvent::State(Box::new(state), Source::Cache));
            }
            Err(error) => {
                let _ = tx.send(SourceEvent::CacheError(format!(
                    "local cache update failed: {error:#}"
                )));
            }
        }
    }
}

fn follow_refresh_commands<S: Snapshot>(
    endpoint: &str,
    token_path: &Path,
    commands: &Receiver<String>,
    tx: &Sender<SourceEvent<S>>,
    stop: &AtomicBool,
) {
    while !stop.load(Ordering::Acquire) {
        let domain = match commands.recv_timeout(Duration::from_millis(250)) {
            Ok(domain) => domain,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        match request_refresh(endpoint, token_path, &domain) {
            Ok(()) => {
                let _ = tx.send(SourceEvent::DaemonHeartbeat);
            }
            Err(error) => {
                let _ = tx.send(SourceEvent::DaemonDown(format!(
                    "refresh request failed: {error:#}"
                )));
            }
        }
    }
}

pub fn request_refresh(endpoint: &str, token_path: &Path, domain: &str) -> Result<()> {
    let endpoint = Endpoint::parse(endpoint)?;
    let token = read_token(token_path)?;
    let path = format!("/refresh?domain={}", encode(domain));
    match endpoint {
        Endpoint::Http(base) => {
            Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()?
                .post(format!("{base}{path}"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .send()
                .context("request daemon refresh")?
                .error_for_status()
                .context("daemon refresh rejected")?;
            Ok(())
        }
        #[cfg(unix)]
        Endpoint::Unix(socket) => {
            let response = unix_request(&socket, "POST", &path, &token, &[])?;
            ensure_http_success(&response, "daemon refresh rejected")?;
            Ok(())
        }
    }
}

pub fn fetch_json<T: DeserializeOwned>(endpoint: &str, token_path: &Path, path: &str) -> Result<T> {
    let endpoint = Endpoint::parse(endpoint)?;
    let token = read_token(token_path)?;
    match endpoint {
        Endpoint::Http(base) => Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(45))
            .build()?
            .get(format!("{base}{path}"))
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(ACCEPT, "application/json")
            .send()
            .context("fetch daemon snapshot")?
            .error_for_status()
            .context("daemon snapshot rejected")?
            .json()
            .context("decode daemon snapshot"),
        #[cfg(unix)]
        Endpoint::Unix(socket) => {
            let response = unix_request(&socket, "GET", path, &token, &[])?;
            let body = ensure_http_success(&response, "daemon snapshot rejected")?;
            serde_json::from_slice(body).context("decode daemon snapshot")
        }
    }
}

pub fn post_json<I: serde::Serialize, O: DeserializeOwned>(
    endpoint: &str,
    token_path: &Path,
    path: &str,
    input: &I,
) -> Result<O> {
    let endpoint = Endpoint::parse(endpoint)?;
    let token = read_token(token_path)?;
    match endpoint {
        Endpoint::Http(base) => Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()?
            .post(format!("{base}{path}"))
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(ACCEPT, "application/json")
            .json(input)
            .send()
            .context("request daemon command")?
            .error_for_status()
            .context("daemon command rejected")?
            .json()
            .context("decode daemon command response"),
        #[cfg(unix)]
        Endpoint::Unix(socket) => {
            let input = serde_json::to_vec(input).context("encode daemon command")?;
            let response = unix_request(&socket, "POST", path, &token, &input)?;
            let body = ensure_http_success(&response, "daemon command rejected")?;
            serde_json::from_slice(body).context("decode daemon command response")
        }
    }
}

fn follow_remote<S: Snapshot>(
    endpoint: &str,
    token_path: &Path,
    tx: &Sender<SourceEvent<S>>,
    stop: &AtomicBool,
) {
    let endpoint = match Endpoint::parse(endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            let _ = tx.send(SourceEvent::DaemonDown(error.to_string()));
            return;
        }
    };
    let mut failures = 0_u32;
    while !stop.load(Ordering::Acquire) {
        let token = match read_token(token_path) {
            Ok(token) => token,
            Err(error) => {
                failures = failures.saturating_add(1);
                let _ = tx.send(SourceEvent::DaemonDown(format!(
                    "daemon auth unavailable: {error:#}"
                )));
                interruptible_sleep(stop, reconnect_delay(failures));
                continue;
            }
        };
        match remote_session(&endpoint, &token, tx, stop) {
            Ok(()) => failures = 0,
            Err(error) if !stop.load(Ordering::Acquire) => {
                failures = failures.saturating_add(1);
                let _ = tx.send(SourceEvent::DaemonDown(format!("{error:#}")));
                interruptible_sleep(stop, reconnect_delay(failures));
            }
            Err(_) => break,
        }
    }
}

fn remote_session<S: Snapshot>(
    endpoint: &Endpoint,
    token: &str,
    tx: &Sender<SourceEvent<S>>,
    stop: &AtomicBool,
) -> Result<()> {
    match endpoint {
        Endpoint::Http(base) => remote_http_session(base, token, tx, stop),
        #[cfg(unix)]
        Endpoint::Unix(socket) => remote_unix_session(socket, token, tx, stop),
    }
}

fn remote_http_session<S: Snapshot>(
    base: &str,
    token: &str,
    tx: &Sender<SourceEvent<S>>,
    stop: &AtomicBool,
) -> Result<()> {
    let snapshot_client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(45))
        .build()?;
    let event_client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(None)
        .build()?;
    let authorization = format!("Bearer {token}");
    let initial: S = snapshot_client
        .get(format!("{base}/snapshot"))
        .header(AUTHORIZATION, &authorization)
        .header(ACCEPT, "application/json")
        .send()
        .context("fetch daemon snapshot")?
        .error_for_status()
        .context("daemon snapshot rejected")?
        .json()
        .context("decode daemon snapshot")?;
    tx.send(SourceEvent::DaemonUp)?;
    tx.send(SourceEvent::State(Box::new(initial), Source::Daemon))?;
    let response = event_client
        .get(format!("{base}/events"))
        .header(AUTHORIZATION, authorization)
        .header(ACCEPT, "text/event-stream")
        .send()
        .context("connect daemon events")?
        .error_for_status()
        .context("daemon event stream rejected")?;
    follow_sse(BufReader::new(response), false, tx, stop)
}

#[cfg(unix)]
fn remote_unix_session<S: Snapshot>(
    socket: &Path,
    token: &str,
    tx: &Sender<SourceEvent<S>>,
    stop: &AtomicBool,
) -> Result<()> {
    use std::os::unix::net::UnixStream;
    let response = unix_request(socket, "GET", "/snapshot", token, &[])?;
    let body = ensure_http_success(&response, "daemon snapshot rejected")?;
    let initial: S = serde_json::from_slice(body).context("decode daemon snapshot")?;
    tx.send(SourceEvent::DaemonUp)?;
    tx.send(SourceEvent::State(Box::new(initial), Source::Daemon))?;

    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("connect daemon Unix socket {}", socket.display()))?;
    write!(
        stream,
        "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\nAuthorization: Bearer {token}\r\n\r\n"
    )?;
    stream.flush()?;
    follow_sse(BufReader::new(stream), true, tx, stop)
}

fn follow_sse<S: Snapshot, R: BufRead>(
    mut reader: R,
    skip_http_headers: bool,
    tx: &Sender<SourceEvent<S>>,
    stop: &AtomicBool,
) -> Result<()> {
    let mut line = String::new();
    if skip_http_headers {
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                bail!("daemon event stream ended before headers");
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            if line.starts_with("HTTP/") && !line.contains(" 200 ") {
                bail!("daemon event stream rejected: {}", line.trim());
            }
        }
    }
    let mut data = String::new();
    while !stop.load(Ordering::Acquire) {
        line.clear();
        let read = reader.read_line(&mut line).context("read daemon event")?;
        if read == 0 {
            bail!("daemon event stream ended");
        }
        if line.len() > MAX_SSE_LINE_BYTES {
            bail!("daemon event exceeds {MAX_SSE_LINE_BYTES} byte safety limit");
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.starts_with(':') {
            let _ = tx.send(SourceEvent::DaemonHeartbeat);
        } else if let Some(fragment) = trimmed.strip_prefix("data:") {
            // This private protocol concatenates bounded fragments exactly.
            // The server emits no separator bytes, so JSON strings may cross
            // fragment boundaries without introducing invalid newlines.
            data.push_str(fragment);
        } else if trimmed.is_empty() && !data.is_empty() {
            let state: S = serde_json::from_str(&data).context("decode daemon event")?;
            tx.send(SourceEvent::State(Box::new(state), Source::Daemon))?;
            data.clear();
        }
    }
    Ok(())
}

#[cfg(unix)]
fn unix_request(
    socket: &Path,
    method: &str,
    path: &str,
    token: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    use std::os::unix::net::UnixStream;
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("connect daemon Unix socket {}", socket.display()))?;
    stream.set_read_timeout(Some(Duration::from_secs(45)))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    Ok(response)
}

fn ensure_http_success<'a>(response: &'a [u8], context: &str) -> Result<&'a [u8]> {
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("daemon response has no HTTP header terminator")?;
    let headers =
        std::str::from_utf8(&response[..split]).context("daemon headers are not UTF-8")?;
    let status = headers.lines().next().unwrap_or_default();
    if !status.contains(" 2") {
        bail!("{context}: {status}");
    }
    Ok(&response[split + 4..])
}

fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// Owner-only lease ensuring at most one local client activates embedded
/// fallback collection while the central daemon/cache are stale.
pub struct FallbackLease {
    path: PathBuf,
    owner: String,
}

impl FallbackLease {
    pub fn try_acquire(path: &Path) -> Result<Option<Self>> {
        if let Ok(metadata) = fs::metadata(path) {
            let stale = metadata
                .modified()
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age >= FALLBACK_LEASE_TTL);
            if stale {
                let _ = fs::remove_file(path);
            }
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create fallback lease dir {}", parent.display()))?;
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = match options.open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create fallback lease {}", path.display()));
            }
        };
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let owner = format!("{}:{nonce}", std::process::id());
        writeln!(file, "{owner}")?;
        file.sync_all()?;
        Ok(Some(Self {
            path: path.to_path_buf(),
            owner,
        }))
    }

    pub fn renew(&self) -> Result<()> {
        let current = fs::read_to_string(&self.path).unwrap_or_default();
        if current.trim() != self.owner {
            bail!("fallback lease ownership changed");
        }
        fs::write(&self.path, format!("{}\n", self.owner))
            .with_context(|| format!("renew fallback lease {}", self.path.display()))
    }
}

impl Drop for FallbackLease {
    fn drop(&mut self) {
        let current = fs::read_to_string(&self.path).unwrap_or_default();
        if current.trim() == self.owner {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn reconnect_delay(failures: u32) -> Duration {
    Duration::from_secs((1_u64 << failures.min(5)).min(30))
}

fn interruptible_sleep(stop: &AtomicBool, duration: Duration) {
    let ticks = duration.as_millis().div_ceil(100);
    for _ in 0..ticks {
        if stop.load(Ordering::Acquire) {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{ServerOptions, SharedSnapshot, start_server};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct State {
        revision: u64,
        saved_at: Option<i64>,
        value: String,
    }
    impl Snapshot for State {
        const APP_NAME: &'static str = "remote-cli-client-test";
        const DISPLAY_NAME: &'static str = "Remote client test";
        const CACHE_DIR_ENV: &'static str = "REMOTE_CLI_CLIENT_TEST_CACHE_DIR";
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

    #[test]
    fn reconnect_backoff_is_bounded() {
        assert_eq!(reconnect_delay(0), Duration::from_secs(1));
        assert_eq!(reconnect_delay(1), Duration::from_secs(2));
        assert_eq!(reconnect_delay(100), Duration::from_secs(30));
    }

    #[test]
    fn disabling_both_sources_requests_immediate_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let subscription = ClientSubscription::<State>::spawn(ClientOptions {
            cache_store: CacheStore::<State>::new(dir.path().join("state.json")),
            use_cache: false,
            use_daemon: false,
            endpoint: "http://127.0.0.1:1".into(),
            token_path: dir.path().join("token"),
            fallback: true,
            fallback_timeout: Duration::from_millis(10),
            fallback_lease_path: dir.path().join("fallback.lock"),
        });
        assert!(matches!(
            subscription
                .rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            ClientUpdate::FallbackRequired(_)
        ));
    }

    #[test]
    fn remote_sse_updates_and_writes_through_cache() {
        let dir = tempfile::tempdir().unwrap();
        let server_store = CacheStore::new(dir.path().join("server.json"));
        let shared = Arc::new(SharedSnapshot::new(
            State {
                value: "initial".into(),
                ..State::default()
            },
            server_store,
        ));
        let token_path = dir.path().join("token");
        let token = crate::load_or_create_token(&token_path).unwrap();
        let handle = start_server(ServerOptions::new(Arc::clone(&shared), token)).unwrap();
        let client_store = CacheStore::<State>::new(dir.path().join("client.json"));
        let subscription = ClientSubscription::spawn(ClientOptions {
            cache_store: client_store.clone(),
            use_cache: true,
            use_daemon: true,
            endpoint: format!("http://{}", handle.http_address.unwrap()),
            token_path,
            fallback: false,
            fallback_timeout: Duration::from_secs(30),
            fallback_lease_path: dir.path().join("fallback.lock"),
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut initial = false;
        while Instant::now() < deadline {
            if matches!(subscription.rx.recv_timeout(Duration::from_millis(250)), Ok(ClientUpdate::State(state, _)) if state.value == "initial")
            {
                initial = true;
                break;
            }
        }
        assert!(initial);
        shared.update(|state| state.value = "live".into());
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut live = false;
        while Instant::now() < deadline {
            if matches!(subscription.rx.recv_timeout(Duration::from_millis(250)), Ok(ClientUpdate::State(state, _)) if state.value == "live")
            {
                live = true;
                break;
            }
        }
        assert!(live);
        assert_eq!(client_store.load().unwrap().value, "live");
    }

    #[test]
    fn remote_sse_reassembles_snapshot_larger_than_line_guard() {
        let dir = tempfile::tempdir().unwrap();
        let value = "é".repeat(MAX_SSE_LINE_BYTES / 2 + 1024);
        let shared = Arc::new(SharedSnapshot::new(
            State {
                revision: 1,
                value: value.clone(),
                ..State::default()
            },
            CacheStore::new(dir.path().join("server.json")),
        ));
        let token_path = dir.path().join("token");
        let token = crate::load_or_create_token(&token_path).unwrap();
        let handle = start_server(ServerOptions::new(Arc::clone(&shared), token)).unwrap();
        let subscription = ClientSubscription::spawn(ClientOptions {
            cache_store: CacheStore::<State>::new(dir.path().join("client.json")),
            use_cache: false,
            use_daemon: true,
            endpoint: format!("http://{}", handle.http_address.unwrap()),
            token_path,
            fallback: false,
            fallback_timeout: Duration::from_secs(30),
            fallback_lease_path: dir.path().join("fallback.lock"),
        });
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if matches!(
                subscription.rx.recv_timeout(Duration::from_millis(500)),
                Ok(ClientUpdate::State(state, _)) if state.value == value
            ) {
                return;
            }
        }
        panic!("large fragmented SSE snapshot was not delivered");
    }

    #[cfg(unix)]
    #[test]
    fn unix_fetch_and_refresh_match_http_contract() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("daemon.sock");
        let shared = Arc::new(SharedSnapshot::new(
            State::default(),
            CacheStore::new(dir.path().join("state.json")),
        ));
        let token_path = dir.path().join("token");
        let token = crate::load_or_create_token(&token_path).unwrap();
        let mut options = ServerOptions::new(Arc::clone(&shared), token);
        options.bind = None;
        options.unix_socket = Some(socket.clone());
        options.refresh_validator = Arc::new(|_, domain| domain == "mail");
        options.command_handler = Some(Arc::new(|operation, input| {
            if operation == "echo" {
                Ok(input)
            } else {
                Err("unknown operation".into())
            }
        }));
        let _handle = start_server(options).unwrap();
        let endpoint = format!("unix://{}", socket.display());
        let state: State = fetch_json(&endpoint, &token_path, "/snapshot").unwrap();
        assert_eq!(state.revision, 0);
        request_refresh(&endpoint, &token_path, "mail").unwrap();
        assert_eq!(shared.take_refresh_request().as_deref(), Some("mail"));
        let output: serde_json::Value = post_json(
            &endpoint,
            &token_path,
            "/command",
            &crate::CommandRequest {
                operation: "echo".into(),
                input: serde_json::json!({"hello": "world"}),
            },
        )
        .unwrap();
        assert_eq!(output["hello"], "world");
    }
}
