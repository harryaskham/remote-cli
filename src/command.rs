//! Classified daemon commands for writes whose outcome matters.
//!
//! Retry-capable callers (review queues such as Outbox, agents, scripts) must
//! know whether a write may have happened. [`command`] classifies every
//! failure, and daemon command handlers tag their error text with a class so
//! the client can tell rejections from transient failures through the
//! conduit's generic HTTP 500:
//!
//! | Exit | Class | Meaning | Examples |
//! |---|---|---|---|
//! | 75 (`EX_TEMPFAIL`) | [`CommandError::Unreachable`], [`CommandError::NotRun`] | not done; safe to retry | DNS, refused, connect timeout, TLS, token rejected, daemon too old, handler `[retry]` |
//! | 76 | [`CommandError::Unknown`] | outcome unknown; it may have happened | timeout or reset after the request was sent, unreadable reply, handler `[unknown]`, untagged handler error |
//! | 1 | [`CommandError::Rejected`] | rejected, not done; retrying will not help | HTTP 400, handler `[rejected]`, invalid endpoint |

use std::path::Path;
use std::time::Duration;

use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::endpoint::Endpoint;
use crate::server::CommandRequest;

/// Not done; safe to retry (`EX_TEMPFAIL`).
pub const EXIT_TEMPFAIL: u8 = 75;
/// The request reached the daemon, but its outcome is unknown.
pub const EXIT_UNKNOWN: u8 = 76;

/// Handler error tags; the text after the tag is shown to the caller.
pub const CLASS_RETRY: &str = "[retry] ";
pub const CLASS_REJECTED: &str = "[rejected] ";
pub const CLASS_UNKNOWN: &str = "[unknown] ";

/// Exit-status help for CLIs that send through [`command`].
pub const EXIT_HELP: &str = "Exit status:
  0   success
  1   failed; for writes: rejected and not done (retrying will not help)
  75  not done: the daemon could not be reached or did not run the command
      (connection, DNS, TLS, token, throttling); safe to retry
  76  the request reached the daemon but its outcome is unknown, so it may
      have happened; check before retrying, or retry with the same
      --idempotency-key";

/// A classified command failure.
#[derive(Debug)]
pub enum CommandError {
    /// The daemon was never reached.
    Unreachable { endpoint: String, cause: String },
    /// The daemon answered but did not run the command.
    NotRun { endpoint: String, reason: String },
    /// The command was refused; it did not happen.
    Rejected { reason: String },
    /// The command may or may not have happened.
    Unknown { endpoint: String, cause: String },
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable { endpoint, cause } => {
                write!(f, "cannot reach daemon at {endpoint}: {cause}")
            }
            Self::NotRun { endpoint, reason } => {
                write!(f, "daemon at {endpoint} did not run the command: {reason}")
            }
            Self::Rejected { reason } => f.write_str(reason),
            Self::Unknown { endpoint, cause } => write!(
                f,
                "outcome unknown at daemon {endpoint} (it may have happened): {cause}"
            ),
        }
    }
}

impl std::error::Error for CommandError {}

impl CommandError {
    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        match self {
            Self::Unreachable { .. } | Self::NotRun { .. } => EXIT_TEMPFAIL,
            Self::Unknown { .. } => EXIT_UNKNOWN,
            Self::Rejected { .. } => 1,
        }
    }

    /// Whether the command certainly did not happen.
    #[must_use]
    pub const fn not_done(&self) -> bool {
        !matches!(self, Self::Unknown { .. })
    }
}

/// The exit status of the first [`CommandError`] in an error chain.
#[must_use]
pub fn exit_code(error: &anyhow::Error) -> Option<u8> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<CommandError>())
        .map(CommandError::exit_code)
}

/// Tag a handler error with its class.
#[must_use]
pub fn tag(class: &str, message: impl std::fmt::Display) -> String {
    format!("{class}{message}")
}

/// Run one daemon command and classify any failure.
pub fn command<I: Serialize, O: DeserializeOwned>(
    endpoint: &str,
    token_path: &Path,
    operation: &str,
    input: &I,
    timeout: Duration,
) -> Result<O, CommandError> {
    let parsed = Endpoint::parse(endpoint).map_err(|error| CommandError::Rejected {
        reason: format!("invalid daemon endpoint {endpoint}: {error:#}"),
    })?;
    let endpoint = endpoint.to_string();
    let token = crate::read_token(token_path).map_err(|error| CommandError::NotRun {
        endpoint: endpoint.clone(),
        reason: format!("no daemon token: {error:#}"),
    })?;
    let request = CommandRequest {
        operation: operation.into(),
        input: serde_json::to_value(input).map_err(|error| CommandError::Rejected {
            reason: format!("encode {operation} input: {error}"),
        })?,
    };
    let (status, body) = match parsed {
        Endpoint::Http(base) => http_post(&endpoint, &base, &token, &request, timeout)?,
        #[cfg(unix)]
        Endpoint::Unix(socket) => unix_post(&endpoint, &socket, &token, &request, timeout)?,
    };
    classify(&endpoint, operation, status, &body)
}

fn cause_text(error: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![error.to_string()];
    let mut current = error.source();
    while let Some(cause) = current {
        let text = cause.to_string();
        if !parts.contains(&text) {
            parts.push(text);
        }
        current = cause.source();
    }
    parts.join(": ")
}

fn http_post(
    endpoint: &str,
    base: &str,
    token: &str,
    request: &CommandRequest,
    timeout: Duration,
) -> Result<(u16, String), CommandError> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(timeout)
        .build()
        .map_err(|error| CommandError::NotRun {
            endpoint: endpoint.into(),
            reason: format!("build HTTP client: {}", cause_text(&error)),
        })?;
    let response = client
        .post(format!("{base}/command"))
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(ACCEPT, "application/json")
        .json(request)
        .send()
        .map_err(|error| {
            // Connect failures (DNS, refused, connect timeout, TLS) happen
            // before any byte of the request reaches the daemon.
            if error.is_connect() || error.is_builder() {
                CommandError::Unreachable {
                    endpoint: endpoint.into(),
                    cause: cause_text(&error),
                }
            } else {
                CommandError::Unknown {
                    endpoint: endpoint.into(),
                    cause: cause_text(&error),
                }
            }
        })?;
    let status = response.status().as_u16();
    let body = response.text().map_err(|error| CommandError::Unknown {
        endpoint: endpoint.into(),
        cause: format!("read reply (HTTP {status}): {}", cause_text(&error)),
    })?;
    Ok((status, body))
}

#[cfg(unix)]
fn unix_post(
    endpoint: &str,
    socket: &Path,
    token: &str,
    request: &CommandRequest,
    timeout: Duration,
) -> Result<(u16, String), CommandError> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    let unreachable = |error: std::io::Error| CommandError::Unreachable {
        endpoint: endpoint.into(),
        cause: error.to_string(),
    };
    let body = serde_json::to_vec(request).map_err(|error| CommandError::Rejected {
        reason: format!("encode command: {error}"),
    })?;
    let mut stream = UnixStream::connect(socket).map_err(unreachable)?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(unreachable)?;
    // The daemon reads the whole declared body before running anything, so a
    // failed or partial write cannot have run the command.
    write!(
        stream,
        "POST /command HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .and_then(|()| stream.write_all(&body))
    .and_then(|()| stream.flush())
    .and_then(|()| stream.shutdown(std::net::Shutdown::Write))
    .map_err(unreachable)?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| CommandError::Unknown {
            endpoint: endpoint.into(),
            cause: format!("read reply: {error}"),
        })?;
    let unreadable = || CommandError::Unknown {
        endpoint: endpoint.into(),
        cause: "the daemon's reply was not HTTP".into(),
    };
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(unreadable)?;
    let status = std::str::from_utf8(&response[..split])
        .ok()
        .and_then(|headers| headers.lines().next())
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(unreadable)?;
    Ok((
        status,
        String::from_utf8_lossy(&response[split + 4..]).into_owned(),
    ))
}

fn classify<O: DeserializeOwned>(
    endpoint: &str,
    operation: &str,
    status: u16,
    text: &str,
) -> Result<O, CommandError> {
    let endpoint = endpoint.to_string();
    let detail = || {
        serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(|e| e.as_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| text.trim().chars().take(300).collect())
    };
    match status {
        200..=299 => serde_json::from_str(text).map_err(|error| CommandError::Unknown {
            endpoint,
            cause: format!("{operation} ran but its reply was unreadable: {error}"),
        }),
        401 | 403 => Err(CommandError::NotRun {
            endpoint,
            reason: format!("the daemon rejected this client's token (HTTP {status})"),
        }),
        404 => Err(CommandError::NotRun {
            endpoint,
            reason: "the daemon's command conduit is disabled (HTTP 404)".into(),
        }),
        // Malformed request or oversized body: refused before running.
        400 | 413 | 431 => Err(CommandError::Rejected {
            reason: format!(
                "the daemon rejected the request (HTTP {status}): {}",
                detail()
            ),
        }),
        500 => {
            let message = detail();
            if let Some(rest) = message.strip_prefix(CLASS_RETRY) {
                Err(CommandError::NotRun {
                    endpoint,
                    reason: rest.into(),
                })
            } else if let Some(rest) = message.strip_prefix(CLASS_REJECTED) {
                Err(CommandError::Rejected {
                    reason: rest.into(),
                })
            } else if let Some(rest) = message.strip_prefix(CLASS_UNKNOWN) {
                Err(CommandError::Unknown {
                    endpoint,
                    cause: rest.into(),
                })
            } else if message.contains("unsupported") && message.contains(operation) {
                Err(CommandError::NotRun {
                    endpoint,
                    reason: format!("this daemon does not support {operation} yet; upgrade it"),
                })
            } else {
                // A handler without classes: assume the worst.
                Err(CommandError::Unknown {
                    endpoint,
                    cause: message,
                })
            }
        }
        502 | 503 => Err(CommandError::NotRun {
            endpoint,
            reason: format!("HTTP {status} before the daemon ran the command"),
        }),
        _ => Err(CommandError::Unknown {
            endpoint,
            cause: format!("HTTP {status}: {}", detail()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{CacheStore, ServerOptions, SharedSnapshot, Snapshot, start_server};

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct State {
        revision: u64,
        saved_at: Option<i64>,
    }

    impl Snapshot for State {
        const APP_NAME: &'static str = "remote-cli-command-test";
        const DISPLAY_NAME: &'static str = "Command test";
        const CACHE_DIR_ENV: &'static str = "REMOTE_CLI_COMMAND_TEST_CACHE_DIR";
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

    fn options(dir: &Path) -> (ServerOptions<State>, std::path::PathBuf) {
        let shared = Arc::new(SharedSnapshot::new(
            State::default(),
            CacheStore::new(dir.join("state.json")),
        ));
        let token_path = dir.join("token");
        let token = crate::load_or_create_token(&token_path).unwrap();
        let mut options = ServerOptions::new(shared, token);
        options.command_handler = Some(Arc::new(|operation, input| match operation {
            "ok" => Ok(serde_json::json!({"echo": input})),
            "retry" => Err(tag(CLASS_RETRY, "throttled")),
            "rejected" => Err(tag(CLASS_REJECTED, "no such chat")),
            "unknown" => Err(tag(CLASS_UNKNOWN, "timed out after posting")),
            "untagged" => Err("something broke".into()),
            "slow" => {
                std::thread::sleep(Duration::from_millis(1500));
                Ok(serde_json::json!({}))
            }
            other => Err(format!("unsupported daemon operation: {other}")),
        }));
        (options, token_path)
    }

    fn run(
        endpoint: &str,
        token: &Path,
        operation: &str,
        timeout: Duration,
    ) -> Result<serde_json::Value, CommandError> {
        command(
            endpoint,
            token,
            operation,
            &serde_json::json!({"x": 1}),
            timeout,
        )
    }

    #[test]
    fn classifies_handler_outcomes_over_http() {
        let dir = tempfile::tempdir().unwrap();
        let (options, token) = options(dir.path());
        let handle = start_server(options).unwrap();
        let url = format!("http://{}", handle.http_address.unwrap());
        let second = Duration::from_secs(5);
        assert_eq!(run(&url, &token, "ok", second).unwrap()["echo"]["x"], 1);
        let code = |operation: &str| {
            run(&url, &token, operation, second)
                .unwrap_err()
                .exit_code()
        };
        assert_eq!(code("retry"), EXIT_TEMPFAIL);
        assert_eq!(code("rejected"), 1);
        assert_eq!(code("unknown"), EXIT_UNKNOWN);
        assert_eq!(code("untagged"), EXIT_UNKNOWN);
        // An older daemon that lacks the operation never ran it.
        let error = run(&url, &token, "newer.op", second).unwrap_err();
        assert_eq!(error.exit_code(), EXIT_TEMPFAIL);
        assert!(error.to_string().contains("upgrade"));
        // A timeout after the request was sent may have run the command.
        let error = run(&url, &token, "slow", Duration::from_millis(300)).unwrap_err();
        assert_eq!(error.exit_code(), EXIT_UNKNOWN, "{error}");
        // A wrong token is refused before anything runs.
        let wrong = dir.path().join("wrong");
        std::fs::write(&wrong, "w".repeat(64)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wrong, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(
            run(&url, &wrong, "ok", second).unwrap_err().exit_code(),
            EXIT_TEMPFAIL
        );
    }

    #[test]
    fn unreachable_and_invalid_endpoints_are_not_done() {
        let dir = tempfile::tempdir().unwrap();
        let (_, token) = options(dir.path());
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = closed.local_addr().unwrap();
        drop(closed);
        let error = run(
            &format!("http://{address}"),
            &token,
            "ok",
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(matches!(error, CommandError::Unreachable { .. }), "{error}");
        assert!(error.not_done());
        let error = run("ftp://nope", &token, "ok", Duration::from_secs(5)).unwrap_err();
        assert_eq!(error.exit_code(), 1);
        let missing = dir.path().join("missing-token");
        let error = run(
            &format!("http://{address}"),
            &missing,
            "ok",
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert_eq!(error.exit_code(), EXIT_TEMPFAIL);
        let chained = anyhow::Error::new(error).context("send mail");
        assert_eq!(exit_code(&chained), Some(EXIT_TEMPFAIL));
    }

    #[cfg(unix)]
    #[test]
    fn classifies_over_unix_sockets() {
        let dir = tempfile::tempdir().unwrap();
        let (mut options, token) = options(dir.path());
        let socket = dir.path().join("command.sock");
        options.bind = None;
        options.unix_socket = Some(socket.clone());
        let _handle = start_server(options).unwrap();
        let url = format!("unix://{}", socket.display());
        let second = Duration::from_secs(5);
        assert_eq!(run(&url, &token, "ok", second).unwrap()["echo"]["x"], 1);
        assert_eq!(
            run(&url, &token, "rejected", second)
                .unwrap_err()
                .exit_code(),
            1
        );
        assert_eq!(
            run(&url, &token, "unknown", second)
                .unwrap_err()
                .exit_code(),
            EXIT_UNKNOWN
        );
        let gone = format!("unix://{}", dir.path().join("gone.sock").display());
        assert_eq!(
            run(&gone, &token, "ok", second).unwrap_err().exit_code(),
            EXIT_TEMPFAIL
        );
    }
}
