use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Smart-client source and fallback policy shared across host applications.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, rename_all = "kebab-case")]
pub struct ClientConfig {
    pub cache: bool,
    pub daemon: bool,
    /// HTTP(S) URL or `unix:///absolute/path.sock`.
    pub daemon_url: String,
    pub token_file: Option<PathBuf>,
    pub fallback: bool,
    pub fallback_timeout_secs: u64,
    pub fallback_lease_file: Option<PathBuf>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            cache: true,
            daemon: true,
            daemon_url: "http://127.0.0.1:7612".into(),
            token_file: None,
            fallback: true,
            fallback_timeout_secs: 90,
            fallback_lease_file: None,
        }
    }
}

impl ClientConfig {
    #[must_use]
    pub fn with_daemon_url(mut self, daemon_url: impl Into<String>) -> Self {
        self.daemon_url = daemon_url.into();
        self
    }

    #[must_use]
    pub fn token_path(&self, daemon_token_path: &Path) -> PathBuf {
        self.token_file
            .clone()
            .unwrap_or_else(|| daemon_token_path.to_path_buf())
    }

    #[must_use]
    pub fn fallback_lease_path(&self, config_dir: &Path) -> PathBuf {
        self.fallback_lease_file
            .clone()
            .unwrap_or_else(|| config_dir.join("fallback-collector.lock"))
    }
}

/// Independent HTTP request bounds. Headers include their final CRLF separator;
/// the body limit counts payload bytes, not headers or JSON characters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct HttpLimits {
    pub max_header_bytes: usize,
    pub max_body_bytes: usize,
}

impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            max_header_bytes: 16 * 1024,
            max_body_bytes: 1024 * 1024,
        }
    }
}

impl HttpLimits {
    pub fn validate(self) -> anyhow::Result<()> {
        if self.max_header_bytes < 4 {
            anyhow::bail!("HTTP max-header-bytes must be at least 4");
        }
        if self
            .max_header_bytes
            .checked_add(self.max_body_bytes)
            .is_none()
        {
            anyhow::bail!("combined HTTP limits exceed the platform's addressable size");
        }
        Ok(())
    }
}

/// Central collector and transport policy.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, rename_all = "kebab-case")]
pub struct DaemonConfig {
    /// Optional HTTP bind. Empty disables TCP/HTTP while retaining a Unix bind.
    pub bind: String,
    /// Optional owner-local Unix-domain socket.
    pub unix_socket: Option<PathBuf>,
    pub token_file: Option<PathBuf>,
    /// Hosts forward this to `ServerOptions::http_limits`.
    pub http_limits: HttpLimits,
    pub min_refresh_secs: u64,
    /// Opt out of the automatic loopback alias that otherwise accompanies a
    /// specific non-loopback `bind` (e.g. a Tailscale IP). When false (the
    /// default), a non-loopback bind also serves `127.0.0.1` on the same port
    /// so local clients keep working without exposing the wildcard LAN.
    /// Consumers map this onto `ServerOptions::disable_default_loopback`.
    pub disable_default_loopback: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:7612".into(),
            unix_socket: None,
            token_file: None,
            http_limits: HttpLimits::default(),
            min_refresh_secs: 2,
            disable_default_loopback: false,
        }
    }
}

impl DaemonConfig {
    #[must_use]
    pub fn token_path(&self, config_dir: &Path) -> PathBuf {
        self.token_file
            .clone()
            .unwrap_or_else(|| config_dir.join("daemon-token"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_limits_default_independently_and_round_trip() {
        let config: DaemonConfig = serde_json::from_value(serde_json::json!({
            "http-limits": { "max-body-bytes": 8 * 1024 * 1024 }
        }))
        .unwrap();
        assert_eq!(config.http_limits.max_header_bytes, 16 * 1024);
        assert_eq!(config.http_limits.max_body_bytes, 8 * 1024 * 1024);
        assert_eq!(
            DaemonConfig::default().http_limits.max_body_bytes,
            1024 * 1024
        );
        assert_eq!(
            serde_json::from_value::<DaemonConfig>(serde_json::to_value(&config).unwrap()).unwrap(),
            config
        );
        assert!(
            serde_json::from_value::<HttpLimits>(serde_json::json!({"max-bdy-bytes": 7})).is_err()
        );
    }

    #[test]
    fn limits_validate_arithmetic_and_allow_bodyless_services() {
        assert!(
            HttpLimits {
                max_body_bytes: 0,
                ..HttpLimits::default()
            }
            .validate()
            .is_ok()
        );
        assert!(
            HttpLimits {
                max_header_bytes: 3,
                ..HttpLimits::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            HttpLimits {
                max_body_bytes: usize::MAX,
                ..HttpLimits::default()
            }
            .validate()
            .is_err()
        );
    }
}
