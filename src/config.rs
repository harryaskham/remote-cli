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

/// Central collector and transport policy.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(default, rename_all = "kebab-case")]
pub struct DaemonConfig {
    /// Optional HTTP bind. Empty disables TCP/HTTP while retaining a Unix bind.
    pub bind: String,
    /// Optional owner-local Unix-domain socket.
    pub unix_socket: Option<PathBuf>,
    pub token_file: Option<PathBuf>,
    pub min_refresh_secs: u64,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:7612".into(),
            unix_socket: None,
            token_file: None,
            min_refresh_secs: 2,
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
