use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    Http(String),
    #[cfg(unix)]
    Unix(PathBuf),
}

impl Endpoint {
    pub fn parse(value: &str) -> Result<Self> {
        if let Some(path) = value.strip_prefix("unix://") {
            #[cfg(unix)]
            {
                if path.is_empty() || !Path::new(path).is_absolute() {
                    bail!("unix daemon endpoint must contain an absolute socket path");
                }
                return Ok(Self::Unix(PathBuf::from(path)));
            }
            #[cfg(not(unix))]
            bail!("unix daemon endpoints are unsupported on this platform");
        }
        if value.starts_with("http://") || value.starts_with("https://") {
            return Ok(Self::Http(value.trim_end_matches('/').to_string()));
        }
        bail!("daemon endpoint must be http://, https://, or unix:///absolute/path")
    }

    #[must_use]
    pub fn display(&self) -> String {
        match self {
            Self::Http(url) => url.clone(),
            #[cfg(unix)]
            Self::Unix(path) => format!("unix://{}", path.display()),
        }
    }
}
