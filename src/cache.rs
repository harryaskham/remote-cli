use std::fs::{self, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::snapshot::{Snapshot, unix_now};

/// Atomic owner-only JSON snapshot store.
#[derive(Debug)]
pub struct CacheStore<S> {
    path: PathBuf,
    marker: PhantomData<fn() -> S>,
}

impl<S: Snapshot> Clone for CacheStore<S> {
    fn clone(&self) -> Self {
        Self::new(self.path.clone())
    }
}

impl<S: Snapshot> CacheStore<S> {
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            marker: PhantomData,
        }
    }

    #[must_use]
    pub fn default_path() -> PathBuf {
        if let Some(path) = std::env::var_os(S::CACHE_DIR_ENV) {
            return PathBuf::from(path).join("state.json");
        }
        dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(S::APP_NAME)
            .join("state.json")
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<S> {
        if !self.path.exists() {
            return Ok(S::default());
        }
        let bytes = fs::read(&self.path)
            .with_context(|| format!("read {} cache {}", S::DISPLAY_NAME, self.path.display()))?;
        let mut state: S = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse {} cache {}", S::DISPLAY_NAME, self.path.display()))?;
        state.normalize();
        Ok(state)
    }

    pub fn save(&self, state: &S) -> Result<()> {
        self.save_inner(state, true)
    }

    /// Preserve the authoritative source timestamp while writing through a
    /// daemon/SSE snapshot.
    pub fn save_exact(&self, state: &S) -> Result<()> {
        self.save_inner(state, false)
    }

    fn save_inner(&self, state: &S, stamp_now: bool) -> Result<()> {
        let parent = self
            .path
            .parent()
            .with_context(|| format!("{} cache path has no parent directory", S::DISPLAY_NAME))?;
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create {} cache directory {}",
                S::DISPLAY_NAME,
                parent.display()
            )
        })?;
        let mut snapshot = state.clone();
        snapshot.normalize();
        if stamp_now {
            snapshot.set_saved_at(Some(unix_now()));
        }
        let bytes = serde_json::to_vec_pretty(&snapshot)
            .with_context(|| format!("serialize {} cache", S::DISPLAY_NAME))?;
        let temporary = self
            .path
            .with_extension(format!("json.{}.tmp", std::process::id()));
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary).with_context(|| {
            format!(
                "create temporary {} cache {}",
                S::DISPLAY_NAME,
                temporary.display()
            )
        })?;
        file.write_all(&bytes).with_context(|| {
            format!(
                "write temporary {} cache {}",
                S::DISPLAY_NAME,
                temporary.display()
            )
        })?;
        file.sync_all().context("sync temporary cache")?;
        fs::rename(&temporary, &self.path).with_context(|| {
            format!(
                "atomically replace {} cache {} with {}",
                S::DISPLAY_NAME,
                self.path.display(),
                temporary.display()
            )
        })?;
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path).with_context(|| {
                format!("remove {} cache {}", S::DISPLAY_NAME, self.path.display())
            })?;
        }
        Ok(())
    }
}
