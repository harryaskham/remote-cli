use std::fs;
use std::io::{BufWriter, Write};
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
        // A PID alone is not unique: collector, reconnect and client threads
        // can save concurrently. Each writer owns an exclusive 0600 file in the
        // destination directory; RAII removes it on every error path.
        let mut temporary = tempfile::Builder::new()
            .prefix(".remote-cli-cache-")
            .tempfile_in(parent)
            .context("create private temporary cache")?;
        {
            // Stream compact JSON rather than allocating another cache-sized
            // pretty-printed buffer while the full snapshot is live.
            let mut writer = BufWriter::new(temporary.as_file_mut());
            serde_json::to_writer(&mut writer, &snapshot)
                .with_context(|| format!("serialize {} cache", S::DISPLAY_NAME))?;
            writer.flush().context("flush temporary cache")?;
        }
        temporary
            .as_file()
            .sync_all()
            .context("sync temporary cache")?;
        temporary.persist(&self.path).with_context(|| {
            format!(
                "atomically replace {} cache {}",
                S::DISPLAY_NAME,
                self.path.display()
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
