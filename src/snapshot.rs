use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::{SystemTime, UNIX_EPOCH};

/// State contract required by the generic cache and live client substrate.
pub trait Snapshot: Clone + Default + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Lowercase application id used for XDG cache/config paths.
    const APP_NAME: &'static str;
    /// Human-readable application name used in diagnostics.
    const DISPLAY_NAME: &'static str;
    /// Optional explicit cache-directory environment override.
    const CACHE_DIR_ENV: &'static str;

    fn normalize(&mut self) {}
    fn revision(&self) -> u64;
    fn set_revision(&mut self, revision: u64);
    fn saved_at(&self) -> Option<i64>;
    fn set_saved_at(&mut self, saved_at: Option<i64>);
    fn latest_refresh(&self) -> Option<i64> {
        None
    }
}

#[must_use]
pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}
