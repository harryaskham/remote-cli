//! Canonical daemon/client substrate for the harryaskham Rust CLI stack.
//!
//! The crate separates domain collection from transport. A host implements
//! [`Snapshot`] for its normalized state and keeps its scheduler/collector in
//! the host crate; `remote-cli` provides atomic cache persistence, authenticated
//! snapshots, SSE, local Unix sockets, remote HTTP, refresh requests, source
//! deduplication, health, and the single-owner embedded-fallback lease.

pub mod auth;
pub mod cache;
pub mod client;
pub mod config;
pub mod endpoint;
pub mod logs;
pub mod server;
pub mod snapshot;

pub use auth::{load_or_create_token, read_token};
pub use cache::CacheStore;
pub use client::{
    ClientHealth, ClientOptions, ClientSubscription, ClientUpdate, FallbackLease, fetch_json,
    post_json, request_refresh,
};
pub use config::{ClientConfig, DaemonConfig};
pub use endpoint::Endpoint;
pub use logs::{DaemonLogOptions, LogStream, show_daemon_logs};
pub use server::{
    CommandHandler, CommandRequest, ProjectedResponse, ServerHandle, ServerOptions, SharedSnapshot,
    SnapshotProjector, start_server,
};
pub use snapshot::{Snapshot, unix_now};
