# remote-cli

Canonical cache/daemon/client transport for Rust applications in the
`mcp-cli` ecosystem. It is the extracted and generalized architecture used by
Slick: one collector owns API traffic, writes an atomic cache, and publishes the
same revision through authenticated snapshots and Server-Sent Events.

Domain scheduling remains in the host. `remote-cli` owns the hard reusable
parts:

- generic typed JSON snapshots with monotonic revisions;
- owner-only atomic cache replacement and cache file watching;
- authenticated `GET /snapshot`, `GET /events`, `GET /health`,
  `POST /refresh?domain=...`, and an optional typed `POST /command` conduit;
- remote HTTP(S) SSE or owner-local `unix:///absolute/path.sock` using the same
  protocol;
- daemon-to-client cache write-through and revision deduplication;
- bounded reconnect backoff and SSE safety limits;
- source health reporting;
- one-owner fallback lease, stale lease recovery, and immediate daemon recovery;
- secure random bearer-token creation and permission checks; token paths expand
  a leading `~`, and SOPS-managed token symlinks are read without replacement
  (including a hard failure rather than token creation for dangling links).

## Host contract

Implement `Snapshot` for the host's state:

```rust
use remote_cli::Snapshot;

# #[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
# struct State { revision: u64, saved_at: Option<i64> }
impl Snapshot for State {
    const APP_NAME: &'static str = "myapp";
    const DISPLAY_NAME: &'static str = "My App";
    const CACHE_DIR_ENV: &'static str = "MYAPP_CACHE_DIR";
    fn revision(&self) -> u64 { self.revision }
    fn set_revision(&mut self, value: u64) { self.revision = value; }
    fn saved_at(&self) -> Option<i64> { self.saved_at }
    fn set_saved_at(&mut self, value: Option<i64>) { self.saved_at = value; }
}
```

The daemon constructs `SharedSnapshot<State>`, starts `start_server`, and uses
`update`/`replace_payload` after each collector task. The smart client creates a
`ClientSubscription<State>` and consumes typed `ClientUpdate` events.

Local sockets and remote HTTP are selected only by endpoint configuration:

```yaml
client:
  daemon-url: unix:///run/user/1000/myapp/daemon.sock
# or: http://private-host:7612
daemon:
  bind: 127.0.0.1:7612
  unix-socket: /run/user/1000/myapp/daemon.sock
```

Bearer authentication remains mandatory on Unix sockets, ensuring identical
behavior when a configuration changes from local to remote transport. The
optional command conduit lets a host keep source credentials, semantic queries,
and confirmation-gated mutations inside its single persistent daemon rather
than spawning an upstream client from every CLI/MCP process.

## Nix service mixin

The flake exports `lib.mkDaemonModules`, which generates matching NixOS,
nix-darwin, and Nix-on-Droid modules for a host binary conforming to:

```text
<app> --config FILE --cache FILE daemon --bind ADDR [--unix-socket PATH] [--token-file FILE]
```

This keeps package-specific modules declarative while preserving one canonical
restart, token, cache, and path contract. Every generated service also exposes:

```nix
services.<app>.preferLocalBinary = true;
services.<app>.localBinary = null; # defaults at runtime to $HOME/.local/bin/<binary>
```

When enabled, a runtime launcher uses the executable local update when present
and executable, otherwise falling back to the immutable Nix package. This is
compatible with `updatable-cli` without making service evaluation depend on a
mutable home-directory path.
