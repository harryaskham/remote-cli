//! Cross-platform daemon log viewer used by host CLIs.

use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result, bail};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    #[default]
    Stderr,
    All,
}

#[derive(Clone, Debug)]
pub struct DaemonLogOptions {
    pub app_name: String,
    pub lines: usize,
    pub follow: bool,
    pub stream: LogStream,
    /// Explicit files override platform service-log discovery.
    pub files: Vec<PathBuf>,
}

impl DaemonLogOptions {
    #[must_use]
    pub fn new(app_name: impl Into<String>) -> Self {
        Self {
            app_name: app_name.into(),
            lines: 50,
            follow: false,
            stream: LogStream::Stderr,
            files: Vec::new(),
        }
    }
}

/// Show current service logs, blocking while `follow` is enabled.
///
/// Darwin services use the canonical launchd files emitted by
/// `mkDaemonModules`; Linux uses the user journal when available and falls back
/// to supervisord on Nix-on-Droid. Explicit files always use `tail`.
pub fn show_daemon_logs(options: &DaemonLogOptions) -> Result<()> {
    if !options.files.is_empty() {
        return tail_files(options, options.files.clone());
    }

    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .context("HOME is unavailable for daemon log discovery")?;
        let log_dir = home.join("Library").join("Logs");
        let stdout = log_dir.join(format!("{}-daemon.log", options.app_name));
        let stderr = log_dir.join(format!("{}-daemon.err.log", options.app_name));
        let files = match options.stream {
            LogStream::Stdout => vec![stdout],
            LogStream::Stderr => vec![stderr],
            LogStream::All => vec![stdout, stderr],
        };
        tail_files(options, files)
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let unit = format!("{}-daemon.service", options.app_name);
        if let Some(journalctl) = executable(&[
            "/run/current-system/sw/bin/journalctl",
            "/usr/bin/journalctl",
            "journalctl",
        ]) {
            let mut command = Command::new(journalctl);
            command
                .args(["--user", "--unit", &unit, "--no-pager", "--lines"])
                .arg(options.lines.max(1).to_string());
            if options.follow {
                command.arg("--follow");
            }
            return run(&mut command, "journalctl");
        }
        if let Some(supervisorctl) = executable(&[
            "/run/current-system/sw/bin/supervisorctl",
            "/usr/bin/supervisorctl",
            "supervisorctl",
        ]) {
            let program = format!("{}-daemon", options.app_name);
            let mut command = Command::new(supervisorctl);
            command.args(["tail"]);
            if options.follow {
                command.arg("-f");
            }
            command.arg(program);
            return run(&mut command, "supervisorctl tail");
        }
        bail!("neither journalctl nor supervisorctl is available; pass --file explicitly");
    }

    #[cfg(not(unix))]
    bail!("daemon log discovery is unsupported on this platform; pass --file explicitly")
}

fn tail_files(options: &DaemonLogOptions, files: Vec<PathBuf>) -> Result<()> {
    let tail = if cfg!(target_os = "macos") {
        PathBuf::from("/usr/bin/tail")
    } else {
        executable(&["/usr/bin/tail", "/bin/tail", "tail"]).context("tail is unavailable")?
    };
    let mut command = Command::new(tail);
    command.arg("-n").arg(options.lines.max(1).to_string());
    if options.follow {
        command.arg("-f");
    }
    command.args(files);
    run(&mut command, "tail daemon logs")
}

fn run(command: &mut Command, label: &str) -> Result<()> {
    let status = command.status().with_context(|| format!("start {label}"))?;
    if status.success() || status.code() == Some(130) {
        Ok(())
    } else {
        bail!("{label} exited with {status}")
    }
}

fn executable(candidates: &[&str]) -> Option<PathBuf> {
    for candidate in candidates {
        let path = PathBuf::from(candidate);
        if path.is_absolute() && path.is_file() {
            return Some(path);
        }
        if !path.is_absolute() {
            if let Some(found) = std::env::var_os("PATH").and_then(|paths| {
                std::env::split_paths(&paths)
                    .map(|dir| dir.join(&path))
                    .find(|path| path.is_file())
            }) {
                return Some(found);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_bounded_stderr() {
        let options = DaemonLogOptions::new("slick");
        assert_eq!(options.lines, 50);
        assert_eq!(options.stream, LogStream::Stderr);
        assert!(!options.follow);
    }
}
