use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;

use anyhow::{Context, Result, bail};

pub fn read_token(path: &Path) -> Result<String> {
    verify_private(path)?;
    let token = fs::read_to_string(path)
        .with_context(|| format!("read daemon token {}", path.display()))?
        .trim()
        .to_string();
    if token.len() < 32 {
        bail!("daemon token {} is empty or too short", path.display());
    }
    Ok(token)
}

pub fn load_or_create_token(path: &Path) -> Result<String> {
    if path.exists() {
        return read_token(path);
    }
    let parent = path.parent().context("daemon token path has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create daemon config directory {}", parent.display()))?;
    let mut random = [0_u8; 32];
    getrandom::fill(&mut random)
        .map_err(|error| anyhow::anyhow!("read operating-system random source: {error}"))?;
    let mut token = String::with_capacity(random.len() * 2);
    for byte in random {
        write!(&mut token, "{byte:02x}").expect("writing to String cannot fail");
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("create daemon token {}", path.display()))?;
    writeln!(file, "{token}")?;
    file.sync_all()?;
    Ok(token)
}

fn verify_private(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path)
            .with_context(|| format!("inspect daemon token {}", path.display()))?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            bail!(
                "daemon token {} has unsafe mode {mode:o}; require 0600 or stricter",
                path.display()
            );
        }
    }
    Ok(())
}
