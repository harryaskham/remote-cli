use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

pub fn read_token(path: &Path) -> Result<String> {
    let path = expand_home(path)?;
    verify_private(&path)?;
    let token = fs::read_to_string(&path)
        .with_context(|| format!("read daemon token {}", path.display()))?
        .trim()
        .to_string();
    if token.len() < 32 {
        bail!("daemon token {} is empty or too short", path.display());
    }
    Ok(token)
}

pub fn load_or_create_token(path: &Path) -> Result<String> {
    let path = expand_home(path)?;
    // `Path::exists` follows symlinks and reports false for a dangling SOPS
    // link. Inspect the directory entry instead: an existing symlink must only
    // ever be read, never replaced or treated as permission to mint a token.
    match fs::symlink_metadata(&path) {
        Ok(_) => return read_token(&path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("inspect daemon token {}", path.display()));
        }
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
        .open(&path)
        .with_context(|| format!("create daemon token {}", path.display()))?;
    writeln!(file, "{token}")?;
    file.sync_all()?;
    Ok(token)
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let Ok(relative) = path.strip_prefix("~") else {
        return Ok(path.to_path_buf());
    };
    let home = dirs::home_dir().context("expand daemon token path: home directory unavailable")?;
    Ok(home.join(relative))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_only_a_standalone_tilde_component() {
        let home = dirs::home_dir().unwrap();
        assert_eq!(
            expand_home(Path::new("~/.config/slick/daemon-token")).unwrap(),
            home.join(".config/slick/daemon-token")
        );
        assert_eq!(
            expand_home(Path::new("~someone/token")).unwrap(),
            PathBuf::from("~someone/token")
        );
        assert_eq!(
            expand_home(Path::new("relative/token")).unwrap(),
            PathBuf::from("relative/token")
        );
    }

    #[cfg(unix)]
    #[test]
    fn reads_a_private_token_through_a_symlink() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("sops/daemon");
        let link = dir.path().join("daemon-token");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, format!("{}\n", "a".repeat(64))).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o400)).unwrap();
        symlink(&target, &link).unwrap();

        assert_eq!(read_token(&link).unwrap(), "a".repeat(64));
        assert_eq!(load_or_create_token(&link).unwrap(), "a".repeat(64));
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn dangling_token_symlink_is_never_replaced() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("daemon-token");
        symlink("missing-sops-secret", &link).unwrap();

        let error = load_or_create_token(&link).unwrap_err();
        assert!(format!("{error:#}").contains("inspect daemon token"));
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read_link(&link).unwrap(),
            PathBuf::from("missing-sops-secret")
        );
    }
}
