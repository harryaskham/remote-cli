#!/usr/bin/env bash
# One-command release: bump the version everywhere, commit, and push a tag.
#
#   nix run .#release -- patch     # 0.1.0 -> 0.1.1
#   nix run .#release -- minor     # 0.1.0 -> 0.2.0
#   nix run .#release -- major     # 0.1.0 -> 1.0.0
#   nix run .#release -- 1.4.2     # set an explicit version
#
# It bumps EVERY version string that must agree — Cargo.toml (+ Cargo.lock via
# `cargo update -p`) and the flake package version (Cargo.toml is the source of
# truth) — then commits and pushes a `vX.Y.Z` tag. The tag triggers
# .github/workflows/release.yml on the self-hosted fleet, which runs the test
# suite and publishes the library release (a tagged crate, not binary assets —
# this is a library, not a CLI).
#
# Flags:
#   --no-push     bump + commit + tag locally, but do not push.
#   --dry-run     print the planned new version and changes, touch nothing.

set -euo pipefail

bump=""
do_push=1
dry=0
for arg in "$@"; do
  case "$arg" in
    major | minor | patch) bump="$arg" ;;
    [0-9]*.[0-9]*.[0-9]*) bump="$arg" ;;
    --no-push) do_push=0 ;;
    --dry-run) dry=1 ;;
    -h | --help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown arg: $arg" >&2
      exit 2
      ;;
  esac
done
if [ -z "$bump" ]; then
  echo "usage: release.sh {major|minor|patch|X.Y.Z} [--no-push] [--dry-run]" >&2
  exit 2
fi

root="$(git rev-parse --show-toplevel)"
cd "$root"

cur="$(grep -m1 '^version' Cargo.toml | sed -E 's/.*"([^"]+)".*/\1/')"
if [ -z "$cur" ]; then
  echo "could not read current version from Cargo.toml" >&2
  exit 1
fi

if [[ "$bump" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  new="$bump"
else
  IFS=. read -r MA MI PA <<<"$cur"
  case "$bump" in
    major) new="$((MA + 1)).0.0" ;;
    minor) new="${MA}.$((MI + 1)).0" ;;
    patch) new="${MA}.${MI}.$((PA + 1))" ;;
  esac
fi

echo "==> $cur -> $new"

if [ "$dry" = "1" ]; then
  echo "[dry-run] would update Cargo.toml, Cargo.lock, flake.nix; commit; tag v$new"
  exit 0
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "working tree is dirty; commit or stash first" >&2
  exit 1
fi

# 1) Cargo.toml package version (first `version = "..."` under [package]).
sed -i -E "0,/^version = \"[^\"]+\"/s//version = \"$new\"/" Cargo.toml

# 2) flake.nix package version (the buildRustPackage `version = "...";`).
sed -i -E "s/version = \"$cur\"/version = \"$new\"/g" flake.nix

# 3) Cargo.lock — refresh just this package's entry.
if command -v cargo >/dev/null 2>&1; then
  cargo update -p remote-cli --precise "$new" --offline 2>/dev/null \
    || sed -i -E "0,/^name = \"remote-cli\"/{n;s/^version = \"[^\"]+\"/version = \"$new\"/}" Cargo.lock
else
  sed -i -E "0,/^name = \"remote-cli\"/{n;s/^version = \"[^\"]+\"/version = \"$new\"/}" Cargo.lock
fi

# Sanity: every tracked version string now agrees.
for f in Cargo.toml flake.nix; do
  if ! grep -q "\"$new\"" "$f"; then
    echo "version bump did not take in $f" >&2
    exit 1
  fi
done
echo "  updated: Cargo.toml, flake.nix, Cargo.lock"

git add Cargo.toml Cargo.lock flake.nix
git commit -q -m "release: v$new"
git tag -a "v$new" -m "v$new"
echo "  committed + tagged v$new"

if [ "$do_push" = "1" ]; then
  branch="$(git rev-parse --abbrev-ref HEAD)"
  git push origin "$branch"
  git push origin "v$new"
  echo "==> pushed v$new — release.yml will publish the library release on the self-hosted fleet"
else
  echo "==> --no-push: run \`git push origin <branch> && git push origin v$new\` when ready"
fi
