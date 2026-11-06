#!/usr/bin/env bash
# VERSION: 1
# Remove local build outputs for the current Nix flake.
#
# Usage:
#   scripts/clean.sh [--dry-run] [--rust-cache] [--root PATH]
#
# The command removes the Nix result symlinks, target/, and .direnv/.
# --rust-cache also removes the Cargo registry, git, and sccache caches.

set -euo pipefail

DRY_RUN=false
RUST_CACHE=false
ROOT=""

usage() {
  cat <<EOF
Usage: $0 [--dry-run] [--rust-cache] [--root PATH]

Remove local build outputs for the current Nix flake.

Options:
  --dry-run      Print the paths without removing them.
  --rust-cache   Also remove the Cargo registry, git, and sccache caches.
  --root PATH    Use PATH instead of searching for flake.nix.
  -h, --help     Show this help.

The command removes:
  result and result-* symlinks
  target/
  .direnv/

The --rust-cache option also removes:
  \$CARGO_HOME/registry/cache
  \$CARGO_HOME/registry/src
  \$CARGO_HOME/git/db
  \$CARGO_HOME/git/checkouts
  \$CARGO_HOME/.package-cache
  the sccache cache directory

The Nix store keeps build outputs until 'nix store gc' runs. This command
does not run a store garbage collection.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      ;;
    --rust-cache)
      RUST_CACHE=true
      ;;
    --root)
      if [ "$#" -lt 2 ] || [ -z "$2" ]; then
        echo "Error: --root needs a path." >&2
        exit 2
      fi
      ROOT="$2"
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [ -z "$ROOT" ]; then
  ROOT="$PWD"
  while [ ! -f "$ROOT/flake.nix" ]; do
    if [ "$ROOT" = "/" ]; then
      ROOT=""
      break
    fi
    ROOT="$(dirname "$ROOT")"
  done
fi

if [ -z "$ROOT" ]; then
  echo "Error: no flake.nix found in this directory or its parents." >&2
  echo "       Run the command in a flake, or use --root PATH." >&2
  exit 1
fi

if [ ! -d "$ROOT" ]; then
  echo "Error: root directory does not exist: $ROOT" >&2
  exit 1
fi

ROOT="$(cd "$ROOT" && pwd -P)"

if [ ! -f "$ROOT/flake.nix" ]; then
  echo "Error: flake.nix does not exist in $ROOT." >&2
  exit 1
fi

remove_path() {
  local path="$1"
  if [ ! -e "$path" ] && [ ! -L "$path" ]; then
    return
  fi
  if [ "$DRY_RUN" = true ]; then
    echo "would remove $path"
  else
    echo "remove $path"
    rm -rf -- "$path"
  fi
}

remove_rust_cache() {
  local cargo_home="${CARGO_HOME:-}"
  if [ -z "$cargo_home" ] && [ -n "${HOME:-}" ]; then
    cargo_home="$HOME/.cargo"
  fi
  if [ -n "$cargo_home" ]; then
    remove_path "$cargo_home/registry/cache"
    remove_path "$cargo_home/registry/src"
    remove_path "$cargo_home/git/db"
    remove_path "$cargo_home/git/checkouts"
    remove_path "$cargo_home/.package-cache"
  fi

  local sccache_dir="${SCCACHE_DIR:-}"
  if [ -z "$sccache_dir" ] && [ -n "${XDG_CACHE_HOME:-}" ]; then
    sccache_dir="$XDG_CACHE_HOME/sccache"
  fi
  if [ -z "$sccache_dir" ] && [ -n "${HOME:-}" ]; then
    if [ "$(uname -s)" = "Darwin" ]; then
      sccache_dir="$HOME/Library/Caches/Mozilla.sccache"
    else
      sccache_dir="$HOME/.cache/sccache"
    fi
  fi
  if [ -n "$sccache_dir" ]; then
    remove_path "$sccache_dir"
  fi
}

while IFS= read -r -d '' path; do
  remove_path "$path"
done < <(find "$ROOT" -maxdepth 1 -type l \( -name result -o -name 'result-*' \) -print0)

remove_path "$ROOT/target"
remove_path "$ROOT/.direnv"

if [ "$RUST_CACHE" = true ]; then
  remove_rust_cache
fi

if [ "$DRY_RUN" = true ]; then
  echo "Dry run complete: $ROOT"
else
  echo "Clean complete: $ROOT"
fi
