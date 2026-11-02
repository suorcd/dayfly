#!/usr/bin/env bash
# VERSION: 1
# Run dayfly locally with the Garage settings from OpenBao.
#
# Quick start:
#   nix develop
#   ./scripts/dev.sh
#   ./scripts/dev.sh --watch
#
# The script reads DAYFLY_BAO_PATH (default: secret/service/lan/dayfly),
# exports every field from that KV secret, and starts `cargo run -- serve`.
# It never prints secret values.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

BAO_PATH="${DAYFLY_BAO_PATH:-secret/service/lan/dayfly}"
BIND="${DAYFLY_HTTP_BIND:-127.0.0.1:8080}"
WATCH=false

usage() {
  cat <<EOF
Usage: $0 [--watch]

Runs dayfly locally on http://$BIND with S3 settings loaded from
OpenBao (DAYFLY_BAO_PATH=$BAO_PATH).

Options:
  --watch   Restart the server when Rust sources change (cargo-watch).
  -h, --help

Environment:
  DAYFLY_BAO_PATH    OpenBao KV path (default: secret/service/lan/dayfly)
  DAYFLY_HTTP_BIND   Bind address (default: 127.0.0.1:8080)
  RUST_LOG           Log filter (default: dayfly=info)
  BAO_TOKEN          OpenBao token; falls back to /run/openbao/token

By default this uses the same dayfly-files bucket as the deployed
instance. For isolation, create a separate bucket/key and point
DAYFLY_BAO_PATH at its KV secret.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --watch)
      WATCH=true
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

for cmd in bao jq cargo; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: '$cmd' not found. Run 'nix develop'." >&2
    exit 1
  fi
done

# The workstation OpenBao agent renews this token. Prefer a working
# BAO_TOKEN, but fall back to the agent file when the exported token is
# missing or expired.
if ! bao token lookup >/dev/null 2>&1 && [ -r /run/openbao/token ]; then
  BAO_TOKEN="$(cat /run/openbao/token)"
  export BAO_TOKEN
fi

if ! bao token lookup >/dev/null 2>&1; then
  echo "Error: OpenBao is not authenticated." >&2
  echo "       Export BAO_TOKEN, run 'bao login', or make /run/openbao/token readable." >&2
  exit 1
fi

if ! BAO_JSON="$(bao kv get -format=json "$BAO_PATH" 2>/dev/null)"; then
  echo "Error: could not read $BAO_PATH from OpenBao." >&2
  echo "       Check DAYFLY_BAO_PATH and that the token can read the path." >&2
  exit 1
fi

while IFS= read -r assignment; do
  [ -n "$assignment" ] || continue
  export "$assignment"
done < <(printf '%s' "$BAO_JSON" | jq -r '.data.data | to_entries[] | "\(.key)=\(.value)"')

for required in S3_ENDPOINT S3_BUCKET S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY; do
  if [ -z "${!required:-}" ]; then
    echo "Error: $required is missing or empty in $BAO_PATH." >&2
    exit 1
  fi
done

export DAYFLY_HTTP_BIND="$BIND"
export RUST_LOG="${RUST_LOG:-dayfly=info}"

echo "Dayfly local dev"
echo "  URL:     http://$BIND"
echo "  Bucket:  $S3_BUCKET"
echo "  OpenBao: $BAO_PATH"
echo "  Mode:    $([ "$WATCH" = true ] && echo "watch" || echo "one-shot")"

cd "$REPO_ROOT"

if [ "$WATCH" = true ]; then
  if ! command -v cargo-watch >/dev/null 2>&1; then
    echo "Error: 'cargo-watch' not found. Run 'nix develop'." >&2
    exit 1
  fi
  exec cargo watch -c -x "run -- serve --bind $BIND"
fi

exec cargo run -- serve --bind "$BIND"
