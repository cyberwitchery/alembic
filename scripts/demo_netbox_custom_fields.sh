#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -z "${NETBOX_URL:-}" || -z "${NETBOX_TOKEN:-}" ]]; then
  echo "NETBOX_URL and NETBOX_TOKEN must be set."
  echo "Example:"
  echo "  export NETBOX_URL=http://localhost:8000"
  echo "  export NETBOX_TOKEN=..."
  exit 1
fi

INVENTORY="examples/talks/dknog-2026/core-v1.yaml"
PLAN="/tmp/dknog-netbox-plan.json"
NETBOX_CONFIG="$(mktemp /tmp/alembic-netbox-XXXX.yaml)"
trap 'rm -f "$NETBOX_CONFIG"' EXIT

cat >"$NETBOX_CONFIG" <<EOF
backend: netbox
url: "$NETBOX_URL"
token: "$NETBOX_TOKEN"
EOF

echo "== preflight =="
curl -fsS -H "Authorization: Token ${NETBOX_TOKEN}" "${NETBOX_URL%/}/api/" >/dev/null

echo "== validate =="
cargo run -p alembic-cli -- validate -f "$INVENTORY"

echo "== plan (custom fields/tags provisioned on apply) =="
cargo run -p alembic-cli -- plan \
  -f "$INVENTORY" \
  --backend-config "$NETBOX_CONFIG" \
  -o "$PLAN"

echo "== apply interactively =="
cargo run -p alembic-cli -- apply \
  -p "$PLAN" \
  --backend-config "$NETBOX_CONFIG" \
  --interactive

echo "NetBox demo complete."
