#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

V1="examples/talks/dknog-2026/core-v1.yaml"
V2="examples/talks/dknog-2026/core-v2.yaml"
EXT="examples/talks/dknog-2026/extended-model.yaml"
PLAN1="/tmp/dknog-plan-v1.json"
PLAN2="/tmp/dknog-plan-v2.json"

if [[ -z "${BACKEND_CONFIG:-}" ]]; then
  echo "BACKEND_CONFIG must be set to a backend config yaml."
  exit 1
fi

echo "== build =="
cargo build -p alembic-cli

echo "== validate core-v1 =="
cargo run -p alembic-cli -- validate -f "$V1"

echo "== plan core-v1 =="
cargo run -p alembic-cli -- plan -f "$V1" -o "$PLAN1" --backend-config "$BACKEND_CONFIG"

echo "== summarize core-v1 ops =="
jq '.ops | map(.type) | group_by(.) | map({op: .[0], count: length})' "$PLAN1"

echo "== apply core-v1 (interactive) =="
cargo run -p alembic-cli -- apply -p "$PLAN1" --interactive --backend-config "$BACKEND_CONFIG"

echo "== plan core-v2 =="
cargo run -p alembic-cli -- plan -f "$V2" -o "$PLAN2" --backend-config "$BACKEND_CONFIG"

echo "== summarize core-v2 ops =="
jq '.ops | map(.type) | group_by(.) | map({op: .[0], count: length})' "$PLAN2"

echo "== validate extended model (custom virt.* types) =="
cargo run -p alembic-cli -- validate -f "$EXT"

echo "Demo flow complete."
