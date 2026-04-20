#!/usr/bin/env bash
# dknog 2026 — command cheat sheet
# run from alembic workspace root

# --- pre-stage ---
export NETBOX_URL=https://netbox.example.com
export NETBOX_TOKEN=changeme
export RUST_LOG=warn
cargo build -p alembic-cli
rm -f /tmp/dknog-plan-v1.json /tmp/dknog-plan-v2.json

# --- 1:30 validate base model ---
cargo run -p alembic-cli -- validate -f examples/talks/dknog-2026/core-v1.yaml

# --- 3:00 plan v1 ---
cargo run -p alembic-cli -- plan -f examples/talks/dknog-2026/core-v1.yaml -o /tmp/dknog-plan-v1.json

# --- plan summary ---
jq '.ops | map(.type) | group_by(.) | map({op: .[0], count: length})' /tmp/dknog-plan-v1.json

# --- apply v1 (interactive) ---
cargo run -p alembic-cli -- apply -p /tmp/dknog-plan-v1.json --interactive

# --- 5:30 plan v2 (adds leaf02, changes leaf01 status) ---
cargo run -p alembic-cli -- plan -f examples/talks/dknog-2026/core-v2.yaml -o /tmp/dknog-plan-v2.json
jq '.ops | map(.type) | group_by(.) | map({op: .[0], count: length})' /tmp/dknog-plan-v2.json

# --- 7:30 validate custom entity model ---
cargo run -p alembic-cli -- validate -f examples/talks/dknog-2026/extended-model.yaml
