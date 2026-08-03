#!/bin/bash
# local ci mirror script - ensures parity with .github/workflows/ci.yml
set -e

# ensure we are in the alembic directory
cd "$(dirname "$0")/.."

echo "--- Lint & Analysis ---"
echo "Running fmt..."
cargo fmt --all -- --check

echo "Running clippy..."
# --all-features matches ci
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "Running cargo-deny..."
if command -v cargo-deny >/dev/null 2>&1; then
    cargo deny check
else
    echo "Error: cargo-deny is not installed."
    echo "Install it with: cargo install cargo-deny"
    exit 1
fi

echo "--- Docs ---"
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

echo "--- Tests & Coverage ---"
# env var used in ci for the django e2e tests
export ALEMBIC_DJANGO_PYTHON=python3
if [ -z "${ALEMBIC_TEST_POSTGRES_URL:-}" ]; then
    echo "Note: ALEMBIC_TEST_POSTGRES_URL is unset, so the postgres state store tests no-op. ci runs them against a service."
fi
if command -v cargo-llvm-cov >/dev/null 2>&1; then
    # llvm-cov runs the suite and does not build examples itself
    cargo build --examples
    # fail under 80% line coverage, excluding the netbox.rs client sources
    cargo llvm-cov --workspace --all-features --fail-under-lines 80 \
      --ignore-filename-regex "netbox\\.rs/"
else
    echo "Error: cargo-llvm-cov is not installed."
    echo "Install it with: cargo install cargo-llvm-cov"
    exit 1
fi

echo "--- Personal Assurance Check: PASSED ---"
