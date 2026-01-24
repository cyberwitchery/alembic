#!/bin/bash
# Local CI mirror script - ensures parity with .github/workflows/ci.yml
set -e

# Ensure we are in the alembic directory
cd "$(dirname "$0")/.."

echo "--- Lint & Analysis ---"
echo "Running fmt..."
cargo fmt --all -- --check

echo "Running clippy..."
# Added --all-features to match CI
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "--- Tests ---"
# Added env var used in CI for cast django tests
export ALEMBIC_CAST_PYTHON=python3
cargo test --workspace

echo "--- Coverage ---"
if command -v cargo-llvm-cov >/dev/null 2>&1; then
    # Aligned flags and regex with CI (ci.yml line 100)
    cargo llvm-cov --workspace --all-features --fail-under-lines 80 \
      --ignore-filename-regex "netbox\.rs/"
else
    echo "Error: cargo-llvm-cov is not installed."
    echo "Install it with: cargo install cargo-llvm-cov"
    exit 1
fi

echo "--- Personal Assurance Check: PASSED ---"