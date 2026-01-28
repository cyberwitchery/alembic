# development

## workspace

```
alembic/
  crates/
    alembic-core
    alembic-engine
    alembic-adapter-netbox
    alembic-adapter-nautobot
    alembic-adapter-generic
    alembic-adapter-peeringdb
    alembic-cli
```

## project status

- **netbox**: stable
- **nautobot**: stable (projection proposal supported)
- **generic rest**: initial release (spec-driven)
- **peeringdb**: read-only

## core features

- deterministic plan/apply pipeline
- schema-required ir with typed references
- projection support (custom fields/tags/local context)
- interactive apply mode (`--interactive`)
- django cast generation

## build

```bash
cargo build --workspace
```

## tests

```bash
cargo test --workspace --all-features
```

## linting

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## coverage

```bash
cargo install cargo-llvm-cov --locked
cargo llvm-cov --workspace --all-features --fail-under-lines 80 \
  --ignore-filename-regex "netbox\\.rs/"
```


## ci

```bash
./scripts/ci.sh
```

- runs fmt, clippy, tests, and coverage
- local mock servers require binding to loopback; some environments may need elevated privileges

## release

- tag a release with `v*` to trigger the publish workflow.
- ensure `CARGO_REGISTRY_TOKEN` is set in repository secrets.
