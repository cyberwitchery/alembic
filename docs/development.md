# development

## workspace

```
alembic/
  crates/
    alembic-core
    alembic-engine
    alembic-adapter-netbox
    alembic-cli
```

## Project Status

- **NetBox**: 100% (stable)
- **Nautobot**: 100% (stable, concurrent observation, projection proposal)
- **Generic REST**: Initial release (specification-driven)
- **PeeringDB**: Read-only support

### Core Features

- Concurrent observation for Nautobot adapter
- Plan summary output
- Interactive apply mode (`--interactive`)
- Enhanced error reporting with source location hints
- Pluggable state store backends (`StateBackend` trait)

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

## release

- tag a release with `v*` to trigger the publish workflow.
- ensure `CARGO_REGISTRY_TOKEN` is set in repository secrets.
