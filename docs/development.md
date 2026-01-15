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
cargo llvm-cov --workspace --all-features --fail-under-lines 80
```

## release

- tag a release with `v*` to trigger the publish workflow.
- ensure `CARGO_REGISTRY_TOKEN` is set in repository secrets.
