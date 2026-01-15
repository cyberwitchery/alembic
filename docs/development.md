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
cargo install cargo-tarpaulin --locked
cargo tarpaulin --workspace --all-features --out Xml
```

## release

- tag a release with `v*` to trigger the publish workflow.
- ensure `CARGO_REGISTRY_TOKEN` is set in repository secrets.
