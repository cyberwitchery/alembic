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
- **nautobot**: stable
- **generic rest**: initial release (spec-driven)
- **peeringdb**: read-only (observer)
- **django**: write-only (emitter)

backends implement one of three capability traits in `alembic-engine`:
`Observer` (read-only), `Emitter` (write-only), or `Adapter: Observer + Emitter`
(read+write, optional schema provisioning). commands that need a missing
capability fail with a clear error (e.g. `apply` against peeringdb).

## core features

- deterministic plan/apply pipeline
- schema-required ir with typed references
- adapter-managed custom fields and tags
- interactive apply mode (`--interactive`)
- django scaffold generation via the write-only `django` backend

## build

```bash
cargo build --workspace
```

## tests

```bash
cargo test --workspace --all-features
```

## e2e

The NetBox + Infrahub end-to-end script provisions schema, applies objects, and imports them back to IR.

```bash
./scripts/e2e_netbox_infrahub.sh
```

Notes:
- Requires Docker and a sibling checkout of `../infrahub.rs` (uses its `docker-compose.yml`).
- Writes plans and imports under `/tmp` (see script output for paths).
- Use `SKIP_DOCKER=1` to reuse running containers, or `CLEANUP=1` to tear them down afterward.

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
