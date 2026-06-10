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
- **peeringdb**: read-only

## core features

- deterministic plan/apply pipeline
- schema-required ir with typed references
- adapter-managed custom fields and tags
- interactive apply mode (`--interactive`)
- django scaffold generation via `alembic cast django`

## build

```bash
cargo build --workspace
```

## tests

```bash
cargo test --workspace --all-features
```

### test environment variables

most of the suite is hermetic and needs no setup. a few tests are opt-in: they **skip silently** (early return, not a failure) when their prerequisites are missing, which is exactly why they are easy to miss.

- `ALEMBIC_TEST_POSTGRES_URL`: a postgres connection URL. enables the postgres state-store roundtrip tests in `alembic-engine`; unset, they are skipped.
- `ALEMBIC_TEST_POSTGRES_TLS_URL`: a TLS-enabled postgres URL. additionally enables the TLS roundtrip test; unset, it is skipped.
- `ALEMBIC_CAST_PYTHON`: python interpreter for the `cast django` e2e tests (default `python3`). those tests also skip when `django` + `djangorestframework` are not importable.

the rest of the suite is hermetic against ambient `ALEMBIC_STATE_*` variables: a stray `ALEMBIC_STATE_BACKEND=postgres` in your shell will not affect `cargo test`. those runtime variables are documented in [state.md](state.md).

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
