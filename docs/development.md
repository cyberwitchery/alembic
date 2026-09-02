# development

## workspace

```
alembic/
  crates/
    alembic-core
    alembic-engine
    alembic-adapter-registry
    alembic-adapter-netbox
    alembic-adapter-nautobot
    alembic-adapter-infrahub
    alembic-adapter-generic
    alembic-adapter-peeringdb
    alembic-adapter-django
    alembic-adapter-test
    alembic-file-generator
    alembic-cli
```

## project status

- **netbox**: stable
- **nautobot**: stable
- **infrahub**: graphql (adapter), optional schema push
- **generic rest**: initial release (spec-driven)
- **peeringdb**: read-only (observer)
- **django**: write-only (emitter)

backends implement one of three capability traits in `alembic-engine`:
`Observer` (read-only), `Emitter` (write-only, with optional schema
provisioning), or `Adapter: Observer + Emitter` (read+write). commands that need
a missing capability fail with a clear error (e.g. `apply` against peeringdb).

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

## adapter contract

the built-in adapters follow the same value-space contract external adapters are
held to (`docs/external-adapters.md`): backend constraints an adapter provisions
are at most as strict as core validation, values a read answers land back in
alembic's value space, and refs are in uid space. `format_regex` in core is the
written-down half of the first clause; a new adapter follows all three.

## e2e

the netbox + infrahub end-to-end script provisions schema, applies objects, and imports them back to ir.

```bash
./scripts/e2e_netbox_infrahub.sh
```

notes:
- requires docker and a sibling checkout of `../infrahub.rs` (uses its `docker-compose.yml`).
- writes plans and imports under `/tmp` (see script output for paths).
- use `SKIP_DOCKER=1` to reuse running containers, or `CLEANUP=1` to tear them down afterward.

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

- runs the same steps as ci: fmt, clippy, cargo-deny, docs, one instrumented test and coverage pass, and the agent fixtures
- needs `cargo-deny` and `cargo-llvm-cov` on the path, and django + djangorestframework + django-filter + drf-spectacular importable from `ALEMBIC_DJANGO_PYTHON`, which defaults to `python3`; exits with an install hint without any of them
- the postgres state store tests no-op unless `ALEMBIC_TEST_POSTGRES_URL` is set; ci runs them against a service
- local mock servers require binding to loopback; some environments may need elevated privileges

## release

- tag a release with `v*` to trigger the publish workflow.
- ensure `CARGO_REGISTRY_TOKEN` is set in repository secrets.
