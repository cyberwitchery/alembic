# development

## workspace

```
alembic/
  crates/
    alembic-core
    alembic-adapter-sdk
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
- **generic rest**: spec-driven, read+write
- **peeringdb**: read-only (observer)
- **django**: write-only (emitter)
- **external adapters**: any executable speaking the json protocol over stdin/stdout (`docs/external-adapters.md`); `alembic-adapter-test` is the conformance runner
- **state**: per-backend identity memory, in a file under `.alembic/state/` or in postgres (`docs/state.md`)
- **journal**: apply journals completed writes, so an interrupted apply resumes instead of starting over (`docs/cli.md`)
- **map**: ir to ir transformation between vocabularies (`docs/map.md`)

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
- requires docker. it downloads infrahub's `docker-compose.yml` for `INFRAHUB_VERSION` to `/tmp` unless it is already there; `INFRAHUB_COMPOSE` points it at a compose file of your own.
- writes plans and imports under `/tmp` (see script output for paths).
- use `SKIP_DOCKER=1` to reuse running containers, or `CLEANUP=1` to tear them down afterward.
- afterwards it runs the docs' netbox blocks (below), emptying netbox before each with `scripts/netbox_reset.py`; `SKIP_DOCKER=1` skips them.

## documented examples

a shell block in the docs runs on ci when the line before its fence marks it:
`<!-- ci: no-backend -->` (every push) or `<!-- ci: netbox -->` (the e2e run).
`<!-- ci: skip <reason> -->` says why one does not; every block under
`docs/examples/` and `docs/case-studies/` carries one of the three.

```bash
ALEMBIC=target/debug/alembic ./scripts/doc_examples.sh no-backend
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

- runs the same steps as ci: the release publish list and changelog fragment checks, fmt, clippy, cargo-deny, docs, one instrumented test and coverage pass, the agent fixtures, and the no-backend documented examples
- the ci workflow also checks the unsafe budget against `unsafe-budget.lock` and runs the shared sbom workflow in report mode; `ci.sh` runs neither
- the e2e workflow runs `scripts/e2e_netbox_infrahub.sh` weekly, on demand, and on a pull request that changes what it runs; miri runs `alembic-core`'s tests weekly and on demand
- needs `cargo-deny` and `cargo-llvm-cov` on the path, and django + djangorestframework + django-filter + drf-spectacular importable from `ALEMBIC_DJANGO_PYTHON`, which defaults to `python3`; exits with an install hint without any of them
- the postgres state store tests no-op unless `ALEMBIC_TEST_POSTGRES_URL` is set; ci runs them against a service
- local mock servers require binding to loopback; some environments may need elevated privileges

## release

- changelog entries wait in `changelog.d/`, one file each. run
  `scripts/changelog.sh release <version>` and commit the result before
  tagging: it moves them into `CHANGELOG.md`, which the publish workflow reads
  the release notes from.
- tag a release with `v*` to trigger the publish workflow.
- ensure `CARGO_REGISTRY_TOKEN` is set in repository secrets.
