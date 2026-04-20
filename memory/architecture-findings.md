# Architecture Findings

Found 2026-04-20 after a codebase-wide pass. 129 tests pass, clippy clean.

## Code-level (cleaned)

- **Removed empty `[dev-dependencies]`** from `alembic-adapter-peeringdb/Cargo.toml` -- only crate with one.
- **Replaced `unimplemented!` with `panic!`** in `extract.rs:81` test mock -- less misleading.
- **Removed `version: 1`** from 6 YAML/MD files -- dead metadata the `Retort` struct ignores.

## Architectural

### 1. Engine crate is monolithic

`alembic-engine` has 12+ modules totaling 6500+ lines of source (not tests). Three files are the biggest:

| File | Lines | Concern |
|------|------:|---------|
| `retort.rs` | 1199 | YAML parsing + IR compilation |
| `django.rs` | 1069 | Django project scaffolding |
| `tests.rs` | 1089 | Engine integration tests |

`retort.rs` alone handles three orthogonal concerns: YAML parsing, variable interpolation, and UID generation. Consider splitting into `retort/parse.rs` and `retort/compile.rs`.


### 3. Adapter-registry pulls in everything

`alembic-adapter-registry` has transitive dependencies on all 5 adapters. A user who imports `alembic-adapter-registry` for a single netbox adapter gets pulled in:

```
alembic-adapter-registry
  -> alembic-adapter-netbox (netbox crate)
  -> alembic-adapter-nautobot (nautobot crate)
  -> alembic-adapter-infrahub (infrahub crate, infrahubctl binary dep)
  -> alembic-adapter-generic (reqwest + futures)
  -> alembic-adapter-peeringdb (peeringdb-rs)
```

If you want adapters to be independently usable (e.g., published as separate crates), the registry needs to be split or use a config-path approach.

### 4. Core mixes IR types and validation

`alembic-core` bundles two unrelated domains:

| Module | Lines | What |
|-------- | ----: |------|
| `ir.rs` | 1263 | IR types: Schema, Object, TypeSchema, FieldType, etc. |
| `validation.rs` | 970 | Schema validation logic |

These are orthogonal -- validation depends on IR types, but IR doesn't need validation. Consider splitting into `alembic-core` (IR types only) and `alembic-schema` (validation).

### 5. PeeringDB adapter is dead weight in the pipeline

`PeeringDBAdapter::write()` always returns `"read-only"` -- it never exercises the apply/retry/diff pipeline. It's a read-only observer, not a full adapter. The `Adapter` trait forces write support. Consider separating into `Observer` (read-only) and `Adapter` (read+write) traits.


