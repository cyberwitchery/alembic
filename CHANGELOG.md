# changelog

## [unreleased]

- engine: retort selectors now support value-filtered predicates (`/devices[role=leaf]`): `=`/`!=` operators, chained `[a=x][b=y]` predicates (logical AND), and key/wildcard/index/bare predicate forms; on a sequence a predicate filters elements, on a mapping it guards the node. segment splitting is bracket-aware so predicate values may contain `/` (e.g. cidrs like `10.0.0.0/24`) (#53)
- engine: retort import-mapping templates now support `${var|transform}` transforms (`upper`, `lower`, `trim`, chainable left-to-right) and coerce non-string scalars (numbers, bools) to their natural string form; nulls, arrays/objects, and unknown transform names remain errors (#54)
- engine: apply now orders operations by a topological sort over references (creates/updates after the objects they reference, deletes in reverse) instead of relying on the retry fixpoint, giving deterministic O(V+E) ordering and correct delete ordering; reference cycles fall back to a stable order and the retry loop stays as a safety net (#47)
- cli: `plan --report` prints a read-only drift report (changed/missing/extra, with per-field diffs) and exits without writing a plan file or saving state
- cli: `plan --report` now surfaces the `extra` category (objects present on the backend but not declared in intent) without requiring `--allow-delete`; previously `extra` was silently always empty
- cli: `plan --report` and `--dry-run` are now mutually exclusive (passing both is rejected) instead of silently ignoring `--dry-run`
- engine: add `DriftReport` (built from a `&Plan`, with `Display` + `Serialize`) surfacing the desired-vs-observed diff as a one-way, read-only report

## [0.3.0] - 2026-05-20

- cli: improved support for running external adapters ("plugins")
- cli: can pass config variables to external adapters

## [0.2.0] - 2026-04-27

- cli: print provisioning summary (fields, tags, object types created/deprecated/deleted) when `--provision` is used or during `apply`
- engine: extract django codegen into a standalone `alembic-django` crate (cli now depends on it directly)

## [0.1.2] - 2026-03-06

- improve idempotency for netbox/nautobot create + provisioning (handle already-exists conflicts)
- infrahub: schema provisioning now cleans up stale menu entries, avoids inferred icons, and uses deterministic anchors
- engine: run schema provisioning on apply even when there are no data ops

## [0.1.1] - 2026-03-06

- add external (stdout) adapter support for process-based backends and sdk helpers

## [0.1.0] - 2026-03-06

- initial version
