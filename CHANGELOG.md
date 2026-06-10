# changelog

## [unreleased]

- **breaking** — removed the raw-yaml→ir mapping engine and its surface (#41). the `distill` subcommand and the raw→ir compiler (formerly `retort`) are gone, and `import` is now backend-only: `import -f <inventory> -o <out>` observes a backend for the types in the inventory's schema. ir comes only from authored inventory files or backend observation; there is no "compile arbitrary yaml/csv into ir" path. the `brew` noun is retired for `inventory` (`docs/brew.md`→`docs/inventory.md`, `examples/brew.yaml`→`examples/inventory.yaml`). the command set is now `validate / import / map / plan / apply` (+ `cast`)

- engine: add `map`, an ir to ir transformation layer (#43): a rule selects source objects by a type-name pattern with optional field predicates (`dcim.device[attrs.role=leaf]`, prefix globs like `dcim.*`, bare `*`) and emits one or more target objects via `${...}` templates with transforms (`upper`/`lower`/`trim`/`slug`) over a fixed `${uid}` / `${type}` / `${key.*}` / `${attrs.*}` var namespace (nested attrs addressable by path). uids default to `uid_v5(target_type, target_key)`; `ref`/`list_ref` attrs are rewired from source to target uids automatically for 1:1 renames, and a multi-emit wires cross-object refs explicitly through named `uids` (`${uids.name}`). output is validated against the target schema; the transform is pure so maps chain
- cli: add `map` subcommand (`alembic map -f <ir> --spec <map> -o <ir>`) wiring the ir to ir transform (#43)
- engine: map rules gain `group_by` for N->1 aggregation (#43): the matched objects are bucketed by a rendered key and the rule emits once per group, with `${group.key}`, `${group.count}`, and per-member values collected into lists as `${group.items.<path>}` (e.g. `${group.items.key.vid}`); groups are keyed deterministically and members keep input order
- engine: map rules gain `lookups` (#43): a lookup renders a `ref` to a uid, finds that object in the input, and binds its `get` field path as `${lookup.name}` (e.g. `{ ref: "${attrs.status}", get: "attrs.name" }`), so an emit can read a value off a referenced object; strict on non-uuid refs, missing referents, and absent fields
- engine: apply now orders operations by a topological sort over references (creates/updates after the objects they reference, deletes in reverse) instead of relying on the retry fixpoint, giving deterministic O(V+E) ordering and correct delete ordering; reference cycles fall back to a stable order and the retry loop stays as a safety net (#47)
- cli: `plan --report` prints a read-only drift report (changed/missing/extra, with per-field diffs) and exits without writing a plan file or saving state
- cli: `plan --report` now surfaces the `extra` category (objects present on the backend but not declared in intent) without requiring `--allow-delete`; previously `extra` was silently always empty
- cli: `plan --report` and `--dry-run` are now mutually exclusive (passing both is rejected) instead of silently ignoring `--dry-run`
- engine: add `DriftReport` (built from a `&Plan`, with `Display` + `Serialize`) surfacing the desired-vs-observed diff as a one-way, read-only report
- cli: explain the file formats each subcommand reads and writes (#89). every argument now carries help text: `--file` inventories are YAML or JSON chosen by extension (`.json` => JSON, else YAML), `plan`/`map`/`import --output` are always JSON regardless of extension, and `apply --plan` is a JSON plan as produced by `alembic plan`. `--help` gains a long description documenting the IR format model in one place, and writing an always-JSON `--output` to a `.yaml`/`.yml` path now prints a non-fatal warning instead of silently producing JSON under a yaml name

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
