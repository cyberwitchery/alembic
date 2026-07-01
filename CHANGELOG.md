# changelog

## [unreleased]

- core: format-typed fields (`cidr`, `prefix`, `mac`, `slug`) now validate their value's format instead of silently accepting any string, matching how `uuid` already validated and the `format:` constraint
- core: validate that schema `ref`/`list_ref` targets name a declared type, including refs nested in `list`/`map` fields, so a misspelled target fails schema validation with one clear error instead of confusing per-object reference errors
- core: validate that schema `pattern:` regexes compile at schema-load time, instead of only failing per-object (and never for types with no objects yet)
- core: reject a `format:`/`pattern:` constraint on a non-string field type (`int`, `float`, `bool`, `list`, `map`, `list_ref`) at schema-load time, instead of only failing per-object with a misleading `expected string` error (and never for types with no objects)
- engine: map rewrites refs nested in `list` and `map` fields across a rename, not just top-level `ref`/`list_ref` attrs
- generic adapter: resolve refs nested in `list` and `map` fields on read, matching the write path and the netbox/nautobot adapters
- infrahub adapter: resolve refs nested in `list` and `map` fields on read, matching the generic/netbox/nautobot adapters and the write path, by sharing the engine's read-side reference normalizer
- core: attribute reference-type-mismatch errors to the referencing object, not the referent
- engine: locate an object at its own `uid:` line, not an earlier line that references its uid, so validation errors point at the right object
- engine: import errors on a malformed tag item instead of silently dropping it
- engine: parse a `u64`-range transform argument literal (above `i64::MAX`) as an exact integer instead of a lossy float, matching the value path
- netbox adapter: singularize `x`/`z`/`ch`/`sh`-stemmed endpoints correctly (`ipam/prefixes/` → `ipam.prefix`, not `ipam.prefixe`), so reading prefixes and decoding refs to them yield the declared schema type
- netbox adapter: singularize `s`-stemmed endpoints correctly (`extras/statuses/` → `extras.status`, not `extras.statu`), so reading and decoding refs to a custom object type whose singular ends in `s` yield the declared schema type
- netbox + nautobot adapters: pluralize vowel + `y` endpoints correctly (`dcim.device_bay` → `dcim/device-bays/`, not `dcim/device-baies/`), so reading and writing those types hit the right REST endpoint
- django adapter: pluralize generated drf router endpoints correctly (`prefix` → `prefixes`, not `prefixs`; `gateway` → `gateways`, not `gatewaies`) by sharing one `pluralize` helper across the netbox, nautobot, and django adapters instead of a stale third copy
- core: reject a wrong-typed `required`/`nullable`/`format`/`pattern` in a field schema instead of silently coercing it to the permissive default, so a malformed schema fails to load rather than dropping the constraint
- add alembic-adapter-test, a standalone conformance runner for external adapter executables
- security: bump anyhow 1.0.102 to 1.0.103 to clear RUSTSEC-2026-0190 (unsoundness in Error::downcast_mut); not reachable in our usage
- netbox adapter: paginate the custom-field fetch, so an instance with more than one page of custom fields routes and provisions all of them instead of silently dropping the rest past the first page
- external adapters: surface an adapter's full error cause chain on failure, and forward its stderr to the host on success, instead of dropping those diagnostics
- cli: reject `plan --provision` together with `--dry-run` at parse time, so a `--dry-run` preview (which writes nothing) can't silently provision backend schema via `ensure_schema`

## [0.5.0] - 2026-06-19

- core: keep source locations on validation errors for dotted type names
- engine: reject lists, dicts, and null as key and uid components
- engine: don't suppress a delete when an undeclared object's backend id collides with a matched object of another type
- netbox adapter: handle NetBox generic foreign keys uniformly in both directions, driven by the metadata in `netbox` 0.6.0. previously the adapter hand-mapped a couple of them and silently dropped the rest on write — an ip's interface assignment, a prefix's scope, and so on — so `apply` could not establish those relationships; now it writes and `import` reads every generic FK, both the split `<field>_type`/`<field>_id` form and the nested `GenericObjectRequest` form. import also round-trips NetBox's composite `(device, name)` interface identity and resolves generic FKs statelessly. **breaking**: the IR field names are now NetBox-faithful — `assigned_object` / `scope` instead of the typed aliases `assigned_interface` / `site`, and a device's primary IP is the native `primary_ip4` rather than a unified `primary_ip`.

## [0.4.0] - 2026-06-18

- **breaking** removed the raw-yaml→ir mapping engine (#41). the `distill` subcommand and the raw→ir compiler (`retort`) are gone; ir now comes only from authored inventory files or backend observation. `import` is backend-only (`import -f <inventory> -o <out>` observes a backend for the inventory's schema types), the `brew` noun is renamed `inventory`, and the command set is `validate / import / map / plan / apply`
- **breaking** removed the `cast` command; django project generation is now a django adapter that runs as part of `write`
- engine + cli: add `map`, an ir→ir transformation layer, via `alembic map -f <ir> --spec <map> -o <ir>` (#43). rules select source objects by type-name pattern with optional field predicates (`dcim.device[attrs.role=leaf]`, globs like `dcim.*`, bare `*`) and emit one or more targets through `${...}` templates with `upper`/`lower`/`trim`/`slug` transforms over a `${uid}`/`${type}`/`${key.*}`/`${attrs.*}` namespace. uids default to `uid_v5(target_type, target_key)`, `ref`/`list_ref` attrs rewire from source to target uids (explicitly via `${uids.name}` for multi-emit), output is validated against the target schema, and the transform is pure so maps chain. rules also support `group_by` (n→1 aggregation, exposing `${group.key}`/`${group.count}`/`${group.items.<path>}`) and `lookups` (read a field off a referenced object as `${lookup.name}`)
- engine + cli: map transforms can be user-defined in [starlark](https://github.com/bazelbuild/starlark) (#99). a `transforms:` block (`file:` or `inline:`) exposes every top-level `def` to `${var|...}` pipelines (shadowing the built-in four), with literal args (`${x|f|g(2)}` is `g(f(x), 2)`). transformed values keep their starlark type in `attrs:` templates (str/int/bool/list/dict to json), while `key:` and other string contexts coerce scalars and reject collections. transforms are hermetic (no i/o, no `while`, no recursion; `load()` restricted to relative paths) and live behind a `starlark` cargo feature on `alembic-engine` (off by default, enabled in the cli). `alembic map transform --spec <map> <name> <json-value> [json-args...]` evaluates a single transform for iteration, printing the typed result as json (`fail()` exits non-zero)
- engine: apply orders operations by a topological sort over references (creates/updates after what they reference, deletes in reverse) instead of the retry fixpoint, giving deterministic O(V+E) ordering and correct delete ordering; cycles fall back to a stable order and the retry loop stays a safety net (#47)
- cli + engine: `plan --report` prints a read-only drift report (changed/missing/extra with per-field diffs) and exits without writing a plan or saving state, backed by a new `DriftReport` type (`Display` + `Serialize`). it surfaces the `extra` category (on-backend but undeclared objects) without `--allow-delete`, and rejects `--report` together with `--dry-run` instead of silently ignoring the latter
- engine: add a `journal` module and resumable-apply groundwork (#46, #114). `apply_non_delete_with_retries` can record applied ops (and their backend ids) to a side journal so a re-run resumes the remainder after a mid-apply error; the journal is flushed at the apply exit points (not per op) and deleted on success. opt-in per adapter and not yet wired into any built-in adapter. `ApplyReport` gains `previously_applied_count`, which the cli prints when a run resumed
- engine: `import` projects observed state onto the schema, dropping attrs whose key isn't declared in the type's `fields` (with a `warn` each), so server-computed fields like `dcim.cable.last_updated` no longer fail validation with `extra attr field` (#45)
- cli: document the file formats each subcommand reads and writes (#89). every argument carries help text (`--file` inventories are yaml or json by extension; `plan`/`map`/`import --output` are always json; `apply --plan` is a json plan), `--help` describes the ir format model, and writing json `--output` to a `.yaml`/`.yml` path warns instead of silently producing json under a yaml name
- generic adapter: resolve refs nested inside `list` and `map` fields, consolidating onto the shared `alembic-engine` resolution helpers (`build_key_from_schema`, `resolve_value_for_type`) instead of a divergent local copy that left them as raw uid strings, matching netbox and nautobot

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
