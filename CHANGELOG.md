# changelog

## Unreleased

- engine: compare attrs by declared field type when diffing, so an `int`/`float` field whose backend representation differs from the inventory's (int vs float, or a numeric string) no longer plans a perpetual no-op update; `list`/`map` fields compare elementwise under their item/value type, every other type compares exactly, and an undeclared field falls back to raw comparison
- engine: gate destructive schema provisioning behind `--allow-delete`. `apply` and `plan --provision` previously deleted alembic-owned custom object types/fields the inventory no longer declared with no gate, even though object deletes required the flag; deleting a custom object type cascades to its objects on the backend. both paths now refuse those deletions (via the read-only schema preview) unless `--allow-delete` is given, matching the object-delete gate
- engine: index inventory uid source-lines in a single pass instead of rescanning the whole file per object, so loading is linear in file size; a 50k-object inventory that previously could not `validate` in ten minutes now loads in under a second. also resolves a uid to its own line by exact value rather than substring, so a uid that is a prefix of another no longer points at the wrong line
- engine: journal hashes no longer depend on the rust version, so a partially-applied plan can be resumed by a different build; a journal from an older version is orphaned once
- conformance runner: accept a `preview_schema` response with a null result (`ok=true`, no result, no error) as the canonical "cannot preview schema" signal, since the host's `call_optional` maps it to `Ok(None)`; a null result on read/write/ensure_schema stays rejected because those dispatch through `call`, which hard-errors on it, so the runner no longer fails a correct adapter the real registry accepts
- generic adapter: `plan`'s schema preview reports nothing to provision (`schema_preview: {}`, no stderr line) instead of `schema preview: unavailable for this backend`, since the generic adapter provisions no schema (its `ensure_schema` is the no-op default), matching the preview==ensure invariant netbox/nautobot/infrahub already hold
- netbox + nautobot adapters: remove the adapter-side `type`/`if_type` aliasing entirely, so the adapter reads and writes the backend's literal `type` field for every type, interfaces included; previously the read renamed a backend `type` to `if_type`, the write remapped a `dcim.interface` `if_type` back to `type`, and provisioning treated `if_type` as a native interface field. reshaping a field like this belongs in an `alembic map` spec, not baked into the adapter on a hardcoded `dcim.interface`, so a user who wants `if_type` in their ir renames `type` to `if_type` in map; reverts the aliasing added in #194/#195
- infrahub adapter: resolve a desired key's `ref`/`list_ref` fields to backend ids before matching it against an existing object, so a ref-keyed type (interfaces, cables, ip assignments) finds its existing object instead of never matching; most severely, a genuinely failing delete of such an object is no longer swallowed and reported as success
- netbox adapter: `plan`'s schema preview rejects an invalid custom-object field name (outside `[A-Za-z0-9_]`) at plan time, matching what `apply` already enforces, instead of reporting a field creation `apply` then refuses
- netbox adapter: classify custom fields by the django content-type form (`app_label.model`, e.g. `ipam.ipaddress`) everywhere, matching how netbox returns and keys them, instead of the endpoint-underscore form (`ipam.ip_address`). previously a custom field on any multi-word model (`ipam.ip_address`, `dcim.device_type`, `dcim.mac_address`, …) was misclassified: `apply` emitted it as a top-level field netbox silently drops, so the field never converged (every plan re-updated it), and `ensure_schema` posted an invalid content type and errored. single-word models were unaffected, so it was invisible until now
- nautobot adapter: create a custom field with `key` (the nautobot 2.x writable identifier), not `name` (which the api has no field for and derives `key` from by slugifying `label`), so a non-slug field name like `assetTag` keeps its identity instead of becoming `assettag` — which the read/detect/write paths then never matched, silently dropping the field's value on write and erroring on the next `ensure_schema`
- generic adapter: clearing a nullable ref (setting it to `null`) no longer aborts the write with `ref value must be a uuid string`; the write body-builder now delegates to the shared engine `build_request_body`, which passes a null straight through to clear the field, instead of re-implementing the loop without that guard

## [0.6.0] - 2026-07-05

- infrahub adapter: write a null-valued attribute under the same `{ "value": … }` wrapper as every non-null attribute and the read path, instead of a bare `null`, so clearing a nullable field sends the attribute-input shape rather than an unwrapped value; a null `ref`/`list_ref` still goes out bare to clear the relationship
- nautobot adapter: write the `dcim.interface` type field back under the api name `type` (not `if_type`) and treat `if_type` as a native field, so an interface create/update sets the real type instead of failing validation or provisioning a spurious `if_type` custom field
- core: format-typed fields (`cidr`, `prefix`, `mac`, `slug`) now validate their value's format instead of silently accepting any string, matching how `uuid` already validated and the `format:` constraint
- core: validate that schema `ref`/`list_ref` targets name a declared type, including refs nested in `list`/`map` fields, so a misspelled target fails schema validation with one clear error instead of confusing per-object reference errors
- core: validate that schema `pattern:` regexes compile at schema-load time, instead of only failing per-object (and never for types with no objects yet)
- core: reject a `format:`/`pattern:` constraint on a non-string field type (`int`, `float`, `bool`, `list`, `map`, `list_ref`) at schema-load time, instead of only failing per-object with a misleading `expected string` error (and never for types with no objects)
- core: reject an empty `enum` field (`values: []`), including enums nested in `list`/`map` fields, at schema-load time, instead of only failing per-object with a confusing `expected: enum()` error (and never for types with no objects)
- engine: map rewrites refs nested in `list` and `map` fields across a rename, not just top-level `ref`/`list_ref` attrs
- engine: drop a present-but-null member value from a `${group.items.<path>}` aggregation list, matching the documented non-missing-values-only contract
- engine: a lone map placeholder whose transform argument contains a quoted `${` keeps its transformed type in `attrs`, instead of being coerced to a string
- generic adapter: resolve refs nested in `list` and `map` fields on read, matching the write path and the netbox/nautobot adapters
- infrahub adapter: resolve refs nested in `list` and `map` fields on read, matching the generic/netbox/nautobot adapters and the write path, by sharing the engine's read-side reference normalizer
- core: attribute reference-type-mismatch errors to the referencing object, not the referent
- engine: locate an object at its own `uid:` line, not an earlier line that references its uid, so validation errors point at the right object
- engine: import errors on a malformed tag item instead of silently dropping it
- engine: parse a `u64`-range transform argument literal (above `i64::MAX`) as an exact integer instead of a lossy float, matching the value path
- core: canonicalize a `u64`-range key/uid component (above `i64::MAX`) as an exact integer instead of a lossy float, so two distinct large-integer keys no longer collapse onto one uid
- netbox adapter: singularize `x`/`z`/`ch`/`sh`-stemmed endpoints correctly (`ipam/prefixes/` → `ipam.prefix`, not `ipam.prefixe`), so reading prefixes and decoding refs to them yield the declared schema type
- netbox adapter: singularize `s`-stemmed endpoints correctly (`extras/statuses/` → `extras.status`, not `extras.statu`), so reading and decoding refs to a custom object type whose singular ends in `s` yield the declared schema type
- netbox + nautobot adapters: pluralize vowel + `y` endpoints correctly (`dcim.device_bay` → `dcim/device-bays/`, not `dcim/device-baies/`), so reading and writing those types hit the right REST endpoint
- django adapter: pluralize generated drf router endpoints correctly (`prefix` → `prefixes`, not `prefixs`; `gateway` → `gateways`, not `gatewaies`) by sharing one `pluralize` helper across the netbox, nautobot, and django adapters instead of a stale third copy
- core: reject a wrong-typed `required`/`nullable`/`format`/`pattern` in a field schema instead of silently coercing it to the permissive default, so a malformed schema fails to load rather than dropping the constraint
- add alembic-adapter-test, a standalone conformance runner for external adapter executables
- security: bump anyhow 1.0.102 to 1.0.103 to clear RUSTSEC-2026-0190 (unsoundness in Error::downcast_mut); not reachable in our usage
- netbox adapter: paginate the custom-field fetch, so an instance with more than one page of custom fields routes and provisions all of them instead of silently dropping the rest past the first page
- external adapters: surface an adapter's full error cause chain on failure, and forward its stderr to the host on success, instead of dropping those diagnostics
- cli: create the parent directory when writing a plan or ir output, instead of failing if it does not exist
- cli: reject `plan --provision` together with `--dry-run` at parse time, so a `--dry-run` preview (which writes nothing) can't silently provision backend schema via `ensure_schema`
- cli: `plan --report` and `plan --dry-run` no longer require `-o`/`--output`, since neither writes a plan file (`-o` is still accepted for backward compatibility)
- engine: `ProvisionReport` accepts an omitted `created_fields`/`created_tags` on deserialize, so an external `ensure_schema` adapter that creates neither isn't rejected
- engine + cli: `plan` now previews the schema provisioning `apply` would perform (object types and fields to create/deprecate/delete) read-only, writing nothing, and carries it in the plan json as `schema_preview` (visible in `plan --dry-run`), so the see-before-write contract finally covers schema and not just data ops; netbox, nautobot, and infrahub all report it (netbox's preview shares its `ensure_schema` create/delete decisions, so it surfaces custom-object type/field deletions too), a backend that cannot preview prints `schema preview: unavailable for this backend`, and `plan --provision` stays the write escape hatch
- external adapter protocol: add a read-only `preview_schema` request alongside `ensure_schema`; an adapter can now surface what provisioning would do at plan time, and the default returns nothing to preview so existing adapters are unaffected

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
