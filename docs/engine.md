# engine

the engine is responsible for loading, validating, planning, and applying changes. it is pure/testable and delegates io to adapters.

## pipeline

1) load brew files (supports `include` / `imports`) or compile raw yaml with a retort
2) validate object envelopes, keys, and schema references
3) apply projection spec (optional) to build backend payloads from `attrs`
4) observe backend state via adapter (default scope: projected/desired types; capabilities included)
5) bootstrap state mappings by key when missing
6) plan deterministic operations
7) apply operations in dependency order
8) optionally extract canonical inventory from backend state

## validation

validation ensures:

- `uid` is unique
- `type` is present
- `key` is unique per type
- references are resolvable by `uid` when declared in the schema

validation errors are aggregated and returned as a single failure.

## planning

the planner diffs desired ir against observed state and emits:

- `create` ops when the object is missing
- `update` ops when attrs differ
- `delete` ops for observed objects not in desired (gated by `--allow-delete`)

plans are stable-sorted by type name and key (canonical JSON of the key map).

## apply ordering

apply uses a dependency-aware ordering:

- creates/updates in type order
- deletes in reverse type order
- unresolved create/update refs are retried until convergence or explicit unresolved-ref failure

## diff rules

diffs are computed at the `attrs` field level plus projected fields (`custom_fields`, `tags`, optional `local_context`). projection source keys are ignored for diffing.

diffing is **additive-only**: only fields declared in your desired inventory are compared. fields present on the backend but absent from your inventory are left untouched. this means:

- alembic only manages what you declare — unmanaged fields are not cleared.
- to stop managing a field, set it to `null` in your `attrs` (sends a null patch to the backend). simply removing it from your inventory has no effect on the backend.
- this is intentional: you can layer alembic alongside manual backend edits on fields you do not declare.

## extract

extraction reads backend state via the adapter and emits a canonical inventory:

- `uid` is re-derived as `uid_v5(type, key)` to keep identities stable
- `attrs` are pulled from observed records
- projection inversion can add additional `attrs` keys when provided

projection inversion is best-effort:

- `strip_prefix` and explicit maps are inverted directly
- `direct` uses the rule key, map, or prefix when available
- transforms are not inverted; the engine emits a warning when they are present
- unmapped custom fields and tags are preserved in `attrs` with their backend names
