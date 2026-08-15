# engine

the engine is responsible for loading, validating, planning, and applying changes. it is pure/testable and delegates io to adapters.

## pipeline

1) load inventory files (supports `include` / `imports`)
2) validate object envelopes, keys, and schema references
3) observe backend state via adapter (default scope: desired + schema types)
4) bootstrap state mappings by key when missing, re-observing while it learns one
5) plan deterministic operations
6) provision schema primitives on apply (custom fields/custom objects where supported)
7) apply operations in dependency order
8) optionally import canonical inventory from backend state

the bootstrap re-reads because adapters resolve ref-typed fields through state: an object keyed on a ref only reads back in uid space once that ref is mapped.

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

diffs are computed at the `attrs` field level.

diffing is **additive-only**: only fields declared in your desired inventory are compared. fields present on the backend but absent from your inventory are left untouched. this means:

- alembic only manages what you declare; unmanaged fields are not cleared.
- to stop managing a field, set it to `null` in your `attrs` (sends a null patch to the backend). simply removing it from your inventory has no effect on the backend.
- this is intentional: you can layer alembic alongside manual backend edits on fields you do not declare.

comparison is type-aware: for a field declared `int` or `float`, values are compared by numeric value (and `list`/`map` fields elementwise), so a backend that returns `1.0` or `"1"` for an int you wrote as `1` does not produce a perpetual update. every other type compares exactly.

## import

import reads backend state via the adapter and emits a canonical inventory:

- `uid` is re-derived as `uid_v5(type, key)` to keep identities stable
- import observes in the canonical uid space: it ignores the state store, so refs come back as canonical uids rather than the state-mapped ones `plan` observes. a ref the adapter can only report as a backend id is resolved against a `backend id -> canonical uid` index built from the observation itself, to a fixpoint since a key field can itself be a ref
- **import validates the inventory before writing it**, as `map` validates what it builds: every consumer validates on load, so a file that does not validate has no use. a ref that came out of the index still holding a backend id fails the import naming the cause rather than the symptom -- `no b.interface with that backend id was observed`, not `expected uuid, got number` -- and says which of the three causes it is: the target was not in the observation, the target is keyed on a reference cycle so no uid can be derived for it, or the target has a uid and the adapter reported this key field only in `key` and not in `attrs`, where only `attrs` is normalized. everything else the inventory gets wrong is reported by `validate` in its own words
- `attrs` are pulled from observed records (including backend custom fields/tags where supported)
- observed attrs are **projected onto the schema**: any attr whose key is not declared in the type's `fields` is dropped, with a `warn` log naming `<type>.<field>`. server-computed fields (e.g. `last_updated`) are not in the schema and could never be managed, so keeping them would only make the imported inventory fail validation. a type absent from the schema keeps its attrs untouched and then fails the import, naming the types import asked for: the schema you import against is also that list, so a type outside it came back from the adapter unasked rather than being missing from your `-f` (unless the schema declares no types, when the `-f` is the cause and the message says so).

import projection and the **additive-only** diff rules above are complementary: import never carries an undeclared field into your inventory, and planning only converges the fields you declare; an attr present on the backend but absent from your inventory is left untouched.
