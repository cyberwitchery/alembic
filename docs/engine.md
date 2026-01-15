# engine

the engine is responsible for loading, validating, planning, and applying changes. it is pure/testable and delegates io to adapters.

## pipeline

1) load brew files (supports `include` / `imports`) or compile raw yaml with a retort
2) build object graph and validate references
3) observe backend state via adapter
4) plan deterministic operations
5) apply operations in dependency order

## validation

validation ensures:

- `uid` is unique
- `key` is unique
- `kind` is present
- references are resolvable by `uid`
- referenced kinds match expected kinds

validation errors are aggregated and returned as a single failure.

## planning

the planner diffs desired ir against observed state and emits:

- `create` ops when the object is missing
- `update` ops when attrs differ
- `delete` ops for observed objects not in desired (gated by `--allow-delete`)

plans are stable-sorted by kind and key:

1) `dcim.site`
2) `dcim.device`
3) `dcim.interface`
4) `ipam.prefix`
5) `ipam.ip_address`
6) custom kinds (sorted by kind string)

## apply ordering

apply uses a dependency-aware ordering:

- creates/updates in kind order
- deletes in reverse kind order

## diff rules

diffs are computed at the `attrs` field level (json value comparison). generic attrs are compared as a single payload. `x` is currently ignored by the planner.
