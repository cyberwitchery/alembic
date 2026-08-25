# inventory

an inventory is a data-model file: a `schema` block plus `objects`. alembic
consumes yaml or json (yaml recommended). this is the authored source of truth;
it is also what `import` writes when it observes a backend.

## inventory format

```yaml
include:
  - other.yaml
imports:
  - more.yaml
schema:
  types:
    dcim.site:
      key:
        slug: { type: slug }
      fields:
        name: { type: string }
        slug: { type: slug }
objects:
  - uid: "..."
    type: dcim.site
    key:
      slug: "fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"
```

- `schema` is required; `objects` is optional and defaults to empty.
- `include` and `imports` are optional and equivalent.
- a top-level key other than these four is a parse error, not a silent no-op.
- paths in `include/imports` are resolved relative to the current file.
- files are loaded once (deduplicated by canonical path).

## schema

inventory files must define schema metadata alongside objects.

```yaml
schema:
  types:
    services.vpn:
      key:
        vpn: { type: slug }
      fields:
        name: { type: string, required: true }
        peers: { type: list, item: { type: json } }
```

string fields can optionally use `format` and/or `pattern`:

```yaml
fields:
  slug: { type: string, format: slug }
  name: { type: string, pattern: "^[A-Z0-9-]+$" }
```

## scope

an optional top-level `scope:` block states what the inventory asserts
completeness over, per type. delete detection (`--allow-delete`) and the drift
report's `extra` category are defined inside it: only observed objects matching
an entry are candidates. types without an entry carry no completeness assertion
at all. without the block, the inventory is complete over every declared type,
the historical behavior.

```yaml
scope:
  dcim.site:
    slug: ["fra1", "fra2"]   # a value or list of values, matched on key fields
  dcim.device: {}            # every device
```

an entry constrains key fields by exact value; an empty entry covers the whole
type. a scope type must be declared in the schema, a scope field must be one of
that type's key fields, and the values must be ones the field could hold — a
`map` that renames a scoped type therefore fails its output validation instead
of silently orphaning the entry (`map` carries `scope:` through verbatim). a
ref-typed key field is constrained by the target's uid, which need not name an
object the inventory manages.

scope bounds delete authority and the `extra` category, not management: a
declared object outside the scope still converges as a create or update. this
lets two inventories share one backend — each declares its own objects, scopes
itself to them, and `--allow-delete` from either no longer plans the neighbor's
objects as deletes. across `include` files, disjoint scope types merge and a
type scoped twice is an error, the way schemas merge.

## json input

json is supported when the file extension is `.json`.

## guidelines

- `uid` is the object's identity, assigned once (see `docs/identity.md`).
- keep `key` human-readable; renaming a key is an ordinary update, since
  identity lives in the uid. key *design* still matters: mirror how the
  backend scopes uniqueness (an interface is `(device, name)`, not `name`).
- keys are canonicalized as JSON for matching and sorting.
- never include backend ids in `attrs`.
- `import` writes only schema-declared attrs; undeclared, server-computed fields (e.g. `last_updated`) are dropped with a warning.
