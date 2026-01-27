# ir

alembic defines a canonical, vendor-neutral ir for dcim/ipam data. all objects share a common envelope and are typed by an explicit `type` string. types are user-defined and must be described with a schema.

## object envelope

every object is represented as:

```yaml
uid: "<uuid>"
type: "<type name>"
key:
  <field>: <value>
attrs: { ... }
```

- `uid`: stable identifier (uuid). never use backend ids in input files.
- `type`: canonical type id for the object (any string).
- `key`: structured key used for matching when no state mapping exists.
- `attrs`: payload for the object. alembic validates structure and references against the schema.

## schema

schemas define key fields, field types, and reference targets so the engine can validate payloads and relationships.

```yaml
schema:
  types:
    dcim.site:
      key:
        slug: { type: slug }
      fields:
        name: { type: string, required: true }
        slug: { type: slug, required: true }
    dcim.device:
      key:
        name: { type: slug }
      fields:
        name: { type: string, required: true }
        site: { type: ref, target: dcim.site, required: true }
        role: { type: string }
        device_type: { type: string }
```

supported field types include primitives (string, int, float, bool, uuid), structured types (list, map, json), and typed references (`ref`, `list_ref`).

### format constraints

string fields can optionally declare `format` and/or `pattern` constraints:

```yaml
fields:
  slug: { type: string, format: slug }
  hostname: { type: string, pattern: "^[a-z0-9-]+$" }
```

`format` supports: `slug`, `ip_address`, `cidr`, `prefix`, `mac`, `uuid`.

## relationships

references are expressed by uid strings in `attrs` and validated when the schema declares a `ref` or `list_ref` target.

```yaml
objects:
  - uid: "00000000-0000-0000-0000-000000000001"
    type: dcim.site
    key:
      slug: "fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"
  - uid: "00000000-0000-0000-0000-000000000002"
    type: dcim.device
    key:
      name: "leaf01"
    attrs:
      name: "leaf01"
      site: "00000000-0000-0000-0000-000000000001"
```

## matching semantics

- primary match: state store mapping (`uid` -> backend id)
- fallback match: `key`
- keys are canonicalized as stable JSON (sorted map) for matching and sorting; any characters are safe in values
