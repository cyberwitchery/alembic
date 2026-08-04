# ir

alembic lets you define your own vendor-neutral data model for dcim/ipam data. all objects share a common envelope and are typed by an explicit `type` string. types are user-defined and must be described with a schema.

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
- `type` may also be spelled `kind`; no other key is accepted, so a typo'd `attrs` is a parse error rather than an object that silently has none.

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

a type block takes `key` and `fields` only; anything else is a parse error.

supported field types include scalar types (string, text, int, float, bool, uuid, date, datetime, time), network types (ip_address, cidr, prefix, mac, slug), structured types (list, map, json, enum), and typed references (`ref`, `list_ref`).

### field schema

a field declares `type` plus optional metadata:

- `required`: the field must be present.
- `nullable`: a null value is accepted; without it a null is an error.
- `format`, `pattern`: string constraints.
- `description`: passed through when an adapter provisions the field (netbox and nautobot custom fields, infrahub attributes and relationships).

```yaml
fields:
  slug: { type: string, format: slug }
  hostname: { type: string, pattern: "^[a-z0-9-]+$" }
  comment: { type: text, nullable: true, description: "operator note" }
```

`format` supports: `slug`, `ip_address`, `cidr`, `prefix`, `mac`, `uuid`.

a `date`, `datetime` or `time` value is rfc 3339: `2026-08-01`, `22:00:00` (fractional seconds optional), and the two joined by `t` with an optional `z` or `+HH:MM` offset (`2026-08-01T22:00:00Z`). lowercase `t` and `z` are accepted, and the offset is optional, so a naive `2026-08-01T22:00:00` is valid. the calendar is checked too, not only the shape, so `2026-02-30` and `2026-13-01` are errors.

`nullable` also shapes provisioned schema: the django adapter emits `null=True`. it does the same for any non-text field that is not `required`, since a django column without `null=True` is NOT NULL whatever the form layer says. optional text fields hold the empty string instead, and optional `json`/`list`/`map` fields get `default=dict`/`default=list`.

composite types take a further mandatory key alongside `type`: `list` takes `item`, `map` takes `value`, `enum` takes `values`, `ref` and `list_ref` take `target`.

```yaml
fields:
  peers: { type: list, item: { type: json } }
  labels: { type: map, value: { type: string } }
  status: { type: enum, values: [active, planned, staged] }
  site: { type: ref, target: dcim.site }
```

a field declaration takes those metadata keys plus the one param its type requires; anything else is a parse error, so a typo'd `required` or `pattern` is reported rather than dropping the constraint it declares.

list and map elements are validated as required and non-nullable, so a null inside a list is rejected whatever the field declares.

### key field rules

a key field feeds uid derivation (see `docs/map.md`, uid), which constrains its declaration:

- it may not be `nullable`: a null has no identity form.
- it may not be a composite type (`list`, `list_ref`, `map`): there is no scalar identity form.
- `required` is not consulted; key fields are mandatory.

the first two are schema-load errors, raised before any object is read.

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
