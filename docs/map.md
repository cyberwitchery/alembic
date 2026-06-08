# map

map is an ir to ir transformation layer. it takes an existing inventory and
re-emits it under a different vocabulary: renaming types and fields, dropping or
deriving values, and rewiring references. it is the migration-between-backends
story made declarative, e.g. `import` from netbox, `map` to an infrahub-shaped
model, then `apply` to infrahub.

a rule selects whole ir objects out of a flat list by their type (with optional
field predicates) and re-emits them, using `${...}` templates with transforms to
reshape keys and attrs.

## running

```bash
alembic map -f examples/map-input.yaml --spec examples/map.yaml -o ir.json
```

- `-f` is the input inventory (ordinary ir, as produced by `import`).
- `--spec` is the map specification.
- `-o` is the transformed ir.

`docs/case-studies/07-nautobot-to-netbox.md` walks through a full cross-backend
migration using these steps.

## shape

```yaml
schema:
  types:
    location.site:
      key:
        slug: { type: slug }
      fields:
        label: { type: string }
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key:
        slug: "${key.slug}"
      attrs:
        label: "${attrs.name|upper}"
```

- `schema` is the target schema. the output inventory is validated against it,
  including reference integrity, before it is written.
- each rule has a `match` selector and one or more `emit` blocks.

## match

`match` is a type-name pattern with optional predicates.

- an exact type name matches that type (`dcim.site`).
- a trailing `*` is a prefix glob (`dcim.*` matches every `dcim.` type); a bare
  `*` matches every type.
- predicates filter the matched objects using mapping's predicate syntax, e.g.
  `dcim.device[attrs.role=leaf]`. quote the selector in yaml when it contains
  brackets.

predicates address the same dotted namespace as templates (see vars below), so
`[attrs.role=leaf]` tests the object's `role` attr and `[key.slug=fra1]` tests
its key. the operators are `=`, `!=`, existence `[field]`, and absence
`[!field]`; `=`/`!=` compare a field's scalar rendering, while `[field]` is true
when the field is present and non-null. chained predicates are ANDed.

## vars

an emit's templates draw on a fixed set of vars derived from the matched source
object, with no `from` extraction step:

- `${uid}` and `${type}`: the source object's uid and type.
- `${key.<field>}`: a key field, e.g. `${key.slug}`.
- `${attrs.<field>}`: an attr field. nested attrs are addressable by path, so a
  source attr `model: { fabric: ... }` is reachable as `${attrs.model.fabric}`.

## templates

strings support `${var}` substitution and `${var|transform|...}` pipelines
(`upper`, `lower`, `trim`, `slug`, chained left-to-right). a lone `${var}`
preserves the value's type; embedded or transformed vars are coerced to text
(numbers and bools to their natural form), while nulls, arrays, and objects in a
template are an error. `slug` lowercases and collapses non-`[a-z0-9]` runs to a
single `-`.

## uid

a uid is a derived projection of `(type, key)`, not a stored identity. by default
an emit's uid is `uid_v5(target_type, target_key)`, recomputed from the target
vocabulary. this is correct for cross-backend work: the target system has its own
identity, and the uid is canonical to the target model.

an emit may override its uid, reusing mapping's forms:

```yaml
uid:
  v5:
    type: "location.site"
    stable: "slug=${key.slug}"
```

or a uuid-string template, `uid: "${attrs.external_id}"`.

## references

references are by uid. when a rule renames a type or reshapes a key, the
referent's uid changes, so map rewrites the references that point at it.

for a 1:1 rule (one emit per matched source), this is automatic: map records the
source-to-target uid for every renamed object and rewrites `ref` / `list_ref`
attrs through that map in a second pass. the `infra.device.location` ref in
`examples/map.yaml` is rewired from the source `dcim.site` uid to the new
`location.site` uid this way, with no extra wiring.

## lookups

a `ref`/`list_ref` attr holds the uid of another object. a lookup follows that
ref and reads a field off the referenced object, so an emit can pull in a value
from a related object rather than just the matched one.

```yaml
rules:
  - name: devices
    match: "dcim.device"
    lookups:
      status_label:
        ref: "${attrs.status}"   # a uid, here the device's status ref
        get: "attrs.label"       # field path on the referenced object
    emit:
      type: dcim.device
      key:
        name: "${key.name}"
      attrs:
        status: "${lookup.status_label}"
```

each lookup binds `${lookup.<name>}` (mirroring `${uids.<name>}`), usable in
`uids`, keys, and attrs.
lookups are strict: a `ref` that is not a uuid, a uid not present in the input, or
a missing `get` field is an error. lookups resolve against the input inventory, so
the referenced object does not need to be emitted by any rule. this is how a
reference-valued field (a status object) becomes a plain value (its label) in the
target.

## aggregation

a rule with `group_by` buckets its matched objects by the rendered key and emits
once per group (N to 1) instead of once per object. emits then draw on:

- `${group.key}`: the rendered group key.
- `${group.count}`: the number of members.
- `${group.items.<path>}`: each member's value at `<path>`, collected into a list
  in member order (present, non-missing values only). paths use the per-object
  namespace, so `${group.items.key.vid}` and `${group.items.attrs.name}` work.

```yaml
rules:
  - name: vrfs
    match: "ipam.vlan"
    group_by: "${attrs.vrf}"
    emit:
      type: ipam.vrf
      key:
        name: "${group.key}"
      attrs:
        vlans: "${group.items.key.vid}"
```

grouping `ipam.vlan` objects with `vrf: blue` / `vrf: red` yields one `ipam.vrf`
per vrf, each carrying the list of its members' vids. groups are keyed
deterministically and members keep input order. aggregation is N to 1, so it
never auto-rewires refs; wire any cross-object references through named `uids`.

## multi-emit

a rule may emit a list of objects, fanning one source out into several. because
auto-rewiring is ambiguous when a source becomes many objects, cross-object
references in a multi-emit are wired explicitly through named `uids`: uids
declared once per matched source and referenced as `${uids.name}`.

```yaml
rules:
  - name: fabric
    match: "net.fabric"
    uids:
      site:
        v5:
          type: "location.site"
          stable: "slug=${attrs.site}"
    emit:
      - type: location.site
        key:
          slug: "${attrs.site}"
        uid: "${uids.site}"
      - type: net.vrf
        key:
          name: "${attrs.vrf}"
        attrs:
          site: "${uids.site}"
```

## composability

map is a pure ir to ir transform, so maps chain: the output of one `map` is a
valid input to the next. each stage validates against its own target schema, so a
chain is a sequence of independently checked translations.

## determinism

- the compiler sorts objects by type name and key (canonical json of the key map).
- the same input and spec yield the same ir and plan order.
- default uids are deterministic functions of the target identity, so identity is
  stable across runs without a state store.
