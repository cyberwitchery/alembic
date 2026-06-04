# retort

retort is a small mapping layer that compiles raw yaml into the canonical ir. it is declarative, yaml-only, and deterministic.

## shape

```yaml
schema:
  types:
    dcim.site:
      key:
        site: { type: slug }
      fields:
        name: { type: string }
        slug: { type: slug }
rules:
  - name: sites
    select: /sites/*
    emit:
      type: dcim.site
      key:
        site: "${slug}"
      uid:
        v5:
          type: "dcim.site"
          stable: "site=${slug}"
      vars:
        slug: { from: .slug, required: true }
        name: { from: .name, required: true }
      attrs:
        name: ${name}
        slug: ${slug}
```

## selection

- `select` uses a yaml pointer with `*` wildcards.
- `/sites/*/devices/*` walks maps and arrays in order.
- wildcards preserve input order for deterministic compilation.

### predicates

- a path segment may carry one or more trailing predicates to filter by a field's value or presence, e.g. `/devices[role=leaf]` or `/devices[primary_ip]`.
- the base may be a key (`devices[role=leaf]`), a wildcard (`*[role=leaf]`), an array index, or absent (a bare `[role=leaf]`).
- value operators are `=` (equals) and `!=` (not-equals). `field` is a single mapping key (no nested paths); `value` is the literal text up to the closing `]`, so values may contain `/` (e.g. a cidr `[prefix=10.0.0.0/24]`).
- applied to a sequence a predicate keeps each element that satisfies it (a filtered wildcard); applied to a mapping it is a guard that keeps the node only when it satisfies the predicate.
- a node satisfies `field=value` when it is a mapping whose `field` is a scalar (string, number, or bool) rendered as text equal to `value`. numbers and bools use their natural form (`42`, `true`); null, sequence, and mapping field values never match.
- `field!=value` requires the field to be present and scalar with a differing rendering — a missing or non-scalar field does not satisfy `!=`.
- a predicate body with no `=` is an existence test. `[field]` keeps a node when it is a mapping whose `field` is present and non-null, for *any* value type (sequences, mappings, empty strings, `false`, and `0` all count); this is presence, not the scalar-only comparison that `=`/`!=` perform, e.g. `/devices[primary_ip]`.
- `[!field]` is the complement of `[field]`: it keeps a node when `field` is absent or null. an empty field name (`[]` or `[!]`) is an error.
- chained predicates on one segment are ANDed, e.g. `[role=leaf][vendor=cisco]` or `[role=leaf][primary_ip]`.

## vars

- `vars` extract data relative to the selected node.
- use `.` for the current node and `^` for the parent.
- example: `site_slug: { from: ^.slug }`
- arrays are allowed with wildcards, e.g. `.interfaces/*/name`.
- `vars` can be defined at rule level (shared by all emits) or at emit level.
- emit-level vars override rule-level vars with the same name.

## templates

- strings support `${var}` substitution.
- if the string is exactly `${var}` with no transform, the var value is inserted as-is, preserving its type.
- when embedded in a larger string (or when a transform is applied), non-string scalars are coerced to their natural form (`42`, `true`); nulls, arrays, and objects are an error.
- transforms can be applied with `${var|transform}` and chained left-to-right, e.g. `${name|trim|slug}`. available transforms: `upper`, `lower`, `trim`, `slug`. an unknown transform name is an error.
- `slug` lowercases the value and replaces every run of characters outside `[a-z0-9]` with a single `-` (trimming leading/trailing `-`), turning a human-readable name into a valid `slug` value, e.g. `${name|slug}` maps `Frankfurt DC1` to `frankfurt-dc1`. it errors if the result would be empty (the input had no ascii alphanumerics).
- missing required vars produce a rule-scoped error.

## uid

- `uid.v5` builds a deterministic uuid from `type` + `stable`.
- `uid` can also be a string template to reuse explicit uuids in raw yaml.
- in `attrs`, `{ uid: { type, stable } }` emits a uid string.
- `uid?` is optional and omitted when a required var is missing.

## format constraints

format constraints can be used inside the schema block:

```yaml
fields:
  slug: { type: string, format: slug }
  name: { type: string, pattern: "^[A-Z0-9-]+$" }
```

## multi-emit

a single rule can emit multiple objects by using a list for `emit`:

```yaml
rules:
  - name: fabric
    select: /fabrics/*
    vars:
      site_slug: { from: .site, required: true }
      vrf_name: { from: .vrf, required: true }
    uids:
      site:
        v5:
          type: "dcim.site"
          stable: "site=${site_slug}"
      vrf:
        v5:
          type: "ipam.vrf"
          stable: "vrf=${vrf_name}"
    emit:
      - type: dcim.site
        key:
          site: "${site_slug}"
        uid: ${uids.site}
        attrs:
          name: ${site_slug}
          slug: ${site_slug}
      - type: ipam.vrf
        key:
          vrf: "${vrf_name}"
        uid: ${uids.vrf}
        attrs:
          name: ${vrf_name}
          site: ${uids.site}
```

- `vars` at rule level are extracted once and shared by all emits.
- `uids` declares named uids computed once and available as `${uids.name}`.
- each emit can have its own `vars` that override rule-level vars.
- named uids can be referenced in subsequent emits for cross-object relationships.

## determinism

- the compiler sorts objects by type name and key (canonical JSON of the key map).
- same raw yaml + same retort yields the same ir and plan order.
- multi-emit rules produce objects in a deterministic order.
