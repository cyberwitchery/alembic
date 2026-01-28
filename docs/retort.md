# retort

retort is a small mapping layer that compiles raw yaml into the canonical ir. it is declarative, yaml-only, and deterministic.

## shape

```yaml
version: 1
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

## vars

- `vars` extract data relative to the selected node.
- use `.` for the current node and `^` for the parent.
- example: `site_slug: { from: ^.slug }`
- arrays are allowed with wildcards, e.g. `.interfaces/*/name`.
- `vars` can be defined at rule level (shared by all emits) or at emit level.
- emit-level vars override rule-level vars with the same name.

## templates

- strings support `${var}` substitution only.
- if the string is exactly `${var}`, the var value is inserted as-is.
- missing required vars produce a rule-scoped error.

## uid

- `uid.v5` builds a deterministic uuid from `type` + `stable`.
- `uid` can also be a string template to reuse explicit uuids in raw yaml.
- in `attrs`, `{ uid: { type, stable } }` emits a uid string.
- `uid?` is optional and omitted when a required var is missing.

## multi-emit

a single rule can emit multiple objects by using a list for `emit`:

```

format constraints can be used inside the schema block:

```yaml
fields:
  slug: { type: string, format: slug }
  name: { type: string, pattern: "^[A-Z0-9-]+$" }
```
yaml
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
