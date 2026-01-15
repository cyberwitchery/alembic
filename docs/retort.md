# retort

retort is a small mapping layer that compiles raw yaml into the canonical ir. it is declarative, yaml-only, and deterministic.

## shape

```yaml
version: 1
rules:
  - name: sites
    select: /sites/*
    emit:
      kind: dcim.site
      key: "site=${slug}"
      uid:
        v5:
          kind: "dcim.site"
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

## templates

- strings support `${var}` substitution only.
- if the string is exactly `${var}`, the var value is inserted as-is.
- missing required vars produce a rule-scoped error.

## uid

- `uid.v5` builds a deterministic uuid from `kind` + `stable`.
- `uid` can also be a string template to reuse explicit uuids in raw yaml.
- in `attrs`, `{ uid: { kind, stable } }` emits a uid string.
- `uid?` is optional and omitted when a required var is missing.

## determinism

- the compiler sorts objects by kind rank, kind string, and key.
- same raw yaml + same retort yields the same ir and plan order.
