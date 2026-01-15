# brew format

alembic consumes yaml or json files. yaml is recommended.

## top-level document

```yaml
include:
  - other.yaml
imports:
  - more.yaml
objects:
  - uid: "..."
    kind: dcim.site
    key: "site=fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"
```

- `objects` is required.
- `include` and `imports` are optional and equivalent.
- paths in `include/imports` are resolved relative to the current file.
- files are loaded once (deduplicated by canonical path).

## json input

json is supported when the file extension is `.json`.

## guidelines

- use stable uuids for `uid`.
- keep `key` human-readable and stable across renames where possible.
- never include backend ids in `attrs`.

## generic kinds

unknown kinds are allowed. when alembic cannot match a typed schema, it stores `attrs` as generic data.

```yaml
objects:
  - uid: "00000000-0000-0000-0000-000000000001"
    kind: services.vpn
    key: "vpn=corp"
    attrs:
      peers:
        - name: site1
          ip: 10.0.0.1
        - name: site2
          ip: 10.0.0.2
      pre_shared_key: "secret"
  - uid: "00000000-0000-0000-0000-000000000002"
    kind: dcim.site
    key: "site=fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"
```
