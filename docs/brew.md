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
