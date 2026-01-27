# brew vs raw

alembic consumes yaml or json files. yaml is recommended. there are two input modes:

- brew: canonical ir objects at `objects:`.
- raw + retort: arbitrary yaml compiled into ir.

## brew format

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

- `objects` is required.
- `include` and `imports` are optional and equivalent.
- paths in `include/imports` are resolved relative to the current file.
- files are loaded once (deduplicated by canonical path).

## schema

brew files must define schema metadata alongside objects.

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

## json input

json is supported when the file extension is `.json`.

## guidelines

- use stable uuids for `uid`.
- keep `key` human-readable and stable across renames where possible.
- never include backend ids in `attrs`.

## raw + retort

raw yaml uses any shape you want, and a retort mapping compiles it into the ir.

```

string fields can optionally use `format` and/or `pattern`:

```yaml
fields:
  slug: { type: string, format: slug }
  name: { type: string, pattern: "^[A-Z0-9-]+$" }
```
bash
alembic distill -f examples/raw.yaml --retort examples/retort.yaml -o ir.json
```

- if the input has a top-level `objects` list, alembic treats it as brew and ignores retort.
- otherwise, `--retort` is required for validate/plan.
