# projection

projection maps `x` extension data into backend fields (custom fields, tags, local context) before planning. it keeps `attrs` portable and moves backend-specific storage to the projection spec.

## spec

```yaml
version: 1
backend: netbox
rules:
  - name: model_to_custom_fields_devices
    on_kind: dcim.device
    from_x:
      map:
        "model.fabric": "fabric"
        "model.role_hint": "role_hint"
    to:
      custom_fields:
        strategy: explicit

  - name: model_tags
    on_kind: "*"
    from_x:
      key: "model.tags"
    to:
      tags:
        source: value
```

see `examples/projection-netbox.yaml` for a full example paired with `examples/raw.yaml`.

## selection

- `on_kind` matches a single kind string (e.g. `dcim.device`) or `*`.
- `from_x` must specify exactly one selector:
  - `prefix`: match keys with a prefix
  - `key`: match a single key
  - `map`: explicit `x_key -> field_name` map

## targets

- `custom_fields` writes to netbox custom fields.
- `tags` expects a list of strings (tag names).
- `local_context` writes a json blob (supported on devices only).

## strategies

- `strip_prefix`: remove a prefix from `x` keys.
- `explicit`: use the `map` entries.
- `direct`: use the `x` key or an explicit `field`.

## transforms

optional transforms can be attached under `from_x.transform`:

- `stringify`
- `drop_if_null`
- `join: ","` (array join)
- `default: <json>`

## strict mode

with `--projection-strict` (default true), planning fails if a custom field is missing in netbox.

## propose mode

use `--projection-propose` to list missing custom fields and tags and optionally create them after confirmation before planning.
