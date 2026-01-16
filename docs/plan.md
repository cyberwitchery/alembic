# plan format

plans are json files that can be re-applied. the plan is deterministic for a given desired state and observed backend snapshot.

## schema (mvp)

```json
{
  "ops": [
    {
      "op": "create",
      "uid": "...",
      "kind": "dcim.site",
      "desired": {
        "base": { "uid": "...", "kind": "dcim.site", "key": "site=fra1", "attrs": { ... } },
        "projection": { "custom_fields": { ... }, "tags": ["..."] }
      }
    },
    {
      "op": "update",
      "uid": "...",
      "kind": "dcim.device",
      "desired": { ... },
      "changes": [
        { "field": "name", "from": "old", "to": "new" }
      ],
      "backend_id": 123
    },
    {
      "op": "delete",
      "uid": "...",
      "kind": "ipam.ip_address",
      "key": "ip=10.0.0.10/24",
      "backend_id": 456
    }
  ]
}
```

## notes

- `kind` may be any custom string; unknown kinds are planned like typed ones.
- generic attrs are compared as an opaque payload.
- `backend_id` is optional and may be absent for creates or if not known.
- deletes are only applied when `--allow-delete` is set.
- `projection` is present only when projection rules apply to an object, and diffs include projected fields.
- optional fields omitted in desired (`null`/missing) are treated as "no change" to preserve idempotence.
