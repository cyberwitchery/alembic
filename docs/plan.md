# plan format

plans are json files that can be re-applied. the plan is deterministic for a given desired state and observed backend snapshot.

## schema (mvp)

```json
{
  "ops": [
    {
      "op": "create",
      "uid": "...",
      "type_name": "dcim.site",
      "desired": {
        "base": { "uid": "...", "type": "dcim.site", "key": "site=fra1", "attrs": { "...": "..." } },
        "projection": { "custom_fields": { "...": "..." }, "tags": ["..."] }
      }
    },
    {
      "op": "update",
      "uid": "...",
      "type_name": "dcim.device",
      "desired": { "...": "..." },
      "changes": [
        { "field": "name", "from": "old", "to": "new" }
      ],
      "backend_id": 123
    },
    {
      "op": "delete",
      "uid": "...",
      "type_name": "ipam.ip_address",
      "key": "ip=10.0.0.10/24",
      "backend_id": 456
    }
  ]
}
```

## notes

- `type_name` may be any custom string.
- `backend_id` is optional and may be absent for creates or if not known.
- deletes are only applied when `--allow-delete` is set.
- `projection` is present only when projection rules apply to an object, and diffs include projected fields.
