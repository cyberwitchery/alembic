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
      "desired": { "uid": "...", "kind": "dcim.site", "key": "site=fra1", "attrs": { ... } }
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

- `backend_id` is optional and may be absent for creates or if not known.
- deletes are only applied when `--allow-delete` is set.
