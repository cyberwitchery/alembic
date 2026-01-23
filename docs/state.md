# state store

alembic maintains a local mapping between ir `uid` and backend ids in `.alembic/state.json`.

## format

```json
{
  "mappings": {
    "dcim.site": {
      "a4d6a0c3-4e73-4a76-b216-4d38f8c55f3d": 12
    },
    "dcim.device": {
      "7b8f7a92-8fd0-4667-9a4b-9f3b5c9a4b1a": "7b8f7a92-8fd0-4667-9a4b-9f3b5c9a4b1a"
    }
  }
}
```

## behavior

- used as the primary match during planning and apply.
- supports both integer (e.g. NetBox) and string/uuid (e.g. Nautobot) backend ids.
- provides stability across renames (key changes).
- when empty, alembic can bootstrap mappings by matching observed objects by key.
- updated after apply based on adapter results.
- safe to delete if you want to re-discover by key, but expect extra lookups.
- custom types are stored under their type string.
