# state store

alembic maintains a mapping between ir `uid` and backend ids. by default it uses
local file storage at `.alembic/state.json`, and can also use a postgres backend.

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

The uid (uuid) is always a string. the backend id can be either an integer (e.g., NetBox) or a string (e.g., Nautobot using UUIDs).

## behavior

- used as the primary match during planning and apply.
- supports both integer (e.g. NetBox) and string/uuid (e.g. Nautobot) backend ids.
- provides stability across renames (key changes).
- when empty, alembic can bootstrap mappings by matching observed objects by key (canonical JSON form).
- updated after apply based on adapter results.
- safe to delete if you want to re-discover by key, but expect extra lookups.
- custom types are stored under their type string.

## backend selection

use environment variables to select a state backend:

- `ALEMBIC_STATE_BACKEND=local` (default) or `file`
- `ALEMBIC_STATE_PATH=/path/to/state.json` (optional override for local backend)
- `ALEMBIC_STATE_BACKEND=postgres`
- `ALEMBIC_STATE_POSTGRES_URL=postgres://user:pass@host:5432/dbname`
- `ALEMBIC_STATE_KEY=my-workspace` (optional logical key, default `default`)
- `ALEMBIC_STATE_POSTGRES_TLS=disable|require` (optional, default `disable`)
- postgres connection warnings are emitted through `tracing` (visible in cli by default at `warn` level)

the postgres backend stores state payloads in `alembic_state(state_key, payload, updated_at)`.
the table can be pre-provisioned (otherwise, the runtime will create
it on first connection).

```sql
CREATE TABLE IF NOT EXISTS alembic_state (
  state_key TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  loaded_version INTEGER NOT NULL DEFAULT 1
);
```
