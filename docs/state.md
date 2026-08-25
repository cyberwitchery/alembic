# state store

state is identity memory: it binds each logical object (`uid`) to its
materialization on exactly one backend instance (see `docs/identity.md`). by
default it lives in a per-backend file under `.alembic/state/`, and can also
use a postgres backend.

## format

```json
{
  "backend": {
    "adapter": "netbox",
    "instance": "https://netbox.example.com"
  },
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

the uid (uuid) is always a string. the backend id can be either an integer
(e.g. netbox) or a string (e.g. nautobot using uuids). `backend` is the stamp:
the adapter kind plus a stable instance identity, derived from the backend
config (the normalized endpoint url for the http adapters, the output
directory for django, a config fingerprint for external adapters) or set
explicitly with `instance:` in the backend config. an `instance:` survives an
endpoint rename; without one, a moved endpoint reads as a different backend.

## backend scoping

- the default path is scoped per backend: `.alembic/state/<adapter>-<hash>.json`,
  where the hash names the instance, so several backends planned from one
  directory each keep their own identity memory.
- a stamped file refuses any other backend by name: `this state belongs to
  netbox (https://a), but the run targets netbox (https://b)`.
- a fresh, empty file takes the stamp of the first backend that saves it.
- a file carrying mappings but no stamp predates backend-scoped state and is
  refused, never claimed: delete it and re-plan (key adoption rebinds the
  mappings), or point `ALEMBIC_STATE_PATH` at a fresh path.
- `ALEMBIC_STATE_PATH` still overrides the path; the stamp check applies to
  whatever file it names.
- apply journals are scoped the same way, so two instances of one backend
  applied from one directory cannot resume into each other's runs.
- postgres rows are backend-scoped too: the row key is
  `<workspace>/<adapter>-<hash>`, with `ALEMBIC_STATE_KEY` as the workspace
  namespace (default `default`), so several backends share one database
  without sharing a row.

## behavior

- used as the primary match during planning and apply, and as the identity
  source for `import` (state-known backend objects keep their uids).
- supports both integer (e.g. NetBox) and string/uuid (e.g. Nautobot) backend ids.
- provides stability across renames (key changes).
- a backend id answers to one uid per type: adopting an object under a new uid supersedes the uid it
  answered to, and drops that uid from the file.
- a file mapping one backend id to several uids answers with one of them, and loading drops none
  of them, so the rename stability above survives until the inventory claims a uid. each such
  backend id is logged when the file loads. only `plan -o` persists the repair; `--report` and
  `--dry-run` save nothing, so the file comes back doubled and logs again.
- when no mapping answers for a declared object, `plan` adopts the observed
  backend object matching its key (canonical JSON form). adoption writes
  identity memory, so every adoption and every superseded binding is reported;
  `--no-adopt` disables key adoption.
- updated after apply based on adapter results.
- deleting the file forgets identity: the next plan re-adopts by key (and
  says so), but objects whose keys changed in the meantime come back as
  create+delete rather than renames.
- custom types are stored under their type string.

## concurrency

for the local backend, a run that loads identity state takes an advisory lock on
a sidecar `<state>.lock` file next to the state file and holds it for its whole
lifetime, so two runs cannot load it and race to save, silently clobbering each
other's mappings. `import`, `plan --report`, and `plan --dry-run` take it shared
because they never save state; adding `--provision` to a report makes the lock
exclusive because that run writes backend schema. a plan that writes a plan file
and `apply` take it exclusive and save state. `validate`, `map`, and stateless
import do not load state or take its lock. neither lock kind starts while an
incompatible holder has it: the refused run fails fast with `another alembic run
holds the state lock` instead of waiting. the lock releases when the run exits
(the `.lock` file is left in place and reused). the postgres backend instead uses
optimistic concurrency via `loaded_version`, failing a save whose base version
changed underneath it.

## backend selection

use environment variables to select a state backend:

- `ALEMBIC_STATE_BACKEND=local` (default) or `file`
- `ALEMBIC_STATE_PATH=/path/to/state.json` (optional override for local backend)
- `ALEMBIC_STATE_BACKEND=postgres`
- `ALEMBIC_STATE_POSTGRES_URL=postgres://user:pass@host:5432/dbname`
- `ALEMBIC_STATE_KEY=my-workspace` (optional workspace namespace; the row key is `<workspace>/<adapter>-<hash>`, default workspace `default`)
- `ALEMBIC_STATE_POSTGRES_TLS=disable|require` (optional, default `disable`)
- postgres connection warnings are emitted through `tracing` (visible in cli by default at `warn` level)

the postgres backend stores the same stamped document as the file backend in
`alembic_state(state_key, payload, updated_at, loaded_version)`, and the same
backend-mismatch and unstamped-state rules apply per `state_key`.  the
table can be pre-provisioned (recommended if the runtime user lacks
DDL privileges). otherwise, the runtime will create it on first
connection.

```sql
CREATE TABLE IF NOT EXISTS alembic_state (
  state_key TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  loaded_version INTEGER NOT NULL DEFAULT 1
);
```
