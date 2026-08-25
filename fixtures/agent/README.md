# agent fixtures

the backend and the inventories `scripts/agent_fixtures.sh` exercises
`.agents/skills/alembic/SKILL.md` against. see `docs/agents.md` for what the
skill claims and why these check it.

- `backend.py` is a file-backed external adapter (`docs/external-adapters.md`)
  standing in for a real backend: it holds objects in `store.json` and appends
  every method it is asked for to `calls.log`, which is how the exercises tell a
  read-only run from one that writes backend schema. it takes both paths from the
  environment (`ALEMBIC_FIXTURE_STORE`, `ALEMBIC_FIXTURE_LOG`).
- `backend.yaml` points `--backend-config` at it.
- `schema.yaml` is the schema every inventory here includes: a site type and a
  device type referencing it.
- `base.yaml` is the starting intent. every other inventory is `base.yaml` with a
  single deliberate edit, so what a run does is attributable to that edit:

  | file | the edit |
  | --- | --- |
  | `renamed.yaml` | the site's key renamed, its uid carried |
  | `recomputed-uid.yaml` | the same rename with the uid recomputed |
  | `description-absent.yaml` | `description` removed from `attrs` |
  | `description-null.yaml` | `description` set to `null` |
  | `device-removed.yaml` | the device no longer declared |
  | `ref-by-backend-id.yaml` | the device's ref holding a backend id |

the exercises run in a scratch directory, so nothing here is written to: the
store, the identity memory and the log are all fresh per exercise.
