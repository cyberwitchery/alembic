# cli

alembic ships a single cli binary with validate, import, map, plan, apply, and cast subcommands.

## validate

```bash
alembic validate -f examples/inventory.yaml
```

- loads and validates an inventory file (plus includes)
- exits non-zero on validation errors

## backend config

backend adapters are configured via a yaml file passed with `--backend-config`.

netbox:

```yaml
backend: netbox
url: https://netbox.example.com
token: $NETBOX_TOKEN
```

infrahub with schema provisioning:

```yaml
backend: infrahub
url: https://infrahub.example.com
token: $INFRAHUB_TOKEN
schema:
  mode: infrahubctl
  schema_path: ./schema/alembic.generated.yaml
  infrahubctl_path: ./scripts/infrahubctl
```

generic:

```yaml
backend: generic
config_path: examples/generic.yaml
```

external process adapter:

```yaml
backend: external
command: ./bin/alembic-adapter-mybackend
args: ["--verbose"]
env:
  MY_BACKEND_URL: https://backend.example.com
  MY_BACKEND_TOKEN: $TOKEN
timeout_seconds: 60
```

if you don't want a config file, you can pass `--backend <name>` and
supply credentials via environment variables.

## plugins

As a convenience, external adapter configs can be kept in a common
directory (default: `plugins` in the working directory, see
`docs/configuration.md` for how to change that). By passing the
filename of such a config as the `--backend`, that backend config is
automatically used.

For example, if there's a backend config for a custom external adapter
in `./plugins/my_adapter.yaml`; here's how to run it:

```bash
$ alembic apply --backend my_adapter
```

## plan

```bash
alembic plan -f examples/inventory.yaml -o plan.json \
  --backend-config examples/backend-netbox.yaml

NETBOX_URL=https://netbox.example.com NETBOX_TOKEN=$NETBOX_TOKEN \
  alembic plan --backend netbox -f examples/inventory.yaml -o plan.json
```

- creates a deterministic plan
- writes json plan to the output path
- honors `--allow-delete` if you want delete ops
- `--provision` runs adapter provisioning (`ensure_schema`) before observing backend state
- `--dry-run` prints the raw plan json instead of writing it
- `--report` prints a read-only drift report and exits without writing a plan file or saving state
- `--report` and `--dry-run` are mutually exclusive (both exit without applying); passing both is rejected at parse time
- accepts any type string and arbitrary attrs (schema validation is required)

### drift report

```bash
NETBOX_URL=https://netbox.example.com NETBOX_TOKEN=$NETBOX_TOKEN \
  alembic plan --backend netbox -f examples/inventory.yaml -o plan.json --report
```

`--report` surfaces the same desired-vs-observed diff that `plan` computes, as a
standalone human-readable summary grouped into three categories:

- **changed**: declared and present on the backend, but one or more fields diverge (lists the per-field `from -> to`)
- **missing**: declared in intent but absent from the backend
- **extra**: present on the backend but not declared in intent

it is one-way by construction: it only ever describes how observed state diverges
from intent and never writes observed state back into the inventory or state
store. `--output` is still required (as with `--dry-run`) but nothing is written
to it.

note that combining `--report` with `--provision` is not fully read-only:
`--provision` still runs adapter provisioning (`ensure_schema`) against the
backend before the report is computed, which can issue schema writes (e.g.
creating netbox custom fields/tags). the report itself remains read-only; the
schema writes come from `--provision`, not from the report.

## apply

```bash
alembic apply -p plan.json \
  --backend-config examples/backend-netbox.yaml \
  --allow-delete

alembic apply -p plan.json \
  --backend-config examples/backend-infrahub.yaml \
  --allow-delete
```

- applies a plan file
- deletes are blocked unless `--allow-delete` is provided
- `--interactive` prompts per operation and applies only approved ops
  through the same engine path used by non-interactive apply
- the `peeringdb` backend is read-only; apply will return an error
- apply runs adapter provisioning (`ensure_schema`) before writes on read+write
  backends; for netbox this can create custom fields/tags and custom object
  types when supported. write-only emitter backends (django) skip provisioning
- infrahub provisioning can generate and load a schema file when
  configured in the backend config

note that apply has no transaction semantics. state is persisted after each successful write, so a crash partway through leaves backend objects with no corresponding cleanup and no rollback.

## map

```bash
alembic map -f examples/map-input.yaml --spec examples/map.yaml -o ir.json
```

- transforms an ir inventory into another ir inventory (ir to ir)
- `--spec` declares the target schema and the rename/reshape rules
- output is validated against the target schema; see `docs/map.md`

### map transform

```bash
alembic map transform --spec examples/map.yaml site_code '"fra1"'
```

- evaluates a single transform (user-defined or built-in) against a
  json-encoded value, without an inventory or backend — the iteration loop for
  writing a spec's starlark transforms (see `docs/map.md`, transforms)
- extra positional arguments are json-encoded transform arguments
- prints the typed result as json; `fail()` exits non-zero with the message

## import

observe a backend's live state into canonical ir.

```bash
alembic import -f examples/inventory.yaml -o observed.yaml \
  --backend-config examples/backend-nautobot.yaml
```

- `-f` is your inventory; its `schema` selects which types to observe.
- `-o` receives the observed inventory (ir).
- `peeringdb` uses `PEERINGDB_API_KEY` for authentication

## cast

```bash
alembic cast django -f examples/inventory.yaml -o ./out \
  --project alembic_project \
  --app alembic_app \
  --python python3
```

- scaffolds a django project/app and runs `manage.py check`
- runs `manage.py makemigrations` and `manage.py migrate` by default
- generates `generated_models.py` and `generated_admin.py` in the app
- only creates user-owned `models.py`/`admin.py`/`extensions.py` if they are missing
- `--no-admin` skips admin generation
- `--no-migrate` skips `migrate` but still runs `makemigrations`

## environment variables

- `NETBOX_URL`
- `NETBOX_TOKEN`
- `NAUTOBOT_URL`
- `NAUTOBOT_TOKEN`
- `INFRAHUB_URL`
- `INFRAHUB_TOKEN`
- `GENERIC_CONFIG` (path to generic adapter config)
- `EXTERNAL_COMMAND` (path to external adapter executable)
- `PEERINGDB_API_KEY`
- `ALEMBIC_STATE_BACKEND` (`local`/`file`/`postgres`, default: `local`)
- `ALEMBIC_STATE_PATH` (optional local state file path override)
- `ALEMBIC_STATE_POSTGRES_URL` (required when `ALEMBIC_STATE_BACKEND=postgres`)
- `ALEMBIC_STATE_KEY` (optional logical key in postgres backend, default: `default`)
- `ALEMBIC_STATE_POSTGRES_TLS` (`disable`/`require`, default: `disable`)
- `RUST_LOG` (optional; defaults to `warn`, used by cli tracing output)
