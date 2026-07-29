# cli

alembic ships a single cli binary with validate, import, map, plan, and apply subcommands.

## validate

```bash
alembic validate -f examples/inventory.yaml
```

- loads and validates an inventory file (plus includes)
- exits non-zero on validation errors

## backend config

backend adapters are configured via a yaml file passed with `--backend-config`.

unknown keys are rejected: a typo'd option is a parse error naming the field,
not a silently ignored key that leaves the default in place.

netbox:

```yaml
backend: netbox
url: https://netbox.example.com
token: nbt_xxx_replace_me
```

infrahub with schema provisioning:

```yaml
backend: infrahub
url: https://infrahub.example.com
token: infrahub_xxx_replace_me
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
working_dir: ./adapters/mybackend
env:
  MY_BACKEND_URL: https://backend.example.com
timeout_seconds: 60
setup:
  my_backend_variable_x: 37.0
```

`env:` values are passed to the adapter verbatim; the adapter also inherits
alembic's environment, so export secrets (e.g. `MY_BACKEND_TOKEN`) instead of
putting them in the config file.

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
- against a write-only (emitter) backend such as `django`, which cannot report existing state, plan produces an all-creates plan against an empty observation
- writes json plan to the `-o`/`--output` path (required only for this default write path), and prints a human-readable per-op summary of that plan (create/update/delete, with per-field `from -> to` for updates; long categories are truncated) so you can read what apply would do before applying
- honors `--allow-delete` if you want delete ops
- without `--provision`, plan asks the backend for a read-only schema preview (what `apply`'s `ensure_schema` would create/delete, writing nothing) and prints it to stderr as `schema preview: ...`; the machine-readable copy rides in the plan's `schema_preview`. backends that cannot preview report `schema preview: unavailable for this backend`
- `--provision` runs adapter provisioning (`ensure_schema`) before observing backend state; provisioning that would delete custom object types/fields the inventory no longer declares is blocked unless `--allow-delete` is also given (such deletes cascade to their objects on the backend)
- `--dry-run` prints the raw plan json instead of writing it (no `-o`/`--output` needed)
- `--report` prints a read-only drift report and exits without writing a plan file or saving state (no `-o`/`--output` needed)
- `--report` and `--dry-run` are mutually exclusive (both exit without applying); passing both is rejected at parse time
- `--provision` cannot be combined with `--dry-run` (rejected at parse time): a `--dry-run` preview promises not to write, but `--provision` still writes backend schema (`ensure_schema`). combining `--provision` with `--report` stays allowed as the documented "provision schema, then preview drift" workflow (see below)
- accepts any type string and arbitrary attrs (schema validation is required)

### drift report

```bash
NETBOX_URL=https://netbox.example.com NETBOX_TOKEN=$NETBOX_TOKEN \
  alembic plan --backend netbox -f examples/inventory.yaml --report
```

`--report` surfaces the same desired-vs-observed diff that `plan` computes, as a
standalone human-readable summary grouped into three categories:

- **changed**: declared and present on the backend, but one or more fields diverge (lists the per-field `from -> to`)
- **missing**: declared in intent but absent from the backend
- **extra**: present on the backend but not declared in intent

it is one-way by construction: it only ever describes how observed state diverges
from intent and never writes observed state back into the inventory or state
store. `-o`/`--output` is optional in `--report` mode (as with `--dry-run`): both
exit without writing a plan file, so neither needs an output path. it is required
only for the default write path.

note that combining `--report` with `--provision` is not fully read-only:
`--provision` still runs adapter provisioning (`ensure_schema`) against the
backend before the report is computed, which can issue schema writes (e.g.
creating netbox custom fields). the report itself remains read-only; the
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
- deletes are blocked unless `--allow-delete` is provided; this covers both object deletes and destructive schema provisioning (deleting custom object types/fields the inventory no longer declares, which cascades to their objects)
- `--interactive` prompts per operation and applies only approved ops
  through the same engine path used by non-interactive apply
- the `peeringdb` backend is read-only; apply will return an error
- apply runs adapter provisioning (`ensure_schema`) before writes on read+write
  backends; for netbox this can create custom fields, custom object types, and
  custom object type fields when supported. write-only emitter backends (django)
  skip provisioning
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

observe a backend's live state into the data model.

```bash
alembic import -f examples/inventory.yaml -o observed.json \
  --backend-config examples/backend-nautobot.yaml
```

- `-f` is your inventory; its `schema` selects which types to observe.
- `-o` receives the observed inventory (ir).
- import neither reads nor locks the state store; it observes in the canonical uid space.
- `peeringdb` uses `PEERINGDB_API_KEY` for authentication

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
