# cli

alembic ships a single cli binary with validate, plan, apply, distill, project, and cast subcommands.

## validate

```bash
alembic validate -f examples/brew.yaml
alembic validate -f examples/raw.yaml --retort examples/retort.yaml
alembic validate -f examples/raw.yaml --retort examples/retort.yaml --projection examples/projection-netbox.yaml
```

- loads and validates a brew file (plus includes)
- or compiles raw yaml with a retort before validation
- exits non-zero on validation errors

## lint

```bash
alembic lint --retort examples/retort.yaml --projection examples/projection-netbox.yaml
alembic lint --retort examples/retort.yaml
alembic lint --projection examples/projection-netbox.yaml
```

- checks retort template references and projection spec consistency
- warns on projection-only attrs keys emitted by retort but not consumed
- exits non-zero if errors are found

## plan

```bash
alembic plan -f examples/brew.yaml -o plan.json \
  --backend nautobot \
  --nautobot-url https://nautobot.example.com \
  --nautobot-token $NAUTOBOT_TOKEN

alembic plan -f examples/brew.yaml -o plan.json \
  --netbox-url https://netbox.example.com \
  --netbox-token $NETBOX_TOKEN

alembic plan -f examples/brew.yaml -o plan.json \
  --backend generic \
  --generic-config examples/generic.yaml
```

- creates a deterministic plan
- writes json plan to the output path
- honors `--allow-delete` if you want delete ops
- accepts any type string and arbitrary attrs (schema validation is required)
- `--backend` selects the target adapter (default: `netbox`, supported: `netbox`, `nautobot`, `generic`, `peeringdb`)
- `--generic-config` is required when using the generic backend
- `--projection` applies attrs -> backend mapping before planning
- `--projection-strict=false` disables custom field existence checks
- `--projection-propose` prints missing custom fields and tags and offers to create them (NetBox only)

## apply

```bash
alembic apply -p plan.json \
  --backend nautobot \
  --nautobot-url https://nautobot.example.com \
  --nautobot-token $NAUTOBOT_TOKEN \
  --allow-delete

alembic apply -p plan.json \
  --backend generic \
  --generic-config examples/generic.yaml \
  --allow-delete
```

- applies a plan file
- deletes are blocked unless `--allow-delete` is provided
- ensure the `--backend` matches the one used during planning
- the `peeringdb` backend is read-only; apply will return an error

## distill

```bash
alembic distill -f examples/raw.yaml --retort examples/retort.yaml -o ir.json
```

- compiles raw yaml into the canonical ir
- outputs deterministic json for debugging

## project

```bash
alembic project -f examples/raw.yaml --retort examples/retort.yaml \
  --projection examples/projection-netbox.yaml -o projected.json
```

- outputs ir + projected backend fields for debugging

## extract

```bash
alembic extract -o inventory.yaml \
  --backend nautobot \
  --nautobot-url https://nautobot.example.com \
  --nautobot-token $NAUTOBOT_TOKEN \
  --retort examples/retort.yaml
```

- observes backend state and emits a canonical inventory
- `--projection` inverts projection into `attrs` keys where possible
- `--retort` provides required schema metadata (retort inversion is not implemented; warning emitted)
- `peeringdb` uses `PEERINGDB_API_KEY` for authentication

## cast

```bash
alembic cast django -f examples/brew.yaml -o ./out \
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
- `PEERINGDB_API_KEY`
