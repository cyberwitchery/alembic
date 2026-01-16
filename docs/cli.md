# cli

alembic ships a single cli binary with validate, plan, apply, distill, and project subcommands.

## validate

```bash
alembic validate -f examples/brew.yaml
alembic validate -f examples/raw.yaml --retort examples/retort.yaml
alembic validate -f examples/raw.yaml --retort examples/retort.yaml --projection examples/projection-netbox.yaml
```

- loads and validates a brew file (plus includes)
- or compiles raw yaml with a retort before validation
- exits non-zero on validation errors

## plan

```bash
alembic plan -f examples/brew.yaml -o plan.json \
  --netbox-url https://netbox.example.com \
  --netbox-token $NETBOX_TOKEN

alembic plan -f examples/raw.yaml --retort examples/retort.yaml -o plan.json \
  --netbox-url https://netbox.example.com \
  --netbox-token $NETBOX_TOKEN

alembic plan -f examples/raw.yaml --retort examples/retort.yaml \
  --projection examples/projection-netbox.yaml \
  -o plan.json --netbox-url https://netbox.example.com \
  --netbox-token $NETBOX_TOKEN
```

- creates a deterministic plan
- writes json plan to the output path
- honors `--allow-delete` if you want delete ops
- accepts generic kinds and attrs
- `--projection` applies x -> backend mapping before planning
- `--projection-strict=false` disables custom field existence checks
- `--projection-propose` prints missing custom fields and tags and offers to create them

## apply

```bash
alembic apply -p plan.json \
  --netbox-url https://netbox.example.com \
  --netbox-token $NETBOX_TOKEN \
  --allow-delete
```

- applies a plan file
- deletes are blocked unless `--allow-delete` is provided

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

## environment variables

- `NETBOX_URL`
- `NETBOX_TOKEN`
