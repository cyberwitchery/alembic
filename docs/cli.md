# cli

alembic ships a single cli binary with validate, plan, and apply subcommands.

## validate

```bash
alembic validate -f examples/brew.yaml
```

- loads and validates a brew file (plus includes)
- exits non-zero on validation errors

## plan

```bash
alembic plan -f examples/brew.yaml -o plan.json \
  --netbox-url https://netbox.example.com \
  --netbox-token $NETBOX_TOKEN
```

- creates a deterministic plan
- writes json plan to the output path
- honors `--allow-delete` if you want delete ops

## apply

```bash
alembic apply -p plan.json \
  --netbox-url https://netbox.example.com \
  --netbox-token $NETBOX_TOKEN \
  --allow-delete
```

- applies a plan file
- deletes are blocked unless `--allow-delete` is provided

## environment variables

- `NETBOX_URL`
- `NETBOX_TOKEN`
