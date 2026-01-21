# case study: import from csv

## goal

convert a csv export into alembic objects using retort, then apply to netbox.

## input (csv)

```csv
site,device
fra1,leaf01
fra1,leaf02
```

## retort

```yaml
version: 1
rules:
  - name: devices
    select: rows
    vars:
      site:
        from: site
      device:
        from: device
    emit:
      type: dcim.device
      key: "name=${device}"
      attrs:
        name: "${device}"
        site: "${uids.site}"
    uids:
      site:
        type: dcim.site
        stable: "slug=${site}"
```

## commands

```bash
alembic distill -f /path/to/devices.csv --retort /path/to/retort.yaml -o /tmp/inventory.yaml

alembic plan -f /tmp/inventory.yaml -o /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN

alembic apply -p /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN --allow-delete
```

## notes

- retort assigns stable uids based on csv values.
- ensure referenced objects (like sites) are in inventory or already exist in netbox.
