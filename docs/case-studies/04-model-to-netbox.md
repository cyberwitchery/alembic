# case study: model data and apply to netbox

## goal

author a simple model in yaml and converge it into netbox.

## steps

1) write an inventory file

```yaml
schema:
  types:
    dcim.site:
      key:
        slug:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
objects:
  - uid: "11111111-1111-1111-1111-111111111111"
    type: dcim.site
    key:
      slug: "lab1"
    attrs:
      name: "lab1"
      slug: "lab1"
```

2) plan + apply

```bash
alembic plan -f /path/to/inventory.yaml -o /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN

alembic apply -p /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN --allow-delete
```

## notes

- keep keys stable and unique.
- use uids for any cross-object references.
