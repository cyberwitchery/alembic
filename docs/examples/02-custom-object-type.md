# example: a type netbox does not have

## goal

netbox core has no maintenance-window object. rather than track windows in a
spreadsheet next to your source of truth, declare the type in your schema and let
alembic provision it on netbox as a custom object, right beside the native site
and device it relates to.

## the model

[`examples/walkthroughs/custom-model.yaml`](../../examples/walkthroughs/custom-model.yaml)
declares a native site and device (plus the manufacturer, device type and role
netbox requires of a device) and one type netbox does not ship:

```yaml
    ops.maintenance_window:
      key: {name: {type: slug}}
      fields:
        name: {type: slug}
        starts_at: {type: datetime}
        ends_at: {type: datetime}
        device: {type: ref, target: dcim.device}   # references a native device
```

alembic resolves each type against netbox's object types. the `dcim.*` types are
native and go to their normal endpoints. `ops.maintenance_window`
is not, so on apply the adapter provisions it as a custom object type (with a
field per schema key and field) through the netbox custom objects plugin, then
creates the object under `/api/plugins/custom-objects/`. its `device` field is an
ordinary `ref`, resolved to the native device's backend id like any other
reference.

## commands

`--provision` creates the custom object type and its fields as part of
applying the plan:

<!-- ci: netbox -->
```bash
BACKEND_CONFIG=/path/to/backend-netbox.yaml

alembic plan -f examples/walkthroughs/custom-model.yaml \
  -o /tmp/plan.json --backend-config "$BACKEND_CONFIG"

alembic apply --provision -p /tmp/plan.json --backend-config "$BACKEND_CONFIG"
```

`apply --provision` prints what it created, as counts:

```
provision: 1 object types created, 4 object fields created
```

## notes

- this needs the netbox custom objects plugin and its REST API; native types are
  unaffected, only the types netbox lacks are provisioned as custom objects.
- removing the type from your schema later deletes the custom object type on the
  next `apply --provision`, which cascades to its objects, so that
  path is gated behind `--allow-delete`.
- a plain `apply` of a plan whose schema preview lists the custom type is refused
  before anything is written, native types included; re-run with `--provision`.
