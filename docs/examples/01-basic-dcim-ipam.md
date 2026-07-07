# example: basic dcim + ipam

## goal

take a small dcim/ipam model, a site, a device, two interfaces, a prefix, and an
ip address, and converge it onto netbox.

the model is authored once and kept vendor-neutral: it names an ip's interface
assignment `assigned_interface`, the name that reads well in your own vocabulary.
that neutrality is the point; it is what lets the same model target more than one
backend. netbox happens to call that field `assigned_object` (a generic foreign
key), so the walkthrough is three steps: author the model, `map` it to netbox's
field names, then plan and apply. every other field already matches netbox, so
the map is almost entirely a pass-through.

## the model

the desired state, authored once as a vendor-neutral inventory. the full file is
[`examples/walkthroughs/01-basic.yaml`](../../examples/walkthroughs/01-basic.yaml)
(eight types: manufacturer, device role and type, site, device, two interfaces,
prefix, ip address). its one netbox-specific wrinkle is how the ip names its
interface assignment:

```yaml
    ipam.ip_address:
      key:
        address:
          type: ip_address
      fields:
        address:
          type: ip_address
        assigned_interface:        # netbox calls this `assigned_object`
          type: ref
          target: dcim.interface
        description:
          type: string
# ...
objects:
  # ...
  - uid: "c4a0c0f0-ef8a-4c7f-9b0a-2ff3a4d14fd1"
    type: ipam.ip_address
    key:
      address: "10.0.0.10/24"
    attrs:
      address: "10.0.0.10/24"
      assigned_interface: "5a1c43a4-4f52-4d07-8a2f-88ad1fbdf8c0"  # the eth0 interface
      description: "leaf01 eth0"
```

## map to netbox

netbox's ipam names an ip's interface assignment `assigned_object`, a generic
foreign key, not `assigned_interface`. one `map` step reshapes the neutral model
to netbox's names: a rule that renames the field on `ipam.ip_address`, and a
`match: "*"` passthrough that carries every other type through unchanged. because
passthrough carries each source type's schema too, the target `schema` only
declares the one type you reshape. refs are rewired automatically, so the ip
still points at its interface even though `map` re-derives uids. the spec is
[`examples/walkthroughs/01-netbox-map.yaml`](../../examples/walkthroughs/01-netbox-map.yaml):

```yaml
schema:
  types:
    ipam.ip_address:
      key: {address: {type: ip_address}}
      fields:
        address: {type: ip_address}
        assigned_object: {type: ref, target: dcim.interface}
        description: {type: string}
rules:
  - name: rename-assignment
    match: ipam.ip_address
    emit:
      type: ipam.ip_address
      key: {address: "${key.address}"}
      attrs:
        address: "${attrs.address}"
        assigned_object: "${attrs.assigned_interface}"
        description: "${attrs.description}"
  - name: rest
    match: "*"
    emit: passthrough
```

## commands

three steps: reshape the model to netbox's names, plan the result, then apply.
`plan` and `apply` never see the neutral model; they work on the mapped
inventory, exactly as if you had authored it netbox-shaped.

run from a checkout of this repo (or point `-f`/`--spec` at your own copies):

```bash
BACKEND_CONFIG=/path/to/backend-netbox.yaml

# 1. reshape the neutral model to netbox's field names.
alembic map -f examples/walkthroughs/01-basic.yaml \
  --spec examples/walkthroughs/01-netbox-map.yaml -o /tmp/netbox.json

# 2. plan and apply the netbox-shaped inventory.
alembic plan -f /tmp/netbox.json -o /tmp/plan.json \
  --backend-config "$BACKEND_CONFIG"

alembic apply -p /tmp/plan.json \
  --backend-config "$BACKEND_CONFIG" --allow-delete
```

## notes

- reference other objects by their uid string; keys are used only for bootstrap or when state is missing.
- only the fields netbox names differently need a rule; the `match: "*"` passthrough carries everything else, including the interface's `type` (already netbox's name), through untouched.
- because `map` re-derives each uid from its `(type, key)`, that identity stays stable across runs even though the mapped uids differ from the ones you authored.
