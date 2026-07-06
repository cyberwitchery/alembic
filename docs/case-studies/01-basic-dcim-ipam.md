# case study: basic dcim + ipam

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

the desired state, as a vendor-neutral inventory:

```yaml
schema:
  types:
    dcim.manufacturer:
      key:
        slug:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
    dcim.device_role:
      key:
        slug:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
    dcim.device_type:
      key:
        slug:
          type: slug
      fields:
        manufacturer:
          type: ref
          target: dcim.manufacturer
        model:
          type: string
        slug:
          type: slug
    dcim.site:
      key:
        slug:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
    dcim.device:
      key:
        name:
          type: slug
      fields:
        name:
          type: string
        site:
          type: ref
          target: dcim.site
        role:
          type: ref
          target: dcim.device_role
        device_type:
          type: ref
          target: dcim.device_type
        status:
          type: string
    dcim.interface:
      key:
        name:
          type: slug
      fields:
        name:
          type: string
        device:
          type: ref
          target: dcim.device
        type:
          type: string
        enabled:
          type: bool
    ipam.prefix:
      key:
        prefix:
          type: prefix
      fields:
        prefix:
          type: prefix
        site:
          type: ref
          target: dcim.site
        description:
          type: string
    ipam.ip_address:
      key:
        address:
          type: ip_address
      fields:
        address:
          type: ip_address
        assigned_interface:
          type: ref
          target: dcim.interface
        description:
          type: string
objects:
  - uid: "f1c8a9d4-2a3b-4c5d-8e9f-0123456789ab"
    type: dcim.manufacturer
    key:
      slug: "acme"
    attrs:
      name: "Acme"
      slug: "acme"

  - uid: "b1c2d3e4-5f60-4a7b-8c9d-0e1f2a3b4c5d"
    type: dcim.device_role
    key:
      slug: "leaf"
    attrs:
      name: "leaf"
      slug: "leaf"

  - uid: "c2d3e4f5-6071-4b8c-9d0e-1f2a3b4c5d6e"
    type: dcim.device_type
    key:
      slug: "leaf-switch"
    attrs:
      manufacturer: "f1c8a9d4-2a3b-4c5d-8e9f-0123456789ab"
      model: "leaf-switch"
      slug: "leaf-switch"

  - uid: "a4d6a0c3-4e73-4a76-b216-4d38f8c55f3d"
    type: dcim.site
    key:
      slug: "fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"

  - uid: "7b8f7a92-8fd0-4667-9a4b-9f3b5c9a4b1a"
    type: dcim.device
    key:
      name: "leaf01"
    attrs:
      name: "leaf01"
      site: "a4d6a0c3-4e73-4a76-b216-4d38f8c55f3d"
      role: "b1c2d3e4-5f60-4a7b-8c9d-0e1f2a3b4c5d"
      device_type: "c2d3e4f5-6071-4b8c-9d0e-1f2a3b4c5d6e"
      status: "active"

  - uid: "5a1c43a4-4f52-4d07-8a2f-88ad1fbdf8c0"
    type: dcim.interface
    key:
      name: "eth0"
    attrs:
      name: "eth0"
      device: "7b8f7a92-8fd0-4667-9a4b-9f3b5c9a4b1a"
      type: "1000base-t"
      enabled: true

  - uid: "4b8a93d3-6a6d-4ef5-9b04-1de2b8f5b8f2"
    type: dcim.interface
    key:
      name: "eth1"
    attrs:
      name: "eth1"
      device: "7b8f7a92-8fd0-4667-9a4b-9f3b5c9a4b1a"
      type: "1000base-t"
      enabled: true

  - uid: "dc0adf72-3c0b-4c3a-8b18-23a7c0a7c0f1"
    type: ipam.prefix
    key:
      prefix: "10.0.0.0/24"
    attrs:
      prefix: "10.0.0.0/24"
      site: "a4d6a0c3-4e73-4a76-b216-4d38f8c55f3d"
      description: "FRA1 leaf subnet"

  - uid: "c4a0c0f0-ef8a-4c7f-9b0a-2ff3a4d14fd1"
    type: ipam.ip_address
    key:
      address: "10.0.0.10/24"
    attrs:
      address: "10.0.0.10/24"
      assigned_interface: "5a1c43a4-4f52-4d07-8a2f-88ad1fbdf8c0"
      description: "leaf01 eth0"
```

## map to netbox

netbox's ipam names an ip's interface assignment `assigned_object`, a generic
foreign key, not `assigned_interface`. one `map` step reshapes the neutral model
to netbox's names: a rule that renames the field on `ipam.ip_address`, and a
`match: "*"` passthrough that carries every other type through unchanged. because
passthrough carries each source type's schema too, the target `schema` only
declares the one type you reshape. refs are rewired automatically, so the ip
still points at its interface even though `map` re-derives uids. save this as
`netbox-map.yaml`:

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

```bash
BACKEND_CONFIG=/path/to/backend-netbox.yaml

# 1. reshape the neutral model to netbox's field names.
alembic map -f /path/to/basic.yaml --spec /path/to/netbox-map.yaml -o /tmp/netbox.json

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
