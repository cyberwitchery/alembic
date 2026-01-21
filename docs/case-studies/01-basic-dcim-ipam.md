# case study: basic dcim + ipam

## goal

create a site, device, two interfaces, a prefix, and an ip address, then converge netbox.

## inventory

```yaml
objects:
  - uid: "f1c8a9d4-2a3b-4c5d-8e9f-0123456789ab"
    type: dcim.manufacturer
    key: "slug=acme"
    attrs:
      name: "Acme"
      slug: "acme"

  - uid: "b1c2d3e4-5f60-4a7b-8c9d-0e1f2a3b4c5d"
    type: dcim.device_role
    key: "slug=leaf"
    attrs:
      name: "leaf"
      slug: "leaf"

  - uid: "c2d3e4f5-6071-4b8c-9d0e-1f2a3b4c5d6e"
    type: dcim.device_type
    key: "slug=leaf-switch"
    attrs:
      manufacturer: "f1c8a9d4-2a3b-4c5d-8e9f-0123456789ab"
      model: "leaf-switch"
      slug: "leaf-switch"

  - uid: "a4d6a0c3-4e73-4a76-b216-4d38f8c55f3d"
    type: dcim.site
    key: "slug=fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"

  - uid: "7b8f7a92-8fd0-4667-9a4b-9f3b5c9a4b1a"
    type: dcim.device
    key: "name=leaf01"
    attrs:
      name: "leaf01"
      site: "a4d6a0c3-4e73-4a76-b216-4d38f8c55f3d"
      role: "b1c2d3e4-5f60-4a7b-8c9d-0e1f2a3b4c5d"
      device_type: "c2d3e4f5-6071-4b8c-9d0e-1f2a3b4c5d6e"
      status: "active"

  - uid: "5a1c43a4-4f52-4d07-8a2f-88ad1fbdf8c0"
    type: dcim.interface
    key: "name=eth0"
    attrs:
      name: "eth0"
      device: "7b8f7a92-8fd0-4667-9a4b-9f3b5c9a4b1a"
      if_type: "1000base-t"
      enabled: true

  - uid: "4b8a93d3-6a6d-4ef5-9b04-1de2b8f5b8f2"
    type: dcim.interface
    key: "name=eth1"
    attrs:
      name: "eth1"
      device: "7b8f7a92-8fd0-4667-9a4b-9f3b5c9a4b1a"
      if_type: "1000base-t"
      enabled: true

  - uid: "dc0adf72-3c0b-4c3a-8b18-23a7c0a7c0f1"
    type: ipam.prefix
    key: "prefix=10.0.0.0/24"
    attrs:
      prefix: "10.0.0.0/24"
      site: "a4d6a0c3-4e73-4a76-b216-4d38f8c55f3d"
      description: "FRA1 leaf subnet"

  - uid: "c4a0c0f0-ef8a-4c7f-9b0a-2ff3a4d14fd1"
    type: ipam.ip_address
    key: "address=10.0.0.10/24"
    attrs:
      address: "10.0.0.10/24"
      assigned_interface: "5a1c43a4-4f52-4d07-8a2f-88ad1fbdf8c0"
      description: "leaf01 eth0"
```

## commands

```bash
alembic plan -f /path/to/basic.yaml -o /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN

alembic apply -p /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN --allow-delete
```

## notes

- use uid strings to reference other objects.
- keys are used only for bootstrap or when state is missing.
