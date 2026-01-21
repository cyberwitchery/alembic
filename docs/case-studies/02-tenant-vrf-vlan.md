# case study: tenant + vrf + vlan + prefix

## goal

model multi-tenant ipam with vrfs and vlans.

## inventory

```yaml
objects:
  - uid: "11111111-1111-1111-1111-111111111111"
    type: tenancy.tenant_group
    key: "slug=customers"
    attrs:
      name: "customers"
      slug: "customers"

  - uid: "22222222-2222-2222-2222-222222222222"
    type: tenancy.tenant
    key: "slug=acme"
    attrs:
      name: "acme"
      slug: "acme"
      group: "11111111-1111-1111-1111-111111111111"

  - uid: "33333333-3333-3333-3333-333333333333"
    type: ipam.vrf
    key: "name=acme-vrf"
    attrs:
      name: "acme-vrf"
      rd: "65000:10"
      tenant: "22222222-2222-2222-2222-222222222222"

  - uid: "44444444-4444-4444-4444-444444444444"
    type: ipam.vlan_group
    key: "name=acme-vlans"
    attrs:
      name: "acme-vlans"
      slug: "acme-vlans"
      scope_type: "tenancy.tenant"
      scope_id: "22222222-2222-2222-2222-222222222222"

  - uid: "55555555-5555-5555-5555-555555555555"
    type: ipam.vlan
    key: "vid=100"
    attrs:
      name: "acme-frontend"
      vid: 100
      group: "44444444-4444-4444-4444-444444444444"
      tenant: "22222222-2222-2222-2222-222222222222"

  - uid: "66666666-6666-6666-6666-666666666666"
    type: ipam.prefix
    key: "prefix=10.10.0.0/24"
    attrs:
      prefix: "10.10.0.0/24"
      vrf: "33333333-3333-3333-3333-333333333333"
      vlan: "55555555-5555-5555-5555-555555555555"
      tenant: "22222222-2222-2222-2222-222222222222"
```

## commands

```bash
alembic plan -f /path/to/tenant.yaml -o /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN

alembic apply -p /tmp/plan.json \
  --netbox-url http://localhost:8000 --netbox-token $NETBOX_TOKEN --allow-delete
```

## notes

- tenant references use uids.
- ensure netbox supports the fields you set in attrs for your version.
