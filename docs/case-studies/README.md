# case studies

small end-to-end scenarios for alembic.

each case study includes a minimal inventory and commands.
keys are structured maps; alembic canonicalizes them as JSON for matching and sorting.

- `01-basic-dcim-ipam.md`: single site + device + interfaces + prefix + ip
- `02-tenant-vrf-vlan.md`: tenant-scoped vrf/vlan/prefix
- `03-circuits.md`: provider + circuit + termination
- `04-model-to-netbox.md`: model data and apply to netbox
- `06-django-dcim.md`: generate a simple django-based dcim
- `07-nautobot-to-netbox.md`: migrate between backends with a `map` translation step
