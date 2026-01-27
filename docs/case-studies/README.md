# case studies

small end-to-end scenarios for alembic.

each case study includes a minimal inventory, optional projection, and commands.
keys are structured maps; alembic canonicalizes them as JSON for matching and sorting.

- `01-basic-dcim-ipam.md`: single site + device + interfaces + prefix + ip
- `02-tenant-vrf-vlan.md`: tenant-scoped vrf/vlan/prefix
- `03-circuits.md`: provider + circuit + termination
- `04-model-to-netbox.md`: model data and apply to netbox
- `05-import-csv.md`: import from csv into netbox via retort
- `06-django-dcim.md`: generate a simple django-based dcim
