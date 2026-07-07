# examples

minimal, single-topic walkthroughs: a small inventory and the commands to converge
it. for worked end-to-end scenarios, see [`../case-studies`](../case-studies).

keys are structured maps; alembic canonicalizes them as JSON for matching and sorting.

- `01-basic-dcim-ipam.md`: a site, device, interfaces, prefix, and ip, with a
  `map` step to netbox's field names.
- `02-tenant-vrf-vlan.md`: tenant-scoped vrf/vlan/prefix.
- `03-circuits.md`: provider + circuit type + circuit + termination.
- `04-django-dcim.md`: generate a simple django-based dcim from a model.
