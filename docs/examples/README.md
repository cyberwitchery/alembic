# examples

minimal, single-topic walkthroughs: a small model and the commands to converge
it. for worked end-to-end scenarios, see [`../case-studies`](../case-studies).

each links its inventory in [`examples/walkthroughs`](../../examples/walkthroughs);
keys are structured maps that alembic canonicalizes as JSON for matching.

- `01-basic-dcim-ipam.md`: a site, device, interfaces, prefix, and ip, with a
  `map` step to netbox's field names.
- `02-custom-object-type.md`: declare a type netbox does not have and let alembic
  provision it as a netbox custom object.
- `03-django-dcim.md`: generate a runnable django app from a model with the
  write-only django backend.
