# dknog 2026 lab: netbox + custom django backend

this note is the practical companion to the 10-minute talk demo.

## 1) netbox path (native backend + custom fields/tags)

goal: show that alembic can converge native netbox objects and propose/create projection-backed custom fields/tags.

pre-req:

- reachable netbox api
- `NETBOX_URL` and `NETBOX_TOKEN` exported

run:

```bash
./scripts/demo_netbox_custom_fields.sh
```

what it demonstrates:

- schema-first validation (`core-v1.yaml`)
- planning against live netbox state
- projection proposal workflow (`--projection-propose`) for custom fields/tags
- interactive apply path

notes:

- this does **not** create brand new netbox object models; it creates/uses supported metadata such as custom fields and tags.
- for truly new domains, use your own schema types and target a backend that supports them.

## 1b) netbox vm + custom hypervisor metadata

goal: reuse native netbox `virtualization.virtualmachine` while layering a custom "hypervisor model"
into custom fields/tags through projection.

run:

```bash
./scripts/demo_netbox_vm_hypervisor.sh
```

inputs:

- `examples/talks/dknog-2026/netbox-vm-hypervisor.yaml`
- `examples/talks/dknog-2026/projection-netbox-vm-hypervisor.yaml`

what it demonstrates:

- custom model keys (`model.hypervisor_*`, `model.workload_domain`) mapped to NetBox custom fields
- model tags projected to NetBox tags
- proposal flow creates missing custom fields/tags (`--projection-propose`)
- convergence reaches zero diff after apply

## 2) custom domain path (rendered django backend)

goal: show model ownership for types that are not universal across dcim/ipam systems.

example model:

- `examples/talks/dknog-2026/extended-model.yaml`
- includes `virt.hypervisor` and `virt.vm`

run:

```bash
python3 -m pip install django djangorestframework
rm -rf /tmp/dknog-django
cargo run -p alembic-cli -- cast django \
  -f examples/talks/dknog-2026/extended-model.yaml \
  -o /tmp/dknog-django \
  --project dknog_demo \
  --app model_api \
  --python python3 \
  --no-migrate
```

expected outputs:

- generated models in `/tmp/dknog-django/model_api/generated_models.py`
- generated API routes in `/tmp/dknog-django/model_api/generated_urls.py`

this gives you a concrete "custom backend" story for the talk: define the model in alembic, render an API/data layer when the dcim target does not natively support your domain types.
