# case study: evaluate dcim systems without the prefill

## goal

you are choosing a dcim/ipam system for a new fabric and want to try a couple of
candidates on your own data before committing. the tedious part is the prefill:
standing up your sites, devices, interfaces, and addressing by hand in each system
just to see how it feels, then doing it all again in the next one.

instead, describe the fabric once as a vendor-neutral model and let alembic stand
it up into each candidate. this walkthrough targets two, netbox and nautobot, from
a single source of truth. when you pick one, the model is already your source of
truth; the other stays reproducible from the same file.

## the model

a trimmed fabric, one site, one device, one interface, one address, authored once
and kept vendor-neutral. the full file is
[`examples/walkthroughs/eval-fabric.yaml`](../../examples/walkthroughs/eval-fabric.yaml);
it names the ip's interface assignment `assigned_interface`:

```yaml
  - type: dcim.site
    key: {slug: "fra1"}
    attrs: {name: "Frankfurt DC1", slug: "fra1"}
  # ...
  - type: ipam.ip_address
    key: {address: "10.0.0.10/24"}
    attrs:
      address: "10.0.0.10/24"
      assigned_interface: "5a1c43a4-..."  # the eth0 interface
```

## where the two systems disagree

the model is close to both systems but identical to neither:

- **netbox** keeps `dcim.site` (keyed by slug) but names an ip's interface
  assignment `assigned_object`, a generic foreign key.
- **nautobot** models a site as `dcim.location`, keyed by its human name with no
  slug, and a device points at `location`, not `site`. the interface and ip keep
  their neutral names through the map.

so each candidate gets its own `map`: reshape the handful of fields it names
differently, and `match: "*" emit: passthrough` carries the rest.

## stand up netbox

netbox needs a single rename
([`eval-fabric-netbox.yaml`](../../examples/walkthroughs/eval-fabric-netbox.yaml)):

```yaml
rules:
  - name: rename-assignment
    match: ipam.ip_address
    emit:
      type: ipam.ip_address
      key: {address: "${key.address}"}
      attrs:
        address: "${attrs.address}"
        assigned_object: "${attrs.assigned_interface}"
  - name: rest
    match: "*"
    emit: passthrough
```

```bash
alembic map -f examples/walkthroughs/eval-fabric.yaml \
  --spec examples/walkthroughs/eval-fabric-netbox.yaml -o /tmp/netbox.json
alembic plan  -f /tmp/netbox.json -o /tmp/plan.json --backend-config backend-netbox.yaml
alembic apply -p /tmp/plan.json --backend-config backend-netbox.yaml
```

## stand up nautobot

nautobot renames the site type and its key, and the device's relation to it
([`eval-fabric-nautobot.yaml`](../../examples/walkthroughs/eval-fabric-nautobot.yaml)):

```yaml
rules:
  - name: sites-to-locations
    match: dcim.site
    emit:
      type: dcim.location
      key: {name: "${attrs.name}"}
      attrs: {name: "${attrs.name}"}
  - name: devices
    match: dcim.device
    emit:
      type: dcim.device
      key: {name: "${key.name}"}
      attrs: {name: "${attrs.name}", location: "${attrs.site}", role: "${attrs.role}"}
  - name: rest
    match: "*"
    emit: passthrough
```

`dcim.site` becomes `dcim.location` keyed by the human name, the device's `site`
relation becomes `location`, and the interface and ip pass through under their
neutral names. the site rule is 1:1, so map rewires
the device's relation from the old site uid to the new location uid.

```bash
alembic map -f examples/walkthroughs/eval-fabric.yaml \
  --spec examples/walkthroughs/eval-fabric-nautobot.yaml -o /tmp/nautobot.json
alembic plan  -f /tmp/nautobot.json -o /tmp/plan.json --backend-config backend-nautobot.yaml
alembic apply -p /tmp/plan.json --backend-config backend-nautobot.yaml
```

## notes

- the same source of truth reached both systems; the only per-backend artefact is
  a small map naming its differences. add a third candidate by writing a third
  map, not a third inventory.
- `map` inherits identity, so netbox's `dcim.site` and nautobot's
  `dcim.location` are the same logical object under one uid, materialized in
  two vocabularies; each backend still assigns its own backend ids on apply,
  and each backend's state file remembers its own.
- these maps reshape only what the two systems name differently. a richer model
  (device types, statuses) grows the maps, not the source model; the
  nautobot-to-netbox case study shows a status modelled as a reference on one side
  and a plain string on the other.
