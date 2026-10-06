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

a trimmed fabric, one site, one device with its role and type, one interface,
one address, authored once and kept vendor-neutral. the full file is
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
  slug and typed by a `dcim.locationtype`, and a device points at `location`,
  not `site`. a device role is a generic `extras.role`, its type names drop the
  underscore (`dcim.devicetype`, `ipam.ipaddress`), and an ip reaches its
  interface through a separate `ipam.ipaddresstointerface` object and needs a
  parent prefix. locations, devices, interfaces, prefixes and ips each require a
  status.

so each candidate gets its own `map`: reshape what it names differently, and
`match: "*" emit: passthrough` carries whatever already fits.

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

nautobot reshapes every type: it renames the site type and its key and the
device's relation to it, splits the ip's assignment into its own object, and
adds the status, location type and prefix nautobot requires
([`eval-fabric-nautobot.yaml`](../../examples/walkthroughs/eval-fabric-nautobot.yaml)):

```yaml
objects:
  - type: extras.status
    key: {name: Active}
    uid: "3915bcde-33d3-4332-9930-9e3684a9d859"
  - type: dcim.locationtype
    key: {name: Site}
    attrs: {name: Site, content_types: ["dcim.device"]}
    uid: {v5: {type: dcim.locationtype, stable: site}}
  - type: ipam.prefix
    key: {prefix: "10.0.0.0/24"}
    attrs: {prefix: "10.0.0.0/24", status: "3915bcde-33d3-4332-9930-9e3684a9d859"}
    uid: {v5: {type: ipam.prefix, stable: "10.0.0.0/24"}}
rules:
  - name: sites-to-locations
    match: dcim.site
    uids:
      location_type: {v5: {type: dcim.locationtype, stable: site}}
    emit:
      type: dcim.location
      key: {name: "${attrs.name}"}
      attrs:
        name: "${attrs.name}"
        location_type: "${uids.location_type}"
        status: "3915bcde-33d3-4332-9930-9e3684a9d859"
  - name: devices
    match: dcim.device
    emit:
      type: dcim.device
      key: {name: "${key.name}"}
      attrs:
        name: "${attrs.name}"
        location: "${attrs.site}"
        role: "${attrs.role}"
        device_type: "${attrs.device_type}"
        status: "3915bcde-33d3-4332-9930-9e3684a9d859"
  # ... 1:1 rules for the role, manufacturer, device type and interface
  - name: ip-addresses
    match: ipam.ip_address
    uids:
      parent: {v5: {type: ipam.prefix, stable: "10.0.0.0/24"}}
    emit:
      - type: ipam.ipaddress
        uid: "${uid}"
        key: {address: "${key.address}"}
        attrs:
          address: "${attrs.address}"
          status: "3915bcde-33d3-4332-9930-9e3684a9d859"
          parent: "${uids.parent}"
      - type: ipam.ipaddresstointerface
        uid: {v5: {type: ipam.ipaddresstointerface, stable: "${uid}#interface"}}
        key: {ip_address: "${uid}", interface: "${attrs.assigned_interface}"}
        attrs: {ip_address: "${uid}", interface: "${attrs.assigned_interface}"}
```

`dcim.site` becomes `dcim.location` keyed by the human name and typed by the one
`Site` location type the spec's `objects:` declares, which allows devices. no
source object models that type, so the location reaches it by deriving the same
`v5` uid. the `Active` status is nautobot's own: declared by key alone, plan
adopts it rather than creating one, and its literal uid lets both the rules and
the prefix, which renders with no vars, point at it. the device's `site`
relation becomes `location`. the site, role and type rules are 1:1 and keep
their source uids, so the device's refs stay valid. the ip rule emits two
objects, so it names each uid: the ip keeps its own, and the link derives one
from it. the ip names
the prefix as its `parent`, reached through the prefix's `v5` uid.

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
- these maps reshape only what the two systems name differently. the source
  model carries what both need, such as a device's role and type; the
  nautobot-to-netbox case study shows a status modelled as a reference on one side
  and a plain string on the other.
