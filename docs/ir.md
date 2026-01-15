# ir

alembic defines a canonical, vendor-neutral ir for dcim/ipam data. all objects share a common envelope and are strongly typed by `kind`.

## object envelope

every object is represented as:

```yaml
uid: "<uuid>"
kind: dcim.site | dcim.device | dcim.interface | ipam.prefix | ipam.ip_address
key: "<human key>"
attrs: { ... }
x: { "namespace.key": <json value> }
```

- `uid`: stable identifier (uuid). never use backend ids in input files.
- `kind`: canonical type id for the object.
- `key`: human/natural key used for matching when no state mapping exists.
- `attrs`: strongly typed fields for the object kind.
- `x`: extension map for future portability (namespaced keys).

## kinds and attrs (mvp)

### dcim.site

```yaml
attrs:
  name: "FRA1"
  slug: "fra1"
  status: "active" # optional
  description: "..." # optional
```

### dcim.device

```yaml
attrs:
  name: "leaf01"
  site: "<site uid>"
  role: "leaf"
  device_type: "leaf-switch"
  status: "active" # optional
```

### dcim.interface

```yaml
attrs:
  name: "eth0"
  device: "<device uid>"
  if_type: "1000base-t" # optional
  enabled: true # optional
  description: "..." # optional
```

### ipam.prefix

```yaml
attrs:
  prefix: "10.0.0.0/24"
  site: "<site uid>" # optional
  description: "..." # optional
```

### ipam.ip_address

```yaml
attrs:
  address: "10.0.0.10/24"
  assigned_interface: "<interface uid>" # optional
  description: "..." # optional
```

## relationships

references are always by `uid` and are validated in the engine:

- `dcim.device.attrs.site` -> `dcim.site.uid`
- `dcim.interface.attrs.device` -> `dcim.device.uid`
- `ipam.ip_address.attrs.assigned_interface` -> `dcim.interface.uid`
- `ipam.prefix.attrs.site` -> `dcim.site.uid` (optional)

## matching semantics

- primary match: state store mapping (`uid` -> backend id)
- fallback match: `key`

## extension map

`x` is a namespaced map for future portability. keys should be namespaced (e.g. `netbox.custom_field`).
