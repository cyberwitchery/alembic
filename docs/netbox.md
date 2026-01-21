# netbox adapter

the netbox adapter maps alembic ir to netbox api calls using the `netbox` crate.

## required netbox objects

alembic expects these objects to exist in netbox (by natural key):

- device roles (matched by `name`)
- device types (matched by `model`)
- sites (matched by `slug`)

if these are missing, create them in netbox before running `alembic plan`.

## mapping summary (mvp)

- `dcim.site`
  - create/update via `WritableSiteRequest` / `PatchedWritableSiteRequest`
  - matching by `slug`
  - projected custom fields/tags patched when configured

- `dcim.device`
  - create/update via `CreateDeviceRequest` / `UpdateDeviceRequest`
  - site resolved via state store
  - role/type resolved by lookup
  - matching by `name`
  - projected custom fields/tags/local context patched when configured

- `dcim.interface`
  - create/update via `WritableInterfaceRequest` / `PatchedWritableInterfaceRequest`
  - device resolved via state store (device name used for reference)
  - limited interface type support (see below)
  - projected custom fields/tags patched when configured

- `ipam.prefix`
  - create/update via `CreatePrefixRequest` / `UpdatePrefixRequest`
  - site optional (mapped via scope_type/scope_id)
  - projected custom fields/tags patched when configured

- `ipam.ip_address`
  - create/update via `CreateIpAddressRequest` / `UpdateIpAddressRequest`
  - assigned interface optional
  - projected custom fields/tags patched when configured

## interface types

supported in the adapter:

- `1000base-t`
- `virtual`
- `bridge`
- `lag`

other values return an error. extend `interface_type_from_str` if you need more.

## known limitations

- ip -> interface assignment is only observed when netbox returns `assigned_object_type == dcim.interface`.
- projection-only attrs are ignored for diffing when a projection spec is supplied.
- generic objects are skipped with a warning in apply.
- projection proposal can create missing custom fields and tags when enabled.
