# nautobot adapter

the nautobot adapter maps alembic ir objects to nautobot REST endpoints. it uses the
`extras/content-types` endpoint to resolve a `type` like `dcim.device` into its REST endpoint.

## object types and endpoints

- the adapter resolves `type` names (e.g. `dcim.device`) to nautobot content types.
- it maps these to endpoints like `/api/dcim/devices/`.
- unlike netbox, nautobot uses UUIDs for all identifiers. alembic stores these as string
  `BackendId` values.

## credentials

the adapter requires:
- `NAUTOBOT_URL`: the base URL of the nautobot instance.
- `NAUTOBOT_TOKEN`: a valid API token.

these can be provided via environment variables or a backend config file, for example:

```yaml
backend: nautobot
url: https://nautobot.example.com
token: nautobot_xxx_replace_me
```

## attrs mapping

- field routing is schema-driven: native fields are sent directly, and known custom fields are
  bundled under `_custom_field_data`.
- `tags` in `attrs` should be a list of strings; the adapter expands them to nautobot tag inputs.
- nested references should be provided as alembic uids (string UUIDs). the adapter resolves those
  to nautobot UUIDs before sending requests, including refs nested in list and map fields.

## custom fields and tags

- observe flattens `_custom_field_data` and `tags` into `attrs` for diffing and import.
- on apply, custom fields and tags are only sent when the object type advertises support
  via the `features` set.
- provisioning creates missing custom fields on apply; the schema preview reports them.

## custom objects

nautobot does not expose a core api for dynamic custom object types. schema types must
exist as nautobot content types (core or app-provided). unknown types will error on
observe/apply.
