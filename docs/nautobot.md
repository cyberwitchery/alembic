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

these can be provided via environment variables or CLI flags (`--nautobot-url`, `--nautobot-token`).

## attrs mapping

- `attrs` are sent as-is in create/update requests.
- nested references should be provided as alembic uids (string UUIDs). the adapter resolves those
  to nautobot UUIDs before sending requests.

## projection data

- `custom_fields` and `tags` are supported.
- `local_context_data` is currently not implemented for nautobot.
- custom fields are handled via the `custom_fields` attribute on nautobot objects.
- tags are handled as a list of names or slug-like identifiers.

## known limitations

- `local_context_data` support is missing.
- projection proposal (`--projection-propose`) is not yet implemented for nautobot.
- nautobot version 2.x is recommended; some endpoints might differ in older versions.
