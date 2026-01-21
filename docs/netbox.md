# netbox adapter

the netbox adapter maps alembic ir objects to netbox endpoints dynamically. it uses the
`core/object-types` endpoint to resolve a `type` like `dcim.site` into its REST endpoint
and supported feature set.

## object types and endpoints

- the adapter uses `object_types.rest_api_endpoint` for each type.
- if a type has no REST endpoint in netbox, apply/observe will fail.

## attrs mapping

- `attrs` are sent as-is in create/update requests, so they must match the netbox API field names.
- nested references should be provided as alembic uids (string UUIDs). the adapter resolves those
  to backend integer ids before sending requests.
- if a referenced uid cannot be resolved (not in state or created earlier in the same apply),
  apply fails with a missing reference error.

## keys and matching

- keys are used to bootstrap state when no mapping exists.
- keys are structured maps; the adapter uses the schema key fields when observing objects.
- key fields are used as query filters when resolving backend ids for updates/deletes.

## projection data

- `custom_fields`, `tags`, and `local_context_data` are removed from `attrs` during observe and
  stored in the projection payload.
- on apply, projection patches are sent only if the type advertises support via the
  object type `features` field.

## known limitations

- only fields typed as `ref`/`list_ref` are resolved to backend ids during apply.
- netbox endpoints that do not accept patch or projection fields will return errors on apply.
