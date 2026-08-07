# netbox adapter

the netbox adapter maps alembic ir objects to netbox endpoints dynamically. it uses the
`core/object-types` endpoint to resolve a `type` like `dcim.site` into its REST endpoint
and supported feature set.

## object types and endpoints

- the adapter uses `object_types.rest_api_endpoint` for each type.
- if a type has no REST endpoint in netbox, apply/observe will fail unless the
  netbox custom objects plugin is installed (see below).

## attrs mapping

- field routing is schema-driven: native fields are sent directly, and known custom fields are
  bundled under `custom_fields`.
- `tags` in `attrs` should be a list of strings; the adapter expands them to netbox tag inputs.
- nested references should be provided as alembic uids (string UUIDs). the adapter resolves those
  to backend integer ids before sending requests, including refs nested in list and map fields.
- if a referenced uid cannot be resolved (not in state or created earlier in the same apply),
  apply fails with a missing reference error.

## keys and matching

- keys are used to bootstrap state when no mapping exists.
- keys are structured maps; the adapter uses the schema key fields when observing objects.
- key fields are used as query filters when resolving backend ids for updates/deletes.
- key matching uses the canonical JSON form of the key map.

## custom fields and tags

- observe flattens `custom_fields` and `tags` into `attrs` for diffing and import.
- on apply, custom fields and tags are only sent when the object type advertises support
  via the `features` set.
- provisioning creates missing custom fields on apply; the schema preview reports them.
- a declared `pattern:` on a text-typed field is provisioned as the custom field's
  `validation_regex`, so the backend enforces it too; an existing field is not updated.
- tags the applied objects reference but the backend lacks are created at apply, not
  during schema provisioning. a successful apply lists them in `provision.created_tags`;
  a tag that already existed is not listed. they are created before the ops, so an apply
  that fails afterwards leaves a tag no report names, in that run or the resumed one.

a declared type is provisioned as the netbox custom field type below. `ref` and `list_ref`
are never provisioned as custom fields; see custom objects.

| declared | netbox |
| --- | --- |
| `string`, `uuid`, `time`, `ip_address`, `cidr`, `prefix`, `mac`, `slug`, `enum` | `text` |
| `text` | `longtext` |
| `int` | `integer` |
| `float` | `decimal` |
| `bool` | `boolean` |
| `date` | `date` |
| `datetime` | `datetime` |
| `json`, `list`, `map` | `json` |

## custom objects (netbox custom objects plugin)

if the schema includes types that are not present in netbox core object types, the adapter
will provision them as custom objects on `apply` using the netbox custom objects plugin:

- creates custom object types for missing schema types
- creates custom object type fields for schema keys + fields
- applies objects via `/api/plugins/custom-objects/<custom-object-type>/`

this requires the `netbox-custom-objects` plugin and its REST API endpoints to be available.

## known limitations

- netbox endpoints that do not accept patch or custom field payloads will return errors on apply.
