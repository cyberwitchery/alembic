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
- provisioning creates missing custom fields on apply and converges existing ones;
  the schema preview reports both.
- a declared `pattern:` on a text-typed field is provisioned as the custom field's
  `validation_regex`, and a declared `format:`, or the format a `uuid`, `cidr`, `prefix`,
  `mac` or `slug` type carries, is provisioned the same way when no `pattern:` overrides
  it, so the backend enforces them too. a field the backend already has is
  converged onto the properties alembic declares: `description` and
  `validation_regex` are patched when they differ from the schema, and `required`
  is set when the schema declares it. a `required: false` reads the same as an
  omitted one, so `required` is only ever tightened, never relaxed. nothing else is
  written -- the field's type is left alone, and a property the schema does not
  declare keeps whatever the backend holds. one backend field can carry several
  content types: when two declared types share one, a property only one of them
  declares is taken as declared, since the other is silent about it rather than
  opposed to it, so the silent type's objects are held to that constraint at the
  backend too. two that declare the same property differently cannot both be
  honoured, so the run fails naming both rather than writing two patches to one
  field. the type is checked the same way though it is never written, since the
  field has only one. a declared `required: false` is silent in that sense, so
  `required` is their union: a field the two disagree about is required for both.
  a shared field is only ever one netbox already attaches to both types, because
  alembic's own create carries a single content type, so a field netbox holds
  against only one of the two declared types is not converged for the other and
  that declaration's create is rejected as a duplicate name. the schema preview
  reports the same updates under `provision.updated_fields`.
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
