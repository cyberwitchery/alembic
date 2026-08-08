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
- a declared `pattern:` on a text-typed field is provisioned as the custom field's
  `validation_regex`, and a declared `format:`, or the format a `uuid`, `cidr`, `prefix`,
  `mac` or `slug` type carries, is provisioned the same way when no `pattern:` overrides
  it, so the backend enforces them too; an existing field is not updated.
- tags the applied objects reference but the backend lacks are created at apply, not
  during schema provisioning. a successful apply lists them in `provision.created_tags`;
  a tag that already existed is not listed. they are created before the ops, so an apply
  that fails afterwards leaves a tag no report names, in that run or the resumed one.

a declared type is provisioned as the nautobot custom field type below. `ref` and
`list_ref` are never provisioned as custom fields.

| declared | nautobot |
| --- | --- |
| `string`, `text`, `uuid`, `time`, `ip_address`, `cidr`, `prefix`, `mac`, `slug` | `text` |
| `int` | `integer` |
| `float` | `json` |
| `bool` | `boolean` |
| `date` | `date` |
| `datetime` | `datetime` |
| `enum` | `select` |
| `list` of `enum` | `multi-select` |
| `json`, `list`, `map` | `json` |

nautobot has no decimal custom field type, so a `float` is stored as `json` to keep it a
number: a text field reads back quoted, which fails validation on `import`.

a `select` is created with one choice per declared value, weighted in declaration order.
choices are written by the create alone, so a field the backend already has keeps the
choices it was created with, like every other property of an existing field. a run that
fails partway through the choices leaves the field created and its remaining choices
unwritten; the next run finds the field present and does not resume them.

a declared `pattern:` or `format:` is not provisioned on a `select`: nautobot enforces
`validation_regex` on text fields only, and the choices are the constraint.

## custom objects

nautobot does not expose a core api for dynamic custom object types. schema types must
exist as nautobot content types (core or app-provided). unknown types will error on
observe/apply.
