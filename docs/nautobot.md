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
  a shared field is only ever one nautobot already attaches to both types, because
  alembic's own create carries a single content type, so a field nautobot holds
  against only one of the two declared types is not converged for the other and
  that declaration's create is rejected as a duplicate key. the schema preview
  reports the same updates under `provision.updated_fields`, each naming the
  property and both sides (`dcim.site.tier: validation_regex "" -> "^[a-z]+$"`),
  so an update to a field this run did not create is read before it is written.
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
choices converge additively on a field the backend already has: a declared value the
field lacks is posted at the weight its declared position implies, which can tie an
existing choice's weight, and nautobot orders those itself. a choice the backend has and
the model does not declare is left alone, because core rejects an undeclared enum value
before a write reaches nautobot: an extra one is inert, a missing one is fatal. two types
sharing one field must declare the same values in the same order, or the run fails naming
both. a run that failed partway through the choices posts the rest on the next run.

a declared `pattern:` or `format:` is not provisioned on a `select`: nautobot enforces
`validation_regex` on text fields only, and the choices are the constraint.

## custom objects

nautobot does not expose a core api for dynamic custom object types. schema types must
exist as nautobot content types (core or app-provided). unknown types will error on
observe/apply.
