# case study: migrate from nautobot to netbox

## goal

move an inventory out of nautobot and into netbox. the two backends do not share
a model: nautobot 2.x organizes sites as `dcim.location` keyed by a human name
with no slug, while netbox uses `dcim.site` with a required `slug`, and device
relations point at `location` in one and `site` in the other. so this is not a
straight export/import: there is a translation step in the middle, which is what
`map` is for.

the flow is `import` (nautobot to ir), `map` (ir to ir), `plan` + `apply`
(ir to netbox).

## 1) import from nautobot

```bash
alembic import -o nautobot-ir.yaml \
  --backend-config backend-nautobot.yaml \
  -f schema-nautobot.yaml
```

`-f` is an inventory whose `schema` declares the nautobot-shaped types to observe;
import writes the observed objects to `nautobot-ir.yaml`. a trimmed example of the
result:

```yaml
schema:
  types:
    extras.status:
      key:
        name: { type: string }
    dcim.location:
      key:
        name: { type: string }
      fields:
        status: { type: ref, target: extras.status }
    dcim.device:
      key:
        name: { type: slug }
      fields:
        status: { type: ref, target: extras.status }
        role: { type: string }
        location: { type: ref, target: dcim.location }
objects:
  - uid: 99999999-9999-9999-9999-999999999999
    type: extras.status
    key:
      name: Active
  - uid: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
    type: dcim.location
    key:
      name: "Frankfurt DC1"
    attrs:
      status: 99999999-9999-9999-9999-999999999999
  - uid: bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
    type: dcim.device
    key:
      name: leaf01
    attrs:
      status: 99999999-9999-9999-9999-999999999999
      role: leaf
      location: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
```

note `status` is a *reference* to an `extras.status` object (this is how nautobot
models status), not a plain string. netbox wants a status string, so the map
follows the reference with a lookup.

## 2) map into netbox's vocabulary

the map declares the netbox-shaped target schema and the rules that reshape the
nautobot objects into it.

```yaml
schema:
  types:
    dcim.site:
      key:
        slug: { type: slug }
      fields:
        name: { type: string }
        slug: { type: slug }
        status: { type: string }
    dcim.device:
      key:
        name: { type: slug }
      fields:
        status: { type: string }
        role: { type: string }
        site: { type: ref, target: dcim.site }
rules:
  # dcim.location -> dcim.site, deriving the slug netbox requires from the
  # location's human name, and resolving the status reference to a string.
  - name: locations-to-sites
    match: "dcim.location"
    lookups:
      status_name: { ref: "${attrs.status}", get: "key.name" }
    emit:
      type: dcim.site
      key:
        slug: "${key.name|slug}"
      attrs:
        name: "${key.name}"
        slug: "${key.name|slug}"
        status: "${lookup.status_name|lower}"

  # devices keep their type but the `location` relation becomes `site`. because
  # the rule above is a 1:1 rename, map rewrites this ref from the old
  # dcim.location uid to the new dcim.site uid automatically.
  - name: devices
    match: "dcim.device"
    lookups:
      status_name: { ref: "${attrs.status}", get: "key.name" }
    emit:
      type: dcim.device
      key:
        name: "${key.name}"
      attrs:
        status: "${lookup.status_name|lower}"
        role: "${attrs.role}"
        site: "${attrs.location}"
```

run it:

```bash
alembic map -f nautobot-ir.yaml --spec map-nautobot-to-netbox.yaml -o netbox-ir.json
```

the result is in netbox's vocabulary. the location's name `Frankfurt DC1` becomes
a site with slug `frankfurt-dc1`, and the device's `site` ref points at that new
site:

```json
{
  "type": "dcim.site",
  "key": { "slug": "frankfurt-dc1" },
  "attrs": { "name": "Frankfurt DC1", "slug": "frankfurt-dc1", "status": "active" }
}
{
  "type": "dcim.device",
  "key": { "name": "leaf01" },
  "attrs": { "role": "leaf", "site": "<new dcim.site uid>", "status": "active" }
}
```

## 3) apply to netbox

```bash
alembic plan -f netbox-ir.json -o /tmp/plan.json \
  --backend-config backend-netbox.yaml

alembic apply -p /tmp/plan.json \
  --backend-config backend-netbox.yaml
```

## notes

- uids are recomputed from the target identity (`dcim.site` + slug), so they are
  stable across runs without carrying nautobot's identity over. for a fresh
  netbox this is what you want; netbox assigns its own backend ids on apply.
- the slug derivation changes the key, and therefore the uid, of every site. the
  device ref still lands correctly because map rewrites references in a second
  pass after every object's new identity is known.
- nautobot models `status` as a reference to a status object, while netbox wants
  a status string. the `lookups` block follows that reference, reads the status
  object's name, and the `lower` transform normalizes it (`Active` -> `active`),
  so a reference-valued field becomes the plain value netbox expects.
