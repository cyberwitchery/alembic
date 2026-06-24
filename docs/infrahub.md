# infrahub adapter

the infrahub adapter reads and writes via the graphql api. alembic type names
are mapped to infrahub graphql object names (for example `infra.device` ->
`InfraDevice`).

## schema expectations

- the adapter can provision missing types/fields when schema push is configured.
- schema provisioning generates an infrahub schema file and applies it via either
  `infrahubctl schema load` or the repository processing hook.
- types should be namespaced (for example `infra.device`), which maps to the
  infrahub graphql type `InfraDevice`. non-namespaced type names are still
  supported for read/write, but provisioning requires a namespace.

schema provisioning is configured in the backend config file, for example:

```yaml
backend: infrahub
url: https://infrahub.example.com
token: infrahub_xxx_replace_me
schema:
  mode: infrahubctl
  schema_path: ./schema/alembic.generated.yaml
  infrahubctl_path: ./scripts/infrahubctl
```

## attrs mapping

- attribute routing is schema-driven:
  - string-like types (`string`, `text`, `uuid`, `date`, `datetime`, `time`, `ip`, `cidr`,
    `prefix`, `mac`, `slug`, `enum`) map to `TextAttribute` inputs.
  - `int`/`float` map to `NumberAttribute` inputs.
  - `bool` maps to `CheckboxAttribute` inputs.
  - `json`/`map` map to `JSONAttribute` inputs.
  - `list` maps to `ListAttribute` inputs.
- refs should be alembic uids (string UUIDs). the adapter resolves them to infrahub ids
  via state and sends `RelatedNodeInput { id }`.

## keys and matching

- key fields are used to derive the object key when observing.
- updates/deletes fall back to a full type scan to resolve backend ids when state
  is missing.

## known limitations

- float handling relies on `NumberAttribute` (infrahub uses `BigInt`). ensure your
  schema types align if floats are required.
- repository mode updates `.infrahub.yml` and writes the schema file, but does not
  commit or push git changes.
