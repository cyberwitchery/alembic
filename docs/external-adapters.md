# external adapters

alembic can delegate adapter operations to an external process. this is useful when you
want to integrate a backend without linking it into the main binary. the adapter is a
standalone executable that reads a single json request from stdin and writes a single
json response to stdout.

## config

```yaml
backend: external
command: ./bin/alembic-adapter-mybackend
args: ["--verbose"]
env:
  MY_BACKEND_URL: https://backend.example.com
  MY_BACKEND_TOKEN: $TOKEN
timeout_seconds: 60
setup:
  my_backend_variable_x: 37.0
```

## protocol

all requests include a `version` field. the current protocol version is `1`.

## rust sdk

`alembic-engine` ships a small helper module for external adapters that removes
request/response boilerplate and guarantees well-formed responses:

```rust
use alembic_engine::external::{ExternalAdapter, ExternalObject};
use alembic_engine::alembic_external_main;

struct MyAdapter;

impl ExternalAdapter for MyAdapter {
    fn setup(&mut self, configuration: &serde_yaml::Value) -> Result<()> {
        if let Some(x) = configuration.get("my_backend_variable_x").and_then(serde_yaml::Value::as_str) {
            ...
        }
        Ok(())
    }

    fn read(
        &mut self,
        schema: &alembic_core::Schema,
        types: &[alembic_core::TypeName],
        state: &alembic_engine::StateData,
    ) -> anyhow::Result<Vec<ExternalObject>> {
        let _ = (schema, types, state);
        Ok(Vec::new())
    }

    fn write(
        &mut self,
        _schema: &alembic_core::Schema,
        _ops: &[alembic_engine::Op],
        _state: &alembic_engine::StateData,
    ) -> anyhow::Result<alembic_engine::ApplyReport> {
        Ok(alembic_engine::ApplyReport { applied: Vec::new() })
    }
}

alembic_external_main!(MyAdapter);
```

for more control, call `run_external_adapter()` directly and build custom
`ExternalResponse` values.

### read

request:

```json
{
  "version": 1,
  "method": "read",
  "schema": { "types": { /* alembic schema */ } },
  "types": ["dcim.site", "dcim.device"],
  "state": { "mappings": { /* uid -> backend id */ } }
}
```

response:

```json
{
  "ok": true,
  "result": [
    {
      "type_name": "dcim.site",
      "key": { "name": "site-a" },
      "attrs": { "name": "Site A" },
      "backend_id": "site-1"
    }
  ]
}
```

### write

request:

```json
{
  "version": 1,
  "method": "write",
  "schema": { "types": { /* alembic schema */ } },
  "ops": [ /* plan ops */ ],
  "state": { "mappings": { /* uid -> backend id */ } }
}
```

response:

```json
{
  "ok": true,
  "result": {
    "applied": [
      {
        "uid": "00000000-0000-0000-0000-000000000001",
        "type_name": "dcim.site",
        "backend_id": "site-1"
      }
    ]
  }
}
```

### ensure_schema

request:

```json
{
  "version": 1,
  "method": "ensure_schema",
  "schema": { "types": { /* alembic schema */ } }
}
```

response:

```json
{
  "ok": true,
  "result": {
    "created_fields": ["field1"],
    "created_tags": [],
    "created_object_types": ["dcim.site"],
    "created_object_fields": ["dcim.site.name"]
  }
}
```

### errors

when the adapter fails, respond with `ok: false`:

```json
{
  "ok": false,
  "error": "explain what went wrong"
}
```
