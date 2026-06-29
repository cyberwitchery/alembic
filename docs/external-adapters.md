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
        if let Some(x) = configuration
            .get("my_backend_variable_x")
            .and_then(serde_yaml::Value::as_f64) {
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
        Ok(alembic_engine::ApplyReport::default())
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

## conformance testing

`alembic-adapter-test` is a standalone runner that checks an adapter executable
against this protocol. it ships alongside alembic, so adapter authors do not need
a rust toolchain. point it at the adapter, whose arguments follow `--`:

```console
alembic-adapter-test -- ./alembic-adapter-mybackend --verbose
```

the built-in checks need no fixtures. they confirm the backend-independent
behaviour: malformed json, an unsupported version, and an unknown method each
produce a structured error; the process exits 0 within the timeout after writing
exactly one json document (surrounding whitespace and multi-line json are fine,
logs on stdout are not); the envelope is consistent; and a valid read of an empty
inventory succeeds with a right-shaped payload, so an adapter that errors on every
request does not pass.

to exercise `read`, `write`, and `ensure_schema` against your own fake or
disposable backend, pass `--cases` a file or directory of cases. a case is a
complete request and an expectation:

```json
{
  "name": "read empty inventory",
  "request": {
    "version": 1,
    "setup": {},
    "method": "read",
    "schema": { "types": {} },
    "types": [],
    "state": { "mappings": {} }
  },
  "expect": { "ok": true, "result": [] }
}
```

`result` is optional: omitted, the runner only checks the payload shape; present,
it compares the returned json structurally. the runner exits `0` when every check
passes, `1` when a check fails, and `2` on a usage or fixtures error, so it drops
straight into ci:

```console
alembic-adapter-test --cases tests/alembic -- ./alembic-adapter-mybackend
```

```yaml
- run: alembic-adapter-test --cases tests/alembic -- ./alembic-adapter-mybackend
```

a worked python adapter and its cases live in
`crates/alembic-adapter-test/examples/`. the canonical request/response pairs in
`fixtures/external_protocol/` document the same protocol as plain json for sdks
in other languages.
