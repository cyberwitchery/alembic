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
working_dir: ./adapters/mybackend
env:
  MY_BACKEND_URL: https://backend.example.com
timeout_seconds: 60
setup:
  my_backend_variable_x: 37.0
```

`working_dir:` sets the directory the adapter process runs in (its cwd); when
unset the adapter inherits alembic's own.

`env:` values are passed to the adapter verbatim. the adapter also inherits
alembic's environment, so secrets don't belong in the config file: export them
(e.g. `MY_BACKEND_TOKEN`) and read them from the adapter's environment.

## protocol

all requests include a `version` field (the current protocol version is `1`) and a
`setup` object: the adapter's `setup:` config block, or `null` when there is none
(`{}` is also accepted).

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
  "setup": {},
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
  "setup": {},
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
  "setup": {},
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
    "created_object_fields": ["dcim.site.name"],
    "deprecated_object_types": [],
    "deprecated_object_fields": [],
    "deleted_object_types": [],
    "deleted_object_fields": []
  }
}
```

the report has eight keys, all optional and defaulting to an empty list, so an
adapter that only ever creates can send the four `created_*` keys alone. all eight
are counted back to the operator on a provisioning run (`provision: 1 object types
created, 1 object fields deleted`). the two `deleted_*` lists are also read by the
host: see the gate under `preview_schema`.

### preview_schema

the host calls this at plan time to show what `ensure_schema` would provision,
without writing anything. return the same `ProvisionReport` shape `ensure_schema`
would, or a `null` result if the adapter cannot preview (which the host reports as
`schema preview: unavailable for this backend`). answer it either way: leaving it to
the unknown-method branch fails the built-in `protocol/preview-schema-empty` check.

this report is also the destructive-provisioning gate. before `plan --provision` or
`apply` calls `ensure_schema`, the host previews and refuses the run with
`provisioning would delete schema (N type(s), M field(s)); re-run with --allow-delete`
when `deleted_object_types` or `deleted_object_fields` is non-empty. schema deletes
cascade to their objects on the backend, so an adapter that drops a type without
listing it here takes the objects with it and never prompts. list what you would
delete, even if the same call also creates.

a `null` result skips the gate rather than failing it, so an adapter that cannot
preview provisions unchecked. that is deliberate, and it is the trade for not
implementing the method: if your adapter can delete schema, preview it.

request:

```json
{
  "version": 1,
  "setup": {},
  "method": "preview_schema",
  "schema": { "types": { /* alembic schema */ } }
}
```

response (a report, or `"result": null` when preview is unsupported). this one
would provision one new type and drop one field, so it trips the gate unless the
operator passed `--allow-delete`, and it omits the six keys that are empty:

```json
{
  "ok": true,
  "result": {
    "created_object_types": ["dcim.site"],
    "deleted_object_fields": ["dcim.rack.legacy_id"]
  }
}
```

### capabilities

the host calls this once when it constructs the backend, to learn which side of
the adapter contract the adapter implements. the result carries a `role`:

- `observer`: read-only. the host calls `read` but never `write`; `apply`
  rejects the backend up front.
- `emitter`: write-only. the host calls `write` but never `read`; plain `plan`
  plans every declared object as a create against an empty observation, while
  `import` and `plan --report` reject the backend up front instead of
  observing nothing.
- `adapter`: read+write; the host may call every method.

an adapter that does not answer capabilities (the unknown-method error, or any
other probe failure) defaults to `adapter`, so existing adapters keep working
unchanged. the rust sdk answers it automatically: the trait's default reports
`adapter`, and an emit-only adapter overrides it in one method:

```rust
fn capabilities(&mut self) -> ExternalCapabilities {
    ExternalCapabilities { role: ExternalRole::Emitter }
}
```

request:

```json
{
  "version": 1,
  "setup": {},
  "method": "capabilities"
}
```

response:

```json
{
  "ok": true,
  "result": { "role": "adapter" }
}
```

### errors

when the adapter fails, respond with `ok: false` and still exit `0`. a non-zero
exit is treated as a hard failure and the response on stdout is ignored, so signal
failure through the envelope, not the exit code:

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
inventory and a schema preview each succeed with a right-shaped payload, so an
adapter that errors on every request does not pass. the runner probes
`capabilities` first: a declared emitter is never sent a read by the host, so its
liveness check is an empty write instead of the empty read, and it may answer
`read` with an error. answering `capabilities` itself with the unknown-method
error stays conformant and means the default read+write role.

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

`expect` takes `ok` plus `result` or `error`, both optional. `result` omitted, the
runner only checks the payload shape; present, it compares the returned json
structurally. `error` pins the exact message an `ok: false` case must come back with.
a key that is none of those three is a parse error naming it, and so is a
stray key beside `name`/`request`/`expect`: `result` and `error` are the assertions, so
a typo in one would drop it and report the case as passing. the runner exits `0` when
every check passes, `1` when a check fails, and `2` on a usage or fixtures error, so it
drops straight into ci:

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
