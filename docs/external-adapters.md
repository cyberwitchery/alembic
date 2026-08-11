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

the result may also carry a `provision` key, in the same shape `ensure_schema`
returns, for anything the write itself provisioned. this is for what only the ops
reveal: the built-in netbox and nautobot adapters create the tags their objects
reference here, since a schema pass cannot know them. the host merges it with the
`ensure_schema` report, so fill only the categories your write actually created.

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
    "updated_fields": [],
    "created_tags": [],
    "created_object_types": ["dcim.site"],
    "created_object_fields": ["dcim.site.name"],
    "updated_object_fields": [],
    "deprecated_object_types": [],
    "deprecated_object_fields": [],
    "deleted_object_types": [],
    "deleted_object_fields": []
  }
}
```

every key of the report is optional and defaults to an empty list, so an adapter
that only ever creates can send the `created_*` keys alone. each of them is
counted back to the operator on a provisioning run (`provision: 1 object types
created, 1 object fields deleted`), and the `updated_*`, `deprecated_*` and
`deleted_*` entries are named under that line, one per entry, since they write to
backend state the run did not create. the `deleted_*` lists are also read by the
host: see the gate under `preview_schema`. `created_tags` may also be answered from
`write` instead, when the tags come from the ops rather than the schema.

answer the method even when you provision nothing, with an empty report: apply
propagates the error otherwise, and leaving it to the unknown-method branch fails
the built-in `protocol/ensure-schema-empty` check.

### preview_schema

the host calls this at plan time to show what `ensure_schema` would provision,
without writing anything. return the same `ProvisionReport` shape `ensure_schema`
would, or a `null` result if the adapter cannot preview (which the host reports as
`schema preview: unavailable for this backend`). answer it either way, unless you
declare the `observer` role: leaving it to the unknown-method branch fails the
built-in `protocol/preview-schema-empty` check.

this report is also the destructive-provisioning gate. before `plan --provision` or
`apply` calls `ensure_schema`, the host previews and refuses the run with
`provisioning would delete schema; re-run with --allow-delete:` followed by a bullet
per entry, when `deleted_object_types` or `deleted_object_fields` is non-empty. schema
deletes cascade to their objects on the backend, so an adapter that drops a type
without listing it here takes the objects with it and never prompts. list what you
would delete, even if the same call also creates: what you list is what the
operator is shown.

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
operator passed `--allow-delete`, and it omits the keys that are empty:

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

- `observer`: read-only. the host calls `read` but never `write` or either
  provisioning method; `apply` and `plan --provision` reject the backend up front.
- `emitter`: write-only. the host calls `write` but never `read`; plain `plan`
  plans every declared object as a create against an empty observation, while
  `import` and `plan --report` reject the backend up front instead of
  observing nothing. the role governs read vs write only: provisioning is a
  write, so `ensure_schema` and `preview_schema` are still called.
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
inventory succeeds with a right-shaped payload, as do a schema preview and an
empty provisioning, so an adapter that errors on every request does not pass.
provisioning follows the declared role, since only an emitting adapter is ever
asked for it. the runner probes `capabilities` first: a declared emitter is never
sent a read by the host, so its liveness check is an empty write instead of the
empty read, and it may answer `read` with an error. the version probe rides that
same role-appropriate method, so an emitter is sent an unsupported-version write:
probed with a read it would refuse for role reasons, answering the check without
ever reading `version`. the malformed-json and unknown-method probes need no such
care, since both are expected to error whatever the role. answering
`capabilities` itself with the unknown-method error stays conformant and means
the default read+write role.

"right-shaped" also means only the keys the protocol defines: a response
carrying an unknown one fails, naming where it sat, from the envelope beside
`result` down to inside `applied` and `provision`. your own `key` and `attrs`
maps are not checked. the host is laxer on purpose, so run the runner that
ships with your alembic.

the empty schema provisioning is a real `ensure_schema`, and a converging
adapter reads it as "delete everything you own". the runner is not the host
and has no `--allow-delete` gate, so point it at a disposable backend.

it runs by default, and `--no-provisioning-check` turns it off. opt-out rather
than opt-in: a check nobody remembers to enable certifies nothing, and the
adapter this one exists for -- the hand-rolled emitter that would otherwise be
certified into an apply that fails on `unknown method` -- is exactly the one
whose author would not enable it. a turned-off check is reported as `skipped`
and counted apart from the passes, so a suite that never sent `ensure_schema`
does not read as one that certified it.

to exercise `read`, `write`, and `ensure_schema` with requests of your own,
pass `--cases` a file or directory of cases. a case is a complete request and
an expectation:

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
