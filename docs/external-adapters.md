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

apart from the `capabilities` probe, whose failures all fall back to the default
role, a response carries only the keys the protocol defines, at every level: an
unknown one is an error naming it, rather than a default standing in for an
answer the adapter never gave. the `key` and `attrs` maps you fill stay your own.

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
  "state": { "mappings": { /* uid -> backend id */ } },
  "scope": {
    "kind": "narrowed",
    "backend_ids": { "dcim.site": ["site-1"] },
    "keys": {
      "dcim.site": [
        { "key": { "name": "site-a" }, "canonical": "{\"name\":\"site-a\"}" }
      ]
    },
    "unnarrowed": ["dcim.device"]
  }
}
```

`scope` is an advisory narrowing hint: what the host already knows it needs, in
backend-neutral terms. an adapter that can filter may translate it into a
backend query, one that cannot may ignore it, and the host behaves identically
either way — returning a *superset* of what the hint names is always valid, so
no correctness may rest on honoring it.

the rule is the union of the two halves, and neither alone is safe. an object
the host has not yet bound is named only by `keys`, so `backend_ids` alone drops
it and turns its adoption into a create. an object whose key drifted on the
backend since the host bound it is named only by `backend_ids`, so `keys` alone
drops it and plans a create over a live object, on that run and every one after,
since the host only ever binds what came back. keep an object either half names.

each entry in `keys` carries the key twice: `key` for an adapter turning the
hint into a backend query, `canonical` for one filtering in memory. the host
matches on `canonical`, so an object read back as `{"vid": 100.0}` and a
declared `{"vid": 100}` are one key to it and two to a structural compare of
`key`. the canonicalization lives in the host, so an adapter that cannot
reproduce it must **keep** an object it is unsure about: a superset is always a
valid answer, dropping one on a key mismatch alone is not.

`unnarrowed` names the types the hint cannot narrow, to be read whole. a
ref-keyed type's declared key holds canonical uids, which is not a space any
backend can be queried in, and not the space the adapter's own rows are in until
`resolve_ref_keyed_identity` has run over the batch it already fetched. such a
type is named in `unnarrowed` and in neither map, and the host asks for all of
it (`docs/engine.md`).

`{"kind": "full"}` asks for every object of every requested type, and is what
delete detection and `import` send, since both are defined against the full
observation. a `narrowed` scope naming nothing for a type is not the same
request: it says nothing of that type is needed.

the field is additive. an older adapter that does not read it is unaffected, and
the rust sdk's `read_scoped` defaults to delegating to `read`, so only an
adapter that wants the hint overrides it.

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

`attrs` and `backend_id` are optional, the way `ensure_schema` below states for
its report: a key-only object answers with `type_name` and `key` alone.

a ref-typed field, in `key` as much as in `attrs`, names the target's uid rather
than the backend's own id; `crates/alembic-cli/examples/ref_chain_adapter.rs`
resolves a chain of them against the state the request carries.

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

the result's keys are optional the same way, so a write that applied nothing
answers `{}`. apply sends the method on every run, including a converged one whose
plan carries no ops.

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

answer the method even when you provision nothing, with an empty report, unless
you declare the `observer` role: apply propagates the error otherwise, and leaving
it to the unknown-method branch fails the built-in `protocol/ensure-schema-empty`
check.

### preview_schema

the host calls this at plan time to show what `ensure_schema` would provision,
without writing anything. return the same `ProvisionReport` shape `ensure_schema`
would (an empty report when there is nothing to provision), or a `null` result if
the adapter cannot preview, which plain `plan` reports as `schema preview:
unavailable for this backend` and the provisioning paths refuse outright. answer it
either way, unless you declare the `observer` role: leaving it to the unknown-method
branch fails the built-in `protocol/preview-schema-empty` check.

this report is also the destructive-provisioning gate. before `plan --provision` or
`apply` calls `ensure_schema`, the host previews and refuses the run with
`provisioning would delete schema; re-run with --allow-delete:` followed by a bullet
per entry, when `deleted_object_types` or `deleted_object_fields` is non-empty. schema
deletes cascade to their objects on the backend, so an adapter that drops a type
without listing it here takes the objects with it and never prompts. list what you
would delete, even if the same call also creates: what you list is what the
operator is shown.

a `null` result refuses the run rather than skipping the gate: `plan --provision` and
`apply` stop with `this backend cannot preview schema`, since a delete nobody can see
is the case the gate is for. so `null` is a capability statement, not a default to
fall through: an adapter that provisions anything must answer a report, and an
operator who wants the run without one passes `--allow-delete`. the rust sdk's default
answers the empty report; check what your sdk's default answers, since one that
answers `null` refuses every provisioning run.

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
`adapter`, and an adapter that implements one side overrides it in one method:

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

## value-space contract

the protocol above is the shape of the conversation; this is the contract on the
values in it. alembic's own validation is the authority in both directions, and
the backend never contradicts it:

- a value alembic accepted must be writable. constraints an adapter provisions
  from a declared schema are at most as strict as alembic's own checks: a
  provisioned regex is at least as wide as the format it mirrors (`format_regex`
  in core), an extra enum choice on the backend is inert while a missing one is
  fatal, and `required` only ever tightens toward the declaration. core
  validation is the gate; the backend's is a mirror that must never reject what
  core passed.
- a value a read answers must land back in alembic's value space. a field the
  request's schema declares validates under the same rules `plan` and `import`
  apply, so a `date` answers as an rfc3339 date, not whatever the backend
  stores, and a ref names the target's uid, never a backend id (`plan` refuses
  an observation that violates this).
- objects of types the request's schema does not declare are a tolerated
  superset: an adapter may answer with more than was asked, and the engine
  ignores what it did not ask for.

the conformance runner enforces the read half on every `--cases` read, so an
adapter learns before its first user does.

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
inventory succeeds with a right-shaped payload, as do an empty write, a schema
preview and an empty provisioning, so an adapter that errors on every request
does not pass. provisioning follows the declared role, since only an emitting
adapter is ever asked for it. the runner probes `capabilities` first: the
liveness checks are the methods the host sends that role, an empty read for an
observer, an empty write for an emitter, both for a full read+write adapter. an
emitter is never sent a read by the host and may answer one with an error. the
version probe rides a method the role implements that writes nothing, an
unsupported-version read for the roles that read and a `preview_schema` for an
emitter: probed with a read an emitter would refuse it for role reasons,
answering the check without ever reading `version`.
the malformed-json and unknown-method probes need no such care, since both are
expected to error whatever the role. answering `capabilities` itself with the
unknown-method error stays conformant and means the default read+write role.

"right-shaped" also means only the keys the protocol defines: a response
carrying an unknown one fails, naming where it sat, from the envelope beside
`result` down to inside `applied` and `provision`. your own `key` and `attrs`
maps are not checked. the host rejects the same keys but names only the key, so
a failure here is the one that says where to look.

two built-ins write. the empty schema provisioning is a real `ensure_schema`,
which a converging adapter reads as "delete everything you own", and the empty
write is a real `write` at the default output path `setup: {}` selects. an
emitter and a full adapter are sent both, an observer neither. the runner has no
`--allow-delete` gate, so point it at a disposable backend and run it from a
scratch directory.

both are off unless you pass `--write-checks`, and each is reported as `skipped`
rather than dropped, counted apart from the passes. every other check still
runs, the version probe included, so a bare invocation certifies everything but
those two. `--no-provisioning-check`, the old spelling, is the default now, so
it warns and refuses `--write-checks`.

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
structurally. a read case's result is additionally validated against the
request's schema (the value-space contract above), so an out-of-space value or a
backend id in a ref field fails the case even when `expect.result` is omitted. `error` pins the exact message an `ok: false` case must come back with.
a key that is none of those three is a parse error naming it, and so is a
stray key beside `name`/`request`/`expect`: `result` and `error` are the assertions, so
a typo in one would drop it and report the case as passing. the runner exits `0` when
every check passes, `1` when a check fails, and `2` on a usage or fixtures error,
including a `--cases` directory with no `.json` files directly in it, or one whose
subdirectories hold cases it would not load, so it drops straight into ci:

```console
alembic-adapter-test --cases tests/alembic -- ./alembic-adapter-mybackend
```

```yaml
- run: alembic-adapter-test --cases tests/alembic -- ./alembic-adapter-mybackend
```

a read case is also held to the narrowing rule above: the runner reads it once
unscoped, then once through each half of the hint alone and once under the scope
the engine itself builds, where a ref-keyed type is held out to be read whole. it
fails the case when an object the scope names does not come back. a superset is
always valid, so an adapter ignoring the hint passes and only a wrong narrower
fails. the comparison is canonical, which the host can do and an adapter cannot.

a worked python adapter and its cases live in
`crates/alembic-adapter-test/examples/`. the canonical request/response pairs in
`fixtures/external_protocol/` document the same protocol as plain json for sdks
in other languages.
