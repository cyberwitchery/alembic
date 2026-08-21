# changelog

## Unreleased

- a read returning two objects under one key names both colliding backend ids, not just the key
- `plan` refuses an observation whose ref-typed fields hold backend ids rather than the target's uid
- a backend id in state answers to one uid, and the inventory decides which, so planning settles without costing a rename
- `alembic-adapter-test` fails a `--cases` directory whose subdirectories hold cases it does not load
- `apply` rejects a plan file carrying an unknown key rather than dropping it, `schema_preview` included
- external adapters: an unknown key in a provisioning report, an applied operation or the response envelope is rejected
- provisioning is refused when the adapter cannot preview schema. the rust sdk default now previews an empty report
- `alembic-adapter-test` writes only when asked. pass `--write-checks`, or a ci run stops certifying the two writing checks
- `alembic-adapter-test` fails a `--cases` path that resolves to no cases instead of certifying only the built-ins
- adapters resolve ref-keyed identity within one read, so an inventory imported from a backend converges against that backend (#307)
- infrahub: state wins for an object it already maps, so a declared uid converges and a second uid stops clearing itself
- **breaking** external adapters: an unknown key in an observed object, capabilities or apply report is now rejected
- an external adapter may omit `applied` from a `write` result and `attrs` from a `read` object. a result the host cannot read names the method it answered
- generic: a plan holding a delete the backend cannot perform is refused before anything is written
- generic: the shipped example inventory declares its key fields in `fields:`, and `missing key field` names what to declare
- `plan --report` and `plan --dry-run` take the state lock shared; `--provision` keeps the run exclusive
- `alembic-file-generator` generates an inventory as well as a plan (`--kind plan|inventory`), and its devices key on their name
- `alembic-adapter-test` fails an adapter whose response carries a key the protocol does not define, envelope and payload both
- **breaking** a `format:` disagreeing with the check its field type already applies fails validation
- a provisioning run names what it deprecated and deleted, under `provision:`, `schema preview:` and the `--allow-delete` refusal
- `alembic-adapter-test`: `protocol/preview-schema-empty` follows the declared role, so a read-only adapter is not asked for a schema preview
- netbox: renaming a declared type into the same custom object type name keeps the backend type it owns (#369)
- infrahub: two declared types that render one graphql kind fail the run; rename one (#372)
- peeringdb observes `netixlan` records rather than organizations, and `PEERINGDB_API_KEY` is optional (#267)
- `alembic-adapter-test`: `protocol/write-empty` joins `protocol/ensure-schema-empty` as a writing check, and a read+write adapter is sent it too
- netbox: two declared types whose names collapse to one custom object type name fail the run; rename one
- netbox converges an existing custom object type field, so a declared `pattern:`, `format:`, `description:` or `required:` reaches the backend
- nautobot: two declared types sharing one custom field must declare the same `enum` values in the same order
- django reports nothing to provision rather than `schema preview: unavailable for this backend`, and the conformance runner gains `protocol/ensure-schema-empty`
- netbox and nautobot converge an existing custom field, so a declared `pattern:` or `format:` reaches the backend
- a write-only backend provisions schema: `ensure_schema` and `preview_schema` move from `Adapter` to `Emitter`, and `Backend::adapter()` is gone
- infrahub: an empty `branch:` counts as unset, so a run with `branch: ""` reads the whole schema
- infrahub: the schema snapshot request carries the configured `branch:`, so `ensure_schema` and `preview_schema` plan against it
- django: the generated model now validates a declared `list`'s element type
- a stat that fails for any reason other than the file being absent is an error naming the path
- django: a field's declared `format:` or `pattern:` replaces the regex its type would derive, instead of stacking with it
- netbox and nautobot provision a declared `format:`, and the format a `uuid`, `cidr`, `prefix`, `mac` or `slug` type carries, as `validation_regex`
- django: the generated validators for `format: uuid` and `format: ip_address` accept the spellings `validate` accepts
- nautobot provisions a declared `enum` as a `select` custom field and a `list` of `enum` as a `multi-select`
- netbox and nautobot: a declared `float` provisions as `json` rather than `decimal`, which nautobot does not have
- cli: `apply --interactive` fails when stdin runs out of answers, instead of declining every remaining operation unasked (#331)
- netbox and nautobot provision a declared `pattern:` as the custom field's `validation_regex`
- engine: `import` validates the inventory before returning it, rather than writing a file `plan`, `apply` and `map` reject (#307)
- conformance runner: the cli test resolves its example adapters from `CARGO_TARGET_DIR`, and a missing one names the path
- cli: `plan --report -o` carries the schema preview the run already computed; `DriftReport` gains an optional `schema_preview`
- conformance runner: `protocol/version-mismatch` follows the declared role, so an emitter is probed with a write rather than a `read`
- core: `validate` names the offending value: `expected mac, got not-a-mac` rather than `expected mac, got string` (#336)
- cli: `plan --report` refuses a write-only backend with `backend is write-only; it cannot observe state`
- **breaking** cli: `alembic.yaml` rejects unknown keys, and the `ALEMBIC_` env layer is narrowed to the keys `AppConfig` owns
- engine: the apply journal survives the delete phase, so a run killed while deleting resumes (#46)
- netbox and nautobot populate `ProvisionReport::created_tags`, so an apply reports the tags it creates
- **breaking** `date`, `datetime` and `time` values are format-checked as rfc 3339, so a malformed timestamp fails `validate` (#336)
- engine: the apply journal is append-only, so a process killed mid-apply resumes; `Journal::save` is gone (#46)
- cli: `validate` writes its report as json through a new `-o`/`--output`, each error carrying its variant as `kind`
- **breaking** the generic backend's config, and the conformance runner's `--cases` fixtures, reject unknown keys, naming the key (#309)
- **breaking** `backend: peeringdb` rejects unknown keys, and gets a reference page at `docs/peeringdb.md`
- cli: an `-o` path that cannot be written is an error before the backend is read (#324)
- cli: `plan --report` writes the drift report as json through `-o`/`--output`, every category always present (#56)
- engine: the apply journal records the backend id each create or update returned, so a resume can reference it (#46)
- **breaking** a field declaration rejects unknown keys: the six metadata keys, plus the one param its type tag takes (#315)
- cli: `apply` writes its report as json through a new `-o`/`--output`, matching `plan`/`map`/`import`
- engine: a failed apply reports what it journaled and that a re-run resumes, at `warn` (#46)
- docs: the published walkthroughs and case studies run in ci, every inventory validated and the documented `map` flows asserted
- **breaking** the inventory file, the object envelope, the type schema and the `alembic map` spec reject unknown keys (#315)
- django adapter: the emit summary goes through `tracing` rather than `println!`, so it obeys `RUST_LOG` (#310)
- ci: the django e2e job installs `django-filter` and `drf-spectacular`, and drives the generated app (#308)
- peeringdb answers `unsupported type <name>` when a run explicitly asks for a type it does not observe
- generic adapter: an endpoint config takes `next_path`, the json path to the next-page url, and observe follows that chain
- generic adapter: a redirect that leaves `base_url`'s origin is refused rather than followed
- django: a datetime whose rfc 3339 separator is lowercase loads instead of failing at apply (#401)

## [0.8.0] - 2026-07-29

- docs: add `docs/django.md`, the reference page the django adapter was missing while every other shipped adapter had one (config keys, emitter semantics, which files are regenerated, the optional-package matrix, and the name rules the emitter enforces). `no_migrate` and `no_admin` were documented nowhere before
- **breaking** adapter configs reject unknown keys. every backend config (`netbox`, `nautobot`, `infrahub` and its nested `schema`, `generic`, `django`, `external`) carried no `deny_unknown_fields`, so a typo was discarded in silence and the run took the default instead. the booleans are what made this bite: a typo'd `no_migrate` migrated and loaded the inventory into a database the user meant to leave untouched, and reported success either way. a typo'd key is now a parse error naming the field, matching `Schema` in core, which already denied them. the `backend:` tag the config dispatches on is not itself an unknown field (#309)
- **breaking** engine: `import` now observes in the canonical uid space, and `import_inventory` drops its `state` parameter
- map: rewire refs inside ref-typed *key* fields through the same source->target remap attr refs already use, and re-derive the referrer's key-derived uid from the rewritten key, so a remapped key ref changes the referrer's uid (an explicit `uid:` stays pinned; the rewrite runs in key-ref dependency order, so chains cascade regardless of rule order). previously map skipped key refs entirely and the output dangled with `MissingReference`. a cycle among key-derived key refs cannot converge and is a clear error
- external adapters: a new `capabilities` protocol method lets an adapter report its role (`observer`, `emitter`, or `adapter`); the host probes it once at construction and wraps the adapter accordingly, so a declared emit-only adapter now gets the same handling as a built-in emitter like django (all-creates `plan`, up-front `import` error) instead of stubbing an empty `read` that silently observes nothing. an adapter that does not answer (the unknown-method error) defaults to the full read+write role, so existing adapters are unchanged. the conformance runner probes the role first and checks a declared emitter with an empty `write` instead of the empty read (#117)
- django adapter: reject a schema whose field or type names cannot be python identifiers, or whose field names collide with the generated `uid`/`key`/`attrs` model attributes, listing every offender in one error instead of emitting a broken or silently wrong app. a field named `uid` used to override the generated uuid primary key without any error from `manage.py check`; a keyword or leading-digit name emitted python that does not parse
- conformance runner: write a case's request on its own thread, after the drains start and under the run timeout, so a request larger than the pipe buffer against an adapter that never reads its stdin trips the timeout instead of blocking the runner in `write_all` (or deadlocking outright against a chatty adapter); matches the fix `ProcessAdapter::run` got in #213
- core: reject an unknown key in a schema block instead of silently discarding it, so a file whose `schema:` is not an inventory schema (`examples/backend-infrahub.yaml`, where it is the infrahub schema-push config) now fails to load instead of validating clean as a schema with zero types
- engine: a non-canonically written `uid` (uppercase, or uuid's simple/braced/urn forms) no longer loses its source line in validation errors; the uid→line index is keyed by the parsed uid, not the raw token
- engine: `import` warned once per dropped attr *per object*, so importing N objects of a type repeated the same `dropping undeclared attr` line N times (a real netbox import carries ~3 dozen undeclared server-computed attrs per object, so a 1000-object import emitted tens of thousands of identical lines at the default `warn` level, burying everything else the import printed); it now warns once per type+field for the whole import
- cli: a malformed `RUST_LOG` is now reported instead of silently ignored. `EnvFilter::try_from_default_env` errors for an unset and a rejected value alike and the fallback discarded which, so a typo (`info,alembic_engine=trce`) fell back to the default `warn` filter with no message, producing output identical to setting nothing and looking like alembic had nothing to say; a rejected value now prints a warning to stderr naming it and the parse error, and still falls back to `warn`, so the command itself is unaffected
- infrahub adapter: name *every* unresolved uid of a ref-typed list in the `unresolved references` apply error. the adapter's local, shallow `describe_missing_refs` (forked in #174 because infrahub refs are flat) extracted only the *first* uid from a `list_ref` array, so an apply left pending on a dangling element after the first (a device's `interfaces` whose second interface is unresolved) reported an empty message; it now collects every element, matching the exhaustive array walk the `key_refs_resolved` check beside it already did
- cli: `plan` reports a failing schema preview as `schema preview failed: <error>` instead of `schema preview: unavailable for this backend (<error>)`. the docs bind that phrase to a backend that cannot preview at all (`preview_schema` answering null), so a real preview error was reported as a capability limit
- conformance runner: a case that pins an `expect.result` now fails when the adapter answers `result: null`, instead of passing without comparing anything. `preview_schema` is the only method allowed to answer null ("cannot preview schema"), so a preview case pinning a `ProvisionReport` used to pass against an adapter that reported it could not preview at all; a case that pins no result still accepts null as before
- django adapter: name every relation's reverse accessor after the model and field that declares it (`site.dcimdevice_site`), so a type with two relations to the same target (two refs, two list_refs, or a ref and a list_ref) no longer fails django's system check with `fields.E304` and aborts the apply. an earlier fix disabled reverse navigation entirely with `related_name="+"`; a name unique by construction fixes the clash without giving up the reverse
- django adapter: write the inventory objects into the generated app. the emitter collected every create op into an inventory and then used only its schema, so apply reported `applied N operations` against an app whose tables were empty. the objects are now emitted as a django fixture (`<app>/fixtures/alembic.json`) and loaded with `loaddata` after `migrate`; the ir uid is the model's primary key, so relations carry over as-is, attrs the schema does not model stay in the `attrs` blob, and a re-run converges the rows instead of duplicating them
- django adapter: give the `uid` primary key a `default=uuid.uuid4`. the field is `editable=False`, so drf marks it read-only and never fills it, and *every* POST to a generated viewset failed with `NOT NULL constraint failed: <table>.uid`: the generated rest api could not create anything
- django adapter: emit `null=True` for optional non-text fields. `blank=True` is a form-level flag only, so an optional `bool`/`int`/`datetime`/`ip_address`/`ref` column was still NOT NULL and saving an object without it failed outright; only `ip_address` had the workaround. optional text fields keep the empty string, and optional `json`/`list`/`map` fields get `default=dict`/`default=list` rather than a null
- django adapter: escape every value interpolated into the generated python. an enum value or `pattern:` containing a quote produced a `SyntaxError` and no importable app; patterns are also no longer emitted as raw strings, which cannot end in a backslash
- django adapter: extend the name validation added above to the rest of what django rejects or quietly redefines: a field named `pk`/`id`/`objects`/`save`/`delete`/`clean`, and a name ending in an underscore or containing a double underscore (fields.E001/E002). two further structural rejections join it: two types that render to one model name (`dcim.site` and `dcim_site`) produced two `class DcimSite` in one module plus a duplicate admin registration and a duplicate route, and a relation to a type outside the model produced a dangling `ForeignKey` that fails `manage.py check`
- django adapter: configure the drf backends that honour what the generated viewsets declare. `filterset_fields`, `search_fields`, and `ordering_fields` were emitted with no `DEFAULT_FILTER_BACKENDS` in settings, so all three were silently ignored and `?field=value` returned the unfiltered list. settings now wire the search and ordering backends plus pagination; `filterset_fields` is only declared when `django-filter` is importable in the target interpreter, and json columns stay out of it (django-filter cannot resolve them). `search_fields` is restricted to text columns, since the `icontains` both the admin and drf emit has no postgres operator for uuid or inet
- django adapter: serve the openapi schema with drf-spectacular (`/api/schema/`, `/api/docs/`) when it is importable, and route nothing otherwise. the route was drf's legacy `get_schema_view`, which asserts its way through three optional packages (`pyyaml`, `uritemplate`, `inflection`) and then fails on any current `django-filter`, which no longer implements the schema hook it calls: the generated app shipped a route that answered 500
- django adapter: generated models gain `__str__`, `Meta.ordering`, `verbose_name` and `help_text`
- cli tests: the django e2e tests fail instead of skipping when `ALEMBIC_DJANGO_PYTHON` names an interpreter without django, so a ci run that cannot import it reports red rather than green-with-nothing-run
- core: a malformed field `pattern:` in a schema is now reported once at the schema level (as an uncompilable-pattern error) instead of once per object value; the per-object pass reuses the regex compiled at schema load rather than recompiling it for every value
- cli: `apply` now fails fast on a backend that cannot apply (a read-only observer such as `peeringdb`), rejecting it right after constructing the backend like `plan`/`import` already do, instead of reading the plan and (under `--interactive`) prompting `create/update/delete ...? [y/N]` for every op before failing deep in the pipeline; the user-visible error is unchanged, only the wasted prompting is removed
- netbox + nautobot adapters: pass a null attribute through the write body to clear a nullable ref, instead of aborting the apply with `ref value must be a uuid string`. both adapters re-implement `build_request_body` locally (for tags and custom-field routing) and lacked the null guard the shared engine helper and the generic/infrahub adapters already had, so clearing a nullable foreign key (a device's `rack`, an interface's `vrf`, or a netbox generic fk like `assigned_object`) now converges instead of failing
- core: `alembic validate` now rejects a schema that declares a key field as `nullable: true`. a key component feeds uid derivation and must render to a scalar, so render already rejected a null key value at map time (a null has no identity form), but validation passed the schema clean, leaving a type whose objects were un-representable by the pipeline. the check runs at schema load, before any object is authored; a nullable non-key field stays legal, as does every scalar or `ref` key
- cli: `plan --provision` now fails closed when the schema preview errors, aborting instead of provisioning blind; a preview error previously slipped past the `--allow-delete` gate and let `ensure_schema` delete unmanaged schema, unlike apply which already failed closed
- infrahub adapter: name an unresolved ref-typed *key* field in the `unresolved references` apply error. the adapter's local, shallow `describe_missing_refs` (forked in #174 because infrahub refs are flat) scanned only `desired.attrs`, so an apply left pending on a dangling key ref (an interface keyed by `(device, name)` whose device is unresolved) reported an empty message; it now scans `desired.key` too, matching the key-values scan the engine's shared copy gained in #221
- engine: mark journal ops done in O(1) via an index, so applying an N-op plan is O(N) not O(N²) (a 50k-op plan spent ~0.9s, growing quadratically, in journal bookkeeping alone)
- add `alembic-file-generator`, an internal tool that emits a synthetic `plan.json` of N create ops over a fixed dcim schema for scale/perf testing (#118)
- external adapters: write the request to a child adapter's stdin concurrently with draining its output, all under the run timeout, so a child that floods its output or never reads its stdin can no longer deadlock the write outside the timeout guard and hang the cli forever
- django adapter: error when urls.py has no `urlpatterns = [` list to wire the api route into, instead of writing the file and reporting success while the generated app's routes are silently never mounted; the closing bracket was already strict, so the opening landmark now matches every sibling scaffolding step
- **breaking** django adapter: rename the `cast_django` module to `emit` and `run_cast_django` to `run_emit`, retiring the last code references to the `cast` command removed in 0.4.0; the test-only `ALEMBIC_CAST_PYTHON` env var becomes `ALEMBIC_DJANGO_PYTHON`
- core: `alembic validate` now prints the offending type's source `file:line` for the four newest schema validators (unknown ref target, uncompilable `pattern:`, `format:`/`pattern:` on a non-string field, empty `enum`), matching every other validation error, instead of dropping the location; their `type_name` now resolves to the declaring type's source line
- core: `alembic validate` now rejects a schema that declares a key field with a composite type (`list`, `list_ref`, or `map`). a key component feeds uid derivation and must render to a scalar, so `map`/render already rejected a composite key/uid value at render time, but validation passed the schema clean, leaving a type whose objects were un-representable by the pipeline. the check runs at schema load, before any object is authored; every scalar key type stays legal, including `ref` (a ref key renders to a scalar uid string)
- netbox + nautobot adapters: resolve a ref-typed *key* field to its canonical uid on read, matching how a ref-typed regular field already resolves. `normalize_attrs` derived its per-field target hint from `.fields` only, so a nested brief under a ref-typed key field (an interface keyed by `(device, name)`, say) short-circuited the key-derivation resolver, and an unmanaged referent read back as its raw backend id rather than the uid; the hint now consults `.key` too, matching the engine's `normalize_attrs_refs` used by the generic/infrahub adapters
- engine: stop cloning every observed field value while diffing; the steady-state (no-drift) path borrows each observed value for the equality check and only clones the values of fields that actually changed

## [0.7.0] - 2026-07-07

- map: `emit: passthrough` copies each matched source object through unchanged, carrying its source-schema type into the output, but only for objects no other rule emits. paired with `match: "*"` it is a terse "reshape the exceptions, pass the rest through" catch-all, so renaming one field no longer needs an identity rule per type; the target `schema` need only declare the types actually reshaped
- external adapters: document the `preview_schema` protocol method (`docs/external-adapters.md`), and the conformance runner now sends a built-in `preview_schema` check plus ships a `preview-schema` case fixture, so an external adapter that omits schema preview is flagged
- cli: `plan` now prints a human-readable per-op summary of the plan it writes (create/update/delete grouped, with per-field `from -> to` for updates, truncated for large plans), instead of only a one-line count; the machine-readable copy is still the written json plan file
- cli: `plan` now works against a write-only (emitter) backend such as `django`, producing an all-creates plan against an empty observation instead of failing with `backend is write-only; it cannot observe state`. this makes the documented `plan` then `apply` flow usable for emitters
- cli: the installed binary is now `alembic` (not `alembic-cli`), matching every doc and its own `--help`; the crate is still published as `alembic-cli`. release tarballs are renamed to `alembic-<version>-<target>.tar.gz`
- cli: add `--version`, and help text for every subcommand and flag (`--help` was previously almost blank)
- engine: the local state store now takes an exclusive advisory lock (a sidecar `<state>.lock` file) for a run's whole lifetime, so two concurrent runs against the same `state.json` can no longer both load it and race to save, silently clobbering each other's uid->backend-id mappings; a second run fails fast with `another alembic run holds the state lock`. the postgres backend already had optimistic locking. the lock is created at load, so a state path whose parent cannot be a directory now errors at load rather than save
- engine: compare attrs by declared field type when diffing, so an `int`/`float` field whose backend representation differs from the inventory's (int vs float, or a numeric string) no longer plans a perpetual no-op update; `list`/`map` fields compare elementwise under their item/value type, every other type compares exactly, and an undeclared field falls back to raw comparison
- engine: gate destructive schema provisioning behind `--allow-delete`. `apply` and `plan --provision` previously deleted alembic-owned custom object types/fields the inventory no longer declared with no gate, even though object deletes required the flag; deleting a custom object type cascades to its objects on the backend. both paths now refuse those deletions (via the read-only schema preview) unless `--allow-delete` is given, matching the object-delete gate
- engine: the `--allow-delete` schema-provisioning gate now fires on every apply path. `apply` previously only checked when the caller populated the plan's schema preview, which the interactive-apply and library `plan()`+`apply_plan()` paths do not, so those could still run cascading custom-object-type/field deletes ungated; `apply` now self-previews via the adapter before `ensure_schema` as the authoritative gate. an adapter that cannot preview schema is unaffected, and a preview error now fails closed rather than provisioning blind
- engine: index inventory uid source-lines in a single pass instead of rescanning the whole file per object, so loading is linear in file size; a 50k-object inventory that previously could not `validate` in ten minutes now loads in under a second. also resolves a uid to its own line by exact value rather than substring, so a uid that is a prefix of another no longer points at the wrong line
- engine: journal hashes no longer depend on the rust version, so a partially-applied plan can be resumed by a different build; a journal from an older version is orphaned once
- conformance runner: accept a `preview_schema` response with a null result (`ok=true`, no result, no error) as the canonical "cannot preview schema" signal, since the host's `call_optional` maps it to `Ok(None)`; a null result on read/write/ensure_schema stays rejected because those dispatch through `call`, which hard-errors on it, so the runner no longer fails a correct adapter the real registry accepts
- generic adapter: `plan`'s schema preview reports nothing to provision (`schema_preview: {}`, no stderr line) instead of `schema preview: unavailable for this backend`, since the generic adapter provisions no schema (its `ensure_schema` is the no-op default), matching the preview==ensure invariant netbox/nautobot/infrahub already hold
- netbox + nautobot adapters: remove the adapter-side `type`/`if_type` aliasing entirely, so the adapter reads and writes the backend's literal `type` field for every type, interfaces included; previously the read renamed a backend `type` to `if_type`, the write remapped a `dcim.interface` `if_type` back to `type`, and provisioning treated `if_type` as a native interface field. reshaping a field like this belongs in an `alembic map` spec, not baked into the adapter on a hardcoded `dcim.interface`, so a user who wants `if_type` in their ir renames `type` to `if_type` in map; reverts the aliasing added in #194/#195
- infrahub adapter: resolve a desired key's `ref`/`list_ref` fields to backend ids before matching it against an existing object, so a ref-keyed type (interfaces, cables, ip assignments) finds its existing object instead of never matching; most severely, a genuinely failing delete of such an object is no longer swallowed and reported as success
- netbox adapter: `plan`'s schema preview rejects an invalid custom-object field name (outside `[A-Za-z0-9_]`) at plan time, matching what `apply` already enforces, instead of reporting a field creation `apply` then refuses
- netbox adapter: classify custom fields by the django content-type form (`app_label.model`, e.g. `ipam.ipaddress`) everywhere, matching how netbox returns and keys them, instead of the endpoint-underscore form (`ipam.ip_address`). previously a custom field on any multi-word model (`ipam.ip_address`, `dcim.device_type`, `dcim.mac_address`, …) was misclassified: `apply` emitted it as a top-level field netbox silently drops, so the field never converged (every plan re-updated it), and `ensure_schema` posted an invalid content type and errored. single-word models were unaffected, so it was invisible until now
- nautobot adapter: create a custom field with `key` (the nautobot 2.x writable identifier), not `name` (which the api has no field for and derives `key` from by slugifying `label`), so a non-slug field name like `assetTag` keeps its identity instead of becoming `assettag`, which the read/detect/write paths then never matched, silently dropping the field's value on write and erroring on the next `ensure_schema`
- generic adapter: clearing a nullable ref (setting it to `null`) no longer aborts the write with `ref value must be a uuid string`; the write body-builder now delegates to the shared engine `build_request_body`, which passes a null straight through to clear the field, instead of re-implementing the loop without that guard
- engine: resolve ref-typed key fields on read and name them in missing-ref errors, matching attribute fields and the validation/infrahub key paths that already treat them as first-class refs

## [0.6.0] - 2026-07-05

- infrahub adapter: write a null-valued attribute under the same `{ "value": … }` wrapper as every non-null attribute and the read path, instead of a bare `null`, so clearing a nullable field sends the attribute-input shape rather than an unwrapped value; a null `ref`/`list_ref` still goes out bare to clear the relationship
- nautobot adapter: write the `dcim.interface` type field back under the api name `type` (not `if_type`) and treat `if_type` as a native field, so an interface create/update sets the real type instead of failing validation or provisioning a spurious `if_type` custom field
- core: format-typed fields (`cidr`, `prefix`, `mac`, `slug`) now validate their value's format instead of silently accepting any string, matching how `uuid` already validated and the `format:` constraint
- core: validate that schema `ref`/`list_ref` targets name a declared type, including refs nested in `list`/`map` fields, so a misspelled target fails schema validation with one clear error instead of confusing per-object reference errors
- core: validate that schema `pattern:` regexes compile at schema-load time, instead of only failing per-object (and never for types with no objects yet)
- core: reject a `format:`/`pattern:` constraint on a non-string field type (`int`, `float`, `bool`, `list`, `map`, `list_ref`) at schema-load time, instead of only failing per-object with a misleading `expected string` error (and never for types with no objects)
- core: reject an empty `enum` field (`values: []`), including enums nested in `list`/`map` fields, at schema-load time, instead of only failing per-object with a confusing `expected: enum()` error (and never for types with no objects)
- engine: map rewrites refs nested in `list` and `map` fields across a rename, not just top-level `ref`/`list_ref` attrs
- engine: drop a present-but-null member value from a `${group.items.<path>}` aggregation list, matching the documented non-missing-values-only contract
- engine: a lone map placeholder whose transform argument contains a quoted `${` keeps its transformed type in `attrs`, instead of being coerced to a string
- generic adapter: resolve refs nested in `list` and `map` fields on read, matching the write path and the netbox/nautobot adapters
- infrahub adapter: resolve refs nested in `list` and `map` fields on read, matching the generic/netbox/nautobot adapters and the write path, by sharing the engine's read-side reference normalizer
- core: attribute reference-type-mismatch errors to the referencing object, not the referent
- engine: locate an object at its own `uid:` line, not an earlier line that references its uid, so validation errors point at the right object
- engine: import errors on a malformed tag item instead of silently dropping it
- engine: parse a `u64`-range transform argument literal (above `i64::MAX`) as an exact integer instead of a lossy float, matching the value path
- core: canonicalize a `u64`-range key/uid component (above `i64::MAX`) as an exact integer instead of a lossy float, so two distinct large-integer keys no longer collapse onto one uid
- netbox adapter: singularize `x`/`z`/`ch`/`sh`-stemmed endpoints correctly (`ipam/prefixes/` → `ipam.prefix`, not `ipam.prefixe`), so reading prefixes and decoding refs to them yield the declared schema type
- netbox adapter: singularize `s`-stemmed endpoints correctly (`extras/statuses/` → `extras.status`, not `extras.statu`), so reading and decoding refs to a custom object type whose singular ends in `s` yield the declared schema type
- netbox + nautobot adapters: pluralize vowel + `y` endpoints correctly (`dcim.device_bay` → `dcim/device-bays/`, not `dcim/device-baies/`), so reading and writing those types hit the right REST endpoint
- django adapter: pluralize generated drf router endpoints correctly (`prefix` → `prefixes`, not `prefixs`; `gateway` → `gateways`, not `gatewaies`) by sharing one `pluralize` helper across the netbox, nautobot, and django adapters instead of a stale third copy
- core: reject a wrong-typed `required`/`nullable`/`format`/`pattern` in a field schema instead of silently coercing it to the permissive default, so a malformed schema fails to load rather than dropping the constraint
- add alembic-adapter-test, a standalone conformance runner for external adapter executables
- security: bump anyhow 1.0.102 to 1.0.103 to clear RUSTSEC-2026-0190 (unsoundness in Error::downcast_mut); not reachable in our usage
- netbox adapter: paginate the custom-field fetch, so an instance with more than one page of custom fields routes and provisions all of them instead of silently dropping the rest past the first page
- external adapters: surface an adapter's full error cause chain on failure, and forward its stderr to the host on success, instead of dropping those diagnostics
- cli: create the parent directory when writing a plan or ir output, instead of failing if it does not exist
- cli: reject `plan --provision` together with `--dry-run` at parse time, so a `--dry-run` preview (which writes nothing) can't silently provision backend schema via `ensure_schema`
- cli: `plan --report` and `plan --dry-run` no longer require `-o`/`--output`, since neither writes a plan file (`-o` is still accepted for backward compatibility)
- engine: `ProvisionReport` accepts an omitted `created_fields`/`created_tags` on deserialize, so an external `ensure_schema` adapter that creates neither isn't rejected
- engine + cli: `plan` now previews the schema provisioning `apply` would perform (object types and fields to create/deprecate/delete) read-only, writing nothing, and carries it in the plan json as `schema_preview` (visible in `plan --dry-run`), so the see-before-write contract finally covers schema and not just data ops; netbox, nautobot, and infrahub all report it (netbox's preview shares its `ensure_schema` create/delete decisions, so it surfaces custom-object type/field deletions too), a backend that cannot preview prints `schema preview: unavailable for this backend`, and `plan --provision` stays the write escape hatch
- external adapter protocol: add a read-only `preview_schema` request alongside `ensure_schema`; an adapter can now surface what provisioning would do at plan time, and the default returns nothing to preview so existing adapters are unaffected

## [0.5.0] - 2026-06-19

- core: keep source locations on validation errors for dotted type names
- engine: reject lists, dicts, and null as key and uid components
- engine: don't suppress a delete when an undeclared object's backend id collides with a matched object of another type
- netbox adapter: handle NetBox generic foreign keys in both directions. **breaking**: ir field names are now NetBox-faithful (`assigned_object`/`scope`, `primary_ip4`)

## [0.4.0] - 2026-06-18

- **breaking** removed the raw-yaml→ir mapping engine (#41). the `distill` subcommand and the raw→ir compiler (`retort`) are gone; ir now comes only from authored inventory files or backend observation. `import` is backend-only (`import -f <inventory> -o <out>` observes a backend for the inventory's schema types), the `brew` noun is renamed `inventory`, and the command set is `validate / import / map / plan / apply`
- **breaking** removed the `cast` command; django project generation is now a django adapter that runs as part of `write`
- engine + cli: add `map`, an ir→ir transformation layer, via `alembic map -f <ir> --spec <map> -o <ir>` (#43). rules select source objects by type-name pattern with optional field predicates (`dcim.device[attrs.role=leaf]`, globs like `dcim.*`, bare `*`) and emit one or more targets through `${...}` templates with `upper`/`lower`/`trim`/`slug` transforms over a `${uid}`/`${type}`/`${key.*}`/`${attrs.*}` namespace. uids default to `uid_v5(target_type, target_key)`, `ref`/`list_ref` attrs rewire from source to target uids (explicitly via `${uids.name}` for multi-emit), output is validated against the target schema, and the transform is pure so maps chain. rules also support `group_by` (n→1 aggregation, exposing `${group.key}`/`${group.count}`/`${group.items.<path>}`) and `lookups` (read a field off a referenced object as `${lookup.name}`)
- engine + cli: map transforms can be user-defined in [starlark](https://github.com/bazelbuild/starlark) (#99)
- engine: apply orders operations by a topological sort over references (creates/updates after what they reference, deletes in reverse) instead of the retry fixpoint, giving deterministic O(V+E) ordering and correct delete ordering; cycles fall back to a stable order and the retry loop stays a safety net (#47)
- cli + engine: `plan --report` prints a read-only drift report (changed/missing/extra with per-field diffs) and exits without writing a plan or saving state, backed by a new `DriftReport` type (`Display` + `Serialize`). it surfaces the `extra` category (on-backend but undeclared objects) without `--allow-delete`, and rejects `--report` together with `--dry-run` instead of silently ignoring the latter
- engine: add a `journal` module and resumable-apply groundwork (#46, #114). `apply_non_delete_with_retries` can record applied ops (and their backend ids) to a side journal so a re-run resumes the remainder after a mid-apply error; the journal is flushed at the apply exit points (not per op) and deleted on success. opt-in per adapter and not yet wired into any built-in adapter. `ApplyReport` gains `previously_applied_count`, which the cli prints when a run resumed
- engine: `import` projects observed state onto the schema, dropping attrs whose key isn't declared in the type's `fields` (with a `warn` each), so server-computed fields like `dcim.cable.last_updated` no longer fail validation with `extra attr field` (#45)
- cli: document the file formats each subcommand reads and writes (#89). every argument carries help text (`--file` inventories are yaml or json by extension; `plan`/`map`/`import --output` are always json; `apply --plan` is a json plan), `--help` describes the ir format model, and writing json `--output` to a `.yaml`/`.yml` path warns instead of silently producing json under a yaml name
- generic adapter: resolve refs nested inside `list` and `map` fields, consolidating onto the shared `alembic-engine` resolution helpers (`build_key_from_schema`, `resolve_value_for_type`) instead of a divergent local copy that left them as raw uid strings, matching netbox and nautobot

## [0.3.0] - 2026-05-20

- cli: improved support for running external adapters ("plugins")
- cli: can pass config variables to external adapters

## [0.2.0] - 2026-04-27

- cli: print provisioning summary (fields, tags, object types created/deprecated/deleted) when `--provision` is used or during `apply`
- engine: extract django codegen into a standalone `alembic-django` crate (cli now depends on it directly)

## [0.1.2] - 2026-03-06

- improve idempotency for netbox/nautobot create + provisioning (handle already-exists conflicts)
- infrahub: schema provisioning now cleans up stale menu entries, avoids inferred icons, and uses deterministic anchors
- engine: run schema provisioning on apply even when there are no data ops

## [0.1.1] - 2026-03-06

- add external (stdout) adapter support for process-based backends and sdk helpers

## [0.1.0] - 2026-03-06

- initial version
