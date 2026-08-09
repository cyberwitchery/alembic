# django adapter

the django adapter is write-only: it does not converge an existing system, it
emits one. given a schema and objects it generates a runnable django app with
models, admin, drf serializers, viewsets, and urls, then migrates it and loads
the objects as a fixture.

for a worked end-to-end run, see `docs/examples/03-django-dcim.md`.

## backend config

```yaml
backend: django
output: /tmp/alembic-adapter-django
project: dcim_project
app: dcim_app
```

| key | default | meaning |
| --- | --- | --- |
| `output` | *required* | directory the app is written to |
| `project` | `alembic_project` | django project name |
| `app` | `alembic_app` | django app name |
| `python` | `python3` | interpreter used for `manage.py` and to detect optional packages |
| `no_migrate` | `false` | skip `migrate` and `loaddata`, so the app is generated but no database is written |
| `no_admin` | `false` | skip admin registration |

unknown keys are rejected (see `docs/cli.md`), so a typo is an error rather than
a silently ignored key. that matters most for the two booleans: a discarded
`no_migrate` would migrate and load a database you meant to leave untouched.

## emitter semantics

- the backend cannot report existing state, so `plan` diffs against an empty
  observation and produces an all-creates plan, one op per object.
- `import` is rejected up front: there is nothing to observe.
- `plan --report` is rejected for the same reason. a drift report asserts what
  the backend holds, so over this backend it would report every declared object
  missing on every run, having read nothing. plain `plan` is unaffected.
- `apply` skips adapter provisioning (`ensure_schema`); the generated migrations
  are the schema.
- the ir uid is the model's primary key, so re-running `apply` converges the
  existing rows instead of duplicating them. objects removed from the inventory
  stay in the app's database.

## generated files

the `generated_*.py` files (`generated_models.py`, `generated_admin.py`,
`generated_serializers.py`, `generated_views.py`, `generated_urls.py`) and the
`<app>/fixtures/alembic.json` fixture are rewritten on every run. everything else the scaffold
creates is yours to edit: treat the app as a starting point, not an artifact to
regenerate over.

## declared constraints

a scalar field carries what it declares: `format:` and `pattern:` become
`RegexValidator`s, a `cidr`, `prefix` or `mac` type becomes one from its own
format, an `enum` becomes the field's `choices`, and `uuid` and `slug` get the
django fields that already check them.

a `list` is a `JSONField`, which has no native element type, so a declared
element is carried as a member check (`_ListMembers`) instead: a `list` of
`enum` takes only the declared values, a `list` of `uuid`, `cidr`, `prefix`,
`mac` or `slug` only members matching that format, and a `list` of `string`,
`text`, `ip_address`, `int`, `float` or `bool` only members of the json type
`validate` requires there.

a `date`, `datetime` or `time` element carries no check: `validate` reads those
as rfc 3339 and checks the calendar with them, while django's own parser takes
shapes it refuses, so a check here would be an approximation either way. a
`json` element carries none either, because `validate` takes any json there.
the column still carries the validator, holding the field itself to a list:
`validate` takes an array there whatever the element type, so a declared `list`
that is not one is refused even where its members go unread.

a nested collection carries its outer shape: a `list` or `list_ref` element
takes only a list, a `map` element only a map. `validate` recurses into the
entries, which this check does not, so the shape is the part of it that is
exact. a `ref` element takes the uid spellings `validate` parses; resolving one
against the inventory is the half the generated app cannot see. a `map` field
itself, as opposed to a `map` element, stays plain json.

no check is stricter than `validate`, so nothing alembic accepts is rejected
here. the ones that are looser are looser in a stated way: a `cidr` or `prefix`
member is held to the regex a backend would install for that format, which is
deliberately wider than the parse `validate` runs; a nested collection's
entries go unread; a `ref` member is parsed but not resolved; and an element
carrying no check takes the null member `validate` refuses everywhere. a member
check matches a format regex in full, while a scalar keeps django's
`RegexValidator`, which searches, as `validate` does for a `pattern:` of your
own.

the checks run on `full_clean()` and on the drf serializers, so the generated
api enforces them; `loaddata` does not run validators, so the fixture loads
whatever `validate` passed.

## optional packages

the generated app only declares what the target interpreter can honour:

| installed | you get |
| --- | --- |
| (nothing extra) | `?search=`, `?ordering=`, paginated list endpoints |
| `django-filter` | per-field filtering (`?role=leaf`, `?site=<uid>`) |
| `drf-spectacular` | an openapi schema at `/api/schema/` and docs at `/api/docs/` |

`search_fields` covers text columns only, since the `icontains` lookup has no
postgres operator for uuid or inet columns, and json columns stay out of
`filterset_fields` because django-filter cannot resolve them.

## name rules

field and type names are interpolated into python source verbatim, so the
adapter rejects names django or python would break on. every violation in the
schema is collected into one error. a name is rejected when it:

- is not a valid python identifier, or is a python keyword
- is `uid`, `key`, or `attrs`, which would shadow the generated model attributes
- is `pk`, `id`, `objects`, `save`, `delete`, `clean`, or `_state`, which django
  already gives a meaning on a model instance
- ends with an underscore, or contains a double underscore (django's
  `fields.E001`/`fields.E002`)

two structural rejections apply to the schema as a whole:

- two types that render to the same model class name, or to the same api route
- a `ref`/`list_ref` whose target is not itself a type in the schema; the django
  backend can only relate to types in the same model

## known limitations

- the generated app is a scaffold. schema changes regenerate the models, but
  reconciling them with migrations you have already applied is yours.
