"""put a declared list's element check through the generated serializer and model.

`apply` already ran makemigrations, migrate and loaddata, so the member check
is serializable into a migration by the time this runs. this checks the half
that only shows through the api: a member the schema does not declare is a 400,
and one it does declare goes through.

argv[1] is the generated project root (the directory holding manage.py).
argv[2] is core's verdict on every (list field, member) pair, which the
generated python has to agree with.
"""

import json
import os
import sys

sys.path.insert(0, sys.argv[1])
os.environ["DJANGO_SETTINGS_MODULE"] = "alembic_project.settings"

import django

django.setup()

from django.core.exceptions import ValidationError

from alembic_app.generated_models import DcimInterface
from alembic_app.generated_serializers import DcimInterfaceSerializer

failures = []


def check(ok, message):
    print(("ok   " if ok else "FAIL ") + message)
    if not ok:
        failures.append(message)


def accepts(**attrs):
    serializer = DcimInterfaceSerializer(
        data={"key": "name=eth1", "name": "eth1", **attrs}
    )
    return serializer.is_valid(), serializer.errors


def cleans(**attrs):
    """the other surface the checks run on: the model, as the admin drives it."""
    try:
        DcimInterface(key="name=eth1", name="eth1", **attrs).full_clean()
        return True
    except ValidationError:
        return False


ok, errors = accepts(modes=["access", "trunk"])
check(ok, f"declared enum members are accepted (got {errors})")

ok, errors = accepts(modes=["bogus"])
check(not ok, "an undeclared enum member is rejected")
check("modes" in errors, f"the rejection names the field (got {errors})")

ok, errors = accepts(peers=["aa:bb:cc:dd:ee:ff", "AA-BB-CC-DD-EE-FF"])
check(ok, f"both mac spellings validate accepts are accepted (got {errors})")

ok, _ = accepts(peers=["not-a-mac"])
check(not ok, "a member that is not a mac address is rejected")

ok, _ = accepts(peers=[7])
check(not ok, "a non-string member of a mac list is rejected")

# a string element declares no format, but core still holds every member to a
# string, and so does the generated column.
ok, _ = accepts(tags=["anything", 7, None])
check(not ok, "a non-string member of a string list is rejected")

ok, errors = accepts(tags=["anything", "goes"])
check(ok, f"any string is a member of a string list (got {errors})")

ok, errors = accepts(modes=[])
check(ok, f"an empty list is accepted (got {errors})")

# core recurses into a nested collection's entries, so only the outer shape
# ships -- but on that shape core is exact, so the wrong one is still a 400.
ok, _ = accepts(nested=["notalist"])
check(not ok, "a member of a list-of-lists that is not a list is rejected")

ok, errors = accepts(nested=[[7], []])
check(ok, f"a list member of a list-of-lists is accepted (got {errors})")

ok, _ = accepts(labels=[[]])
check(not ok, "a member of a list-of-maps that is not a map is rejected")

ok, errors = accepts(labels=[{"a": 1}])
check(ok, f"a map member of a list-of-maps is accepted (got {errors})")

# a ref member is parsed as a uid; resolving it needs an inventory this app
# does not have, so that half is the leniency pinned below.
ok, _ = accepts(refs=["not-a-uuid"])
check(not ok, "a ref member that is not a uid is rejected")

ok, errors = accepts(refs=["44444444-4444-4444-4444-444444444444"])
check(ok, f"a well-formed uid is accepted as a ref member (got {errors})")

corpus = json.load(open(sys.argv[2]))
list_fields = sorted({case["field"] for case in corpus})

checked = {
    field.name
    for field in DcimInterface._meta.get_fields()
    if any(
        type(validator).__name__ == "_ListMembers"
        for validator in getattr(field, "validators", [])
    )
}
check(
    sorted(checked) == list_fields,
    f"every declared list carries the check (got {sorted(checked)})",
)

# the corpus only ever passes lists, so the field-level shape is its blind spot:
# core takes an array for a declared list whatever the element type, and a list
# whose element carries no check has to be held to that much too.
for field in list_fields:
    for value in ["notalist", 7, {"a": 1}]:
        ok, _ = accepts(**{field: value})
        check(not ok, f"{field} refuses the non-list value {value!r}")
        check(not cleans(**{field: value}), f"{field} refuses {value!r} on full_clean()")

# `""` and `{}` are django's own empty values, so `full_clean()` never reaches
# the check with them: this is the leniency docs/django.md states, pinned here
# so tightening it, or widening it past these two, fails rather than drifts.
for field in list_fields:
    for value in ["", {}]:
        check(cleans(**{field: value}), f"{field} takes the empty value {value!r} on full_clean()")
        ok, _ = accepts(**{field: value})
        check(not ok, f"{field} refuses the empty value {value!r} through the serializer")

# core's verdict comes from the rust side; this is the only place the check that
# ships answers for itself, so a divergence between the two regex engines shows
# up here and nowhere else.
#
# every field that takes a member core refuses, grouped by why. the fixture
# declares one list per element type, so this characterises the mapping: a new
# leniency, or one of these becoming exact, fails the comparison below.
LENIENT = {
    # `blobs` is json, which core takes any value for bar null. core reads the
    # other three as rfc 3339 and checks the calendar with them, which django's
    # own parser does not mirror.
    "no check": ["blobs", "clocks", "dates", "stamps"],
    # `format_regex` is by contract the widest regex accepting everything the
    # parse behind it accepts, and a ref's uid still has to resolve against an
    # inventory the generated app cannot see.
    "a check core calls a superset": ["nets", "pools", "refs"],
    # core recurses into the entries of these; only the outer shape ships.
    "the outer shape only": ["groups", "labels", "nested"],
}
expected = sorted(field for group in LENIENT.values() for field in group)

lenient = []
for case in corpus:
    field, member, core = case["field"], case["member"], case["core"]
    ok, _ = accepts(**{field: [member]})
    if core and not ok:
        check(False, f"core accepts {member!r} in {field}, the generated app rejects it")
    elif not core and ok:
        lenient.append((field, member))

loose = sorted({field for field, _ in lenient})
check(loose == expected, f"only {expected} take a member core refuses (got {loose})")
print(f"     {len(lenient)} lenient pairs: {lenient}")

if failures:
    sys.exit("generated django list columns: " + "; ".join(failures))
