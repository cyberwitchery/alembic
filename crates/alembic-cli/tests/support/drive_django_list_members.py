"""put a declared list's element check through the generated drf serializer.

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

# the model itself says which lists carry a check, so the corpus below asks for
# exact agreement only where one was emitted.
checked = {
    field.name
    for field in DcimInterface._meta.get_fields()
    if any(
        type(validator).__name__ == "_ListMembers"
        for validator in getattr(field, "validators", [])
    )
}
check(bool(checked), "the generated model carries member checks")

# core's verdict comes from the rust side; this is the only place the check that
# ships answers for itself, so a divergence between the two regex engines shows
# up here and nowhere else.
#
# `dates` and `blobs` carry no check. `nets` and `pools` carry one that core
# calls a superset by contract: `format_regex` is the widest regex accepting
# everything `matches_format` does, and a cidr is parsed, not matched. so those
# four take members core refuses, and no other field may.
LENIENT = ["blobs", "dates", "nets", "pools"]

lenient = []
for case in json.load(open(sys.argv[2])):
    field, member, core = case["field"], case["member"], case["core"]
    ok, _ = accepts(**{field: [member]})
    if core and not ok:
        check(False, f"core accepts {member!r} in {field}, the generated app rejects it")
    elif not core and ok:
        lenient.append((field, member))

loose = sorted({field for field, _ in lenient})
check(loose == LENIENT, f"only {LENIENT} take a member core refuses (got {loose})")
print(f"     {len(lenient)} lenient pairs: {lenient}")

if failures:
    sys.exit("generated django list columns: " + "; ".join(failures))
