"""put a declared list's element check through the generated drf serializer.

`apply` already ran makemigrations, migrate and loaddata, so the member check
is serializable into a migration by the time this runs. this checks the half
that only shows through the api: a member the schema does not declare is a 400,
and one it does declare goes through.

argv[1] is the generated project root (the directory holding manage.py).
"""

import os
import sys

sys.path.insert(0, sys.argv[1])
os.environ["DJANGO_SETTINGS_MODULE"] = "alembic_project.settings"

import django

django.setup()

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

# a string element declares no constraint, so the column takes any json.
ok, errors = accepts(tags=["anything", 7, None])
check(ok, f"a list of plain strings is unconstrained (got {errors})")

ok, errors = accepts(modes=[])
check(ok, f"an empty list is accepted (got {errors})")

if failures:
    sys.exit("generated django list columns: " + "; ".join(failures))
