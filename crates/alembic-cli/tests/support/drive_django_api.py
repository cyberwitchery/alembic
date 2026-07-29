"""drive the generated django api in-process: list, filtered list, schema, docs.

argv[1] is the generated project root (the directory holding manage.py).
"""

import os
import sys

sys.path.insert(0, sys.argv[1])
os.environ["DJANGO_SETTINGS_MODULE"] = "alembic_project.settings"

import django

django.setup()

from django.conf import settings
from django.test import Client

# the test client sends Host: testserver, which ALLOWED_HOSTS otherwise rejects.
if "testserver" not in settings.ALLOWED_HOSTS:
    settings.ALLOWED_HOSTS = list(settings.ALLOWED_HOSTS) + ["testserver"]

failures = []


def check(ok, message):
    print(("ok   " if ok else "FAIL ") + message)
    if not ok:
        failures.append(message)


client = Client()


def get(path):
    response = client.get(path)
    print(f"GET {path} -> {response.status_code}")
    return response


def rows(response):
    payload = response.json()
    return payload["results"] if isinstance(payload, dict) else payload


devices = get("/api/dcimdevices/")
check(devices.status_code == 200, "device list answers 200")
spines = get("/api/dcimdevices/?role=spine")
check(spines.status_code == 200, "filtered device list answers 200")

total = len(rows(devices))
filtered = len(rows(spines))
print(f"rows: unfiltered={total} filtered(role=spine)={filtered}")
check(total == 3, f"all three devices are listed (got {total})")
check(
    0 < filtered < total,
    f"role=spine returns strictly fewer rows than the full list (got {filtered})",
)
check(
    all(row["role"] == "spine" for row in rows(spines)),
    "every filtered row has role=spine",
)

check(get("/api/schema/").status_code == 200, "/api/schema/ answers 200")
check(get("/api/docs/").status_code == 200, "/api/docs/ answers 200")

if failures:
    sys.exit("generated django api: " + "; ".join(failures))
