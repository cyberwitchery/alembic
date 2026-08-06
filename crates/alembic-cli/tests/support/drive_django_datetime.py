"""read back the date/datetime/time rows `loaddata` parsed, through the orm.

`apply` already ran `loaddata`, so django's own `DateField`/`DateTimeField`/
`TimeField` have parsed every value in fixtures/django_datetime.yaml by the time
this runs -- a shape `validate` accepts and django refuses fails the apply first.
this checks the second half: that the parse landed on the value alembic meant,
rather than on some other datetime django was willing to read the string as.

argv[1] is the generated project root (the directory holding manage.py).
"""

import datetime
import os
import sys

sys.path.insert(0, sys.argv[1])
os.environ["DJANGO_SETTINGS_MODULE"] = "alembic_project.settings"

import django

django.setup()

from alembic_app.generated_models import OpsWindow

failures = []


def check(ok, message):
    print(("ok   " if ok else "FAIL ") + message)
    if not ok:
        failures.append(message)


rows = {row.name: row for row in OpsWindow.objects.all()}
print(f"rows: {sorted(rows)}")
check(len(rows) == 6, f"all six windows loaded (got {len(rows)})")

utc = datetime.timezone.utc

# the year bounds are stored as written, not clamped.
check(
    rows["lower bound"].starts_on == datetime.date(1, 1, 1),
    f"lower bound date is 0001-01-01 (got {rows['lower bound'].starts_on})",
)
check(
    rows["upper bound"].starts_on == datetime.date(9999, 12, 31),
    f"upper bound date is 9999-12-31 (got {rows['upper bound'].starts_on})",
)

# a +02:00 offset is a real offset, so the stored instant is two hours earlier.
check(
    rows["leap day"].starts_at == datetime.datetime(2024, 2, 29, 10, 0, tzinfo=utc),
    f"leap day datetime carries its +02:00 offset (got {rows['leap day'].starts_at})",
)
check(
    rows["leap day"].opens_at == datetime.time(12, 0, 0, 123456),
    f"fractional seconds survive to microseconds (got {rows['leap day'].opens_at})",
)

# a lowercase `t` separator parses to the same instant an uppercase one would.
check(
    rows["lowercase separator"].starts_at
    == datetime.datetime(2026, 8, 1, 22, 0, tzinfo=utc),
    "a lowercase separator parses as the same instant "
    f"(got {rows['lowercase separator'].starts_at})",
)
check(
    rows["negative offset"].starts_at
    == datetime.datetime(2026, 8, 2, 3, 30, tzinfo=utc),
    f"a -05:30 offset shifts forward (got {rows['negative offset'].starts_at})",
)

# the naive value is the one the ir deliberately still accepts; django reads it
# in the project's own TIME_ZONE, so only the wall clock is pinned here.
naive = rows["naive"].starts_at
check(
    (naive.year, naive.month, naive.day) == (2026, 8, 1),
    f"the naive datetime kept its date (got {naive})",
)

if failures:
    sys.exit("generated django date columns: " + "; ".join(failures))
