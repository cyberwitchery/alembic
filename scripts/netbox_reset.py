# empties a netbox for scripts/doc_examples.sh, short of tearing it down: every
# custom object type, then every object in the apps an inventory can write to.
# users, tokens and netbox's own bookkeeping (core) stay, so a backend config
# minted before the reset still authenticates. run inside the container:
#
#   manage.py shell < scripts/netbox_reset.py
import sys

from django.apps import apps
from django.db.models import ProtectedError
from django.db.models.deletion import RestrictedError
from netbox_custom_objects.models import CustomObjectType

# one at a time: the instance delete drops the type's tables, a queryset one
# would leave them behind.
for custom_type in CustomObjectType.objects.all():
    custom_type.delete()

CLEARED = {"circuits", "dcim", "extras", "ipam", "tenancy", "virtualization", "vpn", "wireless"}
models = [
    model
    for model in apps.get_models()
    if model._meta.app_label in CLEARED and model._meta.managed and not model._meta.proxy
]

# protected and restricted foreign keys block a delete until what points at the
# row is gone, so sweep until nothing is left or a sweep frees nothing.
while True:
    blocked = []
    progressed = False
    for model in models:
        if not model.objects.exists():
            continue
        try:
            deleted, _ = model.objects.all().delete()
            progressed = progressed or deleted > 0
        except (ProtectedError, RestrictedError):
            blocked.append(model._meta.label)
    if not blocked:
        break
    if not progressed:
        print("netbox reset is stuck on: " + ", ".join(blocked), file=sys.stderr)
        sys.exit(1)

print("netbox reset")
