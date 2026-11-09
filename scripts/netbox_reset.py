# empties a netbox for scripts/doc_examples.sh. run inside the container:
#
#   manage.py shell < scripts/netbox_reset.py
import sys

from django.apps import apps
from django.db.models import ProtectedError
from django.db.models.deletion import RestrictedError
from netbox_custom_objects.models import CustomObjectType

for custom_type in CustomObjectType.objects.all():
    custom_type.delete()

CLEARED = {"circuits", "dcim", "extras", "ipam", "tenancy", "virtualization", "vpn", "wireless"}
models = [
    model
    for model in apps.get_models()
    if model._meta.app_label in CLEARED and model._meta.managed and not model._meta.proxy
]

# sweep until nothing is left
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
