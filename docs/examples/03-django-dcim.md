# example: generate a django app from a model

## goal

the django backend is write-only: it does not manage an existing system, it
*emits* one. give it a model and it generates a runnable django app, models,
admin, serializers, and urls, that you can migrate and serve. the objects come
along as a fixture, so what you get is a running app with the inventory already
in it. this is how the same inventory that converges netbox can also scaffold a
bespoke app.

## the model

a site and a device
([`examples/walkthroughs/django-dcim.yaml`](../../examples/walkthroughs/django-dcim.yaml)):

```yaml
    dcim.device:
      key: {name: {type: slug}}
      fields:
        name: {type: string}
        site: {type: ref, target: dcim.site}   # a foreign key in the generated model
```

## backend config

save as `django-config.yaml`; `output` is where the app is written:

```yaml
backend: django
output: /tmp/alembic-adapter-django
project: dcim_project
app: dcim_app
```

## commands

django cannot report existing state, so `plan` produces an all-creates plan (one
op per object), and `apply` generates the app:

```bash
alembic plan  --backend-config ./django-config.yaml \
  -f examples/walkthroughs/django-dcim.yaml -o plan.json
alembic apply --backend-config ./django-config.yaml -p plan.json

cd /tmp/alembic-adapter-django
python manage.py createsuperuser
python manage.py runserver
```

apply already ran `check`, `makemigrations`, `migrate`, and `loaddata`, so the
app is serving the inventory at `/api/` and `/admin/` on the first run.

## optional packages

the generated app only declares what the target interpreter can honour:

| installed | you get |
| --- | --- |
| (nothing extra) | `?search=`, `?ordering=`, paginated list endpoints |
| `django-filter` | per-field filtering (`?role=leaf`, `?site=<uid>`) |
| `drf-spectacular` | an openapi schema at `/api/schema/` and docs at `/api/docs/` |

## notes

- the generated app includes models, admin, serializers, and urls; field types
  and relations (the `site` ref becomes a foreign key) come from the schema.
- the ir uid is the primary key, so relations carry over unchanged and
  re-running `apply` converges the rows instead of duplicating them.
- treat it as a scaffold: generate once, then customize the app. the
  `generated_*.py` files are rewritten on every run, the rest is yours.
- because the backend only emits, there is no drift to observe; re-running
  regenerates from the model. objects removed from the inventory stay in the
  app's database.
