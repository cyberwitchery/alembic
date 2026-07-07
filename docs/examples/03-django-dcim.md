# example: generate a django app from a model

## goal

the django backend is write-only: it does not manage an existing system, it
*emits* one. give it a model and it generates a runnable django app, models,
admin, serializers, and urls, that you can migrate and serve. this is how the same
inventory that converges netbox can also scaffold a bespoke app.

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
python manage.py makemigrations --dry-run --verbosity 2
python manage.py migrate
python manage.py runserver
```

## notes

- the generated app includes models, admin stubs, serializers, and urls; field
  types and relations (the `site` ref becomes a foreign key) come from the schema.
- treat it as a scaffold: generate once, then customize the app.
- because the backend only emits, there is no drift to observe; re-running
  regenerates from the model.
