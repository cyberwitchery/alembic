# cast (django)

`alembic cast django` turns a brew/ir file into a runnable Django project + app.
It is regen-safe: generated files are overwritten, while user-owned files are
created once and then preserved.

## quickstart

```bash
alembic cast django -f examples/brew.yaml -o ./out \
  --project alembic_project \
  --app alembic_app \
  --python python3
```

By default, the command:

- runs `django-admin startproject`
- runs `python manage.py startapp`
- inserts the app and `rest_framework` into `INSTALLED_APPS`
- wires `path("api/", include("<app>.urls"))` in the project urls
- runs `manage.py check`
- runs `manage.py makemigrations`
- runs `manage.py migrate`
- generates Django models, admin registrations, serializers, viewsets, and urls

## file layout

Inside the generated Django app:

- `generated_models.py` (overwritten)
- `generated_admin.py` (overwritten)
- `generated_serializers.py` (overwritten)
- `generated_views.py` (overwritten)
- `generated_urls.py` (overwritten)
- `models.py` (user-owned; imports generated models)
- `admin.py` (user-owned; imports generated admin)
- `serializers.py` (user-owned; imports generated serializers)
- `views.py` (user-owned; imports generated viewsets)
- `urls.py` (user-owned; imports generated urls)
- `extensions.py` (user-owned hooks)

`models.py`, `admin.py`, and `views.py` are only replaced if they still contain
the default Django skeleton.

## api surface

By default, the generated app exposes DRF viewsets under `/api/` and an OpenAPI
schema at `/api/schema/`.

## flags

- `--project <name>`: Django project name (default: `alembic_project`)
- `--app <name>`: Django app name (default: `alembic_app`)
- `--python <path>`: Python executable (default: `python3`)
- `--no-admin`: skip admin generation
- `--no-migrate`: skip `manage.py migrate` (still runs `makemigrations`)

## requirements

- A Python environment with Django installed.
- Django REST Framework installed (`pip install djangorestframework`).
- The `--python` executable must point to that environment.
