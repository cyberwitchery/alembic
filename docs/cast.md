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
- inserts the app into `INSTALLED_APPS`
- runs `manage.py check`
- runs `manage.py makemigrations`
- runs `manage.py migrate`
- generates Django models + admin registrations

## file layout

Inside the generated Django app:

- `generated_models.py` (overwritten)
- `generated_admin.py` (overwritten)
- `models.py` (user-owned; imports generated models)
- `admin.py` (user-owned; imports generated admin)
- `extensions.py` (user-owned hooks)

`models.py` and `admin.py` are only replaced if they still contain the default
Django skeleton.

## flags

- `--project <name>`: Django project name (default: `alembic_project`)
- `--app <name>`: Django app name (default: `alembic_app`)
- `--python <path>`: Python executable (default: `python3`)
- `--no-admin`: skip admin generation
- `--no-migrate`: skip `manage.py migrate` (still runs `makemigrations`)

## requirements

- A Python environment with Django installed.
- The `--python` executable must point to that environment.
