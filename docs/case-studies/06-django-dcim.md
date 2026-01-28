# case study: simple django-based dcim

## goal

generate a minimal django app from an alembic inventory and run migrations.

## inventory

```yaml
schema:
  types:
    dcim.site:
      key:
        slug:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
    dcim.device:
      key:
        name:
          type: slug
      fields:
        name:
          type: string
        site:
          type: ref
          target: dcim.site
objects:
  - uid: "11111111-1111-1111-1111-111111111111"
    type: dcim.site
    key:
      slug: "lab1"
    attrs:
      name: "lab1"
      slug: "lab1"

  - uid: "22222222-2222-2222-2222-222222222222"
    type: dcim.device
    key:
      name: "leaf01"
    attrs:
      name: "leaf01"
      site: "11111111-1111-1111-1111-111111111111"
```

## commands

```bash
alembic cast django -f /path/to/inventory.yaml -o /tmp/alembic-django \
  --project dcim_project --app dcim_app

cd /tmp/alembic-django
python manage.py makemigrations --dry-run --verbosity 2
python manage.py migrate
python manage.py runserver
```

## notes

- the generated app includes models, admin stubs, and serializers.
- field types and relations come from the schema.
- treat this as a scaffold, then customize.
