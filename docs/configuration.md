# configuration

the alembic cli can be configured from a layered set of sources, applied in
the following order:

1. built-in app defaults
2. keys set in `alembic.yaml` (or `alembic.yml`), placed in the working directory
3. environment variables with the prefix `ALEMBIC_`

## available configuration keys

- `plugins_dir` where the alembic cli will look for plugin
  configuration files (which must have the file ending `.yaml` or `.yml`).

## examples

to set a path where to look for plugins, use an `alembic.yaml` config file in
your working directory:

```yaml
plugins_dir: "/home/user/alembic_plugins"
```

the same setting as an environment variable:

```bash
export ALEMBIC_PLUGINS_DIR="/home/user/alembic_plugins"
```
