# configuration

The alembic cli tool can be configured using a layered set of
configuration sources, applied in the following order:

1. Built in app defaults
2. Keys set in `alembic.yaml` (or `alembic.yml`), placed in the working directory
3. Environment variables with the prefix "ALEMBIC_"

## Available configuration keys

- `plugins_dir` where the alembic cli will look for plugin
  configuration files (which must have the file ending `.yaml` or `.yml`).

## Examples

To set a path where to look for plugins, you can use an `alembic.yaml`
config file in your working directory:

```yaml
plugins_dir: "/home/user/alembic_plugins"
```

Same setting but using an environment variable:

```bash
export ALEMBIC_PLUGINS_DIR="/home/user/alembic_plugins"
```
