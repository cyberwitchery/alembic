# configuration

the alembic cli can be configured from a layered set of sources, applied in
the following order:

1. built-in app defaults
2. keys set in `alembic.yaml` (or `alembic.yml`), placed in the working directory
3. environment variables named after a configuration key, with the prefix `ALEMBIC_`

the config file rejects a key it does not know. typos surface as errors,
not silently ignored settings.

## available configuration keys

- `plugins_dir` where the alembic cli will look for plugin
  configuration files (which must have the file ending `.yaml` or `.yml`).

- `chatops_backend` if set, this will send chat notifications; see [chatops](chatops.md).

the `ALEMBIC_STATE_*` variables are a separate, env-only surface: they select the
state backend rather than a configuration key, and cannot be set in
`alembic.yaml`. see [state](state.md) for the full list.

- `machine_id_override`, the machine id is used for hashing but can be
  set manually which is useful for tests

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
