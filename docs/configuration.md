# configuration

The alembic cli tool can be configured using a layered set of
configuration sources, applied in the following order:

1. Built in app defaults
2. Keys set in `alembic.toml` (file placed in working directory)
3. Environment variables with the prefix "ALEMBIC_"

## Available configuration keys

- `plugin_search_paths` where the alembic cli will look for plugin
  executables. Note that setting a list of paths in your config will
  replace all existing paths, NOT merge them together with other paths.

## Examples

To set plugin search paths using an `alembic.toml` config file:

```toml
plugin_search_paths=["/somewhere/on/computer", "/another/folder"]
```

Same setting but using an environment variable:

```bash
export ALEMBIC_PLUGIN_SEARCH_PATHS=["/somewhere/on/computer", "/another/folder"]
```
