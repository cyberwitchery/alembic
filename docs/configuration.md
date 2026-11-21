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

the `ALEMBIC_STATE_*` variables are a separate, env-only surface: they select the
state backend rather than a configuration key, and cannot be set in
`alembic.yaml`. see [state](state.md) for the full list.

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

## proxies

the http adapters (netbox, nautobot, infrahub, generic, peeringdb) reach their
backend through the proxy the environment names. for each scheme the first of
these that is set wins:

1. `HTTP_PROXY` for `http://` urls, `HTTPS_PROXY` for `https://` ones
2. on macos and windows, the system proxy settings
3. `ALL_PROXY`

of the system settings, macos gives only the http and https proxy host and
port: its "bypass proxy settings for these hosts & domains" list and "exclude
simple hostnames" are not read, so a host listed only there goes through the
proxy and belongs in `NO_PROXY`. windows also gives its bypass list
(`ProxyOverride`), which is used when `NO_PROXY` is unset.

a proxy url must be `http://` or `https://`. the adapters are built without
socks support, so if `HTTP_PROXY`, `HTTPS_PROXY` or `ALL_PROXY` gives a backend
a `socks4://`, `socks4a://`, `socks5://` or `socks5h://` proxy, the run fails
before anything is sent: exit `69` with `unsupported scheme`, or exit `1` from
peeringdb. to keep such a url set, add the backend to `NO_PROXY`.

`NO_PROXY` lists what is reached directly, comma-separated: hosts, domains (a
domain covers its subdomains), ip addresses and cidr ranges; `*` covers every
host name, but not an ip address. each variable is also read in lowercase when
the uppercase one is unset. external adapters inherit the environment and
decide for themselves.

```bash
export HTTPS_PROXY="http://proxy.example.com:3128"
export NO_PROXY="localhost,127.0.0.1,.corp.example.com"
```
