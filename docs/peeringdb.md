# peeringdb adapter

the peeringdb adapter is read-only: it observes the public peeringdb api and never
writes to it. `plan --report` and `import` work against it; `apply` is rejected right
after the backend is constructed, with `backend is read-only; it cannot apply changes`.

## backend config

```yaml
backend: peeringdb
```

the adapter reads no config keys at all, so the file has nothing but the `backend:`
tag; `--backend peeringdb` with no config file is equivalent. any other key is a parse
error naming it, rather than a key discarded in silence (see `docs/cli.md`). that
matters because the credential does not live here: authentication is the
`PEERINGDB_API_KEY` environment variable, so a `token:` copied from a netbox config
would otherwise leave the run unauthenticated.

## supported types

- `peeringdb.ix`
- `peeringdb.net`
- `peeringdb.org`
- `peeringdb.netixlan`

an empty `types` observes every supported type *the schema declares*, so a schema that
declares none of them observes nothing rather than erroring. an explicitly requested
type outside the four is `unsupported type <name>`, the same error netbox and nautobot
give: `plan` and `import` name every schema-declared type explicitly, so a schema
declaring one of peeringdb's other object types fails there rather than observing it as
absent.

## attrs and keys

- the upstream record is serialized whole into attrs, so a field peeringdb returns is
  an attr whether the schema declares it or not.
- the key is built from the schema's declared key fields, so the schema you write
  decides object identity. a key field missing from the record is an error.
- `backend_id` is peeringdb's own integer id.

## known limitations

- every read fetches a whole table. `peeringdb-rs` 0.2.0 added filtered variants
  (`load_peeringdb_*_filtered`) that take peeringdb's own query parameters, and the
  adapter does not use them yet, so a netixlan read still pulls around 90k rows and
  peeringdb throttles it hard.
- anonymous reads work but are rate-limited; set `PEERINGDB_API_KEY` for anything
  beyond a small read.
