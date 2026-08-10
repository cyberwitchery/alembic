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

these come from the pinned upstream crate (`peeringdb-rs` 0.1.3), not from the adapter;
see issue #267 for the detail and the shapes it could be fixed in.

- `peeringdb.netixlan` fetches `/api/org`. the adapter refuses a record carrying none
  of `net_id`, `ix_id`, `ixlan_id`, `asn`, `ipaddr4`, `ipaddr6`, `speed` or
  `is_rs_peer`, so a read of that type errors naming the endpoint rather than
  observing organizations as netixlan records. the type is unusable until the pin
  moves; it is no longer silently wrong.
- the authorization header is sent even when `PEERINGDB_API_KEY` is unset, and
  peeringdb answers 400 to it, so anonymous reads are not reachable although the api
  itself allows them.
- the loaders take no query parameters, so every read fetches a whole table. netixlan
  is around 90k rows and peeringdb throttles it hard.
