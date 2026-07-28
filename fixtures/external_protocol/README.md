# external adapter protocol fixtures

canonical request/response pairs for the [external adapter
protocol](../../docs/external-adapters.md). each file describes one exchange:

```json
{
  "description": "...",
  "request":  { /* envelope sent to the adapter on stdin */ },
  "response": { /* envelope the adapter must emit on stdout, as one json line */ }
}
```

there is one fixture per protocol case: `read`, `write`, `ensure_schema`,
`preview_schema`, a `preview_schema` that reports it cannot preview (a null
result), `capabilities`, an adapter `error`, and a `version_mismatch`.

these fixtures are the compatibility contract for adapter implementations in any
language: feed each `request` on stdin and emit the matching `response` on stdout.
`alembic-adapter-test` checks an adapter executable against this protocol, both
with built-in checks and with `--cases` files in the same `{name,request,expect}`
form as `crates/alembic-adapter-test/examples/cases/`; other sdks can load these
files and check their adapter the same way.
