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

there is one fixture per protocol case: `read`, `write`, `ensure_schema`, an
adapter `error`, and a `version_mismatch`.

these fixtures are the compatibility contract for adapter implementations in any
language. the rust harness in `crates/alembic-engine/tests/external_protocol.rs`
runs an adapter command for each fixture and checks the process boundary (one
json line on stdout, a valid envelope, a payload that fits the method); other
sdks should load the same files and check their adapter the same way.
