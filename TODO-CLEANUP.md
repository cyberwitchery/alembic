# Alembic Codebase Cleanup Targets

## Completed

### `pipeline.rs:25-28` - Double backend read
**Fixed:** Removed the second `adapter.read()` call. Now only reads once and updates state in-place via `bootstrap_state_from_observed()`.

### `retort.rs` - Version check
**Fixed:** Removed version check and `Retort.version` field. No migration path needed for v1-only schema.

### `external.rs:118-123` - Silent empty input
**Fixed:** Removed early return for empty input. Now returns proper error when parsing fails.

### `planner.rs:140-157` - Duplicate sort logic
**Fixed:** Simplified `sort_ops_for_apply` to use single sort with weight key instead of duplicating comparison logic.

### `pipeline.rs` - Double backend read
**Fixed:** Removed second read, only calls `adapter.read()` once.

### `apply_retry.rs:28-43` - Inefficient retry loop
**Fixed:** Changed to track `applied` count instead of `pending.len()` for progress detection.

### `netbox/nautobot client.rs` - Duplicated pagination
**Fixed:** Kept inline for simplicity (only ~15 lines each).

## Remaining

### `infrahub adapter:440-448`
**Status:** Code already awaits properly with `?`. No fix needed.

### `telemetry.rs:16-24`
**Issue:** `init_tracing` uses `.try_init()` which silently fails if tracing is already initialized.
**Recommendation:** Add guard or use proper error handling.

### `validation.rs:52-96`
**Status:** Helper methods (`uid()`, `key_hint()`, `type_hint()`) are used in `with_sources()`. Keep as-is.

### `cast_django_e2e.rs:5-13`
**Status:** Minimal E2E tests present. Could be expanded if needed.

### `docs/state.md:12-15`
**Issue:** Examples show only integer backend IDs.
**Recommendation:** Update to include string UUID examples.

### `docs/retort.md:3-34`
**Issue:** Example shows `version: 1` but code doesn't validate versions.
**Recommendation:** Remove version requirement from examples.

### `scripts/ci.sh`
**Issue:** Coverage flag `--fail-under-lines 80` without actual coverage threshold.
**Recommendation:** Either add coverage config or remove the flag.

### `cli/tests/support/mod.rs:5-11`
**Issue:** References non-existent `fixtures/` directory.
**Recommendation:** Create fixtures or remove fixture tests.

### `state.rs:59-60`
**Issue:** Backend created but unused if file doesn't exist.
**Recommendation:** Lazy initialization on first use.

### `tests.rs:556-587`
**Issue:** Test uses extra fields not in schema.
**Recommendation:** Update test schema or remove extra fields.
