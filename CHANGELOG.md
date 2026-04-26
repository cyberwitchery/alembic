# changelog

## [unreleased]

## [0.2.0] - 2026-04-27

- cli: print provisioning summary (fields, tags, object types created/deprecated/deleted) when `--provision` is used or during `apply`
- engine: extract django codegen into a standalone `alembic-django` crate (cli now depends on it directly)

## [0.1.2] - 2026-03-06

- improve idempotency for netbox/nautobot create + provisioning (handle already-exists conflicts)
- infrahub: schema provisioning now cleans up stale menu entries, avoids inferred icons, and uses deterministic anchors
- engine: run schema provisioning on apply even when there are no data ops

## [0.1.1] - 2026-03-06

- add external (stdout) adapter support for process-based backends and sdk helpers

## [0.1.0] - 2026-03-06

- initial version
