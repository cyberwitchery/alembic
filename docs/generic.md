# generic rest adapter

the generic rest adapter maps alembic ir types to plain rest endpoints described in a
config file. it makes no assumptions about the backend beyond list/create/update/delete
over json, so it fits apis the netbox, nautobot, and infrahub adapters do not cover.

the backend is selected with `backend: generic` and a `config_path` pointing at the
adapter config (see docs/cli.md); the path can also come from `GENERIC_CONFIG`.

## config

the config file (see examples/generic.yaml) has three top-level keys:

- `base_url` (required) - api base; each type's `path` is appended to it.
- `headers` (optional, default empty) - default headers sent on every request; put auth
  here (for example `Authorization: Token ...`). an invalid header name or value errors
  when the adapter is constructed.
- `types` (required) - map of ir type name to endpoint config. a type with no entry has no
  endpoint, and observing or applying it errors.

each `types` entry is an endpoint config:

- `path` (required) - path for listing and creating objects.
- `results_path` (optional) - json path to the results array in a list response; defaults
  to the response root.
- `id_path` (optional, default `id`) - json path to an object's id in a response.
- `delete_strategy` (optional, default `none`) - `none` or `standard` (see below).
- `update_method` (optional, default `PATCH`) - `PATCH` or `PUT`; any other value errors
  when the adapter is constructed.

`results_path` and `id_path` are dotted paths (for example `results` or `data.id`).

## observe and apply

- observe issues one GET per type to `path`, takes the array at `results_path` (or the
  response root), and reads each object's id from `id_path`.
- create POSTs to `path` with the object's attrs as the json body and reads the new id from
  the response via `id_path`. on a create conflict (409) the adapter re-lists the endpoint
  and reuses the existing object whose key matches; if none matches, the conflict is returned.
- update sends `update_method` (patch or put) to `path`/`id` with the attrs as the json body.
- delete with `delete_strategy: standard` sends DELETE to `path`/`id` (a 404 is treated as
  already gone); with the default `none`, delete ops fail.

## attrs mapping

- nested references should be provided as alembic uids (string UUIDs). the adapter resolves
  those to backend ids (number or string) before sending requests, including refs nested in
  list and map fields.
- if a referenced uid cannot be resolved (not in state or created earlier in the same apply),
  apply fails with a missing reference error.
- on observe, ref-typed fields are normalized back to alembic uids using state.

## keys and matching

- keys are derived from the schema key fields when observing objects.
- key matching uses the canonical form of the key map; it reconciles an existing object
  during create-conflict recovery.

## schema

- the adapter never provisions schema; it assumes the backend schema already exists.
- `preview` reports nothing to provision, rather than a capability gap.

## known limitations

- reads a single list response per type; pagination is not followed, so each type's endpoint
  must return all of that type's objects in one response.
- `update_method` must be `PATCH` or `PUT`.
- deletes require `delete_strategy: standard`; with the default `none`, delete ops fail.
- no schema provisioning; the backend schema must already exist.
