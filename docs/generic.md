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
- `next_path` (optional) - json path to the next-page url in a list response; unset means
  one request per list (see below).
- `id_path` (optional, default `id`) - json path to an object's id in a response.
- `delete_strategy` (optional, default `none`) - `none` or `standard` (see below).
- `update_method` (optional, default `PATCH`) - `PATCH` or `PUT`; any other value errors
  when the adapter is constructed.

`results_path`, `next_path` and `id_path` are dotted paths (for example `results`,
`links.next` or `data.id`).

a key that is not one of the above is a parse error naming it, at both levels and whether
the config is inline under `config:` or in its own file. every one of these keys defaults
silently otherwise, so a typo'd `headers` would send every request unauthenticated and a
typo'd `delete_strategy` would fail the delete ops the plan showed.

## observe and apply

- observe GETs `path` per type, takes the array at `results_path` (or the response root),
  and reads each object's id from `id_path`. with `next_path` set it then follows the page
  chain (see below) and observes every page; without it, exactly one request is issued.
- create POSTs to `path` with the object's attrs as the json body and reads the new id from
  the response via `id_path`. on a create conflict (409) the adapter re-lists the endpoint,
  following pagination the same way observe does, and reuses the existing object whose key
  matches; if none matches, the conflict is returned.
- update sends `update_method` (patch or put) to `path`/`id` with the attrs as the json body.
- delete with `delete_strategy: standard` sends DELETE to `path`/`id` (a 404 is treated as
  already gone); with the default `none`, delete ops fail.

## pagination

`next_path` points at the url of the next page, the shape drf, netbox, nautobot and
peeringdb all return (`{"next": "...?offset=50", "results": [...]}`, so `next_path: next`).
each page's results are appended and the walk continues until the page chain ends.

- the chain ends on an absent key, an explicit `null`, or an empty string. any other shape
  there (a number, an object) is an error rather than a silent stop, since stopping early
  is what this follows the chain to avoid.
- a relative next resolves against the page that returned it, so `?page=2` and
  `/api/devices?page=2` both work.
- a next url on another scheme or host is refused. `headers` is sent on every request the
  adapter makes, so following one would hand the api token to that host.
- a next url already fetched in the same walk is refused, rather than looping forever.
- an empty page that still advertises a next is followed.

leave `next_path` unset for an api that returns everything in one response; the endpoint is
then fetched exactly once, and an endpoint that paginates silently loses every object past
the first page.

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

- pagination needs `next_path`; an api that paginates by a page *number* or a cursor it
  does not echo back as a url is not covered.
- `update_method` must be `PATCH` or `PUT`.
- deletes require `delete_strategy: standard`; with the default `none`, delete ops fail.
- no schema provisioning; the backend schema must already exist.
