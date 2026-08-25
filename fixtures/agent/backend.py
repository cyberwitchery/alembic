#!/usr/bin/env python3
"""a file-backed external adapter, the backend the agent exercises converge against.

it holds objects in a json file instead of a network service, so the exercises in
`scripts/agent_fixtures.sh` run offline and land in state a checker can read. it
also appends every method it is asked for to a log, which is how the exercises tell
a read-only run from one that writes backend schema.

  ALEMBIC_FIXTURE_STORE  where the objects live (default ./store.json)
  ALEMBIC_FIXTURE_LOG    where the method log is appended (default ./calls.log)

the protocol it speaks is `docs/external-adapters.md`.
"""

import json
import os
import sys

STORE = os.environ.get("ALEMBIC_FIXTURE_STORE", "store.json")
LOG = os.environ.get("ALEMBIC_FIXTURE_LOG", "calls.log")


def respond(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()
    sys.exit(0)


def load():
    try:
        with open(STORE) as handle:
            return json.load(handle)
    except FileNotFoundError:
        return {"next_id": 1, "objects": [], "types": []}


def save(store):
    with open(STORE, "w") as handle:
        json.dump(store, handle, indent=2, sort_keys=True)
        handle.write("\n")


def log(method):
    with open(LOG, "a") as handle:
        handle.write(method + "\n")


def find(store, type_name, backend_id, key):
    """the object an op names: by backend id when the host knows one, else by key."""
    for obj in store["objects"]:
        if obj["type_name"] != type_name:
            continue
        if backend_id is not None and obj["backend_id"] == backend_id:
            return obj
        if backend_id is None and obj["key"] == key:
            return obj
    return None


def backend_id_for(op, state):
    if op.get("backend_id") is not None:
        return op["backend_id"]
    mappings = (state or {}).get("mappings", {})
    return mappings.get(op["type_name"], {}).get(op["uid"])


def read(request):
    store = load()
    wanted = request.get("types", [])
    return [
        {
            "type_name": obj["type_name"],
            "key": obj["key"],
            "attrs": obj["attrs"],
            "backend_id": obj["backend_id"],
        }
        for obj in store["objects"]
        if obj["type_name"] in wanted
    ]


def write(request):
    store = load()
    state = request.get("state", {})
    applied = []
    for op in request.get("ops", []):
        kind = op.get("op")
        if kind == "create":
            desired = op["desired"]
            obj = {
                "backend_id": store["next_id"],
                "type_name": op["type_name"],
                "key": desired["key"],
                "attrs": dict(desired.get("attrs", {})),
            }
            store["next_id"] += 1
            store["objects"].append(obj)
        elif kind == "update":
            desired = op["desired"]
            obj = find(store, op["type_name"], backend_id_for(op, state), desired["key"])
            if obj is None:
                return {"ok": False, "error": f"no object to update for uid {op['uid']}"}
            # the key the update carries is the object's now: a rename is an update.
            obj["key"] = desired["key"]
            # merge, never replace: an update writes the declared projection, and a
            # field the inventory does not declare is not the adapter's to clear.
            obj["attrs"].update(desired.get("attrs", {}))
        elif kind == "delete":
            obj = find(store, op["type_name"], backend_id_for(op, state), op.get("key"))
            if obj is not None:
                store["objects"].remove(obj)
            applied.append({"uid": op["uid"], "type_name": op["type_name"]})
            continue
        else:
            return {"ok": False, "error": f"unknown op {kind}"}
        applied.append(
            {
                "uid": op["uid"],
                "type_name": op["type_name"],
                "backend_id": obj["backend_id"],
            }
        )
    save(store)
    return {"ok": True, "result": {"applied": applied}}


def provision_report(request, commit):
    """what `ensure_schema` would do: record the declared types, drop the rest.

    the same report answers `preview_schema`, which is the read-only half; only
    `ensure_schema` passes `commit`.
    """
    store = load()
    declared = list(request.get("schema", {}).get("types", {}).keys())
    created = [name for name in declared if name not in store["types"]]
    deleted = [name for name in store["types"] if name not in declared]
    if commit:
        store["types"] = declared
        save(store)
    return {
        "created_object_types": created,
        "deleted_object_types": deleted,
    }


def main():
    raw = sys.stdin.read()
    try:
        request = json.loads(raw)
    except json.JSONDecodeError as error:
        respond({"ok": False, "error": f"invalid request: {error}"})

    version = request.get("version")
    if version != 1:
        respond({"ok": False, "error": f"unsupported protocol version {version} (expected 1)"})

    method = request.get("method")
    log(method)
    if method == "capabilities":
        respond({"ok": True, "result": {"role": "adapter"}})
    elif method == "read":
        respond({"ok": True, "result": read(request)})
    elif method == "write":
        respond(write(request))
    elif method == "ensure_schema":
        respond({"ok": True, "result": provision_report(request, commit=True)})
    elif method == "preview_schema":
        respond({"ok": True, "result": provision_report(request, commit=False)})
    else:
        respond({"ok": False, "error": f"unknown method {method}"})


if __name__ == "__main__":
    main()
