#!/usr/bin/env python3
"""an external adapter narrowing a read the four ways, picked by argv[1].

only `union` is correct: `keys` drops an object whose key drifted on the backend
since state bound it, `ids` drops one state has never bound. `ignore` answers a
superset, which is always valid.
"""

import json
import sys

SITES = [
    {"type_name": "dcim.site", "key": {"site": "fra1"}, "attrs": {"name": "FRA1"}, "backend_id": 1},
    {"type_name": "dcim.site", "key": {"site": "ber1"}, "attrs": {"name": "BER1"}, "backend_id": 2},
]


def respond(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.exit(0)


def canonical(key):
    return json.dumps(key, separators=(",", ":"), sort_keys=True)


def wanted(mode, scope, site):
    hint = scope.get("keys", {}).get(site["type_name"], [])
    by_key = any(entry["canonical"] == canonical(site["key"]) for entry in hint)
    by_id = site["backend_id"] in scope.get("backend_ids", {}).get(site["type_name"], [])
    return {"keys": by_key, "ids": by_id, "union": by_key or by_id, "ignore": True}[mode]


def main():
    mode = sys.argv[1]
    try:
        request = json.loads(sys.stdin.read())
    except json.JSONDecodeError as e:
        respond({"ok": False, "error": f"invalid request: {e}"})
    if request.get("version") != 1:
        respond({"ok": False, "error": "unsupported protocol version"})
    if request.get("method") == "capabilities":
        respond({"ok": True, "result": {"role": "observer"}})
    if request.get("method") != "read":
        respond({"ok": False, "error": f"{request.get('method')} is not supported"})

    rows = [s for s in SITES if s["type_name"] in request.get("types", [])]
    scope = request.get("scope") or {"kind": "full"}
    if scope.get("kind") != "full":
        rows = [s for s in rows if wanted(mode, scope, s)]
    respond({"ok": True, "result": rows})


main()
