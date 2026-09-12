#!/usr/bin/env python3
"""an external adapter narrowing a read the five ways, picked by argv[1].

only `unnarrowed` is correct: `keys` drops an object whose key drifted on the
backend since state bound it, `ids` drops one state has never bound, and `union`
drops a ref-keyed type the hint holds out of both maps. `ignore` answers a
superset, which is always valid.
"""

import json
import sys

SW1 = "71f14d94-779f-5b19-a404-786883d432af"

ROWS = [
    {"type_name": "dcim.site", "key": {"site": "fra1"}, "attrs": {"name": "FRA1"}, "backend_id": 1},
    {"type_name": "dcim.site", "key": {"site": "ber1"}, "attrs": {"name": "BER1"}, "backend_id": 2},
    {"type_name": "dcim.device", "key": {"name": "sw1"}, "attrs": {"name": "sw1"}, "backend_id": 3},
    # a ref-keyed interface: its key names the device by uid, not by anything the
    # backend stores, so no hint can narrow it.
    {
        "type_name": "dcim.interface",
        "key": {"device": SW1, "name": "eth0"},
        "attrs": {"name": "eth0"},
        "backend_id": 4,
    },
]


def respond(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.exit(0)


def canonical(key):
    return json.dumps(key, separators=(",", ":"), sort_keys=True)


def wanted(mode, scope, row):
    hint = scope.get("keys", {}).get(row["type_name"], [])
    by_key = any(entry["canonical"] == canonical(row["key"]) for entry in hint)
    by_id = row["backend_id"] in scope.get("backend_ids", {}).get(row["type_name"], [])
    held = row["type_name"] in scope.get("unnarrowed", [])
    return {
        "keys": by_key,
        "ids": by_id,
        "union": by_key or by_id,
        "unnarrowed": by_key or by_id or held,
        "ignore": True,
    }[mode]


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

    rows = [r for r in ROWS if r["type_name"] in request.get("types", [])]
    scope = request.get("scope") or {"kind": "full"}
    if scope.get("kind") != "full":
        rows = [r for r in rows if wanted(mode, scope, r)]
    respond({"ok": True, "result": rows})


main()
