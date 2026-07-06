#!/usr/bin/env python3
"""a small conforming external adapter, used by alembic-adapter-test's examples."""

import json
import sys


def respond(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()
    sys.exit(0)


def main():
    raw = sys.stdin.read()
    try:
        request = json.loads(raw)
    except json.JSONDecodeError as e:
        respond({"ok": False, "error": f"invalid request: {e}"})

    version = request.get("version")
    if version != 1:
        respond({"ok": False, "error": f"unsupported protocol version {version} (expected 1)"})

    method = request.get("method")
    if method == "read":
        result = []
        if "dcim.site" in request.get("types", []):
            result.append(
                {
                    "type_name": "dcim.site",
                    "key": {"site": "fra1"},
                    "attrs": {"name": "FRA1", "slug": "fra1"},
                    "backend_id": 1,
                }
            )
        respond({"ok": True, "result": result})
    elif method == "write":
        applied = []
        for op in request.get("ops", []):
            if op.get("op") == "create":
                applied.append(
                    {"uid": op["uid"], "type_name": op["type_name"], "backend_id": 1}
                )
            else:
                respond({"ok": False, "error": f"{op.get('op')} is not supported"})
        respond(
            {
                "ok": True,
                "result": {"applied": applied, "provision": {"created_fields": [], "created_tags": []}},
            }
        )
    elif method == "ensure_schema":
        types = request.get("schema", {}).get("types", {})
        respond(
            {
                "ok": True,
                "result": {
                    "created_fields": [],
                    "created_tags": [],
                    "created_object_types": list(types.keys()),
                    "created_object_fields": [],
                },
            }
        )
    elif method == "preview_schema":
        # preview mirrors ensure_schema without writing: report what it would
        # provision. (an adapter that cannot preview answers {"ok": True,
        # "result": null} instead.)
        types = request.get("schema", {}).get("types", {})
        respond(
            {
                "ok": True,
                "result": {
                    "created_fields": [],
                    "created_tags": [],
                    "created_object_types": list(types.keys()),
                    "created_object_fields": [],
                },
            }
        )
    else:
        respond({"ok": False, "error": f"unknown method {method}"})


if __name__ == "__main__":
    main()
