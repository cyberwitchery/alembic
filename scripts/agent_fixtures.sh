#!/usr/bin/env bash
# the exercises behind `crates/alembic-cli/skills/alembic/SKILL.md`. each one is
# a task an agent is asked to do, run against the file-backed backend in
# `fixtures/agent/` and checked against the artifacts it leaves: the plan's ops,
# the drift report's categories, the backend store, and the adapter's method log.
# no network, no model in the loop -- what is asserted is what the cli did.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURES="$ROOT/fixtures/agent"
ALEMBIC="${ALEMBIC:-cargo run -q --manifest-path $ROOT/Cargo.toml -p alembic-cli --}"
PYTHON="${ALEMBIC_FIXTURE_PYTHON:-python3}"

WORK="$(mktemp -d /tmp/alembic-agent-fixtures-XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

passed=0

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

pass() {
  passed=$((passed + 1))
  echo "  ok: $*"
}

exercise() {
  echo
  echo "== $*"
}

# a workspace holding nothing but the fixtures: fresh backend store, fresh
# identity memory, fresh method log. every exercise starts from one, so none of
# them depends on what the last one left behind.
seed() {
  rm -rf "$WORK/run"
  mkdir -p "$WORK/run"
  cp "$FIXTURES"/*.yaml "$FIXTURES"/backend.py "$WORK/run/"
  cd "$WORK/run"
  : >calls.log
}

# apply the starting intent, so the backend holds the site and the device the
# later edits are edits *of*.
converge_base() {
  $ALEMBIC plan -f base.yaml -o base-plan.json --backend-config backend.yaml >/dev/null
  $ALEMBIC apply -p base-plan.json --backend-config backend.yaml >/dev/null
}

# the plan's op kinds, in plan order: "create", "update delete", or empty.
plan_ops() {
  "$PYTHON" -c 'import json,sys; print(" ".join(op["op"] for op in json.load(open(sys.argv[1]))["ops"]).strip())' "$1"
}

# the plan's op kinds, sorted, for a plan whose order is not the point.
plan_ops_sorted() {
  "$PYTHON" -c 'import json,sys; print(" ".join(sorted(op["op"] for op in json.load(open(sys.argv[1]))["ops"])).strip())' "$1"
}

# the op kinds the plan holds for one type, sorted: what happened to the site,
# with whatever the device did left out of it.
plan_ops_for() {
  "$PYTHON" -c 'import json,sys; print(" ".join(sorted(op["op"] for op in json.load(open(sys.argv[2]))["ops"] if op["type_name"]==sys.argv[1])).strip())' "$1" "$2"
}

# the uids the plan names, in plan order.
plan_uids() {
  "$PYTHON" -c 'import json,sys; print(" ".join(op["uid"] for op in json.load(open(sys.argv[1]))["ops"]).strip())' "$1"
}

# the uids the plan names for one type, in plan order.
plan_uids_for() {
  "$PYTHON" -c 'import json,sys; print(" ".join(op["uid"] for op in json.load(open(sys.argv[2]))["ops"] if op["type_name"]==sys.argv[1]).strip())' "$1" "$2"
}

# a python expression over the backend store, printed. `store` is the document.
store_query() {
  "$PYTHON" -c 'import json,sys; store=json.load(open("store.json")); print(eval(sys.argv[1]))' "$1"
}

# a python expression over a json artifact, printed. `doc` is the document.
doc_query() {
  "$PYTHON" -c 'import json,sys; doc=json.load(open(sys.argv[1])); print(eval(sys.argv[2]))' "$1" "$2"
}

expect() {
  local label="$1" want="$2" got="$3"
  [[ "$want" == "$got" ]] || fail "$label: expected [$want], got [$got]"
  pass "$label"
}

SITE_UID="11111111-1111-1111-1111-111111111111"
OTHER_UID="33333333-3333-3333-3333-333333333333"

exercise "the starting intent validates and converges"
seed
$ALEMBIC validate -f base.yaml
converge_base
expect "the backend holds both declared objects" "2" "$(store_query 'len(store["objects"])')"
expect "the device's ref is stored as the site's uid" \
  "$SITE_UID" \
  "$(store_query '[o for o in store["objects"] if o["type_name"]=="dcim.device"][0]["attrs"]["site"]')"
$ALEMBIC plan -f base.yaml -o converged.json --backend-config backend.yaml >/dev/null
expect "a converged inventory plans nothing" "" "$(plan_ops converged.json)"

exercise "a rename that carries the uid is one update"
seed
converge_base
$ALEMBIC plan -f renamed.yaml -o renamed-plan.json --backend-config backend.yaml >/dev/null
expect "the plan is a single update" "update" "$(plan_ops renamed-plan.json)"
expect "the update names the object's existing uid" "$SITE_UID" "$(plan_uids renamed-plan.json)"
$ALEMBIC apply -p renamed-plan.json --backend-config backend.yaml >/dev/null
expect "the backend still holds two objects" "2" "$(store_query 'len(store["objects"])')"
expect "the site kept its backend id through the rename" \
  "1" \
  "$(store_query '[o for o in store["objects"] if o["type_name"]=="dcim.site"][0]["backend_id"]')"
expect "the site answers to its new key" \
  "fra01" \
  "$(store_query '[o for o in store["objects"] if o["type_name"]=="dcim.site"][0]["key"]["slug"]')"

# the same edit as above with the uid recomputed from the new key. the device's
# ref has to follow the new uid, which is the first sign the edit is not a
# rename: the object the rest of the inventory pointed at is gone.
exercise "the same rename with a recomputed uid is a different object"
seed
converge_base
$ALEMBIC plan -f recomputed-uid.yaml -o recomputed-plan.json --backend-config backend.yaml >/dev/null
expect "the plan creates a second site rather than renaming the first" \
  "create" "$(plan_ops_for dcim.site recomputed-plan.json)"
expect "the create names the recomputed uid" \
  "$OTHER_UID" "$(plan_uids_for dcim.site recomputed-plan.json)"
$ALEMBIC plan -f recomputed-uid.yaml -o recomputed-delete-plan.json \
  --backend-config backend.yaml --allow-delete >/dev/null
expect "under --allow-delete the original is planned for deletion beside it" \
  "create delete" "$(plan_ops_for dcim.site recomputed-delete-plan.json)"

# validate reads no backend, so this one needs none: the ref is wrong before
# anything is observed.
exercise "a ref holds the target's uid, never the backend's id"
seed
if $ALEMBIC validate -f ref-by-backend-id.yaml -o ref-errors.json >/dev/null 2>&1; then
  fail "an inventory whose ref holds a backend id must not validate"
fi
pass "validate rejects a ref holding a backend id"
expect "and the json report carries the error" "True" \
  "$(doc_query ref-errors.json 'len(doc["errors"]) > 0')"

exercise "an absent attr is not managed; a null attr is cleared"
seed
converge_base
$ALEMBIC plan -f description-absent.yaml -o absent-plan.json --backend-config backend.yaml >/dev/null
expect "dropping the field plans nothing" "" "$(plan_ops absent-plan.json)"
expect "and leaves the backend's value in place" \
  "primary site" \
  "$(store_query '[o for o in store["objects"] if o["type_name"]=="dcim.site"][0]["attrs"]["description"]')"
$ALEMBIC plan -f description-null.yaml -o null-plan.json --backend-config backend.yaml >/dev/null
expect "setting it to null plans one update" "update" "$(plan_ops null-plan.json)"
$ALEMBIC apply -p null-plan.json --backend-config backend.yaml >/dev/null
expect "which clears the field on the backend" \
  "None" \
  "$(store_query '[o for o in store["objects"] if o["type_name"]=="dcim.site"][0]["attrs"]["description"]')"

exercise "an undeclared object is drift, and deleting it needs saying so"
seed
converge_base
$ALEMBIC plan -f device-removed.yaml -o kept-plan.json --backend-config backend.yaml >/dev/null
expect "a plan without --allow-delete holds no delete" "" "$(plan_ops kept-plan.json)"
$ALEMBIC plan -f device-removed.yaml --report -o drift.json --backend-config backend.yaml >/dev/null
expect "the drift report reports it as extra" "1" "$(doc_query drift.json 'len(doc["extra"])')"
expect "naming the object the inventory dropped" \
  "dcim.device" "$(doc_query drift.json 'doc["extra"][0]["type_name"]')"
$ALEMBIC plan -f device-removed.yaml -o delete-plan.json \
  --backend-config backend.yaml --allow-delete >/dev/null
expect "--allow-delete plans the delete" "delete" "$(plan_ops delete-plan.json)"
if $ALEMBIC apply -p delete-plan.json --backend-config backend.yaml >/dev/null 2>&1; then
  fail "applying a plan holding a delete without --allow-delete must fail"
fi
pass "apply refuses the delete the operator did not authorize"
expect "and the object is still on the backend" "2" "$(store_query 'len(store["objects"])')"

exercise "--report is read-only; --report --provision is not"
seed
converge_base
: >calls.log
$ALEMBIC plan -f base.yaml --report --backend-config backend.yaml >/dev/null
if grep -q "^ensure_schema$" calls.log; then
  fail "plan --report must not provision backend schema"
fi
pass "plan --report never asks the backend to write schema"
expect "it does ask for the read-only preview" "1" "$(grep -c '^preview_schema$' calls.log)"
: >calls.log
$ALEMBIC plan -f base.yaml --report --provision --backend-config backend.yaml >/dev/null
expect "adding --provision writes schema" "1" "$(grep -c '^ensure_schema$' calls.log)"

echo
echo "--- $passed checks passed ---"
