#!/usr/bin/env bash
# mark shell blocks as runnable on ci by adding a marker:
#
#   <!-- ci: no-backend -->     needs no backend; ci runs it on every push
#   <!-- ci: netbox -->         needs a live netbox; the e2e workflow runs it
#   <!-- ci: skip <reason> -->  deliberately not run
#
# usage: doc_examples.sh no-backend
#        doc_examples.sh netbox <backend-config>
#
# each block runs in a fresh directory holding a copy of `examples/`.
# in netbox mode the config is copied in as `backend-netbox.yaml` and
# stands in for the pages' `/path/to/backend-netbox.yaml`.
# when `$NETBOX_RESET` is set, it runs before each block and empties the netbox.
# when unset, the blocks share what they write!
# every shell block under docs/examples/ and docs/case-studies/ must carry a
# marker, so a walkthrough cannot land unchecked without saying why.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ALEMBIC="${ALEMBIC:-$ROOT/target/debug/alembic}"
MODE="${1:-}"
NETBOX_CONFIG="${2:-}"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

case "$MODE" in
  no-backend) ;;
  netbox)
    [[ -f "$NETBOX_CONFIG" ]] || fail "netbox mode needs a backend config: $0 netbox <backend-config>"
    ;;
  *) fail "usage: $0 no-backend | $0 netbox <backend-config>" ;;
esac
[[ -x "$ALEMBIC" ]] || fail "no alembic binary at $ALEMBIC; build it with cargo build -p alembic-cli or set ALEMBIC"
# the shim links to it from another directory
[[ "$ALEMBIC" == /* ]] || ALEMBIC="$PWD/$ALEMBIC"

WORK="$(mktemp -d /tmp/alembic-doc-examples-XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/bin" "$WORK/blocks"
ln -s "$ALEMBIC" "$WORK/bin/alembic"
: >"$WORK/blocks/index"

cd "$ROOT"
DOCS=(README.md docs/*.md docs/*/*.md)

# one file per marked block under $WORK/blocks, and an index line for each:
# `<mode> <file>:<line> <block file>`.
awk -v dir="$WORK/blocks" '
  function reset() { if (out != "") close(out); inblock = 0; out = ""; prev = "" }
  FNR == 1 { reset() }
  inblock && /^```/ { reset(); next }
  inblock { if (out != "") print > out; next }
  /^```/ {
    inblock = 1
    if ($0 !~ /^```(bash|sh|shell|console)[[:space:]]*$/) next
    where = FILENAME ":" FNR
    if (prev ~ /^<!-- ci: (no-backend|netbox) -->$/) {
      split(prev, marker, " ")
      n++
      out = dir "/" n ".sh"
      printf "" > out
      print marker[3], where, out > (dir "/index")
    } else if (prev ~ /^<!-- ci: skip [^ ].* -->$/) {
      print "skip", where, "-" > (dir "/index")
    } else if (prev ~ /^<!-- ci:/) {
      print where ": malformed marker: " prev > "/dev/stderr"; bad = 1
    } else if (FILENAME ~ /^docs\/(examples|case-studies)\//) {
      print where ": shell block has no <!-- ci: ... --> marker" > "/dev/stderr"; bad = 1
    }
    next
  }
  NF { prev = $0 }
  END { exit bad }
' "${DOCS[@]}" || fail "unmarked or malformed blocks (see above)"

ran=0
skipped=0
while read -r mode where block; do
  if [[ "$mode" != "$MODE" ]]; then
    skipped=$((skipped + 1))
    continue
  fi
  run="$WORK/run"
  rm -rf "$run"
  mkdir -p "$run"
  cp -R "$ROOT/examples" "$run/"
  if [[ "$MODE" == netbox ]]; then
    if [[ -n "${NETBOX_RESET:-}" ]] && ! bash -c "$NETBOX_RESET" >"$WORK/out.log" 2>&1; then
      cat "$WORK/out.log" >&2
      fail "resetting netbox before $where"
    fi
    cp "$NETBOX_CONFIG" "$run/backend-netbox.yaml"
    sed -i.orig 's#/path/to/backend-netbox.yaml#backend-netbox.yaml#g' "$block"
  fi
  if ! (cd "$run" && PATH="$WORK/bin:$PATH" bash -euo pipefail "$block") >"$WORK/out.log" 2>&1; then
    cat "$WORK/out.log" >&2
    fail "$where"
  fi
  ran=$((ran + 1))
  echo "  ok: $where"
done <"$WORK/blocks/index"

echo "$MODE: $ran block(s) ran, $skipped left to another mode or skipped"
