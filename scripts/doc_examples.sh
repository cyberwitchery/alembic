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
mkdir -p "$WORK/bin" "$WORK/blocks" "$WORK/template"
ln -s "$ALEMBIC" "$WORK/bin/alembic"
: >"$WORK/blocks/index"
# every block starts from a copy of this directory
cp -R "$ROOT/examples" "$WORK/template/"
[[ "$MODE" != netbox ]] || cp "$NETBOX_CONFIG" "$WORK/template/backend-netbox.yaml"

cd "$ROOT"
DOCS=(README.md docs/*.md docs/*/*.md)

# one file per block of this mode under $WORK/blocks, and an index line for
# each: `<file>:<line> <block file>`. prints how many blocks it left out.
skipped="$(awk -v mode="$MODE" -v dir="$WORK/blocks" '
  function reset() { if (out != "") close(out); inblock = 0; out = ""; prev = "" }
  function complain(msg) { print FILENAME ":" FNR ": " msg > "/dev/stderr"; bad = 1 }
  FNR == 1 { reset() }
  inblock && /^```/ { reset(); next }
  inblock {
    if (out == "") next
    if (mode == "netbox") gsub(/\/path\/to\/backend-netbox\.yaml/, "backend-netbox.yaml")
    print > out
    next
  }
  /^```(bash|sh|shell|console)[[:space:]]*$/ {
    inblock = 1
    if (prev == "<!-- ci: " mode " -->") {
      n++
      out = dir "/" n ".sh"
      printf "" > out
      print FILENAME ":" FNR, out > (dir "/index")
    } else if (prev ~ /^<!-- ci: (no-backend|netbox|skip [^ ].*) -->$/) {
      skipped++
    } else if (prev ~ /^<!-- ci:/) {
      complain("malformed marker: " prev)
    } else if (FILENAME ~ /^docs\/(examples|case-studies)\//) {
      complain("shell block has no <!-- ci: ... --> marker")
    }
    next
  }
  NF && prev ~ /^<!-- ci:/ { complain("marker is not followed by a bash, sh, shell or console block") }
  /^```/ { inblock = 1; next }
  NF { prev = $0 }
  END { print skipped + 0; exit bad }
' "${DOCS[@]}")" || fail "unmarked or malformed blocks (see above)"

# the index is read on fd 3, so a block reading stdin cannot eat it
ran=0
while read -r where block <&3; do
  run="$WORK/run"
  rm -rf "$run"
  cp -R "$WORK/template" "$run"
  if [[ "$MODE" == netbox && -n "${NETBOX_RESET:-}" ]] && ! bash -c "$NETBOX_RESET" >"$WORK/out.log" 2>&1; then
    cat "$WORK/out.log" >&2
    fail "resetting netbox before $where"
  fi
  if ! (cd "$run" && PATH="$WORK/bin:$PATH" bash -euo pipefail "$block") >"$WORK/out.log" 2>&1; then
    cat "$WORK/out.log" >&2
    fail "$where"
  fi
  ran=$((ran + 1))
  echo "  ok: $where"
done 3<"$WORK/blocks/index"

echo "$MODE: $ran block(s) ran, $skipped left to another mode or skipped"
