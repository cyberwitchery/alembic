#!/usr/bin/env bash
# the shell blocks the docs mark runnable, run as written. a marker is an html
# comment on the line before the fence, which the rendered page does not show:
#
#   <!-- ci: no-backend -->     needs no backend; ci runs it on every push
#   <!-- ci: netbox -->         needs a live netbox; the e2e workflow runs it
#   <!-- ci: skip <reason> -->  deliberately not run
#
# usage: doc_examples.sh no-backend
#        doc_examples.sh netbox <backend-config>
#
# each block runs in a fresh directory holding a copy of `examples/`, with
# `alembic` on PATH. in netbox mode the config is copied in as
# `backend-netbox.yaml` and stands in for the pages' `/path/to/backend-netbox.yaml`,
# and `$NETBOX_RESET`, when set, runs before each block to empty the netbox the
# last one left (scripts/netbox_reset.py); unset, the blocks share what they write.
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
# `<mode> <file>:<line> <block file>`. an unmarked block where one is required,
# or a malformed marker, is reported on stderr and fails the run.
awk -v dir="$WORK/blocks" '
  FNR == 1 { prev = ""; inblock = 0 }
  inblock {
    if ($0 ~ /^```/) { inblock = 0; if (out != "") close(out); out = ""; next }
    if (out != "") print > out
    next
  }
  /^```/ {
    inblock = 1
    out = ""
    if ($0 !~ /^```(bash|sh|shell|console)[[:space:]]*$/) next
    if (prev ~ /^<!-- ci:/) {
      if (prev ~ /^<!-- ci: (no-backend|netbox) -->$/) {
        mode = prev; sub(/^<!-- ci: /, "", mode); sub(/ -->$/, "", mode)
        n++
        out = dir "/" n ".sh"
        printf "" > out
        print mode, FILENAME ":" FNR, out > (dir "/index")
      } else if (prev ~ /^<!-- ci: skip [^ ].* -->$/) {
        print "skip", FILENAME ":" FNR, "-" > (dir "/index")
      } else {
        print FILENAME ":" FNR ": malformed marker: " prev > "/dev/stderr"; bad = 1
      }
    } else if (FILENAME ~ /^docs\/(examples|case-studies)\//) {
      print FILENAME ":" FNR ": shell block has no <!-- ci: ... --> marker" > "/dev/stderr"; bad = 1
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
