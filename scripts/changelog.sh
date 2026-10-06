#!/usr/bin/env bash
# changelog entries live one per file in changelog.d/ until a release collects
# them, so two prs never edit the same lines of CHANGELOG.md.
#
#   scripts/changelog.sh check                    every fragment is one bullet line
#   scripts/changelog.sh release <version> [date] move the fragments into CHANGELOG.md
#
# release writes a `## [<version>] - <date>` section under `## Unreleased`,
# newest fragment first (by the commit that added it), and deletes the fragments.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fragments="$root/changelog.d"
changelog="$root/CHANGELOG.md"

die() {
  echo "changelog: $*" >&2
  exit 1
}

# the fragment files, README excluded, as paths relative to the repo root.
list_fragments() {
  local file
  for file in "$fragments"/*.md; do
    [ -e "$file" ] || continue
    [ "$(basename "$file")" = "README.md" ] && continue
    echo "${file#"$root"/}"
  done
}

check() {
  local file count=0 lines
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    count=$((count + 1))
    lines="$(grep -c '' "$root/$file")"
    [ "$lines" -eq 1 ] || die "$file has $lines lines; an entry is one line"
    grep -q '^- [^ ]' "$root/$file" || die "$file is not a bullet: start it with '- '"
  done < <(list_fragments)
  echo "changelog: $count fragment(s) ok"
}

release() {
  local version="${1:-}" date="${2:-$(date +%Y-%m-%d)}"
  [ -n "$version" ] || die "usage: changelog.sh release <version> [date]"
  grep -q "^## \[$version\]" "$changelog" && die "CHANGELOG.md already has $version"
  grep -q '^## Unreleased$' "$changelog" || die "CHANGELOG.md has no '## Unreleased' heading"
  check >/dev/null

  # newest first: by the time of the commit that added each fragment, then name.
  local file added ordered=()
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    added="$(git -C "$root" log -1 --diff-filter=A --format=%ct -- "$file")"
    ordered+=("${added:-9999999999} $file")
  done < <(list_fragments)
  [ "${#ordered[@]}" -gt 0 ] || die "no fragments in changelog.d/ to release"

  local section tmp
  section="$(mktemp)"
  tmp="$(mktemp)"
  printf '%s\n' "${ordered[@]}" | sort -k1,1nr -k2,2 | while read -r _ file; do
    cat "$root/$file"
  done > "$section"

  # everything under `## Unreleased` is replaced by the pointer line; a bullet
  # still written there by hand is released along with the fragments, not lost.
  awk -v heading="## [$version] - $date" -v section="$section" '
    function flush() {
      print ""
      print "unreleased changes live in `changelog.d/` until a release collects them."
      print ""
      print heading
      print ""
      while ((getline line < section) > 0) print line
      if (extra != "") printf "%s", extra
      print ""
      skipping = 0
      done = 1
    }
    skipping && /^## / { flush() }
    skipping { if ($0 ~ /^- /) extra = extra $0 "\n"; next }
    { print }
    $0 == "## Unreleased" && !done { skipping = 1 }
    END { if (skipping) flush() }
  ' "$changelog" > "$tmp"
  mv "$tmp" "$changelog"
  rm "$section"

  for entry in "${ordered[@]}"; do
    rm "$root/${entry#* }"
  done
  echo "changelog: released ${#ordered[@]} fragment(s) as $version"
}

case "${1:-}" in
  check) check ;;
  release) shift; release "$@" ;;
  *) die "usage: changelog.sh check | release <version> [date]" ;;
esac
