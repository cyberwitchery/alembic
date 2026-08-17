#!/usr/bin/env bash
# every workspace member is either published by release.yml or says publish =
# false, so a crate added to the workspace fails here instead of quietly missing
# the release. both lists are read and counted rather than grepped for a match: a
# grep that finds nothing exits 0 and reads as "everything is fine".
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release="$root/.github/workflows/release.yml"

contains() {
  local needle="$1"
  shift
  local item
  for item in "$@"; do
    if [ "$item" = "$needle" ]; then
      return 0
    fi
  done
  return 1
}

members=()
while read -r path; do
  members+=("$path")
done < <(awk '
  /^\[/ { section = $0 }
  section == "[workspace]" && /^members[[:space:]]*=[[:space:]]*\[/ { inside = 1 }
  inside {
    sub(/#.*/, "")
    n = split($0, parts, "\"")
    for (i = 2; i <= n; i += 2) print parts[i]
    if (/\]/) inside = 0
  }
' "$root/Cargo.toml")

steps=()
while read -r crate; do
  steps+=("$crate")
done < <(grep -oE 'cargo publish -p [A-Za-z0-9_-]+' "$release" | awk '{ print $NF }')

if [ "${#members[@]}" -eq 0 ]; then
  echo "read no [workspace] members out of $root/Cargo.toml" >&2
  exit 1
fi
if [ "${#steps[@]}" -eq 0 ]; then
  echo "read no \`cargo publish -p\` steps out of $release" >&2
  exit 1
fi

echo "==> ${#members[@]} workspace members, ${#steps[@]} publish steps in release.yml"

names=()
problems=()
for path in "${members[@]}"; do
  manifest="$root/$path/Cargo.toml"
  if [ ! -f "$manifest" ]; then
    echo "workspace member $path has no Cargo.toml" >&2
    exit 1
  fi
  crate="$(awk '
    /^\[/ { pkg = ($0 == "[package]") }
    pkg && /^name[[:space:]]*=/ { split($0, a, "\""); print a[2]; exit }
  ' "$manifest")"
  if [ -z "$crate" ]; then
    echo "workspace member $path names no package" >&2
    exit 1
  fi
  names+=("$crate")

  if awk '
    /^\[/ { pkg = ($0 == "[package]") }
    pkg && /^publish[[:space:]]*=[[:space:]]*false/ { found = 1 }
    END { exit !found }
  ' "$manifest"; then
    if contains "$crate" "${steps[@]}"; then
      problems+=("$crate says publish = false, but release.yml publishes it")
    fi
  elif ! contains "$crate" "${steps[@]}"; then
    problems+=("$crate is a workspace member release.yml never publishes: add a \`cargo publish -p $crate\` step, or set publish = false in $path/Cargo.toml")
  fi
done

for crate in "${steps[@]}"; do
  if ! contains "$crate" "${names[@]}"; then
    problems+=("release.yml publishes $crate, which is not a workspace member")
  fi
done

if [ "${#problems[@]}" -gt 0 ]; then
  printf 'FAIL: %s\n' "${problems[@]}" >&2
  exit 1
fi

echo "PASS: every workspace member is published or says publish = false"
