#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 3 ]]; then
  echo "usage: infrahubctl_docker.sh schema load <schema_path> [args...]" >&2
  exit 2
fi

args=("$@")

container="${INFRAHUB_CONTAINER:-}"
if [[ -z "$container" ]]; then
  container="$(docker ps --format '{{.Names}}' | awk '/infrahub-server/ {print $1; exit}')"
fi
if [[ -z "$container" ]]; then
  echo "infrahub container not found (set INFRAHUB_CONTAINER to override)" >&2
  exit 2
fi

if [[ "${args[0]}" == "schema" && "${args[1]}" == "load" ]]; then
  schema_path="${args[2]}"
  if [[ ! -f "$schema_path" ]]; then
    echo "schema file not found: $schema_path" >&2
    exit 2
  fi
  tmp_path="/tmp/alembic-schema.yaml"
  cat "$schema_path" | docker exec -i "$container" sh -c "cat > $tmp_path"
  args[2]="$tmp_path"
fi

: "${INFRAHUB_API_TOKEN:?INFRAHUB_API_TOKEN must be set}"

docker exec \
  -e INFRAHUB_ADDRESS="http://localhost:8000" \
  -e INFRAHUB_API_TOKEN="$INFRAHUB_API_TOKEN" \
  "$container" \
  infrahubctl "${args[@]}"
