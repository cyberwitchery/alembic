#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NETBOX_URL="${NETBOX_URL:-http://localhost:8001}"
NETBOX_TOKEN="${NETBOX_TOKEN:-0123456789abcdef0123456789abcdef01234567}"
INFRAHUB_URL="${INFRAHUB_URL:-http://localhost:8000}"
INFRAHUB_TOKEN="${INFRAHUB_TOKEN:-06438eb2-8019-4776-878c-0941b1f1d1ec}"
INFRAHUB_SCHEMA_PATH="${INFRAHUB_SCHEMA_PATH:-/tmp/alembic-infrahub-schema.yaml}"
INFRAHUBCTL_PATH="${INFRAHUBCTL_PATH:-$ROOT/scripts/infrahubctl_docker.sh}"

NETBOX_COMPOSE="${NETBOX_COMPOSE:-$ROOT/docker-compose.netbox.yml}"
INFRAHUB_COMPOSE="${INFRAHUB_COMPOSE:-$ROOT/../infrahub.rs/docker-compose.yml}"
INFRAHUB_STATE_PATH="$(mktemp /tmp/alembic-state-infrahub-XXXXXX.json)"
NETBOX_STATE_PATH="$(mktemp /tmp/alembic-state-netbox-XXXXXX.json)"
rm -f "$INFRAHUB_STATE_PATH" "$NETBOX_STATE_PATH"

wait_for_url() {
  local url="$1"
  local name="$2"
  local timeout="${3:-180}"
  local accept_403="${4:-0}"
  local i=0
  while [[ $i -lt $timeout ]]; do
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$url" || true)"
    if [[ "$code" == "200" || ( "$accept_403" == "1" && "$code" == "403" ) ]]; then
      echo "$name is ready."
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  echo "timed out waiting for $name at $url" >&2
  return 1
}

if [[ "${SKIP_DOCKER:-0}" != "1" ]]; then
  docker compose -f "$INFRAHUB_COMPOSE" up -d
  docker compose -f "$NETBOX_COMPOSE" up -d --build
fi

wait_for_url "http://localhost:8000/api/config" "infrahub"
wait_for_url "http://localhost:8001/api/" "netbox" 180 1
wait_for_url "http://localhost:8001/api/plugins/custom-objects/custom-object-types/" "netbox custom objects" 180 1

NETBOX_CONFIG="$(mktemp /tmp/alembic-netbox-XXXXXX.yaml)"
INFRAHUB_CONFIG="$(mktemp /tmp/alembic-infrahub-XXXXXX.yaml)"
trap 'rm -f "$NETBOX_CONFIG" "$INFRAHUB_CONFIG" "$INFRAHUB_STATE_PATH" "$NETBOX_STATE_PATH"' EXIT

NETBOX_TOKEN="$(docker exec -i alembic-netbox-1 /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py shell -c 'from django.contrib.auth import get_user_model; from users.models import Token; User=get_user_model(); u=User.objects.get(username="admin"); t=Token.objects.create(user=u); print("nbt_{}.{}".format(t.key, t.token))' | tail -n 1)"

cat >"$NETBOX_CONFIG" <<EOF
backend: netbox
url: "$NETBOX_URL"
token: "$NETBOX_TOKEN"
EOF

cat >"$INFRAHUB_CONFIG" <<EOF
backend: infrahub
url: "$INFRAHUB_URL"
token: "$INFRAHUB_TOKEN"
schema:
  mode: infrahubctl
  schema_path: "$INFRAHUB_SCHEMA_PATH"
  infrahubctl_path: "$INFRAHUBCTL_PATH"
EOF

echo "planning infrahub changes..."
ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$INFRAHUB_STATE_PATH" \
  cargo run -p alembic-cli -- plan \
  -f "$ROOT/examples/e2e.yaml" \
  -o /tmp/alembic-infrahub-plan.json \
  --backend-config "$INFRAHUB_CONFIG" \
  --provision \
  --allow-delete

echo "applying infrahub plan (with schema push)..."
ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$INFRAHUB_STATE_PATH" \
  cargo run -p alembic-cli -- apply \
  -p /tmp/alembic-infrahub-plan.json \
  --backend-config "$INFRAHUB_CONFIG"

echo "planning netbox changes..."
ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$NETBOX_STATE_PATH" \
  cargo run -p alembic-cli -- plan \
  -f "$ROOT/examples/e2e.yaml" \
  -o /tmp/alembic-netbox-plan.json \
  --backend-config "$NETBOX_CONFIG" \
  --allow-delete

echo "applying netbox plan (with custom field + tag provisioning)..."
ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$NETBOX_STATE_PATH" \
  cargo run -p alembic-cli -- apply \
  -p /tmp/alembic-netbox-plan.json \
  --backend-config "$NETBOX_CONFIG"

echo "importing from infrahub..."
ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$INFRAHUB_STATE_PATH" \
  cargo run -p alembic-cli -- import \
  -o /tmp/alembic-infrahub-import.yaml \
  --retort "$ROOT/examples/e2e-retort.yaml" \
  --backend-config "$INFRAHUB_CONFIG"

echo "importing from netbox..."
ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$NETBOX_STATE_PATH" \
  cargo run -p alembic-cli -- import \
  -o /tmp/alembic-netbox-import.yaml \
  --retort "$ROOT/examples/e2e-retort.yaml" \
  --backend-config "$NETBOX_CONFIG"

grep -q "firmware_version" /tmp/alembic-infrahub-import.yaml
grep -q "firmware_version" /tmp/alembic-netbox-import.yaml
grep -q "alembic-e2e" /tmp/alembic-netbox-import.yaml
grep -q "ops.service" /tmp/alembic-infrahub-import.yaml
grep -q "ops.service_instance" /tmp/alembic-netbox-import.yaml
grep -q "edge-dhcp" /tmp/alembic-netbox-import.yaml

echo "e2e imports written to /tmp/alembic-infrahub-import.yaml and /tmp/alembic-netbox-import.yaml"

if [[ "${CLEANUP:-0}" == "1" ]]; then
  docker compose -f "$NETBOX_COMPOSE" down -v
  docker compose -f "$INFRAHUB_COMPOSE" down -v
fi
