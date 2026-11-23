#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NAUTOBOT_URL="${NAUTOBOT_URL:-http://localhost:8002}"
NAUTOBOT_TOKEN="${NAUTOBOT_TOKEN:-0123456789abcdef0123456789abcdef01234567}"
NAUTOBOT_COMPOSE="${NAUTOBOT_COMPOSE:-$ROOT/docker-compose.nautobot.yml}"
INVENTORY="$ROOT/examples/e2e-nautobot.yaml"
NAUTOBOT_STATE_PATH="$(mktemp /tmp/alembic-state-nautobot-XXXXXX.json)"
rm -f "$NAUTOBOT_STATE_PATH"

# nautobot migrates its database on first start, which takes minutes; the
# superuser token only answers once that is done.
wait_for_nautobot() {
  local timeout="${1:-300}"
  local i=0
  while [[ $i -lt $timeout ]]; do
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 \
      -H "Authorization: Token $NAUTOBOT_TOKEN" "$NAUTOBOT_URL/api/status/" || true)"
    if [[ "$code" == "200" ]]; then
      echo "nautobot is ready."
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  echo "timed out waiting for nautobot at $NAUTOBOT_URL" >&2
  return 1
}

if [[ "${SKIP_DOCKER:-0}" != "1" ]]; then
  docker compose -f "$NAUTOBOT_COMPOSE" up -d
fi

wait_for_nautobot

NAUTOBOT_CONFIG="$(mktemp /tmp/alembic-nautobot-XXXXXX.yaml)"
trap 'rm -f "$NAUTOBOT_CONFIG" "$NAUTOBOT_STATE_PATH"' EXIT

cat >"$NAUTOBOT_CONFIG" <<EOF
backend: nautobot
url: "$NAUTOBOT_URL"
token: "$NAUTOBOT_TOKEN"
EOF

echo "planning nautobot changes..."
ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$NAUTOBOT_STATE_PATH" \
  cargo run -p alembic-cli -- plan \
  -f "$INVENTORY" \
  -o /tmp/alembic-nautobot-plan.json \
  --backend-config "$NAUTOBOT_CONFIG" \
  --allow-delete

echo "applying nautobot plan (with custom field + tag provisioning)..."
ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$NAUTOBOT_STATE_PATH" \
  cargo run -p alembic-cli -- apply \
  -p /tmp/alembic-nautobot-plan.json \
  --backend-config "$NAUTOBOT_CONFIG" \
  --allow-delete

# a declared field the backend drops on write would plan again forever, so the
# backend the apply produced must plan nothing.
echo "planning nautobot again: it must have converged..."
replan="$(ALEMBIC_STATE_BACKEND=local ALEMBIC_STATE_PATH="$NAUTOBOT_STATE_PATH" \
  cargo run -q -p alembic-cli -- plan \
  -f "$INVENTORY" \
  -o /tmp/alembic-nautobot-replan.json \
  --backend-config "$NAUTOBOT_CONFIG" \
  --allow-delete)"
if ! grep -q "^plan: 0 to create, 0 to update, 0 to delete$" <<<"$replan"; then
  echo "nautobot did not converge after apply:" >&2
  echo "$replan" >&2
  exit 1
fi
echo "nautobot converged."

if [[ "${CLEANUP:-0}" == "1" ]]; then
  docker compose -f "$NAUTOBOT_COMPOSE" down -v
fi
