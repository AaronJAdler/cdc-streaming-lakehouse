#!/usr/bin/env bash
# End to end through real Debezium and Kafka. Needs `docker compose up -d` first.
#
# 1. Rows written before capture starts arrive as Debezium's initial snapshot (op "r").
# 2. Live traffic follows, including ALTER TABLE ... ADD COLUMN halfway through.
# 3. Spark is killed after committing one table of a batch and before the next.
# 4. Spark restarts from its checkpoint and catches up.
# 5. The mirror must match Postgres row for row, and the gap in live metrics is reported.
set -euo pipefail

DSN=${DSN:-postgresql://postgres:postgres@localhost:5432/shop}
CONNECT=${CONNECT:-http://localhost:8083}
BOOTSTRAP=${BOOTSTRAP:-localhost:29092}
WORK=${WORK:-.e2e}
LAKE=$WORK/lake
CHECKPOINTS=$WORK/checkpoints
rm -rf "$WORK" && mkdir -p "$WORK"

wait_for() {
  local what=$1; shift
  for _ in $(seq 1 90); do
    if "$@" >/dev/null 2>&1; then echo "$what is ready"; return 0; fi
    sleep 2
  done
  echo "$what never became ready" >&2
  return 1
}

connector_running() {
  curl -fsS "$CONNECT/connectors/shop/status" | python3 -c '
import json, sys
s = json.load(sys.stdin)
ok = s["connector"]["state"] == "RUNNING" and s["tasks"] and all(t["state"] == "RUNNING" for t in s["tasks"])
sys.exit(0 if ok else 1)'
}

topic_offsets() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic 'shop\.public\..*' \
    | awk -F: '{ total += $3 } END { print total + 0 }'
}

wait_until_caught_up() {
  # Debezium has published everything once the topic offsets stop moving.
  local before after
  before=$(topic_offsets)
  for _ in $(seq 1 60); do
    sleep 3
    after=$(topic_offsets)
    if [ "$after" -gt 0 ] && [ "$after" = "$before" ]; then
      echo "kafka holds $after change events"
      return 0
    fi
    before=$after
  done
  echo "Debezium never caught up" >&2
  return 1
}

stream() {
  uv run cdclake stream --kafka "$BOOTSTRAP" --lake "$LAKE" --checkpoints "$CHECKPOINTS"
}

wait_for postgres uv run python -c "import psycopg; psycopg.connect('$DSN').close()"
wait_for "kafka connect" curl -fsS "$CONNECT/connectors"

echo "== 1. rows that exist before capture starts"
uv run cdclake setup --dsn "$DSN"
uv run cdclake workload --dsn "$DSN" --orders 150 --seed 1 --no-schema-change

echo "== 2. register the Debezium connector (initial snapshot)"
curl -fsS -X PUT -H "Content-Type: application/json" \
  --data @debezium/shop-connector.json "$CONNECT/connectors/shop/config"
echo
wait_for "debezium connector" connector_running

echo "== 3. live traffic, with a column added halfway"
uv run cdclake workload --dsn "$DSN" --orders 300 --seed 2 --start 2026-01-01T12:00:00+00:00
wait_until_caught_up

echo "== 4. stream, killing Spark between two table commits of batch 1"
set +e
CDCLAKE_CRASH_AFTER=1:orders stream
code=$?
set -e
if [ "$code" != 137 ]; then
  echo "expected the injected crash (exit 137), got $code" >&2
  exit 1
fi
echo "crashed as planned"

echo "== 5. restart from the checkpoint and reconcile"
matched=0
for attempt in 1 2 3 4 5; do
  stream
  if uv run cdclake reconcile --dsn "$DSN" --lake "$LAKE" > "$WORK/reconcile.json"; then
    matched=1
    break
  fi
  echo "attempt $attempt: mirror not yet identical"; cat "$WORK/reconcile.json"; sleep 5
done
cat "$WORK/reconcile.json"
[ "$matched" = 1 ] || { echo "mirror never matched the source" >&2; exit 1; }

uv run python - "$LAKE" <<'EOF'
import sys
from collections import Counter
from deltalake import DeltaTable

lake = sys.argv[1]
orders = DeltaTable(f"{lake}/mirror/orders").to_pyarrow_table().to_pylist()
last_ops = Counter(r["_op"] for r in orders)
print("orders by last change:", dict(last_ops))
assert last_ops["r"] > 0, "expected snapshot rows that never changed afterwards"
assert last_ops["d"] > 0, "expected cancelled orders kept as tombstones"
assert any(r.get("channel") for r in orders), "expected the column added mid-stream"
keys = [r["order_id"] for r in orders]
assert len(keys) == len(set(keys)), "duplicate keys after the crash"
print("checks passed")
EOF

echo "== live metrics against the exact restatement"
uv run cdclake restate --lake "$LAKE"
