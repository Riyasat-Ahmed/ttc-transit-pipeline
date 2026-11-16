#!/usr/bin/env bash
# Restart test: SIGKILL the processor while the producer keeps publishing, bring
# it back, and prove that nothing was lost and nothing was double-counted.
#
#   ./scripts/restart_test.sh [downtime_seconds]
#
# Checks:
#   1. Every heartbeat published to Redpanda during the test is in feed_health
#      exactly once (heartbeats are 1 per poll, so this is an exact count).
#   2. Every new vehicle snapshot published during the downtime has rows in
#      vehicle_positions.
#   3. No duplicate keys in stop_delays or vehicle_positions.
set -euo pipefail
cd "$(dirname "$0")/.."

DOWN=${1:-90}
dc() { docker compose "$@"; }
psql_q() { dc exec -T timescaledb psql -U ttc -d ttc -At -F $'\t' -c "$1"; }
lag() {
  dc exec -T redpanda rpk group describe ttc-processor 2>/dev/null \
    | awk '/^TOTAL-LAG/ {print $2}'
}

echo "== waiting for the pipeline to be caught up"
for _ in $(seq 60); do [[ "$(lag)" == "0" ]] && break; sleep 2; done

T0=$(date +%s)
echo "== $(date -u +%T) SIGKILL processor; producer keeps running for ${DOWN}s"
dc kill -s SIGKILL processor >/dev/null
sleep "$DOWN"
echo "== lag while down: $(lag) messages"
T1=$(date +%s)
echo "== $(date -u +%T) restarting processor"
dc start processor >/dev/null

for _ in $(seq 90); do
  L=$(lag); [[ "$L" == "0" ]] && break; sleep 2
done
echo "== caught up (lag=${L:-?}) after restart"
sleep 3

echo "== collecting heartbeats from Redpanda and feed_health from TimescaleDB"
TMP=$(mktemp -d)
dc exec -T redpanda rpk topic consume heartbeats -o :end -f '%v\n' > "$TMP/topic.jsonl"
psql_q "SELECT feed, to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS') FROM feed_health
        WHERE ts BETWEEN to_timestamp($T0 - 5) AND to_timestamp($T1 + 5)" > "$TMP/db.tsv"
psql_q "SELECT extract(epoch FROM feed_ts)::bigint, count(*) FROM vehicle_positions
        WHERE feed_ts BETWEEN to_timestamp($T0 - 60) AND to_timestamp($T1 + 5)
        GROUP BY 1" > "$TMP/vp.tsv"
DUPES=$(psql_q "SELECT
  (SELECT count(*) - count(DISTINCT (trip_id, service_date, stop_sequence)) FROM stop_delays)
  + (SELECT count(*) - count(DISTINCT (vehicle_id, ts)) FROM vehicle_positions)")

python3 - "$TMP" "$T0" "$T1" "$DUPES" <<'PY'
import json, sys
from collections import Counter
from datetime import datetime, timezone
tmp, t0, t1, dupes = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
iso = lambda e: datetime.fromtimestamp(e, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
topic = [json.loads(l) for l in open(f"{tmp}/topic.jsonl") if l.strip()]
window = [h for h in topic if t0 - 5 <= h["polled_at"] <= t1 + 5]
want = Counter((h["feed"], iso(h["polled_at"])) for h in window)
got = Counter(tuple(l.rstrip("\n").split("\t")) for l in open(f"{tmp}/db.tsv") if l.strip())
missing = want - got
extra = {k: v for k, v in got.items() if v > 1}
vp = {int(l.split("\t")[0]) for l in open(f"{tmp}/vp.tsv") if l.strip()}
snaps = {h["feed_ts"] for h in window if h["feed"] == "vehicles" and h.get("new_snapshot")}
lost_snaps = sorted(s for s in snaps if s not in vp)

print(f"heartbeats published in window : {sum(want.values())}")
print(f"heartbeats in feed_health      : {sum(got.values())}")
print(f"missing heartbeats             : {sum(missing.values())}")
print(f"duplicated heartbeats          : {len(extra)}")
print(f"new vehicle snapshots in window: {len(snaps)}, without rows: {len(lost_snaps)}")
print(f"duplicate keys in fact tables  : {dupes}")
ok = not missing and not extra and not lost_snaps and dupes == 0 and want
print("RESULT:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
