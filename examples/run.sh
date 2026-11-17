#!/usr/bin/env bash
# Runnable local example against a live API.
#
#   docker compose up --build -d
#   ./examples/run.sh                      # http://localhost:8000
#   API=http://127.0.0.1:9000 ./examples/run.sh
#
# Each run uses a unique ID prefix, so the script can be run repeatedly against
# the same database without clashing with earlier runs.
set -euo pipefail

API="${API:-http://localhost:8000}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN="$(date +%s)$$"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pretty() { python3 -m json.tool; }

# Prefix every sample/batch id in the example payload with this run's token.
python3 - "$HERE/intake.json" "$RUN" >"$WORK/intake.json" <<'PY'
import json, re, sys
path, run = sys.argv[1], sys.argv[2]
payload = json.load(open(path))
def rename(value):
    return re.sub(r"^([SB])-", r"\g<1>" + run + "-", value)
payload["source_sample_ids"] = [rename(v) for v in payload["source_sample_ids"]]
for sample in payload["samples"]:
    sample["sample_id"] = rename(sample["sample_id"])
    sample["batch_nos"] = [rename(v) for v in sample["batch_nos"]]
for batch in payload["batches"]:
    batch["batch_no"] = rename(batch["batch_no"])
json.dump(payload, sys.stdout)
PY

echo "== health =="
curl -fsS "$API/healthz"; echo

echo
echo "== register samples and transfer batches (run $RUN) =="
curl -fsS -X POST "$API/intakes" \
  -H 'Content-Type: application/json' \
  --data @"$WORK/intake.json" | pretty

echo
echo "== trace contamination (sources flagged at intake) =="
TRACE=$(curl -fsS -X POST "$API/traces" -H 'Content-Type: application/json' -d '{}')
echo "$TRACE" | pretty
TRACE_ID=$(echo "$TRACE" | python3 -c 'import json,sys; print(json.load(sys.stdin)["trace_id"])')

echo
echo "== correct B${RUN}-1002 (client saw version 1): Reference-Lab -> Cold-Room =="
curl -fsS -X POST "$API/batches/B${RUN}-1002/corrections" \
  -H 'Content-Type: application/json' \
  -d '{
    "seen_version": 1,
    "origin": "Hub-North",
    "destination": "Cold-Room",
    "departed_at": "2026-01-01T20:00:00Z",
    "arrived_at": "2026-01-01T21:00:00Z"
  }' | pretty

echo
echo "== the old snapshot is now invalid =="
curl -sS -o "$WORK/old.json" -w 'HTTP %{http_code}\n' "$API/traces/$TRACE_ID"
pretty <"$WORK/old.json" || cat "$WORK/old.json"

echo
echo "== a fresh trace reflects the corrected batch =="
curl -fsS -X POST "$API/traces" -H 'Content-Type: application/json' -d '{}' | pretty

echo
echo "== stale correction with seen_version=1 is rejected =="
curl -sS -o "$WORK/stale.json" -w 'HTTP %{http_code}\n' \
  -X POST "$API/batches/B${RUN}-1002/corrections" \
  -H 'Content-Type: application/json' \
  -d '{
    "seen_version": 1,
    "origin": "Hub-North",
    "destination": "Archive",
    "departed_at": "2026-01-01T20:00:00Z",
    "arrived_at": "2026-01-01T21:00:00Z"
  }'
pretty <"$WORK/stale.json" || cat "$WORK/stale.json"

echo
echo "== record scan receipts for B${RUN}-1001 (arrival scanned 5 min late) =="
curl -fsS -X POST "$API/scans" \
  -H 'Content-Type: application/json' \
  -d "{
    \"scans\": [
      {\"scan_id\": \"SC${RUN}-1\", \"batch_no\": \"B${RUN}-1001\",
       \"direction\": \"departure\", \"site\": \"Collection-Lab\",
       \"scanned_at\": \"2026-01-01T08:00:00Z\"},
      {\"scan_id\": \"SC${RUN}-2\", \"batch_no\": \"B${RUN}-1001\",
       \"direction\": \"arrival\", \"site\": \"Hub-North\",
       \"scanned_at\": \"2026-01-01T10:05:00Z\"}
    ]
  }" | pretty

echo
echo "== identical re-report is idempotent (duplicates, nothing recorded) =="
curl -fsS -X POST "$API/scans" \
  -H 'Content-Type: application/json' \
  -d "{
    \"scans\": [
      {\"scan_id\": \"SC${RUN}-1\", \"batch_no\": \"B${RUN}-1001\",
       \"direction\": \"departure\", \"site\": \"Collection-Lab\",
       \"scanned_at\": \"2026-01-01T08:00:00Z\"}
    ]
  }" | pretty

echo
echo "== reconcile B${RUN}-1001: arrival time mismatches the ledger =="
RECON=$(curl -fsS -X POST "$API/batches/B${RUN}-1001/reconciliations")
echo "$RECON" | pretty
RECON_ID=$(echo "$RECON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["reconciliation_id"])')

echo
echo "== correct B${RUN}-1001 to the scanned arrival time; report goes stale =="
curl -fsS -X POST "$API/batches/B${RUN}-1001/corrections" \
  -H 'Content-Type: application/json' \
  -d '{
    "seen_version": 1,
    "origin": "Collection-Lab",
    "destination": "Hub-North",
    "departed_at": "2026-01-01T08:00:00Z",
    "arrived_at": "2026-01-01T10:05:00Z"
  }' | pretty

echo
echo "== the old report stays readable but is marked stale =="
curl -fsS "$API/reconciliations/$RECON_ID" | pretty

echo
echo "== a fresh reconciliation against the corrected ledger matches =="
curl -fsS -X POST "$API/batches/B${RUN}-1001/reconciliations" | pretty
