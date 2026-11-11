#!/usr/bin/env bash
set -euo pipefail

# End-to-end verification script for FluxCD on Agent Substrate (--mode=local or live cluster).
BASE_URL="${FLUX_SUBSTRATE_URL:-http://127.0.0.1:8080}"

echo "==> Verifying Mission-Control health at ${BASE_URL}/readyz"
curl -fsS "${BASE_URL}/readyz" >/dev/null

echo "==> Verifying 24-tenant fleet state at ${BASE_URL}/api/state"
STATE_JSON="$(curl -fsS "${BASE_URL}/api/state")"
python3 - "${STATE_JSON}" << 'EOF'
import json, sys
data = json.loads(sys.argv[1])
summary = data["summary"]
assert summary["totalTenants"] == 24, f"expected 24 tenants, got {summary['totalTenants']}"
assert summary["workerCapacity"] == 9, f"expected 9 worker slots, got {summary['workerCapacity']}"
print(f"Verified fleet: {summary['readyTenants']}/{summary['totalTenants']} Ready, {summary['suspendedActors']} SUSPENDED, {summary['snapshotCount']} DurableDir snapshots")
EOF

echo "==> Verifying / and /about HTML routes"
curl -fsS "${BASE_URL}/" | head -n 5 >/dev/null
curl -fsS "${BASE_URL}/about" | head -n 5 >/dev/null

echo "==> Triggering 24-Tenant Commit Wave across 9 WorkerPool slots"
curl -fsS -X POST "${BASE_URL}/api/wave" \
  -H "Content-Type: application/json" \
  -d '{"message":"Verify 24-tenant Commit Wave"}' >/dev/null

echo "==> Triggering Memory-Bomb Kustomize OOM Poison Pill on tenant-01 and recovering via RevertActor"
curl -fsS -X POST "${BASE_URL}/api/tenants/tenant-01/poison/oom" \
  -H "Content-Type: application/json" \
  -d '{"wait":true}' >/dev/null

curl -fsS -X POST "${BASE_URL}/api/tenants/tenant-01/recover" \
  -H "Content-Type: application/json" \
  -d '{"wait":true}' >/dev/null

echo "==> All end-to-end checks passed!"
