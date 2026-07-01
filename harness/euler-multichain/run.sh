#!/usr/bin/env bash
# Flagship e2e: index EVERY Portal-supported Euler chain (15) in ONE Ponder app, full history
# [0 → finalized head] per chain, Portal backfill, Postgres storage. Reproducible:
#
#   docker compose up -d postgres
#   cp .env.example .env      # fill in PORTAL_API_KEY (+ SQD_RPC_KEY for the 10 SQD-served chains)
#   ./run.sh
#
# Metrics land in ./metrics/ (per-chain Portal fetch/decode/gate breakdown) and the Portal gate
# concurrency/backpressure is logged live (PORTAL_GATE_LOG). See REPORT.md for a captured run.
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] && { set -a; . ./.env; set +a; }
: "${PORTAL_API_KEY:?set PORTAL_API_KEY in .env (SQD Portal key)}"

echo "▶ ensuring Postgres is up"
docker compose up -d postgres >/dev/null
for _ in $(seq 1 40); do docker compose exec -T postgres pg_isready -U postgres -q 2>/dev/null && break; sleep 2; done

echo "▶ installing @subsquid/ponder"
# default: the published fork; override with SQD_PONDER_TARBALL=/path/to/tgz to test a local build
[ -n "${SQD_PONDER_TARBALL:-}" ] && node -e "const p=require('./package.json');p.dependencies['@subsquid/ponder']='file:'+process.env.SQD_PONDER_TARBALL;require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))"
npm install --no-audit --no-fund --silent

mkdir -p metrics
export PORTAL_METRICS_FILE="${PORTAL_METRICS_FILE:-$PWD/metrics/portal}"
export PORTAL_GATE_LOG="${PORTAL_GATE_LOG:-1}"
export PONDER_LOG_LEVEL="${PONDER_LOG_LEVEL:-info}"
echo "▶ starting 15-chain backfill (DB: ${DATABASE_URL:-pglite})"
exec ./node_modules/.bin/ponder start --schema euler
