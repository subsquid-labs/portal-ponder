#!/usr/bin/env bash
# Reproducible byte-identity test: index the SAME bounded range twice on @subsquid/ponder —
# once with `portal:` (the Portal backfill) and once without it (the stock RPC backfill; same
# package, only the backfill source differs) — then diff the ponder_sync store (logs ·
# transactions · receipts · traces) for byte-identity.
#
#   PONDER_RPC_URL_1=<eth archive RPC w/ debug_traceBlockByNumber> bash harness/diff/run.sh [start end]
#
# Optional: PORTAL_URL_1 (default public ethereum-mainnet), PORTAL_API_KEY, SQD_PONDER_TARBALL.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
APP="${DIFF_APP:-$ROOT/harness/diff/app}"   # DIFF_APP=harness/diff/euler-app for the larger factory run
START="${1:-${PONDER_START:-22200000}}"; END="${2:-${PONDER_END:-22200030}}"   # small default; the diff is per-block deterministic, so a wider range only adds coverage
PORTAL="${PORTAL_URL_1:-https://portal.sqd.dev/datasets/ethereum-mainnet}"
: "${PONDER_RPC_URL_1:?set PONDER_RPC_URL_1 to an eth archive RPC that supports debug_traceBlockByNumber}"

pkill -f 'ponder start --schema diff_' 2>/dev/null
WORK="$(mktemp -d)"; trap 'pkill -f "ponder start --schema diff_" 2>/dev/null' EXIT
cp -r "$APP/." "$WORK/"; cd "$WORK"
echo "▶ workspace $WORK  range [$START,$END]"
[ -n "${SQD_PONDER_TARBALL:-}" ] && node -e "const p=require('./package.json');p.dependencies['@subsquid/ponder']='file:$SQD_PONDER_TARBALL';require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))"
echo "▶ npm install"; npm install --no-audit --no-fund --silent || { echo "✗ install failed"; exit 1; }

run () { # $1=label  $2=portal-url-or-empty  $3=db  $4=port
  echo "▶ $1 backfill …"
  rm -rf "$3"
  export PONDER_START="$START" PONDER_END="$END" PGLITE_DIR="$3" PONDER_LOG_LEVEL=info CI=true
  if [ -n "$2" ]; then
    export PORTAL_URL_1="$2"
    # NO chunk tuning for normal ranges — the fork's intrinsic clamp bounds the fetch to the
    # backfill window automatically (this is what a real deploy→head client gets: zero params).
    # Only for a LARGE bounded *test* range do we split it into chunks, purely so a 500k-block
    # comparison shows incremental progress / read-ahead instead of one big fetch.
    if [ "$(( END - START ))" -gt 60000 ]; then export PORTAL_CHUNK_FIXED=1 PORTAL_CHUNK_BLOCKS=50000 PORTAL_READAHEAD="${READAHEAD:-4}"; else unset PORTAL_CHUNK_FIXED PORTAL_CHUNK_BLOCKS PORTAL_READAHEAD; fi
  else unset PORTAL_URL_1 PORTAL_CHUNK_FIXED PORTAL_CHUNK_BLOCKS PORTAL_READAHEAD; fi
  local t0=$SECONDS
  ./node_modules/.bin/ponder start --schema "diff_$1" --port "$4" > "/tmp/diff-$1.log" 2>&1 &
  local pid=$! done=0
  for _ in $(seq 1 "${MAXPOLL:-200}"); do   # 200×3s = 10min; raise MAXPOLL for very large ranges
    grep -qiE 'Completed indexing across' "/tmp/diff-$1.log" 2>/dev/null && { done=1; break; }
    grep -qiE 'error while processing|Build failed|Cannot find' "/tmp/diff-$1.log" 2>/dev/null && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 3
  done
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; sleep 1
  [ "$done" = 1 ] || { echo "✗ $1 did not complete:"; tail -4 "/tmp/diff-$1.log" | sed -E 's/\x1b\[[0-9;]*m//g'; exit 1; }
  local el=$(( SECONDS - t0 )); eval "WALL_$1=$el"
  echo "  ⏱ $1 backfill ${el}s — $(grep -oiE 'Completed indexing across all chains \([^)]*\)' "/tmp/diff-$1.log" | tail -1)"
}

run portal "$PORTAL" "$WORK/dbPortal" 42270
run rpc    ""         "$WORK/dbRpc"    42271

echo ""
SPEEDUP=$(awk -v p="${WALL_portal:-0}" -v r="${WALL_rpc:-0}" 'BEGIN{ if (p>0) printf "%.1fx faster", r/p; else print "n/a" }')
echo "⏱ BACKFILL WALL-CLOCK [$START,$END] — Portal ${WALL_portal}s vs RPC ${WALL_rpc}s  →  $SPEEDUP"
echo ""
echo "▶ diffing ponder_sync stores"
cp "$ROOT/harness/diff/diff.mjs" "$WORK/diff.mjs"   # run from $WORK so @electric-sql/pglite resolves
node "$WORK/diff.mjs" "$WORK/dbPortal" "$WORK/dbRpc"
