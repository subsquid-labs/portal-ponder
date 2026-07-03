#!/usr/bin/env bash
# run-cell.sh — run ONE validation-campaign cell end-to-end (see harness/validate/README.md).
#
#   SQD_PONDER_TARBALL=/path/to/subsquid-ponder-*.tgz \
#   SQD_RPC_KEY=<paid-rpc-key> \
#     bash harness/validate/run-cell.sh <cellId>
#
# For each window of the cell it: enforces the cumulative request budget, resets the request meter,
# runs harness/diff/run.sh (Portal-backfill vs stock-RPC-backfill, byte-diffed) with the paid RPC
# routed through rpc-meter.mjs, then records {pass, requests, duration, matchedLogs} to
# results/<cellId>.json. Dense windows (>autoShrink.threshold matched rows) are halved and re-run.
#
# Public smoke (no paid endpoints): point RPC_URL_OVERRIDE at a free RPC and PORTAL is public.
#   RPC_URL_OVERRIDE=https://ethereum-rpc.publicnode.com bash harness/validate/run-cell.sh L-eth
set -uo pipefail

VDIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$VDIR/../.." && pwd)"
CELL="${1:?usage: run-cell.sh <cellId>}"

command -v node >/dev/null || { echo "✗ node not found"; exit 1; }
command -v curl >/dev/null || { echo "✗ curl not found"; exit 1; }

# ── resolve the cell (config + windows). Frontier / full-range windows need the live Portal head. ──
eval "$(node "$VDIR/resolve-cell.mjs" "$CELL" --sh)" || { echo "✗ cannot resolve cell $CELL"; exit 1; }

if [ "$CELL_RUNNER" = "ctrl-cell" ]; then
  echo "→ $CELL is a CTRL cell; run it with ctrl-cell.sh (genuine upstream vs fork-portal-unset)"; exit 2
fi
if [ "$CELL_RUNNER" = "differential" ]; then
  echo "→ $CELL is a raw-breadth cell; run it with harness/compare/differential.ts (not run-cell.sh)"; exit 2
fi

# WINDOW_OVERRIDE=FROM-TO forces a single window (smoke / debugging); skips head resolution.
if [ -n "${WINDOW_OVERRIDE:-}" ]; then
  IFS='-' read -r ov_from ov_to <<<"$WINDOW_OVERRIDE"
  CELL_WINDOWS="$ov_from|$ov_to|override"
  CELL_NEEDS_HEAD=""
  echo "▶ WINDOW_OVERRIDE → single window [$ov_from,$ov_to]"
fi

if [ -n "$CELL_NEEDS_HEAD" ]; then
  echo "▶ resolving Portal head for frontier/full-range windows: $CELL_PORTAL_URL/finalized-head"
  HEAD="$(curl -sf "$CELL_PORTAL_URL/finalized-head" | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{try{process.stdout.write(String(JSON.parse(d).number))}catch{process.exit(1)}})')" \
    || { echo "✗ could not fetch Portal head"; exit 1; }
  echo "  head=$HEAD"
  eval "$(node "$VDIR/resolve-cell.mjs" "$CELL" --head "$HEAD" --sh)"
fi

[ -n "$CELL_WINDOWS" ] || { echo "✗ cell $CELL resolved to zero windows"; exit 1; }

# ── compose the paid-RPC target the meter forwards to ──────────────────────────────────────────
if [ -n "${RPC_URL_OVERRIDE:-}" ]; then
  RPC_TARGET="$RPC_URL_OVERRIDE"
else
  : "${SQD_RPC_KEY:?set SQD_RPC_KEY (paid rpc.subsquid.io key) or RPC_URL_OVERRIDE for a free RPC}"
  RPC_TARGET="$CELL_RPC_BASE/$CELL_RPC_SLUG/$SQD_RPC_KEY"
fi

if [ -z "${SQD_PONDER_TARBALL:-}" ]; then
  echo "⚠ SQD_PONDER_TARBALL unset — using the PUBLISHED @subsquid/ponder. Campaign runs MUST pin one tarball for reproducibility."
fi

# ── start the request meter (one per cell; reset per window) ───────────────────────────────────
METER_PORT="$(( 8600 + (RANDOM % 300) ))"
METER_FILE="$(mktemp)"
METER_TARGET="$RPC_TARGET" METER_PORT="$METER_PORT" METER_FILE="$METER_FILE" node "$VDIR/rpc-meter.mjs" &
METER_PID=$!
cleanup () { kill "$METER_PID" 2>/dev/null; pkill -f 'ponder start --schema diff_' 2>/dev/null; rm -f "$METER_FILE"; }
trap cleanup EXIT

for _ in $(seq 1 40); do curl -sf "http://127.0.0.1:$METER_PORT/__count" >/dev/null 2>&1 && break; sleep 0.25; done
curl -sf "http://127.0.0.1:$METER_PORT/__count" >/dev/null 2>&1 || { echo "✗ meter did not start on :$METER_PORT"; exit 1; }
echo "▶ cell $CELL  app=$CELL_APP_NAME chain=$CELL_CHAIN_ID  meter :$METER_PORT → $CELL_RPC_SLUG"

meter_total () { curl -sf "http://127.0.0.1:$METER_PORT/__count" | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>process.stdout.write(String(JSON.parse(d).total)))'; }

# run one window; args: from to tag shrunkFlag
run_window () {
  local from="$1" to="$2" tag="$3" shrunk="$4"
  local wlog; wlog="$(mktemp)"

  # budget guard — refuse to START a window once the campaign has met the ceiling
  node "$VDIR/budget-sum.mjs" --check >/dev/null || { echo "✗ BUDGET: refusing to start $CELL/$tag"; return 3; }

  curl -sf -X POST "http://127.0.0.1:$METER_PORT/__reset" >/dev/null

  export DIFF_APP="$CELL_APP_PATH"
  export PONDER_RPC_URL_1="http://127.0.0.1:$METER_PORT"
  export PORTAL_URL_1="$CELL_PORTAL_URL"
  export CHAIN_ID="$CELL_CHAIN_ID"
  export INCLUDE_RECEIPTS="$CELL_RECEIPTS"
  export PORTAL_CHUNK_FIXED=1 PORTAL_CHUNK_BLOCKS="${PORTAL_CHUNK_BLOCKS_OVERRIDE:-500000}" PORTAL_CHUNK_PINNED=1
  export PORTAL_CHECKS=strict
  [ -n "$CELL_EULER_FACTORY" ] && export EULER_FACTORY="$CELL_EULER_FACTORY"
  [ -n "$CELL_ERC20" ] && export ERC20_ADDRESS="$CELL_ERC20"
  [ "$CELL_DIFFER" = "batched" ] && export DIFF_SCRIPT="$VDIR/diff-batched.mjs" || unset DIFF_SCRIPT
  [ -n "$CELL_APP_HASH" ] && export DIFF_ARGS="--app-hash" || unset DIFF_ARGS
  export MAXPOLL="${MAXPOLL:-400}"

  local t0=$SECONDS rc=0
  echo "  ▷ window $tag  [$from,$to]"
  bash "$ROOT/harness/diff/run.sh" "$from" "$to" >"$wlog" 2>&1 || rc=$?
  local dur=$(( SECONDS - t0 ))
  local requests; requests="$(meter_total)"
  local matched; matched="$(sed -nE 's/.*logs[[:space:]]+portal=[[:space:]]*([0-9]+).*/\1/p' "$wlog" | head -1)"
  [ -n "$matched" ] || matched="nan"

  local pass=0
  [ "$rc" -eq 0 ] && pass=1
  tail -30 "$wlog" > "$wlog.tail"
  node "$VDIR/record-result.mjs" "$CELL" "$tag" "$from" "$to" "$pass" "$requests" "$dur" "$matched" "$shrunk" "$wlog.tail"
  echo "    $([ $pass = 1 ] && echo PASS || echo FAIL)  requests=$requests  ${dur}s  matched=$matched"
  [ $pass = 1 ] || { echo "    ── diff/run tail ──"; sed 's/^/    /' "$wlog.tail"; }

  # auto-shrink: a dense window (> threshold matched rows) is halved and re-run once
  if [ "$shrunk" = "0" ] && [ -n "$CELL_SHRINK" ] && [ "$matched" != "nan" ] && [ "$matched" -gt "$CELL_SHRINK" ] 2>/dev/null; then
    local half=$(( (to - from) / 2 ))
    echo "    ↳ auto-shrink: $matched > $CELL_SHRINK matched rows → halving to [$from,$(( from + half ))]"
    run_window "$from" "$(( from + half ))" "$tag+shrunk" 1
  fi

  rm -f "$wlog" "$wlog.tail"
  return 0
}

fail=0
for spec in $CELL_WINDOWS; do
  IFS='|' read -r from to tag <<<"$spec"
  run_window "$from" "$to" "$tag" 0 || fail=1
done

echo "▶ cell $CELL done. results/$CELL.json  (cumulative budget: $(node "$VDIR/budget-sum.mjs")/$(node -e "console.log(require('$VDIR/budget.json').maxRequests)") requests)"
exit $fail
