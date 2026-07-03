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

# A cell that declares `requires` (e.g. "explicit addresses" — the traces app on a non-eth chain whose
# per-chain Pool/Router addresses are not yet wired) MUST be given those overrides before it can run.
# Refuse to run it with cross-chain-wrong defaults unless the operator has supplied the needed env
# (checked here for the address case) — failing loud beats silently backfilling wrong addresses (#7).
if [ -n "${CELL_REQUIRES:-}" ]; then
  case "$CELL_REQUIRES" in
    *"explicit addresses"*)
      if [ -z "${POOL_ADDRESS:-}" ] || [ -z "${ROUTER_ADDRESS:-}" ]; then
        echo "✗ cell $CELL requires explicit per-chain addresses ($CELL_REQUIRES): set POOL_ADDRESS and ROUTER_ADDRESS"
        echo "  (the traces app defaults to Ethereum-mainnet Pool/Router; running them on chain $CELL_CHAIN_ID would index nonexistent contracts)"
        exit 2
      fi
      ;;
    *)
      echo "✗ cell $CELL declares requires='$CELL_REQUIRES' but run-cell.sh has no handler for it — refusing to run"
      exit 2
      ;;
  esac
fi

# WINDOW_OVERRIDE=FROM-TO forces a single window (smoke / debugging); skips head resolution.
if [ -n "${WINDOW_OVERRIDE:-}" ]; then
  IFS='-' read -r ov_from ov_to <<<"$WINDOW_OVERRIDE"
  CELL_WINDOWS="$ov_from|$ov_to|override"
  CELL_NEEDS_HEAD=""
  echo "▶ WINDOW_OVERRIDE → single window [$ov_from,$ov_to]"
fi

# Frontier / full-range windows resolve against a head. For REPRODUCIBILITY the pinned per-chain head
# in cells.json (CELL_PINNED_HEAD) is preferred — a live /finalized-head fetch makes the same cell
# resolve to different windows on every run, defeating "pinned head". Override with:
#   HEAD_OVERRIDE=<number>  → use that exact head        HEAD_OVERRIDE=live → force a live fetch
RESOLVED_HEAD=""
if [ -n "$CELL_NEEDS_HEAD" ]; then
  if [ -n "${HEAD_OVERRIDE:-}" ] && [ "${HEAD_OVERRIDE}" != "live" ]; then
    RESOLVED_HEAD="$HEAD_OVERRIDE"
    echo "▶ using HEAD_OVERRIDE=$RESOLVED_HEAD for frontier/full-range windows"
  elif [ "${HEAD_OVERRIDE:-}" != "live" ] && [ -n "$CELL_PINNED_HEAD" ]; then
    RESOLVED_HEAD="$CELL_PINNED_HEAD"
    echo "▶ using PINNED head $RESOLVED_HEAD (cells.json) for frontier/full-range windows — reproducible"
  else
    echo "▶ fetching LIVE Portal head for frontier/full-range windows: $CELL_PORTAL_URL/finalized-head"
    RESOLVED_HEAD="$(curl -sf "$CELL_PORTAL_URL/finalized-head" | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{try{process.stdout.write(String(JSON.parse(d).number))}catch{process.exit(1)}})')" \
      || { echo "✗ could not fetch Portal head"; exit 1; }
  fi
  echo "  resolved head=$RESOLVED_HEAD"
  eval "$(node "$VDIR/resolve-cell.mjs" "$CELL" --head "$RESOLVED_HEAD" --sh)"
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
# WINDOW_TMP tracks the in-flight window's tmpfiles so an interrupt mid-window does not leak them
# (each window normally removes its own on the happy path). Set KEEP_WORKSPACES=1 to retain.
WINDOW_TMP=""
cleanup () {
  kill "$METER_PID" 2>/dev/null
  pkill -f 'ponder start --schema diff_' 2>/dev/null
  [ -n "${KEEP_WORKSPACES:-}" ] && return
  rm -f "$METER_FILE"
  [ -n "$WINDOW_TMP" ] && rm -f $WINDOW_TMP
}
trap cleanup EXIT INT TERM

for _ in $(seq 1 40); do curl -sf "http://127.0.0.1:$METER_PORT/__count" >/dev/null 2>&1 && break; sleep 0.25; done
curl -sf "http://127.0.0.1:$METER_PORT/__count" >/dev/null 2>&1 || { echo "✗ meter did not start on :$METER_PORT"; exit 1; }
echo "▶ cell $CELL  app=$CELL_APP_NAME chain=$CELL_CHAIN_ID  meter :$METER_PORT → $CELL_RPC_SLUG"

# meter_total prints the current count, or NOTHING (empty) if the meter is unreachable / returns
# non-JSON — the node reader exits non-zero on a parse failure so a dead meter yields "" not "0".
# NOTE: no `set -e` here — the window loop relies on many idioms that legitimately return non-zero
# (grep -q ||, arithmetic that evaluates to 0, run_window returning the window verdict, kill 2>/dev/null),
# so set -e would abort mid-cell instead of recording every window. Instead each money-critical step
# (budget precheck, meter reset, meter count) is checked explicitly and fails the WINDOW, never
# silently records requests=0 from an unreachable meter.
meter_total () { curl -sf "http://127.0.0.1:$METER_PORT/__count" | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{try{const t=JSON.parse(d).total;if(!Number.isFinite(Number(t)))process.exit(1);process.stdout.write(String(t))}catch{process.exit(1)}})'; }

# run one window; args: from to tag shrunkFlag
run_window () {
  local from="$1" to="$2" tag="$3" shrunk="$4"
  local wlog; wlog="$(mktemp)"
  WINDOW_TMP="$wlog $wlog.tail"

  # budget guard — refuse to START a window once the campaign has met the ceiling (fails closed on a
  # corrupt/unreadable results file too, per budget-sum.mjs)
  node "$VDIR/budget-sum.mjs" --check >/dev/null || { echo "✗ BUDGET: refusing to start $CELL/$tag"; return 3; }

  # reset the meter — a dead/unreachable meter here MUST fail the window: without a working meter we
  # cannot count spend, so we must never proceed and record requests=0 (silently free real spend).
  curl -sf -X POST "http://127.0.0.1:$METER_PORT/__reset" >/dev/null || { echo "✗ METER: reset failed (meter down?) — failing $CELL/$tag"; return 4; }

  export DIFF_APP="$CELL_APP_PATH"
  export PONDER_RPC_URL_1="http://127.0.0.1:$METER_PORT"
  export PORTAL_URL_1="$CELL_PORTAL_URL"
  export CHAIN_ID="$CELL_CHAIN_ID"
  export INCLUDE_RECEIPTS="$CELL_RECEIPTS"
  # per-cell env overrides from cells.json (e.g. per-chain POOL_ADDRESS/ROUTER_ADDRESS for a non-eth
  # traces cell). Applied FIRST so an operator's explicit POOL_ADDRESS/ROUTER_ADDRESS in the
  # environment still wins for the `requires` case (they are exported here only if the cell sets them).
  [ -n "${CELL_ENV_EXPORTS:-}" ] && eval "$CELL_ENV_EXPORTS"
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

  # read the metered request count. An empty result = the meter died mid-window; we CANNOT record
  # requests=0 (that hides real spend from the budget guard), so the window fails outright.
  local requests; requests="$(meter_total)"
  if [ -z "$requests" ]; then
    echo "    ✗ METER: no request count after window (meter down?) — failing $CELL/$tag"
    [ -n "${KEEP_WORKSPACES:-}" ] || rm -f "$wlog" "$wlog.tail"

    return 4
  fi

  local matched; matched="$(sed -nE 's/.*logs[[:space:]]+portal=[[:space:]]*([0-9]+).*/\1/p' "$wlog" | head -1)"
  [ -n "$matched" ] || matched="nan"

  local pass=0
  [ "$rc" -eq 0 ] && pass=1
  tail -30 "$wlog" > "$wlog.tail"

  # persisting the result IS the budget record — if the write fails, the spend went untracked, so the
  # window must fail (never let a metered window's requests silently escape the running total).
  node "$VDIR/record-result.mjs" "$CELL" "$tag" "$from" "$to" "$pass" "$requests" "$dur" "$matched" "$shrunk" "$wlog.tail" \
    || { echo "    ✗ RECORD: failed to persist $CELL/$tag ($requests requests untracked) — failing"; [ -n "${KEEP_WORKSPACES:-}" ] || rm -f "$wlog" "$wlog.tail"; return 5; }
  echo "    $([ $pass = 1 ] && echo PASS || echo FAIL)  requests=$requests  ${dur}s  matched=$matched"
  [ $pass = 1 ] || { echo "    ── diff/run tail ──"; sed 's/^/    /' "$wlog.tail"; }

  # window status carries the diff verdict up to `exit $fail`: a FAILing window (pass=0) MUST make
  # the cell fail. Without this, run_window always returned 0 and cell exit status was always 0 —
  # a data-mismatch was recorded as pass=false in the json yet the script exited "success".
  local wstatus=0
  [ $pass = 1 ] || wstatus=1

  # auto-shrink: a dense window (> threshold matched rows) is halved and re-run once. The shrunk
  # sub-run's failure propagates too.
  if [ "$shrunk" = "0" ] && [ -n "$CELL_SHRINK" ] && [ "$matched" != "nan" ] && [ "$matched" -gt "$CELL_SHRINK" ] 2>/dev/null; then
    local half=$(( (to - from) / 2 ))
    echo "    ↳ auto-shrink: $matched > $CELL_SHRINK matched rows → halving to [$from,$(( from + half ))]"
    run_window "$from" "$(( from + half ))" "$tag+shrunk" 1 || wstatus=1
  fi

  [ -n "${KEEP_WORKSPACES:-}" ] || rm -f "$wlog" "$wlog.tail"
  return $wstatus
}

# record-result.mjs stamps the results doc with the head its windows were cut from (reproducibility).
[ -n "$RESOLVED_HEAD" ] && export RESOLVED_HEAD

fail=0
for spec in $CELL_WINDOWS; do
  IFS='|' read -r from to tag <<<"$spec"
  run_window "$from" "$to" "$tag" 0 || fail=1
done

echo "▶ cell $CELL done. results/$CELL.json  (cumulative budget: $(node "$VDIR/budget-sum.mjs")/$(node -e "console.log(require('$VDIR/budget.json').maxRequests)") requests)"
exit $fail
