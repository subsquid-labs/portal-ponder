#!/usr/bin/env bash
# ctrl-cell.sh — the CTRL cell (evidence layer 4: patch inertness).
#
# Indexes the SAME app + range TWICE with @subsquid/ponder's package name resolving to two different
# builds, both on the STOCK RPC path (Portal unset):
#   • fork side     : @subsquid/ponder = file:$SQD_PONDER_TARBALL   (the Portal fork, portal inert)
#   • upstream side : @subsquid/ponder = npm:ponder@<version>       (genuine upstream ponder)
# then byte-diffs the two ponder_sync stores. Byte-identity proves the Portal patch changes NOTHING
# on the stock path — the fork is a true drop-in when no `portal:` is configured.
#
#   SQD_PONDER_TARBALL=/path/to/subsquid-ponder-0.16.6-sqd.2.tgz \
#   UPSTREAM_PONDER_VERSION=0.16.6 \
#   RPC_URL_OVERRIDE=https://ethereum-rpc.publicnode.com \   # or SQD_RPC_KEY for the paid endpoint
#     bash harness/validate/ctrl-cell.sh CTRL
set -uo pipefail

VDIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$VDIR/../.." && pwd)"
CELL="${1:-CTRL}"
UPSTREAM="${UPSTREAM_PONDER_VERSION:-0.16.6}"

: "${SQD_PONDER_TARBALL:?CTRL needs the fork tarball (file:…) to compare against genuine upstream}"

eval "$(node "$VDIR/resolve-cell.mjs" "$CELL" --sh)" || { echo "✗ cannot resolve $CELL"; exit 1; }
[ "$CELL_RUNNER" = "ctrl-cell" ] || echo "⚠ $CELL is not marked runner=ctrl-cell — proceeding anyway"
[ -n "$CELL_WINDOWS" ] || { echo "✗ no windows for $CELL"; exit 1; }

if [ -n "${RPC_URL_OVERRIDE:-}" ]; then
  RPC_TARGET="$RPC_URL_OVERRIDE"
else
  : "${SQD_RPC_KEY:?set SQD_RPC_KEY or RPC_URL_OVERRIDE}"
  RPC_TARGET="$CELL_RPC_BASE/$CELL_RPC_SLUG/$SQD_RPC_KEY"
fi

# meter (budget accounting; both sides go through it)
METER_PORT="$(( 8900 + (RANDOM % 300) ))"
METER_TARGET="$RPC_TARGET" METER_PORT="$METER_PORT" node "$VDIR/rpc-meter.mjs" &
METER_PID=$!
# Cleanup on ANY exit (including interrupt mid-window): kill the meter + backfills and remove the
# current window's throwaway workspace and temp files. Set KEEP_WORKSPACES=1 to retain them.
WORK=""
wlog=""
cleanup () {
  kill "$METER_PID" 2>/dev/null
  pkill -f 'ponder start --schema ctrl_' 2>/dev/null
  [ -n "${KEEP_WORKSPACES:-}" ] && return
  [ -n "$WORK" ] && rm -rf "$WORK"
  [ -n "$wlog" ] && rm -f "$wlog" "$wlog.tail"
}
trap cleanup EXIT INT TERM
# The meter MUST be ready before any metered window — otherwise both backfills would run with an
# untracked meter and the campaign would spend real requests it never counted. Hard-fail if it never
# came up (the loop below is best-effort; the assertion after it is the guarantee).
for _ in $(seq 1 40); do curl -sf "http://127.0.0.1:$METER_PORT/__count" >/dev/null 2>&1 && break; sleep 0.25; done
curl -sf "http://127.0.0.1:$METER_PORT/__count" >/dev/null 2>&1 || { echo "✗ meter did not start on :$METER_PORT — refusing to run metered CTRL windows"; exit 1; }

# budget precheck — share run-cell's guard so CTRL cannot start a metered window once the campaign has
# met the ceiling (fails closed on a corrupt results file too, per budget-sum.mjs).
node "$VDIR/budget-sum.mjs" --check >/dev/null || { echo "✗ BUDGET: refusing to start CTRL windows"; exit 3; }

# install a workspace whose @subsquid/ponder resolves to $2 (a file: tarball or npm:ponder alias)
install_ws () { # $1=dir  $2=dep-spec
  rm -rf "$1"; cp -r "$CELL_APP_PATH/." "$1"; ( cd "$1"
    node -e "const p=require('./package.json');p.dependencies['@subsquid/ponder']=process.argv[1];require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))" "$2"
    npm install --no-audit --no-fund --silent --cache "$(mktemp -d)" ) || { echo "✗ install failed for $2"; return 1; }
}

# run a bounded backfill (RPC only; portal unset) and wait for completion
run_backfill () { # $1=label $2=dir $3=schema $4=port
  ( cd "$2"
    rm -rf ./db
    PONDER_START="$FROM" PONDER_END="$TO" PGLITE_DIR="./db" \
    PONDER_RPC_URL_1="http://127.0.0.1:$METER_PORT" CHAIN_ID="$CELL_CHAIN_ID" \
    ERC20_ADDRESS="$CELL_ERC20" INCLUDE_RECEIPTS="$CELL_RECEIPTS" \
    PONDER_LOG_LEVEL=info CI=true \
    ./node_modules/.bin/ponder start --schema "$3" --port "$4" > "/tmp/ctrl-$1.log" 2>&1 & )
  local pid
  for _ in $(seq 1 "${MAXPOLL:-400}"); do
    grep -qiE 'Completed indexing across' "/tmp/ctrl-$1.log" 2>/dev/null && { pid=done; break; }
    grep -qiE 'error while processing|Build failed|Cannot find' "/tmp/ctrl-$1.log" 2>/dev/null && break
    sleep 3
  done
  pkill -f "ponder start --schema $3" 2>/dev/null; sleep 1
  [ "${pid:-}" = done ] || { echo "✗ $1 backfill did not complete:"; tail -4 "/tmp/ctrl-$1.log" | sed -E 's/\x1b\[[0-9;]*m//g'; return 1; }
}

fail=0
for spec in $CELL_WINDOWS; do
  IFS='|' read -r FROM TO tag <<<"$spec"
  echo "▶ CTRL window $tag [$FROM,$TO]"
  # per-window budget precheck — refuse to START a window once the ceiling is met (fails closed)
  node "$VDIR/budget-sum.mjs" --check >/dev/null || { echo "✗ BUDGET: refusing CTRL/$tag"; fail=1; break; }
  # reset the meter — a dead meter must fail the window, never record requests=0 (untracked spend)
  curl -sf -X POST "http://127.0.0.1:$METER_PORT/__reset" >/dev/null || { echo "✗ METER: reset failed — failing CTRL/$tag"; fail=1; continue; }
  WORK="$(mktemp -d)"
  install_ws "$WORK/fork" "file:$SQD_PONDER_TARBALL"   || { fail=1; rm -rf "$WORK"; continue; }
  install_ws "$WORK/up"   "npm:ponder@$UPSTREAM"       || { fail=1; rm -rf "$WORK"; continue; }
  t0=$SECONDS rc=0
  run_backfill fork "$WORK/fork" "ctrl_fork" 43280 || rc=1
  run_backfill up   "$WORK/up"   "ctrl_up"   43281 || rc=1
  dur=$(( SECONDS - t0 ))
  wlog="$(mktemp)"
  if [ $rc -eq 0 ]; then
    cp "$ROOT/harness/diff/diff.mjs" "$WORK/fork/diff.mjs"
    ( cd "$WORK/fork" && node diff.mjs "$WORK/fork/db" "$WORK/up/db" ) >"$wlog" 2>&1 || rc=1
    cat "$wlog"
  fi
  # read the metered count; empty = meter died → fail the window, never record requests=0
  requests="$(curl -sf "http://127.0.0.1:$METER_PORT/__count" | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{try{const t=JSON.parse(d).total;if(!Number.isFinite(Number(t)))process.exit(1);process.stdout.write(String(t))}catch{process.exit(1)}})')"
  if [ -z "$requests" ]; then echo "  ✗ METER: no request count (meter down?) — failing CTRL/$tag"; fail=1; [ -n "${KEEP_WORKSPACES:-}" ] || rm -rf "$WORK" "$wlog" "$wlog.tail"; continue; fi
  matched="$(sed -nE 's/.*logs[[:space:]]+portal=[[:space:]]*([0-9]+).*/\1/p' "$wlog" | head -1)"; [ -n "$matched" ] || matched="nan"
  pass=0; [ $rc -eq 0 ] && pass=1; [ $pass = 1 ] || fail=1
  tail -30 "$wlog" > "$wlog.tail"
  node "$VDIR/record-result.mjs" "$CELL" "$tag" "$FROM" "$TO" "$pass" "$requests" "$dur" "$matched" 0 "$wlog.tail"
  echo "  $([ $pass = 1 ] && echo 'PASS (fork-portal-unset ≡ upstream)' || echo FAIL)  requests=$requests  ${dur}s"
  [ -n "${KEEP_WORKSPACES:-}" ] || rm -rf "$WORK" "$wlog" "$wlog.tail"
done

echo "▶ CTRL done. results/$CELL.json"
exit $fail
