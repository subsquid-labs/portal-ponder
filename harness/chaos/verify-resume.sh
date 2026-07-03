#!/usr/bin/env bash
# verify-resume.sh — acceptance for a chaos-resumed store (produced by kill-loop.sh):
#   1. byte-diff the chaos store against a CLEAN uninterrupted baseline (harness/validate/diff-batched.mjs)
#   2. assert ponder_sync.intervals tiles [from,to] exactly (harness/chaos/check-intervals.mjs)
# Both must pass: a resume that dropped or duplicated any row, or left an interval gap, fails here.
#
#   SQD_PONDER_TARBALL=/path/to/tgz bash harness/chaos/verify-resume.sh <chaosDb> <from> <to>
#
# The baseline is built once (BASELINE_DB, default /tmp/chaos-baseline) with the SAME app / range /
# portal and NO kills; set BASELINE_DB to an existing clean store to skip the rebuild.
set -uo pipefail

CDIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$CDIR/../.." && pwd)"

CHAOS_DB="${1:?usage: verify-resume.sh <chaosDb> <from> <to>}"
FROM="${2:?}"; TO="${3:?}"

# Throwaway install/diff workspaces are tracked and removed on ANY exit (success, failure, interrupt).
# The chaos store, the baseline store, and the baseline log are intentionally retained. Set
# KEEP_WORKSPACES=1 to keep the workspaces for debugging.
WORKSPACES=""
cleanup () {
  [ -n "${KEEP_WORKSPACES:-}" ] && return
  [ -n "$WORKSPACES" ] && rm -rf $WORKSPACES
}
trap cleanup EXIT INT TERM
APP="${CHAOS_APP:-$ROOT/harness/diff/euler-app}"
PORTAL="${CHAOS_PORTAL:-https://portal.sqd.dev/datasets/ethereum-mainnet}"
RPC="${CHAOS_RPC:-https://ethereum-rpc.publicnode.com}"
BASELINE_DB="${BASELINE_DB:-/tmp/chaos-baseline}"
CHAIN_ID="${CHAOS_CHAIN_ID:-1}"
FACTORY="${EULER_FACTORY:-0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e}"

if [ ! -d "$BASELINE_DB" ]; then
  echo "▶ building clean baseline store $BASELINE_DB (no kills)"
  WORK="$(mktemp -d)"; WORKSPACES="$WORKSPACES $WORK"; cp -r "$APP/." "$WORK/"; cd "$WORK"
  [ -n "${SQD_PONDER_TARBALL:-}" ] && node -e "const p=require('./package.json');p.dependencies['@subsquid/ponder']='file:'+process.env.SQD_PONDER_TARBALL;require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))"
  npm install --no-audit --no-fund --silent ${SQD_PONDER_TARBALL:+--cache "$(mktemp -d)"} || { echo "✗ install failed"; exit 1; }
  PONDER_START="$FROM" PONDER_END="$TO" PGLITE_DIR="$BASELINE_DB" \
  PORTAL_URL_1="$PORTAL" PONDER_RPC_URL_1="$RPC" CHAIN_ID="$CHAIN_ID" EULER_FACTORY="$FACTORY" \
  PORTAL_CHECKS=strict CI=true \
  ./node_modules/.bin/ponder start --schema baseline --port 44301 > /tmp/chaos-baseline.log 2>&1 &
  BP=$!
  for _ in $(seq 1 600); do
    grep -qiE 'Completed indexing across' /tmp/chaos-baseline.log && break
    kill -0 "$BP" 2>/dev/null || break
    sleep 2
  done
  pkill -f 'ponder start --schema baseline' 2>/dev/null; sleep 1
  grep -qiE 'Completed indexing across' /tmp/chaos-baseline.log || { echo "✗ baseline did not complete"; tail -4 /tmp/chaos-baseline.log; exit 1; }
  cd "$ROOT"
fi

fail=0
echo "▶ byte-diff chaos vs baseline"
# diff-batched needs @electric-sql/pglite resolvable — run it from a workspace that has it installed.
DIFFWORK="$(mktemp -d)"; WORKSPACES="$WORKSPACES $DIFFWORK"; cp -r "$APP/." "$DIFFWORK/"; cd "$DIFFWORK"
[ -d node_modules/@electric-sql/pglite ] || npm install --no-audit --no-fund --silent >/dev/null 2>&1
cp "$ROOT/harness/validate/diff-batched.mjs" "$DIFFWORK/diff-batched.mjs"
node "$DIFFWORK/diff-batched.mjs" "$CHAOS_DB" "$BASELINE_DB" || fail=1
cp "$CDIR/check-intervals.mjs" "$DIFFWORK/check-intervals.mjs"
echo "▶ intervals tiling check"
node "$DIFFWORK/check-intervals.mjs" "$CHAOS_DB" "$FROM" "$TO" || fail=1
cd "$ROOT"

[ $fail = 0 ] && echo "✅ RESUME VERIFIED: chaos store byte-identical to baseline + intervals tile exactly" \
             || echo "❌ RESUME FAILED (see above)"
exit $fail
