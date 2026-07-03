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
BASELINE_META="$BASELINE_DB.meta.json"
CHAOS_META="$CHAOS_DB.meta.json"
# a resume verification is only meaningful if the chaos store was actually killed enough — enforce the
# same kill floor kill-loop uses, at VERIFY time, from the chaos store's OWN recorded metadata.
MIN_KILLS="${MIN_KILLS:-1}"

# write baseline metadata describing what this baseline was built for, so a future reuse can confirm
# it still matches the chaos run it's asked to validate.
write_baseline_meta () {
  CHAOS_META_APP="$APP" CHAOS_META_FROM="$FROM" CHAOS_META_TO="$TO" CHAOS_META_PORTAL="$PORTAL" \
  CHAOS_META_TARBALL="${SQD_PONDER_TARBALL:-}" CHAOS_META_CHAIN_ID="$CHAIN_ID" CHAOS_META_FACTORY="$FACTORY" \
  CHAOS_META_SCENARIO="baseline" CHAOS_META_KILLS="0" \
    node "$CDIR/chaos-meta.mjs" write "$BASELINE_META"
}

# Every verify path REQUIRES the chaos store's real metadata (written by kill-loop.sh). We never
# synthesize it from this invocation's env — a synthesized record cannot prove the store was actually
# produced by a killed run of THIS app/range/portal/tarball, and would defeat the metadata check and
# the kill-floor enforcement (a hand-built store would sail through). Missing metadata → hard FAIL.
if [ ! -f "$CHAOS_META" ]; then
  echo "✗ chaos store $CHAOS_DB has NO metadata ($CHAOS_META) — refusing to verify. Re-run kill-loop.sh to produce it."
  exit 1
fi
# enforce kills >= MIN_KILLS from the recorded metadata BEFORE any diff — an under-killed store proves
# nothing about resume even if it happens to be byte-identical.
node "$CDIR/chaos-meta.mjs" kills "$CHAOS_META" "$MIN_KILLS" \
  || { echo "✗ chaos store did not clear the kill floor — refusing to verify"; exit 1; }

if [ ! -d "$BASELINE_DB" ]; then
  echo "▶ building clean baseline store $BASELINE_DB (no kills)"
  WORK="$(mktemp -d)"; WORKSPACES="$WORKSPACES $WORK"; cp -r "$APP/." "$WORK/"; cd "$WORK"
  [ -n "${SQD_PONDER_TARBALL:-}" ] && node -e "const p=require('./package.json');p.dependencies['@subsquid/ponder']='file:'+process.env.SQD_PONDER_TARBALL;require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))"
  # track the throwaway npm cache dir (local-tarball install only) so the trap removes it too
  NPM_CACHE=""; [ -n "${SQD_PONDER_TARBALL:-}" ] && { NPM_CACHE="$(mktemp -d)"; WORKSPACES="$WORKSPACES $NPM_CACHE"; }
  npm install --no-audit --no-fund --silent ${NPM_CACHE:+--cache "$NPM_CACHE"} || { echo "✗ install failed"; exit 1; }
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
  write_baseline_meta || { echo "✗ could not write baseline metadata"; exit 1; }
  # confirm the freshly-built baseline was built for the SAME app/range/portal/tarball (content hash
  # included) as the chaos store — e.g. a different SQD_PONDER_TARBALL between the chaos run and this
  # verify invocation must NOT be silently diffed as if it were the same build.
  node "$CDIR/chaos-meta.mjs" match "$BASELINE_META" "$CHAOS_META" \
    || { echo "✗ freshly-built baseline does not match the chaos run (different app/range/portal/tarball) — refusing"; exit 1; }
else
  echo "▶ reusing existing baseline store $BASELINE_DB — validating its metadata matches the chaos run"
  # a REUSED baseline must have been built for the SAME app/range/portal/tarball (INCLUDING the
  # tarball's sha256 content hash), else a byte-diff of two unrelated stores is meaningless (a silent
  # false pass). We compare the baseline's recorded metadata against the CHAOS store's OWN metadata
  # (guaranteed present above) — never a record synthesized from this invocation's env. Refuse a
  # stale/mismatched baseline.
  if [ ! -f "$BASELINE_META" ]; then
    echo "✗ reused baseline $BASELINE_DB has NO metadata — refusing (cannot prove it matches the chaos run). Delete it to rebuild."
    exit 1
  fi
  node "$CDIR/chaos-meta.mjs" match "$BASELINE_META" "$CHAOS_META" || { echo "✗ baseline is stale/mismatched — refusing to reuse it"; exit 1; }
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
