#!/usr/bin/env bash
# kill-loop.sh — chaos-resume harness. Runs a bounded Portal backfill into a PERSISTENT store,
# SIGKILLs it per trigger, restarts (ponder resumes from ponder_sync), and repeats until the range
# completes. Acceptance (aggregate across many invocations): ≥200 kills over ≥25 completed
# backfills, 100% byte-identical vs a clean baseline (verify-resume.sh), zero InvariantViolation
# (PORTAL_CHECKS=strict), and ponder_sync.intervals tiling the window exactly.
#
#   SQD_PONDER_TARBALL=/path/to/tgz CHAOS_FROM=20529207 CHAOS_TO=20579207 \
#   TRIGGER=poisson-45s CHAOS_DB=/tmp/chaos-store \
#     bash harness/chaos/kill-loop.sh
#
# Triggers (env TRIGGER): poisson-45s (default; robust, no log dependency),
#   on-chunk-fetch-log | on-discovery-log | between-rangedata-blockdata | on-child-flush-log.
# The log triggers match TRIGGER_REGEX against the run log; the fork emits few distinct per-event
# lines today, so the defaults target the [portalGate] gate ticker / trace logs — set TRIGGER_REGEX
# to your build's actual event line for a precise kill point. See harness/validate/README.md.
set -uo pipefail

CDIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$CDIR/../.." && pwd)"

APP="${CHAOS_APP:-$ROOT/harness/diff/euler-app}"
FROM="${CHAOS_FROM:?set CHAOS_FROM}"; TO="${CHAOS_TO:?set CHAOS_TO}"
PORTAL="${CHAOS_PORTAL:-https://portal.sqd.dev/datasets/ethereum-mainnet}"
RPC="${CHAOS_RPC:-https://ethereum-rpc.publicnode.com}"
DB="${CHAOS_DB:-/tmp/chaos-store}"
CHAIN_ID="${CHAOS_CHAIN_ID:-1}"
FACTORY="${EULER_FACTORY:-0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e}"
TRIGGER="${TRIGGER:-poisson-45s}"
MEAN="${MEAN:-45}"
MAX_KILLS="${MAX_KILLS:-30}"
# a resume run is only meaningful if it was actually killed at least once — a run that "completes"
# with kills==0 proves nothing about resume. Fail unless at least MIN_KILLS kills happened.
MIN_KILLS="${MIN_KILLS:-1}"
SCENARIO="${CHAOS_SCENARIO:-none}"
# run metadata sits next to the chaos DB so verify-resume can confirm the baseline it diffs against
# was built for the SAME app/range/portal/tarball (a stale/mismatched baseline is a silent false pass)
META="$DB.meta.json"

case "$TRIGGER" in
  on-chunk-fetch-log)         REGEX="${TRIGGER_REGEX:-\[portalGate\]}" ;;
  on-discovery-log)           REGEX="${TRIGGER_REGEX:-discover|factory}" ;;
  between-rangedata-blockdata) REGEX="${TRIGGER_REGEX:-syncBlockData|block.data}" ;;
  on-child-flush-log)         REGEX="${TRIGGER_REGEX:-insertChild|child}" ;;
  poisson-45s)                REGEX="" ;;
  *) echo "✗ unknown TRIGGER '$TRIGGER'"; exit 2 ;;
esac

WORK="$(mktemp -d)"
# NPM_CACHE tracks the throwaway npm cache dir (local-tarball install only) so the trap removes it
# too — the old trap leaked the mktemp -d cache each run.
NPM_CACHE=""
# Cleanup on ANY exit (completion, interrupt, InvariantViolation-stop): remove the throwaway install
# workspace, the npm cache, and the per-attempt run logs. The chaos store ($DB) is intentionally kept
# (verify-resume reads it). Set KEEP_WORKSPACES=1 to retain the workspace + logs for debugging.
ATTEMPT_LOG_GLOB="/tmp/chaos-attempt-*.log"
cleanup () {
  [ -n "${KEEP_WORKSPACES:-}" ] && return
  rm -rf "$WORK"
  [ -n "$NPM_CACHE" ] && rm -rf "$NPM_CACHE"
  rm -f $ATTEMPT_LOG_GLOB
}
trap cleanup EXIT INT TERM
cp -r "$APP/." "$WORK/"
cd "$WORK"
[ -n "${SQD_PONDER_TARBALL:-}" ] && node -e "const p=require('./package.json');p.dependencies['@subsquid/ponder']='file:'+process.env.SQD_PONDER_TARBALL;require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))"
echo "▶ installing @subsquid/ponder (chaos workspace $WORK)"
[ -n "${SQD_PONDER_TARBALL:-}" ] && NPM_CACHE="$(mktemp -d)"
npm install --no-audit --no-fund --silent ${NPM_CACHE:+--cache "$NPM_CACHE"} || { echo "✗ install failed"; exit 1; }

export PONDER_START="$FROM" PONDER_END="$TO" PGLITE_DIR="$DB"
export PORTAL_URL_1="$PORTAL" PONDER_RPC_URL_1="$RPC" CHAIN_ID="$CHAIN_ID" EULER_FACTORY="$FACTORY"
export PORTAL_CHECKS=strict PORTAL_GATE_LOG=1 PONDER_LOG_LEVEL="${PONDER_LOG_LEVEL:-trace}" CI=true
echo "▶ chaos store $DB  range [$FROM,$TO]  trigger=$TRIGGER  regex='${REGEX:-<poisson $MEAN s>}'"

poisson_sleep () { node -e "process.stdout.write(String(Math.max(1,Math.round(-$MEAN*Math.log(Math.random())))))"; }

completed () { grep -qiE 'Completed indexing across' "$1" 2>/dev/null; }
crashed_invariant () { grep -qiE 'InvariantViolation' "$1" 2>/dev/null; }

kills=0
attempt=0
DONE=0
while [ "$DONE" = 0 ] && [ "$kills" -lt "$MAX_KILLS" ]; do
  attempt=$(( attempt + 1 ))
  LOG="/tmp/chaos-attempt-$attempt.log"
  : > "$LOG"
  setsid ./node_modules/.bin/ponder start --schema chaos --port 44300 >"$LOG" 2>&1 &
  PID=$!
  PGID=$PID   # setsid → new process group == PID; kill the whole group
  echo "  ▷ attempt $attempt pid=$PID"

  if [ "$TRIGGER" = "poisson-45s" ]; then
    SLEPT=0; T="$(poisson_sleep)"
    while [ "$SLEPT" -lt "$T" ]; do
      completed "$LOG" && DONE=1 && break
      kill -0 "$PID" 2>/dev/null || break
      sleep 1; SLEPT=$(( SLEPT + 1 ))
    done
  else
    for _ in $(seq 1 600); do
      completed "$LOG" && { DONE=1; break; }
      grep -qiE "$REGEX" "$LOG" 2>/dev/null && break
      kill -0 "$PID" 2>/dev/null || break
      sleep 0.5
    done
  fi

  if crashed_invariant "$LOG"; then
    # a real failure to investigate — preserve the log past the cleanup trap by moving it aside
    KEEP_LOG="$(mktemp /tmp/chaos-invariant-XXXXXX.log)"; cp "$LOG" "$KEEP_LOG"
    echo "  ✗ InvariantViolation in attempt $attempt — STOP (see $KEEP_LOG)"; kill -9 -"$PGID" 2>/dev/null; exit 1
  fi

  if [ "$DONE" = 1 ]; then
    echo "  ✓ completed on attempt $attempt after $kills kills"; kill -9 -"$PGID" 2>/dev/null; break
  fi

  if kill -0 "$PID" 2>/dev/null; then
    kill -9 -"$PGID" 2>/dev/null; wait "$PID" 2>/dev/null
    kills=$(( kills + 1 ))
    echo "    ✗ killed (kill #$kills)"
  else
    # process exited on its own without completing → surface it (not a resume kill)
    completed "$LOG" && DONE=1 || { echo "    ⚠ process exited early without completion:"; tail -3 "$LOG"; }
  fi
done

echo "▶ kill-loop finished: attempts=$attempt kills=$kills done=$DONE  store=$DB"
[ "$DONE" = 1 ] || { echo "✗ did not complete within MAX_KILLS=$MAX_KILLS"; exit 1; }

# a completion with too few kills proves nothing about resume — fail unless MIN_KILLS were reached.
if [ "$kills" -lt "$MIN_KILLS" ]; then
  echo "✗ completed with kills=$kills < MIN_KILLS=$MIN_KILLS — a resume run must be killed at least MIN_KILLS times to prove anything"
  exit 1
fi

# persist run metadata alongside the store so verify-resume can confirm its baseline is comparable
CHAOS_META_APP="$APP" CHAOS_META_FROM="$FROM" CHAOS_META_TO="$TO" CHAOS_META_PORTAL="$PORTAL" \
CHAOS_META_TARBALL="${SQD_PONDER_TARBALL:-}" CHAOS_META_CHAIN_ID="$CHAIN_ID" CHAOS_META_FACTORY="$FACTORY" \
CHAOS_META_SCENARIO="$SCENARIO" CHAOS_META_KILLS="$kills" \
  node "$CDIR/chaos-meta.mjs" write "$META" || { echo "✗ could not write chaos metadata $META"; exit 1; }

echo "→ now verify: bash harness/chaos/verify-resume.sh $DB $FROM $TO"
