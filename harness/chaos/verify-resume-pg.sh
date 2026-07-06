#!/usr/bin/env bash
# verify-resume-pg.sh — acceptance for a chaos-resumed store on native Postgres (issue #52); the pg
# analogue of harness/chaos/verify-resume.sh. Because the backend is crash-DURABLE, a SIGKILL mid-write
# is recovered by WAL replay, which legitimately changes PHYSICAL bytes while row CONTENT is identical.
# So byte-diff is WRONG here — store identity is a deterministic LOGICAL DIGEST over ponder_sync row
# content (pg-digest.mjs). The kill-floor + baseline-metadata-match gates are UNCHANGED from the repo's
# verify-resume.sh (its chaos-meta.mjs is reused byte-for-byte); only the identity check swaps
# byte-diff → logical digest, and PGlite intervals check → pg intervals check.
#
#   verify-resume-pg.sh <chaosConnString> <from> <to>
#     env (required):
#       BASELINE_URL     conn string of the persistent, complete baseline DB
#       CHAOS_META       path to the chaos store's <db>.meta.json (written by the driver)
#       BASELINE_META    path to the baseline's meta.json
#       PROBE_DIR        a workspace where `pg` resolves (an installed app-pg) — runs the .mjs tools
#       CHAOS_META_MJS   path to the repo's chaos-meta.mjs (reused unchanged)
#       DIGEST_MJS / CHECK_INTERVALS_MJS  paths to the pg tools in this dir (pg-digest.mjs,
#                                         check-intervals-pg.mjs)
#     MIN_KILLS (default 1): kill floor enforced from the chaos store's OWN recorded metadata.
#
# Exit 0 = VERIFIED (digest-identical + exact tiling + kill floor + metadata match); nonzero = FAIL.
set -uo pipefail

CHAOS_URL="${1:?usage: verify-resume-pg.sh <chaosConnString> <from> <to>}"
FROM="${2:?}"; TO="${3:?}"

BASELINE_URL="${BASELINE_URL:?BASELINE_URL required}"
CHAOS_META="${CHAOS_META:?CHAOS_META required}"
BASELINE_META="${BASELINE_META:?BASELINE_META required}"
PROBE_DIR="${PROBE_DIR:?PROBE_DIR required (a workspace where pg resolves)}"
CHAOS_META_MJS="${CHAOS_META_MJS:?CHAOS_META_MJS required}"
DIGEST_MJS="${DIGEST_MJS:?DIGEST_MJS required}"
CHECK_INTERVALS_MJS="${CHECK_INTERVALS_MJS:?CHECK_INTERVALS_MJS required}"
MIN_KILLS="${MIN_KILLS:-1}"

# ── gate 1: the chaos store must carry its OWN metadata (never synthesized from this env) ──────────
if [ ! -f "$CHAOS_META" ]; then
  echo "✗ chaos store has NO metadata ($CHAOS_META) — refusing to verify."
  exit 1
fi
# ── gate 2: kill floor from the recorded metadata (an under-killed store proves nothing) ───────────
node "$CHAOS_META_MJS" kills "$CHAOS_META" "$MIN_KILLS" \
  || { echo "✗ chaos store did not clear the kill floor — refusing to verify"; exit 1; }

# ── gate 3: baseline metadata must match the chaos run (same app/range/portal/tarball incl. sha256) ─
if [ ! -f "$BASELINE_META" ]; then
  echo "✗ baseline has NO metadata ($BASELINE_META) — refusing (cannot prove it matches the chaos run)."
  exit 1
fi
node "$CHAOS_META_MJS" match "$BASELINE_META" "$CHAOS_META" \
  || { echo "✗ baseline is stale/mismatched — refusing to reuse it"; exit 1; }

fail=0

# ── gate 4: LOGICAL DIGEST identity (chaos == baseline over ponder_sync row content) ──────────────
echo "▶ logical-digest identity (chaos vs baseline)"
CHAOS_DIGEST="$( ( cd "$PROBE_DIR" && node "$DIGEST_MJS" "$CHAOS_URL" ) )"
cd_rc=$?
BASE_DIGEST="$( ( cd "$PROBE_DIR" && node "$DIGEST_MJS" "$BASELINE_URL" ) )"
bd_rc=$?
if [ "$cd_rc" -ne 0 ] || [ "$bd_rc" -ne 0 ] || [ -z "$CHAOS_DIGEST" ] || [ -z "$BASE_DIGEST" ]; then
  # a digest that could not be computed must FAIL closed, never pass as an empty match.
  echo "  ❌ digest computation failed (chaos_rc=$cd_rc base_rc=$bd_rc chaos='$CHAOS_DIGEST' base='$BASE_DIGEST')"
  fail=1
elif [ "$CHAOS_DIGEST" = "$BASE_DIGEST" ]; then
  echo "  ✅ digest identical: $CHAOS_DIGEST"
else
  echo "  ❌ digest MISMATCH:"
  echo "     chaos    = $CHAOS_DIGEST"
  echo "     baseline = $BASE_DIGEST"
  # emit per-table detail to localize the divergence in the evidence log.
  echo "  ── per-table digests (chaos | baseline):"
  ( cd "$PROBE_DIR" && node "$DIGEST_MJS" "$CHAOS_URL" --json )    2>/dev/null | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{for(const t of JSON.parse(s).perTable)console.log('     C '+t.table+' '+t.digest+' rows='+t.rows)}catch{}})"
  ( cd "$PROBE_DIR" && node "$DIGEST_MJS" "$BASELINE_URL" --json ) 2>/dev/null | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{for(const t of JSON.parse(s).perTable)console.log('     B '+t.table+' '+t.digest+' rows='+t.rows)}catch{}})"
  fail=1
fi

# ── gate 5: intervals tile [from,to] exactly (pg-native) ──────────────────────────────────────────
echo "▶ intervals tiling check (pg)"
( cd "$PROBE_DIR" && node "$CHECK_INTERVALS_MJS" "$CHAOS_URL" "$FROM" "$TO" ) || fail=1

if [ "$fail" = 0 ]; then
  echo "✅ RESUME VERIFIED (pg): chaos store logically identical to baseline + intervals tile exactly"
else
  echo "❌ RESUME FAILED (pg) — see above"
fi
exit $fail
