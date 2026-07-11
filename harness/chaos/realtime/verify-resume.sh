#!/usr/bin/env bash
# verify-resume.sh - realtime chaos verifier for native Postgres.
#
# Usage:
#   verify-resume.sh <conn> <baseline-store.digest> <baseline-app.digest> <phase.log> <target-phase>
#
# Required env:
#   ROOT              repo root (defaults to current repo root)
#   PROBE_DIR         workspace where the `pg` package resolves
#   STORE_SCHEMA      ponder sync schema (default realtime-chaos)
#   APP_SCHEMA        app schema (default public)
set -uo pipefail

CONN="${1:?usage: verify-resume.sh <conn> <baseline-store.digest> <baseline-app.digest> <phase.log> <target-phase>}"
BASE_STORE="${2:?}"
BASE_APP="${3:?}"
PHASE_LOG="${4:?}"
TARGET="${5:?}"

CDIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="${ROOT:-$(cd "$CDIR/../../.." && pwd)}"
PROBE_DIR="${PROBE_DIR:?PROBE_DIR required}"
STORE_SCHEMA="${STORE_SCHEMA:-realtime-chaos}"
APP_SCHEMA="${APP_SCHEMA:-public}"
PSQL="${CHAOS_PSQL:-psql}"
EMPTY_APP_DIGEST="9ae7e2d5dd53674ab09ac347e9bc71b1"
CHECKPOINT_BLOCK_SQL_POS=27
CHECKPOINT_BLOCK_LEN=16

fail=0
phase_ok=0
store_match=0
app_match=0
empty_tripwire_ok=0
rows_ok=0
finalized_hi=""
dup_count=0

echo "▶ phase landing audit"
if [ ! -f "$PHASE_LOG" ]; then
  echo "  ✗ missing phase log: $PHASE_LOG"
  fail=1
else
  last_phase="$(node -e "const fs=require('fs');const lines=fs.readFileSync(process.argv[1],'utf8').trim().split(/\\n+/).filter(Boolean);let last='';for(const line of lines){const j=JSON.parse(line);if(j.blocked)last=j.name;}console.log(last)" "$PHASE_LOG" 2>/dev/null)"
  if [ "$last_phase" = "$TARGET" ]; then
    echo "  ✓ last blocked phase: $last_phase"
    phase_ok=1
  else
    echo "  ✗ KILL_RACE: last blocked phase '$last_phase' != target '$TARGET'"
    fail=1
  fi
fi

echo "▶ store digest"
store_digest="$( ( cd "$PROBE_DIR" && node "$ROOT/harness/chaos/pg-digest.mjs" "$CONN" --schema "$STORE_SCHEMA" ) )"
store_rc=$?
want_store="$(cat "$BASE_STORE" 2>/dev/null)"
if [ "$store_rc" -ne 0 ] || [ -z "$store_digest" ]; then
  echo "  ✗ store digest failed"
  fail=1
elif [ "$store_digest" = "$want_store" ]; then
  echo "  ✓ store digest identical: $store_digest"
  store_match=1
else
  echo "  ✗ store digest mismatch: got=$store_digest want=$want_store"
  fail=1
fi

echo "▶ app digest"
app_json="$( ( cd "$PROBE_DIR" && node "$ROOT/harness/chaos/realtime/app-digest.mjs" "$CONN" --schema "$APP_SCHEMA" --json ) )"
app_rc=$?
app_digest=""
vault_rows="0"
deposit_rows="0"
if [ "$app_rc" -eq 0 ] && [ -n "$app_json" ]; then
  app_facts="$(
    APP_DIGEST_JSON="$app_json" node -e "const d=JSON.parse(process.env.APP_DIGEST_JSON); const rows=(table)=>d.perTable.find((r)=>r.table===table)?.rows ?? 0; console.log([d.store, rows('vault'), rows('deposit')].join(' '));"
  )"
  read -r app_digest vault_rows deposit_rows <<< "$app_facts"
fi
want_app="$(cat "$BASE_APP" 2>/dev/null)"
if [ "$app_rc" -ne 0 ] || [ -z "$app_digest" ]; then
  echo "  ✗ app digest failed"
  fail=1
elif [ "$app_digest" = "$want_app" ]; then
  echo "  ✓ app digest identical: $app_digest"
  app_match=1
else
  echo "  ✗ app digest mismatch: got=$app_digest want=$want_app"
  fail=1
fi
if [ -n "$app_digest" ] && [ "$app_digest" = "$EMPTY_APP_DIGEST" ]; then
  echo "  ✗ app digest is the empty-table constant: $app_digest"
  fail=1
elif [ -n "$app_digest" ]; then
  echo "  ✓ app digest is non-empty: $app_digest"
  empty_tripwire_ok=1
else
  echo "  ✗ app digest is unavailable for empty-table tripwire"
  fail=1
fi
if [ "${vault_rows:-0}" -gt 0 ] && [ "${deposit_rows:-0}" -gt 0 ]; then
  echo "  ✓ app rows: vault=$vault_rows deposit=$deposit_rows"
  rows_ok=1
else
  echo "  ✗ app rows are vacuous: vault=${vault_rows:-0} deposit=${deposit_rows:-0}"
  fail=1
fi

echo "▶ double-indexed finalized rows"
finalized_hi="$(
  "$PSQL" -Atqc "
select coalesce(
  max(substring(finalized_checkpoint from $CHECKPOINT_BLOCK_SQL_POS for $CHECKPOINT_BLOCK_LEN)::numeric),
  -1
)::text
  from \"$APP_SCHEMA\"._ponder_checkpoint
 where finalized_checkpoint is not null;
" "$CONN" 2>/dev/null
)"
if [ -z "$finalized_hi" ] || [ "$finalized_hi" = "-1" ]; then
  echo "  ✗ finalized checkpoint unavailable in $APP_SCHEMA._ponder_checkpoint"
  fail=1
else
  echo "  ✓ finalized checkpoint head: $finalized_hi"
fi
dupes="$(
  "$PSQL" -Atqc "
with tables as (
  select table_name
    from information_schema.columns
   where table_schema = '$APP_SCHEMA'
     and column_name in ('block_number', 'log_index')
     and table_name not like '\\_ponder\\_%' escape '\\'
     and table_name not like '\\_reorg\\_\\_%' escape '\\'
   group by table_name
  having count(distinct column_name) = 2
)
select string_agg(format('%I', table_name), ',')
  from tables;
" "$CONN" 2>/dev/null
)"

dup_rows=""
finalized_scanned=0
if [ -n "$dupes" ] && [ -n "$finalized_hi" ] && [ "$finalized_hi" != "-1" ]; then
  echo "  ✓ scanned double-indexed tables: $dupes"
  IFS=',' read -r -a tables <<< "$dupes"
  for table in "${tables[@]}"; do
    scanned="$(
      "$PSQL" -Atqc "
select count(*)
  from \"$APP_SCHEMA\".$table
 where block_number <= $finalized_hi;
" "$CONN" 2>/dev/null
    )"
    finalized_scanned=$(( finalized_scanned + ${scanned:-0} ))
    rows="$(
      "$PSQL" -Atqc "
select '$table', block_number, log_index, count(*)
  from \"$APP_SCHEMA\".$table
 where block_number <= $finalized_hi
 group by block_number, log_index
having count(*) > 1;
" "$CONN" 2>/dev/null
    )"
    if [ -n "$rows" ]; then
      dup_rows="${dup_rows}${rows}
"
    fi
  done
else
  echo "  ✗ no double-indexed app tables scanned"
  fail=1
fi
echo "  · finalized-region rows scanned for dup: $finalized_scanned"

if [ -n "$dup_rows" ]; then
  dup_count="$(printf '%s\n' "$dup_rows" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')"
  echo "$dup_rows"
  echo "  ✗ duplicate finalized app rows"
  fail=1
elif [ -n "$dupes" ]; then
  echo "  ✓ zero duplicate finalized app rows"
fi

if [ -n "${VERIFY_FACTS:-}" ]; then
  VERIFY_FACTS="$VERIFY_FACTS" \
  VERIFY_FAIL="$fail" \
  VERIFY_PHASE_OK="$phase_ok" \
  VERIFY_STORE_MATCH="$store_match" \
  VERIFY_APP_MATCH="$app_match" \
  VERIFY_EMPTY_TRIPWIRE_OK="$empty_tripwire_ok" \
  VERIFY_ROWS_OK="$rows_ok" \
  VERIFY_FINALIZED_HI="${finalized_hi:-}" \
  VERIFY_FINALIZED_SCANNED="$finalized_scanned" \
  VERIFY_DUP_COUNT="$dup_count" \
  VERIFY_STORE_DIGEST="${store_digest:-}" \
  VERIFY_APP_DIGEST="${app_digest:-}" \
  node - <<'NODE'
const fs = require('node:fs');
const facts = {
  ok: process.env.VERIFY_FAIL === '0',
  phaseOk: process.env.VERIFY_PHASE_OK === '1',
  storeMatch: process.env.VERIFY_STORE_MATCH === '1',
  appMatch: process.env.VERIFY_APP_MATCH === '1',
  emptyTripwireOk: process.env.VERIFY_EMPTY_TRIPWIRE_OK === '1',
  rowsOk: process.env.VERIFY_ROWS_OK === '1',
  finalizedHead: process.env.VERIFY_FINALIZED_HI === ''
    ? null
    : Number(process.env.VERIFY_FINALIZED_HI),
  finalizedScannedRows: Number(process.env.VERIFY_FINALIZED_SCANNED || '0'),
  duplicateFinalizedRows: Number(process.env.VERIFY_DUP_COUNT || '0'),
  storeDigest: process.env.VERIFY_STORE_DIGEST || null,
  appDigest: process.env.VERIFY_APP_DIGEST || null,
};
fs.writeFileSync(process.env.VERIFY_FACTS, `${JSON.stringify(facts, null, 2)}\n`);
NODE
fi

exit "$fail"
