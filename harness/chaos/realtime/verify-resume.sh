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

fail=0

echo "▶ phase landing audit"
if [ ! -f "$PHASE_LOG" ]; then
  echo "  ✗ missing phase log: $PHASE_LOG"
  fail=1
else
  last_phase="$(node -e "const fs=require('fs');const lines=fs.readFileSync(process.argv[1],'utf8').trim().split(/\\n+/).filter(Boolean);let last='';for(const line of lines){const j=JSON.parse(line);if(j.blocked)last=j.name;}console.log(last)" "$PHASE_LOG" 2>/dev/null)"
  if [ "$last_phase" = "$TARGET" ]; then
    echo "  ✓ last blocked phase: $last_phase"
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
else
  echo "  ✗ store digest mismatch: got=$store_digest want=$want_store"
  fail=1
fi

echo "▶ app digest"
app_digest="$( ( cd "$PROBE_DIR" && node "$ROOT/harness/chaos/realtime/app-digest.mjs" "$CONN" --schema "$APP_SCHEMA" ) )"
app_rc=$?
want_app="$(cat "$BASE_APP" 2>/dev/null)"
if [ "$app_rc" -ne 0 ] || [ -z "$app_digest" ]; then
  echo "  ✗ app digest failed"
  fail=1
elif [ "$app_digest" = "$want_app" ]; then
  echo "  ✓ app digest identical: $app_digest"
else
  echo "  ✗ app digest mismatch: got=$app_digest want=$want_app"
  fail=1
fi

echo "▶ double-indexed finalized rows"
dupes="$(
  "$PSQL" -Atqc "
with fin as (
  select coalesce(max(end_block), -1) as hi
    from \"$STORE_SCHEMA\".intervals
),
tables as (
  select table_name
    from information_schema.columns
   where table_schema = '$APP_SCHEMA'
     and column_name in ('block_number', 'log_index')
   group by table_name
  having count(distinct column_name) = 2
)
select string_agg(format('%I', table_name), ',')
  from tables;
" "$CONN" 2>/dev/null
)"

dup_rows=""
if [ -n "$dupes" ]; then
  IFS=',' read -r -a tables <<< "$dupes"
  for table in "${tables[@]}"; do
    rows="$(
      "$PSQL" -Atqc "
with fin as (
  select coalesce(max(end_block), -1) as hi
    from \"$STORE_SCHEMA\".intervals
)
select '$table', block_number, log_index, count(*)
  from \"$APP_SCHEMA\".$table, fin
 where block_number <= fin.hi
 group by block_number, log_index
having count(*) > 1;
" "$CONN" 2>/dev/null
    )"
    if [ -n "$rows" ]; then
      dup_rows="${dup_rows}${rows}
"
    fi
  done
fi

if [ -n "$dup_rows" ]; then
  echo "$dup_rows"
  echo "  ✗ duplicate finalized app rows"
  fail=1
else
  echo "  ✓ zero duplicate finalized app rows"
fi

exit "$fail"
