#!/usr/bin/env bash
# build-baseline-pg.sh — build the fresh Postgres-tier baseline for the chaos kill/resume campaign
# (issue #52) on the crash-durable native Postgres cluster. A clean, UNKILLED, COMPLETE store built
# with the SAME small-fixed-chunk params the campaign runs under, so the logical-digest identity check
# (pg-digest.mjs) is against a same-parameterization, same-backend baseline. Writes standard metadata
# via the repo's chaos-meta.mjs (so verify-resume-pg's `match` accepts it) plus a `backend`/`digest`/
# `tier1` block. Proves digest DETERMINISM: digests the completed store twice AND once more after a
# pg_ctl restart — all three must be identical.
#
# Env:
#   SQD_PONDER_TARBALL   (required)  path to the @subsquid/ponder tarball under test
#   CHAOS_APP            (required)  path to a Postgres-backed ponder app dir (installs the tarball,
#                                    reads its store URL from $CHAOS_PG_URL — see the app's ponder.config)
#   CHAOS_META_MJS       (required)  path to harness/chaos/chaos-meta.mjs (reused unchanged)
#   CHAOS_PORTAL / CHAOS_RPC         Portal dataset URL / RPC URL (public endpoints; defaulted)
#   CHAOS_CHAIN_ID / CHAOS_FACTORY   chain id / factory address (defaulted to the eth Euler factory)
#   CHAOS_FROM / CHAOS_TO            block range (defaulted)
#   CHAOS_CHUNK_BLOCKS / CHAOS_CHUNK_FIXED / CHAOS_READAHEAD   small-fixed-chunk params (2000/1/1)
#   CHAOS_PGPORT / CHAOS_PGSOCK      cluster TCP port / socket dir (must match pg-ctl-chaos.sh)
#   CHAOS_BASELINE_DBNAME            baseline DB name (default: chaos_baseline_t1)
#   CHAOS_BASELINE_META              output metadata path (default: baseline-pg.meta.json next to this)
#   CHAOS_PSQL / CHAOS_CREATEDB / CHAOS_DROPDB   client bins (default: psql/createdb/dropdb on PATH)
set -uo pipefail

CDIR="$(cd "$(dirname "$0")" && pwd)"

TARBALL="${SQD_PONDER_TARBALL:?SQD_PONDER_TARBALL required (path to the @subsquid/ponder tarball)}"
APP="${CHAOS_APP:?CHAOS_APP required (a Postgres-backed ponder app dir)}"
CHAOS_META_MJS="${CHAOS_META_MJS:?CHAOS_META_MJS required (path to harness/chaos/chaos-meta.mjs)}"

PORTAL="${CHAOS_PORTAL:-https://portal.sqd.dev/datasets/ethereum-mainnet}"
RPC="${CHAOS_RPC:-https://ethereum-rpc.publicnode.com}"
CHAIN_ID="${CHAOS_CHAIN_ID:-1}"
FACTORY="${CHAOS_FACTORY:-0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e}"
FROM="${CHAOS_FROM:-20529207}"
TO="${CHAOS_TO:-20579207}"
PORT="${CHAOS_BASELINE_PORT:-44331}"

T1_CHUNK_BLOCKS="${CHAOS_CHUNK_BLOCKS:-2000}"
T1_CHUNK_FIXED="${CHAOS_CHUNK_FIXED:-1}"
T1_READAHEAD="${CHAOS_READAHEAD:-1}"

PGPORT="${CHAOS_PGPORT:-54329}"
PGHOST_TCP="127.0.0.1"
PGSOCK="${CHAOS_PGSOCK:-$CDIR/.chaos-pg/pgsock}"
BASELINE_DBNAME="${CHAOS_BASELINE_DBNAME:-chaos_baseline_t1}"
BASELINE_URL="postgres://postgres@$PGHOST_TCP:$PGPORT/$BASELINE_DBNAME"
BASELINE_META="${CHAOS_BASELINE_META:-$CDIR/baseline-pg.meta.json}"

PSQL="${CHAOS_PSQL:-psql}"
CREATEDB="${CHAOS_CREATEDB:-createdb}"
DROPDB="${CHAOS_DROPDB:-dropdb}"

DIGEST_MJS="$CDIR/pg-digest.mjs"
CHECK_INTERVALS_MJS="$CDIR/check-intervals-pg.mjs"
ART="${CHAOS_ART:-$CDIR/.chaos-pg/artifacts}"
LOG="$ART/baseline-pg-build.log"
mkdir -p "$ART"

log () { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

log "▶ building fresh Postgres-tier baseline → db=$BASELINE_DBNAME (chunk=$T1_CHUNK_BLOCKS fixed=$T1_CHUNK_FIXED readahead=$T1_READAHEAD)"
[ -f "$TARBALL" ] || { log "✗ tarball not found: $TARBALL"; exit 2; }
[ -d "$APP" ] || { log "✗ app not found: $APP"; exit 2; }
[ -f "$CHAOS_META_MJS" ] || { log "✗ chaos-meta.mjs not found: $CHAOS_META_MJS"; exit 2; }

# cluster must be up
bash "$CDIR/pg-ctl-chaos.sh" ensure || { log "✗ could not ensure pg cluster"; exit 1; }

export PGHOST="$PGSOCK" PGPORT="$PGPORT"
# refuse to clobber an existing baseline (persistent evidence).
if "$PSQL" -U postgres -Atqc "select 1 from pg_database where datname='$BASELINE_DBNAME'" postgres | grep -q 1; then
  log "✗ baseline DB $BASELINE_DBNAME already exists — refusing to overwrite (drop it first)"
  exit 2
fi
"$CREATEDB" -U postgres "$BASELINE_DBNAME" || { log "✗ createdb failed"; exit 1; }
log "created baseline DB $BASELINE_DBNAME"

# install the app into a throwaway workspace
WORK="$(mktemp -d)"
NPM_CACHE="$(mktemp -d)"
BASELINE_PIDFILE="$(mktemp)"
cleanup () { rm -rf "$WORK" "$NPM_CACHE"; rm -f "$BASELINE_PIDFILE"; }
trap cleanup EXIT INT TERM

cp -r "$APP/." "$WORK/"
SQD_PONDER_TARBALL="$TARBALL" node -e "const p=require('$WORK/package.json');p.dependencies['@subsquid/ponder']='file:'+process.env.SQD_PONDER_TARBALL;require('fs').writeFileSync('$WORK/package.json',JSON.stringify(p,null,2))"
( cd "$WORK" && npm install --no-audit --no-fund --silent --cache "$NPM_CACHE" ) || { log "✗ install failed"; exit 1; }
[ -x "$WORK/node_modules/.bin/ponder" ] || { log "✗ ponder bin missing"; exit 1; }
# copy the pg tools into the workspace so their `import 'pg'` resolves.
cp "$DIGEST_MJS" "$CHECK_INTERVALS_MJS" "$WORK/"

: > "$LOG"
( cd "$WORK" && \
  PONDER_START="$FROM" PONDER_END="$TO" \
  CHAOS_PG_URL="$BASELINE_URL" \
  PORTAL_URL_1="$PORTAL" PONDER_RPC_URL_1="$RPC" CHAIN_ID="$CHAIN_ID" EULER_FACTORY="$FACTORY" \
  PORTAL_CHECKS=strict CI=true \
  PORTAL_CHUNK_BLOCKS="$T1_CHUNK_BLOCKS" PORTAL_CHUNK_FIXED="$T1_CHUNK_FIXED" PORTAL_READAHEAD="$T1_READAHEAD" \
  setsid ./node_modules/.bin/ponder start --schema baseline --port "$PORT" >"$LOG" 2>&1 & echo $! > "$BASELINE_PIDFILE" )
BP="$(cat "$BASELINE_PIDFILE")"

DONE=0
for _ in $(seq 1 300); do
  grep -qiE 'Completed indexing across' "$LOG" && { DONE=1; break; }
  kill -0 "$BP" 2>/dev/null || break
  sleep 1
done
kill -9 -"$BP" 2>/dev/null
STRAG="$(ss -H -tlnp "sport = :$PORT" 2>/dev/null | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)"
[ -n "$STRAG" ] && kill -9 "$STRAG" 2>/dev/null
wait "$BP" 2>/dev/null

if [ "$DONE" != 1 ]; then
  log "✗ baseline did not complete — tail:"; tail -6 "$LOG"
  "$DROPDB" -U postgres --if-exists "$BASELINE_DBNAME"
  exit 1
fi
log "✓ baseline backfill completed"

# ── verify complete: intervals tile exactly ──────────────────────────────────────────────────────
if ! ( cd "$WORK" && node ./check-intervals-pg.mjs "$BASELINE_URL" "$FROM" "$TO" ); then
  log "✗ baseline intervals do NOT tile [$FROM,$TO] — refusing to accept an incomplete baseline"
  "$DROPDB" -U postgres --if-exists "$BASELINE_DBNAME"
  exit 1
fi
log "✓ baseline intervals tile [$FROM,$TO] exactly"

# ── digest DETERMINISM proof: twice on the same store, once more after a pg_ctl restart ───────────
D1="$( ( cd "$WORK" && node ./pg-digest.mjs "$BASELINE_URL" ) )"
D2="$( ( cd "$WORK" && node ./pg-digest.mjs "$BASELINE_URL" ) )"
log "digest #1 = $D1"
log "digest #2 (same store) = $D2"
log "▶ restarting pg cluster to prove digest survives a WAL-durable restart identically"
bash "$CDIR/pg-ctl-chaos.sh" restart || { log "✗ pg restart failed"; exit 1; }
D3="$( ( cd "$WORK" && node ./pg-digest.mjs "$BASELINE_URL" ) )"
log "digest #3 (after pg_ctl restart) = $D3"
if [ -z "$D1" ] || [ "$D1" != "$D2" ] || [ "$D1" != "$D3" ]; then
  log "✗ digest NOT deterministic (#1=$D1 #2=$D2 #3=$D3) — refusing baseline"
  exit 1
fi
log "✓ digest deterministic across 2 reads + 1 pg_ctl restart: $D1"

# per-table detail for the metadata
PERTABLE_JSON="$( ( cd "$WORK" && node ./pg-digest.mjs "$BASELINE_URL" --json ) )"

# ── standard metadata via the repo's chaos-meta.mjs (scenario=baseline) so `match` accepts it ─────
CHAOS_META_APP="$APP" CHAOS_META_FROM="$FROM" CHAOS_META_TO="$TO" CHAOS_META_PORTAL="$PORTAL" \
CHAOS_META_TARBALL="$TARBALL" CHAOS_META_CHAIN_ID="$CHAIN_ID" CHAOS_META_FACTORY="$FACTORY" \
CHAOS_META_SCENARIO="baseline" CHAOS_META_KILLS="0" \
  node "$CHAOS_META_MJS" write "$BASELINE_META" || { log "✗ could not write baseline metadata"; exit 1; }

TARBALL_SHA="$(sha256sum "$TARBALL" | awk '{print $1}')"
BASELINE_META="$BASELINE_META" DIGEST="$D1" PERTABLE_JSON="$PERTABLE_JSON" \
T1_CHUNK_BLOCKS="$T1_CHUNK_BLOCKS" T1_CHUNK_FIXED="$T1_CHUNK_FIXED" T1_READAHEAD="$T1_READAHEAD" \
BASELINE_DBNAME="$BASELINE_DBNAME" TARBALL_SHA="$TARBALL_SHA" PGPORT="$PGPORT" \
node -e '
const fs = require("fs");
const f = process.env.BASELINE_META;
const m = JSON.parse(fs.readFileSync(f, "utf8"));
m.backend = "postgres16-fsync-on";
m.databaseName = process.env.BASELINE_DBNAME;
m.pgPort = Number(process.env.PGPORT);
m.driver = "pg";
m.tarballSha256 = process.env.TARBALL_SHA;
m.tier1 = {
  chunkBlocks: Number(process.env.T1_CHUNK_BLOCKS),
  chunkFixed: Number(process.env.T1_CHUNK_FIXED),
  readahead: Number(process.env.T1_READAHEAD),
};
let pertable = [];
try { pertable = JSON.parse(process.env.PERTABLE_JSON).perTable.map((t)=>({table:t.table,digest:t.digest,rows:t.rows})); } catch {}
m.digest = {
  algorithm: "md5-of-sorted-per-table-md5(order-by-natural-key of md5((to_jsonb(row) - surrogate-id)::text))",
  store: process.env.DIGEST,
  deterministic: true,
  determinismProof: "digested twice on the same store + once after a pg_ctl restart; all identical",
  tables: pertable,
};
fs.writeFileSync(f, JSON.stringify(m, null, 2) + "\n");
' || { log "✗ could not augment baseline metadata"; exit 1; }

log "✓ baseline built + metadata written → $BASELINE_META"
cat "$BASELINE_META"
log "▶ baseline DB size:"
"$PSQL" -U postgres -Atc "select pg_size_pretty(pg_database_size('$BASELINE_DBNAME'))" postgres
