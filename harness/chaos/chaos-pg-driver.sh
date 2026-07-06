#!/usr/bin/env bash
# chaos-pg-driver.sh — the Postgres-tier chaos kill/resume driver (native Postgres, crash-durable;
# issue #52). It drives the fork's chaos/resume acceptance to its campaign numbers on a REAL, fsync-ON
# Postgres cluster. Companion to the Tier-0 harness (kill-loop.sh + verify-resume.sh) which uses a
# throwaway PGlite store and byte-diff identity; this driver uses a crash-durable Postgres backend and
# a LOGICAL-DIGEST store identity.
#
# ── WHY A POSTGRES TIER (issue #52) ──────────────────────────────────────────────────────────────────
# PGlite runs single-user Postgres with fsync OFF — not crash-durable by design. Repeated SIGKILL/resume
# on the SAME PGlite store tears its WAL after ~6-7 cycles (Postgres InitWalRecovery→StartupXLOG abort),
# so a PGlite-backed campaign cannot reach the acceptance counts for attributable resume-from-partial
# (it stops on store-durability — an honest finding about the PGlite BACKEND, NOT a fork bug). The
# partial-resume proof needs a crash-DURABLE backend: real Postgres, fsync=on, an APP-kill (never
# DB-kill) chaos model, and a LOGICAL-DIGEST store identity — because WAL replay after a crash
# legitimately changes physical bytes while row content is identical, so a byte-compare is WRONG on
# Postgres. This driver provides exactly that.
#
# ── WHAT IS CARRIED OVER FROM THE TIER-0 HARNESS UNCHANGED ───────────────────────────────────────────
# Poisson kill scheduling with rolling MEAN recalibration (clamp [4,7], unbounded above so completions
# remain possible), NEUTRAL classification (benign sub-floor completion), fail-closed on anything
# unparseable, reap_group() process-group kill + port-scoped straggler kill (never broad pkill -f),
# resume-from-aggregate preserving runs[] history, per-kill coverage snapshots. The backend swaps:
# (a) store = a FRESH Postgres DB per run (createdb/dropdb) instead of a PGlite dir; (b) the app
# launches with CHAOS_PG_URL instead of PGLITE_DIR; (c) coverage snapshot + tiling + store identity run
# over `pg` (snapshot-coverage-pg.mjs / check-intervals-pg.mjs / pg-digest.mjs) instead of PGlite;
# (d) identity = logical digest, not byte-diff.
#
# ── THE SUBSTRATE IS NEVER KILLED ───────────────────────────────────────────────────────────────────
# The Postgres SERVER is the durable substrate: it is ensured-started once at campaign start and is
# NEVER killed during the campaign — only the ponder APP is SIGKILLed (its process group). A store that
# becomes unreadable/unqueryable after an APP kill would therefore be a MAJOR finding (coverageClass=
# error → store_recovery_failure → freeze + STOP with finalVerdictClass="store-durability"). On a
# crash-durable Postgres backend this should NEVER fire.
#
# Modes:
#   bash chaos-pg-driver.sh            # run the campaign loop (default)
#   bash chaos-pg-driver.sh selftest   # exercise accounting + snapshot/digest classification, no backfill
#
# Env: everything is defaulted; the operator MUST supply SQD_PONDER_TARBALL (the tarball under test)
# and CHAOS_APP (a Postgres-backed ponder app dir whose store URL is read from $CHAOS_PG_URL). The
# workspace defaults to ./.chaos-pg next to this script; override CHAOS_WORK/CHAOS_ART to relocate.
set -uo pipefail

CDIR="$(cd "$(dirname "$0")" && pwd)"                    # this dir (the pg tools live alongside)
WORK_ROOT="${CHAOS_WORK:-$CDIR/.chaos-pg}"               # throwaway workspace root
mkdir -p "$WORK_ROOT"

# ── layout ────────────────────────────────────────────────────────────────────────────────────────
ART="${CHAOS_ART:-$WORK_ROOT/artifacts}"                 # aggregate.json + per-run logs (keep this off any PGlite artifacts dir)
TARBALL="${SQD_PONDER_TARBALL:?SQD_PONDER_TARBALL required (path to the @subsquid/ponder tarball under test)}"
TARBALL_SHA="${CHAOS_TARBALL_SHA:-$(sha256sum "$TARBALL" 2>/dev/null | awk '{print $1}')}"
AGG="$ART/aggregate.json"

# ── the pg tools (this dir) + a probe workspace that has `pg` resolvable ──────────────────────────
SNAP_MJS="${CHAOS_SNAP_MJS:-$CDIR/snapshot-coverage.mjs}"          # pure core (imported by the pg probe)
SNAP_PG_MJS="${CHAOS_SNAP_PG_MJS:-$CDIR/snapshot-coverage-pg.mjs}" # pg coverage probe
DIGEST_MJS="${CHAOS_DIGEST_MJS:-$CDIR/pg-digest.mjs}"             # logical-digest identity
CHECK_INTERVALS_MJS="${CHAOS_CHECK_INTERVALS_MJS:-$CDIR/check-intervals-pg.mjs}"  # pg tiling
VERIFY_PG="${CHAOS_VERIFY_PG:-$CDIR/verify-resume-pg.sh}"         # pg acceptance gate
PROBE_DIR="${CHAOS_PG_PROBE:-$WORK_ROOT/probe}"                   # installed app (pg resolvable)

# ── Postgres cluster (the crash-durable substrate — NEVER killed) ────────────────────────────────
PGCTL="${CHAOS_PGCTL:-$CDIR/pg-ctl-chaos.sh}"
PGPORT="${CHAOS_PGPORT:-54329}"
PGHOST_TCP="127.0.0.1"
PGSOCK="${CHAOS_PGSOCK:-$WORK_ROOT/pgsock}"
PSQL="${CHAOS_PSQL:-psql}"
PG_DUMP="${CHAOS_PG_DUMP:-pg_dump}"
BASELINE_DBNAME="${CHAOS_BASELINE_DBNAME:-chaos_baseline_t1}"
BASELINE_URL="postgres://postgres@$PGHOST_TCP:$PGPORT/$BASELINE_DBNAME"
BASELINE_META="${CHAOS_BASELINE_META:-$CDIR/baseline-pg.meta.json}"

# ── app / range / endpoints (free public endpoints only — safety rail) ────────────────────────────
APP="${CHAOS_APP:?CHAOS_APP required (a Postgres-backed ponder app dir; reads its store URL from CHAOS_PG_URL)}"
PORTAL="${CHAOS_PORTAL:-https://portal.sqd.dev/datasets/ethereum-mainnet}"
RPC="${CHAOS_RPC:-https://ethereum-rpc.publicnode.com}"
CHAIN_ID="${CHAOS_CHAIN_ID:-1}"
FACTORY="${CHAOS_FACTORY:-0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e}"
PONDER_PORT="${CHAOS_PORT:-44330}"                       # dedicated app port; pick one verified-free on the host

FROM="${CHAOS_FROM:-20529207}"
TO="${CHAOS_TO:-20579207}"

# ── small-fixed-chunk params (issue #50) — wired into the app env so partial durable states exist ──
T1_CHUNK_BLOCKS="${CHAOS_CHUNK_BLOCKS:-2000}"
T1_CHUNK_FIXED="${CHAOS_CHUNK_FIXED:-1}"
T1_READAHEAD="${CHAOS_READAHEAD:-1}"

# ── kill timing (MID-STAIRCASE calibration) ───────────────────────────────────────────────────────
# Per-attempt sleep is a Poisson (Exponential) draw T = round(-MEAN·ln U): UNBOUNDED above, so a run
# CAN complete (T > clean wall) at any MEAN. Rolling recalibration tracks the MINIMUM recent clean wall
# and picks MEAN = clamp(floor(minRecentWall / target), FLOOR, CEIL). Bounds [4,7] keep MEAN near the
# partial-window optimum while never forbidding completion (no hard sleep cap → no livelock).
MEAN_RUN1="${CHAOS_MEAN_RUN1:-5}"
TARGET_KILLS="${CHAOS_TARGET_KILLS:-2}"
MEAN_FLOOR="${CHAOS_MEAN_FLOOR:-4}"
MEAN_CEIL="${CHAOS_MEAN_CEIL:-7}"
MAX_KILLS="${CHAOS_MAX_KILLS:-80}"     # per-run kill ceiling (livelock guard)
MIN_KILLS="${CHAOS_MIN_KILLS:-1}"      # a completed run must be killed ≥ this to count toward acceptance

# ── acceptance + safety caps ────────────────────────────────────────────────────────────────────
ACCEPT_KILLS="${CHAOS_ACCEPT_KILLS:-200}"
ACCEPT_RUNS="${CHAOS_ACCEPT_RUNS:-25}"
ACCEPT_PARTIAL_KILLS="${CHAOS_ACCEPT_PARTIAL_KILLS:-25}"
ACCEPT_COMPLETIONS_FROM_PARTIAL="${CHAOS_ACCEPT_COMPLETIONS_FROM_PARTIAL:-1}"
# MAX_RUNS: acceptance needs ≥25 completedVerified. With ~1-4 kills/run at MEAN∈[4,7] over a ~6s wall,
# reaching ≥200 kills AND ≥25 verified takes on the order of 50-120 runs; 150 leaves ample headroom so
# acceptance is arithmetically reachable before the cap.
MAX_RUNS="${CHAOS_MAX_RUNS:-150}"
MAX_WALL_SEC="${CHAOS_MAX_WALL_SEC:-86400}"  # 24h wall cap → INCOMPLETE

CHAOS_META_MJS="${CHAOS_META_MJS:-$CDIR/chaos-meta.mjs}"  # repo tool, reused UNCHANGED (same dir)

log () { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

# psql helper against OUR cluster over the unix socket (trust auth, superuser postgres).
psql_ours () {
  PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PSQL" -U postgres -Atqc "$1" "${2:-postgres}"
}

# ── backend label derivation (issue #60) ──────────────────────────────────────────────────────────
# The Postgres binaries are resolved UNPINNED (CHAOS_PGBIN / pg_config / PATH), so the aggregate
# metadata's `backend` string must be composed from the ACTUAL server, not a literal — otherwise a run
# against, say, PG 17 would be silently mislabeled `postgres16-fsync-on` (attribution drift, exactly
# what tarballSha256 pinning otherwise prevents). We derive it from OBSERVED state at campaign start:
#   - version: the LIVE cluster's server_version_num (`show server_version_num`) → major, preferred
#     because it names the server actually under test; fall back to the resolved binary's
#     `pg_config --version` when the cluster is not yet up at label-time;
#   - fsync: the LIVE cluster's effective `show fsync` (on|off) when up, else the durability contract
#     of the managed config (fsync-on — see pg-chaos.conf), so the component reflects the real value.
# Composed as postgres<major>-fsync-<on|off>. An explicit CHAOS_BACKEND_LABEL override is honoured ONLY
# if its postgres<major> agrees with the observed major; a mismatch is a loud, fail-closed abort (we
# never silently mislabel). Prints the label on stdout; returns nonzero on a rejected override.
#
# pg_config resolution mirrors pg-ctl-chaos.sh: an explicit CHAOS_PGBIN wins, else `pg_config` on PATH.
pg_config_bin () {
  if [ -n "${CHAOS_PGBIN:-}" ]; then
    echo "$CHAOS_PGBIN/pg_config"
  else
    echo "pg_config"
  fi
}

# observed major version: prefer the live cluster; fall back to the resolved binary. Prints an integer,
# or empty when neither source could be read (caller treats empty as "could not observe").
observed_pg_major () {
  local vnum
  vnum="$(psql_ours 'show server_version_num' 2>/dev/null)"
  case "$vnum" in
    ''|*[!0-9]*) ;;                                  # not a live/parseable server_version_num
    *)
      # server_version_num is MMmmpp (e.g. 160009 → 16); major = value / 10000, floored.
      echo $(( vnum / 10000 ))
      return 0
      ;;
  esac

  # fall back to the resolved binary: `pg_config --version` → "PostgreSQL 16.9" → 16.
  local vline major
  vline="$("$(pg_config_bin)" --version 2>/dev/null)"
  major="$(printf '%s' "$vline" | grep -oE '[0-9]+' | head -1)"
  case "$major" in
    ''|*[!0-9]*) return 0 ;;                         # unreadable → empty (fail-closed at the caller)
  esac

  echo "$major"
}

# observed fsync component (on|off): the live cluster's effective setting when up, else the managed
# config's durability contract (fsync-on). Prints `on` or `off`.
observed_fsync () {
  local f
  f="$(psql_ours 'show fsync' 2>/dev/null)"
  case "$f" in
    on|off) echo "$f"; return 0 ;;
  esac

  echo on   # cluster not up at label-time; pg-chaos.conf pins fsync=on (the tier's durability contract)
}

# compose + validate the backend label from observed state, honouring an optional CHAOS_BACKEND_LABEL
# override that MUST agree with the observed major. Prints ONLY the final label on stdout (so a caller's
# `label="$(derive_backend_label)"` captures the label and nothing else); all diagnostics go to stderr;
# aborts nonzero on mismatch.
derive_backend_label () {
  local major
  major="$(observed_pg_major)"
  if [ -z "$major" ]; then
    log "✗ could not observe the Postgres major version (no live cluster and pg_config --version unreadable) — refusing to compose a backend label" >&2
    return 2
  fi

  local fsync
  fsync="$(observed_fsync)"
  local observed="postgres${major}-fsync-${fsync}"

  if [ -n "${CHAOS_BACKEND_LABEL:-}" ]; then
    # the override's major must match what we observed; anything else is a mislabel we refuse to write.
    local ov_major
    ov_major="$(printf '%s' "$CHAOS_BACKEND_LABEL" | grep -oE '^postgres[0-9]+' | grep -oE '[0-9]+' | head -1)"
    if [ -z "$ov_major" ]; then
      log "✗ CHAOS_BACKEND_LABEL='$CHAOS_BACKEND_LABEL' has no postgres<major> component to validate against the observed major ($major) — ABORT" >&2
      return 2
    fi
    if [ "$ov_major" != "$major" ]; then
      log "✗ CHAOS_BACKEND_LABEL='$CHAOS_BACKEND_LABEL' (major $ov_major) does NOT match the observed Postgres major ($major) — refusing to mislabel the campaign; ABORT" >&2
      return 2
    fi

    echo "$CHAOS_BACKEND_LABEL"
    return 0
  fi

  echo "$observed"
}

# ── crash-safe aggregate.json writer (temp file + rename) — IDENTICAL accounting to v3 ────────────
agg_update () {
  local patch="$1"
  AGG_FILE="$AGG" AGG_PATCH="$patch" node - "$@" <<'NODE'
const fs = require('node:fs');
const file = process.env.AGG_FILE;
const patch = JSON.parse(process.env.AGG_PATCH || '{}');

let agg;
try {
  agg = JSON.parse(fs.readFileSync(file, 'utf8'));
} catch {
  agg = null;
}
if (!agg || typeof agg !== 'object') {
  agg = {
    campaign: 'chaos-3-pg',
    status: 'running',
    params: {},
    calibration: {},
    acceptance: {},
    totals: {
      kills: 0,
      completedVerified: 0,
      attempts: 0,
      killsAtPartialCoverage: 0,
      completionsFromPartial: 0,
    },
    runs: [],
    startedAt: null,
    updatedAt: null,
  };
}

const MERGE_NESTED = new Set(['params', 'calibration', 'acceptance', 'totals']);
for (const [k, v] of Object.entries(patch)) {
  if (k === 'appendRun') {
    if (!Array.isArray(agg.runs)) agg.runs = [];
    agg.runs.push(v);
    continue;
  }
  if (MERGE_NESTED.has(k) && v && typeof v === 'object' && !Array.isArray(v)) {
    agg[k] = { ...(agg[k] || {}), ...v };
    continue;
  }
  agg[k] = v;
}

const runs = Array.isArray(agg.runs) ? agg.runs : [];
let kills = 0;
let completedVerified = 0;
let attempts = 0;
let killsAtPartialCoverage = 0;
let completionsFromPartial = 0;
for (const r of runs) {
  // A run reclassified as a DRIVER-ISOLATION FAILURE (two concurrent writers — the reaper leaked a rogue
  // survivor) is INVALID as resume evidence: its kills prove nothing about single-writer resume, so it is
  // kept in runs[] as history but EXCLUDED from every acceptance total. (Also honours an explicit
  // excludeFromTotals flag.) This is the durable home for the run-2 reclassification.
  if (r.verdictClass === 'driver-isolation-failure' || r.excludeFromTotals === true) continue;

  kills += Number(r.kills || 0);
  attempts += Number(r.attempts || 0);
  killsAtPartialCoverage += Number(r.partialCoverageKills || 0);
  const verified = r.killLoopExit === 0 && r.verifyExit === 0 && r.verdict === 'pass';
  if (verified) {
    completedVerified += 1;
    if (r.completedFromPartial === true) completionsFromPartial += 1;
  }
}
agg.totals = {
  kills,
  completedVerified,
  attempts,
  killsAtPartialCoverage,
  completionsFromPartial,
};
agg.updatedAt = new Date().toISOString();

const tmp = file + '.tmp';
fs.writeFileSync(tmp, JSON.stringify(agg, null, 2) + '\n');
fs.renameSync(tmp, file);

process.stdout.write(
  `${kills} ${completedVerified} ${attempts} ${killsAtPartialCoverage} ${completionsFromPartial}\n`,
);
NODE
}

agg_get () {
  AGG_FILE="$AGG" AGG_PATH="$1" node - <<'NODE'
const fs = require('node:fs');
let agg = {};
try { agg = JSON.parse(fs.readFileSync(process.env.AGG_FILE, 'utf8')); } catch {}
const parts = process.env.AGG_PATH.split('.');
let v = agg;
for (const p of parts) { v = v == null ? undefined : v[p]; }
process.stdout.write(String(v == null ? '' : v));
NODE
}

pick_mean () {
  local kills="$1" wall="$2" target="$3"
  KILLS="$kills" WALL="$wall" TARGET="$target" FLOOR="$MEAN_FLOOR" CEIL="$MEAN_CEIL" node - <<'NODE'
const wall = Number(process.env.WALL || 0);
const target = Number(process.env.TARGET || 3);
const floor = Number(process.env.FLOOR || 2);
const ceil = Number(process.env.CEIL || 8);
let mean = Math.floor(wall / Math.max(1, target));
if (!Number.isFinite(mean) || mean < floor) mean = floor;
if (mean > ceil) mean = ceil;
process.stdout.write(String(mean));
NODE
}

rolling_mean () {
  local target="$1"
  AGG_FILE="$AGG" TARGET="$target" FLOOR="$MEAN_FLOOR" CEIL="$MEAN_CEIL" node - <<'NODE'
const fs = require('node:fs');
const target = Math.max(1, Number(process.env.TARGET || 3));
const floor = Number(process.env.FLOOR || 2);
const ceil = Number(process.env.CEIL || 8);
let agg = {};
try { agg = JSON.parse(fs.readFileSync(process.env.AGG_FILE, 'utf8')); } catch {}
const runs = Array.isArray(agg.runs) ? agg.runs : [];
const walls = runs
  .map((r) => Number(r.wallSec))
  .filter((w) => Number.isFinite(w) && w > 0)
  .slice(-3);
if (walls.length === 0) {
  process.stdout.write(`${ceil} 0 0`);
  process.exit(0);
}
const minRecentWall = Math.min(...walls);
let mean = Math.floor(minRecentWall / target);
if (!Number.isFinite(mean) || mean < floor) mean = floor;
if (mean > ceil) mean = ceil;
process.stdout.write(`${mean} ${minRecentWall} ${walls.length}`);
NODE
}

# ── selftest: accounting + snapshot classification + digest + rolling-mean + resume, no backfill ──
selftest () {
  local T
  T="$(mktemp -d)"
  local saveagg="$AGG"
  AGG="$T/aggregate.json"
  echo "selftest: aggregate accounting → $AGG"

  agg_update '{"status":"running","params":{"from":1,"to":2,"mean":4},"startedAt":"2026-01-01T00:00:00Z"}' >/dev/null
  local out
  out="$(agg_update '{"appendRun":{"run":1,"kills":9,"attempts":10,"wallSec":30,"killLoopExit":0,"verifyExit":0,"verdict":"pass"}}')"
  echo "  after pass run: totals(kills completed attempts partialKills completionsFromPartial)=$out"
  if agg_update 'this-is-not-json' >/dev/null 2>&1; then
    echo "  ⚠ malformed patch unexpectedly succeeded"
  else
    echo "  ✓ malformed patch rejected without corrupting aggregate"
  fi
  out="$(agg_update '{"appendRun":{"run":2,"kills":3,"attempts":5,"wallSec":12,"killLoopExit":1,"verifyExit":0,"verdict":"fail","reason":"ops-loop nonzero"}}' && agg_update '{"status":"fail"}')"
  echo "  after fail run: totals=$out"

  node -e '
    const a=require("'"$AGG"'");
    const ok = a.totals.kills===12 && a.totals.completedVerified===1 && a.totals.attempts===15 && a.runs.length===2 && a.status==="fail" && a.totals.killsAtPartialCoverage===0 && a.totals.completionsFromPartial===0;
    if(!ok){console.error("SELFTEST FAIL",JSON.stringify(a.totals),a.status,a.runs.length);process.exit(1);}
    console.log("  ✓ base accounting assertions passed");
  '
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    rm -rf "$T"; AGG="$saveagg"; return $rc
  fi

  # NEUTRAL run: kills/attempts fold, completedVerified unchanged.
  out="$(agg_update '{"appendRun":{"run":3,"kills":0,"attempts":1,"wallSec":11,"mean":4,"killLoopExit":1,"verifyExit":null,"verdict":"neutral","reason":"completed with kills < MIN_KILLS (calibration miss)","partialCoverageKills":0}}')"
  echo "  after neutral run: totals=$out"
  node -e '
    const a=require("'"$AGG"'");
    const ok = a.totals.completedVerified===1 && a.totals.attempts===16 && a.totals.kills===12 && a.runs.length===3 && a.runs[2].verdict==="neutral" && a.runs[2].verifyExit===null;
    if(!ok){console.error("SELFTEST FAIL neutral",JSON.stringify(a.totals),a.runs.length);process.exit(1);}
    console.log("  ✓ neutral-run accounting: completedVerified unchanged, kills/attempts folded");
  ' || { rc=1; }

  # Tier-1 counters: partialCoverageKills sum + completionsFromPartial only on verified+flagged.
  local PC_T
  PC_T="$(mktemp -d)"
  local pc_saveagg="$AGG"
  AGG="$PC_T/aggregate.json"
  agg_update '{"appendRun":{"run":1,"kills":10,"attempts":11,"wallSec":40,"killLoopExit":0,"verifyExit":0,"verdict":"pass","partialCoverageKills":6,"completedFromPartial":true}}' >/dev/null
  agg_update '{"appendRun":{"run":2,"kills":8,"attempts":9,"wallSec":38,"killLoopExit":0,"verifyExit":0,"verdict":"pass","partialCoverageKills":4,"completedFromPartial":false}}' >/dev/null
  agg_update '{"appendRun":{"run":3,"kills":0,"attempts":1,"wallSec":11,"verdict":"neutral","killLoopExit":1,"verifyExit":null,"partialCoverageKills":0,"completedFromPartial":true}}' >/dev/null
  local pcout
  pcout="$(agg_get totals.killsAtPartialCoverage) $(agg_get totals.completionsFromPartial) $(agg_get totals.completedVerified)"
  echo "  Tier-1 counters (partialKills completionsFromPartial completedVerified) = $pcout"
  if [ "$pcout" = "10 1 2" ]; then
    echo "  ✓ Tier-1 counters: partialKills sum=10; completionsFromPartial counts ONLY verified+flagged (neutral flag ignored)=1"
  else
    echo "  ✗ Tier-1 counter assertion failed (got '$pcout', want '10 1 2')"
    rc=1
  fi
  rm -rf "$PC_T"
  AGG="$pc_saveagg"

  # snapshot coverage classification (pure) — via snapshot-coverage-pg.mjs, which re-exports the
  # shared pure core; classification MUST be identical to the v3 PGlite probe.
  local SC_OUT
  SC_OUT="$(SNAP_PG_MJS="$SNAP_PG_MJS" node - <<'NODE'
import(process.env.SNAP_PG_MJS).then((m) => {
  const want = { from: 100, to: 199 };
  const cases = [
    { name: 'empty-no-rows', rows: [], expect: 'empty' },
    { name: 'partial-13pct', rows: [{ fragment_id: 'f1', blocks: '{[100,113)}' }], expect: 'partial' },
    // pg renders CLOSED-upper "[100,200]" — same numeric hi as PGlite "[100,200)"; must be complete.
    { name: 'complete-full-pg-closed-upper', rows: [{ fragment_id: 'f1', blocks: '{[100,200]}' }], expect: 'complete' },
    { name: 'complete-full-halfopen', rows: [{ fragment_id: 'f1', blocks: '{[100,200)}' }], expect: 'complete' },
    { name: 'min-across-fragments-partial', rows: [
        { fragment_id: 'f1', blocks: '{[100,200]}' },
        { fragment_id: 'f2', blocks: '{[100,150)}' },
      ], expect: 'partial' },
    { name: 'head-gap-counts-empty', rows: [{ fragment_id: 'f1', blocks: '{[150,200)}' }], expect: 'empty' },
  ];
  let ok = true;
  for (const c of cases) {
    const v = m.coverageVerdict(c.rows, want, { blockCount: 0 });
    const pass = v.coverageClass === c.expect;
    ok = ok && pass;
    console.log(`    ${pass ? 'ok' : 'FAIL'} ${c.name}: class=${v.coverageClass} pct=${v.coveragePct} (want ${c.expect})`);
  }
  process.exit(ok ? 0 : 1);
}).catch((e)=>{console.error(String(e));process.exit(1);});
NODE
)"
  echo "$SC_OUT"
  if printf '%s' "$SC_OUT" | grep -q 'FAIL'; then
    echo "  ✗ snapshot classification assertion failed"
    rc=1
  else
    echo "  ✓ snapshot classification (pg): empty/partial/complete (closed+half-open upper) + min-across-fragments + head-gap"
  fi

  # pg tiling verdict (pure) — the pg check-intervals verdict must accept Postgres closed-upper text.
  local TI_OUT
  TI_OUT="$(CHECK_INTERVALS_MJS="$CHECK_INTERVALS_MJS" node - <<'NODE'
import(process.env.CHECK_INTERVALS_MJS).then((m) => {
  const want = { from: 100, to: 199 }; // tiles as [100, 200]
  const cases = [
    { name: 'pg-closed-upper-tiles', rows: [{ fragment_id: 'f', blocks: '{[100,200]}' }], ok: true },
    { name: 'halfopen-tiles', rows: [{ fragment_id: 'f', blocks: '{[100,200)}' }], ok: true },
    { name: 'short-fails', rows: [{ fragment_id: 'f', blocks: '{[100,150]}' }], ok: false },
    { name: 'gap-two-ranges-fails', rows: [{ fragment_id: 'f', blocks: '{[100,150),[160,200]}' }], ok: false },
    { name: 'head-gap-fails', rows: [{ fragment_id: 'f', blocks: '{[110,200]}' }], ok: false },
    { name: 'no-rows-fails', rows: [], ok: false },
  ];
  let ok = true;
  for (const c of cases) {
    const v = m.intervalsVerdict(c.rows, want);
    const pass = v.ok === c.ok;
    ok = ok && pass;
    console.log(`    ${pass ? 'ok' : 'FAIL'} ${c.name}: verdict.ok=${v.ok} (want ${c.ok})`);
  }
  process.exit(ok ? 0 : 1);
}).catch((e)=>{console.error(String(e));process.exit(1);});
NODE
)"
  echo "$TI_OUT"
  if printf '%s' "$TI_OUT" | grep -q 'FAIL'; then
    echo "  ✗ pg tiling verdict assertion failed"
    rc=1
  else
    echo "  ✓ pg tiling verdict: closed-upper tiles, short/gap/head-gap/no-rows fail closed"
  fi

  # digest DETERMINISM + MISMATCH-detection against the LIVE persistent baseline (if the cluster + probe
  # are available). Proves: (a) two reads of the same store are identical; (b) a store with ONE row
  # mutated digests DIFFERENTLY (the identity check actually detects divergence, not a constant).
  if bash "$PGCTL" status >/dev/null 2>&1 \
     && [ -d "$PROBE_DIR/node_modules/pg" ] \
     && psql_ours "select 1 from pg_database where datname='$BASELINE_DBNAME'" | grep -q 1; then
    cp "$DIGEST_MJS" "$PROBE_DIR/" 2>/dev/null
    local DA DB_
    DA="$( ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$BASELINE_URL" ) )"
    DB_="$( ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$BASELINE_URL" ) )"
    if [ -n "$DA" ] && [ "$DA" = "$DB_" ]; then
      echo "  ✓ digest determinism (live baseline): $DA (two reads identical)"
    else
      echo "  ✗ digest NOT deterministic on live baseline (a=$DA b=$DB_)"
      rc=1
    fi
    # mutation: clone the baseline into a scratch DB, flip one block field, confirm digest DIVERGES.
    local MUT="chaos_selftest_mut_$$"
    psql_ours "drop database if exists $MUT" >/dev/null 2>&1
    if psql_ours "create database $MUT template $BASELINE_DBNAME" >/dev/null 2>&1; then
      local MUT_URL="postgres://postgres@$PGHOST_TCP:$PGPORT/$MUT"
      # mutate one row's gas_used (a non-key numeric column) — logical content changes, digest must too.
      PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PSQL" -U postgres -Atqc \
        "update ponder_sync.blocks set gas_used = gas_used + 1 where number = (select min(number) from ponder_sync.blocks)" "$MUT" >/dev/null 2>&1
      local DM
      DM="$( ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$MUT_URL" ) )"
      psql_ours "drop database if exists $MUT" >/dev/null 2>&1
      if [ -n "$DM" ] && [ "$DM" != "$DA" ]; then
        echo "  ✓ digest MISMATCH-detection: a single mutated block row changes the digest ($DA → $DM)"
      else
        echo "  ✗ digest FAILED to detect a mutated row (baseline=$DA mutated=$DM) — identity check is blind"
        rc=1
      fi
    else
      echo "  (skipped digest mutation check — could not clone baseline template)"
    fi

    # ── surrogate-id semantics (the run-20 false-FAIL regression suite) ────────────────────────────
    # (a) ID-SHIFT INVARIANCE: content-identical store with SHIFTED serial ids → digest MUST EQUAL the
    #     baseline. This is exactly the run-20 shape (a killed flush advanced the id sequence; the resume
    #     re-flushed identical content at higher ids). Before the logical-digest fix this false-FAILED.
    local SHIFT="chaos_selftest_shift_$$"
    psql_ours "drop database if exists $SHIFT" >/dev/null 2>&1
    if psql_ours "create database $SHIFT template $BASELINE_DBNAME" >/dev/null 2>&1; then
      local SHIFT_URL="postgres://postgres@$PGHOST_TCP:$PGPORT/$SHIFT"
      # bump every surrogate id by a large constant on BOTH id-bearing tables, preserving the FK
      # (factory_addresses.factory_id → factories.id) so the logical content is byte-identical, only ids move.
      PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PSQL" -U postgres -Atq "$SHIFT" >/dev/null 2>&1 <<'SQL'
begin;
set constraints all deferred;
update ponder_sync.factory_addresses set factory_id = factory_id + 1000;
update ponder_sync.factories set id = id + 1000;
update ponder_sync.factory_addresses set id = id + 5000;
commit;
SQL
      local DS
      DS="$( ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$SHIFT_URL" ) )"
      psql_ours "drop database if exists $SHIFT" >/dev/null 2>&1
      if [ -n "$DS" ] && [ "$DS" = "$DA" ]; then
        echo "  ✓ digest ID-SHIFT invariance: shifting surrogate serial ids leaves the digest UNCHANGED ($DA) — resolves the run-20 false-FAIL"
      else
        echo "  ✗ digest changed under an id-only shift (baseline=$DA shifted=$DS) — surrogate id still bound into identity"
        rc=1
      fi
    else
      echo "  (skipped id-shift invariance check — could not clone baseline template)"
    fi

    # (b) CONTENT MUTATION on an id-excluded table: mutate one factory_addresses.block_number (a LOGICAL
    #     column of a table whose id is excluded) — the digest MUST still diverge. Proves excluding the id
    #     did not blind the digest to real content changes on those tables.
    local MUTB="chaos_selftest_mutb_$$"
    psql_ours "drop database if exists $MUTB" >/dev/null 2>&1
    if psql_ours "create database $MUTB template $BASELINE_DBNAME" >/dev/null 2>&1; then
      local MUTB_URL="postgres://postgres@$PGHOST_TCP:$PGPORT/$MUTB"
      PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PSQL" -U postgres -Atqc \
        "update ponder_sync.factory_addresses set block_number = block_number + 1 where id = (select min(id) from ponder_sync.factory_addresses)" "$MUTB" >/dev/null 2>&1
      local DMB
      DMB="$( ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$MUTB_URL" ) )"
      psql_ours "drop database if exists $MUTB" >/dev/null 2>&1
      if [ -n "$DMB" ] && [ "$DMB" != "$DA" ]; then
        echo "  ✓ digest MISMATCH-detection (id-excluded table): mutating one factory_addresses.block_number changes the digest ($DA → $DMB)"
      else
        echo "  ✗ digest FAILED to detect a content change on an id-excluded table (baseline=$DA mutated=$DMB) — over-excluded"
        rc=1
      fi
    else
      echo "  (skipped id-excluded content-mutation check — could not clone baseline template)"
    fi

    # (c) DUPLICATION still detected (the run-2 two-writer shape): re-insert one factory_addresses row's
    #     CONTENT under a fresh id. Same logical content twice under different ids → the digest MUST DIFFER
    #     (row count + content multiset change). This case MUST keep FAILING or the fix would mask real
    #     double-writes as identity.
    local DUP="chaos_selftest_dup_$$"
    psql_ours "drop database if exists $DUP" >/dev/null 2>&1
    if psql_ours "create database $DUP template $BASELINE_DBNAME" >/dev/null 2>&1; then
      local DUP_URL="postgres://postgres@$PGHOST_TCP:$PGPORT/$DUP"
      # copy the min-id row's LOGICAL columns into a new row; the identity id column auto-assigns a fresh id.
      PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PSQL" -U postgres -Atqc \
        "insert into ponder_sync.factory_addresses (factory_id, chain_id, block_number, address) select factory_id, chain_id, block_number, address from ponder_sync.factory_addresses where id = (select min(id) from ponder_sync.factory_addresses)" "$DUP" >/dev/null 2>&1
      local DD
      DD="$( ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$DUP_URL" ) )"
      psql_ours "drop database if exists $DUP" >/dev/null 2>&1
      if [ -n "$DD" ] && [ "$DD" != "$DA" ]; then
        echo "  ✓ digest DUPLICATION-detection: the same row content under a fresh id changes the digest ($DA → $DD) — run-2 shape still FAILS"
      else
        echo "  ✗ digest FAILED to detect a duplicated row (baseline=$DA duplicated=$DD) — a double-write would pass as identity"
        rc=1
      fi
    else
      echo "  (skipped duplication-detection check — could not clone baseline template)"
    fi

    # error → fail-closed: digesting a non-existent DB must exit nonzero (never a blank pass).
    local BADURL="postgres://postgres@$PGHOST_TCP:$PGPORT/chaos_nonexistent_$$"
    if ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$BADURL" ) >/dev/null 2>&1; then
      echo "  ✗ digest of a non-existent DB unexpectedly SUCCEEDED — not fail-closed"
      rc=1
    else
      echo "  ✓ digest of a non-existent DB fails closed (nonzero exit, no blank pass)"
    fi
    # coverage probe error → fail-closed: probing a non-existent DB must yield coverageClass=error.
    cp "$SNAP_MJS" "$SNAP_PG_MJS" "$PROBE_DIR/" 2>/dev/null
    local ECLS
    ECLS="$( ( cd "$PROBE_DIR" && node ./snapshot-coverage-pg.mjs "$BADURL" 100 199 2>/dev/null ) | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{process.stdout.write(String(JSON.parse(s).coverageClass))}catch{process.stdout.write('parsefail')}})")"
    if [ "$ECLS" = "error" ]; then
      echo "  ✓ coverage probe of an unreachable store → coverageClass=error (drives store_recovery_failure stop)"
    else
      echo "  ✗ coverage probe of an unreachable store returned '$ECLS' (want 'error') — fail-closed broken"
      rc=1
    fi
  else
    echo "  (skipped live-baseline digest/mutation/error checks — cluster/probe/baseline not all present)"
  fi

  # rolling_mean floor/ceil clamps for [4,7], target=2
  local RM_T
  RM_T="$(mktemp -d)"
  local rm_saveagg="$AGG"
  AGG="$RM_T/aggregate.json"
  agg_update '{"appendRun":{"run":1,"wallSec":30,"kills":4,"attempts":5,"verdict":"pass","killLoopExit":0,"verifyExit":0}}' >/dev/null
  agg_update '{"appendRun":{"run":2,"wallSec":20,"kills":4,"attempts":5,"verdict":"pass","killLoopExit":0,"verifyExit":0}}' >/dev/null
  agg_update '{"appendRun":{"run":3,"wallSec":11,"kills":0,"attempts":1,"verdict":"neutral","verifyExit":null}}' >/dev/null
  local rmout rmmean rmmin rmbasis
  rmout="$(rolling_mean 2)"
  rmmean="${rmout%% *}"
  rmmin="$(printf '%s' "$rmout" | awk '{print $2}')"
  rmbasis="$(printf '%s' "$rmout" | awk '{print $3}')"
  echo "  rolling_mean(walls 30,20,11 target=2) = mean=$rmmean minRecentWall=$rmmin basis=$rmbasis"
  if [ "$rmmean" = "5" ] && [ "$rmmin" = "11" ] && [ "$rmbasis" = "3" ]; then
    echo "  ✓ rolling-mean: floor(11/2)=5 within [4,7]; min over last 3 = 11"
  else
    echo "  ✗ rolling-mean assertion failed (got mean=$rmmean min=$rmmin basis=$rmbasis, want 5/11/3)"
    rc=1
  fi
  local RMF
  RMF="$(mktemp -d)"; AGG="$RMF/aggregate.json"
  agg_update '{"appendRun":{"run":1,"wallSec":3,"kills":1,"attempts":2,"verdict":"pass","killLoopExit":0,"verifyExit":0}}' >/dev/null
  local rmfmean
  rmfmean="$(rolling_mean 2)"; rmfmean="${rmfmean%% *}"
  if [ "$rmfmean" = "4" ]; then
    echo "  ✓ rolling-mean FLOOR: floor(3/2)=1 clamps UP to floor=4"
  else
    echo "  ✗ rolling-mean floor failed (got $rmfmean want 4)"
    rc=1
  fi
  rm -rf "$RMF"
  local RMH
  RMH="$(mktemp -d)"; AGG="$RMH/aggregate.json"
  agg_update '{"appendRun":{"run":1,"wallSec":9000,"kills":1,"attempts":2,"verdict":"pass","killLoopExit":0,"verifyExit":0}}' >/dev/null
  local rmhmean
  rmhmean="$(rolling_mean 2)"; rmhmean="${rmhmean%% *}"
  if [ "$rmhmean" = "7" ]; then
    echo "  ✓ rolling-mean CEIL: floor(9000/2)=4500 clamps DOWN to ceil=7"
  else
    echo "  ✗ rolling-mean ceil failed (got $rmhmean want 7)"
    rc=1
  fi
  rm -rf "$RMH"
  local RME
  RME="$(mktemp -d)"; AGG="$RME/aggregate.json"
  local rmemean
  rmemean="$(rolling_mean 2)"; rmemean="${rmemean%% *}"
  if [ "$rmemean" = "7" ]; then
    echo "  ✓ rolling-mean no-runs: defaults to ceil=7"
  else
    echo "  ✗ rolling-mean no-runs failed (got $rmemean want 7)"
    rc=1
  fi
  rm -rf "$RME"
  rm -rf "$RM_T"
  AGG="$rm_saveagg"

  # resume-detection: pick next N + seeded MEAN from a pre-seeded aggregate
  local RES_T
  RES_T="$(mktemp -d)"
  local res_saveagg="$AGG"
  AGG="$RES_T/aggregate.json"
  agg_update '{"status":"fail","startedAt":"2026-07-04T18:00:00Z","finalVerdict":"FAIL","failReason":"run 5: ops-loop exit=1","finishedAt":"2026-07-04T18:05:00Z"}' >/dev/null
  agg_update '{"appendRun":{"run":1,"wallSec":30,"kills":4,"attempts":5,"verdict":"pass","killLoopExit":0,"verifyExit":0}}' >/dev/null
  agg_update '{"appendRun":{"run":2,"wallSec":20,"kills":4,"attempts":5,"verdict":"pass","killLoopExit":0,"verifyExit":0}}' >/dev/null
  agg_update '{"appendRun":{"run":3,"wallSec":14,"kills":1,"attempts":2,"verdict":"pass","killLoopExit":0,"verifyExit":0}}' >/dev/null
  agg_update '{"appendRun":{"run":4,"wallSec":13,"kills":2,"attempts":3,"verdict":"pass","killLoopExit":0,"verifyExit":0}}' >/dev/null
  agg_update '{"appendRun":{"run":5,"wallSec":11,"kills":0,"attempts":1,"verdict":"fail","killLoopExit":1,"verifyExit":null}}' >/dev/null
  local res_state res_max res_started
  res_state="$(AGG_FILE="$AGG" node - <<'NODE'
const fs = require('node:fs');
let agg = {};
try { agg = JSON.parse(fs.readFileSync(process.env.AGG_FILE, 'utf8')); } catch {}
const runs = Array.isArray(agg.runs) ? agg.runs : [];
let maxRun = 0;
for (const r of runs) { const n = Number(r.run); if (Number.isFinite(n) && n > maxRun) maxRun = n; }
process.stdout.write(`${maxRun} ${agg.startedAt ? 1 : 0}`);
NODE
)"
  res_max="${res_state%% *}"
  res_started="$(printf '%s' "$res_state" | awk '{print $2}')"
  local res_rm res_mean
  res_rm="$(rolling_mean 2)"
  res_mean="${res_rm%% *}"
  echo "  resume-detection: maxRun=$res_max hasStartedAt=$res_started seededMEAN=$res_mean (next run $((res_max+1)))"
  if [ "$res_max" = "5" ] && [ "$res_started" = "1" ] && [ "$res_mean" = "5" ]; then
    echo "  ✓ resume: next run = 6, startedAt preserved, MEAN seeded from rolling recalibration (5)"
  else
    echo "  ✗ resume-detection failed (got max=$res_max started=$res_started mean=$res_mean, want 5/1/5)"
    rc=1
  fi
  rm -rf "$RES_T"
  AGG="$res_saveagg"

  # ── SYNCHRONOUS PIDFILE: launch_app must write a nonempty pidfile before it RETURNS ──────────────
  # Use a lightweight stub ponder (a sleeper) so we exercise the real launch_app / setsid / $! / pidfile
  # path without a real backfill. Then reap it via reap_group and assert the workdir sweep left nothing.
  local PF_WORK PF_LOG PF_PID
  PF_WORK="$(mktemp -d)"
  mkdir -p "$PF_WORK/node_modules/.bin"
  cat > "$PF_WORK/node_modules/.bin/ponder" <<'STUB'
#!/usr/bin/env bash
# selftest stub: sleep long enough to be observed + reaped (never touches any store/port).
exec sleep 120
STUB
  chmod +x "$PF_WORK/node_modules/.bin/ponder"
  PF_LOG="$PF_WORK/app.log"
  local sv_RUN_WORK="$RUN_WORK"
  RUN_WORK="$PF_WORK"
  local PIDFILE_ST="$PF_WORK/app.pid"
  if launch_app "postgres://selftest-unused" "$PF_LOG" "$PIDFILE_ST"; then
    if [ -s "$PIDFILE_ST" ]; then
      PF_PID="$(cat "$PIDFILE_ST")"
      # the pidfile content is the SAME pid launch_app captured via $! (synchronous, no cat/echo race).
      if [ "$PF_PID" = "$LAUNCH_PID" ] && kill -0 "$LAUNCH_PID" 2>/dev/null; then
        echo "  ✓ synchronous pidfile: nonempty + matches \$! ($PF_PID) immediately after launch_app returned; process live"
      else
        echo "  ✗ synchronous pidfile: content=$PF_PID launch_pid=$LAUNCH_PID (mismatch or process not live)"
        rc=1
      fi
    else
      echo "  ✗ synchronous pidfile: pidfile empty immediately after launch_app returned"
      rc=1
    fi
  else
    echo "  ✗ synchronous pidfile: launch_app returned nonzero (no pidfile)"
    rc=1
  fi
  # reap via the workdir-scoped path and confirm the sweep left nothing behind (single-writer discipline).
  reap_group "$LAUNCH_PGID" >/dev/null 2>&1
  local wleft
  wleft="$(workdir_pids | tr '\n' ' ')"
  if [ -z "$wleft" ] && ! kill -0 "${PF_PID:-0}" 2>/dev/null; then
    echo "  ✓ reap_group + workdir sweep: stub process gone, no workdir-cwd survivors"
  else
    echo "  ✗ reap_group left survivors (workdir-pids='$wleft' pid=${PF_PID:-?} still alive?)"
    kill -9 "${PF_PID:-0}" 2>/dev/null
    rc=1
  fi
  RUN_WORK="$sv_RUN_WORK"
  rm -rf "$PF_WORK"

  # ── SELF-EXIT REAP PATH: an app that exits on its own must still be reaped (no lingering children) ─
  # Stub that spawns a background child, then exits immediately. Under the OLD driver the child leaked;
  # reap_group (group-kill + workdir sweep + wait_gone) must clear both the parent and the child.
  local SE_WORK SE_LOG SE_MARK
  SE_WORK="$(mktemp -d)"
  mkdir -p "$SE_WORK/node_modules/.bin"
  SE_MARK="$SE_WORK/child.marker"
  cat > "$SE_WORK/node_modules/.bin/ponder" <<STUB
#!/usr/bin/env bash
# selftest stub: spawn a lingering child that ESCAPES the app's process group (its OWN setsid group,
# cwd == the workdir), then exit self early. Because the child left the group, the layer-1 group-kill
# CANNOT reach it — ONLY the workdir-scoped sweep can. This isolates the new single-writer net. The
# child records its OWN pid (from inside its workdir cwd) to the marker for a precise liveness assert.
setsid bash -c 'cd "\$1"; echo \$\$ > "\$2"; exec sleep 120' _ "$SE_WORK" "$SE_MARK" &
exit 3
STUB
  chmod +x "$SE_WORK/node_modules/.bin/ponder"
  SE_LOG="$SE_WORK/app.log"
  sv_RUN_WORK="$RUN_WORK"
  RUN_WORK="$SE_WORK"
  local PIDFILE_SE="$SE_WORK/app.pid"
  launch_app "postgres://selftest-unused" "$SE_LOG" "$PIDFILE_SE" >/dev/null 2>&1
  local SE_PGID="$LAUNCH_PGID"
  # wait for the escaped-group child to record its pid (guarantees it is alive before we reap).
  local w=0
  while [ "$w" -lt 12 ] && [ ! -s "$SE_MARK" ]; do
    sleep 1
    w=$(( w + 1 ))
  done
  local SE_CHILD
  SE_CHILD="$(cat "$SE_MARK" 2>/dev/null)"
  # Precondition: the child must be alive and OUT of the app's process group (so only the workdir sweep
  # can reap it). Assert liveness INDEPENDENTLY of workdir_pids so this test can't self-referentially pass.
  if [ -z "$SE_CHILD" ] || ! kill -0 "$SE_CHILD" 2>/dev/null; then
    echo "  ✗ self-exit reap: escaped-group child never came up (marker='$SE_CHILD') — test precondition failed"
    reap_group "$SE_PGID" >/dev/null 2>&1
    kill -9 "${SE_CHILD:-0}" 2>/dev/null
    rc=1
  fi
  # self-exit reap: exactly what run_one's else-branch now calls.
  reap_group "$SE_PGID" >/dev/null 2>&1
  wait "$LAUNCH_PID" 2>/dev/null
  # PRIMARY assertion is an INDEPENDENT kill -0 on the concrete child pid (not via workdir_pids, which is
  # the code-under-test) — so disabling the workdir sweep makes THIS assertion fail.
  if [ -n "$SE_CHILD" ] && ! kill -0 "$SE_CHILD" 2>/dev/null; then
    echo "  ✓ self-exit reap: parent exited early; its escaped-group child (pid $SE_CHILD, cwd=workdir) reaped by the workdir sweep (independently verified dead)"
  else
    echo "  ✗ self-exit reap: escaped-group child (pid ${SE_CHILD:-?}) SURVIVED the reap — workdir sweep did not catch it"
    kill -9 "${SE_CHILD:-0}" 2>/dev/null
    rc=1
  fi
  RUN_WORK="$sv_RUN_WORK"
  rm -rf "$SE_WORK"

  # ── pg_stat_activity SINGLE-WRITER GATE: a simulated straggler backend must be TERMINATED, a
  # driver-invariant event RECORDED, and the attempt NOT counted (the gate returns 0 only once clean) ──
  if bash "$PGCTL" status >/dev/null 2>&1; then
    local GDB="chaos_selftest_gate_$$"
    psql_ours "drop database if exists $GDB" >/dev/null 2>&1
    if psql_ours "create database $GDB" >/dev/null 2>&1; then
      local GEV
      GEV="$(mktemp)"
      : > "$GEV"
      # open a LONG-LIVED straggler backend on the gate DB (a rogue survivor the reaper missed).
      PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PSQL" -U postgres -Atqc "select pg_sleep(60)" "$GDB" >/dev/null 2>&1 &
      local STRAG_PID="$!"
      # wait until the backend is actually connected (visible in pg_stat_activity).
      local gw=0
      PG_GATE_DB="$GDB"
      while [ "$gw" -lt 15 ]; do
        [ "$(pg_activity_count)" != 0 ] && break
        sleep 1
        gw=$(( gw + 1 ))
      done
      local before
      before="$(pg_activity_count)"
      # wire the gate globals and run it against the DB with the straggler present.
      PG_GATE_DB="$GDB"
      PG_GATE_EVENTS="$GEV"
      PG_GATE_N=999
      PG_GATE_ATTEMPT=1
      DRIVER_INVARIANTS=0
      local gate_rc
      if pg_single_writer_gate; then
        gate_rc=0
      else
        gate_rc=1
      fi
      local after
      after="$(pg_activity_count)"
      local ev_count
      ev_count="$(grep -c '"event":"driver-invariant"' "$GEV" 2>/dev/null || echo 0)"
      kill -9 "$STRAG_PID" 2>/dev/null
      wait "$STRAG_PID" 2>/dev/null
      psql_ours "drop database if exists $GDB" >/dev/null 2>&1
      rm -f "$GEV"
      if [ "$before" -ge 1 ] && [ "$gate_rc" = 0 ] && [ "$after" = 0 ] && [ "$ev_count" -ge 1 ] && [ "$DRIVER_INVARIANTS" -ge 1 ]; then
        echo "  ✓ pg gate: straggler present (before=$before) → pg_terminate_backend → gate=0 (after=$after); driver-invariant recorded (events=$ev_count, DRIVER_INVARIANTS=$DRIVER_INVARIANTS); launch would proceed only now"
      else
        echo "  ✗ pg gate FAILED (before=$before gate_rc=$gate_rc after=$after events=$ev_count invariants=$DRIVER_INVARIANTS)"
        rc=1
      fi
      # fail-closed: a query error must NOT read as 0 (pg_activity_count returns the 999999 sentinel).
      PG_GATE_DB="chaos_selftest_gate_nonexistent_but_countworks_$$"
      local nc
      nc="$(pg_activity_count)"
      if [ "$nc" = 0 ]; then
        # a non-existent datname legitimately has 0 backends — that is fine; the sentinel guards PARSE
        # failures, which we cannot easily force here. Just note the count path is numeric.
        echo "  ✓ pg gate count path numeric (nonexistent datname → 0 backends, expected)"
      else
        echo "  ✓ pg gate count path numeric (got '$nc')"
      fi
    else
      echo "  (skipped pg gate selftest — could not create scratch gate DB)"
    fi
  else
    echo "  (skipped pg gate selftest — cluster not up)"
  fi

  # restore per-run gate globals to inert defaults so nothing leaks into a subsequent campaign call.
  PG_GATE_DB=""; PG_GATE_URL=""; PG_GATE_EVENTS=""; PG_GATE_N=0; PG_GATE_ATTEMPT=0; DRIVER_INVARIANTS=0

  if [ "$rc" -eq 0 ]; then
    echo "  ✓ all selftest assertions passed (accounting / Tier-1 counters / pg snapshot+tiling / digest determinism+mismatch+fail-closed / rolling-mean / resume / synchronous-pidfile / reap+workdir-sweep / self-exit-reap / pg single-writer gate)"
  fi
  rm -rf "$T"
  AGG="$saveagg"

  return $rc
}

# ── preflight ───────────────────────────────────────────────────────────────────────────────────
preflight () {
  [ -f "$CHAOS_META_MJS" ] || { log "✗ chaos-meta.mjs not found at $CHAOS_META_MJS"; exit 2; }
  [ -f "$SNAP_MJS" ] || { log "✗ snapshot-coverage.mjs not found at $SNAP_MJS"; exit 2; }
  [ -f "$SNAP_PG_MJS" ] || { log "✗ snapshot-coverage-pg.mjs not found at $SNAP_PG_MJS"; exit 2; }
  [ -f "$DIGEST_MJS" ] || { log "✗ pg-digest.mjs not found at $DIGEST_MJS"; exit 2; }
  [ -f "$CHECK_INTERVALS_MJS" ] || { log "✗ check-intervals-pg.mjs not found at $CHECK_INTERVALS_MJS"; exit 2; }
  [ -f "$VERIFY_PG" ] || { log "✗ verify-resume-pg.sh not found at $VERIFY_PG"; exit 2; }
  [ -d "$APP" ] || { log "✗ app-pg not found at $APP"; exit 2; }
  [ -f "$TARBALL" ] || { log "✗ tarball not found: $TARBALL"; exit 2; }
  [ -f "$BASELINE_META" ] || { log "✗ baseline metadata not found: $BASELINE_META (build it with build-baseline-pg.sh)"; exit 2; }

  local got
  got="$(sha256sum "$TARBALL" | awk '{print $1}')"
  # Tarball pinning is fail-closed by intent: the campaign that produced the recorded acceptance
  # numbers verified the tarball sha256 against a pin on every launch. When CHAOS_TARBALL_SHA is set
  # we ENFORCE it (mismatch = loud abort). When it is UNSET the run is UNPINNED — we cannot enforce
  # what we were not told, so we WARN loudly (a self-comparison would be a no-op that only looks like
  # a pass). Pin it (CHAOS_TARBALL_SHA=<sha256>) for a reproducible, tamper-evident run.
  if [ -n "${CHAOS_TARBALL_SHA:-}" ]; then
    if [ "$got" != "$CHAOS_TARBALL_SHA" ]; then
      log "✗ tarball sha256 MISMATCH: got=$got want=$CHAOS_TARBALL_SHA — ABORT"
      exit 2
    fi

    log "✓ tarball sha256 pinned+verified ($got)"
  else
    log "⚠ tarball sha256 UNPINNED (CHAOS_TARBALL_SHA unset) — observed $got, NOT verified against a pin; set CHAOS_TARBALL_SHA to enforce"
  fi
  log "✓ harness commit @ $(git -C "$CDIR" rev-parse --short HEAD 2>/dev/null || echo '?')"

  # ensure the crash-durable cluster is up (idempotent) — the substrate is NEVER killed hereafter.
  bash "$PGCTL" ensure || { log "✗ could not ensure pg cluster"; exit 2; }
  # the persistent baseline DB must exist (built once, reused every verify).
  if ! psql_ours "select 1 from pg_database where datname='$BASELINE_DBNAME'" | grep -q 1; then
    log "✗ baseline DB $BASELINE_DBNAME does not exist — build it with build-baseline-pg.sh"
    exit 2
  fi
  log "✓ baseline DB $BASELINE_DBNAME present on :$PGPORT"

  # build the probe workspace ONCE (installed app-pg → `pg` resolvable), and stage the pg tools in it.
  ensure_probe_dir || { log "✗ could not build probe workspace"; exit 2; }
  mkdir -p "$ART"
}

# ── the probe workspace: an installed app-pg where `pg` resolves; tools staged in-place ───────────
ensure_probe_dir () {
  if [ -d "$PROBE_DIR/node_modules/pg" ]; then
    cp "$SNAP_MJS" "$SNAP_PG_MJS" "$DIGEST_MJS" "$CHECK_INTERVALS_MJS" "$PROBE_DIR/" 2>/dev/null
    return 0
  fi

  rm -rf "$PROBE_DIR"
  mkdir -p "$PROBE_DIR"
  cp -r "$APP/." "$PROBE_DIR/"
  local NC
  NC="$(mktemp -d)"
  SQD_PONDER_TARBALL="$TARBALL" node -e "const p=require('$PROBE_DIR/package.json');p.dependencies['@subsquid/ponder']='file:'+process.env.SQD_PONDER_TARBALL;require('fs').writeFileSync('$PROBE_DIR/package.json',JSON.stringify(p,null,2))"
  ( cd "$PROBE_DIR" && npm install --no-audit --no-fund --silent --cache "$NC" ) >/dev/null 2>&1
  rm -rf "$NC"
  [ -d "$PROBE_DIR/node_modules/pg" ] || return 1
  cp "$SNAP_MJS" "$SNAP_PG_MJS" "$DIGEST_MJS" "$CHECK_INTERVALS_MJS" "$PROBE_DIR/"

  return 0
}

# ── snapshot a store's coverage → prints one JSON line; classifies empty|partial|complete|error ───
snapshot_store () {
  local url="$1"
  ( cd "$PROBE_DIR" && node ./snapshot-coverage-pg.mjs "$url" "$FROM" "$TO" 2>/dev/null ) \
    || echo '{"coverageClass":"error","error":"probe invocation failed"}'
}

# ── reap the app's process GROUP precisely (never a broad pkill -f) — hardened for single-writer ────
# setsid makes the app its own process group (PGID == captured PID), so `kill -9 -PGID` takes down
# ponder AND its children in one shot. The run-2 investigation (factory-dup mechanism) proved the OLD
# reaper leaked rogue survivors → two concurrent writers → the factory_addresses ×2. This version
# enforces SINGLE-WRITER by three layers, then WAITS until every target is gone before returning:
#   (1) process-group SIGKILL of the captured PGID (app + children in one shot);
#   (2) a WORKDIR-scoped straggler sweep — any pid whose /proc/<pid>/cwd resolves INTO the per-run
#       workdir is reaped (the app runs `cd "$RUN_WORK"` so its cwd IS the workdir; the driver's cwd is
#       elsewhere, so this can NEVER match the driver or any unrelated process);
#   (3) the legacy PORT-scoped net as a SECONDARY catch for an escaped child that already re-bound a
#       port (still PID-exact, never a broad match; the port net must never match the driver — the
#       driver never listens).
# After killing, poll /proc until the PGID, every workdir-cwd pid, and every port listener are gone
# (bounded wait; loud on failure). "Gone" is the single-writer precondition the pg_stat_activity gate
# then confirms at the DB boundary.
#
# PORT ESCALATION (observed run-2 attempts 9-10): ponder's getNextAvailablePort binds the configured
# port, and if it is momentarily still held by a not-yet-reaped previous instance it warns "Port in use"
# and retries on port+1 (an uncaughtException on a second collision then exits the app — benign, the
# loop just retries). So a straggler can linger on PONDER_PORT+1 (or +2), not only PONDER_PORT. We scan
# the small escalation window [PONDER_PORT, PONDER_PORT+PORT_ESCALATION_MAX] and kill each listener's PID
# individually. Still strictly port-scoped and PID-exact; never the cluster port or a broad match.
PORT_ESCALATION_MAX="${CHAOS_PORT_ESCALATION_MAX:-2}"
REAP_WAIT_MAX="${CHAOS_REAP_WAIT_MAX:-30}"   # seconds to poll /proc until every target is gone

# list pids whose /proc/<pid>/cwd resolves INTO $RUN_WORK (the per-run workdir). Prints pids, one/line.
# NEVER matches the driver (cwd=$HOME) — guard: empty/short RUN_WORK ⇒ no matches, ever.
workdir_pids () {
  local wd="${RUN_WORK:-}"
  # refuse to sweep on a missing/short/unsafe workdir (defence-in-depth: never sweep '/', $HOME, etc.)
  case "$wd" in
    ""|"/"|"$HOME"|"$HOME/") return 0 ;;
  esac
  [ "${#wd}" -lt 8 ] && return 0

  local d target self="$$"
  for d in /proc/[0-9]*; do
    local pid="${d#/proc/}"
    [ "$pid" = "$self" ] && continue

    target="$(readlink "$d/cwd" 2>/dev/null)" || continue
    # match the workdir itself or anything beneath it; strip a trailing " (deleted)" if the dir is gone.
    target="${target% (deleted)}"
    case "$target" in
      "$wd"|"$wd"/*) echo "$pid" ;;
    esac
  done
}

# list pids LISTENING on our app port window (secondary net). Prints pids, one/line. Never the driver.
port_pids () {
  local p pids sp
  for p in $(seq "$PONDER_PORT" "$(( PONDER_PORT + PORT_ESCALATION_MAX ))"); do
    pids="$(ss -H -tlnp "sport = :$p" 2>/dev/null | grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -u)"
    for sp in $pids; do
      echo "$sp"
    done
  done
}

# read a pid's real process-group id from /proc/<pid>/stat (field 5, pgrp). Prints '' if unreadable.
# comm (field 2) is parenthesised and may itself contain ') ', so strip up to the LAST ')' greedily.
pgid_of () {
  local pid="$1"
  local stat
  stat="$(cat "/proc/$pid/stat" 2>/dev/null)" || return 1
  local rest="${stat##*) }"   # rest = "<state> <ppid> <pgrp> ..."
  set -- $rest
  [ -n "${3:-}" ] && printf '%s' "$3"
}

# true iff a process group has ANY live member (scan /proc/*/stat field 5 == pgid).
pgid_alive () {
  local pgid="$1"
  [ -n "$pgid" ] || return 1

  local d p
  for d in /proc/[0-9]*; do
    p="${d#/proc/}"
    [ "$(pgid_of "$p")" = "$pgid" ] && return 0
  done

  return 1
}

# poll until PGID + workdir-cwd pids + port listeners are ALL gone; return 0 gone, 1 timed out (loud).
wait_gone () {
  local pgid="$1"
  local waited=0
  while [ "$waited" -lt "$REAP_WAIT_MAX" ]; do
    local alive=0
    { [ -n "$pgid" ] && kill -0 "$pgid" 2>/dev/null; } && alive=1
    [ "$alive" = 0 ] && pgid_alive "$pgid" && alive=1
    [ "$alive" = 0 ] && [ -n "$(workdir_pids)" ] && alive=1
    [ "$alive" = 0 ] && [ -n "$(port_pids)" ] && alive=1
    [ "$alive" = 0 ] && return 0

    sleep 1
    waited=$(( waited + 1 ))
  done

  return 1
}

# SIGKILL a whole set of pids AND each pid's REAL process group. setsid puts the app in its own group
# whose pgid == the app's OWN pid (NOT the $! the driver captured — $! is the wrapping subshell/setsid
# launcher, which is unrelated). So a correct group-kill must read each straggler's actual pgrp from
# /proc and `kill -9 -<pgrp>` that. This makes reaping depend on the WORKDIR detection (proven reliable),
# not on the fragile $!==pgid assumption the OLD driver leaned on.
kill_pids_and_groups () {
  local p g seen_groups=" "
  for p in "$@"; do
    [ -n "$p" ] || continue

    kill -9 "$p" 2>/dev/null
    g="$(pgid_of "$p")"
    # kill each distinct real group once; NEVER the driver's own group ($$) or its pgrp.
    if [ -n "$g" ] && [ "$g" != "$(pgid_of "$$")" ]; then
      case "$seen_groups" in
        *" $g "*) : ;;
        *) kill -9 -"$g" 2>/dev/null; seen_groups="$seen_groups$g " ;;
      esac
    fi
  done
}

reap_group () {
  local pgid="$1"
  # (1) best-effort fast path on the captured pgid ($!): harmless if it is the launcher, not the leader.
  [ -n "$pgid" ] && kill -9 -"$pgid" 2>/dev/null
  [ -n "$pgid" ] && kill -9 "$pgid" 2>/dev/null

  # (2) workdir-scoped straggler sweep (PRIMARY single-writer net): reap every pid whose cwd is the
  # per-run workdir AND its REAL process group (derived from /proc, not from $!). Guarded to never touch
  # the driver/$HOME. This is the layer that actually guarantees no rogue survivor.
  kill_pids_and_groups $(workdir_pids)

  # (3) legacy port-scoped net (SECONDARY) — PID-exact listeners on the app port window + their groups.
  kill_pids_and_groups $(port_pids)

  # WAIT until every target is gone; if it will not die, re-sweep once, then FAIL LOUD (the caller's
  # pg_stat_activity gate is the definitive backstop, but a reap that cannot complete is a driver bug).
  if ! wait_gone "$pgid"; then
    log "  ⚠ reap_group: targets still alive after ${REAP_WAIT_MAX}s (pgid=$pgid workdir-pids='$(workdir_pids | tr '\n' ' ')' port-pids='$(port_pids | tr '\n' ' ')') — re-sweeping"
    [ -n "$pgid" ] && kill -9 -"$pgid" 2>/dev/null
    kill_pids_and_groups $(workdir_pids)
    kill_pids_and_groups $(port_pids)
    if ! wait_gone "$pgid"; then
      log "  ✗ reap_group: FAILED to reap all targets after re-sweep — single-writer cannot be guaranteed by process reaping alone (pg gate will backstop)"
      return 1
    fi
  fi

  return 0
}

# ── install the app once into a fresh workspace (SAME steps as v3 / kill-loop.sh) ──────────────────
RUN_WORK=""
RUN_NPM_CACHE=""
install_app () {
  RUN_WORK="$(mktemp -d)"
  RUN_NPM_CACHE="$(mktemp -d)"
  cp -r "$APP/." "$RUN_WORK/"
  SQD_PONDER_TARBALL="$TARBALL" node -e "const p=require('$RUN_WORK/package.json');p.dependencies['@subsquid/ponder']='file:'+process.env.SQD_PONDER_TARBALL;require('fs').writeFileSync('$RUN_WORK/package.json',JSON.stringify(p,null,2))"
  ( cd "$RUN_WORK" && npm install --no-audit --no-fund --silent --cache "$RUN_NPM_CACHE" ) >/dev/null 2>&1 || return 1
  [ -x "$RUN_WORK/node_modules/.bin/ponder" ] || return 1

  return 0
}

cleanup_run_work () {
  [ -n "${KEEP_WORKSPACES:-}" ] && return
  [ -n "$RUN_WORK" ] && rm -rf "$RUN_WORK"
  [ -n "$RUN_NPM_CACHE" ] && rm -rf "$RUN_NPM_CACHE"
  RUN_WORK=""; RUN_NPM_CACHE=""
}

# fresh database per run: drop-if-exists then create. Returns 0/1.
make_run_db () {
  local db="$1"
  psql_ours "drop database if exists $db" >/dev/null 2>&1
  psql_ours "create database $db" >/dev/null 2>&1 || return 1

  return 0
}

drop_run_db () {
  local db="$1"
  psql_ours "drop database if exists $db" >/dev/null 2>&1
}

# ── THE DEFINITIVE SINGLE-WRITER GATE, at the DB boundary, before EVERY (re)launch ─────────────────
# Count live backends on the run DB other than our own probe connection:
#   SELECT count(*) FROM pg_stat_activity WHERE datname='<run db>' AND pid <> pg_backend_pid()
# If it is NOT 0, some ponder instance (a rogue survivor the reaper missed) is still connected to the
# store — the exact two-writer precondition that produced run-2's factory_addresses ×2. We
# pg_terminate_backend the stragglers, RECORD a `driver-invariant` event (per-run jsonl + a run-level
# counter folded into the run record), then RE-CHECK. The (re)launch does NOT proceed until the gate
# reads 0. If it cannot be satisfied after PG_GATE_MAX_TRIES, return 1 → caller freezes + STOPs (loud).
#
# Globals it reads/writes (set by run_one before each attempt): PG_GATE_DB, PG_GATE_URL,
# PG_GATE_EVENTS (jsonl path), PG_GATE_N (run number), PG_GATE_ATTEMPT (attempt number), and it bumps
# DRIVER_INVARIANTS (run-level count).
PG_GATE_MAX_TRIES="${CHAOS_PG_GATE_MAX_TRIES:-8}"
DRIVER_INVARIANTS=0

pg_activity_count () {
  # backends on $PG_GATE_DB other than THIS probe connection. Prints an integer (fail-closed: on any
  # query error prints a large sentinel so the gate never falsely passes).
  local n
  n="$(psql_ours "select count(*) from pg_stat_activity where datname='$PG_GATE_DB' and pid <> pg_backend_pid()" 2>/dev/null)"
  case "$n" in
    ''|*[!0-9]*) echo 999999 ;;
    *)           echo "$n" ;;
  esac
}

pg_activity_pids () {
  psql_ours "select string_agg(pid::text, ' ') from pg_stat_activity where datname='$PG_GATE_DB' and pid <> pg_backend_pid()" 2>/dev/null
}

record_driver_invariant () {
  local reason="$1" pids="$2" countBefore="$3"
  DRIVER_INVARIANTS=$(( DRIVER_INVARIANTS + 1 ))
  # jsonl event — mirrors the per-kill snapshot accounting style (one JSON object per line).
  if [ -n "${PG_GATE_EVENTS:-}" ]; then
    printf '{"event":"driver-invariant","run":%s,"attempt":%s,"ts":"%s","db":"%s","backendsBefore":%s,"terminatedPids":"%s","reason":"%s"}\n' \
      "${PG_GATE_N:-0}" "${PG_GATE_ATTEMPT:-0}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "$PG_GATE_DB" "$countBefore" "${pids:-}" "$reason" >> "$PG_GATE_EVENTS"
  fi
  log "  ⚠ driver-invariant #$DRIVER_INVARIANTS (run ${PG_GATE_N:-?} attempt ${PG_GATE_ATTEMPT:-?}): $reason — backends=$countBefore pids='${pids:-}' on $PG_GATE_DB; terminating + re-checking"
}

# returns 0 when the gate reads 0 (single writer guaranteed at the DB boundary); 1 if unsatisfiable.
pg_single_writer_gate () {
  local try=0
  while [ "$try" -lt "$PG_GATE_MAX_TRIES" ]; do
    local n
    n="$(pg_activity_count)"
    if [ "$n" = 0 ]; then
      return 0
    fi

    local pids
    pids="$(pg_activity_pids)"
    record_driver_invariant "pg_stat_activity gate found $n straggler backend(s) on $PG_GATE_DB before (re)launch (rogue survivor → two-writer risk)" "$pids" "$n"
    # terminate the stragglers at the DB boundary (does not touch the server, only these backends).
    psql_ours "select pg_terminate_backend(pid) from pg_stat_activity where datname='$PG_GATE_DB' and pid <> pg_backend_pid()" >/dev/null 2>&1
    sleep 1
    try=$(( try + 1 ))
  done

  # last check after the final terminate.
  [ "$(pg_activity_count)" = 0 ] && return 0

  return 1
}

# freeze a failed run's evidence and mark the campaign failed in aggregate.json. For the pg backend the
# "store" is a DB: we pg_dump it (schema+data of ponder_sync) as durable evidence rather than copying a
# datadir. The frozen DB is ALSO left in place (not dropped) so it can be inspected live.
freeze_and_fail () {
  local N="$1" MEAN="$2" kills="$3" attempts="$4" kl_exit="$5" vr_exit="$6" reason="$7"
  local RUN_DB="$8" RUN_LOG="$9" VR_LOG="${10}" SNAP_FILE="${11}" t0="${12}" partial="${13}" cfp="${14}" META="${15}"
  local frozen="$ART/frozen-run-$N"
  mkdir -p "$frozen"
  # dump the ponder_sync schema (logical evidence — physical datadir bytes are not meaningful on pg).
  if psql_ours "select 1 from pg_database where datname='$RUN_DB'" | grep -q 1; then
    PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PG_DUMP" -U postgres \
      --schema=ponder_sync --no-owner "$RUN_DB" > "$frozen/ponder_sync.sql" 2>/dev/null || true
    # also record row counts + intervals for a quick-look, and KEEP the live DB for inspection.
    psql_ours "select 'blocks='||count(*) from ponder_sync.blocks" "$RUN_DB" > "$frozen/rowcounts.txt" 2>/dev/null || true
    echo "frozen DB kept live as: $RUN_DB (drop manually after inspection)" >> "$frozen/rowcounts.txt"
  fi
  [ -f "$META" ] && cp "$META" "$frozen/store.meta.json" 2>/dev/null
  [ -f "$RUN_LOG" ] && cp "$RUN_LOG" "$frozen/" 2>/dev/null
  [ -f "$VR_LOG" ] && cp "$VR_LOG" "$frozen/" 2>/dev/null
  [ -f "$SNAP_FILE" ] && cp "$SNAP_FILE" "$frozen/" 2>/dev/null
  [ -f "$ART/run-$N.invariant.log" ] && cp "$ART/run-$N.invariant.log" "$frozen/" 2>/dev/null
  log "  frozen evidence → $frozen (DB $RUN_DB left live)"

  local t1
  t1="$(date +%s)"
  local wall=$(( t1 - t0 ))
  local vrfield="null"
  [ -n "$vr_exit" ] && vrfield="$vr_exit"
  agg_update "{\"appendRun\":{\"run\":$N,\"kills\":$kills,\"attempts\":$attempts,\"wallSec\":$wall,\"mean\":$MEAN,\"partialCoverageKills\":${partial:-0},\"completedFromPartial\":${cfp:-false},\"driverInvariants\":${DRIVER_INVARIANTS:-0},\"killLoopExit\":$kl_exit,\"verifyExit\":$vrfield,\"verdict\":\"fail\",\"reason\":\"$reason\",\"frozen\":\"$(basename "$frozen")\",\"frozenDb\":\"$RUN_DB\",\"at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" >/dev/null
  agg_update "{\"status\":\"fail\",\"finalVerdict\":\"FAIL\",\"failReason\":\"run $N: $reason\",\"finishedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
}

# ── one campaign run: kill+snapshot loop, then pg verify (digest + tiling) ────────────────────────
# args: run-number mean
# ── SYNCHRONOUS launcher: start the app, capture its PID, write the pidfile — ALL before returning ──
# The run-2 investigation blamed the OLD `( ... & echo $! > pidfile )` subshell for a pidfile race: the
# PID is captured/written in a SUBSHELL, and the parent then `cat`s the file back, so a mis-timed read
# (or a subshell/parent scheduling skew) could read an empty/stale pidfile and mis-track the instance.
# Here the app is backgrounded in the CURRENT shell; `$!` is the app's PID captured synchronously; the
# pidfile is written (temp+rename so a reader never sees a partial write) BEFORE this function returns.
# setsid makes the app its own process group; NOTE that $! here is the backgrounded subshell/launcher,
# NOT necessarily the app's own pgid — so LAUNCH_PGID is only a BEST-EFFORT fast-path for reap_group,
# which derives the REAL process group from /proc via the workdir sweep (the reliable single-writer net).
# Sets the globals LAUNCH_PID / LAUNCH_PGID and writes $PIDFILE. Returns 0 on a nonempty pidfile.
LAUNCH_PID=""
LAUNCH_PGID=""
launch_app () {
  local run_url="$1" run_log="$2" pidfile="$3"
  : > "$run_log"
  # background the app; $! is captured synchronously (no `( ... & echo $! > f )` subshell echo race).
  ( cd "$RUN_WORK" && \
    PONDER_START="$FROM" PONDER_END="$TO" CHAOS_PG_URL="$run_url" \
    PORTAL_URL_1="$PORTAL" PONDER_RPC_URL_1="$RPC" CHAIN_ID="$CHAIN_ID" EULER_FACTORY="$FACTORY" \
    PORTAL_CHECKS=strict PORTAL_GATE_LOG=1 PONDER_LOG_LEVEL="${PONDER_LOG_LEVEL:-info}" CI=true \
    PORTAL_CHUNK_BLOCKS="$T1_CHUNK_BLOCKS" PORTAL_CHUNK_FIXED="$T1_CHUNK_FIXED" PORTAL_READAHEAD="$T1_READAHEAD" \
    exec setsid ./node_modules/.bin/ponder start --schema chaos --port "$PONDER_PORT" >"$run_log" 2>&1 ) &
  LAUNCH_PID="$!"
  LAUNCH_PGID="$LAUNCH_PID"
  # write the pidfile SYNCHRONOUSLY (temp+rename) before return, so the caller never reads it empty.
  printf '%s\n' "$LAUNCH_PID" > "$pidfile.tmp" && mv -f "$pidfile.tmp" "$pidfile"
  [ -s "$pidfile" ] || return 1

  return 0
}

run_one () {
  local N="$1" MEAN="$2"
  local RUN_DB="chaos_run_$N"
  local RUN_URL="postgres://postgres@$PGHOST_TCP:$PGPORT/$RUN_DB"
  local META="$WORK_ROOT/run-$N.meta.json"
  local RUN_LOG="$ART/run-$N.app.log"
  local SNAP_FILE="$ART/run-$N.snapshots.jsonl"
  local VR_LOG="$ART/run-$N.verify.log"
  local INV_EVENTS="$ART/run-$N.invariant-events.jsonl"
  local PIDFILE="$WORK_ROOT/run-$N.pid"
  rm -f "$META" "$PIDFILE" "$PIDFILE.tmp"
  : > "$SNAP_FILE"
  : > "$INV_EVENTS"

  # per-run gate wiring (read by pg_single_writer_gate / record_driver_invariant).
  PG_GATE_DB="$RUN_DB"
  PG_GATE_URL="$RUN_URL"
  PG_GATE_EVENTS="$INV_EVENTS"
  PG_GATE_N="$N"
  PG_GATE_ATTEMPT=0
  DRIVER_INVARIANTS=0

  log "── RUN $N  range=[$FROM,$TO]  MEAN=${MEAN}s  MAX_KILLS=$MAX_KILLS  chunk=${T1_CHUNK_BLOCKS}(fixed=$T1_CHUNK_FIXED) readahead=$T1_READAHEAD  db=$RUN_DB"
  local t0
  t0="$(date +%s)"

  if ! make_run_db "$RUN_DB"; then
    log "✗ RUN $N: could not create fresh DB $RUN_DB — freezing"
    freeze_and_fail "$N" "$MEAN" 0 0 1 "" "createdb failed" "$RUN_DB" "$RUN_LOG" "$VR_LOG" "$SNAP_FILE" "$t0" 0 false "$META"

    return 1
  fi

  if ! install_app; then
    log "✗ RUN $N: app install failed — freezing"
    freeze_and_fail "$N" "$MEAN" 0 0 1 "" "app install failed" "$RUN_DB" "$RUN_LOG" "$VR_LOG" "$SNAP_FILE" "$t0" 0 false "$META"
    cleanup_run_work

    return 1
  fi

  local kills=0
  local attempt=0
  local DONE=0
  local partial_kills=0
  local invariant=0
  local store_recovery_failure=0          # store became UNQUERYABLE after an APP kill (see below)
  local last_pre_complete_class="empty"   # coverageClass just before the completing attempt
  local last_snap_json='{}'
  local gate_failed=0                      # pg_stat_activity gate could not be satisfied → loud STOP

  while [ "$DONE" = 0 ] && [ "$kills" -lt "$MAX_KILLS" ]; do
    attempt=$(( attempt + 1 ))
    PG_GATE_ATTEMPT="$attempt"

    # ── THE DEFINITIVE SINGLE-WRITER GATE, BEFORE THIS (re)launch ───────────────────────────────────
    # No new attempt may start while ANY other backend is connected to the run DB (a rogue survivor the
    # reaper missed). If the gate cannot be satisfied, the attempt does not start and does not count.
    if ! pg_single_writer_gate; then
      log "✗ RUN $N attempt $attempt: pg_stat_activity single-writer gate UNSATISFIABLE after $PG_GATE_MAX_TRIES tries — stragglers persist on $RUN_DB — freezing + STOP"
      attempt=$(( attempt - 1 ))   # this attempt never launched → do not count it
      gate_failed=1
      break
    fi

    # The store is Postgres (CHAOS_PG_URL) not a PGlite dir. The launcher writes the pidfile
    # SYNCHRONOUSLY and captures $! in-shell (no subshell echo race). setsid ⇒ new process group == PID.
    if ! launch_app "$RUN_URL" "$RUN_LOG" "$PIDFILE"; then
      log "✗ RUN $N attempt $attempt: launcher failed to write pidfile — freezing"
      invariant=0
      gate_failed=1
      break
    fi
    local PID="$LAUNCH_PID"
    local PGID="$LAUNCH_PGID"

    local SLEPT=0
    local T
    T="$(node -e "process.stdout.write(String(Math.max(1,Math.round(-$MEAN*Math.log(Math.random())))))")"
    while [ "$SLEPT" -lt "$T" ]; do
      grep -qiE 'Completed indexing across' "$RUN_LOG" 2>/dev/null && { DONE=1; break; }
      kill -0 "$PID" 2>/dev/null || break
      sleep 1
      SLEPT=$(( SLEPT + 1 ))
    done

    if grep -qiE 'InvariantViolation' "$RUN_LOG" 2>/dev/null; then
      invariant=1
      cp "$RUN_LOG" "$ART/run-$N.invariant.log" 2>/dev/null
      reap_group "$PGID"
      wait "$PID" 2>/dev/null
      break
    fi

    if [ "$DONE" = 1 ]; then
      reap_group "$PGID"
      wait "$PID" 2>/dev/null
      break
    fi

    if kill -0 "$PID" 2>/dev/null; then
      # ── SIGKILL the whole APP process group, then SNAPSHOT before resume (the V3 evidence) ───────
      # The Postgres SERVER is untouched — only the app dies. The store must stay queryable. reap_group
      # now WAITS until the group / workdir pids / port listeners are all gone before returning, so the
      # next iteration's gate starts from a clean process table.
      reap_group "$PGID"
      wait "$PID" 2>/dev/null
      kills=$(( kills + 1 ))

      local snap
      snap="$(snapshot_store "$RUN_URL")"
      last_snap_json="$snap"
      local cclass
      cclass="$(printf '%s' "$snap" | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{process.stdout.write(String(JSON.parse(s).coverageClass||'error'))}catch{process.stdout.write('error')}})")"
      printf '{"kill":%d,"attempt":%d,"ts":"%s","mean":%d,"poissonT":%d,"snap":%s}\n' \
        "$kills" "$attempt" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$MEAN" "$T" "$snap" >> "$SNAP_FILE"

      case "$cclass" in
        partial)  partial_kills=$(( partial_kills + 1 )); last_pre_complete_class="partial" ;;
        complete) last_pre_complete_class="complete" ;;
        empty)    last_pre_complete_class="empty" ;;
        # An UNQUERYABLE store (coverageClass=error) after an APP kill on a CRASH-DURABLE Postgres
        # backend should be IMPOSSIBLE — the server (fsync-on, WAL replay on the app's reconnect) is
        # never killed here. If it EVER fires, that is a MAJOR finding about durability under this fork's
        # write pattern, not a mislabeled resume bug: we freeze ALL evidence and STOP with a distinct
        # store-durability verdict (below). This is the honest analogue of the v3 torn-WAL stop, but on
        # a backend where it must not happen.
        *)        store_recovery_failure=1 ;;
      esac
    else
      # ── SELF-EXIT PATH: the app exited on its own (completion OR an early fatal). Either way we MUST
      # run the FULL reap before the next (re)launch — the OLD driver fell through here and left whatever
      # the exiting attempt spawned alive (a documented rogue-survivor source in the run-2 forensics).
      if grep -qiE 'Completed indexing across' "$RUN_LOG" 2>/dev/null; then
        DONE=1
      else
        log "  ⚠ RUN $N attempt $attempt: process exited early without completion — reaping before relaunch"
        tail -3 "$RUN_LOG" 2>/dev/null | sed 's/^/      /'
      fi
      reap_group "$PGID"
      wait "$PID" 2>/dev/null
    fi

    if [ "$store_recovery_failure" = 1 ]; then
      break
    fi
  done

  local t1
  t1="$(date +%s)"
  local wall=$(( t1 - t0 ))
  rm -f "$PIDFILE" "$PIDFILE.tmp"

  if [ "$DRIVER_INVARIANTS" -gt 0 ]; then
    log "  RUN $N: driver-invariant events this run = $DRIVER_INVARIANTS (stragglers terminated at the pg gate — see $(basename "$INV_EVENTS"))"
  fi

  # ── failure branches (loud stop) ──────────────────────────────────────────────────────────────
  if [ "$gate_failed" = 1 ]; then
    log "✗ RUN $N: single-writer gate could not be satisfied (or launcher failed) — freezing + STOP"
    freeze_and_fail "$N" "$MEAN" "$kills" "$attempt" 1 "" "single-writer pg_stat_activity gate unsatisfiable (rogue survivor persisted; driverInvariants=$DRIVER_INVARIANTS)" "$RUN_DB" "$RUN_LOG" "$VR_LOG" "$SNAP_FILE" "$t0" "$partial_kills" false "$META"
    agg_update "{\"finalVerdictClass\":\"driver-invariant\",\"finalVerdictNote\":\"The driver's DB-boundary single-writer gate (SELECT count(*) FROM pg_stat_activity WHERE datname=run-db AND pid<>pg_backend_pid()) could not be driven to 0 before a (re)launch: a rogue ponder backend survived reaping and pg_terminate_backend. Refusing to launch a second concurrent writer (the run-2 factory_addresses ×2 precondition). All evidence frozen.\"}" >/dev/null
    cleanup_run_work

    return 1
  fi
  if [ "$invariant" = 1 ]; then
    log "✗ RUN $N: InvariantViolation — freezing"
    freeze_and_fail "$N" "$MEAN" "$kills" "$attempt" 1 "" "InvariantViolation" "$RUN_DB" "$RUN_LOG" "$VR_LOG" "$SNAP_FILE" "$t0" "$partial_kills" false "$META"
    cleanup_run_work

    return 1
  fi
  if [ "$store_recovery_failure" = 1 ]; then
    log "✗ RUN $N: store became UNQUERYABLE after $kills APP-SIGKILL/resume cycles on a CRASH-DURABLE Postgres backend — this MUST NOT happen (the server is never killed). MAJOR finding. last snap: $last_snap_json — freezing + STOP"
    freeze_and_fail "$N" "$MEAN" "$kills" "$attempt" 1 "" "store recovery failure on crash-durable Postgres (MAJOR — server never killed, app-kill only)" "$RUN_DB" "$RUN_LOG" "$VR_LOG" "$SNAP_FILE" "$t0" "$partial_kills" false "$META"
    agg_update "{\"finalVerdictClass\":\"store-durability\",\"finalVerdictNote\":\"On a crash-durable native Postgres backend (fsync=on, full_page_writes=on), the store became unqueryable after repeated APP-only SIGKILL/resume. The Postgres SERVER was never killed, so WAL replay on the app's reconnect should always recover the store. If reproduced, this is a MAJOR durability finding about the fork's write pattern — NOT a PGlite artifact and NOT a mislabeled resume-logic bug. All evidence frozen.\"}" >/dev/null
    cleanup_run_work

    return 1
  fi

  # ── NEUTRAL: benign completion below the kill floor (calibration miss) ─────────────────────────
  if [ "$DONE" = 1 ] && [ "$kills" -lt "$MIN_KILLS" ]; then
    log "  RUN $N NEUTRAL (completed with kills=$kills < MIN_KILLS=$MIN_KILLS — calibration miss)"
    local outn
    outn="$(agg_update "{\"appendRun\":{\"run\":$N,\"kills\":$kills,\"attempts\":$attempt,\"wallSec\":$wall,\"mean\":$MEAN,\"partialCoverageKills\":$partial_kills,\"driverInvariants\":$DRIVER_INVARIANTS,\"killLoopExit\":1,\"verifyExit\":null,\"verdict\":\"neutral\",\"reason\":\"completed with kills < MIN_KILLS (calibration miss)\",\"appLog\":\"$(basename "$RUN_LOG")\",\"snapshots\":\"$(basename "$SNAP_FILE")\",\"at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}")"
    log "  totals now (kills completed attempts partialKills completionsFromPartial): $outn"
    drop_run_db "$RUN_DB"; rm -f "$META"
    cleanup_run_work
    LAST_KILLS="$kills"; LAST_WALL="$wall"; LAST_PARTIAL="$partial_kills"

    return 0
  fi

  # ── did not complete within MAX_KILLS → loud stop (livelock / mis-calibration) ─────────────────
  if [ "$DONE" != 1 ]; then
    log "✗ RUN $N: did not complete within MAX_KILLS=$MAX_KILLS (kills=$kills) — freezing"
    freeze_and_fail "$N" "$MEAN" "$kills" "$attempt" 1 "" "did not complete within MAX_KILLS=$MAX_KILLS" "$RUN_DB" "$RUN_LOG" "$VR_LOG" "$SNAP_FILE" "$t0" "$partial_kills" false "$META"
    cleanup_run_work

    return 1
  fi

  # ── completed with kills ≥ MIN_KILLS → write metadata (repo tool) then pg verify ────────────────
  log "  ops-loop OK: kills=$kills attempts=$attempt partialKills=$partial_kills lastPreCompleteClass=$last_pre_complete_class — verifying resume (pg)"

  CHAOS_META_APP="$APP" CHAOS_META_FROM="$FROM" CHAOS_META_TO="$TO" CHAOS_META_PORTAL="$PORTAL" \
  CHAOS_META_TARBALL="$TARBALL" CHAOS_META_CHAIN_ID="$CHAIN_ID" CHAOS_META_FACTORY="$FACTORY" \
  CHAOS_META_SCENARIO="chaos-t1-pg" CHAOS_META_KILLS="$kills" \
    node "$CHAOS_META_MJS" write "$META" >/dev/null 2>&1 || {
      log "✗ RUN $N: could not write chaos metadata — freezing"
      freeze_and_fail "$N" "$MEAN" "$kills" "$attempt" 1 "" "chaos-meta write failed" "$RUN_DB" "$RUN_LOG" "$VR_LOG" "$SNAP_FILE" "$t0" "$partial_kills" false "$META"
      cleanup_run_work

      return 1
    }

  BASELINE_URL="$BASELINE_URL" CHAOS_META="$META" BASELINE_META="$BASELINE_META" \
  PROBE_DIR="$PROBE_DIR" CHAOS_META_MJS="$CHAOS_META_MJS" \
  DIGEST_MJS="./pg-digest.mjs" CHECK_INTERVALS_MJS="./check-intervals-pg.mjs" \
  MIN_KILLS="$MIN_KILLS" \
    bash "$VERIFY_PG" "$RUN_URL" "$FROM" "$TO" >"$VR_LOG" 2>&1
  local vr_exit=$?

  local t2
  t2="$(date +%s)"
  wall=$(( t2 - t0 ))

  if [ "$vr_exit" -ne 0 ]; then
    log "✗ RUN $N verify FAILED (exit=$vr_exit) — freezing"
    freeze_and_fail "$N" "$MEAN" "$kills" "$attempt" 0 "$vr_exit" "verify-resume-pg FAIL" "$RUN_DB" "$RUN_LOG" "$VR_LOG" "$SNAP_FILE" "$t0" "$partial_kills" false "$META"
    cleanup_run_work

    return 1
  fi

  local completed_from_partial="false"
  [ "$last_pre_complete_class" = "partial" ] && completed_from_partial="true"

  log "  ✓ RUN $N PASS (kills=$kills attempts=$attempt partialKills=$partial_kills wall=${wall}s completedFromPartial=$completed_from_partial) — dropping DB"
  local out
  out="$(agg_update "{\"appendRun\":{\"run\":$N,\"kills\":$kills,\"attempts\":$attempt,\"wallSec\":$wall,\"mean\":$MEAN,\"partialCoverageKills\":$partial_kills,\"completedFromPartial\":$completed_from_partial,\"lastPreCompleteClass\":\"$last_pre_complete_class\",\"driverInvariants\":$DRIVER_INVARIANTS,\"killLoopExit\":0,\"verifyExit\":$vr_exit,\"verdict\":\"pass\",\"appLog\":\"$(basename "$RUN_LOG")\",\"snapshots\":\"$(basename "$SNAP_FILE")\",\"verifyLog\":\"$(basename "$VR_LOG")\",\"at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}")"
  log "  totals now (kills completed attempts partialKills completionsFromPartial): $out"
  drop_run_db "$RUN_DB"; rm -f "$META"
  cleanup_run_work
  LAST_KILLS="$kills"; LAST_WALL="$wall"; LAST_PARTIAL="$partial_kills"

  return 0
}

# ── campaign loop ───────────────────────────────────────────────────────────────────────────────
campaign () {
  preflight

  local START
  START="$(date +%s)"
  local NOW
  NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  local resume_state maxRun hasStartedAt
  resume_state="$(AGG_FILE="$AGG" node - <<'NODE'
const fs = require('node:fs');
let agg = {};
try { agg = JSON.parse(fs.readFileSync(process.env.AGG_FILE, 'utf8')); } catch {}
const runs = Array.isArray(agg.runs) ? agg.runs : [];
let maxRun = 0;
for (const r of runs) {
  const n = Number(r.run);
  if (Number.isFinite(n) && n > maxRun) maxRun = n;
}
const hasStartedAt = agg.startedAt ? 1 : 0;
process.stdout.write(`${maxRun} ${hasStartedAt}`);
NODE
)"
  maxRun="${resume_state%% *}"
  hasStartedAt="$(printf '%s' "$resume_state" | awk '{print $2}')"
  [ -z "$maxRun" ] && maxRun=0
  [ -z "$hasStartedAt" ] && hasStartedAt=0

  local N=0
  local MEAN="$MEAN_RUN1"
  local CALIBRATED=0
  LAST_KILLS=0
  LAST_WALL=0
  LAST_PARTIAL=0

  local baseline_digest
  baseline_digest="$(node -e "try{const m=require('$BASELINE_META');process.stdout.write(String(m.digest&&m.digest.store||''))}catch{process.stdout.write('')}")"

  # Derive the backend label from OBSERVED state (issue #60): the cluster is up (preflight ensured it),
  # so this reads the live server. A rejected CHAOS_BACKEND_LABEL override aborts the campaign here,
  # BEFORE any metadata is written — never a mislabeled record.
  local backend_label
  backend_label="$(derive_backend_label)" || { log "✗ backend label derivation failed — ABORT"; exit 2; }
  log "✓ backend label (observed): $backend_label"

  local params_json
  params_json="{\"from\":$FROM,\"to\":$TO,\"maxKills\":$MAX_KILLS,\"minKills\":$MIN_KILLS,\"trigger\":\"poisson\",\"targetKillsPerRun\":$TARGET_KILLS,\"meanFloorSec\":$MEAN_FLOOR,\"meanCeilSec\":$MEAN_CEIL,\"backend\":\"$backend_label\",\"pgPort\":$PGPORT,\"storeIdentity\":\"logical-digest\",\"baselineDigest\":\"$baseline_digest\",\"tier1\":{\"chunkBlocks\":$T1_CHUNK_BLOCKS,\"chunkFixed\":$T1_CHUNK_FIXED,\"readahead\":$T1_READAHEAD}}"
  local accept_json
  accept_json="{\"kills\":$ACCEPT_KILLS,\"runs\":$ACCEPT_RUNS,\"partialCoverageKills\":$ACCEPT_PARTIAL_KILLS,\"completionsFromPartial\":$ACCEPT_COMPLETIONS_FROM_PARTIAL,\"maxRuns\":$MAX_RUNS,\"maxWallSec\":$MAX_WALL_SEC}"

  if [ "$maxRun" -gt 0 ]; then
    N="$maxRun"
    CALIBRATED=1
    local rm_out
    rm_out="$(rolling_mean "$TARGET_KILLS")"
    MEAN="${rm_out%% *}"
    log "▶ RESUMING campaign from aggregate: maxRun=$maxRun → next run $((maxRun+1)); seeded MEAN=${MEAN}s (rolling over existing runs). Prior terminal record left in runs[] as history."
    local startedPatch=""
    [ "$hasStartedAt" -eq 0 ] && startedPatch="\"startedAt\":\"$NOW\","
    agg_update "{${startedPatch}\"resumedAt\":\"$NOW\",\"status\":\"running\",\"finalVerdict\":null,\"failReason\":null,\"finishedAt\":null,\"driver\":\"pg\",\"harnessCommit\":\"$(git -C "$CDIR" rev-parse HEAD 2>/dev/null || echo unknown)\",\"tarball\":\"$(basename "$TARBALL")\",\"tarballSha256\":\"$TARBALL_SHA\",\"params\":$params_json,\"acceptance\":$accept_json}" >/dev/null
  else
    agg_update "{\"status\":\"running\",\"driver\":\"pg\",\"startedAt\":\"$NOW\",\"harnessCommit\":\"$(git -C "$CDIR" rev-parse HEAD 2>/dev/null || echo unknown)\",\"tarball\":\"$(basename "$TARBALL")\",\"tarballSha256\":\"$TARBALL_SHA\",\"params\":$params_json,\"acceptance\":$accept_json}" >/dev/null
  fi

  while : ; do
    N=$(( N + 1 ))

    if [ "$N" -gt "$MAX_RUNS" ]; then
      log "✗ hit MAX_RUNS=$MAX_RUNS without acceptance — INCOMPLETE"
      agg_update "{\"status\":\"incomplete\",\"finalVerdict\":\"INCOMPLETE\",\"failReason\":\"exceeded MAX_RUNS=$MAX_RUNS\",\"finishedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
      exit 3
    fi
    local elapsed=$(( $(date +%s) - START ))
    if [ "$elapsed" -ge "$MAX_WALL_SEC" ]; then
      log "✗ hit MAX_WALL_SEC=$MAX_WALL_SEC (${elapsed}s) without acceptance — INCOMPLETE"
      agg_update "{\"status\":\"incomplete\",\"finalVerdict\":\"INCOMPLETE\",\"failReason\":\"exceeded MAX_WALL_SEC=$MAX_WALL_SEC\",\"finishedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
      exit 3
    fi

    if ! run_one "$N" "$MEAN"; then
      log "✗ CAMPAIGN STOPPED at run $N (failure frozen)"
      exit 1
    fi

    if [ "$CALIBRATED" -eq 0 ]; then
      local run1mean
      run1mean="$(pick_mean "$LAST_KILLS" "$LAST_WALL" "$TARGET_KILLS")"
      agg_update "{\"calibration\":{\"run1Kills\":$LAST_KILLS,\"run1WallSec\":$LAST_WALL,\"run1PartialKills\":$LAST_PARTIAL,\"targetKillsPerRun\":$TARGET_KILLS,\"chosenMean\":$run1mean,\"meanFloorSec\":$MEAN_FLOOR,\"meanCeilSec\":$MEAN_CEIL,\"reasoning\":\"MEAN=clamp(floor(wall/target),floor,ceil) to land kills mid-staircase; range fixed so the single baseline stays reusable via metadata match\"}}" >/dev/null
      CALIBRATED=1
    fi

    local rm_out newmean minwall basis
    rm_out="$(rolling_mean "$TARGET_KILLS")"
    newmean="${rm_out%% *}"
    minwall="$(printf '%s' "$rm_out" | awk '{print $2}')"
    basis="$(printf '%s' "$rm_out" | awk '{print $3}')"
    if [ -n "$newmean" ] && [ "$newmean" != "$MEAN" ]; then
      log "  recalibration: minRecentWall=${minwall}s (last $basis runs) target=$TARGET_KILLS → MEAN ${MEAN}s→${newmean}s"
      MEAN="$newmean"
    fi
    agg_update "{\"calibration\":{\"rollingMean\":$newmean,\"rollingBasis\":{\"minRecentWallSec\":$minwall,\"runsConsidered\":$basis,\"targetKillsPerRun\":$TARGET_KILLS}}}" >/dev/null

    local tk tc tp tcfp
    tk="$(agg_get totals.kills)"
    tc="$(agg_get totals.completedVerified)"
    tp="$(agg_get totals.killsAtPartialCoverage)"
    tcfp="$(agg_get totals.completionsFromPartial)"
    log "  progress: kills=$tk/$ACCEPT_KILLS  completedVerified=$tc/$ACCEPT_RUNS  partialKills=$tp/$ACCEPT_PARTIAL_KILLS  completionsFromPartial=$tcfp/$ACCEPT_COMPLETIONS_FROM_PARTIAL"
    if [ "${tk:-0}" -ge "$ACCEPT_KILLS" ] && [ "${tc:-0}" -ge "$ACCEPT_RUNS" ] \
       && [ "${tp:-0}" -ge "$ACCEPT_PARTIAL_KILLS" ] && [ "${tcfp:-0}" -ge "$ACCEPT_COMPLETIONS_FROM_PARTIAL" ]; then
      log "🎉 ACCEPTANCE MET: kills=$tk completedVerified=$tc partialKills=$tp completionsFromPartial=$tcfp"
      agg_update "{\"status\":\"pass\",\"finalVerdict\":\"PASS\",\"finishedAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" >/dev/null
      exit 0
    fi
  done
}

# backend-label: derive + print the backend label from observed state (issue #60), applying the same
# CHAOS_BACKEND_LABEL override validation the campaign uses. Prints the label on success (exit 0);
# aborts nonzero on a rejected override or when the major cannot be observed. A real entrypoint for
# debugging and for the mutation-verified label test — it exercises the PRODUCTION helper directly.
backend_label_cmd () {
  local label
  label="$(derive_backend_label)" || exit 2
  echo "$label"
}

case "${1:-campaign}" in
  selftest) selftest ;;
  campaign) campaign ;;
  backend-label) backend_label_cmd ;;
  *) echo "usage: chaos-pg-driver.sh [campaign|selftest|backend-label]"; exit 2 ;;
esac
