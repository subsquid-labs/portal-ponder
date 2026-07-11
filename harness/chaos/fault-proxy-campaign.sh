#!/usr/bin/env bash
# G4 fault-proxy chaos campaign. Reuses the Postgres chaos driver helpers in library mode, drives the
# local proxy scenarios, and records evidence under harness/chaos/.faultproxy.
set -uo pipefail

CDIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$CDIR/../.." && pwd)"

DEFAULT_TARBALL="/tmp/p1-pack/subsquid-ponder-0.16.8-sqd.1.tgz"
DEFAULT_TARBALL_SHA="b5a0daa6f8439a67afd36d715451f040a9addc1e7d55f7043400092ac3869b70"

export SQD_PONDER_TARBALL="${SQD_PONDER_TARBALL:-$DEFAULT_TARBALL}"
export CHAOS_TARBALL_SHA="${CHAOS_TARBALL_SHA:-$DEFAULT_TARBALL_SHA}"
export CHAOS_WORK="${CHAOS_WORK:-$CDIR/.faultproxy}"
export CHAOS_ART="${CHAOS_ART:-$CHAOS_WORK/artifacts}"
export CHAOS_BASELINE_DBNAME="${CHAOS_BASELINE_DBNAME:-p1_faultproxy_baseline}"
export CHAOS_BASELINE_META="${CHAOS_BASELINE_META:-$CHAOS_ART/baseline.meta.json}"
export CHAOS_APP="${CHAOS_APP:-$ROOT/harness/diff/euler-app}"
export CHAOS_PGPORT="${CHAOS_PGPORT:-54329}"
export PORTAL_MIN_CONCURRENCY="${PORTAL_MIN_CONCURRENCY:-1}"
export PORTAL_START_CONCURRENCY="${PORTAL_START_CONCURRENCY:-4}"
export PORTAL_MAX_CONCURRENCY="${PORTAL_MAX_CONCURRENCY:-32}"
export PORTAL_REQUEST_TIMEOUT="${PORTAL_REQUEST_TIMEOUT:-30000}"

CHAOS_LIB_ONLY=1
export CHAOS_LIB_ONLY
source "$CDIR/chaos-pg-driver.sh"

SCENARIOS_DIR="$CDIR/scenarios/faultproxy"
PORTAL_UPSTREAM="${FAULT_PROXY_UPSTREAM:-https://portal.sqd.dev/datasets/ethereum-mainnet}"
CHAOS_PROXY_PORT="${FAULT_PROXY_PORT:-8700}"
PROXY_URL="http://127.0.0.1:$CHAOS_PROXY_PORT"
META_PORTAL="$PORTAL_UPSTREAM"
BASELINE_DBNAME="$CHAOS_BASELINE_DBNAME"
BASELINE_URL="postgres://postgres@$PGHOST_TCP:$PGPORT/$BASELINE_DBNAME"
BASELINE_META="$CHAOS_BASELINE_META"
AGG="$ART/aggregate.json"
RESULT_FILE="${CHAOS_RESULT_FILE:-$ART/RESULT.md}"
CX_SUMMARY="${CHAOS_CX_SUMMARY:-$ART/cx-summary.md}"
BACKEND_LABEL=""
PROXY_PID=""
BASELINE_WALL_SEC=600
FAILED_DB=""
FAILED_REASON=""

SCENARIO_ORDER=(
  429-burst
  5xx-storm
  retry-after
  tcp-reset-mid-ndjson
  90s-stall
  truncated-gzip
  malformed-ndjson
  spurious-204
  head-freeze
  head-regression-100k
  head-flap
)

log () { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

assert_db_name () {
  case "$1" in
    p1_faultproxy_[a-z0-9_]*)
      case "$1" in *[!a-z0-9_]*|"p1_faultproxy_") return 1 ;; esac
      return 0
      ;;
  esac
  return 1
}

db_for_scenario () {
  local name="$1"
  local db="p1_faultproxy_${name//-/_}"
  assert_db_name "$db" || { log "fatal: unsafe DB name derived from scenario '$name': $db"; exit 2; }
  echo "$db"
}

scenario_path () {
  local name="$1"
  local path="$SCENARIOS_DIR/$name.json"
  [ -f "$path" ] || { log "fatal: scenario not found: $name"; exit 2; }
  echo "$path"
}

completed () {
  grep -qiE 'Completed indexing across' "$1" 2>/dev/null
}

loud_failure_line () {
  grep -iE 'Portal(Error|ConfigError)|TypedError|FatalError|InvariantViolation|ERR_[A-Z0-9_]+|SyntaxError|Z_DATA_ERROR|incorrect header check|unexpected end|Unexpected token|BodyTimeoutError|RequestTimeout|Error:' "$1" 2>/dev/null | head -1
}

live_fatal_line () {
  grep -iE 'Portal(Error|ConfigError)|TypedError|FatalError|InvariantViolation|ERR_[A-Z0-9_]+|Uncaught|panic' "$1" 2>/dev/null | head -1
}

write_meta () {
  local meta="$1" scenario="$2"
  CHAOS_META_APP="$APP" CHAOS_META_FROM="$FROM" CHAOS_META_TO="$TO" \
  CHAOS_META_PORTAL="$META_PORTAL" CHAOS_META_TARBALL="$TARBALL" \
  CHAOS_META_CHAIN_ID="$CHAIN_ID" CHAOS_META_FACTORY="$FACTORY" \
  CHAOS_META_SCENARIO="$scenario" CHAOS_META_KILLS=0 \
    node "$CHAOS_META_MJS" write "$meta" >/dev/null
}

adapt_run_work_for_postgres () {
  CONFIG_FILE="$RUN_WORK/ponder.config.ts" node - <<'NODE'
const fs = require('node:fs');
const file = process.env.CONFIG_FILE;
let src = fs.readFileSync(file, 'utf8');
if (src.includes('CHAOS_PG_URL')) process.exit(0);
const re =
  /database:\s*\{\s*kind:\s*['"]pglite['"],\s*directory:\s*process\.env\.PGLITE_DIR\s*\?\?\s*['"]\.\/\.ponder\/pglite['"],?\s*\},/s;
if (!re.test(src)) {
  console.error(`could not locate the pglite database block in ${file}`);
  process.exit(2);
}
src = src.replace(
  re,
  `database: process.env.CHAOS_PG_URL
    ? { kind: 'postgres', connectionString: process.env.CHAOS_PG_URL }
    : {
        kind: 'pglite',
        directory: process.env.PGLITE_DIR ?? './.ponder/pglite',
      },`,
);
fs.writeFileSync(file, src);
NODE
}

install_faultproxy_app () {
  install_app || return 1
  cp "$CDIR/chaos-aimd-trace.cjs" "$RUN_WORK/chaos-aimd-trace.cjs" || return 1
  adapt_run_work_for_postgres || return 1
  return 0
}

preflight_faultproxy () {
  mkdir -p "$ART/runs"
  [ "$PGPORT" = "54329" ] || { log "fatal: this campaign only runs on PG port 54329"; exit 2; }
  [ "$CHAOS_PROXY_PORT" = "8700" ] || { log "fatal: proxy port must be 8700"; exit 2; }
  [ "$PORTAL_UPSTREAM" = "https://portal.sqd.dev/datasets/ethereum-mainnet" ] || {
    log "fatal: upstream must be the SQD Portal ethereum-mainnet dataset"
    exit 2
  }
  [ -f "$TARBALL" ] || { log "fatal: tarball not found: $TARBALL"; exit 2; }
  [ -d "$APP" ] || { log "fatal: app dir not found: $APP"; exit 2; }
  [ -d "$SCENARIOS_DIR" ] || { log "fatal: scenario dir missing: $SCENARIOS_DIR"; exit 2; }

  local got
  got="$(sha256sum "$TARBALL" | awk '{print $1}')"
  if [ "$got" != "$CHAOS_TARBALL_SHA" ]; then
    log "fatal: tarball sha256 mismatch: got=$got want=$CHAOS_TARBALL_SHA"
    exit 2
  fi
  log "tarball sha256 pinned+verified ($got)"

  bash "$PGCTL" ensure || { log "fatal: could not ensure throwaway Postgres"; exit 2; }
  [ "$(psql_ours 'show fsync')" = "on" ] || { log "fatal: Postgres fsync is not on"; exit 2; }
  BACKEND_LABEL="$(derive_backend_label)" || { log "fatal: backend label derivation failed"; exit 2; }
  ensure_probe_dir || { log "fatal: could not build probe workspace"; exit 2; }

  AGG_FILE="$AGG" APP_CHOSEN="$APP" BACKEND_LABEL="$BACKEND_LABEL" TARBALL_SHA="$got" \
  FROM="$FROM" TO="$TO" FACTORY="$FACTORY" PORTAL="$META_PORTAL" node - <<'NODE'
const fs = require('node:fs');
const file = process.env.AGG_FILE;
const agg = {
  campaign: 'fault-proxy',
  status: 'running',
  startedAt: new Date().toISOString(),
  params: {
    app: process.env.APP_CHOSEN,
    appRationale:
      'Same app family used by the accepted kill-loop leg; the dense range and factory are the documented non-empty kill-loop window.',
    from: Number(process.env.FROM),
    to: Number(process.env.TO),
    factory: process.env.FACTORY,
    portal: process.env.PORTAL,
    backend: process.env.BACKEND_LABEL,
    tarballSha256: process.env.TARBALL_SHA,
    aimdSource:
      'portalGate log-line parse from @subsquid/ponder plus PORTAL_METRICS_FILE final snapshots; fetch-derived in-flight samples are fallback evidence.',
  },
  runs: [],
};
fs.mkdirSync(require('node:path').dirname(file), { recursive: true });
fs.writeFileSync(file, JSON.stringify(agg, null, 2) + '\n');
NODE
}

start_proxy () {
  log "starting fault proxy on $PROXY_URL"
  : > "$ART/proxy.log"
  PORTAL_UPSTREAM="$PORTAL_UPSTREAM" CHAOS_PORT="$CHAOS_PROXY_PORT" \
    node "$CDIR/proxy.mjs" >"$ART/proxy.log" 2>&1 &
  PROXY_PID="$!"
  local i=0
  while [ "$i" -lt 60 ]; do
    curl -sf "$PROXY_URL/__scenario" >/dev/null 2>&1 && return 0
    sleep 0.5
    i=$(( i + 1 ))
  done
  log "fatal: proxy did not become healthy"
  tail -20 "$ART/proxy.log" 2>/dev/null
  exit 2
}

set_proxy_passthrough () {
  curl -sfX POST "$PROXY_URL/__scenario" \
    -H 'content-type: application/json' \
    -d '{"head":{"mode":"passthrough"},"faults":{}}' >/dev/null
  curl -sfX POST "$PROXY_URL/__reset" >/dev/null
}

wait_for_outcome () {
  local pid="$1" log_file="$2" cap="$3"
  OUTCOME_DONE=0
  OUTCOME_LOUD=0
  OUTCOME_LINE=""
  local elapsed=0
  while [ "$elapsed" -lt "$cap" ]; do
    if completed "$log_file"; then OUTCOME_DONE=1; return 0; fi
    OUTCOME_LINE="$(live_fatal_line "$log_file")"
    if [ -n "$OUTCOME_LINE" ]; then OUTCOME_LOUD=1; return 0; fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 5
    elapsed=$(( elapsed + 5 ))
  done
  if completed "$log_file"; then OUTCOME_DONE=1; return 0; fi
  if kill -0 "$pid" 2>/dev/null; then return 0; fi
  OUTCOME_LINE="$(loud_failure_line "$log_file")"
  if [ -n "$OUTCOME_LINE" ]; then OUTCOME_LOUD=1; return 0; fi
  return 0
}

append_metrics_snapshot () {
  local run_dir="$1" trace_file="$2"
  RUN_DIR="$run_dir" TRACE_FILE="$trace_file" node - <<'NODE'
const fs = require('node:fs');
const path = require('node:path');
const dir = process.env.RUN_DIR;
const trace = process.env.TRACE_FILE;
if (!fs.existsSync(dir)) process.exit(0);
for (const name of fs.readdirSync(dir)) {
  if (!name.startsWith('portal-metrics.')) continue;
  try {
    const metric = JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8'));
    const gate = metric.portalGate;
    if (!gate) continue;
    fs.appendFileSync(
      trace,
      `${JSON.stringify({
        ts: new Date().toISOString(),
        event: 'aimd-metrics-snapshot',
        source: 'PORTAL_METRICS_FILE',
        chainId: metric.chainId ?? null,
        concurrency: Number(gate.limit),
        limit: Number(gate.limit),
        active: Number(gate.active),
        rows: Number(gate.rows),
      })}\n`,
    );
  } catch {}
}
NODE
}

aimd_summary () {
  local trace_file="$1" required="$2"
  TRACE_FILE="$trace_file" REQUIRED="$required" \
  AIMD_MIN="${PORTAL_MIN_CONCURRENCY:-1}" AIMD_MAX="${PORTAL_MAX_CONCURRENCY:-32}" node - <<'NODE'
const fs = require('node:fs');
const file = process.env.TRACE_FILE;
const required = process.env.REQUIRED === '1';
const minBound = Number(process.env.AIMD_MIN || 1);
const maxBound = Number(process.env.AIMD_MAX || 32);
let rows = [];
try {
  rows = fs
    .readFileSync(file, 'utf8')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
} catch {}
const gate = rows.filter((r) => Number.isFinite(Number(r.limit)));
const derived = rows.filter(
  (r) =>
    r.source === 'fetch-derived' &&
    r.event !== 'aimd-fetch-release' &&
    Number.isFinite(Number(r.inFlight)) &&
    Number(r.inFlight) > 0,
);
const sourceRows = gate.length ? gate : derived;
if (!sourceRows.length) {
  process.stdout.write(
    JSON.stringify({
      ok: !required,
      source: 'none',
      sampleCount: 0,
      min: null,
      max: null,
      final: null,
      recovered: !required,
      note: required ? 'no AIMD samples captured' : 'not applicable before stream admission',
    }),
  );
  process.exit(0);
}
const values = sourceRows.map((r) =>
  gate.length ? Number(r.limit) : Number(r.inFlight),
);
const min = Math.min(...values);
const max = Math.max(...values);
const final = values[values.length - 1];
const within = min >= (gate.length ? minBound : 1) && max <= maxBound;
// Fewer than 3 samples cannot demonstrate multiplicative-decrease-then-recovery.
// Report this honestly as `sparse` and do NOT let it assert `recovered` — a single
// concurrency reading is not evidence of AIMD recovery (committee finding #3).
const sparse = sourceRows.length < 3;
const recovered =
  !sparse &&
  (final > (gate.length ? minBound : 0) || max > (gate.length ? minBound : 1));
// AIMD is SECONDARY evidence: the correctness gate is digest-identity + interval tiling
// (spec fix #2). Sparse/insufficient AIMD must NOT fail a scenario whose digest+intervals
// pass — it is simply labelled "sparse — insufficient samples", and never claims
// "recovered". The one AIMD condition that still fails a scenario is a genuine bounds
// violation (`within=false`: an observed concurrency outside [min,max]) — a real anomaly,
// not mere sparseness. `required` only affects the no-samples-at-all branch above.
const ok = within && (recovered || sparse || !required);
process.stdout.write(
  JSON.stringify({
    ok,
    source: gate.length ? 'portalGate' : 'fetch-derived',
    sampleCount: sourceRows.length,
    min,
    max,
    final,
    recovered,
    within,
    sparse,
    note: sparse
      ? 'sparse — insufficient samples to assert AIMD recovery'
      : recovered
        ? 'AIMD recovery observed'
        : 'no AIMD recovery observed',
  }),
);
NODE
}

fault_summary () {
  local run_dir="$1"
  SCENARIO_FILE="$run_dir/scenario.applied.json" STATS_FILE="$run_dir/proxy-stats.json" node - <<'NODE'
const fs = require('node:fs');
const scen = JSON.parse(fs.readFileSync(process.env.SCENARIO_FILE, 'utf8'));
const stats = JSON.parse(fs.readFileSync(process.env.STATS_FILE, 'utf8'));
const f = scen.faults || {};
const map = {
  p429: 'r429',
  p5xx: 'r5xx',
  p204: 'r204',
  pTruncatedGzip: 'gzip',
  pReset: 'reset',
  pStall: 'stall',
  pMalformedNdjson: 'ndjson',
};
const expected = [];
const failures = [];
for (const [prob, counter] of Object.entries(map)) {
  if (Number(f[prob] || 0) > 0) {
    expected.push(counter);
    if (!(Number(stats[counter] || 0) > 0)) {
      failures.push(`${counter}=0`);
    }
  }
}
if (Number(f.pReset || 0) > 0 && Number(stats.missedReset || 0) !== 0) {
  failures.push(`missedReset=${stats.missedReset}`);
}
if (
  Number(f.pMalformedNdjson || 0) > 0 &&
  Number(stats.missedNdjson || 0) !== 0
) {
  failures.push(`missedNdjson=${stats.missedNdjson}`);
}
process.stdout.write(
  JSON.stringify({
    ok: failures.length === 0,
    expected,
    failures,
    counts: {
      requests: stats.requests || 0,
      r429: stats.r429 || 0,
      r5xx: stats.r5xx || 0,
      reset: stats.reset || 0,
      stall: stats.stall || 0,
      gzip: stats.gzip || 0,
      ndjson: stats.ndjson || 0,
      r204: stats.r204 || 0,
      missedReset: stats.missedReset || 0,
      missedNdjson: stats.missedNdjson || 0,
    },
  }),
);
NODE
}

append_verdict () {
  local verdict_file="$1"
  VERDICT_FILE="$verdict_file" AGG_FILE="$AGG" node - <<'NODE'
const fs = require('node:fs');
const verdict = JSON.parse(fs.readFileSync(process.env.VERDICT_FILE, 'utf8'));
let agg;
try {
  agg = JSON.parse(fs.readFileSync(process.env.AGG_FILE, 'utf8'));
} catch {
  agg = { campaign: 'fault-proxy', params: {}, runs: [] };
}
agg.runs = Array.isArray(agg.runs)
  ? agg.runs.filter((r) => r.scenario !== verdict.scenario)
  : [];
agg.runs.push(verdict);
agg.updatedAt = new Date().toISOString();
// A not-applicable verdict (pass:false, notApplicable:true) is vacuous, not a failure —
// it must not flip the campaign to `fail`. Only a genuine pass:false failure does.
agg.status =
  verdict.pass === false && !verdict.notApplicable
    ? 'fail'
    : agg.status || 'running';
fs.writeFileSync(process.env.AGG_FILE, JSON.stringify(agg, null, 2) + '\n');
NODE
}

write_verdict () {
  local run_dir="$1" scenario="$2" outcome="$3" pass="$4" verify_exit="$5" digest="$6" baseline_digest="$7" fault_json="$8" aimd_json="$9" loud_line="${10}" note="${11}"
  VERDICT_FILE="$run_dir/verdict.json" SCENARIO="$scenario" OUTCOME="$outcome" PASS="$pass" \
  VERIFY_EXIT="$verify_exit" DIGEST="$digest" BASELINE_DIGEST="$baseline_digest" \
  FAULT_JSON="$fault_json" AIMD_JSON="$aimd_json" LOUD_LINE="$loud_line" NOTE="$note" node - <<'NODE'
const fs = require('node:fs');
const verdict = {
  scenario: process.env.SCENARIO,
  pass: process.env.PASS === 'true',
  // A not-applicable scenario is neither a pass nor a fail: it is vacuous for this
  // fixed-range historical harness (committee finding #1/#2). It must not flip the
  // campaign status to `fail`, nor be counted as a head-tolerance PASS.
  notApplicable: process.env.OUTCOME === 'not-applicable',
  outcome: process.env.OUTCOME,
  verifyExit:
    process.env.VERIFY_EXIT === '' ? null : Number(process.env.VERIFY_EXIT),
  digest: process.env.DIGEST || 'n/a',
  baselineDigest: process.env.BASELINE_DIGEST || 'n/a',
  digestEqual:
    process.env.DIGEST !== '' &&
    process.env.BASELINE_DIGEST !== '' &&
    process.env.DIGEST === process.env.BASELINE_DIGEST,
  fault: JSON.parse(process.env.FAULT_JSON),
  aimd: JSON.parse(process.env.AIMD_JSON),
  loudLine: process.env.LOUD_LINE || '',
  note: process.env.NOTE || '',
  at: new Date().toISOString(),
};
fs.writeFileSync(process.env.VERDICT_FILE, JSON.stringify(verdict, null, 2) + '\n');
NODE
  append_verdict "$run_dir/verdict.json"
}

freeze_and_fail_faultproxy () {
  local scenario="$1" db="$2" run_dir="$3" reason="$4"
  local frozen="$ART/frozen-$scenario"
  mkdir -p "$frozen"
  if psql_ours "select 1 from pg_database where datname='$db'" | grep -q 1; then
    PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PG_DUMP" -U postgres \
      --schema=ponder_sync --no-owner "$db" >"$frozen/ponder_sync.sql" 2>/dev/null || true
    echo "$db" > "$frozen/live-db.txt"
  fi
  cp -R "$run_dir/." "$frozen/" 2>/dev/null || true
  printf '%s\n' "$reason" > "$frozen/reason.txt"
  FAILED_DB="$db"
  FAILED_REASON="$reason"
  log "frozen evidence for $scenario -> $frozen (DB kept live as $db)"
}

run_verify () {
  local url="$1" meta="$2" log_file="$3"
  BASELINE_URL="$BASELINE_URL" CHAOS_META="$meta" BASELINE_META="$BASELINE_META" \
  PROBE_DIR="$PROBE_DIR" CHAOS_META_MJS="$CHAOS_META_MJS" \
  DIGEST_MJS="./pg-digest.mjs" CHECK_INTERVALS_MJS="./check-intervals-pg.mjs" \
  MIN_KILLS=0 \
    bash "$VERIFY_PG" "$url" "$FROM" "$TO" >"$log_file" 2>&1
}

build_baseline () {
  log "building clean baseline DB $BASELINE_DBNAME"
  set_proxy_passthrough || { log "fatal: could not set proxy passthrough"; exit 2; }
  assert_db_name "$BASELINE_DBNAME" || { log "fatal: unsafe baseline DB name $BASELINE_DBNAME"; exit 2; }
  make_run_db "$BASELINE_DBNAME" || { log "fatal: could not create baseline DB"; exit 2; }

  local t0 t1 pid pgid pidfile="$WORK_ROOT/baseline.pid"
  t0="$(date +%s)"
  install_faultproxy_app || { log "fatal: baseline app install failed"; exit 2; }
  DENSE_TRACE_FILE="$ART/baseline.dense-trace.jsonl"
  export CHAOS_AIMD_TRACE_FILE="$ART/baseline.aimd-trace.jsonl"
  export AIMD_TRACE_FILE="$ART/baseline.aimd-trace.jsonl"
  export PORTAL_METRICS_FILE="$ART/baseline.portal-metrics"
  local save_node_options="${NODE_OPTIONS:-}"
  NODE_OPTIONS="--require $RUN_WORK/chaos-aimd-trace.cjs${save_node_options:+ $save_node_options}"
  PORTAL="$PROXY_URL"
  launch_app "$BASELINE_URL" "$ART/baseline.app.log" "$pidfile" || { log "fatal: baseline launch failed"; exit 2; }
  pid="$LAUNCH_PID"; pgid="$LAUNCH_PGID"
  wait_for_outcome "$pid" "$ART/baseline.app.log" 1800
  reap_group "$pgid" >/dev/null 2>&1
  wait "$pid" 2>/dev/null
  NODE_OPTIONS="$save_node_options"
  append_metrics_snapshot "$ART" "$ART/baseline.aimd-trace.jsonl"
  cleanup_run_work
  if [ "$OUTCOME_DONE" != 1 ]; then
    log "fatal: baseline did not complete"
    tail -20 "$ART/baseline.app.log" 2>/dev/null
    exit 1
  fi
  t1="$(date +%s)"
  BASELINE_WALL_SEC=$(( t1 - t0 ))
  [ "$BASELINE_WALL_SEC" -lt 300 ] && BASELINE_WALL_SEC=300
  write_meta "$BASELINE_META" "baseline-clean"
  ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$BASELINE_URL" ) > "$ART/baseline.digest"
  ( cd "$PROBE_DIR" && node ./check-intervals-pg.mjs "$BASELINE_URL" "$FROM" "$TO" ) > "$ART/baseline.intervals.log" 2>&1 || {
    log "fatal: baseline intervals did not tile"
    exit 1
  }
  run_verify "$BASELINE_URL" "$BASELINE_META" "$ART/baseline.verify.log" || {
    log "fatal: baseline self-verify failed"
    tail -40 "$ART/baseline.verify.log" 2>/dev/null
    exit 1
  }
  log "baseline verified; digest=$(cat "$ART/baseline.digest") wall=${BASELINE_WALL_SEC}s"
}

head_recovery_check () {
  local scenario="$1" url="$2" db="$3" run_dir="$4"
  log "head recovery for $scenario: passthrough resume on same DB"
  set_proxy_passthrough || return 1
  install_faultproxy_app || return 1
  DENSE_TRACE_FILE="$run_dir/recovery-dense-trace.jsonl"
  export CHAOS_AIMD_TRACE_FILE="$run_dir/aimd-trace.jsonl"
  export AIMD_TRACE_FILE="$run_dir/aimd-trace.jsonl"
  export PORTAL_METRICS_FILE="$run_dir/recovery-portal-metrics"
  local save_node_options="${NODE_OPTIONS:-}"
  NODE_OPTIONS="--require $RUN_WORK/chaos-aimd-trace.cjs${save_node_options:+ $save_node_options}"
  local pidfile="$WORK_ROOT/recovery-$scenario.pid"
  launch_app "$url" "$run_dir/recovery.log" "$pidfile" || return 1
  local pid="$LAUNCH_PID" pgid="$LAUNCH_PGID"
  wait_for_outcome "$pid" "$run_dir/recovery.log" "$(( BASELINE_WALL_SEC * 2 ))"
  reap_group "$pgid" >/dev/null 2>&1
  wait "$pid" 2>/dev/null
  NODE_OPTIONS="$save_node_options"
  append_metrics_snapshot "$run_dir" "$run_dir/aimd-trace.jsonl"
  cleanup_run_work
  [ "$OUTCOME_DONE" = 1 ] || return 1
  ( cd "$PROBE_DIR" && node ./check-intervals-pg.mjs "$url" "$FROM" "$TO" ) \
    > "$run_dir/intervals-recovered.log" 2>&1
}

run_scenario () {
  local scenario="$1"
  local scen_file
  scen_file="$(scenario_path "$scenario")"
  local db
  db="$(db_for_scenario "$scenario")"
  local url="postgres://postgres@$PGHOST_TCP:$PGPORT/$db"
  local run_dir="$ART/runs/$scenario"
  local meta="$run_dir/store.meta.json"
  local verify_log="$run_dir/verify.log"
  local run_log="$run_dir/app.log"
  local pidfile="$WORK_ROOT/run-$scenario.pid"
  mkdir -p "$run_dir"
  rm -f "$run_dir"/* "$pidfile" "$pidfile.tmp" 2>/dev/null || true
  log "scenario $scenario -> DB $db"

  make_run_db "$db" || { log "fatal: could not create DB $db"; exit 2; }
  curl -sfX POST "$PROXY_URL/__scenario" \
    -H 'content-type: application/json' \
    --data-binary "@$scen_file" > "$run_dir/scenario.applied.json" || {
      log "fatal: could not apply scenario $scenario"
      exit 2
    }
  curl -sfX POST "$PROXY_URL/__reset" >/dev/null || { log "fatal: could not reset proxy stats"; exit 2; }

  install_faultproxy_app || {
    freeze_and_fail_faultproxy "$scenario" "$db" "$run_dir" "app install failed"
    return 1
  }
  DENSE_TRACE_FILE="$run_dir/dense-trace.jsonl"
  export CHAOS_AIMD_TRACE_FILE="$run_dir/aimd-trace.jsonl"
  export AIMD_TRACE_FILE="$run_dir/aimd-trace.jsonl"
  export PORTAL_METRICS_FILE="$run_dir/portal-metrics"
  local save_node_options="${NODE_OPTIONS:-}"
  NODE_OPTIONS="--require $RUN_WORK/chaos-aimd-trace.cjs${save_node_options:+ $save_node_options}"
  PORTAL="$PROXY_URL"
  launch_app "$url" "$run_log" "$pidfile" || {
    freeze_and_fail_faultproxy "$scenario" "$db" "$run_dir" "launch failed"
    return 1
  }
  local pid="$LAUNCH_PID" pgid="$LAUNCH_PGID"
  local scenario_wall_cap=$(( BASELINE_WALL_SEC * 2 ))
  case "$scenario" in
    head-freeze|head-flap|head-regression-100k) scenario_wall_cap=120 ;;
  esac
  wait_for_outcome "$pid" "$run_log" "$scenario_wall_cap"
  local done="$OUTCOME_DONE" loud="$OUTCOME_LOUD" loud_line="$OUTCOME_LINE"
  reap_group "$pgid" >/dev/null 2>&1
  wait "$pid" 2>/dev/null
  NODE_OPTIONS="$save_node_options"
  append_metrics_snapshot "$run_dir" "$run_dir/aimd-trace.jsonl"
  cleanup_run_work

  curl -sf "$PROXY_URL/__stats" > "$run_dir/proxy-stats.json" || {
    log "fatal: could not capture proxy stats"
    exit 2
  }

  local fault_json
  fault_json="$(fault_summary "$run_dir")"

  # committee finding #1/#2 (unanimous): head-regression-100k and head-flap manipulate the head far
  # ABOVE the pinned endBlock (live head ~21M, regressed/flapped head still > endBlock 20579207), so
  # the manipulated head never intersects the fixed range [20529207,20579207]. A fixed-range HISTORICAL
  # backfill is structurally near-immune to a head that stays >= endBlock — it only ever waits for the
  # head to advance and never rolls back an already-finalized+indexed block. These scenarios therefore
  # prove nothing the clean baseline did not. Rather than emit a bogus head-tolerance PASS, mark them
  # NOT-APPLICABLE for this harness. Head-reorg/rollback tolerance is a STREAM-mode property, exercised
  # by the stream-mode soak, not by this fixed-range historical harness. head-freeze (frozen head BELOW
  # endBlock) remains the one meaningful in-range head scenario. See RESULT §head-scenarios.
  case "$scenario" in
    head-regression-100k|head-flap)
      local na_aimd na_note na_digest
      na_aimd="$(aimd_summary "$run_dir/aimd-trace.jsonl" 0)"
      na_note="not-applicable: manipulated head stays >= endBlock $TO, so it never intersects the fixed range [$FROM,$TO]; a fixed-range historical backfill cannot exercise head-reorg/rollback (a STREAM-mode property covered by the stream-mode soak). Not counted as a head-tolerance PASS."
      na_digest=""
      if [ "$done" = 1 ]; then
        ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$url" ) > "$run_dir/digest" 2>"$run_dir/digest.err" || true
        na_digest="$(cat "$run_dir/digest" 2>/dev/null || true)"
      fi
      write_verdict "$run_dir" "$scenario" "not-applicable" "false" "" "$na_digest" \
        "$(cat "$ART/baseline.digest" 2>/dev/null || true)" "$fault_json" "$na_aimd" "$loud_line" "$na_note"
      drop_run_db "$db"
      rm -f "$meta" "$pidfile" "$pidfile.tmp"
      log "scenario $scenario NOT-APPLICABLE (head stays >= endBlock; stream-mode property)"

      return 0
      ;;
  esac

  # committee finding #4 (elif false-pass): only run head recovery when phase 1 did NOT complete.
  # head-freeze is the sole in-range head scenario: its frozen head (BELOW endBlock) stalls the Portal
  # (finalized head behind the requested range) so phase 1 makes little/no progress; recovery then
  # resumes on the SAME DB with the fault removed. If head_recovery_check FAILS we must propagate that
  # failure (leave done=0), never retain a stale pass. If phase 1 unexpectedly completes despite the
  # freeze, no recovery is run and the outcome is a plain completion, NOT byte-identical-recovery.
  local recovery=0
  if [ "$scenario" = "head-freeze" ] && [ "$done" != 1 ] && [ "$loud" != 1 ]; then
    recovery=1
    if head_recovery_check "$scenario" "$url" "$db" "$run_dir"; then
      done=1
      loud=0
      loud_line=""
    else
      loud_line="$(loud_failure_line "$run_dir/recovery.log")"
      [ -n "$loud_line" ] && loud=1
    fi
  fi

  local aimd_required=1
  if [ "$loud" = 1 ] && [ ! -s "$run_dir/dense-trace.jsonl" ]; then aimd_required=0; fi
  local aimd_json
  aimd_json="$(aimd_summary "$run_dir/aimd-trace.jsonl" "$aimd_required")"

  local baseline_digest digest verify_exit outcome pass note
  baseline_digest="$(cat "$ART/baseline.digest" 2>/dev/null || true)"
  digest=""
  verify_exit=""
  pass=false
  note=""
  if [ "$done" = 1 ]; then
    write_meta "$meta" "$scenario"
    ( cd "$PROBE_DIR" && node ./pg-digest.mjs "$url" ) > "$run_dir/digest" 2>"$run_dir/digest.err"
    digest="$(cat "$run_dir/digest" 2>/dev/null || true)"
    if run_verify "$url" "$meta" "$verify_log"; then
      verify_exit=0
    else
      verify_exit=$?
    fi
    if [ "$verify_exit" = 0 ] && node -e "const f=$fault_json,a=$aimd_json; process.exit(f.ok&&a.ok?0:1)"; then
      pass=true
      if [ "$recovery" = 1 ]; then
        outcome="byte-identical-recovery"
        # committee finding #5 (candor): head-freeze phase 1 stalls (frozen head behind the requested
        # range) with little/no progress; "recovery" is a clean restart on the SAME DB with the fault
        # removed, which then completes byte-identical. State honestly what was proven — this is a
        # stall-without-corruption + clean same-DB restart, NOT a partial-progress resume claim.
        note="head-freeze stalled without corruption; clean restart on the same DB (fault removed) completed byte-identical"
      else
        outcome="byte-identical-completion"
      fi
    else
      outcome="silent-corruption-or-verifier-failure"
      note="completed but digest/interval/fault/AIMD acceptance failed"
      write_verdict "$run_dir" "$scenario" "$outcome" "$pass" "$verify_exit" "$digest" "$baseline_digest" "$fault_json" "$aimd_json" "$loud_line" "$note"
      freeze_and_fail_faultproxy "$scenario" "$db" "$run_dir" "$note"
      return 1
    fi
  elif [ "$loud" = 1 ]; then
    outcome="loud-typed-failure"
    if node -e "const f=$fault_json,a=$aimd_json; process.exit(f.ok&&a.ok?0:1)"; then
      pass=true
    else
      note="loud failure occurred, but fault-fired or AIMD acceptance failed"
      write_verdict "$run_dir" "$scenario" "$outcome" "$pass" "$verify_exit" "$digest" "$baseline_digest" "$fault_json" "$aimd_json" "$loud_line" "$note"
      freeze_and_fail_faultproxy "$scenario" "$db" "$run_dir" "$note"
      return 1
    fi
  else
    outcome="silent-timeout-or-exit"
    note="neither byte-identical completion nor loud typed failure"
    write_verdict "$run_dir" "$scenario" "$outcome" "$pass" "$verify_exit" "$digest" "$baseline_digest" "$fault_json" "$aimd_json" "$loud_line" "$note"
    freeze_and_fail_faultproxy "$scenario" "$db" "$run_dir" "$note"
    return 1
  fi

  write_verdict "$run_dir" "$scenario" "$outcome" "$pass" "$verify_exit" "$digest" "$baseline_digest" "$fault_json" "$aimd_json" "$loud_line" "$note"
  drop_run_db "$db"
  rm -f "$meta" "$pidfile" "$pidfile.tmp"
  log "scenario $scenario PASS ($outcome)"
}

write_aggregate_summary () {
  AGG_FILE="$AGG" SUMMARY_FILE="$ART/summary.md" RESULT_FILE="$RESULT_FILE" CX_SUMMARY="$CX_SUMMARY" node - <<'NODE'
const fs = require('node:fs');
const path = require('node:path');
const agg = JSON.parse(fs.readFileSync(process.env.AGG_FILE, 'utf8'));
const runs = Array.isArray(agg.runs) ? agg.runs : [];
const order = [
  '429-burst',
  '5xx-storm',
  'retry-after',
  'tcp-reset-mid-ndjson',
  '90s-stall',
  'truncated-gzip',
  'malformed-ndjson',
  'spurious-204',
  'head-freeze',
  'head-regression-100k',
  'head-flap',
];
const byName = new Map(runs.map((r) => [r.scenario, r]));
const rows = order.map((name, i) => {
  const r = byName.get(name);
  if (!r) return `| ${i + 1} | ${name} | not-run | n/a | n/a | n/a | n/a | n/a | pending | |`;
  const c = r.fault?.counts || {};
  const expected = r.fault?.expected?.length
    ? r.fault.expected.map((k) => `${k}=${c[k] ?? 0}`).join(', ')
    : 'head-only';
  return `| ${i + 1} | ${name} | ${r.outcome} | ${expected} | ${c.missedReset ?? 'n/a'} | ${c.missedNdjson ?? 'n/a'} | ${r.digestEqual ? 'yes' : r.digest === 'n/a' ? 'n/a' : 'no'} | ${r.verifyExit === 0 ? 'yes' : 'n/a'} | ${r.aimd?.ok ? 'yes' : 'no'} (${r.aimd?.source ?? 'none'} ${r.aimd?.min ?? 'n/a'}/${r.aimd?.max ?? 'n/a'}/${r.aimd?.final ?? 'n/a'}) | ${r.note || r.loudLine || ''} |`;
});
// A not-applicable scenario is vacuous for this fixed-range historical harness — count it
// separately, never as a pass and never as a fail (committee finding #1/#2).
const notApplicable = runs.filter((r) => r.notApplicable).length;
const passed = runs.filter((r) => r.pass && !r.notApplicable).length;
const failed = runs.filter((r) => r.pass === false && !r.notApplicable).length;
agg.status =
  failed > 0 ? 'fail' : runs.length === order.length ? 'pass' : 'incomplete';
agg.finishedAt = new Date().toISOString();
fs.writeFileSync(process.env.AGG_FILE, JSON.stringify(agg, null, 2) + '\n');
const table = [
  '| # | Scenario | Outcome | Fault fired | missedReset | missedNdjson | Digest == baseline | Intervals tile | AIMD | Notes |',
  '|---|---|---|---|---|---|---|---|---|---|',
  ...rows,
].join('\n');
const truncated = byName.get('truncated-gzip')?.outcome || 'pending';
const malformed = byName.get('malformed-ndjson')?.outcome || 'pending';
const body = `# P1 Fault-Proxy Campaign Result

Chosen app: ${agg.params?.app || 'unknown'}

Rationale: ${agg.params?.appRationale || ''}

Range/factory: [${agg.params?.from}, ${agg.params?.to}], ${agg.params?.factory}

Tarball SHA: ${agg.params?.tarballSha256}

AIMD source decision: ${agg.params?.aimdSource}

Outcome probe decisions:
- truncated-gzip: ${truncated}
- malformed-ndjson: ${malformed}

Verdicts: ${passed} pass, ${failed} fail, ${notApplicable} not-applicable, ${runs.length}/${order.length} scenarios recorded.

${table}

Head-scenario scope: head-freeze (frozen head BELOW endBlock ${agg.params?.to}) is the one meaningful in-range head scenario — its frozen head stalls the Portal, and a clean restart on the same DB with the fault removed completes byte-identical. head-regression-100k and head-flap manipulate the head ABOVE endBlock, so it never intersects the fixed range [${agg.params?.from}, ${agg.params?.to}]; a fixed-range historical backfill never rolls back an already-finalized+indexed block, so these are marked not-applicable (NOT a head-tolerance PASS). Head-reorg/rollback tolerance is a stream-mode property, exercised by the stream-mode soak, not this historical harness.

Artifacts: ${path.dirname(process.env.SUMMARY_FILE)}
`;
fs.writeFileSync(process.env.SUMMARY_FILE, body);
fs.writeFileSync(process.env.RESULT_FILE, body);
fs.writeFileSync(process.env.CX_SUMMARY, body);
NODE
}

teardown () {
  local rc=$?
  if [ -n "${PROXY_PID:-}" ]; then
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
    PROXY_PID=""
  fi
  if psql_ours "select 1 from pg_database where datname='$BASELINE_DBNAME'" 2>/dev/null | grep -q 1; then
    drop_run_db "$BASELINE_DBNAME" >/dev/null 2>&1 || true
  fi
  cleanup_run_work >/dev/null 2>&1 || true
  return "$rc"
}

campaign_main () {
  trap teardown EXIT INT TERM
  preflight_faultproxy
  start_proxy
  build_baseline
  local scenario
  for scenario in "${SCENARIO_ORDER[@]}"; do
    run_scenario "$scenario" || {
      write_aggregate_summary
      return 1
    }
  done
  write_aggregate_summary
}

baseline_only_main () {
  trap teardown EXIT INT TERM
  preflight_faultproxy
  start_proxy
  build_baseline
  write_aggregate_summary
}

one_scenario_main () {
  local scenario="${1:?usage: fault-proxy-campaign.sh one-scenario <name>}"
  trap teardown EXIT INT TERM
  preflight_faultproxy
  start_proxy
  build_baseline
  run_scenario "$scenario"
  write_aggregate_summary
}

case "${1:-campaign}" in
  campaign) campaign_main ;;
  baseline-only) baseline_only_main ;;
  one-scenario) shift; one_scenario_main "$@" ;;
  *) echo "usage: fault-proxy-campaign.sh [campaign|baseline-only|one-scenario <name>]"; exit 2 ;;
esac
