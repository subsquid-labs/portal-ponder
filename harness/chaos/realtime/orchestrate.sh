#!/usr/bin/env bash
# orchestrate.sh - realtime chaos K1-K6 orchestrator.
#
# Defaults target the acceptance bar: >=200 kills total and >=25 per class.
# For a cheap proof run:
#   CHAOS_KILLS_PER_CLASS=1 harness/chaos/realtime/orchestrate.sh
set -uo pipefail

CDIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$CDIR/../../.." && pwd)"

PONDER_VERSION="${PONDER_VERSION:-0.16.9}"
FROM="${PONDER_START:-100}"
TO="${PONDER_END:-112}"
PONDER_SCHEMA="${PONDER_SCHEMA:-realtime-chaos}"
STORE_SCHEMA="${STORE_SCHEMA:-ponder_sync}"
APP_SCHEMA="${APP_SCHEMA:-$PONDER_SCHEMA}"
MOCK_PORT="${MOCK_PORT:-8701}"
MOCK_URL="http://127.0.0.1:$MOCK_PORT"
PONDER_PORT="${PONDER_PORT:-44301}"
FACTORY="${EULER_FACTORY:-0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e}"
PGPORT="${CHAOS_PGPORT:-54331}"
WORK_ROOT="${CHAOS_WORK:-$CDIR/.chaos-pg}"
PGSOCK="${CHAOS_PGSOCK:-$WORK_ROOT/pgsock}"
PSQL="${CHAOS_PSQL:-psql}"
CREATEDB="${CHAOS_CREATEDB:-createdb}"
DROPDB="${CHAOS_DROPDB:-dropdb}"
PGCTL="$ROOT/harness/chaos/pg-ctl-chaos.sh"
VERIFY="$CDIR/verify-resume.sh"
BASE_DIR="$CDIR/baselines"
RUN_ROOT="${CHAOS_RUN_ROOT:-$CDIR/run/orchestrate-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
SUMMARY_FILE="$CDIR/orchestrate.summary.json"
RESULT_FILE="$CDIR/RESULT.md"
RECORDS="$RUN_ROOT/run-records.ndjson"

MIN_PER_CLASS="${CHAOS_MIN_PER_CLASS:-25}"
TOTAL_TARGET="${CHAOS_TOTAL_KILLS:-}"
KILLS_PER_CLASS="${CHAOS_KILLS_PER_CLASS:-}"

mkdir -p "$CDIR/work"
WORK="$(mktemp -d "$CDIR/work/orchestrate.XXXXXX")"
NPM_CACHE="$(mktemp -d "$CDIR/work/npm-cache.XXXXXX")"
MOCK_PID=""
APP_PID=""
APP_PGID=""
PG_NODE_MODULES_LINK_CREATED=0

log () { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" >&2; }

cleanup () {
  if [ -n "$APP_PGID" ]; then kill -9 "-$APP_PGID" 2>/dev/null || true; fi
  if [ -n "$APP_PID" ]; then wait "$APP_PID" 2>/dev/null || true; fi
  if [ -n "$MOCK_PID" ]; then kill "$MOCK_PID" 2>/dev/null || true; fi
  if [ -n "$MOCK_PID" ]; then wait "$MOCK_PID" 2>/dev/null || true; fi
  if [ "$PG_NODE_MODULES_LINK_CREATED" = 1 ]; then rm -f "$ROOT/harness/chaos/node_modules"; fi
  if [ -z "${KEEP_WORKSPACES:-}" ]; then rm -rf "$WORK" "$NPM_CACHE"; fi
}
trap cleanup EXIT INT TERM

mkdir -p "$BASE_DIR" "$RUN_ROOT"
: > "$RECORDS"

curl_json () {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [ -n "$body" ]; then
    curl -fsS -X "$method" -H 'content-type: application/json' --data "$body" "$MOCK_URL$path"
  else
    curl -fsS -X "$method" "$MOCK_URL$path"
  fi
}

wait_http () {
  local i=0
  while [ "$i" -lt 200 ]; do
    curl -fsS "$MOCK_URL/__phase" >/dev/null 2>&1 && return 0
    sleep 0.05
    i=$(( i + 1 ))
  done
  return 1
}

phase_name () {
  node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);console.log(j.name||j.phase||'')})"
}

phase_blocked () {
  node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);console.log(j.blocked?'1':'0')})"
}

wait_phase () {
  local target="$1"
  local i=0
  while [ "$i" -lt 1200 ]; do
    local phase
    phase="$(curl_json GET /__phase 2>/dev/null || true)"
    if [ -n "$phase" ]; then
      local name blocked
      name="$(printf '%s' "$phase" | phase_name)"
      blocked="$(printf '%s' "$phase" | phase_blocked)"
      if [ "$name" = "$target" ] && [ "$blocked" = "1" ]; then
        return 0
      fi
    fi
    sleep 0.05
    i=$(( i + 1 ))
  done
  return 1
}

completed () { grep -qiE 'Completed indexing across all chains' "$1" 2>/dev/null; }
invariant () { grep -qiE 'InvariantViolation|restart to re-sync|deterministic, not retried|Cannot finalize safely|Cannot reconcile safely' "$1" 2>/dev/null; }

wait_complete () {
  local pid="$1"
  local log_file="$2"
  local i=0
  while [ "$i" -lt 1800 ]; do
    if invariant "$log_file"; then
      log "fatal: realtime invariant/fork fatal in $log_file"
      tail -100 "$log_file" || true
      return 2
    fi
    if completed "$log_file"; then return 0; fi
    if ! kill -0 "$pid" 2>/dev/null; then
      log "fatal: app exited before completion; tail follows"
      tail -100 "$log_file" || true
      return 1
    fi
    sleep 0.5
    i=$(( i + 1 ))
  done
  log "fatal: timeout waiting for completion; tail follows"
  tail -100 "$log_file" || true
  return 1
}

ensure_graft () {
  if [ -n "${PONDER_CORE_DIR:-}" ]; then
    echo "$PONDER_CORE_DIR"
    return 0
  fi

  local graft="$WORK/graft"
  log "building @subsquid/ponder graft $PONDER_VERSION under $graft"
  ( cd "$ROOT" && SYNC_WORKDIR="$graft" scripts/sync-upstream.sh "$PONDER_VERSION" ) \
    >"$RUN_ROOT/sync-upstream.log" 2>&1 || {
      log "fatal: sync-upstream failed; tail follows"
      tail -100 "$RUN_ROOT/sync-upstream.log" || true
      return 1
    }
  echo "$graft/$PONDER_VERSION/packages/core"
}

prepare_app () {
  local core="$1"
  local app="$WORK/euler-app"
  cp -R "$ROOT/harness/diff/euler-app" "$app"
  cp "$ROOT/harness/diff/euler-app/euler-app.stream.ts" "$app/ponder.config.ts"
  cp "$ROOT/harness/chaos/realtime/euler-app-stream/ponder.schema.ts" "$app/ponder.schema.ts"
  cp -R "$ROOT/harness/chaos/realtime/euler-app-stream/src/." "$app/src/"
  APP_DIR="$app" CORE_DIR="$core" node - <<'NODE'
const fs = require('node:fs');
const path = require('node:path');
const file = path.join(process.env.APP_DIR, 'package.json');
const pkg = JSON.parse(fs.readFileSync(file, 'utf8'));
pkg.dependencies['@subsquid/ponder'] = `file:${process.env.CORE_DIR}`;
pkg.dependencies.pg = pkg.dependencies.pg ?? '8.13.1';
fs.writeFileSync(file, `${JSON.stringify(pkg, null, 2)}\n`);
NODE
  log "installing app workspace"
  ( cd "$app" && npm install --no-audit --no-fund --silent --cache "$NPM_CACHE" ) \
    >"$RUN_ROOT/npm-install.log" 2>&1 || {
      log "fatal: npm install failed; tail follows"
      tail -100 "$RUN_ROOT/npm-install.log" || true
      return 1
    }
  echo "$app"
}

ensure_pg_resolution () {
  local app="$1"
  if [ -e "$ROOT/harness/chaos/node_modules" ]; then return 0; fi

  ln -s "$app/node_modules" "$ROOT/harness/chaos/node_modules" || return 1
  PG_NODE_MODULES_LINK_CREATED=1
}

psql_ours () {
  PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PSQL" -U postgres -Atqc "$1" "${2:-postgres}"
}

drop_create_db () {
  local db="$1"
  PGHOST="$PGSOCK" PGPORT="$PGPORT" "$DROPDB" -U postgres --if-exists "$db" >/dev/null 2>&1 || return 1
  PGHOST="$PGSOCK" PGPORT="$PGPORT" "$CREATEDB" -U postgres "$db" >/dev/null 2>&1 || return 1
}

start_mock () {
  local scenario="$1"
  local phase_log="$2"
  local mock_log="$3"
  local killat_block="${4:-}"
  MOCK_PHASE_LOG="$phase_log" MOCK_SCENARIO="$scenario" MOCK_PORT="$MOCK_PORT" \
  MOCK_KILLAT_BLOCK="$killat_block" \
    node "$CDIR/mock-portal.mjs" >"$mock_log" 2>&1 &
  MOCK_PID="$!"
  wait_http || {
    log "fatal: mock did not start; tail follows"
    tail -100 "$mock_log" || true
    return 1
  }
}

stop_mock () {
  if [ -n "$MOCK_PID" ]; then
    kill "$MOCK_PID" 2>/dev/null || true
    wait "$MOCK_PID" 2>/dev/null || true
  fi
  MOCK_PID=""
}

start_app () {
  local app="$1"
  local url="$2"
  local log_file="$3"
  : > "$log_file"
  (
    cd "$app" && \
    PONDER_START="$FROM" PONDER_END="$TO" \
    CHAOS_PG_URL="$url" DATABASE_URL="$url" \
    PORTAL_URL_1="$MOCK_URL" PONDER_RPC_URL_1=mock CHAOS_MOCK_RPC_URL="$MOCK_URL/rpc" \
    CHAIN_ID=1 EULER_FACTORY="$FACTORY" \
    CHAOS_SKIP_METADATA=1 \
    PORTAL_REALTIME=stream PORTAL_CHECKS=strict PORTAL_GATE_LOG=1 PONDER_LOG_LEVEL=trace CI=true \
    PORTAL_CHUNK_FIXED=1 PORTAL_CHUNK_BLOCKS=10 PORTAL_WARMUP_BLOCKS=10 PORTAL_READAHEAD=0 \
    exec setsid ./node_modules/.bin/ponder start --schema "$PONDER_SCHEMA" --port "$PONDER_PORT" \
      >"$log_file" 2>&1
  ) &
  APP_PID="$!"
  APP_PGID="$APP_PID"
}

kill_app () {
  if [ -n "$APP_PGID" ]; then kill -9 "-$APP_PGID" 2>/dev/null || true; fi
  if [ -n "$APP_PID" ]; then wait "$APP_PID" 2>/dev/null || true; fi
  APP_PID=""
  APP_PGID=""
}

digest_store () {
  local app="$1"
  local url="$2"
  ( cd "$app" && node "$ROOT/harness/chaos/pg-digest.mjs" "$url" --schema "$STORE_SCHEMA" )
}

digest_app () {
  local app="$1"
  local url="$2"
  ( cd "$app" && node "$CDIR/app-digest.mjs" "$url" --schema "$APP_SCHEMA" )
}

scenario_for_class () {
  case "$1" in
    K1) echo "$CDIR/scenarios/k1-append.json" ;;
    K2) echo "$CDIR/scenarios/k2-midstream.json" ;;
    K3) echo "$CDIR/scenarios/k3-redelivery.json" ;;
    K4) echo "$CDIR/scenarios/k4-idle.json" ;;
    K5) echo "$CDIR/scenarios/k5-409.json" ;;
    K6) echo "$CDIR/scenarios/k6-cutover.json" ;;
    K7) echo "$CDIR/scenarios/k7-rollback.json" ;;
  esac
}

baseline_scenario_for_class () {
  case "$1" in
    K5) echo "$CDIR/scenarios/k5-canonical.json" ;;
    *) scenario_for_class "$1" ;;
  esac
}

phase_for_class () {
  case "$1" in
    K1) echo "K1-append" ;;
    K2) echo "K2-midstream" ;;
    K3) echo "K3-redeliver" ;;
    K4) echo "K4-idle" ;;
    K5) echo "K5-409" ;;
    K6) echo "K6-cutover" ;;
    K7) echo "K7-rollback" ;;
  esac
}

variants_for_class () {
  case "$1" in
    K5)
      printf '%s\t%s\t%s\t%s\n' \
        "409" "$CDIR/scenarios/k5-409.json" "K5-409" "$(fidelity_for_class K5)"
      printf '%s\t%s\t%s\t%s\n' \
        "wrongfork" "$CDIR/scenarios/k5-reorg.json" "K5-wrongfork" "$(fidelity_for_class K5)"
      ;;
    *)
      printf '%s\t%s\t%s\t%s\n' \
        "main" "$(scenario_for_class "$1")" "$(phase_for_class "$1")" "$(fidelity_for_class "$1")"
      ;;
  esac
}

# K2 varies the mid-stream kill point across iterations so the "arbitrary N/M" claim is real
# rather than one fixed block repeated. The k2-midstream stream is `blocks count:12` from 101
# (blocks 101..112); interior mid-stream kill points are 103..110 (leave the first two blocks so a
# vault+deposit row exists pre-kill, and the last block so the kill is strictly mid-stream). Empty
# output ⇒ the scenario's own killAt.block is used unchanged.
kill_block_for () {
  local class="$1"
  local iteration="$2"
  case "$class" in
    K2)
      local lo=103
      local span=8
      echo "$(( lo + ( (iteration - 1) % span ) ))"
      ;;
    *) echo "" ;;
  esac
}

fidelity_for_class () {
  case "$1" in
    K1) echo "HIGH" ;;
    K2) echo "HIGH" ;;
    K3) echo "MEDIUM" ;;
    K4) echo "HIGH" ;;
    K5) echo "MEDIUM" ;;
    K6) echo "MEDIUM" ;;
    K7) echo "MEDIUM" ;;
  esac
}

record_run () {
  local class="$1"
  local variant="$2"
  local iteration="$3"
  local status="$4"
  local run_dir="$5"
  local scenario="$6"
  local phase="$7"
  local fidelity="$8"
  RUN_CLASS="$class" RUN_VARIANT="$variant" RUN_ITERATION="$iteration" RUN_STATUS="$status" RUN_DIR="$run_dir" \
  RUN_SCENARIO="$scenario" RUN_PHASE="$phase" RUN_FIDELITY="$fidelity" \
  node - <<'NODE' >> "$RECORDS"
const fs = require('node:fs');
const readJson = (file) => {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch { return null; }
};
const dir = process.env.RUN_DIR;
// Empirically recover the block the kill gate fired on from the kill-side phase log: the last
// `blocked:true` marker for this run's phase carries the gate details (`nextBlock`/`afterBlock`/
// `block`). This proves the kill point from the artifact rather than trusting the env override.
const killBlockFromPhaseLog = (file, phase) => {
  let text;
  try { text = fs.readFileSync(file, 'utf8'); }
  catch { return null; }
  let block = null;
  for (const line of text.trim().split(/\n+/).filter(Boolean)) {
    let entry;
    try { entry = JSON.parse(line); }
    catch { continue; }
    if (entry.name !== phase || entry.blocked !== true) continue;

    const d = entry.details ?? {};
    const candidate = d.nextBlock ?? d.block ?? (d.afterBlock !== undefined ? Number(d.afterBlock) + 1 : undefined);
    if (candidate !== undefined) block = Number(candidate);
  }

  return block;
};
const record = {
  class: process.env.RUN_CLASS,
  variant: process.env.RUN_VARIANT,
  iteration: Number(process.env.RUN_ITERATION),
  status: process.env.RUN_STATUS,
  scenario: process.env.RUN_SCENARIO,
  phase: process.env.RUN_PHASE,
  fidelity: process.env.RUN_FIDELITY,
  killAtBlock: killBlockFromPhaseLog(`${dir}/kill.phase.log`, process.env.RUN_PHASE),
  verify: readJson(`${dir}/verify.facts.json`),
  stats: readJson(`${dir}/mock-stats.json`),
  killStats: readJson(`${dir}/kill.mock-stats.json`),
};
console.log(JSON.stringify(record));
NODE
}

run_baseline () {
  local app="$1"
  local class="$2"
  local scenario="$3"
  local phase="$4"
  local db="rg3_${class,,}_baseline_$$"
  local url="postgres://postgres@127.0.0.1:$PGPORT/$db"
  local dir="$RUN_ROOT/baselines/$class"
  mkdir -p "$dir"
  drop_create_db "$db" || return 1
  start_mock "$scenario" "$dir/phase.log" "$dir/mock.log" || return 1
  start_app "$app" "$url" "$dir/app.log"
  if [ -n "$phase" ]; then
    wait_phase "$phase" || {
      log "fatal: $class baseline did not reach $phase"
      return 1
    }
    curl_json POST /__release "{\"phase\":\"$phase\"}" >/dev/null || return 1
  fi
  wait_complete "$APP_PID" "$dir/app.log" || return 1
  kill_app
  curl_json GET /__stats > "$dir/mock-stats.json" || true
  stop_mock

  local store app_digest
  store="$(digest_store "$app" "$url")" || return 1
  app_digest="$(digest_app "$app" "$url")" || return 1
  printf '%s\n' "$store" > "$BASE_DIR/${class,,}.store.digest"
  printf '%s\n' "$app_digest" > "$BASE_DIR/${class,,}.app.digest"
  printf '%s\n' "$store" > "$dir/store.digest"
  printf '%s\n' "$app_digest" > "$dir/app.digest"
  log "$class baseline store=$store app=$app_digest"
}

run_kill_resume () {
  local app="$1"
  local class="$2"
  local variant="$3"
  local iteration="$4"
  local scenario="$5"
  local phase="$6"
  local fidelity="$7"
  local db="rg3_${class,,}_${variant}_${iteration}_$$"
  local url="postgres://postgres@127.0.0.1:$PGPORT/$db"
  local dir="$RUN_ROOT/classes/$class/$variant-$iteration"
  local killat_block
  killat_block="$(kill_block_for "$class" "$iteration")"
  mkdir -p "$dir"
  drop_create_db "$db" || return 1
  start_mock "$scenario" "$dir/phase.log" "$dir/mock.log" "$killat_block" || return 1
  start_app "$app" "$url" "$dir/kill.app.log"
  wait_phase "$phase" || {
    log "fatal: $class/$variant#$iteration did not reach $phase"
    record_run "$class" "$variant" "$iteration" "phase-timeout" "$dir" "$scenario" "$phase" "$fidelity"
    return 1
  }
  cp "$dir/phase.log" "$dir/kill.phase.log"
  kill_app
  if invariant "$dir/kill.app.log"; then
    log "fatal: invariant before kill for $class/$variant#$iteration"
    record_run "$class" "$variant" "$iteration" "kill-invariant" "$dir" "$scenario" "$phase" "$fidelity"
    return 1
  fi
  curl_json GET /__stats > "$dir/kill.mock-stats.json" || true
  curl_json POST /__reset '{}' >/dev/null || return 1
  start_app "$app" "$url" "$dir/resume.app.log"
  wait_phase "$phase" || {
    log "fatal: $class/$variant#$iteration resume did not reach $phase"
    record_run "$class" "$variant" "$iteration" "resume-phase-timeout" "$dir" "$scenario" "$phase" "$fidelity"
    return 1
  }
  curl_json POST /__release "{\"phase\":\"$phase\"}" >/dev/null || return 1
  wait_complete "$APP_PID" "$dir/resume.app.log" || {
    record_run "$class" "$variant" "$iteration" "resume-incomplete" "$dir" "$scenario" "$phase" "$fidelity"
    return 1
  }
  kill_app
  curl_json GET /__stats > "$dir/mock-stats.json" || true
  stop_mock

  ROOT="$ROOT" PROBE_DIR="$app" STORE_SCHEMA="$STORE_SCHEMA" APP_SCHEMA="$APP_SCHEMA" \
    VERIFY_FACTS="$dir/verify.facts.json" \
    VERIFY_TARGET_PHASE="$phase" \
    "$VERIFY" "$url" "$BASE_DIR/${class,,}.store.digest" "$BASE_DIR/${class,,}.app.digest" \
      "$dir/kill.phase.log" "$phase" >"$dir/verify.log" 2>&1 || {
        log "fatal: verify failed for $class/$variant#$iteration"
        cat "$dir/verify.log"
        record_run "$class" "$variant" "$iteration" "verify-failed" "$dir" "$scenario" "$phase" "$fidelity"
        return 1
      }
  record_run "$class" "$variant" "$iteration" "ok" "$dir" "$scenario" "$phase" "$fidelity"
}

write_summary () {
  SUMMARY_FILE="$SUMMARY_FILE" RESULT_FILE="$RESULT_FILE" RECORDS="$RECORDS" \
  TARGET_TOTAL="$TOTAL_TARGET" MIN_PER_CLASS="$MIN_PER_CLASS" KILLS_PER_CLASS="$KILLS_PER_CLASS" \
  RUN_CLASSES="$ACTIVE_CLASSES" RUN_ROOT="$RUN_ROOT" node - <<'NODE'
const fs = require('node:fs');
const classes = (process.env.RUN_CLASSES || 'K1 K2 K3 K4 K5 K6')
  .trim()
  .split(/\s+/)
  .filter(Boolean);
const labels = {
  K1: 'HIGH',
  K2: 'HIGH',
  K3: 'MEDIUM',
  K4: 'HIGH',
  K5: 'MEDIUM',
  K6: 'MEDIUM',
  K7: 'MEDIUM',
};
const candor = {
  K1: 'append gate baseline',
  K2: 'mid-stream kill; kill block varied across iterations over the interior of the 101..112 stream',
  K3: 'redelivery-of-block only; redelivery watchdog + held-finalize drain NOT exercised (need real Portal / halted-chain sim).',
  K4: 'idle / 204 retry path',
  K5: "synthetic fork via stale-parentBlockHash 409 (client consumes the canonical replacement chain and resumes byte-identically) PLUS a wrong-fork-finalize handler-REACHABILITY probe. handlerStats.wrongForkFinalizes counts mock-side handler entries (reachability) ONLY — the counter increments on handler entry, before the gate, so on a killed run the splice may not even execute; the injected wrong finalized head is NOT consumed by the client here (finalize poll cadence ~4s > the post-injection scenario window ~1.8s), so this is not clean-resume wrong-fork evidence. The client-side wrong-fork-finalize FATAL guard (hash-mismatch at finality) is proven deterministically by portal-realtime.test.ts ('a finalize whose canonical hash mismatches the local block is FATAL', finalizePollMs:0). Real L1-reorg wire fidelity NOT exercised.",
  K6: "kill lands at the backfill→live chunk boundary (cutoverGate fires on the client's natural fromBlock=106 request, not a fixed block), so resume re-drives an organic cutover within the mock's deterministic capability. Probe-retry wire fidelity (the 3-attempt loop in clampFinalizedToPortalHead) NOT exercised — mock's /finalized-head succeeds first try; probe-retry fidelity rests on portal-realtime-wire.test.ts.",
  K7: "kill fires mid-stream of the rollback branch: after 101..106 on main, the client resumes at fromBlock=107/parent=106:main, the mock serves a reorg branch forking at block 104 (104:rollback carries parent 103:main, in-window above the <=102 finalized anchor), so reconcile -> {kind:'reorg'} rolls back 104..106:main and re-applies the rollback branch 104..112 (extended to PONDER_END so the app completes). The SIGKILL fires when the mock has written fork block 104 and is holding at the block-105 gate; the kill-controller waits on the MOCK's phase gate, not on any app-side reorg marker, so this is mock-side provable only. It is NOT claimed that the client consumed 104, emitted {type:'reorg'}, and applied the reorg before the kill — that would be an unwitnessed TCP/event-loop race. The proof that the rollback-apply CODE PATH is exercised and correct is the byte-identical RESUME digest (the baseline reorgs, the resumed run reorgs to the same digest). The fork point is strictly ABOVE the finalized anchor (<=102) so this is an ACCEPTED reorg, never the below-floor gap fatal. reorgApplied counts the mock serving a genuine below-tip cross-branch fork block (reorgBlock < fromBlock, parent on parentBranch), so a scenario that degraded to a plain append cannot self-report a K7 pass. Mock-driven crash-timing coverage of INV-10's rollback arm (same product code path from portal-realtime.ts:1105 as K5-409's accepted reorg). What is NOT proven here: live-protocol fork-choice fidelity — a real Portal 409/reorg with competing finalized sources and real parentHash chains — remains RG4/RG5.",
};
const lines = fs.existsSync(process.env.RECORDS)
  ? fs.readFileSync(process.env.RECORDS, 'utf8').trim().split(/\n+/).filter(Boolean)
  : [];
const records = lines.map((line) => JSON.parse(line));
const perClass = {};
for (const klass of classes) {
  const rows = records.filter((r) => r.class === klass);
  const ok = rows.filter((r) => r.status === 'ok');
  const digestMatch = ok.filter((r) => r.verify?.storeMatch && r.verify?.appMatch);
  const dupCount = ok.reduce((n, r) => n + Number(r.verify?.duplicateFinalizedRows ?? 0), 0);
  const finalizedScannedRows = ok.reduce((n, r) => n + Number(r.verify?.finalizedScannedRows ?? 0), 0);
  const stats = ok.reduce((acc, r) => {
    const s = r.stats ?? {};
    acc.r204 += Number(s.r204 ?? 0);
    acc.r409 += Number(s.r409 ?? 0);
    acc.redeliveryReopens += Number(s.redeliveryReopens ?? 0);
    acc.finalizedHead += Number(s.finalizedHead ?? 0);
    acc.finalizedHeadGates += Number(s.finalizedHeadGates ?? 0);
    acc.wrongForkFinalizes += Number(s.wrongForkFinalizes ?? 0);
    acc.wrongForkFinalizeConsumed += Number(s.wrongForkFinalizeConsumed ?? 0);
    acc.wrongForkFinalizeRejected += Number(s.wrongForkFinalizeRejected ?? 0);
    acc.reorgApplied += Number(s.reorgApplied ?? 0);
    return acc;
  }, {
    r204: 0,
    r409: 0,
    redeliveryReopens: 0,
    finalizedHead: 0,
    finalizedHeadGates: 0,
    wrongForkFinalizes: 0,
    wrongForkFinalizeConsumed: 0,
    wrongForkFinalizeRejected: 0,
    reorgApplied: 0,
  });
  // Distinct empirical kill blocks (from each run's kill.phase.log) — proves K2's varied mid-stream
  // coverage rather than one repeated fixed block. `killBlocksRecorded` keeps every row's non-null
  // block so a cert can demand that EVERY kill recorded a block (a dropped/null row must not be able
  // to slip past a distinct-set check where another row happened to record the expected block).
  const killBlocksRecorded = rows.map((r) => r.killAtBlock).filter((b) => b != null);
  const killBlocks = [...new Set(killBlocksRecorded)].sort((a, b) => a - b);
  // Live-cutover proof: kills where the mock had seen >=1 live /stream request before the kill. For K6
  // this is the load-bearing evidence that the kill lands at the backfill→live cutover request, not on a startup probe.
  const killsWithLiveStream = rows.filter((r) => Number(r.killStats?.stream ?? 0) > 0).length;
  const wrongForkRows = ok.filter((r) => r.variant === 'wrongfork');
  const wrongForkConsumedRuns = wrongForkRows.filter((r) => Number(r.stats?.wrongForkFinalizeConsumed ?? 0) > 0).length;
  // K7 non-vacuity: count ok runs whose FINAL mock stats recorded the reorg branch actually served
  // (reorgApplied > 0) on the resumed run. A scenario that silently degraded to a plain append (fork
  // point drifting to the tip) never increments the mock-side counter, so a vacuous K7 pass cannot
  // self-report reorgAppliedRuns === killCount.
  const reorgAppliedRuns = ok.filter((r) => Number(r.stats?.reorgApplied ?? 0) > 0).length;
  // Killed-run reachability: the mock's `reorgApplied` counter increments at the fork block (104),
  // BEFORE the block-105 gate where the SIGKILL lands. So the KILLED run's own final stats (captured
  // right before the reset) must already record reorgApplied > 0 — direct mock-side evidence that
  // fork block 104 was served before the kill (the kill lands mid-stream of the rollback branch),
  // not only that the resumed run re-served it.
  const reorgAppliedKillRuns = ok.filter((r) => Number(r.killStats?.reorgApplied ?? 0) > 0).length;
  perClass[klass] = {
    scenarios: [...new Set(rows.map((r) => r.scenario).filter(Boolean))],
    variantLabels: [...new Set(rows.map((r) => r.variant).filter(Boolean))].join(', '),
    killCount: rows.length,
    killBlocks,
    killBlocksRecordedCount: killBlocksRecorded.length,
    killsWithLiveStream,
    wrongForkRunCount: wrongForkRows.length,
    wrongForkConsumedRuns,
    reorgAppliedRuns,
    reorgAppliedKillRuns,
    cleanResumeCount: ok.length,
    digestMatchCount: digestMatch.length,
    duplicateFinalizedRows: dupCount,
    finalizedScannedRows,
    fidelity: labels[klass],
    candor: candor[klass],
    handlerStats: stats,
    failures: rows.filter((r) => r.status !== 'ok').map((r) => ({
      iteration: r.iteration,
      variant: r.variant,
      status: r.status,
    })),
  };
}
const totals = Object.values(perClass).reduce((acc, c) => {
  acc.killCount += c.killCount;
  acc.cleanResumeCount += c.cleanResumeCount;
  acc.digestMatchCount += c.digestMatchCount;
  acc.duplicateFinalizedRows += c.duplicateFinalizedRows;
  acc.finalizedScannedRows += c.finalizedScannedRows;
  return acc;
}, { killCount: 0, cleanResumeCount: 0, digestMatchCount: 0, duplicateFinalizedRows: 0, finalizedScannedRows: 0 });
const targetTotal = Number(process.env.TARGET_TOTAL);
const minPerClass = Number(process.env.MIN_PER_CLASS);
const isDupCheckExempt = (klass) => /^K[16]/i.test(klass);
const loadBearingClasses = classes.filter((klass) => !isDupCheckExempt(klass));
const hasClass = (klass) => perClass[klass] !== undefined;
const acceptance = {
  targetTotal,
  minPerClass,
  requestedKillsPerClass: Number(process.env.KILLS_PER_CLASS),
  totalKillsOk: totals.killCount >= targetTotal,
  perClassKillsOk: classes.every((klass) => perClass[klass].killCount >= minPerClass),
  cleanResumeOk: totals.killCount > 0 && totals.cleanResumeCount === totals.killCount,
  digestMatchOk: totals.killCount > 0 && totals.digestMatchCount === totals.killCount,
  duplicateFinalizedRowsOk: totals.duplicateFinalizedRows === 0,
  dupCheckLoadBearingOk: loadBearingClasses.every((klass) => perClass[klass].finalizedScannedRows > 0),
  // Self-certify the two non-vacuity properties this harness exists to prove, so a regression to the
  // original vacuous-K6 bug (kill landing on the /finalized-head startup probe, killStats.stream===0)
  // or to a fixed-block K2 cannot silently re-produce `accepted:true` on a byte-identical resume.
  k6CutoverNonVacuousOk: !hasClass('K6')
    || (perClass.K6.killCount > 0 && perClass.K6.killsWithLiveStream === perClass.K6.killCount),
  k6CutoverOrganicOk: !hasClass('K6')
    || (perClass.K6.killCount > 0
    && perClass.K6.killBlocksRecordedCount === perClass.K6.killCount
    && perClass.K6.killBlocks.length === 1
    && perClass.K6.killBlocks[0] === 106
    && perClass.K6.killsWithLiveStream === perClass.K6.killCount),
  k2KillSpreadOk: !hasClass('K2') || perClass.K2.killBlocks.length >= 3,
  // Left `undefined` (not `true`) when K7 is absent so the guards below skip it entirely — a class
  // that never ran must not self-report a vacuous "non-vacuous: true" pass in RESULT.md. When present,
  // mirror k6CutoverOrganicOk's non-vacuity self-cert so a scenario that silently degraded to a plain
  // append (fork point drifting to the tip, so the kill never lands mid the rollback branch) cannot
  // report a byte-identical pass: every K7 kill must have seen a live /stream request before the kill
  // (killsWithLiveStream), the empirical kill-block set must be exactly {105} (the gate block), every
  // ok run's FINAL mock stats must record the reorg branch actually served (reorgAppliedRuns), AND the
  // KILLED run's own stats must already record the fork served before the kill (reorgAppliedKillRuns) —
  // mock-side proof the SIGKILL lands mid-stream of the rollback branch, not that the client applied
  // the reorg before the kill (an unwitnessed race; the byte-identical RESUME digest is what proves
  // the apply code path). These are the mock-side reachability floor for the load-bearing splice.
  k7RollbackNonVacuousOk: !hasClass('K7')
    ? undefined
    : (perClass.K7.killCount > 0
      && perClass.K7.cleanResumeCount === perClass.K7.killCount
      && perClass.K7.digestMatchCount === perClass.K7.killCount
      && perClass.K7.killsWithLiveStream === perClass.K7.killCount
      && perClass.K7.killBlocksRecordedCount === perClass.K7.killCount
      && perClass.K7.killBlocks.length === 1
      && perClass.K7.killBlocks[0] === 105
      && perClass.K7.reorgAppliedRuns === perClass.K7.killCount
      && perClass.K7.reorgAppliedKillRuns === perClass.K7.killCount),
};
acceptance.accepted = acceptance.totalKillsOk
  && acceptance.perClassKillsOk
  && acceptance.cleanResumeOk
  && acceptance.digestMatchOk
  && acceptance.duplicateFinalizedRowsOk
  && acceptance.dupCheckLoadBearingOk
  && acceptance.k6CutoverNonVacuousOk
  && acceptance.k6CutoverOrganicOk
  && acceptance.k2KillSpreadOk;
if (acceptance.k7RollbackNonVacuousOk !== undefined) {
  acceptance.accepted = acceptance.accepted && acceptance.k7RollbackNonVacuousOk;
}
const summary = {
  generatedAt: new Date().toISOString(),
  runRoot: process.env.RUN_ROOT,
  acceptance,
  totals,
  perClass,
};
fs.writeFileSync(process.env.SUMMARY_FILE, `${JSON.stringify(summary, null, 2)}\n`);

const md = [];
md.push('# RG3 Phase B Realtime Chaos Result');
md.push('');
md.push(`Run root: \`${process.env.RUN_ROOT}\``);
md.push('');
md.push('## Totals');
md.push('');
md.push(`- kills: ${totals.killCount} / target ${targetTotal}`);
md.push(`- clean resumes: ${totals.cleanResumeCount} / ${totals.killCount}`);
md.push(`- digest matches: ${totals.digestMatchCount} / ${totals.killCount}`);
md.push(`- duplicate FINALIZED rows: ${totals.duplicateFinalizedRows}`);
md.push(`- dup-check load-bearing: ${acceptance.dupCheckLoadBearingOk}`);
md.push(`- K6 cutover non-vacuous (every K6 kill after >=1 live /stream request): ${acceptance.k6CutoverNonVacuousOk}`);
md.push(`- K6 organic cutover (every K6 kill on block 106): ${acceptance.k6CutoverOrganicOk}`);
md.push(`- K2 kill-block spread (>=3 distinct interior blocks): ${acceptance.k2KillSpreadOk}`);
if (acceptance.k7RollbackNonVacuousOk !== undefined) {
  md.push(`- K7 rollback non-vacuous: ${acceptance.k7RollbackNonVacuousOk}`);
}
md.push(`- per-class minimum: ${minPerClass}`);
md.push('');
md.push('## Classes');
md.push('');
for (const klass of classes) {
  const c = perClass[klass];
  md.push(`### ${klass}`);
  md.push('');
  md.push(`- scenarios: ${c.scenarios.map((s) => `\`${s}\``).join(', ') || 'none'}`);
  md.push(`- variants: ${c.variantLabels || 'none'}`);
  md.push(`- kills: ${c.killCount}`);
  md.push(`- clean resumes: ${c.cleanResumeCount}`);
  md.push(`- digest matches: ${c.digestMatchCount}`);
  md.push(`- duplicate FINALIZED rows: ${c.duplicateFinalizedRows}`);
  md.push(`- finalized rows scanned: ${c.finalizedScannedRows}`);
  md.push(`- kill blocks (distinct, from kill.phase.log): ${c.killBlocks.length > 0 ? c.killBlocks.join(', ') : 'n/a'}`);
  md.push(`- kills after >=1 live /stream request: ${c.killsWithLiveStream} / ${c.killCount}`);
  if (klass === 'K5') md.push(`- wrong-fork consumed runs: ${c.wrongForkConsumedRuns} / ${c.wrongForkRunCount}`);
  if (klass === 'K7') md.push(`- reorg-applied runs (mock re-served the rollback branch on resume): ${c.reorgAppliedRuns} / ${c.killCount}`);
  if (klass === 'K7') md.push(`- reorg-applied on the KILLED run (fork served before the kill): ${c.reorgAppliedKillRuns} / ${c.killCount}`);
  md.push(`- handler stats: ${JSON.stringify(c.handlerStats)}`);
  md.push(`- fidelity: ${c.fidelity} — ${c.candor}`);
  if (c.failures.length > 0) md.push(`- failures: ${JSON.stringify(c.failures)}`);
  md.push('');
}
md.push('## Finish Command');
md.push('');
md.push(`Full acceptance command: \`CHAOS_KILLS_PER_CLASS=${Math.max(34, minPerClass)} harness/chaos/realtime/orchestrate.sh\``);
md.push('');
fs.writeFileSync(process.env.RESULT_FILE, `${md.join('\n')}\n`);
NODE
}

main () {
  log "df before orchestrate"
  df -h "$ROOT" /tmp | tee "$RUN_ROOT/df-before.txt"
  [ "$MOCK_PORT" != "9547" ] || { log "fatal: guarded port 9547 is forbidden"; exit 2; }

  CHAOS_WORK="$WORK_ROOT" CHAOS_PGPORT="$PGPORT" CHAOS_PGSOCK="$PGSOCK" bash "$PGCTL" ensure \
    >"$RUN_ROOT/pg-ctl.log" 2>&1 || {
      log "fatal: could not start throwaway Postgres; tail follows"
      tail -100 "$RUN_ROOT/pg-ctl.log" || true
      exit 1
    }
  local fsync
  fsync="$(psql_ours 'show fsync')" || exit 1
  [ "$fsync" = "on" ] || { log "fatal: Postgres fsync is not on"; exit 1; }

  local core app
  core="$(ensure_graft)" || exit 1
  app="$(prepare_app "$core")" || exit 1
  ensure_pg_resolution "$app" || exit 1

  # CHAOS_CLASSES (space/comma-separated, e.g. "K6" or "K2 K6") restricts the run to a subset of
  # classes — for targeted proofs/reruns. Default: the full K1..K6 acceptance set. A subset run does NOT
  # meet the >=200-kill / all-class acceptance bar; it is a probe, not a certification.
  local classes=(K1 K2 K3 K4 K5 K6)
  if [ -f "$CDIR/scenarios/k7-rollback.json" ]; then
    classes+=(K7)
    log "K7 scenario present: including kill-during-rollback-apply class"
  else
    log "K7 scenario absent: deferring kill-during-rollback-apply to RG4/RG5"
  fi
  if [ -n "${CHAOS_CLASSES:-}" ]; then
    read -r -a classes <<< "$(printf '%s' "$CHAOS_CLASSES" | tr ',' ' ')"
    log "CHAOS_CLASSES set: running subset ${classes[*]} (NOT a full acceptance run)"
  fi
  ACTIVE_CLASSES="${classes[*]}"
  if [ -z "$TOTAL_TARGET" ]; then
    if [[ " ${classes[*]} " == *" K7 "* ]]; then
      TOTAL_TARGET=238
    else
      TOTAL_TARGET=200
    fi
  fi
  if [ -z "$KILLS_PER_CLASS" ]; then
    local nclasses="${#classes[@]}"
    KILLS_PER_CLASS="$(( (TOTAL_TARGET + nclasses - 1) / nclasses ))"
    if [ "$KILLS_PER_CLASS" -lt "$MIN_PER_CLASS" ]; then
      KILLS_PER_CLASS="$MIN_PER_CLASS"
    fi
  fi
  for class in "${classes[@]}"; do
    local baseline_scenario phase
    baseline_scenario="$(baseline_scenario_for_class "$class")"
    phase="$(phase_for_class "$class")"
    if [ "$class" = "K5" ]; then phase=""; fi
    log "building $class baseline with $baseline_scenario"
    run_baseline "$app" "$class" "$baseline_scenario" "$phase" || exit 1
  done

  for class in "${classes[@]}"; do
    while IFS=$'\t' read -r variant scenario phase fidelity; do
      local i=1
      while [ "$i" -le "$KILLS_PER_CLASS" ]; do
        log "running $class/$variant kill $i/$KILLS_PER_CLASS"
        run_kill_resume "$app" "$class" "$variant" "$i" "$scenario" "$phase" "$fidelity" || {
          write_summary
          exit 1
        }
        i=$(( i + 1 ))
      done
    done < <(variants_for_class "$class")
  done

  write_summary
  log "orchestrate complete; summary=$SUMMARY_FILE result=$RESULT_FILE"
}

main "$@"
