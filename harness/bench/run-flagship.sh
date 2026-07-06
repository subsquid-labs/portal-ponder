#!/usr/bin/env bash
# run-flagship.sh — drive ONE deterministic 15-chain flagship benchmark run of harness/euler-multichain
# against the local pinned-anchor shim, then emit the machine-readable result + reproducibility manifest.
#
# Fully PARAMETERIZED — no hardcoded paths. Everything comes from env so the driver runs anywhere and
# stays generic (it is meant to be launched INSIDE a caller's process supervisor / systemd-run scope;
# it NEVER calls systemd-run itself):
#   SQD_PONDER_TARBALL   the @subsquid/ponder fork build under test (file:… installed by run.sh)
#   DATABASE_URL         a FRESH postgres DB (the driver refuses to run against a non-empty ponder_sync)
#   PORTAL_URL           Portal base (the ONLY data source at run time)
#   PORTAL_API_KEY       Portal key (from env; never printed/committed)
#   BENCH_RPC_BASE       the pinned-anchor shim base, e.g. http://127.0.0.1:8645 (probed at preflight)
#   ANCHORS_FILE         the committed anchors-<date>.json the shim serves (this driver can start it)
#   BENCH_SCHEMA         ponder --schema (default: euler_bench)
#   BENCH_PORT           ponder --port    (default: 42069) — /ready + /metrics live here
#   BENCH_OUT_DIR        where result/manifest/metrics land (default: ./bench-out)
#   BENCH_POLL_SECONDS   completion poll interval (default 15)  BENCH_TIMEOUT_SECONDS (default 9000)
#   BENCH_LOAD           free-text load-conditions note recorded in the manifest
#   BENCH_MAX_OLD_SPACE_MB  V8 --max-old-space-size for the ponder child (default 32768 — the value
#                        every baseline run was measured with; node's ~4 GB default OOMs a 15-chain
#                        full backfill)
#
#   bash harness/bench/run-flagship.sh            # full 15-chain run
#   bash harness/bench/run-flagship.sh --smoke    # single tiny chain (polygon) — validates the shim
#                                                 # surface is complete before committing a full run
#
# --smoke is the ONE place EULER_CHAINS is set (to polygon, explicitly by the flag). For the full run
# EULER_CHAINS MUST be UNSET — it silently subsets the 15 chains — and the preflight ABORTS if it is set.
set -uo pipefail

BDIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BDIR/../.." && pwd)"
APP_DIR="$ROOT/harness/euler-multichain"

SMOKE=0
for arg in "$@"; do
  case "$arg" in
    --smoke) SMOKE=1 ;;
    *) echo "✗ unknown arg: $arg (only --smoke is supported)"; exit 2 ;;
  esac
done

command -v node >/dev/null || { echo "✗ node not found"; exit 1; }
command -v curl >/dev/null || { echo "✗ curl not found"; exit 1; }

BENCH_SCHEMA="${BENCH_SCHEMA:-euler_bench}"
BENCH_PORT="${BENCH_PORT:-42069}"
BENCH_OUT_DIR="${BENCH_OUT_DIR:-$PWD/bench-out}"
BENCH_POLL_SECONDS="${BENCH_POLL_SECONDS:-15}"
BENCH_TIMEOUT_SECONDS="${BENCH_TIMEOUT_SECONDS:-9000}"
BENCH_LOAD="${BENCH_LOAD:-<not recorded>}"
mkdir -p "$BENCH_OUT_DIR"

# ── preflight: fail LOUD on any misconfiguration BEFORE a multi-hour run ──────────────────────────
echo "▶ preflight"

# EULER_CHAINS guard: unset for a full run (it silently subsets the 15 chains). --smoke sets it itself.
if [ "$SMOKE" = "0" ] && [ -n "${EULER_CHAINS:-}" ]; then
  echo "✗ EULER_CHAINS is set ('$EULER_CHAINS') — it silently subsets the run. Unset it for the full 15-chain bench (or use --smoke)."
  exit 2
fi

: "${PORTAL_URL:?set PORTAL_URL (the Portal base — the only run-time data source)}"
: "${PORTAL_API_KEY:?set PORTAL_API_KEY (Portal key; from env, never printed)}"
: "${DATABASE_URL:?set DATABASE_URL to a FRESH postgres DB}"
: "${SQD_PONDER_TARBALL:?set SQD_PONDER_TARBALL to the fork build under test}"
: "${BENCH_RPC_BASE:?set BENCH_RPC_BASE to the pinned-anchor shim base (e.g. http://127.0.0.1:8645)}"

[ -f "$SQD_PONDER_TARBALL" ] || { echo "✗ SQD_PONDER_TARBALL not found: $SQD_PONDER_TARBALL"; exit 1; }

# DB freshness: refuse to run if ponder_sync already has data (a re-run against a dirty DB is not a
# reproducible bench and would double-count / mislead the parity check). A missing schema is fine (fresh).
FRESH="$(node "$BDIR/db-fresh.mjs" "$DATABASE_URL" 2>/dev/null)"
case "$FRESH" in
  fresh) echo "  ✓ DB fresh (no ponder_sync.logs rows)" ;;
  dirty:*) echo "✗ DATABASE_URL is NOT fresh (${FRESH#dirty:} ponder_sync.logs rows). Use a fresh DB."; exit 2 ;;
  *) echo "✗ could not verify DB freshness ($FRESH)"; exit 2 ;;
esac

# Shim probe: the shim MUST be up (this driver does not depend on any external RPC). If ANCHORS_FILE is
# set and the shim is not answering, start it in the background; otherwise require it already up.
SHIM_PID=""
probe_shim() { curl -sf "$BENCH_RPC_BASE/health" >/dev/null 2>&1; }
if probe_shim; then
  echo "  ✓ shim already up at $BENCH_RPC_BASE"
elif [ -n "${ANCHORS_FILE:-}" ]; then
  [ -f "$ANCHORS_FILE" ] || { echo "✗ ANCHORS_FILE not found: $ANCHORS_FILE"; exit 1; }
  # derive the port from BENCH_RPC_BASE so the shim listens where the config points.
  SHIM_PORT="$(node -e 'console.log(new URL(process.env.BENCH_RPC_BASE).port || 8645)')"
  echo "  ▷ starting shim on :$SHIM_PORT from $(basename "$ANCHORS_FILE")"
  node "$BDIR/anchor-shim.mjs" --anchors "$ANCHORS_FILE" --port "$SHIM_PORT" >"$BENCH_OUT_DIR/shim.log" 2>&1 &
  SHIM_PID=$!
  for _ in $(seq 1 40); do probe_shim && break; sleep 0.25; done
  probe_shim || { echo "✗ shim did not come up on $BENCH_RPC_BASE (see $BENCH_OUT_DIR/shim.log)"; exit 1; }
  echo "  ✓ shim up (pid $SHIM_PID)"
else
  echo "✗ shim not reachable at $BENCH_RPC_BASE and ANCHORS_FILE unset — cannot start it. Bring the shim up first."
  exit 1
fi

cleanup() {
  [ -n "${PONDER_PID:-}" ] && kill "$PONDER_PID" 2>/dev/null
  # only stop the shim if THIS driver started it (never kill a shim the operator owns).
  [ -n "$SHIM_PID" ] && kill "$SHIM_PID" 2>/dev/null
}
trap cleanup EXIT INT TERM

# ── install the fork tarball into the app (run.sh does the file: rewrite + npm install) ───────────
echo "▶ installing @subsquid/ponder ($SQD_PONDER_TARBALL) into the app"
node -e '
  const fs = require("fs");
  const p = require(process.argv[1]);
  p.dependencies["@subsquid/ponder"] = "file:" + process.env.SQD_PONDER_TARBALL;
  fs.writeFileSync(process.argv[1], JSON.stringify(p, null, 2) + "\n");
' "$APP_DIR/package.json"
( cd "$APP_DIR" && npm install --no-audit --no-fund --silent ) || { echo "✗ npm install failed"; exit 1; }

# ── launch ponder ─────────────────────────────────────────────────────────────────────────────────
export PORTAL_METRICS_FILE="${PORTAL_METRICS_FILE:-$BENCH_OUT_DIR/portal}"
export PONDER_LOG_LEVEL="${PONDER_LOG_LEVEL:-info}"
# V8 heap for the ponder child. node's default (~4 GB) dies in ineffective mark-compacts partway
# through a 15-chain full backfill; every baseline run (44m55s config-b, bench-2a) was measured
# with --max-old-space-size=32768 inside a 16 GB cgroup, so that is the default here. The cgroup
# (the caller's systemd-run scope) stays the real memory ceiling.
export NODE_OPTIONS="--max-old-space-size=${BENCH_MAX_OLD_SPACE_MB:-32768}"
if [ "$SMOKE" = "1" ]; then
  # smoke: the ONE allowed EULER_CHAINS set — a single tiny chain to validate the shim surface fast.
  export EULER_CHAINS="polygon"
  echo "▶ SMOKE run — EULER_CHAINS=polygon (validates the shim surface is complete)"
  EXPECT_CHAINS="polygon"
else
  echo "▶ FULL run — 15 chains"
  EXPECT_CHAINS="$(node -e 'const c=require(process.argv[1]);process.stdout.write(c.map(x=>x.name).join(","))' "$APP_DIR/chains.json")"
fi

UNIT_START_MS="$(node -e 'console.log(Date.now())')"
PONDER_LOG="$BENCH_OUT_DIR/ponder.log"
echo "▶ ponder start --schema $BENCH_SCHEMA --port $BENCH_PORT (log: $PONDER_LOG)"
( cd "$APP_DIR" && exec ./node_modules/.bin/ponder start --schema "$BENCH_SCHEMA" --port "$BENCH_PORT" ) >"$PONDER_LOG" 2>&1 &
PONDER_PID=$!

metrics_url="http://127.0.0.1:$BENCH_PORT/metrics"
ready_url="http://127.0.0.1:$BENCH_PORT/ready"

# ── poll completion: /ready == 200 AND ponder_sync_is_complete==1 for every expected chain ────────
READY_MS=""
DEADLINE=$(( $(date +%s) + BENCH_TIMEOUT_SECONDS ))
COMPLETE=0
echo "▶ polling completion (every ${BENCH_POLL_SECONDS}s, timeout ${BENCH_TIMEOUT_SECONDS}s)"
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  # the app process must still be alive; a crash is an immediate failure.
  if ! kill -0 "$PONDER_PID" 2>/dev/null; then
    echo "✗ ponder exited before completion — tail of $PONDER_LOG:"
    tail -30 "$PONDER_LOG" | sed 's/^/    /'
    exit 1
  fi

  # /ready flips to 200 the moment historical indexing is complete; record when we first see it.
  if [ -z "$READY_MS" ] && curl -sf -o /dev/null "$ready_url"; then
    READY_MS="$(node -e 'console.log(Date.now())')"
    echo "  ✓ /ready → 200 (historical indexing complete)"
  fi

  # authoritative completion: every expected chain reports ponder_sync_is_complete==1.
  body="$(curl -sf "$metrics_url" 2>/dev/null)"
  if [ -n "$body" ]; then
    all_done="$(printf '%s' "$body" | EXPECT_CHAINS="$EXPECT_CHAINS" node -e '
      let d=""; process.stdin.on("data",c=>d+=c).on("end",()=>{
        const want = process.env.EXPECT_CHAINS.split(",").filter(Boolean);
        const done = new Set();
        for (const line of d.split("\n")) {
          const m = line.match(/^ponder_sync_is_complete\{chain="([^"]+)"\}\s+1\b/);
          if (m) done.add(m[1]);
        }
        process.stdout.write(want.every(c => done.has(c)) ? "yes" : "no");
      });
    ')"
    if [ "$all_done" = "yes" ]; then
      COMPLETE=1
      break
    fi
  fi

  sleep "$BENCH_POLL_SECONDS"
done

if [ "$COMPLETE" != "1" ]; then
  echo "✗ run did not complete within ${BENCH_TIMEOUT_SECONDS}s — tail of $PONDER_LOG:"
  tail -30 "$PONDER_LOG" | sed 's/^/    /'
  exit 1
fi
echo "▶ all chains complete — snapshotting metrics + emitting result"

# ── snapshot full /metrics, emit the result JSON + manifest ───────────────────────────────────────
curl -sf "$metrics_url" >"$BENCH_OUT_DIR/metrics.txt" || { echo "✗ could not snapshot /metrics"; exit 1; }

RESULT_JSON="$BENCH_OUT_DIR/bench.result.json"
node "$BDIR/emit-result.mjs" \
  --metrics-url "$metrics_url" \
  --chains "$EXPECT_CHAINS" \
  --unit-start-ms "$UNIT_START_MS" \
  ${READY_MS:+--ready-ms "$READY_MS"} \
  --out "$RESULT_JSON"
RESULT_RC=$?

node "$BDIR/emit-manifest.mjs" \
  --out "$BENCH_OUT_DIR/bench.manifest.json" \
  ${ANCHORS_FILE:+--anchors "$ANCHORS_FILE"} \
  --load "$BENCH_LOAD" \
  --repro "BENCH_RPC_BASE=$BENCH_RPC_BASE ANCHORS_FILE=<anchors.json> SQD_PONDER_TARBALL=<tgz> DATABASE_URL=<fresh> PORTAL_URL=<portal> PORTAL_API_KEY=<from-env> BENCH_SCHEMA=$BENCH_SCHEMA BENCH_PORT=$BENCH_PORT bash harness/bench/run-flagship.sh$([ "$SMOKE" = 1 ] && echo ' --smoke')"
MANIFEST_RC=$?

echo "▶ done. artifacts in $BENCH_OUT_DIR:"
echo "    bench.result.json   (wall time, per-chain blocks, rpc counts)"
echo "    bench.manifest.json (reproducibility)"
echo "    metrics.txt         (full /metrics snapshot)"

# A clean run WITHOUT its reproducibility manifest is not a valid, defensible bench — the manifest is
# the WHAT-ran record. So a manifest-emit failure fails the run even when the result was clean: exit
# nonzero if EITHER the result is not clean (RESULT_RC) OR manifest emission failed (MANIFEST_RC).
if [ "$MANIFEST_RC" -ne 0 ]; then
  echo "✗ emit-manifest failed (rc=$MANIFEST_RC) — the run has no reproducibility manifest; failing the run."
  exit "$MANIFEST_RC"
fi

# exit reflects a CLEAN run (complete + zero rpc errors). emit-result exits 0 clean, 1 not-clean.
exit "$RESULT_RC"
