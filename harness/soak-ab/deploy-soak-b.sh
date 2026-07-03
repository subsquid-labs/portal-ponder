#!/usr/bin/env bash
# deploy-soak-b.sh — provision the Soak B systemd unit on the box. BUILD-ONLY here: it renders +
# installs + `enable`s the unit but never STARTS it (the operator does that deliberately). Takes the
# pinned fork tarball as its argument.
#
#   bash harness/soak-ab/deploy-soak-b.sh /path/to/subsquid-ponder-0.16.6-sqd.2.tgz
#
# GUARDRAILS (hard-refuse): the DB is euler_rt_b and NOTHING else; the port is not :9547; the euler
# prod DB is never touched. Secrets are read from an existing operator env file (SOAK_A_ENV, default
# ~/euler-flagship/euler-rt.env) and copied into a fresh chmod-600 env file — never printed, never
# committed.
set -euo pipefail

TARBALL="${1:?usage: deploy-soak-b.sh <fork-tarball.tgz>}"
[ -f "$TARBALL" ] || { echo "✗ tarball not found: $TARBALL"; exit 1; }

# ── configuration (override via env) ──
DB_NAME="euler_rt_b"
PORT="${SOAK_B_PORT:-9548}"
WORKDIR="${SOAK_B_WORKDIR:-$HOME/soak-b}"
ENVFILE="${SOAK_B_ENVFILE:-$WORKDIR/soak-b.env}"
SOAK_A_DIR="${SOAK_A_CONFIG_DIR:-$HOME/euler-flagship}"
SOAK_A_ENV="${SOAK_A_ENV:-$SOAK_A_DIR/euler-rt.env}"
PGADMIN_URL="${PGADMIN_URL:-postgres:///postgres}"
MEM_HIGH="${SOAK_B_MEM_HIGH:-6G}"
MEM_MAX="${SOAK_B_MEM_MAX:-8G}"
UNIT_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
RESTART_LOG="${SOAK_B_RESTART_LOG:-$HOME/soak-b-restarts.log}"
RESTART_LOG_DIR="$(dirname "$RESTART_LOG")"
# The app schema. Rendered into BOTH the env file (DATABASE_SCHEMA) and the unit's `ponder start
# --schema` from this single knob, so a redeploy over an existing soak on a different schema stays
# consistent instead of silently pointing the app at soak_b. Default: soak_b.
RUN_SCHEMA="${SOAK_B_SCHEMA:-soak_b}"
# The indexed chains. The app (harness/euler-multichain/ponder.config.ts) reads EULER_CHAINS with the
# FULL chains.json names. Accept either SOAK_CHAINS or EULER_CHAINS as the knob; resolve+validate to
# full names below (fail loud on any unknown name). Default: the Soak-A trio.
RUN_CHAINS="${EULER_CHAINS:-${SOAK_CHAINS:-ethereum,base,arbitrum}}"
# The unit runs UNPRIVILEGED. Default to the invoking operator's own account (resolved live), never
# root and never a name baked into the committed unit template.
RUN_USER="${SOAK_B_USER:-$(id -un)}"
RUN_GROUP="${SOAK_B_GROUP:-$(id -gn)}"

HERE="$(cd "$(dirname "$0")" && pwd)"
HELPERS="$HERE/deploy-helpers.mjs"
# chains.json is the app's single source of truth for valid chain names (the `name` field).
CHAINS_JSON="${SOAK_B_CHAINS_JSON:-$HERE/../euler-multichain/chains.json}"

# ── GUARDRAILS ── absolute refusals, checked before anything is created ──
[ "$DB_NAME" = "euler_rt_b" ] || { echo "✗ GUARDRAIL: DB name must be euler_rt_b (got $DB_NAME)"; exit 1; }
[ "$PORT" != "9547" ] || { echo "✗ GUARDRAIL: :9547 is the euler prod port — refusing"; exit 1; }
[ "$RUN_USER" != "root" ] || { echo "✗ GUARDRAIL: refusing to run the soak as root — set SOAK_B_USER"; exit 1; }
case "$RUN_SCHEMA" in
  [A-Za-z_]*) : ;;
  *) echo "✗ SOAK_B_SCHEMA is not a valid SQL identifier: $RUN_SCHEMA"; exit 1 ;;
esac
case "$RUN_SCHEMA" in *[!A-Za-z0-9_]*) echo "✗ SOAK_B_SCHEMA is not a valid SQL identifier: $RUN_SCHEMA"; exit 1 ;; esac
case "$SOAK_A_ENV" in *euler-rt.env|*.env) : ;; *) echo "✗ SOAK_A_ENV must be an .env file"; exit 1 ;; esac

# Resolve + validate the chains knob to full EULER_CHAINS names NOW (fail loud before creating the DB
# or writing files). The pure resolver lives in deploy-helpers.mjs (node --test covered).
if command -v node >/dev/null; then
  RUN_CHAINS="$(node "$HELPERS" resolve-chains "$RUN_CHAINS" "$CHAINS_JSON")" || {
    echo "✗ GUARDRAIL: invalid chains knob (SOAK_CHAINS/EULER_CHAINS) — see message above"; exit 1;
  }
else
  echo "⚠ node not found — cannot validate chain names; writing EULER_CHAINS=$RUN_CHAINS as given"
fi
if grep -qiE 'DATABASE_URL=.*/euler(\b|["'\'' ])' "$SOAK_A_ENV" 2>/dev/null; then
  : # Soak A's env legitimately references its own DB; we DERIVE a new one below and never reuse it.
fi

echo "▶ Soak B deploy (BUILD-ONLY, will not start)"
echo "  db=$DB_NAME schema=$RUN_SCHEMA port=$PORT workdir=$WORKDIR user=$RUN_USER unit=$UNIT_DIR/soak-b.service"
echo "  chains=$RUN_CHAINS"

# ── 1. workspace: copy Soak A's app config, swap in the pinned tarball ──
mkdir -p "$WORKDIR"
if [ -d "$SOAK_A_DIR" ]; then
  # copy config only (ponder.config.ts, schema, abis, package.json) — NOT Soak A's node_modules / DB
  for f in ponder.config.ts ponder.schema.ts package.json tsconfig.json; do
    [ -f "$SOAK_A_DIR/$f" ] && cp "$SOAK_A_DIR/$f" "$WORKDIR/"
  done
  [ -d "$SOAK_A_DIR/abis" ] && cp -r "$SOAK_A_DIR/abis" "$WORKDIR/"
  [ -d "$SOAK_A_DIR/src" ] && cp -r "$SOAK_A_DIR/src" "$WORKDIR/"
else
  echo "⚠ $SOAK_A_DIR not found — copy the Soak A app config into $WORKDIR before starting"
fi

if [ -f "$WORKDIR/package.json" ]; then
  ( cd "$WORKDIR"
    node -e "const p=require('./package.json');p.dependencies=p.dependencies||{};p.dependencies['@subsquid/ponder']='file:'+process.argv[1];require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))" "$TARBALL"
    npm install --no-audit --no-fund --silent --cache "$(mktemp -d)" )
else
  echo "⚠ no package.json in $WORKDIR — install @subsquid/ponder from $TARBALL manually"
fi

# ── 2. database: create euler_rt_b if absent (guarded) ──
if command -v psql >/dev/null; then
  EXISTS="$(psql "$PGADMIN_URL" -Xtqc "select 1 from pg_database where datname='${DB_NAME}'" 2>/dev/null | tr -d '[:space:]')"
  if [ "$EXISTS" != "1" ]; then
    echo "▶ creating database $DB_NAME"
    psql "$PGADMIN_URL" -Xqc "CREATE DATABASE ${DB_NAME}"
  else
    echo "  database $DB_NAME already exists"
  fi
else
  echo "⚠ psql not found — create the $DB_NAME database manually"
fi

# ── 3. secrets: derive a fresh chmod-600 env file from Soak A's (never print, never commit) ──
umask 077
{
  echo "# Soak B env — chmod 600, generated $(date -u +%FT%TZ). DO NOT COMMIT."
  # Carry the operative env vars from the source env file: PRESERVE-ALL-THEN-OVERRIDE. Every
  # well-formed KEY=value is carried EXCEPT the small set this script re-derives below (see
  # OVERRIDDEN_KEYS in deploy-helpers.mjs) — so PORTAL_URL (no underscore), EULER_CHAINS,
  # DATABASE_SCHEMA, NODE_OPTIONS, PORTAL_* tunables, keys and per-chain URLs are all preserved,
  # not silently dropped, while the vars we author below always win. Pure + node --test covered.
  if [ -f "$SOAK_A_ENV" ]; then
    if command -v node >/dev/null; then
      node "$HELPERS" carry-env "$SOAK_A_ENV" || true
    else
      # Fallback when node is unavailable: widened allowlist covering the operative variables.
      grep -E '^(export[[:space:]]+)?(PORTAL_API_KEY|SQD_RPC_KEY|PORTAL_URL|PONDER_RPC_URL_|EULER_CHAINS|DATABASE_SCHEMA|NODE_OPTIONS|PORTAL_)' "$SOAK_A_ENV" || true
    fi
  fi
  # Authoritative overrides (these always win over any carried value; keep in sync with
  # OVERRIDDEN_KEYS in deploy-helpers.mjs).
  echo "DATABASE_URL=postgresql:///${DB_NAME}"
  echo "DATABASE_SCHEMA=${RUN_SCHEMA}"
  echo "EULER_CHAINS=${RUN_CHAINS}"
  echo "PORTAL_REALTIME=stream"
  echo "PORTAL_CHECKS=on"
} > "$ENVFILE"
chmod 600 "$ENVFILE"
echo "  wrote $ENVFILE (chmod 600)"

# ── 4. render + install the unit; enable but DO NOT start ──
touch "$RESTART_LOG" 2>/dev/null || true
RENDER="$(mktemp)"
sed -e "s#@@WORKDIR@@#${WORKDIR}#g" \
    -e "s#@@ENVFILE@@#${ENVFILE}#g" \
    -e "s#@@PORT@@#${PORT}#g" \
    -e "s#@@SCHEMA@@#${RUN_SCHEMA}#g" \
    -e "s#@@USER@@#${RUN_USER}#g" \
    -e "s#@@GROUP@@#${RUN_GROUP}#g" \
    -e "s#@@MEM_HIGH@@#${MEM_HIGH}#g" \
    -e "s#@@MEM_MAX@@#${MEM_MAX}#g" \
    -e "s#@@RESTART_LOG@@#${RESTART_LOG}#g" \
    -e "s#@@RESTART_LOG_DIR@@#${RESTART_LOG_DIR}#g" \
    "$HERE/soak-b.service" > "$RENDER"

if [ -w "$UNIT_DIR" ] || [ "$(id -u)" = 0 ]; then
  cp "$RENDER" "$UNIT_DIR/soak-b.service"
  systemctl daemon-reload
  systemctl enable soak-b.service
  echo "✅ installed + enabled soak-b.service (NOT started)."
  echo "   start it deliberately with:  sudo systemctl start soak-b.service"
  # Only clean up the temp render once it has been installed. On the non-root path below the
  # operator still needs this file to copy into place, so it MUST outlive the script there.
  rm -f "$RENDER"
else
  echo "▶ rendered unit at $RENDER (install manually — no write access to $UNIT_DIR):"
  echo "   sudo cp $RENDER $UNIT_DIR/soak-b.service && sudo systemctl daemon-reload && sudo systemctl enable soak-b.service"
fi
echo "▶ Soak B provisioned. Start manually; then run harness/soak-ab/ab-diff.mjs hourly."
echo "   restart log: $RESTART_LOG (ab-diff.mjs reads restartCount/lastRestartAt; >3/h = crash-loop alert)"
