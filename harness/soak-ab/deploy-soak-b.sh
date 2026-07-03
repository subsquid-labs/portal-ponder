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

HERE="$(cd "$(dirname "$0")" && pwd)"

# ── GUARDRAILS ── absolute refusals, checked before anything is created ──
[ "$DB_NAME" = "euler_rt_b" ] || { echo "✗ GUARDRAIL: DB name must be euler_rt_b (got $DB_NAME)"; exit 1; }
[ "$PORT" != "9547" ] || { echo "✗ GUARDRAIL: :9547 is the euler prod port — refusing"; exit 1; }
case "$SOAK_A_ENV" in *euler-rt.env|*.env) : ;; *) echo "✗ SOAK_A_ENV must be an .env file"; exit 1 ;; esac
if grep -qiE 'DATABASE_URL=.*/euler(\b|["'\'' ])' "$SOAK_A_ENV" 2>/dev/null; then
  : # Soak A's env legitimately references its own DB; we DERIVE a new one below and never reuse it.
fi

echo "▶ Soak B deploy (BUILD-ONLY, will not start)"
echo "  db=$DB_NAME port=$PORT workdir=$WORKDIR unit=$UNIT_DIR/soak-b.service"

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
  # carry only the secrets/tunables Soak B needs from Soak A's env
  if [ -f "$SOAK_A_ENV" ]; then
    grep -E '^(PORTAL_API_KEY|SQD_RPC_KEY|PORTAL_URL_|PONDER_RPC_URL_)' "$SOAK_A_ENV" || true
  fi
  echo "DATABASE_URL=postgresql:///${DB_NAME}"
  echo "PORTAL_REALTIME=stream"
  echo "PORTAL_CHECKS=on"
} > "$ENVFILE"
chmod 600 "$ENVFILE"
echo "  wrote $ENVFILE (chmod 600)"

# ── 4. render + install the unit; enable but DO NOT start ──
RENDER="$(mktemp)"
sed -e "s#@@WORKDIR@@#${WORKDIR}#g" \
    -e "s#@@ENVFILE@@#${ENVFILE}#g" \
    -e "s#@@PORT@@#${PORT}#g" \
    -e "s#@@MEM_HIGH@@#${MEM_HIGH}#g" \
    -e "s#@@MEM_MAX@@#${MEM_MAX}#g" \
    "$HERE/soak-b.service" > "$RENDER"

if [ -w "$UNIT_DIR" ] || [ "$(id -u)" = 0 ]; then
  cp "$RENDER" "$UNIT_DIR/soak-b.service"
  systemctl daemon-reload
  systemctl enable soak-b.service
  echo "✅ installed + enabled soak-b.service (NOT started)."
  echo "   start it deliberately with:  sudo systemctl start soak-b.service"
else
  echo "▶ rendered unit (install manually — no write access to $UNIT_DIR):"
  echo "   sudo cp $RENDER $UNIT_DIR/soak-b.service && sudo systemctl daemon-reload && sudo systemctl enable soak-b.service"
fi
rm -f "$RENDER"
echo "▶ Soak B provisioned. Start manually; then run harness/soak-ab/ab-diff.mjs hourly."
