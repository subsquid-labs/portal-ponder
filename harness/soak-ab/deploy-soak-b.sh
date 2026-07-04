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

# node is REQUIRED: the app this script deploys IS a node process, so the deploy target always has
# node. The two correctness-sensitive helpers (chain validation, env-carry) run through node; a
# silently-different degraded path (a narrower grep allowlist that drops operative vars and mangles
# `export ` lines) is worse than a clear error, so FAIL LOUD if node is missing rather than falling
# back. Checked before the DB or any file is created.
command -v node >/dev/null || {
  echo "✗ node not found on PATH — required to validate chains + carry the env file safely."
  echo "  Install node (the soak app itself is a node process) and re-run the deploy."
  exit 1
}

# Resolve + validate the chains knob to full EULER_CHAINS names NOW (fail loud before creating the DB
# or writing files). The pure resolver lives in deploy-helpers.mjs (node --test covered).
RUN_CHAINS="$(node "$HELPERS" resolve-chains "$RUN_CHAINS" "$CHAINS_JSON")" || {
  echo "✗ GUARDRAIL: invalid chains knob (SOAK_CHAINS/EULER_CHAINS) — see message above"; exit 1;
}
if grep -qiE 'DATABASE_URL=.*/euler(\b|["'\'' ])' "$SOAK_A_ENV" 2>/dev/null; then
  : # Soak A's env legitimately references its own DB; we DERIVE a new one below and never reuse it.
fi

echo "▶ Soak B deploy (BUILD-ONLY, will not start)"
echo "  db=$DB_NAME schema=$RUN_SCHEMA port=$PORT workdir=$WORKDIR user=$RUN_USER unit=$UNIT_DIR/soak-b.service"
echo "  chains=$RUN_CHAINS"

# ── 1. workspace: copy Soak A's app config, swap in the pinned tarball ──
mkdir -p "$WORKDIR"
if [ -d "$SOAK_A_DIR" ]; then
  # copy config only (ponder.config.ts, schema, abis, package.json) — NOT Soak A's node_modules / DB.
  # Track whether ANY expected config was actually copied: a source dir that exists but holds none of
  # the expected files would otherwise no-op silently and leave a half-provisioned workdir (only the
  # package.json warning below firing on a fresh deploy). Warn LOUD naming the source dir instead.
  COPIED_ANY=0
  for f in ponder.config.ts ponder.schema.ts package.json tsconfig.json; do
    if [ -f "$SOAK_A_DIR/$f" ]; then
      cp "$SOAK_A_DIR/$f" "$WORKDIR/"
      COPIED_ANY=1
    fi
  done
  if [ -d "$SOAK_A_DIR/abis" ]; then
    cp -r "$SOAK_A_DIR/abis" "$WORKDIR/"
    COPIED_ANY=1
  fi
  if [ -d "$SOAK_A_DIR/src" ]; then
    cp -r "$SOAK_A_DIR/src" "$WORKDIR/"
    COPIED_ANY=1
  fi
  if [ "$COPIED_ANY" != "1" ]; then
    echo "⚠ $SOAK_A_DIR exists but has none of the expected app config"
    echo "  (ponder.config.ts / ponder.schema.ts / package.json / tsconfig.json / abis / src) —"
    echo "  copy the Soak A app config into $WORKDIR before starting, or set SOAK_A_CONFIG_DIR."
  fi
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
  # Run the probe OUTSIDE the assignment and check psql's exit EXPLICITLY. The old form
  # `EXISTS="$(psql … 2>/dev/null | tr -d …)"` died silently under `set -euo pipefail`: when psql
  # cannot connect (no matching role / pg_hba / socket for the invoking user) the command-substitution
  # pipeline fails under pipefail, `set -e` aborts the whole deploy with a bare exit 2, and `2>/dev/null`
  # has swallowed the only diagnostic — nothing points at PGADMIN_URL. Now: capture psql's own exit,
  # keep its stderr, and fail LOUD naming PGADMIN_URL if the connection itself failed.
  PROBE_OUT="$(mktemp)"
  set +e
  psql "$PGADMIN_URL" -Xtqc "select 1 from pg_database where datname='${DB_NAME}'" >"$PROBE_OUT" 2>&1
  PROBE_RC=$?
  set -e
  # Consume the temp NOW (both branches) and delete it, so nothing downstream depends on the file
  # still existing. This also keeps the probe-failure `exit 1` the SOLE abort of the fail path: a
  # later read of a removed temp must never become a second, masking abort.
  PROBE_DIAG="$(cat "$PROBE_OUT")"
  EXISTS="$(printf '%s' "$PROBE_DIAG" | tr -d '[:space:]')"
  rm -f "$PROBE_OUT"
  if [ "$PROBE_RC" != "0" ]; then
    # NEVER echo PGADMIN_URL raw: it can carry admin credentials in its userinfo
    # (postgres://user:pw@host/db), and this script's own guardrail is "never print" secrets.
    # Redact the ENTIRE userinfo (the credential-bearing part) but keep scheme/host/db for provenance —
    # mirrors the redaction precedent in harness/validate/rpc-meter.mjs (redactTarget), which keeps
    # the endpoint identity and replaces only the credential. psql's own captured diagnostic
    # (PROBE_DIAG) does not contain the password, so it may keep printing verbatim below.
    #
    # The regex is ANCHORED to the scheme and consumes the whole authority up to the LAST `@` before
    # the first `/?#`, so: (a) a password containing a raw `@` (e.g. `p@ss`) is fully redacted, not
    # just up to its first `@`; and (b) a stray `://…@` inside the PATH/QUERY of a URL with no userinfo
    # is never rewritten (only the real authority is touched, and only when it actually has userinfo).
    PGADMIN_URL_SAFE="$(printf '%s' "$PGADMIN_URL" | sed -E 's#^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]*@#\1<redacted>@#')"
    echo "✗ could not connect to Postgres to check for database $DB_NAME."
    echo "  PGADMIN_URL=$PGADMIN_URL_SAFE — psql exited $PROBE_RC. Its diagnostic was:"
    printf '%s\n' "$PROBE_DIAG" | sed 's/^/    /'
    echo "  Fix PGADMIN_URL (role / pg_hba / socket) so the invoking user can reach an admin DB, or"
    echo "  create the $DB_NAME database manually and re-run."
    exit 1
  fi
  if [ "$EXISTS" != "1" ]; then
    echo "▶ creating database $DB_NAME"
    psql "$PGADMIN_URL" -Xqc "CREATE DATABASE ${DB_NAME}"
  else
    echo "  database $DB_NAME already exists"
  fi
else
  echo "⚠ psql not found — create the $DB_NAME database manually"
fi

# ── 2b. DATABASE_URL: derive from the source env's own DATABASE_URL, swapping ONLY the database ──
# The old code authored `postgresql:///${DB_NAME}` unconditionally — a peer-auth form that silently
# clobbers a role-authenticated TCP DATABASE_URL from the source env (the app then spins on DB-connect
# diagnostics while the unit sits `active`). Derive from the source URL with the node helper (real URL
# parser, never shell `${A%/*}/newdb` surgery that corrupts reserved-char passwords). Fall back to the
# peer-auth form ONLY when the source has no DATABASE_URL (helper exit 3); an UNPARSEABLE source URL is
# a loud abort (exit 1). Never printed — the derived URL is written into the chmod-600 env file only.
NEW_DATABASE_URL="postgresql:///${DB_NAME}"
if [ -f "$SOAK_A_ENV" ]; then
  set +e
  DERIVED="$(node "$HELPERS" derive-database-url "$SOAK_A_ENV" "$DB_NAME")"
  DERIVE_RC=$?
  set -e
  case "$DERIVE_RC" in
    0) NEW_DATABASE_URL="$DERIVED" ;;
    3) : ;; # source has no DATABASE_URL — keep the peer-auth fallback (do not print the URL)
    *)
      echo "✗ could not derive DATABASE_URL from the source env's DATABASE_URL (unparseable — fix it)"
      echo "  the source env file is: $SOAK_A_ENV"
      exit 1
      ;;
  esac
fi

# ── 3. secrets: derive a fresh chmod-600 env file from Soak A's (never print, never commit) ──
# Build into a temp file, then mv into place ONLY after the whole block succeeds. `{ … } > "$ENVFILE"`
# truncates $ENVFILE the instant the redirection opens, so a mid-block carry-env failure (the F3
# multi-line abort) under `set -e` would leave a partial/empty secrets file behind. The temp lives in
# $ENVFILE's own directory (same filesystem → atomic mv) and is created under umask 077, so it is
# never world-readable even transiently; it is cleaned up on any failure by the EXIT trap.
umask 077
ENVDIR="$(dirname "$ENVFILE")"
ENVFILE_TMP="$(mktemp "${ENVDIR}/.soak-b.env.XXXXXX")"
trap 'rm -f "$ENVFILE_TMP"' EXIT
{
  echo "# Soak B env — chmod 600, generated $(date -u +%FT%TZ). DO NOT COMMIT."
  # Carry the operative env vars from the source env file: PRESERVE-ALL-THEN-OVERRIDE. Every
  # well-formed KEY=value is carried EXCEPT the small set this script re-derives below (see
  # OVERRIDDEN_KEYS in deploy-helpers.mjs) — so PORTAL_URL (no underscore), EULER_CHAINS,
  # DATABASE_SCHEMA, NODE_OPTIONS, PORTAL_* tunables, keys and per-chain URLs are all preserved,
  # not silently dropped, while the vars we author below always win. Pure + node --test covered.
  #
  # node is required (guarded above). Passing the unit template makes carry-env ALSO exclude every key
  # the unit renders via Environment= (F1) — systemd EnvironmentFile= OVERRIDES Environment=, so a
  # stale carried copy would shadow the unit's fresh render — and fail loud on any multi-line/
  # unterminated-quote value (F3). No `|| true`: a carry failure must abort, not write a partial file.
  if [ -f "$SOAK_A_ENV" ]; then
    node "$HELPERS" carry-env "$SOAK_A_ENV" "$HERE/soak-b.service"
  fi
  # Authoritative overrides (these always win over any carried value; keep in sync with
  # OVERRIDDEN_KEYS in deploy-helpers.mjs).
  echo "DATABASE_URL=${NEW_DATABASE_URL}"
  echo "DATABASE_SCHEMA=${RUN_SCHEMA}"
  echo "EULER_CHAINS=${RUN_CHAINS}"
  echo "PORTAL_REALTIME=stream"
  echo "PORTAL_CHECKS=on"
} > "$ENVFILE_TMP"
chmod 600 "$ENVFILE_TMP"
mv -f "$ENVFILE_TMP" "$ENVFILE"
trap - EXIT
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
