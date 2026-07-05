#!/usr/bin/env bash
# pg-ctl-chaos.sh — idempotent lifecycle for the native Postgres cluster backing the Postgres-tier
# chaos kill/resume campaign (issue #52). This cluster is the crash-DURABLE substrate: it is NEVER
# killed during the campaign; only the ponder app is. It is a throwaway, single-purpose cluster on a
# dedicated port — do NOT point it at a shared/system cluster.
#
#   pg-ctl-chaos.sh ensure    # initdb (if needed) + wire config + start (if not running); idempotent
#   pg-ctl-chaos.sh start     # start only (assumes initialized)
#   pg-ctl-chaos.sh stop      # fast graceful stop
#   pg-ctl-chaos.sh status    # exit 0 if accepting connections on $CHAOS_PGPORT
#   pg-ctl-chaos.sh restart   # stop + start (used by the digest-determinism proof)
#
# GUARDRAIL: operates ONLY on the datadir at $CHAOS_PGDATA (a throwaway dir under $CHAOS_WORK).
# Never point CHAOS_PGDATA/CHAOS_PGPORT at a system Postgres cluster (default :5432).
#
# Env (all defaulted; override to relocate off the defaults):
#   CHAOS_PGBIN   Postgres bin dir containing initdb/pg_ctl/pg_isready (default: on PATH via `pg_config`
#                 --bindir, else the dir the tools already resolve to)
#   CHAOS_WORK    workspace root for the throwaway cluster (default: ./.chaos-pg next to this script)
#   CHAOS_PGDATA  cluster datadir (default: $CHAOS_WORK/pgdata)
#   CHAOS_PGSOCK  unix socket dir (default: $CHAOS_WORK/pgsock)
#   CHAOS_PGLOG   cluster log file (default: $CHAOS_WORK/pg.log)
#   CHAOS_PGPORT  dedicated, verified-free TCP port (default: 54329)
#   CHAOS_PGCONF  managed config template (default: pg-chaos.conf next to this script)
set -uo pipefail

CDIR="$(cd "$(dirname "$0")" && pwd)"

# Resolve the Postgres bin dir: an explicit CHAOS_PGBIN wins; otherwise trust PATH (pg_config --bindir
# if available, else assume initdb/pg_ctl/pg_isready are on PATH and use bare names).
if [ -n "${CHAOS_PGBIN:-}" ]; then
  PGBIN="$CHAOS_PGBIN"
elif command -v pg_config >/dev/null 2>&1; then
  PGBIN="$(pg_config --bindir)"
else
  PGBIN=""   # empty ⇒ bare tool names resolved on PATH (see pgbin() below)
fi
# pgbin <tool>: absolute path when PGBIN is set, else the bare name (found on PATH).
pgbin () { if [ -n "$PGBIN" ]; then echo "$PGBIN/$1"; else echo "$1"; fi; }

WORK="${CHAOS_WORK:-$CDIR/.chaos-pg}"
PGDATA="${CHAOS_PGDATA:-$WORK/pgdata}"
PGPORT="${CHAOS_PGPORT:-54329}"
PGSOCK="${CHAOS_PGSOCK:-$WORK/pgsock}"
PGLOG="${CHAOS_PGLOG:-$WORK/pg.log}"
PGCONF="${CHAOS_PGCONF:-$CDIR/pg-chaos.conf}"
PSQL="${CHAOS_PSQL:-psql}"

log () { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [pg-ctl] $*"; }

# psql against our cluster over the unix socket (trust auth, superuser postgres).
psql_ours () {
  PGHOST="$PGSOCK" PGPORT="$PGPORT" "$PSQL" -U postgres -Atqc "$1" "${2:-postgres}"
}

is_up () {
  PGHOST="$PGSOCK" PGPORT="$PGPORT" "$(pgbin pg_isready)" -U postgres -q 2>/dev/null
}

wire_config () {
  # Render the managed config: substitute the @CHAOS_PGSOCK@ placeholder in the template with our
  # actual (absolute) socket dir, into a rendered file next to the datadir; then ensure
  # postgresql.conf includes it exactly once.
  local rendered="$PGDATA/pg-chaos.rendered.conf"
  sed "s#@CHAOS_PGSOCK@#$PGSOCK#g" "$PGCONF" > "$rendered"
  local inc="include = '$rendered'"
  if ! grep -qF "$inc" "$PGDATA/postgresql.conf" 2>/dev/null; then
    printf '\n# ── chaos-pg managed config (issue #52) ──\n%s\n' "$inc" >> "$PGDATA/postgresql.conf"
    log "wired managed config include into postgresql.conf"
  fi
}

do_init () {
  if [ -f "$PGDATA/PG_VERSION" ]; then
    return 0
  fi

  mkdir -p "$PGSOCK"
  log "initdb → $PGDATA (C locale, trust auth, fsync-on cluster)"
  "$(pgbin initdb)" -D "$PGDATA" -U postgres --auth-local=trust --auth-host=trust \
    --encoding=UTF8 --locale=C >"$WORK/initdb.log" 2>&1 \
    || { log "✗ initdb failed"; tail -20 "$WORK/initdb.log"; return 1; }
}

do_start () {
  if is_up; then
    log "cluster already accepting connections on :$PGPORT — start is a no-op"
    return 0
  fi

  mkdir -p "$PGSOCK"
  wire_config
  # -w waits for readiness; config is via the wired include, but pin the datadir explicitly.
  "$(pgbin pg_ctl)" -D "$PGDATA" -l "$PGLOG" -w -t 60 start \
    || { log "✗ pg_ctl start failed — tail cluster log:"; tail -20 "$PGLOG" 2>/dev/null; return 1; }
  # confirm it really came up on OUR port/socket (config could have been ignored).
  local i=0
  while [ "$i" -lt 30 ]; do
    if is_up; then
      log "✓ cluster up on :$PGPORT (socket $PGSOCK)"
      return 0
    fi
    sleep 1
    i=$(( i + 1 ))
  done
  log "✗ cluster did not become ready on :$PGPORT within 30s"; tail -20 "$PGLOG" 2>/dev/null
  return 1
}

do_stop () {
  if ! [ -f "$PGDATA/postmaster.pid" ]; then
    log "no postmaster.pid — cluster not running"
    return 0
  fi
  "$(pgbin pg_ctl)" -D "$PGDATA" -m fast -w -t 60 stop \
    || { log "✗ pg_ctl stop failed"; return 1; }
  log "✓ cluster stopped"
}

mkdir -p "$WORK"

case "${1:-ensure}" in
  ensure)
    do_init || exit 1
    do_start || exit 1
    ;;
  init)    do_init || exit 1 ;;
  start)   do_start || exit 1 ;;
  stop)    do_stop || exit 1 ;;
  restart) do_stop || exit 1; do_start || exit 1 ;;
  status)  is_up && { echo "up"; exit 0; } || { echo "down"; exit 1; } ;;
  *) echo "usage: pg-ctl-chaos.sh ensure|init|start|stop|restart|status"; exit 2 ;;
esac
