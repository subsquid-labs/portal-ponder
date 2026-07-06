import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

// These tests pin the CHAOS_PGPORT plumbing between pg-chaos.conf and pg-ctl-chaos.sh (issue #52).
// The bug they lock out: pg-chaos.conf used to hardcode `port = 54329`, so overriding CHAOS_PGPORT
// moved the port the driver + digest tools connect on WITHOUT moving the port the server listens on —
// every connection then silently failed. The conf now carries an @CHAOS_PGPORT@ placeholder that
// pg-ctl-chaos.sh's wire_config substitutes, keeping the server's listen port in lockstep with the
// override. We test the substitution contract (unit) and, when a Postgres 16 toolchain is present,
// that a throwaway cluster actually comes up on the overridden port (end-to-end).

const HERE = dirname(fileURLToPath(import.meta.url));
const CONF = join(HERE, 'pg-chaos.conf');

// Reproduce exactly the substitution pg-ctl-chaos.sh's wire_config applies to the conf template.
function renderConf(port, sock) {
  const raw = readFileSync(CONF, 'utf8');
  return raw
    .split('@CHAOS_PGPORT@')
    .join(String(port))
    .split('@CHAOS_PGSOCK@')
    .join(sock);
}

// The effective `port = ...` directive (a real config line, not a comment).
function effectivePort(rendered) {
  const line = rendered.split('\n').find((l) => /^\s*port\s*=/.test(l));

  return line
    ? line
        .replace(/^\s*port\s*=\s*/, '')
        .split('#')[0]
        .trim()
    : null;
}

function effectiveSocketDir(rendered) {
  const line = rendered
    .split('\n')
    .find((l) => /^\s*unix_socket_directories\s*=/.test(l));

  return line
    ? line
        .replace(/^\s*unix_socket_directories\s*=\s*/, '')
        .split('#')[0]
        .trim()
    : null;
}

test('pg-chaos.conf carries a substitutable port placeholder (not a hardcoded port)', () => {
  const raw = readFileSync(CONF, 'utf8');
  const portLine = raw.split('\n').find((l) => /^\s*port\s*=/.test(l));

  assert.ok(portLine, 'conf must have a `port = ...` directive');
  assert.match(
    portLine,
    /@CHAOS_PGPORT@/,
    'the port directive MUST be the @CHAOS_PGPORT@ placeholder — a hardcoded port desyncs the server from CHAOS_PGPORT',
  );
});

test('wire_config substitution renders the effective port from the override (not 54329)', () => {
  const rendered = renderConf(54999, '/tmp/whatever/sock');

  assert.equal(
    effectivePort(rendered),
    '54999',
    'the effective port must be the overridden value',
  );
  assert.equal(effectiveSocketDir(rendered), "'/tmp/whatever/sock'");
});

test('wire_config substitution leaves no @CHAOS_*@ residue on the effective directives', () => {
  const rendered = renderConf(54321, '/some/sock');
  const portLine = rendered.split('\n').find((l) => /^\s*port\s*=/.test(l));
  const sockLine = rendered
    .split('\n')
    .find((l) => /^\s*unix_socket_directories\s*=/.test(l));

  assert.doesNotMatch(
    portLine,
    /@CHAOS_/,
    'port directive still has an unsubstituted token',
  );
  assert.doesNotMatch(
    sockLine,
    /@CHAOS_/,
    'socket directive still has an unsubstituted token',
  );
});

test('the default port still resolves to 54329 when CHAOS_PGPORT is not overridden', () => {
  // pg-ctl-chaos.sh defaults PGPORT to 54329; the conf itself must NOT bake a port, so the default
  // flows from the script, not the template.
  const rendered = renderConf(54329, '/default/sock');

  assert.equal(effectivePort(rendered), '54329');
});

// End-to-end: only runs where a Postgres 16 toolchain is available. Brings up a throwaway cluster on
// an overridden CHAOS_PGPORT and asserts the server actually listens there. Skipped (not failed) when
// no local Postgres bin is present, so CI on a runner without Postgres stays green.
function resolvePgBin() {
  const candidates = ['/usr/lib/postgresql/16/bin', '/usr/local/pgsql/bin'];
  for (const c of candidates) {
    if (existsSync(join(c, 'pg_ctl'))) {
      return c;
    }
  }

  return null;
}

test('CHAOS_PGPORT override brings a throwaway cluster up on the overridden port (end-to-end)', (t) => {
  const pgbin = resolvePgBin();
  if (!pgbin) {
    t.skip('no local Postgres 16 toolchain — end-to-end port check skipped');

    return;
  }

  const work = mkdtempSync(join(tmpdir(), 'chaos-pgport-'));
  const port = '54987';
  const env = {
    ...process.env,
    CHAOS_WORK: work,
    CHAOS_PGPORT: port,
    CHAOS_PGBIN: pgbin,
  };
  const ctl = join(HERE, 'pg-ctl-chaos.sh');

  try {
    execFileSync('bash', [ctl, 'ensure'], {
      env,
      stdio: 'pipe',
      timeout: 120000,
    });

    const rendered = readFileSync(
      join(work, 'pgdata', 'pg-chaos.rendered.conf'),
      'utf8',
    );
    assert.equal(
      effectivePort(rendered),
      port,
      'rendered conf must carry the overridden port',
    );

    const ready = execFileSync(
      join(pgbin, 'pg_isready'),
      ['-U', 'postgres', '-q'],
      {
        env: { ...env, PGHOST: join(work, 'pgsock'), PGPORT: port },
        stdio: 'pipe',
      },
    );
    // pg_isready exits 0 (empty output) when accepting connections on the given port.
    assert.equal(ready.toString(), '');
  } finally {
    try {
      execFileSync('bash', [ctl, 'stop'], {
        env,
        stdio: 'pipe',
        timeout: 60000,
      });
    } catch {
      // best-effort teardown
    }
    rmSync(work, { recursive: true, force: true });
  }
});
