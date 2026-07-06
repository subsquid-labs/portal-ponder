import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

// These tests pin the OBSERVED-STATE backend label derivation in chaos-pg-driver.sh (issue #60). The
// bug they lock out: the driver recorded the campaign backend as the LITERAL `postgres16-fsync-on` in
// aggregate metadata while the Postgres binaries are resolved UNPINNED (CHAOS_PGBIN / pg_config /
// PATH) — so a run against, say, PG 17 would be silently mislabeled, the exact attribution drift the
// harness prevents elsewhere (tarballSha256). The fix derives `postgres<major>-fsync-<on|off>` from
// the live cluster (preferred) or the resolved binary, and rejects a CHAOS_BACKEND_LABEL override
// whose major disagrees with the observed one (fail-closed, never a silent mislabel).
//
// We drive the REAL driver's `backend-label` subcommand (a production entrypoint that invokes the same
// derive_backend_label helper the campaign uses). With no live chaos cluster, derivation falls back to
// `pg_config --version` of the resolved binary — so the observed major is the toolchain's own major,
// which the test computes INDEPENDENTLY and asserts against (never hardcoded).

const HERE = dirname(fileURLToPath(import.meta.url));
const DRIVER = join(HERE, 'chaos-pg-driver.sh');
const PGCTL = join(HERE, 'pg-ctl-chaos.sh');

// Resolve a local Postgres bin dir (initdb/pg_ctl/psql). Mirrors pg-ctl-chaos.test.mjs's E2E guard:
// when no toolchain is present the live-cluster test skips rather than fails.
function resolvePgBin() {
  const candidates = ['/usr/lib/postgresql/16/bin', '/usr/local/pgsql/bin'];
  for (const c of candidates) {
    if (existsSync(join(c, 'pg_ctl'))) {
      return c;
    }
  }

  return null;
}

// Probe a free loopback TCP port (bind to 0, read the OS-assigned port, RELEASE it before returning).
// Hardcoding a port would collide when two `node --test` runs execute concurrently (issue #60).
function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.once('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close((err) => {
        if (err) {
          reject(err);

          return;
        }

        resolve(String(port));
      });
    });
  });
}

// The label subcommand does not need a real tarball or app dir, but the driver's top-level `${VAR:?}`
// guards do — supply throwaway values so dispatch reaches the subcommand.
function driverEnv(extra) {
  const work = mkdtempSync(join(tmpdir(), 'chaos-label-'));
  const tarball = join(work, 'ponder.tgz');
  writeFileSync(tarball, 'unused');
  const app = join(work, 'app');
  spawnSync('mkdir', ['-p', app]);

  const env = {
    ...process.env,
    SQD_PONDER_TARBALL: tarball,
    CHAOS_APP: app,
    CHAOS_WORK: work,
    ...extra,
  };
  // The unset-override case must actually exercise the no-override branch: a CHAOS_BACKEND_LABEL
  // leaking from the runner's environment would defeat that.
  if (!('CHAOS_BACKEND_LABEL' in extra)) {
    delete env.CHAOS_BACKEND_LABEL;
  }

  return { env, work };
}

function runLabel(extra = {}) {
  const { env, work } = driverEnv(extra);
  const res = spawnSync('bash', [DRIVER, 'backend-label'], {
    env,
    encoding: 'utf8',
    timeout: 60000,
  });
  rmSync(work, { recursive: true, force: true });

  return { ...res, out: `${res.stdout || ''}${res.stderr || ''}` };
}

// Independently observe the toolchain's Postgres major (the derivation's fallback source): resolve
// pg_config the same way the scripts do (CHAOS_PGBIN wins, else PATH) and read its --version. Returns
// an integer or null when no toolchain is present (then the derivation itself can't observe a major
// either, and those assertions are skipped).
function observedMajorFromBinary() {
  const bin = process.env.CHAOS_PGBIN
    ? join(process.env.CHAOS_PGBIN, 'pg_config')
    : 'pg_config';
  try {
    const v = execFileSync(bin, ['--version'], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const m = v.match(/(\d+)/);

    return m ? Number(m[1]) : null;
  } catch {
    return null;
  }
}

function pgConfigPresent() {
  const bin = process.env.CHAOS_PGBIN
    ? join(process.env.CHAOS_PGBIN, 'pg_config')
    : null;
  if (bin) {
    return existsSync(bin);
  }

  // pg_config on PATH?
  const res = spawnSync('pg_config', ['--version'], { stdio: 'ignore' });

  return res.status === 0;
}

test('backend label tracks the OBSERVED Postgres major (not a hardcoded 16)', (t) => {
  const major = observedMajorFromBinary();
  if (major == null) {
    t.skip(
      'no Postgres toolchain to observe a major — derivation cannot run here',
    );

    return;
  }

  const { out, status } = runLabel();
  assert.equal(
    status,
    0,
    `derivation must succeed with a toolchain present (out: ${out})`,
  );
  // The printed label's major must equal the INDEPENDENTLY observed major. A mutant that hardcodes
  // `postgres16-...` would fail this whenever the toolchain is not 16.
  const printed = out.trim().split('\n').pop().trim();
  assert.match(
    printed,
    /^postgres\d+-fsync-(on|off)$/,
    `label must be postgres<major>-fsync-<on|off>, got ${JSON.stringify(printed)}`,
  );
  const printedMajor = Number(printed.match(/^postgres(\d+)-/)[1]);
  assert.equal(
    printedMajor,
    major,
    `label major (${printedMajor}) must equal the observed toolchain major (${major}) — a literal would drift`,
  );
});

test('a CHAOS_BACKEND_LABEL override whose major MISMATCHES the observed major aborts nonzero', (t) => {
  const major = observedMajorFromBinary();
  if (major == null) {
    t.skip('no Postgres toolchain to observe a major');

    return;
  }

  // Pick a major that definitely differs from the observed one.
  const wrongMajor = major + 1;
  const { out, status } = runLabel({
    CHAOS_BACKEND_LABEL: `postgres${wrongMajor}-fsync-on`,
  });

  assert.notEqual(
    status,
    0,
    'a mismatched override major must exit nonzero (fail-closed)',
  );
  assert.match(
    out,
    /does NOT match the observed Postgres major/i,
    'the abort must name the mismatch loudly',
  );
  assert.doesNotMatch(
    out,
    new RegExp(`^postgres${wrongMajor}-fsync-on$`, 'm'),
    'a rejected override must NOT be printed as the label',
  );
});

test('a CHAOS_BACKEND_LABEL override whose major MATCHES the observed major is honoured verbatim', (t) => {
  const major = observedMajorFromBinary();
  if (major == null) {
    t.skip('no Postgres toolchain to observe a major');

    return;
  }

  // A matching-major override is accepted verbatim (the operator may encode the exact fsync they ran).
  const override = `postgres${major}-fsync-off`;
  const { out, status } = runLabel({ CHAOS_BACKEND_LABEL: override });

  assert.equal(
    status,
    0,
    `a matching-major override must be accepted (out: ${out})`,
  );
  const printed = out.trim().split('\n').pop().trim();
  assert.equal(
    printed,
    override,
    'a matching override must be recorded verbatim',
  );
});

test('a CHAOS_BACKEND_LABEL override with no postgres<major> component aborts nonzero', (t) => {
  if (!pgConfigPresent()) {
    t.skip('no Postgres toolchain to observe a major');

    return;
  }

  const { out, status } = runLabel({ CHAOS_BACKEND_LABEL: 'sqlite-fsync-on' });

  assert.notEqual(
    status,
    0,
    'an override with no postgres<major> must be rejected',
  );
  assert.match(
    out,
    /no postgres<major>/i,
    'the abort must explain the override lacks a validatable major',
  );
});

// HARDENING (issue #60): the override major extraction is ANCHORED (^postgres[0-9]+), so a label that
// merely CONTAINS `postgres<major>` as a substring — `xpostgres16-fsync-on`, `foo-postgres16-bar` —
// no longer smuggles a major through. Such a label has no leading postgres<major> component and must
// hit the same fail-closed abort as `sqlite-fsync-on`. (`postgres16-fsync-off`, which STARTS with a
// major, stays accepted verbatim — pinned by the matching-major test above.)
test('a CHAOS_BACKEND_LABEL whose postgres<major> is only a SUBSTRING (xpostgres16-...) aborts nonzero', (t) => {
  if (!pgConfigPresent()) {
    t.skip('no Postgres toolchain to observe a major');

    return;
  }

  const { out, status } = runLabel({
    CHAOS_BACKEND_LABEL: 'xpostgres16-fsync-on',
  });

  assert.notEqual(
    status,
    0,
    'a label whose postgres<major> is not leading (xpostgres16-...) must be rejected',
  );
  assert.match(
    out,
    /no postgres<major>/i,
    'the abort must explain the override lacks a leading, validatable major',
  );
  assert.doesNotMatch(
    out,
    /^xpostgres16-fsync-on$/m,
    'a rejected substring label must NOT be printed as the label',
  );
});

// Independently read the LIVE cluster's major from `show server_version_num` (MMmmpp), floor-divided by
// 10000 IN THE TEST — never via pg_config. This makes the assertion meaningful even where the live
// major equals the toolchain major: it pins the driver's PRIMARY (live-cluster) derivation path, whose
// `vnum / 10000` has zero coverage in the fallback-only tests above.
function liveMajorAndFsync(pgbin, sockDir, port) {
  const env = { ...process.env, PGHOST: sockDir, PGPORT: port };
  const vnum = execFileSync(
    join(pgbin, 'psql'),
    ['-U', 'postgres', '-Atqc', 'show server_version_num', 'postgres'],
    { env, encoding: 'utf8' },
  ).trim();
  const fsync = execFileSync(
    join(pgbin, 'psql'),
    ['-U', 'postgres', '-Atqc', 'show fsync', 'postgres'],
    { env, encoding: 'utf8' },
  ).trim();

  return { major: Math.floor(Number(vnum) / 10000), fsync };
}

test('backend label is derived from the LIVE cluster (show server_version_num), not pg_config (end-to-end)', async (t) => {
  const pgbin = resolvePgBin();
  if (!pgbin) {
    t.skip('no local Postgres toolchain — live-cluster label check skipped');

    return;
  }

  const work = mkdtempSync(join(tmpdir(), 'chaos-label-live-'));
  const port = await freePort();
  const sockDir = join(work, 'pgsock');
  // These env vars make BOTH pg-ctl-chaos.sh (ensure) and the driver's psql_ours target THIS throwaway
  // cluster: the driver defaults PGSOCK to $CHAOS_WORK/pgsock and PGPORT to $CHAOS_PGPORT, exactly where
  // pg-ctl-chaos.sh ensure brings the socket + listener up.
  const clusterEnv = {
    ...process.env,
    CHAOS_WORK: work,
    CHAOS_PGPORT: port,
    CHAOS_PGBIN: pgbin,
  };

  try {
    execFileSync('bash', [PGCTL, 'ensure'], {
      env: clusterEnv,
      stdio: 'pipe',
      timeout: 120000,
    });

    // Compute the expected components INDEPENDENTLY from the live server.
    const { major, fsync } = liveMajorAndFsync(pgbin, sockDir, port);
    const expected = `postgres${major}-fsync-${fsync}`;

    // Drive the REAL backend-label subcommand against the live cluster (throwaway tarball + app satisfy
    // the driver's top-level ${VAR:?} guards; CHAOS_BACKEND_LABEL must not leak from the runner env).
    const tarball = join(work, 'ponder.tgz');
    writeFileSync(tarball, 'unused');
    const app = join(work, 'app');
    spawnSync('mkdir', ['-p', app]);
    const labelEnv = {
      ...clusterEnv,
      SQD_PONDER_TARBALL: tarball,
      CHAOS_APP: app,
    };
    delete labelEnv.CHAOS_BACKEND_LABEL;

    const res = spawnSync('bash', [DRIVER, 'backend-label'], {
      env: labelEnv,
      encoding: 'utf8',
      timeout: 60000,
    });
    const out = `${res.stdout || ''}${res.stderr || ''}`;

    assert.equal(
      res.status,
      0,
      `derivation must succeed against a live cluster (out: ${out})`,
    );
    const printed = out.trim().split('\n').pop().trim();
    // The label MUST equal the value computed from the LIVE server. A mutant that changes the live
    // branch's `vnum / 10000` to `vnum / 100000` mislabels PG16 as postgres1 and fails exactly here.
    assert.equal(
      printed,
      expected,
      `label must match the live-derived value ${JSON.stringify(expected)}, got ${JSON.stringify(printed)}`,
    );
  } finally {
    try {
      execFileSync('bash', [PGCTL, 'stop'], {
        env: clusterEnv,
        stdio: 'pipe',
        timeout: 60000,
      });
    } catch {
      // best-effort teardown
    }
    rmSync(work, { recursive: true, force: true });
  }
});
