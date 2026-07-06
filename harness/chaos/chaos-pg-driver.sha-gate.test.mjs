import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

// These tests pin the tarball-sha256 gate in chaos-pg-driver.sh's preflight (issue #52). The bug they
// lock out: the sanitized driver defaulted the pin to the sha of the tarball ITSELF and only enforced
// when CHAOS_TARBALL_SHA was set — so an unset run "verified" a hash against itself (always a pass)
// and logged a ✓, hiding that nothing was pinned. The ops campaign that produced the recorded
// acceptance numbers verified the sha against a pin on EVERY launch. The gate is now fail-closed:
//   - CHAOS_TARBALL_SHA set + mismatch  → loud ABORT (nonzero exit, MISMATCH log)
//   - CHAOS_TARBALL_SHA set + match      → "pinned+verified"
//   - CHAOS_TARBALL_SHA unset            → loud UNPINNED warning (never a bare ✓ pass)
//
// The test drives the REAL driver into preflight with fixtures that satisfy the file checks, and stubs
// CHAOS_PGCTL so preflight halts right after the sha gate (before touching any Postgres cluster). We
// assert on the sha-gate log lines / exit code, which are emitted before the pg ensure.

const HERE = dirname(fileURLToPath(import.meta.url));
const DRIVER = join(HERE, 'chaos-pg-driver.sh');

// sha256 of the empty string — a stable, known-wrong pin for the mismatch case.
const SHA_OF_EMPTY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

function runPreflight(extraEnv, tarballBytes = 'not empty') {
  const work = mkdtempSync(join(tmpdir(), 'chaos-sha-'));
  const tarball = join(work, 'ponder.tgz');
  writeFileSync(tarball, tarballBytes);

  // minimal fixtures for the pre-sha existence checks
  const app = join(work, 'app');
  const baselineMeta = join(work, 'baseline.meta.json');
  writeFileSync(baselineMeta, '{}');

  // stub pg-ctl: any pg operation halts preflight loudly AFTER the sha gate.
  const pgctlStub = join(work, 'pgctl-stub.sh');
  writeFileSync(pgctlStub, '#!/usr/bin/env bash\necho "STUB-PGCTL-REACHED"\nexit 1\n');
  chmodSync(pgctlStub, 0o755);

  const env = {
    ...process.env,
    SQD_PONDER_TARBALL: tarball,
    CHAOS_APP: app,
    CHAOS_WORK: work,
    CHAOS_PGCTL: pgctlStub,
    CHAOS_BASELINE_META: baselineMeta,
    // the .mjs tools + verify script default to the committed files next to the driver
    ...extraEnv,
  };

  // create the app dir (preflight checks `-d "$APP"`)
  spawnSync('mkdir', ['-p', app]);

  const res = spawnSync('bash', [DRIVER, 'campaign'], {
    env,
    encoding: 'utf8',
    timeout: 60000,
  });

  rmSync(work, { recursive: true, force: true });

  return { ...res, out: `${res.stdout || ''}${res.stderr || ''}` };
}

test('sha gate: CHAOS_TARBALL_SHA set + MISMATCH → loud abort, never reaches pg-ctl', () => {
  const { out, status } = runPreflight({ CHAOS_TARBALL_SHA: SHA_OF_EMPTY }, 'definitely not empty');

  assert.match(out, /tarball sha256 MISMATCH/i, 'a set-but-wrong pin must log MISMATCH');
  assert.doesNotMatch(
    out,
    /STUB-PGCTL-REACHED/,
    'preflight must abort at the sha gate, before ensuring the pg cluster',
  );
  assert.notEqual(status, 0, 'a sha mismatch must exit nonzero (fail-closed)');
});

test('sha gate: CHAOS_TARBALL_SHA set + MATCH → verified, proceeds past the gate', () => {
  // pin to the empty-string sha and feed an empty tarball so they match.
  const { out } = runPreflight({ CHAOS_TARBALL_SHA: SHA_OF_EMPTY }, '');

  assert.match(out, /pinned\+verified/i, 'a matching pin must be reported as pinned+verified');
  assert.doesNotMatch(out, /MISMATCH/i, 'a matching pin must not log a mismatch');
  // with a good pin the gate passes and preflight advances to the pg-ctl stub.
  assert.match(out, /STUB-PGCTL-REACHED/, 'a verified sha must let preflight continue');
});

test('sha gate: CHAOS_TARBALL_SHA UNSET → loud UNPINNED warning, never a bare pass', () => {
  const { out } = runPreflight({}, 'some tarball bytes');

  assert.match(out, /UNPINNED/i, 'an unset pin must warn loudly that the run is unpinned');
  assert.doesNotMatch(
    out,
    /sha256 (pinned\+verified|verified)\b/i,
    'an unset pin must NOT claim the sha was verified (the old self-comparison false pass)',
  );
});
