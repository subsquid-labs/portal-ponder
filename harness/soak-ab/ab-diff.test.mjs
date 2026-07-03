import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  checkpointMonotonic,
  classifyTxDiff,
  compareBucketHashes,
  psqlExitVerdict,
  restartStats,
} from './ab-diff.mjs';

test('classifyTxDiff: B missing A parent txs, all log-referenced → expected class (PASS)', () => {
  const onlyA = ['0xaa', '0xbb']; // A has these, B (realtime stream) does not
  const onlyB = [];
  const referenced = new Set(['0xaa', '0xbb']); // both are parent txs of A-side logs
  const r = classifyTxDiff(onlyA, onlyB, referenced);
  assert.equal(r.fail, false);
  assert.equal(r.class, 'realtime-parent-tx-gap');
  assert.equal(r.expectedMissing, 2);
});

test('classifyTxDiff: any B-extra tx → FAIL', () => {
  const r = classifyTxDiff(['0xaa'], ['0xff'], new Set(['0xaa']));
  assert.equal(r.fail, true);
  assert.deepEqual(r.unexpectedB, ['0xff']);
});

test('classifyTxDiff: an A-only tx no log references → FAIL', () => {
  const r = classifyTxDiff(['0xaa', '0xorphan'], [], new Set(['0xaa']));
  assert.equal(r.fail, true);
  assert.deepEqual(r.unreferencedA, ['0xorphan']);
});

test('classifyTxDiff: perfect identity (no diff at all) → PASS', () => {
  const r = classifyTxDiff([], [], new Set());
  assert.equal(r.fail, false);
  assert.equal(r.expectedMissing, 0);
  assert.equal(r.sharedMismatch, 0);
});

test('classifyTxDiff: a SHARED tx whose full-row fields diverge → FAIL', () => {
  // both stores have the tx (no onlyA/onlyB), but its normalized full-row hash differs on one field
  // — the same finalized transaction is NOT byte-identical, which must FAIL. The old code selected
  // only "hash", so a shared tx with a divergent field silently passed.
  const r = classifyTxDiff([], [], new Set(), 1);
  assert.equal(r.fail, true);
  assert.equal(r.sharedMismatch, 1);
  assert.equal(r.class, 'UNEXPECTED');

  // zero shared mismatches with an otherwise-clean diff still passes (the expected realtime gap)
  const ok = classifyTxDiff(['0xaa'], [], new Set(['0xaa']), 0);
  assert.equal(ok.fail, false);
});

test('checkpointMonotonic: non-decreasing passes, a regression fails at the point', () => {
  assert.deepEqual(checkpointMonotonic([1n, 1n, 5n, 9n]), { ok: true });
  assert.deepEqual(checkpointMonotonic(['10', '20', '20', '30']), { ok: true });
  const bad = checkpointMonotonic([100n, 250n, 240n]);
  assert.equal(bad.ok, false);
  assert.equal(bad.at, 2);
});

test('psqlExitVerdict: a clean exit passes; ANY non-clean exit fails (no silent zero-rows)', () => {
  // the ONLY passing case: exit 0, no signal, no spawn error
  assert.deepEqual(
    psqlExitVerdict({ code: 0, signal: null, spawnError: null }),
    {
      ok: true,
    },
  );

  // a non-zero exit (bad SQL / connection refused / auth failure) must FAIL, never read as zero rows
  const nonzero = psqlExitVerdict({ code: 2, signal: null, spawnError: null });
  assert.equal(nonzero.ok, false);
  assert.match(nonzero.reason, /exited 2/);

  // killed by signal → fail
  const killed = psqlExitVerdict({
    code: null,
    signal: 'SIGKILL',
    spawnError: null,
  });
  assert.equal(killed.ok, false);
  assert.match(killed.reason, /SIGKILL/);

  // spawn failure (psql not on PATH) → fail, takes precedence
  const noBin = psqlExitVerdict({
    code: null,
    signal: null,
    spawnError: 'spawn psql ENOENT',
  });
  assert.equal(noBin.ok, false);
  assert.match(noBin.reason, /ENOENT/);
});

test('compareBucketHashes: shared buckets must match; one-sided buckets are reported not failed', () => {
  const a = new Map([
    ['0', 'h0'],
    ['1', 'h1'],
    ['2', 'h2'],
  ]);
  const good = new Map([
    ['0', 'h0'],
    ['1', 'h1'],
    ['3', 'hEdge'], // B has an extra edge bucket A hasn't reached
  ]);
  const okRes = compareBucketHashes(a, good);
  assert.equal(okRes.ok, true, 'edge-only buckets do not fail');
  assert.equal(okRes.onlyA, 1); // bucket 2
  assert.equal(okRes.onlyB, 1); // bucket 3

  const drift = new Map([
    ['0', 'h0'],
    ['1', 'DIFFERENT'],
  ]);
  const badRes = compareBucketHashes(a, drift);
  assert.equal(badRes.ok, false);
  assert.equal(badRes.mismatches.length, 1);
  assert.equal(badRes.mismatches[0].bucket, '1');
});

test('restartStats: counts restarts, reports last, flags crash-loop only above 3/hour', () => {
  const now = Date.parse('2026-07-03T12:00:00Z');
  // 2 restarts within the hour, 1 old → not a crash-loop
  const calm = restartStats(
    [
      '2026-07-03T09:00:00Z restart',
      '2026-07-03T11:30:00Z restart',
      '2026-07-03T11:55:00Z restart',
      '', // trailing blank line from the log split
    ],
    now,
  );
  assert.equal(calm.restartCount, 3);
  assert.equal(calm.lastRestartAt, '2026-07-03T11:55:00.000Z');
  assert.equal(calm.restartsLastHour, 2);
  assert.equal(calm.crashLoop, false);

  // 4 restarts within the hour → crash-loop
  const loop = restartStats(
    [
      '2026-07-03T11:10:00Z restart',
      '2026-07-03T11:25:00Z restart',
      '2026-07-03T11:40:00Z restart',
      '2026-07-03T11:59:00Z restart',
    ],
    now,
  );
  assert.equal(loop.restartsLastHour, 4);
  assert.equal(loop.crashLoop, true);

  // empty / never-started
  const none = restartStats([], now);
  assert.deepEqual(none, {
    restartCount: 0,
    lastRestartAt: null,
    restartsLastHour: 0,
    crashLoop: false,
  });
});
