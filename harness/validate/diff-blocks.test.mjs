import assert from 'node:assert/strict';
import { test } from 'node:test';
// diff.mjs lives in harness/diff/; this test sits in harness/validate/ so it runs under the existing
// CI test glob (harness/validate/*.test.mjs) without a workflow change.
import { blocksVerdict, setDiff } from '../diff/diff.mjs';

// Normalized block row-strings keyed by number. Same number, different hash = a reorg divergence
// that must FAIL; the OLD hash-keyed code hid it as two one-sided (portal-only + rpc-only) extras.
const block = (number, hash, extra = '') =>
  JSON.stringify({ hash, number, x: extra });

test('setDiff: identical row-sets pass; a one-sided row on either side fails', () => {
  const rows = ['r0', 'r1', 'r2'];
  assert.equal(setDiff(rows, rows.slice()).ok, true);

  const missB = setDiff(['r0', 'r1', 'r2'], ['r0', 'r2']);
  assert.equal(missB.ok, false);
  assert.deepEqual(missB.onlyA, ['r1']);

  const extraB = setDiff(['r0'], ['r0', 'r9']);
  assert.equal(extraB.ok, false);
  assert.deepEqual(extraB.onlyB, ['r9']);
});

test('blocksVerdict: same number / different hash is a MISMATCH → FAIL (not two one-sided extras)', () => {
  // This is the exact case the hash-keyed diff let through: block 100 has a different hash on each
  // side. Keyed by number it is a single shared MISMATCH; keyed by hash it looked like one portal-
  // only + one rpc-only block, which the old `ok` (shared-hash-mismatch only) never failed on.
  const portal = [block(100, '0xAAA'), block(101, '0xB')];
  const rpc = [block(100, '0xZZZ'), block(101, '0xB')];
  const v = blocksVerdict(portal, rpc);
  assert.equal(
    v.ok,
    false,
    'a same-number/different-hash reorg divergence must FAIL',
  );
  assert.deepEqual(v.mismatch, [100]);
  assert.equal(
    v.portalOnly.length,
    0,
    'the pair is counted once as shared, not as two extras',
  );
  assert.equal(v.rpcExtra.length, 0);
});

test('blocksVerdict: a portal-only block number FAILS', () => {
  const portal = [block(100, '0xA'), block(200, '0xInvented')];
  const rpc = [block(100, '0xA')];
  const v = blocksVerdict(portal, rpc);
  assert.equal(
    v.ok,
    false,
    'the Portal path inventing a block RPC never saw must FAIL',
  );
  assert.deepEqual(v.portalOnly, [200]);
});

test('blocksVerdict: an rpc-only inert event-less block is tolerated (reported, not failed)', () => {
  const portal = [block(100, '0xA'), block(102, '0xC')];
  const rpc = [block(100, '0xA'), block(101, '0xInert'), block(102, '0xC')];
  const v = blocksVerdict(portal, rpc);
  assert.equal(v.ok, true, 'an rpc-only traced event-less block does not fail');
  assert.deepEqual(v.rpcExtra, [101]);
  assert.equal(v.shared.length, 2);
});
