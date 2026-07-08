import assert from 'node:assert/strict';
import { test } from 'node:test';
// diff.mjs lives in harness/diff/; this test sits in harness/validate/ so it runs under the existing
// CI test glob (harness/validate/*.test.mjs) without a workflow change.
import { blocksVerdict, setDiff } from '../diff/diff.mjs';

// Normalized block row-strings keyed by number. Same number, different hash = a reorg divergence
// that must FAIL; the OLD hash-keyed code hid it as two one-sided (portal-only + rpc-only) extras.
// `chain_id` defaults to 1 so the existing single-chain tests are unchanged (blocks_pkey is
// (chain_id, number); blocksVerdict now keys by that composite — see the multi-chain test below).
const block = (number, hash, extra = '', chain_id = 1) =>
  JSON.stringify({ chain_id, hash, number, x: extra });

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
  assert.deepEqual(v.mismatch, ['1:100']); // keyed by (chain_id, number); default chain 1
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
  assert.deepEqual(v.portalOnly, ['1:200']); // keyed by (chain_id, number); default chain 1
});

test('blocksVerdict: an rpc-only inert event-less block is tolerated (reported, not failed)', () => {
  const portal = [block(100, '0xA'), block(102, '0xC')];
  const rpc = [block(100, '0xA'), block(101, '0xInert'), block(102, '0xC')];
  const v = blocksVerdict(portal, rpc);
  assert.equal(v.ok, true, 'an rpc-only traced event-less block does not fail');
  assert.deepEqual(v.rpcExtra, ['1:101']); // keyed by (chain_id, number); default chain 1
  assert.equal(v.shared.length, 2);
});

test('blocksVerdict: multi-chain — chain-1 differs at height 100 while chain-2 matches → FAIL (no conflation)', () => {
  // The false-pass the number-only key produced: in a multi-chain store two chains legitimately hold
  // block 100. Keyed by number alone, chain 1 (0xAAA vs 0xZZZ, a real divergence) and chain 2 (0xBBB
  // on both sides, matching) collapse into ONE Map slot — the last row inserted wins on each side.
  // With chain 2 inserted last, both maps retain the MATCHING chain-2 row and blocksVerdict returns
  // ok:true, hiding the chain-1 divergence. Keyed by (chain_id, number) both chains are compared
  // independently, so the chain-1 mismatch is caught.
  const portal = [block(100, '0xAAA', '', 1), block(100, '0xBBB', '', 2)];
  const rpc = [block(100, '0xZZZ', '', 1), block(100, '0xBBB', '', 2)];
  const v = blocksVerdict(portal, rpc);
  assert.equal(
    v.ok,
    false,
    'a per-chain divergence must FAIL even when a same-height sibling chain matches',
  );
  assert.deepEqual(
    v.mismatch,
    ['1:100'],
    'only the chain-1 block is a mismatch; the chain-2 block matches',
  );
  assert.equal(
    v.shared.length,
    2,
    'both (chain,100) pairs are shared, counted once each',
  );
});

// ── issues #76, #106: tolerated upstream block.size-only derivation artifact ─────────────────────
// `size` is a node-derived, non-consensus field NOT committed by the block hash. Two dataset
// signatures observed, both with an IDENTICAL hash: #76 (eth-mainnet, rpc === portal + 1 only at/above
// 65540) and #106 (BSC, pervasive portal === rpc + 1 below 65540 plus occasional large size-only
// deltas). blocksVerdict tolerates a shared block whose SOLE differing field is `size` — regardless of
// delta magnitude or sign — ANCHORED on both rows carrying a present, equal `hash`. Any SECOND
// differing field (including `hash` itself) still FAILS; a size-only diff with a missing/empty hash is
// not anchored and FAILS. A `size`-bearing block row: same (chain_id, number, hash) both sides so
// `size` is the SOLE diff by default.
const sizeBlock = (number, size, hash = '0xdeadbeef', chain_id = 1) =>
  JSON.stringify({ chain_id, hash, number, size });

// T-bsc-plus1 — the #106 BSC pervasive signature: portal === rpc + 1 (rpc = portal − 1), BELOW 65540,
// hash equal → tolerated. MUTATION: on origin/main's predicate (rpc === portal + 1 && rpc ≥ 65540) the
// sign is wrong AND it is sub-threshold, so this lands in `mismatch` not `sizeTolerated` and v.ok is
// false → this test FAILS on the pre-fix code.
test('blocksVerdict #106: a lone size-only diff below 65540 (BSC portal=rpc+1) is tolerated', () => {
  const portal = [sizeBlock(97964878, 33097)];
  const rpc = [sizeBlock(97964878, 33096)];
  const v = blocksVerdict(portal, rpc);
  assert.equal(
    v.ok,
    true,
    'a lone BSC size-only diff (hash equal) does not fail',
  );
  assert.deepEqual(v.sizeTolerated, ['1:97964878']);
  assert.equal(
    v.mismatch.length,
    0,
    'the tolerated row is not counted as a mismatch',
  );
  assert.equal(v.shared.length, 1);
});

// T-bsc-large — the #106 occasional large size-only delta, hash equal → tolerated regardless of
// magnitude. MUTATION: origin/main tolerates only rpc === portal + 1, so a 131265-wide delta lands in
// `mismatch` and v.ok is false → this test FAILS on the pre-fix code.
test('blocksVerdict #106: a lone LARGE size-only delta (hash equal) is tolerated', () => {
  const portal = [sizeBlock(97964870, 171807)];
  const rpc = [sizeBlock(97964870, 40542)];
  const v = blocksVerdict(portal, rpc);
  assert.equal(
    v.ok,
    true,
    'a lone large size-only delta (hash equal) does not fail',
  );
  assert.deepEqual(v.sizeTolerated, ['1:97964870']);
  assert.equal(v.mismatch.length, 0);
  assert.equal(v.shared.length, 1);
});

// T-eth76 — regression: the original #76 signature (rpc === portal + 1, ≥ 65540, hash equal) must
// still be tolerated after the generalization.
test('blocksVerdict #76: a lone size off-by-one at/above 65540 is still tolerated (regression)', () => {
  const portal = [sizeBlock(19963775, 66755)];
  const rpc = [sizeBlock(19963775, 66756)];
  const v = blocksVerdict(portal, rpc);
  assert.equal(v.ok, true, 'the original #76 off-by-one still does not fail');
  assert.deepEqual(v.sizeTolerated, ['1:19963775']);
  assert.equal(v.mismatch.length, 0);
  assert.equal(v.shared.length, 1);
});

// T-safety-2field — a second differing field defeats the tolerance even when size is a valid delta.
test('blocksVerdict: size differs AND a SECOND field (gas_used) differs still FAILS', () => {
  const portal = [
    JSON.stringify({
      chain_id: 1,
      gas_used: 100,
      hash: '0xa',
      number: 100,
      size: 66755,
    }),
  ];
  const rpc = [
    JSON.stringify({
      chain_id: 1,
      gas_used: 200,
      hash: '0xa',
      number: 100,
      size: 66756,
    }),
  ];
  const v = blocksVerdict(portal, rpc);
  assert.equal(
    v.ok,
    false,
    'a second differing field defeats the size tolerance',
  );
  assert.deepEqual(v.mismatch, ['1:100']);
  assert.equal(v.sizeTolerated.length, 0);
});

// T-safety-hash — the load-bearing anti-masking test: `hash` differs (a real reorg/wrong-block
// divergence) AND size differs → hash is the SECOND diff, so it is NEVER masked. This is the safety
// invariant the generalization rests on: hash-match ⟺ identical canonical block.
test('blocksVerdict: a consensus-field (hash) divergence alongside a size diff is NEVER masked', () => {
  const v = blocksVerdict(
    [sizeBlock(100, 66755, '0xAAA')],
    [sizeBlock(100, 66756, '0xZZZ')],
  );
  assert.equal(
    v.ok,
    false,
    'a differing hash is a real mismatch even alongside a size difference',
  );
  assert.deepEqual(v.mismatch, ['1:100']);
  assert.equal(v.sizeTolerated.length, 0);
});

// T-no-hash-anchor — defensive: a size-only diff with an empty hash on one side is NOT anchored (the
// hash-match safety proof does not hold) → NOT tolerated → mismatch.
test('blocksVerdict: a size-only diff with a missing/empty hash is NOT anchored → FAILS', () => {
  const portal = [sizeBlock(100, 33097, '')];
  const rpc = [sizeBlock(100, 33096, '')];
  const v = blocksVerdict(portal, rpc);
  assert.equal(
    v.ok,
    false,
    'an empty hash cannot anchor the size tolerance (no identical-block proof)',
  );
  assert.deepEqual(v.mismatch, ['1:100']);
  assert.equal(v.sizeTolerated.length, 0);
});

test('setDiff: multi-chain STRICT tables do not conflate — same-height rows differ per chain', () => {
  // STRICT tables (logs/transactions/receipts/traces) compare full normalized row-strings (which
  // include chain_id) as a set, so they never had the number-key conflation. This pins that: two
  // chains at the same block_number, chain-1 row differs, chain-2 identical → the differing chain-1
  // rows surface as one-sided extras and setDiff FAILS.
  const rowP1 = JSON.stringify({
    block_number: 100,
    chain_id: 1,
    log_index: 0,
    data: '0xAAA',
  });
  const rowR1 = JSON.stringify({
    block_number: 100,
    chain_id: 1,
    log_index: 0,
    data: '0xZZZ',
  });
  const rowShared2 = JSON.stringify({
    block_number: 100,
    chain_id: 2,
    log_index: 0,
    data: '0xBBB',
  });
  const portal = [rowP1, rowShared2];
  const rpc = [rowR1, rowShared2];
  const d = setDiff(portal, rpc);
  assert.equal(
    d.ok,
    false,
    'a per-chain row divergence in a STRICT table must FAIL even when a same-height chain matches',
  );
  assert.deepEqual(d.onlyA, [rowP1]);
  assert.deepEqual(d.onlyB, [rowR1]);
});
