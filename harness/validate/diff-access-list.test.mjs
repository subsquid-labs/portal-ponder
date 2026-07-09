import assert from 'node:assert/strict';
import { test } from 'node:test';
// The access_list column-gap tolerance class (issues #83/#32) lives in BOTH differ paths: the cell
// differ harness/diff/diff.mjs (transactionsVerdict) and the paged default harness/validate/
// diff-batched.mjs (streamingDiff, mode:'transactions'). This file pins the tolerance, its regression
// sentinel, its chain scope, and its column scope on both — and asserts the two agree (route-a parity).
//
// The SQD Portal drops transactions.access_list on base-mainnet (8453), arbitrum-one (42161), and
// avalanche-mainnet (43114). Our fork stores an HONEST SQL NULL there (#110/#111 — never a fabricated
// "[]"); the stock-RPC leg stores the real list. So a `transactions` row on those chains can differ on
// access_list alone. We tolerate ONLY Portal-IS-NULL — a non-NULL Portal value (incl. a reappearing
// "[]", the #110 defect) still FAILS.
import {
  ACCESS_LIST_GAP_CHAINS as MJS_CHAINS,
  norm,
  transactionsVerdict,
} from '../diff/diff.mjs';
import {
  ACCESS_LIST_GAP_CHAINS as BATCHED_CHAINS,
  cmpKey,
  mergeCompare,
  TABLES,
} from './diff-batched.mjs';

const BASE = 8453;
const ARB = 42161;
const AVAX = 43114;
const ETH = 1; // out-of-scope: the Portal SERVES access_list on eth-mainnet

// A transactions row. access_list defaults to a populated list; pass null for the Portal dropped-column
// value, or "[]" for the fabricated #110 shape. Extra columns let us add a SECOND diff.
const tx = (
  bn,
  ti,
  chainId,
  accessList = '[{"address":"0xabc"}]',
  extra = {},
) => ({
  chain_id: chainId,
  block_number: bn,
  transaction_index: ti,
  hash: `0x${bn}${ti}`,
  access_list: accessList,
  ...extra,
});

// ── diff.mjs (cell differ) via transactionsVerdict ────────────────────────────────────────────────
// transactionsVerdict keys by (chain_id, block_number, transaction_index) over norm() row-strings.
const mjsVerdict = (portalRows, rpcRows) => {
  const a = portalRows.map((r) => norm(r)).sort();
  const b = rpcRows.map((r) => norm(r)).sort();

  return transactionsVerdict(a, b);
};

// ── diff-batched.mjs (paged default) via streaming merge in mode:'transactions' ──────────────────
const txKey = (r) => [
  BigInt(r.chain_id),
  BigInt(r.block_number),
  BigInt(r.transaction_index),
];
const byKey = (keyFn) => (x, y) => cmpKey(keyFn(x), keyFn(y));
const batchedVerdict = (portalRows, rpcRows) =>
  mergeCompare(
    portalRows.slice().sort(byKey(txKey)),
    rpcRows.slice().sort(byKey(txKey)),
    { keyFn: txKey, mode: 'transactions' },
  ); // returns a Promise — callers await it

// ── (1) TOLERANCE WORKS (mutation-distinguishing) ────────────────────────────────────────────────
// On an in-scope chain, a shared tx where Portal access_list=NULL and RPC=populated is NOT a mismatch.
// MUTATION (origin/main): transactions ran as a pure set-identity STRICT table (no tolerance) — a
// NULL-vs-populated row surfaced as one portal-only + one rpc-only extra and FAILED. With the fix it is
// one shared row, tolerated, ok=true.
for (const chain of [BASE, ARB, AVAX]) {
  test(`tolerance: Portal-NULL vs RPC-populated access_list on chain ${chain} is TOLERATED (not a mismatch)`, async () => {
    const portal = [tx(100, 0, chain, null)];
    const rpc = [tx(100, 0, chain, '[{"address":"0xabc"}]')];

    const v = mjsVerdict(portal, rpc);
    assert.equal(v.ok, true, 'diff.mjs: tolerated → ok');
    assert.equal(v.accessListTolerated.length, 1);
    assert.equal(v.mismatch.length, 0, 'not counted as a mismatch');
    assert.equal(
      v.portalOnly.length,
      0,
      'resolved as one shared row, not a portal-only extra',
    );
    assert.equal(
      v.rpcOnly.length,
      0,
      'resolved as one shared row, not an rpc-only extra',
    );

    const b = await batchedVerdict(portal, rpc);
    assert.equal(b.fail, false, 'diff-batched: tolerated → no fail');
    assert.equal(b.accessListTolerated, 1);
    assert.equal(b.mismatch, 0);
    assert.equal(b.onlyA, 0);
    assert.equal(b.onlyB, 0);
  });
}

test('tolerance: RPC side may be "[]" or any value — only Portal-IS-NULL matters', async () => {
  // Portal NULL, RPC an empty-list literal "[]" (a legit RPC representation) → still tolerated.
  const portal = [tx(100, 0, BASE, null)];
  const rpc = [tx(100, 0, BASE, '[]')];

  assert.equal(mjsVerdict(portal, rpc).ok, true);
  assert.equal(mjsVerdict(portal, rpc).accessListTolerated.length, 1);

  const b = await batchedVerdict(portal, rpc);
  assert.equal(b.fail, false);
  assert.equal(b.accessListTolerated, 1);
});

// ── (2) REGRESSION SENTINEL — the #110 fork defect must STILL FAIL ────────────────────────────────
// Portal access_list="[]" (NON-NULL, the exact #110 fabricated shape) vs RPC-populated on an in-scope
// chain → a real MISMATCH. Only Portal-IS-NULL is tolerated; two differing NON-NULL values never are.
test('sentinel: Portal "[]" (non-NULL, #110 defect) vs RPC-populated STILL FAILS on an in-scope chain', async () => {
  const portal = [tx(100, 0, ARB, '[]')];
  const rpc = [tx(100, 0, ARB, '[{"address":"0xabc"}]')];

  const v = mjsVerdict(portal, rpc);
  assert.equal(
    v.ok,
    false,
    'diff.mjs: a reappearing "[]" is a real divergence → FAIL',
  );
  assert.equal(v.mismatch.length, 1);
  assert.equal(
    v.accessListTolerated.length,
    0,
    'a non-NULL Portal value is NEVER tolerated',
  );

  const b = await batchedVerdict(portal, rpc);
  assert.equal(b.fail, true);
  assert.equal(b.mismatch, 1);
  assert.equal(b.accessListTolerated, 0);
});

test('sentinel: two differing NON-NULL access_list values are never tolerated', async () => {
  // Two concrete-but-different lists on an in-scope chain — a genuine data divergence, not the gap.
  const portal = [tx(100, 0, BASE, '[{"address":"0xAAA"}]')];
  const rpc = [tx(100, 0, BASE, '[{"address":"0xBBB"}]')];

  assert.equal(mjsVerdict(portal, rpc).ok, false);
  assert.equal(mjsVerdict(portal, rpc).mismatch.length, 1);

  const b = await batchedVerdict(portal, rpc);
  assert.equal(b.fail, true);
});

// ── (3) CHAIN SCOPE GUARD — the tolerance must not leak to chains that DO serve the column ─────────
// The same Portal-NULL-vs-populated shape on eth-mainnet (which serves access_list) is NOT tolerated.
test('scope: Portal-NULL vs RPC-populated on OUT-of-scope eth-mainnet (1) still FAILS', async () => {
  const portal = [tx(100, 0, ETH, null)];
  const rpc = [tx(100, 0, ETH, '[{"address":"0xabc"}]')];

  const v = mjsVerdict(portal, rpc);
  assert.equal(
    v.ok,
    false,
    'diff.mjs: eth serves the column — a NULL there is a real divergence',
  );
  assert.equal(v.mismatch.length, 1);
  assert.equal(v.accessListTolerated.length, 0);

  const b = await batchedVerdict(portal, rpc);
  assert.equal(b.fail, true);
  assert.equal(b.accessListTolerated, 0);
});

test('scope: ACCESS_LIST_GAP_CHAINS is exactly {base, arbitrum, avalanche} and both differs agree', () => {
  assert.deepEqual(
    [...MJS_CHAINS].sort((x, y) => x - y),
    [BASE, ARB, AVAX].sort((x, y) => x - y),
  );
  assert.deepEqual(
    [...MJS_CHAINS].sort((x, y) => x - y),
    [...BATCHED_CHAINS].sort((x, y) => x - y),
    'both differ paths must scope the tolerance to the same chains',
  );
  // eth-mainnet must not be in scope.
  assert.equal(MJS_CHAINS.has(ETH), false);
});

// ── (4) COLUMN SCOPE — a second differing column is NOT tolerated (column-scoped, not row-scoped) ──
// access_list differs (Portal NULL) AND a second column (nonce) differs → the row is a MISMATCH: the
// tolerance covers ONLY the lone access_list gap, never a co-occurring real divergence.
test('column-scope: access_list-NULL AND a second differing column STILL FAILS (in-scope chain)', async () => {
  const portal = [tx(100, 0, BASE, null, { nonce: '7' })];
  const rpc = [tx(100, 0, BASE, '[{"address":"0xabc"}]', { nonce: '9' })];

  const v = mjsVerdict(portal, rpc);
  assert.equal(
    v.ok,
    false,
    'a co-occurring second-column diff is not masked by the access_list gap',
  );
  assert.equal(v.mismatch.length, 1);
  assert.equal(v.accessListTolerated.length, 0);

  const b = await batchedVerdict(portal, rpc);
  assert.equal(b.fail, true);
  assert.equal(b.mismatch, 1);
  assert.equal(b.accessListTolerated, 0);
});

// ── one-sided rows stay STRICT (transactions is a required byte-identity table) ───────────────────
test('strictness: a portal-only OR rpc-only tx FAILS under the transactions tolerance', async () => {
  const shared = tx(100, 0, BASE, null);
  const extra = tx(100, 1, BASE, '[{"address":"0xabc"}]');

  // portal-only extra tx
  assert.equal(mjsVerdict([shared, extra], [shared]).ok, false);
  assert.equal((await batchedVerdict([shared, extra], [shared])).fail, true);

  // rpc-only extra tx
  assert.equal(mjsVerdict([shared], [shared, extra]).ok, false);
  assert.equal((await batchedVerdict([shared], [shared, extra])).fail, true);
});

// ── clean stores are unaffected: identical txs pass with zero tolerated ───────────────────────────
test('baseline: byte-identical transactions pass with nothing tolerated (both differs)', async () => {
  const rows = [tx(100, 0, BASE), tx(100, 1, BASE), tx(101, 0, BASE)];

  const v = mjsVerdict(rows, rows.slice());
  assert.equal(v.ok, true);
  assert.equal(v.accessListTolerated.length, 0);

  const b = await batchedVerdict(rows, rows.slice());
  assert.equal(b.fail, false);
  assert.equal(b.accessListTolerated, 0);
});

// ── route-a parity: the transactions table is still covered by both differ paths ──────────────────
test('parity: diff-batched TABLES.transactions uses the transactions mode (scoped tolerance path)', () => {
  assert.equal(
    TABLES.transactions.mode,
    'transactions',
    'the paged default must route transactions through the access_list-tolerant mode',
  );
});
