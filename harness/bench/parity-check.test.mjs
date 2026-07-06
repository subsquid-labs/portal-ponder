import assert from 'node:assert/strict';
import { test } from 'node:test';
import { compareSnapshots } from './parity-check.mjs';

// Build a snapshot in the shape snapshot() produces (a Map<chainId, {...}> + totals).
function snap(chains, totals) {
  const byChain = new Map();
  for (const c of chains) {
    byChain.set(c.chainId, c);
  }

  return { byChain, totals };
}

const CHAIN_1 = {
  chainId: '1',
  logCount: '100',
  minBlock: '10',
  maxBlock: '999',
  distinctTx: '42',
};
const CHAIN_137 = {
  chainId: '137',
  logCount: '5000',
  minBlock: '200',
  maxBlock: '8000',
  distinctTx: '1234',
};

test('compareSnapshots: identical stores PASS with every cell matching', () => {
  const bench = snap([CHAIN_1, CHAIN_137], {
    blocks: '9000',
    transactions: '4000',
  });
  const reference = snap([{ ...CHAIN_1 }, { ...CHAIN_137 }], {
    blocks: '9000',
    transactions: '4000',
  });

  const result = compareSnapshots(bench, reference);
  assert.equal(result.pass, true);
  assert.ok(
    result.rows.every((r) => r.match),
    'all per-chain cells match',
  );
  assert.ok(
    result.totals.every((r) => r.match),
    'both totals match',
  );
  // 2 chains × 4 fields = 8 per-chain cells; 2 totals
  assert.equal(result.rows.length, 8);
  assert.equal(result.totals.length, 2);
});

test('compareSnapshots: a single differing cell FAILs and is reported', () => {
  const bench = snap([CHAIN_1], { blocks: '9000', transactions: '4000' });
  const reference = snap([{ ...CHAIN_1, distinctTx: '41' }], {
    blocks: '9000',
    transactions: '4000',
  });

  const result = compareSnapshots(bench, reference);
  assert.equal(result.pass, false);
  const diff = result.rows.find((r) => !r.match);
  assert.equal(diff.field, 'distinctTx');
  assert.equal(diff.bench, '42');
  assert.equal(diff.reference, '41');
});

test('compareSnapshots: a chain present in ONLY one store is a mismatch on every cell', () => {
  const bench = snap([CHAIN_1, CHAIN_137], {
    blocks: '9000',
    transactions: '4000',
  });
  const reference = snap([CHAIN_1], { blocks: '9000', transactions: '4000' });

  const result = compareSnapshots(bench, reference);
  assert.equal(result.pass, false);
  const polyRows = result.rows.filter((r) => r.scope === 'logs[chain 137]');
  assert.equal(
    polyRows.length,
    4,
    'all four fields for the extra chain are compared',
  );
  assert.ok(
    polyRows.every((r) => !r.match && r.reference === '<absent>'),
    'the missing side is <absent> and every cell mismatches',
  );
});

test('compareSnapshots: a totals difference (blocks or transactions) FAILs', () => {
  const bench = snap([CHAIN_1], { blocks: '9000', transactions: '4000' });
  const reference = snap([CHAIN_1], { blocks: '8999', transactions: '4000' });

  const result = compareSnapshots(bench, reference);
  assert.equal(result.pass, false);
  const blocksTotal = result.totals.find((r) => r.scope === 'blocks total');
  assert.equal(blocksTotal.match, false);
  const txTotal = result.totals.find((r) => r.scope === 'transactions total');
  assert.equal(
    txTotal.match,
    true,
    'the matching total is not falsely flagged',
  );
});
