import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  appHashVerdict,
  cmpKey,
  hashRows,
  mergeCompare,
  normRow,
  resolveTableSpecs,
} from './diff-batched.mjs';

// key by (block_number, log_index) as the real logs table would be ordered
const logKey = (r) => [BigInt(r.block_number), BigInt(r.log_index)];
const blockKey = (r) => [BigInt(r.number)];

const log = (bn, li, extra = {}) => ({
  block_number: bn,
  log_index: li,
  data: `d${bn}-${li}`,
  ...extra,
});

test('normRow: sorted keys, bigint→decimal, bytes→hex, dropped column removed', () => {
  const s = normRow(
    { z: 1n, a: new Uint8Array([0xde, 0xad]), total_difficulty: 5n, b: 'x' },
    new Set(['total_difficulty']),
  );
  assert.equal(s, JSON.stringify({ a: 'dead', b: 'x', z: '1' }));
});

test('cmpKey: lexicographic over composite keys', () => {
  assert.equal(cmpKey([1n, 2n], [1n, 3n]), -1);
  assert.equal(cmpKey([2n], [1n, 9n]), 1);
  assert.equal(cmpKey([5n, 5n], [5n, 5n]), 0);
});

test('strict mode: identical ordered streams pass', async () => {
  const rows = [log(100, 0), log(100, 1), log(101, 0)];
  const r = await mergeCompare(rows, rows.slice(), {
    keyFn: logKey,
    mode: 'strict',
  });
  assert.equal(r.fail, false);
  assert.equal(r.shared, 3);
  assert.equal(r.mismatch, 0);
});

test('strict mode: a portal-only log FAILS', async () => {
  const a = [log(100, 0), log(100, 1), log(101, 0)];
  const b = [log(100, 0), log(101, 0)]; // missing (100,1)
  const r = await mergeCompare(a, b, { keyFn: logKey, mode: 'strict' });
  assert.equal(r.fail, true);
  assert.equal(r.onlyA, 1);
  assert.equal(r.onlyB, 0);
});

test('strict mode: a shared-key field mismatch FAILS', async () => {
  const a = [log(100, 0, { data: 'X' })];
  const b = [log(100, 0, { data: 'Y' })];
  const r = await mergeCompare(a, b, { keyFn: logKey, mode: 'strict' });
  assert.equal(r.fail, true);
  assert.equal(r.mismatch, 1);
});

test('blocks mode: RPC-only (B-only) inert block is reported, not failed', async () => {
  // A = portal, B = rpc (diffStores/run.sh: dirA=dbPortal, dirB=dbRpc)
  const portal = [
    { number: 100, hash: 'h100' },
    { number: 102, hash: 'h102' },
  ];
  const rpc = [
    { number: 100, hash: 'h100' },
    { number: 101, hash: 'h101' }, // inert event-less block only the RPC path traced
    { number: 102, hash: 'h102' },
  ];
  const r = await mergeCompare(portal, rpc, {
    keyFn: blockKey,
    mode: 'blocks',
  });
  assert.equal(r.fail, false, 'extra RPC-only block must not fail');
  assert.equal(r.onlyB, 1);
  assert.equal(r.shared, 2);
});

// #19 — the batched 'blocks' mode is ASYMMETRIC (mirrors harness/diff/diff.mjs blocksVerdict): a
// PORTAL-only block (onlyA) is a block the Portal path invented that RPC never saw → FAIL. Before the
// fix 'blocks' mode set res.fail only on a shared mismatch, so a portal-only block sailed through the
// F-full batched differ. MUTATION: revert the `mode === 'blocks'` branch on the onlyA (step<0) side
// back to `mode === 'strict'` → this test fails (r.fail becomes false).
test('blocks mode: a PORTAL-only (A-only) block FAILS (asymmetric — #19)', async () => {
  const portal = [
    { number: 100, hash: 'h100' },
    { number: 101, hash: 'h101' }, // portal invented block 101 — RPC never saw it
    { number: 102, hash: 'h102' },
  ];
  const rpc = [
    { number: 100, hash: 'h100' },
    { number: 102, hash: 'h102' },
  ];
  const r = await mergeCompare(portal, rpc, {
    keyFn: blockKey,
    mode: 'blocks',
  });
  assert.equal(r.fail, true, 'a portal-only block must FAIL under blocks mode');
  assert.equal(r.onlyA, 1);
  assert.equal(r.onlyB, 0);
  assert.equal(r.shared, 2);
});

test('blocks mode: total_difficulty is excluded, real field mismatch still fails', async () => {
  const drop = new Set(['total_difficulty']);
  const portal = [{ number: 100, hash: 'h', total_difficulty: null }];
  const rpcOk = [{ number: 100, hash: 'h', total_difficulty: 999n }];
  const okRes = await mergeCompare(portal, rpcOk, {
    keyFn: blockKey,
    drop,
    mode: 'blocks',
  });
  assert.equal(okRes.fail, false, 'total_difficulty divergence is tolerated');

  const rpcBad = [{ number: 100, hash: 'DIFFERENT', total_difficulty: 999n }];
  const badRes = await mergeCompare(portal, rpcBad, {
    keyFn: blockKey,
    drop,
    mode: 'blocks',
  });
  assert.equal(badRes.fail, true, 'a real shared-block field mismatch fails');
  assert.equal(badRes.mismatch, 1);
});

// ab-diff.mjs feeds streamingDiff single-field {key,hash} rows (the SQL-computed block/log md5). In
// the FINALIZED overlap a one-sided block is a real gap, so ab-diff now runs its block diff in
// 'strict' (not the 'blocks' tolerance). This locks that shape: a B-missing block FAILs under strict.
test('strict mode over {key,hash} rows: a one-sided block FAILS (ab-diff finalized-overlap)', async () => {
  const hashRow = (n, h) => ({ key: [BigInt(n)], hash: h });
  const keyOfHashRow = (r) => r.key;
  const a = [hashRow(100, 'h100'), hashRow(101, 'h101'), hashRow(102, 'h102')];
  const b = [hashRow(100, 'h100'), hashRow(102, 'h102')]; // B missing block 101

  const strict = await mergeCompare(a, b, {
    keyFn: keyOfHashRow,
    mode: 'strict',
  });
  assert.equal(
    strict.fail,
    true,
    'a one-sided finalized block must FAIL under strict',
  );
  assert.equal(strict.onlyA, 1);

  // proof strict is the mode that matters for the FINALIZED overlap: an rpc-only (B-only) block —
  // which the asymmetric 'blocks' mode still TOLERATES as an inert event-less RPC block — must FAIL
  // under strict, because in the finalized overlap a one-sided block is a real gap, not inert.
  const bExtra = [
    hashRow(100, 'h100'),
    hashRow(101, 'h101'),
    hashRow(102, 'h102'),
    hashRow(103, 'h103'), // only in B
  ];
  const strictBExtra = await mergeCompare(a, bExtra, {
    keyFn: keyOfHashRow,
    mode: 'strict',
  });
  assert.equal(
    strictBExtra.fail,
    true,
    'a B-only finalized block must FAIL under strict',
  );
  assert.equal(strictBExtra.onlyB, 1);

  const tolerant = await mergeCompare(a, bExtra, {
    keyFn: keyOfHashRow,
    mode: 'blocks',
  });
  assert.equal(
    tolerant.fail,
    false,
    "'blocks' mode tolerates the rpc-only inert block (not the finalized-overlap mode)",
  );
});

test('hashRows: order-independent, field-sensitive, deterministic', () => {
  const rows = [log(101, 0), log(100, 1), log(100, 0)];
  const shuffled = [log(100, 0), log(101, 0), log(100, 1)];
  assert.equal(
    hashRows(rows, logKey),
    hashRows(shuffled, logKey),
    'canonical ordering makes the hash input-order-independent',
  );

  const changed = hashRows(
    [log(100, 0, { data: 'changed' }), log(100, 1), log(101, 0)],
    logKey,
  );
  assert.notEqual(
    hashRows(rows, logKey),
    changed,
    'a field change changes the hash',
  );
});

// #15 — --app-hash must NOT report a meaningful PASS when there are no nonempty user tables (the diff
// apps ship a no-op `noop` table and write no user rows, so the checkpoint would be vacuous).
test('appHashVerdict: zero nonempty user tables is NO-USER-TABLES, not a PASS', () => {
  const empty = { combined: 'abc', nonEmptyTables: 0 };
  const v = appHashVerdict(empty, { combined: 'abc', nonEmptyTables: 0 });
  assert.equal(v.ok, false, 'identical-but-vacuous app hashes must NOT pass');
  assert.equal(v.verdict, 'NO-USER-TABLES');

  // one side nonempty, the other empty → still not a meaningful checkpoint
  const oneSide = appHashVerdict(
    { combined: 'x', nonEmptyTables: 3 },
    { combined: 'x', nonEmptyTables: 0 },
  );
  assert.equal(oneSide.ok, false);
});

test('appHashVerdict: with nonempty tables, PASS on identical / DIVERGE on different hashes', () => {
  const pass = appHashVerdict(
    { combined: 'same', nonEmptyTables: 2 },
    { combined: 'same', nonEmptyTables: 2 },
  );
  assert.equal(pass.ok, true);
  assert.equal(pass.verdict, 'PASS');

  const diverge = appHashVerdict(
    { combined: 'aaa', nonEmptyTables: 2 },
    { combined: 'bbb', nonEmptyTables: 2 },
  );
  assert.equal(diverge.ok, false);
  assert.equal(diverge.verdict, 'DIVERGE');
});

// STRICT_BLOCKS override (chaos verify-resume: portal-vs-PORTAL). Default the blocks table is
// ASYMMETRIC ('blocks'): a B-only block is a tolerated inert RPC block. In the chaos context both
// stores are Portal-built, so a BASELINE-only (B-only) block means the RESUMED store is MISSING a
// block → must FAIL. resolveTableSpecs(true) must flip ONLY blocks to 'strict'.
// MUTATION: make resolveTableSpecs ignore the flag (return TABLES / drop the `table==='blocks'`
// override) → the strict-mode B-only diff below no longer fails and this test fails.
test('resolveTableSpecs: STRICT_BLOCKS flips only the blocks table to strict', () => {
  const def = resolveTableSpecs(false);
  assert.equal(def.blocks.mode, 'blocks', 'default blocks mode is asymmetric');

  const strict = resolveTableSpecs(true);
  assert.equal(
    strict.blocks.mode,
    'strict',
    'override promotes blocks to strict',
  );
  assert.equal(strict.logs.mode, 'strict', 'logs unchanged');
  assert.equal(strict.transactions.mode, 'strict', 'transactions unchanged');
  // total_difficulty must still be dropped under the override — the promotion changes mode only.
  assert.ok(strict.blocks.drop.has('total_difficulty'));
  // the default spec object must not be mutated in place
  assert.equal(def.blocks.mode, 'blocks');
});

test('STRICT_BLOCKS: a baseline-only (B-only) block FAILS (chaos resume vs baseline)', async () => {
  // chaos-resumed store A is MISSING baseline block 101 that a clean run has.
  const chaos = [
    { number: 100, hash: 'h100' },
    { number: 102, hash: 'h102' },
  ];
  const baseline = [
    { number: 100, hash: 'h100' },
    { number: 101, hash: 'h101' }, // baseline has it; the resumed store dropped it → real gap
    { number: 102, hash: 'h102' },
  ];
  const strictMode = resolveTableSpecs(true).blocks.mode;
  const strict = await mergeCompare(chaos, baseline, {
    keyFn: blockKey,
    drop: resolveTableSpecs(true).blocks.drop,
    mode: strictMode,
  });
  assert.equal(
    strict.fail,
    true,
    'a baseline-only block (missing from the resumed store) must FAIL under STRICT_BLOCKS',
  );
  assert.equal(strict.onlyB, 1);

  // proof the default mode would WRONGLY tolerate the same missing block (why the override exists).
  const tolerant = await mergeCompare(chaos, baseline, {
    keyFn: blockKey,
    drop: resolveTableSpecs(false).blocks.drop,
    mode: resolveTableSpecs(false).blocks.mode,
  });
  assert.equal(
    tolerant.fail,
    false,
    'the default asymmetric blocks mode tolerates the B-only block — the bug the override fixes',
  );
});
