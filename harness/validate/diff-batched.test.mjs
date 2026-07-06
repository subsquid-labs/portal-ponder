import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  appHashVerdict,
  buildKeysetSql,
  cmpKey,
  hashRows,
  keysetRows,
  mergeCompare,
  nextBatchSize,
  normRow,
  parseByteTarget,
  resolveTableSpecs,
  rowBytes,
  SYNC_STORE_PKS,
  TABLES,
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

// ── issue #76: tolerated upstream block.size off-by-one (blocks mode only) ───────────────────────
// Mirrors harness/diff/diff.mjs blocksVerdict. A=portal, B=rpc. A shared block whose ONLY differing
// field is `size` with rpc==portal+1 and rpc>=65540 is counted in res.sizeTolerated (not res.mismatch)
// and does NOT fail. Everything else about size, a second differing field, and strict mode all FAIL.
const szBlock = (number, size, extra = {}) => ({
  number,
  hash: 'h',
  size,
  ...extra,
});

// MUTATION: run this against origin/main's diff-batched.mjs (no sizeTolerated branch) → na!==nb sets
// res.mismatch and res.fail, so r.fail is true and r.sizeTolerated is undefined → this test FAILS.
test('blocks mode #76: a lone size off-by-one at/above 65540 is tolerated, not a mismatch', async () => {
  const r = await mergeCompare(
    [szBlock(19963775, 66755)],
    [szBlock(19963775, 66756)],
    {
      keyFn: blockKey,
      mode: 'blocks',
    },
  );
  assert.equal(r.fail, false, 'a lone size off-by-one at scale does not fail');
  assert.equal(r.sizeTolerated, 1);
  assert.equal(r.mismatch, 0, 'the tolerated row is not counted as a mismatch');
  assert.equal(r.shared, 1);
});

test('blocks mode #76: a sub-threshold size delta (< 65540) still FAILS', async () => {
  const r = await mergeCompare([szBlock(100, 30000)], [szBlock(100, 30001)], {
    keyFn: blockKey,
    mode: 'blocks',
  });
  assert.equal(
    r.fail,
    true,
    'below the 65540 boundary the off-by-one is a real mismatch',
  );
  assert.equal(r.mismatch, 1);
  assert.equal(r.sizeTolerated, 0);
});

test('blocks mode #76: a size delta of 2 (not exactly +1) still FAILS', async () => {
  const r = await mergeCompare([szBlock(100, 66754)], [szBlock(100, 66756)], {
    keyFn: blockKey,
    mode: 'blocks',
  });
  assert.equal(r.fail, true, 'only an exact +1 delta is tolerated');
  assert.equal(r.mismatch, 1);
  assert.equal(r.sizeTolerated, 0);
});

test('blocks mode #76: portal LARGER than rpc (opposite sign) still FAILS', async () => {
  const r = await mergeCompare([szBlock(100, 66757)], [szBlock(100, 66756)], {
    keyFn: blockKey,
    mode: 'blocks',
  });
  assert.equal(
    r.fail,
    true,
    'only rpc == portal+1 is tolerated, never portal > rpc',
  );
  assert.equal(r.mismatch, 1);
  assert.equal(r.sizeTolerated, 0);
});

test('blocks mode #76: size within tolerance but a SECOND field also differs still FAILS', async () => {
  const portal = [szBlock(100, 66755, { gas_used: 100n })];
  const rpc = [szBlock(100, 66756, { gas_used: 200n })];
  const r = await mergeCompare(portal, rpc, {
    keyFn: blockKey,
    mode: 'blocks',
  });
  assert.equal(
    r.fail,
    true,
    'a second differing field defeats the size tolerance',
  );
  assert.equal(r.mismatch, 1);
  assert.equal(r.sizeTolerated, 0);
});

test('blocks mode #76: a consensus-field (hash) divergence at large size is NEVER masked', async () => {
  const portal = [szBlock(100, 66755, { hash: '0xAAA' })];
  const rpc = [szBlock(100, 66756, { hash: '0xZZZ' })];
  const r = await mergeCompare(portal, rpc, {
    keyFn: blockKey,
    mode: 'blocks',
  });
  assert.equal(
    r.fail,
    true,
    'a differing hash is a real mismatch even alongside a size off-by-one',
  );
  assert.equal(r.mismatch, 1);
  assert.equal(r.sizeTolerated, 0);
});

test('strict mode #76: the size off-by-one is NOT tolerated under strict (scoped to blocks mode)', async () => {
  // The tolerance is calibrated for the asymmetric portal-vs-rpc blocks comparison only. Under strict
  // (logs/txs/receipts/traces, and portal-vs-portal STRICT_BLOCKS) a size difference is a real mismatch.
  const r = await mergeCompare([szBlock(100, 66755)], [szBlock(100, 66756)], {
    keyFn: blockKey,
    mode: 'strict',
  });
  assert.equal(r.fail, true, 'strict mode tolerates nothing');
  assert.equal(r.mismatch, 1);
  assert.equal(r.sizeTolerated, 0);
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

// #58 — keyset pagination must ORDER BY the sync-store PK column order (chain_id-first) so every
// 50k-row page is a forward index scan on the PK, not a full-table sort. This pins every TABLES
// spec's keyset to the PK column order (SYNC_STORE_PKS, taken verbatim from ponder's sync-store
// schema). MUTATION: drop 'chain_id' from any spec's keys (or reorder the tuple) → this test FAILS.
test('#58 keyset↔PK: every TABLES keyset is exactly the chain_id-prefixed sync-store PK order', () => {
  // same table set on both sides — no spec silently added/removed.
  assert.deepEqual(
    Object.keys(TABLES).sort(),
    Object.keys(SYNC_STORE_PKS).sort(),
  );

  for (const [table, pk] of Object.entries(SYNC_STORE_PKS)) {
    const spec = TABLES[table];
    assert.ok(spec, `TABLES has a spec for ${table}`);
    assert.equal(
      spec.keys[0],
      'chain_id',
      `${table} keyset must LEAD with chain_id (the PK prefix) — else every page full-sorts`,
    );
    assert.deepEqual(
      spec.keys,
      pk,
      `${table} keyset must match the PK column order exactly (${pk.join(', ')})`,
    );
  }
});

// #58 — pin the SQL the keyset builder emits for each table: ORDER BY and the tuple-cursor WHERE must
// list the PK columns in PK order, so the planner uses a single forward PK index scan (no per-page
// sort). MUTATION: drop chain_id from a spec → the asserted ORDER BY / tuple LHS no longer starts
// with "chain_id" → this test FAILS.
test('#58 buildKeysetSql: ORDER BY + tuple-WHERE follow the chain_id-prefixed PK per table', () => {
  const expected = {
    logs: '"chain_id", "block_number", "log_index"',
    transactions: '"chain_id", "block_number", "transaction_index"',
    transaction_receipts: '"chain_id", "block_number", "transaction_index"',
    traces: '"chain_id", "block_number", "transaction_index", "trace_index"',
    blocks: '"chain_id", "number"',
  };

  for (const [table, spec] of Object.entries(TABLES)) {
    const cols = expected[table];

    // The per-page LIMIT is now byte-aware (issue #63): the caller passes it and it is the ONLY thing
    // that varies between pages. Pin the emitted SQL against a caller-supplied limit; the cursor
    // WHERE/ORDER BY must not depend on it.
    const limit = 12_345;

    // first page: ORDER BY the PK columns, no WHERE.
    const first = buildKeysetSql(table, spec.keys, false, limit);
    assert.equal(
      first,
      `select * from ponder_sync."${table}"  order by ${cols} limit ${limit}`,
      `${table} first-page SQL must ORDER BY the chain_id-prefixed PK`,
    );

    // cursor page: row-wise tuple WHERE over the SAME PK columns + $-placeholders, then ORDER BY.
    const cursor = buildKeysetSql(table, spec.keys, true, limit);
    const rhs = spec.keys.map((_, i) => `$${i + 1}`).join(', ');
    assert.equal(
      cursor,
      `select * from ponder_sync."${table}" where (${cols}) > (${rhs}) order by ${cols} limit ${limit}`,
      `${table} cursor-page SQL must tuple-compare + ORDER BY the chain_id-prefixed PK`,
    );
    // the tuple LHS and the ORDER BY must lead with chain_id (the PK prefix that makes it an index scan).
    assert.ok(
      cursor.includes('("chain_id",') || cursor.includes('("chain_id")'),
      `${table} cursor tuple must lead with chain_id`,
    );

    // A DIFFERENT limit changes ONLY the trailing `limit N` — the cursor WHERE + ORDER BY are byte,
    // for byte, identical (this is the invariant issue #63's varying page size must preserve).
    const other = buildKeysetSql(table, spec.keys, true, 999);
    assert.equal(
      other.replace(/ limit \d+$/, ''),
      cursor.replace(/ limit \d+$/, ''),
      `${table} cursor SQL (minus the limit) must not depend on the page size`,
    );
  }
});

// #58 — functional multi-chain regression. Two chains' rows interleave by block_number: chain 1 has
// blocks 100,102; chain 2 has block 101. Keyed per-(chain_id, block_number) (the fixed keyset) the two
// stores are IDENTICAL and every row is shared. Keyed by (block_number) ALONE (the OLD keyset) the
// merge mis-orders across chains — chain 2's block 101 sorts BETWEEN chain 1's 100 and 102 — and any
// cross-chain block-number collision would be silently conflated. This proves the fix makes the diff
// well-defined per (chain_id, …) tuple; the OLD keyset would mis-order these rows.
test('#58 multi-chain: streamingDiff is well-defined per (chain_id, block_number) tuple', async () => {
  const row = (chain, bn, extra = {}) => ({
    chain_id: chain,
    block_number: bn,
    hash: `h${chain}-${bn}`,
    ...extra,
  });

  // interleave the two chains so their block numbers are NOT globally monotone in row order — the
  // stream is only monotone under the (chain_id, block_number) key, exactly like a real two-chain store.
  const store = [row(1n, 100n), row(1n, 102n), row(2n, 101n)];

  const chainKey = (r) => [r.chain_id, r.block_number];
  const same = await mergeCompare(store, store.slice(), {
    keyFn: chainKey,
    mode: 'strict',
  });
  assert.equal(
    same.fail,
    false,
    'identical multi-chain stores must be shared, not divergent',
  );
  assert.equal(same.shared, 3);
  assert.equal(same.onlyA, 0);
  assert.equal(same.onlyB, 0);

  // a cross-chain collision the OLD (block_number-only) key would CONFLATE: chain 1 block 101 and
  // chain 2 block 101 are DISTINCT rows. Under the fixed (chain_id, block_number) key A has (1,101)
  // and B has (2,101) — a real one-sided divergence on EACH side that strict must FAIL.
  const a = [row(1n, 100n), row(1n, 101n, { hash: 'A-only' })];
  const b = [row(1n, 100n), row(2n, 101n, { hash: 'B-only' })];
  const withChain = await mergeCompare(a, b, {
    keyFn: chainKey,
    mode: 'strict',
  });
  assert.equal(
    withChain.fail,
    true,
    'distinct-chain rows sharing a block number must NOT be conflated — strict fails on the one-sided rows',
  );
  assert.equal(withChain.onlyA, 1, 'chain-1 block 101 is A-only');
  assert.equal(withChain.onlyB, 1, 'chain-2 block 101 is B-only');
  assert.equal(withChain.shared, 1, 'only chain-1 block 100 is shared');

  // the OLD block_number-only key would WRONGLY treat (1,101) and (2,101) as the SAME row and report a
  // field MISMATCH instead of two distinct one-sided rows — conflating two chains. This locks the
  // semantic improvement: the chain_id prefix keeps per-chain identity distinct.
  const oldKey = (r) => [r.block_number];
  const conflated = await mergeCompare(a, b, { keyFn: oldKey, mode: 'strict' });
  assert.equal(
    conflated.shared,
    2,
    'the OLD block_number-only key CONFLATES the two chains block-101 rows as one shared key',
  );
  assert.equal(
    conflated.onlyA,
    0,
    'proof the old key hides the cross-chain divergence as a shared mismatch, not a one-sided row',
  );
  assert.equal(
    conflated.mismatch,
    1,
    'old key mislabels the cross-chain rows as a field mismatch',
  );
});

// ── #63 byte-aware page sizing ─────────────────────────────────────────────────────────────────

// #63 — the sizing POLICY. nextBatchSize(avg, target, floor, ceiling) returns floor(target/avg)
// clamped to [floor, ceiling]. This is the whole point of the fix: the limit ADAPTS to the observed
// row width so the per-query payload stays near `target`, instead of a fixed row count that wedges
// PGlite on fat tables (a 50k-row page of ~300MB detoast). MUTATION: stub nextBatchSize to a constant
// (e.g. `return 50_000`) — the adaptation, floor, and ceiling assertions below all FAIL.
test('#63 nextBatchSize: limit adapts to observed row width toward the byte target', () => {
  const target = 32 * 1024 * 1024; // 32MB
  const floor = 5_000;
  const ceiling = 50_000;

  // a slim ~200-byte row → target/avg ≈ 167k rows, clamped DOWN to the ceiling.
  assert.equal(
    nextBatchSize(200, target, floor, ceiling),
    ceiling,
    'slim rows page at the ceiling',
  );

  // a fat ~64KB row (fat calldata) → target/avg = 512 rows, clamped UP to the floor.
  assert.equal(
    nextBatchSize(64 * 1024, target, floor, ceiling),
    floor,
    'fat rows page at the floor',
  );

  // a mid-width row that lands strictly INSIDE the window must adapt, not clamp: avg=2048 →
  // 32MB/2048 = 16384 rows, which is between floor(5000) and ceiling(50000).
  assert.equal(
    nextBatchSize(2048, target, floor, ceiling),
    16_384,
    'a mid-width row adapts to floor(target/avg), not a fixed count',
  );

  // strict monotonicity: a wider row ⇒ a smaller-or-equal limit (the adaptation direction the fix
  // depends on — wider detoast ⇒ fewer rows per query).
  const wide = nextBatchSize(8_000, target, floor, ceiling);
  const wider = nextBatchSize(16_000, target, floor, ceiling);
  assert.ok(
    wider <= wide,
    'a wider observed row must not produce a larger limit',
  );
});

// #63 — the byte TARGET actually scales the limit. Doubling the target roughly doubles the row count
// for the same observed width (while both stay inside the clamp window). MUTATION: ignore `targetBytes`
// (e.g. hardcode the numerator) → this proportionality breaks.
test('#63 nextBatchSize: the byte target scales the row limit', () => {
  const avg = 4096;
  const floor = 1;
  const ceiling = 10_000_000;
  const small = nextBatchSize(avg, 8 * 1024 * 1024, floor, ceiling);
  const big = nextBatchSize(avg, 16 * 1024 * 1024, floor, ceiling);
  assert.equal(
    big,
    small * 2,
    'doubling the target doubles the adaptive limit',
  );
});

// #63 — degenerate observations carry no width signal ⇒ fall back to the FLOOR, the conservative
// limit that cannot wedge on an unknown-/zero-width table. This covers the first-page default (no
// observation yet) and any pathological page (all-empty rows → avg 0; a NaN/∞ average). MUTATION:
// return the ceiling (or `est`) on a bad input → these FAIL, and the tool would fetch a huge first
// page on an unknown-width table (the exact wedge #63 fixes).
test('#63 nextBatchSize: degenerate inputs (0 / NaN / -/∞) fall back to the floor', () => {
  const target = 32 * 1024 * 1024;
  const floor = 5_000;
  const ceiling = 50_000;
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, -1000]) {
    assert.equal(
      nextBatchSize(bad, target, floor, ceiling),
      floor,
      `degenerate avg ${bad} must fall back to the floor`,
    );
  }
});

// #63 — the FIRST page (no previous observation) must default to the floor. The stream starts before
// any width is known, so the first `limit` cannot be an aggressive guess on an unknown-width table.
// The default args make nextBatchSize() with a degenerate/absent observation return the floor.
test('#63 nextBatchSize: first-page default (no observation) is the floor', () => {
  // no observation is modelled as a degenerate avg; with the module defaults this is MIN_BATCH=5000.
  assert.equal(nextBatchSize(Number.NaN), 5_000);
  assert.equal(nextBatchSize(0), 5_000);
});

// #63 — rowBytes: a cheap, deterministic per-row width ≈ the detoast volume. Bytes columns count as
// their RAW byte length (the fat `input` Uint8Array — its byteLength IS the detoast cost), so a fat
// calldata row measures far wider than a slim one. Deterministic: identical rows measure identically.
test('#63 rowBytes: bytes columns dominate the width and the measure is deterministic', () => {
  const slim = { chain_id: 1, block_number: 100, input: new Uint8Array(4) };
  const fat = {
    chain_id: 1,
    block_number: 100,
    input: new Uint8Array(200_000),
  };
  assert.ok(
    rowBytes(fat) > rowBytes(slim) + 190_000,
    'a fat-calldata row must measure far wider than a slim one',
  );
  assert.equal(
    rowBytes(fat),
    rowBytes({ ...fat }),
    'identical rows measure identically (deterministic)',
  );
  // strings count as UTF-8 byte length, bigints as decimal-digit length, null as a floor of 1.
  const mixed = { a: 'abc', b: 123456789n, c: null };
  assert.equal(rowBytes(mixed), 3 + 9 + 1);
});

// #63 — parseByteTarget: CLI flag beats env beats default; junk falls through to the next source
// (never a silent 0 that would collapse every page to the floor).
test('#63 parseByteTarget: --byte-target > DIFF_BYTE_TARGET > default, junk falls through', () => {
  const dflt = 32 * 1024 * 1024;
  assert.equal(parseByteTarget([], {}, dflt), dflt, 'default when nothing set');
  assert.equal(
    parseByteTarget([], { DIFF_BYTE_TARGET: '8388608' }, dflt),
    8388608,
    'env override',
  );
  assert.equal(
    parseByteTarget(
      ['A', 'B', '--byte-target', '16777216'],
      { DIFF_BYTE_TARGET: '8388608' },
      dflt,
    ),
    16777216,
    'CLI flag beats env',
  );
  // junk flag value → fall through to env; junk env → fall through to default.
  assert.equal(
    parseByteTarget(
      ['--byte-target', 'nope'],
      { DIFF_BYTE_TARGET: '4096' },
      dflt,
    ),
    4096,
    'non-numeric flag falls through to env',
  );
  assert.equal(
    parseByteTarget(['--byte-target', '0'], { DIFF_BYTE_TARGET: '-5' }, dflt),
    dflt,
    'non-positive flag and env both fall through to the default',
  );
});

// A faithful in-memory keyset-paginated fake DB: it answers exactly the SQL keysetRows emits (parses
// the trailing `limit N`, applies the row-wise tuple cursor from the params, orders by the key tuple)
// so keysetRows drives it identically to real PGlite — but every page size the differ chooses is
// honoured, letting us prove the row STREAM is independent of the page-size sequence. It also records
// the exact limit sequence used, so we can assert two runs really did page differently.
function makeFakeDb(rows, keys) {
  const cmp = (a, b) => {
    for (const k of keys) {
      const x = BigInt(a[k]);
      const y = BigInt(b[k]);
      if (x < y) {
        return -1;
      }
      if (x > y) {
        return 1;
      }
    }

    return 0;
  };
  const ordered = rows.slice().sort(cmp);
  const limits = [];

  const db = {
    limits,
    query(sql, params) {
      const m = sql.match(/limit (\d+)$/);
      const limit = Number(m[1]);
      limits.push(limit);
      let start = 0;
      if (params.length > 0) {
        const cursor = {};
        keys.forEach((k, i) => {
          cursor[k] = params[i];
        });
        while (start < ordered.length && cmp(ordered[start], cursor) <= 0) {
          start += 1;
        }
      }

      return Promise.resolve({ rows: ordered.slice(start, start + limit) });
    },
  };

  return db;
}

async function drain(iter) {
  const out = [];
  for await (const r of iter) {
    out.push(r);
  }

  return out;
}

// #63 — CURSOR INDEPENDENCE: keysetRows must yield the SAME ordered row stream regardless of the
// page-size sequence the byte-aware sizing chooses. The keyset cursor (tuple-WHERE > previous tail) is
// what guarantees this — only the `limit` varies. We drive one dataset through a range of byte targets
// (which forces genuinely different limit sequences) and assert every run yields a byte-identical
// stream. MUTATION: make the cursor depend on the page size (e.g. advance by the limit instead of the
// tail row, or reset `last`) → the streams diverge and this FAILS.
test('#63 keysetRows: the row stream is independent of the byte-aware page-size sequence', async () => {
  const keys = ['chain_id', 'block_number'];
  // Enough slim rows to span MANY pages (past both the floor=5000 and ceiling=50000 limits), so the
  // byte-aware sizing genuinely pages this dataset differently under different targets. Rows are slim
  // (the width signal comes from `input`), so 120k tiny rows cost little memory but force multi-page
  // walks. A small periodic width swing keeps the average realistic without bloating the fixture.
  const rows = [];
  for (let bn = 0; bn < 120_000; bn++) {
    const width = bn % 7 === 0 ? 64 : 8;
    rows.push({
      chain_id: 1,
      block_number: bn,
      input: new Uint8Array(width),
    });
  }

  const reference = await drain(keysetRows(makeFakeDb(rows, keys), 't', keys));
  // reference must be the full dataset in key order.
  assert.equal(reference.length, rows.length, 'reference stream is complete');
  for (let i = 1; i < reference.length; i++) {
    assert.ok(
      reference[i].block_number > reference[i - 1].block_number,
      'reference stream is strictly key-ordered',
    );
  }

  const seqs = new Set();
  // targets from tiny (clamps to the floor → many 5k pages) to large (clamps to the ceiling → few 50k
  // pages) → genuinely different limit sequences over the SAME rows.
  for (const target of [200_000, 32 * 1024 * 1024]) {
    const db = makeFakeDb(rows, keys);
    const stream = await drain(keysetRows(db, 't', keys, target));
    seqs.add(db.limits.join(','));
    assert.equal(
      stream.length,
      reference.length,
      `byte-target ${target}: same row count`,
    );
    for (let i = 0; i < stream.length; i++) {
      assert.equal(
        stream[i].block_number,
        reference[i].block_number,
        `byte-target ${target}: row ${i} identical regardless of page size`,
      );
      assert.equal(
        stream[i].input.byteLength,
        reference[i].input.byteLength,
        `byte-target ${target}: row ${i} payload identical`,
      );
    }
  }

  // the runs really DID page differently — otherwise "independence" would be vacuous.
  assert.ok(
    seqs.size > 1,
    'the byte targets must produce genuinely different page-size sequences',
  );
});

// #63 — the FIRST page uses the floor limit (no observation yet). keysetRows must issue its very first
// query with `limit 5000` on a fresh, unknown-width table — the conservative default that cannot wedge.
// MUTATION: seed the first `limit` from a large constant (e.g. 50000) → the recorded first limit is no
// longer the floor and this FAILS.
test('#63 keysetRows: the FIRST query uses the floor limit on an unknown-width table', async () => {
  const keys = ['chain_id', 'block_number'];
  const rows = [{ chain_id: 1, block_number: 0, input: new Uint8Array(8) }];
  const db = makeFakeDb(rows, keys);
  await drain(keysetRows(db, 't', keys));
  assert.equal(db.limits[0], 5_000, 'first page is fetched at the floor limit');
});
