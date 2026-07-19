import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';
// The cell differ (harness/diff/diff.mjs) exports its pure verdict cores + normalizer; the paged
// differ (diff-batched.mjs) exports its streaming verdict core + keyset paging. This test proves the
// run.sh default now routes the cell path through the PAGED differ (issue #78) and that its verdict is
// byte-for-byte class-identical to the legacy in-memory differ on a shared fixture store.
import { blocksVerdict, norm, STRICT, setDiff } from '../diff/diff.mjs';
import { cmpKey, mergeCompare, TABLES } from './diff-batched.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, '..', '..');
const RUN_SH = join(ROOT, 'harness', 'diff', 'run.sh');

// ── (1) structural: the run.sh default differ pages, it does not whole-table materialize ──────────
// MUTATION (origin/main): run.sh's default DIFF_SCRIPT is harness/diff/diff.mjs, whose dump() issues a
// single unbounded `select * from ponder_sync."<t>"` per table and exports no keyset reader — so the
// dynamic import below has no `keysetRows`/`buildKeysetSql` and this test FAILS. With the #78 fix the
// default is diff-batched.mjs (byte-aware keyset pages) and it PASSES.
test('#78 run.sh routes the cell diff through a byte-aware PAGED differ (not whole-table select*)', async () => {
  const runSh = readFileSync(RUN_SH, 'utf8');
  const m = runSh.match(
    /DIFF_SCRIPT="\$\{DIFF_SCRIPT:-\$ROOT\/([^"}]+\.mjs)\}"/,
  );
  assert.ok(
    m,
    `run.sh must define a default DIFF_SCRIPT of the form \${DIFF_SCRIPT:-$ROOT/…mjs}`,
  );

  const defaultDiffer = resolve(ROOT, m[1]);
  const mod = await import(pathToFileURL(defaultDiffer).href);

  // the default differ MUST expose byte-aware keyset paging (issue #63/#72/#78). The legacy in-memory
  // diff.mjs has no such reader (it materializes whole tables), so these are absent on origin/main.
  assert.equal(
    typeof mod.keysetRows,
    'function',
    'the run.sh default differ must page reads via keysetRows (not whole-table select*)',
  );
  assert.equal(
    typeof mod.buildKeysetSql,
    'function',
    'the run.sh default differ must build bounded keyset pages',
  );
  assert.equal(
    typeof mod.nextBatchSize,
    'function',
    'the run.sh default differ must size pages byte-aware (issue #63)',
  );

  // every per-table read must carry a LIMIT (a bounded page) over the ponder_sync store — never the
  // unbounded whole-table select that wedged PGlite's WASM heap on dense windows (issue #78).
  const sql = mod.buildKeysetSql(
    'logs',
    ['chain_id', 'block_number', 'log_index'],
    false,
    5000,
  );
  assert.match(
    sql,
    / limit 5000$/,
    'each page read must be bounded by a LIMIT',
  );
  assert.match(
    sql,
    /from ponder_sync\."logs"/,
    'the paged read must target the ponder_sync store table',
  );
});

// ── (2) verdict parity: diff.mjs (legacy) vs diff-batched.mjs (new default) on a shared fixture ────
// The route-a substitution is only safe if the two differs return the SAME verdict class (ok / fail /
// #77 sizeTolerated) on the same stores. We feed identical fixture rows to BOTH — diff.mjs via its
// set-identity / blocksVerdict cores (over its own `norm` strings), diff-batched via streamingDiff —
// and assert the verdicts agree, including a #77 tolerated case and a genuine-divergence FAIL case.

const drop = new Set(['total_difficulty']);

const logRow = (bn, li, extra = {}) => ({
  chain_id: 1,
  block_number: bn,
  log_index: li,
  data: `d${bn}-${li}`,
  ...extra,
});

const blockRow = (number, extra = {}) => ({
  chain_id: 1,
  number,
  hash: `h${number}`,
  ...extra,
});

const byKey = (keyFn) => (x, y) => cmpKey(keyFn(x), keyFn(y));
const logKey = (r) => [
  BigInt(r.chain_id),
  BigInt(r.block_number),
  BigInt(r.log_index),
];
const blockKey = (r) => [BigInt(r.chain_id), BigInt(r.number)];

// diff.mjs verdict for a STRICT table = set-identity over its own normalized row-strings (exactly
// what dump()+setDiff do at runtime).
function diffMjsStrict(portal, rpc) {
  const a = portal.map((r) => norm(r)).sort();
  const b = rpc.map((r) => norm(r)).sort();

  return setDiff(a, b).ok;
}

// diff-batched verdict for a STRICT table = streaming merge over key-ordered rows.
async function batchedStrict(portal, rpc) {
  const r = await mergeCompare(
    portal.slice().sort(byKey(logKey)),
    rpc.slice().sort(byKey(logKey)),
    { keyFn: logKey, mode: 'strict' },
  );

  return !r.fail;
}

function diffMjsBlocks(portal, rpc) {
  const a = portal.map((r) => norm(r, drop)).sort();
  const b = rpc.map((r) => norm(r, drop)).sort();
  const v = blocksVerdict(a, b);

  return {
    ok: v.ok,
    sizeTolerated: v.sizeTolerated.length,
    baseFeeTolerated: v.baseFeeTolerated.length,
    mismatch: v.mismatch.length,
  };
}

async function batchedBlocks(portal, rpc) {
  const r = await mergeCompare(
    portal.slice().sort(byKey(blockKey)),
    rpc.slice().sort(byKey(blockKey)),
    { keyFn: blockKey, drop, mode: 'blocks' },
  );

  return {
    ok: !r.fail,
    sizeTolerated: r.sizeTolerated,
    baseFeeTolerated: r.baseFeeTolerated,
    mismatch: r.mismatch,
  };
}

test('#78 parity: the two differs normalize a row byte-for-byte identically', async () => {
  const { normRow } = await import('./diff-batched.mjs');
  const row = {
    z: 1n,
    a: new Uint8Array([0xde, 0xad]),
    total_difficulty: 5n,
    b: 'x',
  };
  assert.equal(
    norm(row, drop),
    normRow(row, drop),
    'diff.mjs norm and diff-batched normRow must agree (sorted keys, bigint→dec, bytes→hex, drops)',
  );
});

test('#78 parity: STRICT-table verdict matches diff.mjs on identical / mismatch / one-sided stores', async () => {
  // identical → both OK
  const same = [logRow(100, 0), logRow(100, 1), logRow(101, 0)];
  assert.equal(diffMjsStrict(same, same.slice()), true);
  assert.equal(await batchedStrict(same, same.slice()), true);

  // a genuine shared-key field mismatch → both FAIL
  const mmA = [logRow(100, 0, { data: 'X' })];
  const mmB = [logRow(100, 0, { data: 'Y' })];
  assert.equal(diffMjsStrict(mmA, mmB), false);
  assert.equal(await batchedStrict(mmA, mmB), false);
  assert.equal(
    diffMjsStrict(mmA, mmB),
    await batchedStrict(mmA, mmB),
    'genuine mismatch: both differs FAIL',
  );

  // a portal-only (A-only) row → both FAIL
  const onlyA_p = [logRow(100, 0), logRow(100, 1)];
  const onlyA_r = [logRow(100, 0)];
  assert.equal(diffMjsStrict(onlyA_p, onlyA_r), false);
  assert.equal(await batchedStrict(onlyA_p, onlyA_r), false);

  // an rpc-only (B-only) row → both FAIL (strict tolerates nothing on these four tables)
  const onlyB_p = [logRow(100, 0)];
  const onlyB_r = [logRow(100, 0), logRow(100, 1)];
  assert.equal(diffMjsStrict(onlyB_p, onlyB_r), false);
  assert.equal(await batchedStrict(onlyB_p, onlyB_r), false);
});

test('#78 parity: blocks verdict matches diff.mjs incl. the #76/#106 size-only + U-eth base_fee tolerances', async () => {
  // identical → both OK, nothing tolerated
  const same = [blockRow(100), blockRow(102)];
  assert.deepEqual(diffMjsBlocks(same, same.slice()), {
    ok: true,
    sizeTolerated: 0,
    baseFeeTolerated: 0,
    mismatch: 0,
  });
  assert.deepEqual(await batchedBlocks(same, same.slice()), {
    ok: true,
    sizeTolerated: 0,
    baseFeeTolerated: 0,
    mismatch: 0,
  });

  // #76 tolerated: the lone upstream block.size off-by-one at/above 65540 (rpc == portal + 1) →
  // both differs classify it sizeTolerated (NOT a mismatch) and do NOT fail.
  const tolP = [blockRow(19963775, { size: 66755 })];
  const tolR = [blockRow(19963775, { size: 66756 })];
  const mjsTol = diffMjsBlocks(tolP, tolR);
  const batchedTol = await batchedBlocks(tolP, tolR);
  assert.deepEqual(mjsTol, {
    ok: true,
    sizeTolerated: 1,
    baseFeeTolerated: 0,
    mismatch: 0,
  });
  assert.deepEqual(
    batchedTol,
    mjsTol,
    'both differs tolerate the #76 size off-by-one identically',
  );

  // #106 tolerated: a lone size-only diff BELOW 65540 (BSC portal == rpc + 1) over an equal hash →
  // both differs classify it sizeTolerated identically (the generalized, hash-anchored tolerance).
  const bscP = [blockRow(97964878, { size: 33097 })];
  const bscR = [blockRow(97964878, { size: 33096 })];
  const mjsBsc = diffMjsBlocks(bscP, bscR);
  const batchedBsc = await batchedBlocks(bscP, bscR);
  assert.deepEqual(mjsBsc, {
    ok: true,
    sizeTolerated: 1,
    baseFeeTolerated: 0,
    mismatch: 0,
  });
  assert.deepEqual(
    batchedBsc,
    mjsBsc,
    'both differs tolerate the #106 sub-threshold size-only diff identically',
  );

  // U-eth tolerated: a lone pre-London base_fee null-vs-0 diff over an equal hash → both differs
  // classify it baseFeeTolerated identically (Portal null vs RPC "0", canonical field-absent).
  const bfP = [blockRow(12453996, { base_fee_per_gas: null })];
  const bfR = [blockRow(12453996, { base_fee_per_gas: 0n })];
  const mjsBf = diffMjsBlocks(bfP, bfR);
  const batchedBf = await batchedBlocks(bfP, bfR);
  assert.deepEqual(mjsBf, {
    ok: true,
    sizeTolerated: 0,
    baseFeeTolerated: 1,
    mismatch: 0,
  });
  assert.deepEqual(
    batchedBf,
    mjsBf,
    'both differs tolerate the U-eth pre-London base_fee null-vs-0 diff identically',
  );

  // scope parity: the SAME null-vs-0 signature on an OUT-OF-SCOPE chain (56 = BSC) is a real mismatch
  // in BOTH differs — the tolerance is scoped to eth-mainnet (BASE_FEE_PRELONDON_CHAINS = {1}), so a
  // non-eth chain exhibiting the class FAILs identically in both mirrors (lockstep on the scope guard).
  const bfOosP = [blockRow(12453996, { base_fee_per_gas: null, chain_id: 56 })];
  const bfOosR = [blockRow(12453996, { base_fee_per_gas: 0n, chain_id: 56 })];
  const mjsBfOos = diffMjsBlocks(bfOosP, bfOosR);
  const batchedBfOos = await batchedBlocks(bfOosP, bfOosR);
  assert.deepEqual(mjsBfOos, {
    ok: false,
    sizeTolerated: 0,
    baseFeeTolerated: 0,
    mismatch: 1,
  });
  assert.deepEqual(
    batchedBfOos,
    mjsBfOos,
    'both differs FAIL the null-vs-0 class on an out-of-scope chain identically (scope guard lockstep)',
  );

  // a size-only diff with a differing hash is NOT anchored → both FAIL it as a mismatch (the safety
  // invariant: hash is the second diff, so it is never masked).
  const badSzP = [blockRow(100, { size: 30000, hash: '0xAAA' })];
  const badSzR = [blockRow(100, { size: 30001, hash: '0xZZZ' })];
  assert.deepEqual(diffMjsBlocks(badSzP, badSzR), {
    ok: false,
    sizeTolerated: 0,
    baseFeeTolerated: 0,
    mismatch: 1,
  });
  assert.deepEqual(await batchedBlocks(badSzP, badSzR), {
    ok: false,
    sizeTolerated: 0,
    baseFeeTolerated: 0,
    mismatch: 1,
  });

  // a genuine shared-block field divergence (different hash) → both FAIL
  const badP = [blockRow(100, { hash: '0xAAA' })];
  const badR = [blockRow(100, { hash: '0xZZZ' })];
  assert.equal(diffMjsBlocks(badP, badR).ok, false);
  assert.equal((await batchedBlocks(badP, badR)).ok, false);

  // a portal-only block → both FAIL (the Portal path invented a block RPC never saw)
  const poP = [blockRow(100), blockRow(101), blockRow(102)];
  const poR = [blockRow(100), blockRow(102)];
  assert.equal(diffMjsBlocks(poP, poR).ok, false);
  assert.equal((await batchedBlocks(poP, poR)).ok, false);

  // an rpc-only inert event-less block → both OK (tolerated on the asymmetric blocks comparison)
  const roP = [blockRow(100), blockRow(102)];
  const roR = [blockRow(100), blockRow(101), blockRow(102)];
  assert.equal(diffMjsBlocks(roP, roR).ok, true);
  assert.equal((await batchedBlocks(roP, roR)).ok, true);
});

test('#78 parity: both differs cover exactly the same tables (logs/txs/receipts/traces + blocks)', () => {
  // diff.mjs handles the four STRICT tables + blocks (as a separate keyed verdict); diff-batched lists
  // all five in TABLES. Route-a is only valid if the covered set is identical.
  const cellTables = new Set([...STRICT, 'blocks']);
  assert.deepEqual(
    [...cellTables].sort(),
    Object.keys(TABLES).sort(),
    'the paged differ must cover exactly the tables the cell differ did',
  );
});
