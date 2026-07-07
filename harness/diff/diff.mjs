// Byte-identity diff of two ponder_sync stores (Portal-backfilled vs RPC-backfilled).
// Compares the full row SET of each table (normalized JSON), so it needs no column-name
// assumptions; both DBs use the same pglite engine, so representations are directly comparable.
//   node diff.mjs <pgliteDirA> <pgliteDirB>     exit 0 = identical, 1 = divergence
//
// The pure comparison core (setDiff, blocksVerdict) is exported for unit tests; pglite is imported
// lazily inside main() so the exports load with zero dependencies in the repo test runner.

// strict byte-identity is required for these four paths
// (exported so a verdict-parity test can assert this differ and the paged diff-batched.mjs cover the
//  same tables — the run.sh default now routes the cell path through diff-batched.mjs, issue #78)
export const STRICT = [
  'logs',
  'transactions',
  'transaction_receipts',
  'traces',
];

// total_difficulty is meaningless post-Merge and RPC-dependent (null vs "0" vs the frozen TTD),
// so it's not a Portal-vs-RPC parity signal — exclude it from the block comparison.
const BLOCK_DROP = new Set(['total_difficulty']);

// exported so a verdict-parity test can normalize fixture rows with THIS differ's own normalizer and
// prove it matches diff-batched.mjs `normRow` byte-for-byte (issue #78 route-a parity).
export const norm = (row, drop) => {
  const o = {};
  for (const k of Object.keys(row).sort()) {
    if (drop?.has(k)) continue;
    const v = row[k];
    o[k] =
      typeof v === 'bigint'
        ? v.toString()
        : v instanceof Uint8Array
          ? Buffer.from(v).toString('hex')
          : v;
  }
  return JSON.stringify(o);
};

// ── pure comparison core (exported, tested on fixture row-strings) ──────────────────────────────

// Strict set-identity of two normalized row-string arrays: any row on only one side fails.
export function setDiff(a, b) {
  const sa = new Set(a);
  const sb = new Set(b);
  const onlyA = a.filter((x) => !sb.has(x));
  const onlyB = b.filter((x) => !sa.has(x));
  const ok = onlyA.length === 0 && onlyB.length === 0 && a.length === b.length;

  return { ok, onlyA, onlyB };
}

// ── known upstream defect: block.size off-by-one (issue #76) ─────────────────────────────────────
// The SQD Portal eth-mainnet dataset computes a block's `size` with a fixed 2-byte RLP
// length-of-length prefix, so every block whose RLP payload crosses 2^16 (canonical size ≥ 65540,
// which needs a 3-byte length prefix) is reported one byte short: portal.size === rpc.size − 1. The
// block hash and every consensus field are identical, and logs/transactions/receipts/traces are
// byte-identical; only this one derived header field differs, on ~0.3% of mainnet blocks (the large
// ones). The fork's transform is a pass-through, so the value is upstream data, not our code (issue
// #76 tracks the dataset fix). We tolerate a shared block row whose ONLY differing field is `size`
// with EXACTLY that signature — delta precisely +1, only at/above the 65540 boundary. Anything else
// about size (a different delta, the opposite sign, a sub-threshold delta) OR any second differing
// field still FAILS. Self-retiring: once the dataset is fixed the rows compare equal and this never
// fires.
const SIZE_TOLERANCE_MIN = 65540;

// portalRow / rpcRow are normalized block row-strings (norm output: sorted keys, bigint→decimal,
// bytes→hex, total_difficulty dropped). Returns true iff `size` is the SOLE differing field and the
// pair matches the issue-#76 off-by-one signature (rpc === portal + 1, rpc ≥ 65540).
function sizeOffByOneTolerated(portalRow, rpcRow) {
  const p = JSON.parse(portalRow);
  const r = JSON.parse(rpcRow);

  let diffField = null;
  for (const k of new Set([...Object.keys(p), ...Object.keys(r)])) {
    if (p[k] === r[k]) continue;

    if (diffField !== null) return false;

    diffField = k;
  }

  if (diffField !== 'size') return false;

  const portalSize = Number(p.size);
  const rpcSize = Number(r.size);

  return (
    Number.isFinite(portalSize) &&
    Number.isFinite(rpcSize) &&
    rpcSize === portalSize + 1 &&
    rpcSize >= SIZE_TOLERANCE_MIN
  );
}

// Block-identity keyed by (CHAIN_ID, NUMBER) — parsed from each normalized row-string. Rules:
//   • a (chain,number) PRESENT on both sides whose full-row string differs → MISMATCH → FAIL
//     (keying by hash would hide a same-number/different-hash reorg divergence as two one-sided
//      extras that the old `ok` never checked) — EXCEPT the known upstream block.size off-by-one
//      (issue #76): a shared block whose ONLY differing field is `size` with rpc === portal + 1 and
//      rpc ≥ 65540 is classified `sizeTolerated`, not `mismatch`, so it does not fail (self-retiring)
//   • a portal-only (chain,number) (in A, not B) → FAIL — the Portal path invented a block RPC never saw
//   • an rpc-only (chain,number) (in B, not A) → tolerated: the stock RPC path stores inert event-less
//     blocks it traced (never referenced)
// A same-(chain,number) pair is counted ONCE as shared (match or mismatch) — never double-counted as
// two one-sided extras.
//
// The key MUST include chain_id: blocks_pkey is (chain_id, number), so in a multi-chain store two
// different chains legitimately hold the same block number. Keying by number alone would collapse
// same-height blocks from different chains into one Map slot — the last-inserted row wins and silently
// overwrites the others, so a real per-chain divergence can hide behind a matching sibling chain
// (proven false-pass). chain_id is a COMPARE key, never a drop key; for a single-chain store it is a
// constant column, so the verdict is unchanged.
export function blocksVerdict(
  aRows,
  bRows,
  keyOf = (r) => {
    const o = JSON.parse(r);
    return `${o.chain_id}:${o.number}`;
  },
) {
  const a = new Map(aRows.map((r) => [keyOf(r), r]));
  const b = new Map(bRows.map((r) => [keyOf(r), r]));
  const shared = [...a.keys()].filter((n) => b.has(n));

  // A shared key whose row-strings differ is a MISMATCH, EXCEPT the tolerated upstream size
  // off-by-one (issue #76) — those are split into sizeTolerated and do NOT fail.
  const mismatch = [];
  const sizeTolerated = [];
  for (const n of shared) {
    if (a.get(n) === b.get(n)) continue;

    if (sizeOffByOneTolerated(a.get(n), b.get(n))) {
      sizeTolerated.push(n);
    } else {
      mismatch.push(n);
    }
  }

  const portalOnly = [...a.keys()].filter((n) => !b.has(n));
  const rpcExtra = [...b.keys()].filter((n) => !a.has(n));
  const ok = mismatch.length === 0 && portalOnly.length === 0;

  return {
    ok,
    aSize: a.size,
    bSize: b.size,
    shared,
    mismatch,
    sizeTolerated,
    portalOnly,
    rpcExtra,
    sampleA: (n) => a.get(n),
    sampleB: (n) => b.get(n),
  };
}

// ── DB adapters + CLI (pglite imported lazily) ─────────────────────────────────────────────────

async function main() {
  const [dirA, dirB] = process.argv.slice(2);
  if (!dirA || !dirB) {
    console.error('usage: diff.mjs <pgliteDirA> <pgliteDirB>');
    process.exit(2);
  }

  const { PGlite } = await import('@electric-sql/pglite');

  const dump = async (dir, table, drop) => {
    const db = await PGlite.create(dir);
    let rows = [];
    try {
      ({ rows } = await db.query(`select * from ponder_sync."${table}"`));
    } catch (e) {
      await db.close();
      throw new Error(`${table}@${dir}: ${e.message}`);
    }
    await db.close();
    return rows.map((r) => norm(r, drop)).sort();
  };

  let fail = 0;
  console.log(`\nbyte-identity diff  (portal=${dirA}  vs  rpc=${dirB})\n`);

  // the four required paths — strict set-identity
  for (const t of STRICT) {
    const [a, b] = await Promise.all([dump(dirA, t), dump(dirB, t)]);
    const { ok, onlyA, onlyB } = setDiff(a, b);
    console.log(
      `  ${ok ? '✅' : '❌'} ${t.padEnd(20)} portal=${String(a.length).padStart(6)}  rpc=${String(b.length).padStart(6)}` +
        (ok
          ? '  identical'
          : `  | portal-only=${onlyA.length} rpc-only=${onlyB.length}`),
    );
    if (!ok) {
      fail = 1;
      for (const x of onlyA.slice(0, 2))
        console.log('       portal-only:', x.slice(0, 320));
      for (const x of onlyB.slice(0, 2))
        console.log('       rpc-only:   ', x.slice(0, 320));
    }
  }

  // blocks: key by (CHAIN_ID, NUMBER) (see blocksVerdict). Same-key field mismatch and portal-only
  // blocks FAIL; only rpc-only inert event-less blocks are tolerated.
  {
    const [a, b] = await Promise.all([
      dump(dirA, 'blocks', BLOCK_DROP),
      dump(dirB, 'blocks', BLOCK_DROP),
    ]);
    const v = blocksVerdict(a, b);
    console.log(
      `  ${v.ok ? '✅' : '❌'} ${'blocks'.padEnd(20)} portal=${String(v.aSize).padStart(6)}  rpc=${String(v.bSize).padStart(6)}  shared=${v.shared.length} match` +
        (v.sizeTolerated.length
          ? `, ${v.sizeTolerated.length} tolerated (upstream size off-by-one, issue #76)`
          : '') +
        (v.rpcExtra.length
          ? `, +${v.rpcExtra.length} inert event-less (RPC-only)`
          : '') +
        (v.ok
          ? ''
          : `  | ${v.mismatch.length} shared MISMATCH, ${v.portalOnly.length} portal-only`),
    );
    if (!v.ok) {
      fail = 1;
      for (const n of v.mismatch.slice(0, 2)) {
        console.log('       portal:', v.sampleA(n).slice(0, 320));
        console.log('       rpc:   ', v.sampleB(n).slice(0, 320));
      }
      for (const n of v.portalOnly.slice(0, 2)) {
        console.log('       portal-only block:', v.sampleA(n).slice(0, 320));
      }
    }
  }

  console.log(
    fail
      ? '\n❌ DIVERGENCE — not byte-identical (see rows above)\n'
      : '\n✅ BYTE-IDENTICAL across logs / transactions / receipts / traces (event-bearing blocks too)\n',
  );
  process.exit(fail);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(e);
    process.exit(2);
  });
}
