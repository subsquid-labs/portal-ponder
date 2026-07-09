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

// ── known upstream derivation artifact: block.size only-diff (issues #76, #106) ──────────────────
// `size` is a node-DERIVED, non-consensus header field — it is NOT committed by the block hash, so
// different sources legitimately compute it differently. Two dataset signatures have been observed,
// both with an IDENTICAL block hash and byte-identical logs/transactions/receipts/traces:
//   • #76 (eth-mainnet): a fixed 2-byte RLP length-of-length prefix reports every block whose RLP
//     payload crosses 2^16 (canonical size ≥ 65540) one byte short — portal.size === rpc.size − 1.
//   • #106 (BSC / chain 56): pervasive portal === rpc + 1 (opposite sign, below 65540) plus
//     occasional large size-only deltas — same block hash on both sides throughout.
// Both are instances of the same thing: a source-side size derivation artifact, never a Portal
// content defect. So we tolerate a shared block row whose SOLE differing field is `size`, regardless
// of the delta's magnitude or sign.
//
// SAFETY INVARIANT (why this cannot mask a real divergence): `hash` is a COMPARED field. The block
// hash commits every consensus field, so an equal, non-empty hash on both sides ⟺ the same canonical
// block — a size-only difference over a matching hash is provably a derivation artifact. We anchor on
// exactly that: tolerate only when both rows carry a present (non-null/non-empty) and EQUAL `hash`.
// Any SECOND differing field — including `hash` itself (a reorg/wrong-block divergence) — is the
// second diff, so the predicate returns false and blocksVerdict FAILS. Self-retiring: once a source
// aligns its size derivation the rows compare equal and this never fires.

// portalRow / rpcRow are normalized block row-strings (norm output: sorted keys, bigint→decimal,
// bytes→hex, total_difficulty dropped). Returns true iff `size` is the SOLE differing field AND both
// rows carry a present, equal `hash` (the safety anchor above). Delta magnitude and sign are
// irrelevant — a matching hash proves the canonical block is identical (issues #76, #106).
function sizeOnlyDiffTolerated(portalRow, rpcRow) {
  const p = JSON.parse(portalRow);
  const r = JSON.parse(rpcRow);

  let diffField = null;
  for (const k of new Set([...Object.keys(p), ...Object.keys(r)])) {
    if (p[k] === r[k]) continue;

    if (diffField !== null) return false;

    diffField = k;
  }

  if (diffField !== 'size') return false;

  // Safety anchor: a size-only diff is a derivation artifact ONLY over an identical canonical block,
  // which a present, equal hash proves. A missing/empty hash on either side is not anchored → FAIL.
  const { hash } = p;

  return hash !== null && hash !== undefined && hash !== '' && hash === r.hash;
}

// Block-identity keyed by (CHAIN_ID, NUMBER) — parsed from each normalized row-string. Rules:
//   • a (chain,number) PRESENT on both sides whose full-row string differs → MISMATCH → FAIL
//     (keying by hash would hide a same-number/different-hash reorg divergence as two one-sided
//      extras that the old `ok` never checked) — EXCEPT the known upstream block.size derivation
//      artifact (issues #76, #106): a shared block whose ONLY differing field is `size`, over a
//      present+equal hash, is classified `sizeTolerated`, not `mismatch`, so it does not fail
//      (any second differing field, including hash, still FAILS; self-retiring)
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

  // A shared key whose row-strings differ is a MISMATCH, EXCEPT the tolerated upstream size-only
  // derivation artifact (issues #76, #106) — those are split into sizeTolerated and do NOT fail.
  const mismatch = [];
  const sizeTolerated = [];
  for (const n of shared) {
    if (a.get(n) === b.get(n)) continue;

    if (sizeOnlyDiffTolerated(a.get(n), b.get(n))) {
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

// ── known upstream dataset gap: transactions.access_list column dropped (issues #83, #32) ─────────
// The SQD Portal dataset for base-mainnet, arbitrum-one, and avalanche-mainnet DROPS the
// `transactions.access_list` column (upstream #83-family gap, refining #32). On those chains the
// Portal-backfill leg (A) stores an HONEST SQL NULL for that column — the fork records the dropped
// value faithfully as NULL, not a fabricated `"[]"` (the old fork defect, fixed by #110/#111). The
// stock-RPC leg (B) backfills from a full node and stores the REAL populated access list. So a
// `transactions` row on exactly these chains can differ on `access_list` alone, while every other
// column and all logs/blocks are byte-identical. Mirrors harness/validate/diff-batched.mjs exactly.
//
// REGRESSION-SENTINEL INVARIANT: the tolerance fires ONLY when the Portal side (A) is SQL NULL. A
// non-NULL Portal value that differs from RPC — in particular a reappearing fabricated `"[]"` (the
// exact #110 defect) — returns false → real MISMATCH → FAIL. Two differing non-NULL values are never
// tolerated; any SECOND differing column also fails (column-scoped, not row-scoped). Self-retiring:
// once the Portal serves the column the rows compare equal and this never fires. SCOPED to the
// #83-family chains only — an access_list divergence on any other chain is a hard FAIL.

// The chain_id set the SQD Portal drops transactions.access_list for (base-mainnet 8453, arbitrum-one
// 42161, avalanche-mainnet 43114 — see harness/validate/cells.json). chain_id is a COMPARED column in
// every transactions row (the PK leads with it), so the scope is read from the row itself.
export const ACCESS_LIST_GAP_CHAINS = new Set([8453, 42161, 43114]);

// portalRow / rpcRow are normalized transactions row-strings (norm output: sorted keys, bigint→decimal,
// bytes→hex). Returns true iff `access_list` is the SOLE differing field, the Portal side (A) is SQL
// NULL, AND the row's chain_id is an in-scope #83-family chain. The RPC side (B) may be anything — only
// Portal-IS-NULL is tolerated (issues #83, #32).
function accessListColumnGapTolerated(portalRow, rpcRow) {
  const p = JSON.parse(portalRow);
  const r = JSON.parse(rpcRow);

  if (!ACCESS_LIST_GAP_CHAINS.has(Number(p.chain_id))) return false;

  let diffField = null;
  for (const k of new Set([...Object.keys(p), ...Object.keys(r)])) {
    if (p[k] === r[k]) continue;

    if (diffField !== null) return false;

    diffField = k;
  }

  if (diffField !== 'access_list') return false;

  // Regression sentinel: tolerate ONLY the honest dropped-column value (Portal side SQL NULL). A
  // non-NULL Portal value that differs from RPC (e.g. a reappearing fabricated "[]" — #110) FAILS.
  return p.access_list === null;
}

// Transaction-identity keyed by (CHAIN_ID, BLOCK_NUMBER, TRANSACTION_INDEX) — the transactions_pkey
// column order — parsed from each normalized row-string. STRICT one-sided semantics (a portal-only OR
// rpc-only tx FAILS — transactions is a required byte-identity table), with ONE tolerated shared-key
// class: the upstream access_list column gap (issues #83, #32). A shared tx whose ONLY differing column
// is `access_list`, where the Portal side (A) is SQL NULL and chain_id is a #83-family chain, is
// classified `accessListTolerated`, not `mismatch`, and does not fail. A non-NULL Portal value (incl. a
// reappearing fabricated "[]" — #110), any second differing column, or an out-of-scope chain FAILS.
// Keying by the full PK (not set-identity) is what lets a NULL-vs-populated access_list pair resolve as
// ONE shared row rather than two one-sided extras — the prerequisite for a per-column tolerance.
export function transactionsVerdict(
  aRows,
  bRows,
  keyOf = (r) => {
    const o = JSON.parse(r);
    return `${o.chain_id}:${o.block_number}:${o.transaction_index}`;
  },
) {
  const a = new Map(aRows.map((r) => [keyOf(r), r]));
  const b = new Map(bRows.map((r) => [keyOf(r), r]));
  const shared = [...a.keys()].filter((n) => b.has(n));

  const mismatch = [];
  const accessListTolerated = [];
  for (const n of shared) {
    if (a.get(n) === b.get(n)) continue;

    if (accessListColumnGapTolerated(a.get(n), b.get(n))) {
      accessListTolerated.push(n);
    } else {
      mismatch.push(n);
    }
  }

  const portalOnly = [...a.keys()].filter((n) => !b.has(n));
  const rpcOnly = [...b.keys()].filter((n) => !a.has(n));
  // STRICT: a one-sided tx on EITHER side is a real gap → FAIL (unlike blocks, no rpc-only tolerance).
  const ok =
    mismatch.length === 0 && portalOnly.length === 0 && rpcOnly.length === 0;

  return {
    ok,
    aSize: a.size,
    bSize: b.size,
    shared,
    mismatch,
    accessListTolerated,
    portalOnly,
    rpcOnly,
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

  // the required paths — strict set-identity. `transactions` is handled separately below (keyed
  // verdict) so the scoped access_list column-gap tolerance can resolve a NULL-vs-populated pair as one
  // shared row rather than two one-sided extras; the other three stay pure set-identity.
  for (const t of STRICT) {
    if (t === 'transactions') continue;

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

  // transactions: keyed by (CHAIN_ID, BLOCK_NUMBER, TRANSACTION_INDEX) (see transactionsVerdict).
  // STRICT one-sided (a portal-only OR rpc-only tx FAILS) with the scoped access_list column-gap
  // tolerance (issues #83/#32): a shared tx whose SOLE diff is a Portal-NULL access_list on a
  // #83-family chain is classified accessListTolerated and does NOT fail.
  {
    const [a, b] = await Promise.all([
      dump(dirA, 'transactions'),
      dump(dirB, 'transactions'),
    ]);
    const v = transactionsVerdict(a, b);
    console.log(
      `  ${v.ok ? '✅' : '❌'} ${'transactions'.padEnd(20)} portal=${String(v.aSize).padStart(6)}  rpc=${String(v.bSize).padStart(6)}  shared=${v.shared.length} match` +
        (v.accessListTolerated.length
          ? `, ${v.accessListTolerated.length} tolerated (Portal access_list column gap, issues #83/#32)`
          : '') +
        (v.ok
          ? ''
          : `  | ${v.mismatch.length} shared MISMATCH, ${v.portalOnly.length} portal-only, ${v.rpcOnly.length} rpc-only`),
    );
    if (!v.ok) {
      fail = 1;
      for (const n of v.mismatch.slice(0, 2)) {
        console.log('       portal:', v.sampleA(n).slice(0, 320));
        console.log('       rpc:   ', v.sampleB(n).slice(0, 320));
      }
      for (const n of v.portalOnly.slice(0, 2))
        console.log('       portal-only tx:', v.sampleA(n).slice(0, 320));
      for (const n of v.rpcOnly.slice(0, 2))
        console.log('       rpc-only tx:   ', v.sampleB(n).slice(0, 320));
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
          ? `, ${v.sizeTolerated.length} tolerated (upstream size-only derivation, issues #76/#106)`
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
