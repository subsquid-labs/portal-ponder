// Byte-identity diff of two ponder_sync stores (Portal-backfilled vs RPC-backfilled).
// Compares the full row SET of each table (normalized JSON), so it needs no column-name
// assumptions; both DBs use the same pglite engine, so representations are directly comparable.
//   node diff.mjs <pgliteDirA> <pgliteDirB>     exit 0 = identical, 1 = divergence
//
// The pure comparison core (setDiff, blocksVerdict) is exported for unit tests; pglite is imported
// lazily inside main() so the exports load with zero dependencies in the repo test runner.

// strict byte-identity is required for these four paths
const STRICT = ['logs', 'transactions', 'transaction_receipts', 'traces'];

// total_difficulty is meaningless post-Merge and RPC-dependent (null vs "0" vs the frozen TTD),
// so it's not a Portal-vs-RPC parity signal — exclude it from the block comparison.
const BLOCK_DROP = new Set(['total_difficulty']);

const norm = (row, drop) => {
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

// Block-identity keyed by NUMBER (parsed from each normalized row-string). Rules:
//   • a number PRESENT on both sides whose full-row string differs → MISMATCH → FAIL
//     (keying by hash would hide a same-number/different-hash reorg divergence as two one-sided
//      extras that the old `ok` never checked)
//   • a portal-only number (in A, not B) → FAIL — the Portal path invented a block RPC never saw
//   • an rpc-only number (in B, not A) → tolerated: the stock RPC path stores inert event-less
//     blocks it traced (never referenced)
// A same-number pair is counted ONCE as shared (match or mismatch) — never double-counted as two
// one-sided extras.
export function blocksVerdict(
  aRows,
  bRows,
  keyOf = (r) => JSON.parse(r).number,
) {
  const a = new Map(aRows.map((r) => [keyOf(r), r]));
  const b = new Map(bRows.map((r) => [keyOf(r), r]));
  const shared = [...a.keys()].filter((n) => b.has(n));
  const mismatch = shared.filter((n) => a.get(n) !== b.get(n));
  const portalOnly = [...a.keys()].filter((n) => !b.has(n));
  const rpcExtra = [...b.keys()].filter((n) => !a.has(n));
  const ok = mismatch.length === 0 && portalOnly.length === 0;

  return {
    ok,
    aSize: a.size,
    bSize: b.size,
    shared,
    mismatch,
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

  // blocks: key by NUMBER (see blocksVerdict). Same-number field mismatch and portal-only blocks
  // FAIL; only rpc-only inert event-less blocks are tolerated.
  {
    const [a, b] = await Promise.all([
      dump(dirA, 'blocks', BLOCK_DROP),
      dump(dirB, 'blocks', BLOCK_DROP),
    ]);
    const v = blocksVerdict(a, b);
    console.log(
      `  ${v.ok ? '✅' : '❌'} ${'blocks'.padEnd(20)} portal=${String(v.aSize).padStart(6)}  rpc=${String(v.bSize).padStart(6)}  shared=${v.shared.length} match` +
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
