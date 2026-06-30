// Byte-identity diff of two ponder_sync stores (Portal-backfilled vs RPC-backfilled).
// Compares the full row SET of each table (normalized JSON), so it needs no column-name
// assumptions; both DBs use the same pglite engine, so representations are directly comparable.
//   node diff.mjs <pgliteDirA> <pgliteDirB>     exit 0 = identical, 1 = divergence
import { PGlite } from "@electric-sql/pglite";

const [dirA, dirB] = process.argv.slice(2);
if (!dirA || !dirB) { console.error("usage: diff.mjs <pgliteDirA> <pgliteDirB>"); process.exit(2); }

// strict byte-identity is required for these four paths
const STRICT = ["logs", "transactions", "transaction_receipts", "traces"];

// total_difficulty is meaningless post-Merge and RPC-dependent (null vs "0" vs the frozen TTD),
// so it's not a Portal-vs-RPC parity signal — exclude it from the block comparison.
const BLOCK_DROP = new Set(["total_difficulty"]);

const norm = (row, drop) => {
  const o = {};
  for (const k of Object.keys(row).sort()) {
    if (drop && drop.has(k)) continue;
    const v = row[k];
    o[k] = typeof v === "bigint" ? v.toString() : v instanceof Uint8Array ? Buffer.from(v).toString("hex") : v;
  }
  return JSON.stringify(o);
};

const dump = async (dir, table, drop) => {
  const db = await PGlite.create(dir);
  let rows = [];
  try { ({ rows } = await db.query(`select * from ponder_sync."${table}"`)); }
  catch (e) { await db.close(); throw new Error(`${table}@${dir}: ${e.message}`); }
  await db.close();
  return rows.map((r) => norm(r, drop)).sort();
};

let fail = 0;
console.log(`\nbyte-identity diff  (portal=${dirA}  vs  rpc=${dirB})\n`);

// the four required paths — strict set-identity
for (const t of STRICT) {
  const [a, b] = await Promise.all([dump(dirA, t), dump(dirB, t)]);
  const sa = new Set(a), sb = new Set(b);
  const onlyA = a.filter((x) => !sb.has(x));
  const onlyB = b.filter((x) => !sa.has(x));
  const ok = onlyA.length === 0 && onlyB.length === 0 && a.length === b.length;
  console.log(`  ${ok ? "✅" : "❌"} ${t.padEnd(20)} portal=${String(a.length).padStart(6)}  rpc=${String(b.length).padStart(6)}` + (ok ? "  identical" : `  | portal-only=${onlyA.length} rpc-only=${onlyB.length}`));
  if (!ok) {
    fail = 1;
    for (const x of onlyA.slice(0, 2)) console.log("       portal-only:", x.slice(0, 320));
    for (const x of onlyB.slice(0, 2)) console.log("       rpc-only:   ", x.slice(0, 320));
  }
}

// blocks: every block PRESENT in both must match field-for-field; the stock RPC path additionally
// stores event-less blocks it traced (inert, never referenced) — reported, not failed.
{
  const byHash = (rows) => new Map(rows.map((r) => [JSON.parse(r).hash, r]));
  const [a, b] = await Promise.all([dump(dirA, "blocks", BLOCK_DROP).then(byHash), dump(dirB, "blocks", BLOCK_DROP).then(byHash)]);
  const shared = [...a.keys()].filter((h) => b.has(h));
  const mismatch = shared.filter((h) => a.get(h) !== b.get(h));
  const rpcExtra = [...b.keys()].filter((h) => !a.has(h)).length;
  const ok = mismatch.length === 0;
  console.log(`  ${ok ? "✅" : "❌"} ${"blocks".padEnd(20)} portal=${String(a.size).padStart(6)}  rpc=${String(b.size).padStart(6)}  shared=${shared.length} match` + (rpcExtra ? `, +${rpcExtra} inert event-less (RPC-only)` : "") + (ok ? "" : `  | ${mismatch.length} shared MISMATCH`));
  if (!ok) { fail = 1; for (const h of mismatch.slice(0, 2)) { console.log("       portal:", a.get(h).slice(0, 320)); console.log("       rpc:   ", b.get(h).slice(0, 320)); } }
}

console.log(fail ? "\n❌ DIVERGENCE — not byte-identical (see rows above)\n" : "\n✅ BYTE-IDENTICAL across logs / transactions / receipts / traces (event-bearing blocks too)\n");
process.exit(fail);
