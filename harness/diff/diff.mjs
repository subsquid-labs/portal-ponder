// Byte-identity diff of two ponder_sync stores (Portal-backfilled vs RPC-backfilled).
// Compares the full row SET of each table (normalized JSON), so it needs no column-name
// assumptions; both DBs use the same pglite engine, so representations are directly comparable.
//   node diff.mjs <pgliteDirA> <pgliteDirB>     exit 0 = identical, 1 = divergence
import { PGlite } from "@electric-sql/pglite";

const [dirA, dirB] = process.argv.slice(2);
if (!dirA || !dirB) { console.error("usage: diff.mjs <pgliteDirA> <pgliteDirB>"); process.exit(2); }

// the source paths we backfill: logs · transactions · receipts · traces (+ blocks for context)
const TABLES = ["logs", "transactions", "transaction_receipts", "traces", "blocks"];

const norm = (row) => {
  const o = {};
  for (const k of Object.keys(row).sort()) {
    const v = row[k];
    o[k] = typeof v === "bigint" ? v.toString() : v instanceof Uint8Array ? Buffer.from(v).toString("hex") : v;
  }
  return JSON.stringify(o);
};

const dump = async (dir, table) => {
  const db = await PGlite.create(dir);
  let rows = [];
  try { ({ rows } = await db.query(`select * from ponder_sync."${table}"`)); }
  catch (e) { await db.close(); throw new Error(`${table}@${dir}: ${e.message}`); }
  await db.close();
  return rows.map(norm).sort();
};

let fail = 0;
console.log(`\nbyte-identity diff  (portal=${dirA}  vs  rpc=${dirB})\n`);
for (const t of TABLES) {
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
console.log(fail ? "\n❌ DIVERGENCE — not byte-identical (see rows above)\n" : "\n✅ BYTE-IDENTICAL across logs / transactions / receipts / traces\n");
process.exit(fail);
