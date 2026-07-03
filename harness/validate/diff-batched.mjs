// Streaming / constant-memory byte-identity diff of two ponder_sync stores — the F-full variant of
// harness/diff/diff.mjs. Where diff.mjs loads whole tables into memory, this walks each table in
// ORDER BY key batches of 50k rows (keyset pagination) and merge-compares the two ordered streams,
// so peak memory is one batch per side regardless of table size (the Euler-eth full history is
// millions of rows). Tolerances match diff.mjs exactly:
//   - logs / transactions / transaction_receipts / traces : strict set + field identity
//   - blocks : total_difficulty excluded; blocks present on only one side are reported, not failed
//     (the stock RPC path stores inert event-less blocks it traced)
//
// Modes:
//   node diff-batched.mjs <pgliteDirA> <pgliteDirB> [--app-hash]   diff sync stores (exit 0/1)
//   node diff-batched.mjs --app-hash <pgliteDir> [--schema NAME]   ordered md5 over app tables
//
// The pure comparison + hashing core is exported for unit tests (fixture rows, no database).

import { createHash } from 'node:crypto';

// ── pure core (exported, tested on fixtures) ───────────────────────────────────────────────────

// Normalize a row to a stable JSON string: sorted keys, bigint→decimal, bytes→hex, dropped columns
// removed. Identical logical rows across the two pglite engines produce identical strings.
export function normRow(row, drop) {
  const o = {};
  for (const k of Object.keys(row).sort()) {
    if (drop?.has(k)) {
      continue;
    }

    const v = row[k];
    if (typeof v === 'bigint') {
      o[k] = v.toString();
    } else if (v instanceof Uint8Array) {
      o[k] = Buffer.from(v).toString('hex');
    } else {
      o[k] = v;
    }
  }

  return JSON.stringify(o);
}

// Lexicographic compare of two composite keys (arrays of bigint|number|string).
export function cmpKey(a, b) {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a[i];
    const y = b[i];
    if (x < y) {
      return -1;
    }
    if (x > y) {
      return 1;
    }
  }

  return a.length - b.length;
}

// Turn an array into an async iterator so the array (test) and DB (CLI) paths share one merge.
async function* fromArray(rows) {
  for (const r of rows) {
    yield r;
  }
}

// Streaming merge-compare of two key-ordered async row streams. `mode='strict'` fails on any
// only-one-side row; `mode='blocks'` reports them but only fails on a shared-key field mismatch.
// Returns counters and small samples (never the whole diff) so memory stays bounded.
export async function streamingDiff(
  iterA,
  iterB,
  { keyFn, drop, mode = 'strict' },
) {
  const itA = iterA[Symbol.asyncIterator]();
  const itB = iterB[Symbol.asyncIterator]();
  let a = await itA.next();
  let b = await itB.next();

  const res = {
    fail: false,
    aCount: 0,
    bCount: 0,
    onlyA: 0,
    onlyB: 0,
    mismatch: 0,
    shared: 0,
    samples: [],
  };
  const sample = (kind, text) => {
    if (res.samples.length < 6) {
      res.samples.push(`${kind}: ${text.slice(0, 300)}`);
    }
  };

  while (!a.done || !b.done) {
    const ka = a.done ? null : keyFn(a.value);
    const kb = b.done ? null : keyFn(b.value);
    let step;
    if (kb === null) {
      step = -1;
    } else if (ka === null) {
      step = 1;
    } else {
      step = cmpKey(ka, kb);
    }

    if (step < 0) {
      res.aCount++;
      res.onlyA++;
      if (mode === 'strict') {
        res.fail = true;
        sample('A-only', normRow(a.value, drop));
      }
      a = await itA.next();
    } else if (step > 0) {
      res.bCount++;
      res.onlyB++;
      if (mode === 'strict') {
        res.fail = true;
        sample('B-only', normRow(b.value, drop));
      }
      b = await itB.next();
    } else {
      res.aCount++;
      res.bCount++;
      res.shared++;
      const na = normRow(a.value, drop);
      const nb = normRow(b.value, drop);
      if (na !== nb) {
        res.mismatch++;
        res.fail = true;
        sample('A', na);
        sample('B', nb);
      }
      a = await itA.next();
      b = await itB.next();
    }
  }

  return res;
}

// Array convenience wrapper used by the tests — inputs must already be key-ordered.
export function mergeCompare(rowsA, rowsB, opts) {
  return streamingDiff(fromArray(rowsA), fromArray(rowsB), opts);
}

// Ordered md5 over a set of rows (canonical order = by key). Same rows in any input order → same
// hash, so it is a determinism checkpoint for app tables.
export function hashRows(rows, keyFn, drop) {
  const lines = rows
    .map((r) => ({ k: keyFn(r), s: normRow(r, drop) }))
    .sort((x, y) => cmpKey(x.k, y.k))
    .map((x) => x.s);
  const h = createHash('md5');
  for (const line of lines) {
    h.update(line);
    h.update('\n');
  }

  return h.digest('hex');
}

// ── DB adapters + CLI (only run when invoked directly; pglite is imported lazily so the pure
//    exports above load with zero dependencies in the repo test runner) ─────────────────────────

// Key columns + comparison drop-set per sync-store table. A single-chain diff is uniquely ordered by
// these; the campaign always diffs one chain at a time.
const TABLES = {
  logs: { keys: ['block_number', 'log_index'], mode: 'strict' },
  transactions: { keys: ['block_number', 'transaction_index'], mode: 'strict' },
  transaction_receipts: {
    keys: ['block_number', 'transaction_index'],
    mode: 'strict',
  },
  traces: {
    keys: ['block_number', 'transaction_index', 'trace_index'],
    mode: 'strict',
  },
  blocks: {
    keys: ['number'],
    mode: 'blocks',
    drop: new Set(['total_difficulty']),
  },
};

const BATCH = 50_000;

const toBig = (v) => (typeof v === 'bigint' ? v : BigInt(v));

// Keyset-paginated async row stream: pulls BATCH rows at a time ordered by `keys`, holding at most
// one batch in memory. `keys` are numeric sync-store columns (block/index) → BigInt-comparable.
async function* keysetRows(db, table, keys) {
  const cols = keys.map((k) => `"${k}"`).join(', ');
  const order = `order by ${cols}`;
  let last = null;
  for (;;) {
    let where = '';
    const params = [];
    if (last) {
      // (k0,k1,…) > (last0,last1,…) — row-wise tuple comparison for a stable keyset cursor.
      const lhs = `(${cols})`;
      const rhs = `(${keys.map((_, i) => `$${i + 1}`).join(', ')})`;
      where = `where ${lhs} > ${rhs}`;
      for (const k of keys) {
        params.push(last[k]);
      }
    }

    const sql = `select * from ponder_sync."${table}" ${where} ${order} limit ${BATCH}`;
    const { rows } = await db.query(sql, params);
    if (rows.length === 0) {
      return;
    }

    for (const r of rows) {
      yield r;
    }

    if (rows.length < BATCH) {
      return;
    }

    const tail = rows[rows.length - 1];
    last = {};
    for (const k of keys) {
      last[k] = tail[k];
    }
  }
}

function keyFnFor(keys) {
  return (row) => keys.map((k) => toBig(row[k]));
}

async function diffStores(dirA, dirB, { PGlite }) {
  const dbA = await PGlite.create(dirA);
  const dbB = await PGlite.create(dirB);
  let fail = 0;
  console.log(
    `\nbatched byte-identity diff  (portal=${dirA}  vs  rpc=${dirB})\n`,
  );

  for (const [table, spec] of Object.entries(TABLES)) {
    const keyFn = keyFnFor(spec.keys);
    const streamA = keysetRows(dbA, table, spec.keys);
    const streamB = keysetRows(dbB, table, spec.keys);
    const r = await streamingDiff(streamA, streamB, {
      keyFn,
      drop: spec.drop,
      mode: spec.mode,
    });
    const ok = !r.fail;
    if (!ok) {
      fail = 1;
    }

    const extra =
      spec.mode === 'blocks'
        ? `  shared=${r.shared} match` +
          (r.onlyB ? `, +${r.onlyB} inert event-less (RPC-only)` : '') +
          (r.onlyA ? `, +${r.onlyA} portal-only` : '') +
          (r.mismatch ? `  | ${r.mismatch} shared MISMATCH` : '')
        : r.fail
          ? `  | portal-only=${r.onlyA} rpc-only=${r.onlyB} mismatch=${r.mismatch}`
          : '  identical';
    console.log(
      `  ${ok ? '✅' : '❌'} ${table.padEnd(20)} portal=${String(r.aCount).padStart(8)}  rpc=${String(r.bCount).padStart(8)}${extra}`,
    );
    for (const s of r.samples) {
      console.log(`       ${s}`);
    }
  }

  await dbA.close();
  await dbB.close();
  console.log(
    fail
      ? '\n❌ DIVERGENCE — not byte-identical (see rows above)\n'
      : '\n✅ BYTE-IDENTICAL across logs / transactions / receipts / traces (event-bearing blocks too)\n',
  );
  console.log(`RESULT_JSON ${JSON.stringify({ fail: !!fail })}`);

  return fail;
}

// Ordered md5 over every user table in `schema` (the app tables ponder writes from ponder.schema.ts).
// Deterministic column order + ORDER BY the row's canonical text repr → identical data ⇒ identical
// hash on both sides, independent of physical row order. This is the determinism checkpoint.
async function appHash(dir, schema, { PGlite }) {
  const db = await PGlite.create(dir);
  const resolvedSchema = schema ?? (await pickAppSchema(db));
  // Only the USER tables (from ponder.schema.ts) are the determinism signal. ponder reserves the
  // `_` prefix for its own tables (_ponder_checkpoint, _ponder_meta, _reorg__*), which carry
  // per-run state (build id, checkpoints) that legitimately differs between two runs — exclude them.
  const { rows: tables } = await db.query(
    `select table_name from information_schema.tables
       where table_schema=$1 and table_type='BASE TABLE' and substr(table_name,1,1) <> '_'
       order by table_name`,
    [resolvedSchema],
  );

  const out = { schema: resolvedSchema, tables: {} };
  const combined = createHash('md5');
  for (const { table_name: t } of tables) {
    const { rows: cols } = await db.query(
      `select column_name from information_schema.columns
         where table_schema=$1 and table_name=$2 order by column_name`,
      [resolvedSchema, t],
    );
    if (cols.length === 0) {
      continue;
    }

    const repr = cols
      .map((c) => `coalesce("${c.column_name}"::text,'∅')`)
      .join(`||'|'||`);
    const { rows } = await db.query(
      `select coalesce(md5(string_agg(r, chr(10) order by r)), 'empty') as h
         from (select ${repr} as r from "${resolvedSchema}"."${t}") s`,
    );
    const h = rows[0].h;
    out.tables[t] = h;
    combined.update(`${t}:${h}\n`);
  }

  await db.close();
  out.combined = combined.digest('hex');

  return out;
}

// The app schema = the one non-system, non-ponder_sync schema ponder created for this instance.
async function pickAppSchema(db) {
  const { rows } = await db.query(
    `select schema_name from information_schema.schemata
       where schema_name not in ('ponder_sync','information_schema','pg_catalog','pg_toast','public')
         and schema_name not like 'pg_%' order by schema_name`,
  );
  if (rows.length === 0) {
    return 'public';
  }

  return rows[0].schema_name;
}

async function main() {
  const argv = process.argv.slice(2);
  const { PGlite } = await import('@electric-sql/pglite');

  if (argv[0] === '--app-hash') {
    const dir = argv[1];
    const schemaFlag = argv.indexOf('--schema');
    const schema = schemaFlag >= 0 ? argv[schemaFlag + 1] : undefined;
    if (!dir) {
      console.error(
        'usage: diff-batched.mjs --app-hash <pgliteDir> [--schema NAME]',
      );
      process.exit(2);
    }

    console.log(
      JSON.stringify(await appHash(dir, schema, { PGlite }), null, 2),
    );

    return;
  }

  const [dirA, dirB] = argv;
  if (!dirA || !dirB) {
    console.error(
      'usage: diff-batched.mjs <pgliteDirA> <pgliteDirB> [--app-hash]',
    );
    process.exit(2);
  }

  const fail = await diffStores(dirA, dirB, { PGlite });

  if (argv.includes('--app-hash')) {
    const [ha, hb] = await Promise.all([
      appHash(dirA, undefined, { PGlite }),
      appHash(dirB, undefined, { PGlite }),
    ]);
    const same = ha.combined === hb.combined;
    console.log(
      `\napp-table determinism hash  portal=${ha.combined}  rpc=${hb.combined}`,
    );
    console.log(same ? '✅ app tables identical' : '❌ app tables DIVERGE');
    if (!same) {
      process.exit(1);
    }
  }

  process.exit(fail);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(e);
    process.exit(2);
  });
}
