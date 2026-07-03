// Streaming / constant-memory byte-identity diff of two ponder_sync stores — the F-full variant of
// harness/diff/diff.mjs. Where diff.mjs loads whole tables into memory, this walks each table in
// ORDER BY key batches of 50k rows (keyset pagination) and merge-compares the two ordered streams,
// so peak memory is one batch per side regardless of table size (the Euler-eth full history is
// millions of rows). Tolerances match diff.mjs exactly:
//   - logs / transactions / transaction_receipts / traces : strict set + field identity
//   - blocks : total_difficulty excluded; ASYMMETRIC by default — a portal-only block (A) FAILS (the
//     Portal path invented a block RPC never saw); only an rpc-only block (B) is tolerated (the stock
//     RPC path stores inert event-less blocks it traced); a shared-key field mismatch always FAILS.
//     This asymmetry is CALIBRATED FOR portal-vs-RPC (A=portal, B=rpc). For a portal-vs-PORTAL diff
//     (e.g. the chaos verify: a resumed store vs a clean baseline, both Portal-built) there is no
//     inert-block asymmetry — a B-only (baseline-only) block means the resumed store is MISSING a
//     block, which MUST fail. Pass STRICT_BLOCKS=1 (or --strict-blocks) to force the `blocks` table
//     to mode:'strict' so a one-sided block on EITHER side fails.
//
// Modes:
//   node diff-batched.mjs <pgliteDirA> <pgliteDirB> [--app-hash]   diff sync stores (exit 0/1)
//   node diff-batched.mjs --app-hash <pgliteDir> [--schema NAME]   ordered md5 over app tables
//   STRICT_BLOCKS=1 node diff-batched.mjs A B                       blocks table strict (portal-vs-portal)
//
// The pure comparison + hashing core is exported for unit tests (fixture rows, no database).

import { createHash } from 'node:crypto';

// ── pure core (exported, tested on fixtures) ───────────────────────────────────────────────────

// A JSON.stringify replacer so a bigint ANYWHERE (top-level column OR nested, e.g. a composite
// {key:[bigint]} sample row) serializes to its decimal string instead of throwing. DB rows carry
// only scalar columns, so this is a no-op there; it just makes the sampler total.
const bigintSafe = (_k, v) => (typeof v === 'bigint' ? v.toString() : v);

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

  return JSON.stringify(o, bigintSafe);
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

// Streaming merge-compare of two key-ordered async row streams. Modes:
//   • 'strict' : ANY only-one-side row fails, plus any shared-key field mismatch.
//   • 'blocks' : ASYMMETRIC, mirroring harness/diff/diff.mjs `blocksVerdict`. A is the Portal store
//     and B is the stock-RPC store (see diffStores / run.sh: dirA=portal, dirB=rpc). A portal-only
//     block (onlyA) is a block the Portal path invented that RPC never saw → FAIL. An rpc-only block
//     (onlyB) is a tolerated inert event-less block the stock RPC path traced but never referenced →
//     reported, not failed. A shared-key field mismatch is always a FAIL. Before this fix 'blocks'
//     was vacuous — it failed only on a shared mismatch and let a portal-only block sail through the
//     F-full differ.
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
      // A-only (portal-only) fails under BOTH strict and blocks: strict tolerates nothing, and a
      // portal-only block is a block the Portal path invented that RPC never saw (asymmetric blocks).
      if (mode === 'strict' || mode === 'blocks') {
        res.fail = true;
        sample('A-only', normRow(a.value, drop));
      }
      a = await itA.next();
    } else if (step > 0) {
      res.bCount++;
      res.onlyB++;
      // B-only (rpc-only) fails ONLY under strict; under blocks it is the tolerated inert event-less
      // block the stock RPC path traced but never referenced.
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

// Resolve the per-table comparison specs for a run. `strictBlocks` promotes the `blocks` table from
// its default ASYMMETRIC 'blocks' mode to 'strict': the asymmetry only holds for portal-vs-RPC (a
// tolerated inert RPC-only block); for a portal-vs-PORTAL diff (chaos resume vs baseline) a one-sided
// block on EITHER side is a real gap and must fail. Pure + exported so a test can assert the override
// flips ONLY the blocks mode (and a mutation that ignores the flag is caught).
export function resolveTableSpecs(strictBlocks) {
  if (!strictBlocks) {
    return TABLES;
  }

  const out = {};
  for (const [table, spec] of Object.entries(TABLES)) {
    out[table] = table === 'blocks' ? { ...spec, mode: 'strict' } : spec;
  }

  return out;
}

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

async function diffStores(dirA, dirB, { PGlite, strictBlocks = false }) {
  const dbA = await PGlite.create(dirA);
  const dbB = await PGlite.create(dirB);
  let fail = 0;
  const specs = resolveTableSpecs(strictBlocks);
  console.log(
    `\nbatched byte-identity diff  (A=${dirA}  vs  B=${dirB})${strictBlocks ? '  [STRICT_BLOCKS: portal-vs-portal]' : ''}\n`,
  );

  for (const [table, spec] of Object.entries(specs)) {
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

  const out = { schema: resolvedSchema, tables: {}, rowCounts: {} };
  const combined = createHash('md5');
  let nonEmptyTables = 0;
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
      `select count(*)::int as n, coalesce(md5(string_agg(r, chr(10) order by r)), 'empty') as h
         from (select ${repr} as r from "${resolvedSchema}"."${t}") s`,
    );
    const h = rows[0].h;
    const n = rows[0].n;
    out.tables[t] = h;
    out.rowCounts[t] = n;
    if (n > 0) {
      nonEmptyTables += 1;
    }

    combined.update(`${t}:${h}\n`);
  }

  await db.close();
  out.combined = combined.digest('hex');
  // The determinism checkpoint is only meaningful over tables the app actually WROTE. The diff apps
  // ship a no-op `noop` table (they exist to populate ponder_sync, not user tables), so an app hash
  // over zero nonempty user tables is VACUOUS — it must not read as a meaningful PASS.
  out.nonEmptyTables = nonEmptyTables;

  return out;
}

// Verdict for the two-store --app-hash comparison. A meaningful PASS requires BOTH sides to have at
// least one nonempty user table AND identical combined hashes. Zero nonempty user tables on either
// side is NOT a pass — it is an explicit NO-USER-TABLES verdict (the diff apps write no user rows),
// so the determinism checkpoint can never be silently vacuous. Pure + exported for unit tests.
export function appHashVerdict(ha, hb) {
  if (ha.nonEmptyTables === 0 || hb.nonEmptyTables === 0) {
    return {
      ok: false,
      verdict: 'NO-USER-TABLES',
      reason:
        'app hash is vacuous — no nonempty user tables to compare (the diff apps write only ' +
        'ponder_sync, no user rows). Use an app that writes deterministic rows for a real checkpoint.',
    };
  }

  if (ha.combined !== hb.combined) {
    return { ok: false, verdict: 'DIVERGE', reason: 'app-table hashes differ' };
  }

  return { ok: true, verdict: 'PASS', reason: 'app tables identical' };
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
      'usage: diff-batched.mjs <pgliteDirA> <pgliteDirB> [--app-hash] [--strict-blocks]',
    );
    process.exit(2);
  }

  // STRICT_BLOCKS=1 or --strict-blocks promotes the blocks table to strict for a portal-vs-PORTAL
  // diff (e.g. chaos resume vs baseline) where a one-sided block on EITHER side is a real gap.
  const strictBlocks =
    process.env.STRICT_BLOCKS === '1' || argv.includes('--strict-blocks');
  const fail = await diffStores(dirA, dirB, { PGlite, strictBlocks });

  if (argv.includes('--app-hash')) {
    const [ha, hb] = await Promise.all([
      appHash(dirA, undefined, { PGlite }),
      appHash(dirB, undefined, { PGlite }),
    ]);
    const v = appHashVerdict(ha, hb);
    console.log(
      `\napp-table determinism hash  portal=${ha.combined} (${ha.nonEmptyTables} nonempty)  rpc=${hb.combined} (${hb.nonEmptyTables} nonempty)`,
    );
    console.log(v.ok ? `✅ ${v.reason}` : `❌ ${v.verdict}: ${v.reason}`);
    if (!v.ok) {
      // a vacuous (NO-USER-TABLES) or divergent app hash is a non-zero exit: the determinism
      // checkpoint must never silently pass on zero user rows.
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
