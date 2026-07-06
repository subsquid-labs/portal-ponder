// pg-digest.mjs — deterministic LOGICAL digest of a ponder_sync store on native Postgres, for the
// chaos-3-pg campaign (issue #52). This is the STORE-IDENTITY primitive that replaces the PGlite
// byte-diff of the v3 (PGlite) campaign.
//
// WHY logical, not byte: on a crash-DURABLE Postgres backend, a SIGKILL mid-write is recovered by WAL
// replay on the next start. Replay legitimately changes the PHYSICAL bytes on disk (different WAL
// segment offsets, checkpoint records, free-space-map, hint bits, page LSNs) while the LOGICAL row
// content is identical. So a byte-compare of two Postgres datadirs is WRONG here — it would flag a
// correctly-recovered, logically-identical store as different. The correct identity is a deterministic
// digest over ROW CONTENT.
//
// WHY logical means CONTENT, not the surrogate key (the run-20 false-FAIL, fixed here): several
// ponder_sync tables carry a SURROGATE serial `id` — a Postgres `GENERATED ALWAYS AS IDENTITY`
// column (`factories.id`, `factory_addresses.id`). That id is drawn from a SEQUENCE, and sequences
// are NON-TRANSACTIONAL: a SIGKILL that rolls back a flush transaction removes the rows but does NOT
// roll back the sequence, so the resume re-flushes byte-identical logical content at SHIFTED ids
// (chaos_run_20 had factory_addresses ids 9..16 for content that is ids 1..8 in the baseline). The
// surrogate id is store bookkeeping, not sync data — the LOGICAL identity of a factory-address row is
// (factory_id, chain_id, block_number, address). Digesting `to_jsonb(row)` verbatim bound the serial
// id into the hash and false-FAILED a perfectly-resumed store. So for tables with a surrogate id we
// digest the LOGICAL columns only — `to_jsonb(t) - 'id'` (jsonb minus the key) — and order by the
// natural key, NOT by the drifting serial PK. This makes the header's promise ("digest over ROW
// CONTENT") literally true: an id-shift is invisible, but any change to the actual row content — or a
// DUPLICATED row (same content under two ids, the run-2 two-writer shape) — still diverges the digest.
//
// COLUMN EXCLUSIONS (per DIGEST_TABLES member; each exclusion justified):
//   - factories.id, factory_addresses.id : surrogate serial (GENERATED ALWAYS AS IDENTITY), sequence-
//     backed and non-transactional → drifts across a killed+resumed flush while content is identical.
//     EXCLUDED from content; ordering switches to the natural key. Verified via information_schema:
//     these two are the ONLY tables in DIGEST_TABLES with a surrogate/identity column — blocks, logs,
//     transactions, transaction_receipts, traces, intervals all key on NATURAL columns (see below) and
//     have NO serial id, so nothing is excluded there.
//   No other nondeterministic column exists in the digest set: every remaining column is
//   content-bearing sync data (block/tx/log/receipt/trace fields, interval fragment_id + block ranges)
//   whose value is a deterministic function of the backfilled range, not of WHEN or in what order the
//   rows were written. (Wall-clock / cache tables are excluded at the TABLE level below.)
//
// HOW deterministic:
//   - per table: md5 over the concatenation of md5(logical_jsonb(row)::text) for every row, taken in a
//     TOTAL, deterministic ORDER. `logical_jsonb` is `to_jsonb(t)` minus any excluded surrogate key.
//     The ORDER BY is a TOTAL order with NO ties: it sorts by the natural-key columns and then, as a
//     final tie-break, by the full logical-jsonb text — so two rows can tie in the ORDER BY only when
//     their entire logical content is identical, in which case their contribution to the aggregate is
//     identical and their relative order is irrelevant. (A duplicated logical row therefore still
//     changes the row COUNT and the aggregate, so duplication is still detected.) Feeding md5(row)
//     (not the raw row) into string_agg keeps the aggregated string bounded and its ordering the ONLY
//     thing that matters; the outer md5 collapses it to 32 hex chars.
//   - to_jsonb(row) canonicalizes every column to a JSON scalar (numeric/text/bool/null) independent
//     of on-disk representation, so hint bits / page layout / vacuum state never leak into the digest.
//   - the store digest is md5 over the sorted list of "table=perTableDigest:rowcount" lines, so table
//     ORDER and per-table row COUNT both bind into the final digest.
//
// TABLE SET (DIGEST_TABLES): the authoritative sync-state tables — the same data the repo's byte-diff
// compares (blocks/logs/transactions/transaction_receipts/traces) PLUS intervals + factories +
// factory_addresses (durable sync bookkeeping). EXCLUDED, deliberately:
//   - kysely_migration / kysely_migration_lock : carry a wall-clock `timestamp` of WHEN migrations ran
//     → nondeterministic across builds; pure infra, not sync data.
//   - rpc_request_results : an RPC response CACHE (not authoritative sync state); Portal-path presence
//     is opportunistic. Excluded so the digest reflects the SYNC RESULT, not cache-population timing.
//   A table listed in DIGEST_TABLES that is ABSENT is a hard error (fail closed) — the schema shape is
//   part of the identity; a missing sync table must never be silently skipped to a matching digest.
//
//   node pg-digest.mjs <connString> [--schema ponder_sync] [--json]
//     stdout: the 32-hex store digest (or a JSON object with per-table detail under --json)
//     exit 0 on success; exit 2 on usage/connection/query error (fail closed — never a blank pass).

import { createHash } from 'node:crypto';

export const DIGEST_TABLES = [
  'blocks',
  'logs',
  'transactions',
  'transaction_receipts',
  'traces',
  'factories',
  'factory_addresses',
  'intervals',
];

// Columns EXCLUDED from the logical content of a table (surrogate/nondeterministic). Keyed by table.
// See the header COLUMN EXCLUSIONS note for the justification of each entry. Discovered surrogate ids
// are additionally verified at runtime against information_schema (see tableExcludedCols) so this list
// can never silently under-exclude a serial that the schema actually carries.
export const EXCLUDED_COLS = {
  factories: ['id'],
  factory_addresses: ['id'],
};

// Pure: combine per-table {table, digest, rows} records into the single store digest. Sorted by table
// name so input order never matters; each line binds table name, its digest, and its row count.
export function combineDigests(perTable) {
  const lines = perTable.map((t) => `${t.table}=${t.digest}:${t.rows}`).sort();
  const h = createHash('md5');
  h.update(lines.join('\n'));

  return { store: h.digest('hex'), lines };
}

// Build the deterministic per-table digest SQL for one table.
//   - `logicalCols` are the content-bearing columns (all columns minus the excluded surrogate ids);
//     the per-row payload is md5 over the jsonb of the row with the excluded keys removed.
//   - `orderCols` are the natural-key columns to sort by; the sort is made TOTAL by appending the full
//     logical-jsonb text as a final tie-break, so rows tie only when their whole content is identical.
// COALESCE to the empty string so a zero-row table yields md5('') deterministically rather than NULL.
function perTableSql(schema, table, orderCols, excludedCols) {
  // logical jsonb = to_jsonb(t) with each excluded key removed via the jsonb `- text` operator.
  let logicalJsonb = 'to_jsonb(t)';
  for (const c of excludedCols) {
    logicalJsonb = `(${logicalJsonb} - '${c.replace(/'/g, "''")}')`;
  }

  // ORDER BY: natural-key columns first, then the full logical-jsonb text as the total-order tie-break.
  const orderParts = orderCols.map((c) => `t."${c}"`);
  orderParts.push(`(${logicalJsonb})::text`);
  const orderBy = orderParts.join(', ');

  return `
    select
      md5(coalesce(string_agg(md5(${logicalJsonb}::text), '' order by ${orderBy}), '')) as digest,
      count(*)::bigint as rows
    from "${schema}"."${table}" t`;
}

// The surrogate/nondeterministic columns to exclude for a table = the static EXCLUDED_COLS list
// intersected with columns the table actually has, UNION any identity/serial column discovered live in
// information_schema (defence in depth: a schema that grew a new serial id is caught even if the static
// list lags). Returns the excluded column names.
async function tableExcludedCols(client, schema, table) {
  const declared = EXCLUDED_COLS[table] ?? [];

  // discover any identity column (GENERATED ... AS IDENTITY) or nextval-serial default on this table.
  const disc = await client.query(
    `select column_name from information_schema.columns
      where table_schema = $1 and table_name = $2
        and (is_identity = 'YES' or column_default like 'nextval(%')`,
    [schema, table],
  );
  const discovered = disc.rows.map((r) => r.column_name);

  // present columns (guard the static list against a column the table does not actually have).
  const colsRes = await client.query(
    `select column_name from information_schema.columns
      where table_schema = $1 and table_name = $2`,
    [schema, table],
  );
  const present = new Set(colsRes.rows.map((r) => r.column_name));

  const excluded = new Set();
  for (const c of declared) {
    if (present.has(c)) {
      excluded.add(c);
    }
  }
  for (const c of discovered) {
    excluded.add(c);
  }

  return [...excluded];
}

// Natural-key ORDER BY columns for a table: its primary-key columns MINUS any excluded surrogate id,
// falling back to ALL non-excluded columns (ordinal order) when the PK is empty or fully excluded. The
// per-table SQL appends the logical-jsonb text as a final tie-break, so this need only be a good
// prefix — totality is guaranteed by that tie-break regardless.
async function tableOrderCols(client, schema, table, excludedCols) {
  const excluded = new Set(excludedCols);

  const pk = await client.query(
    `select kcu.column_name
       from information_schema.table_constraints tc
       join information_schema.key_column_usage kcu
         on tc.constraint_name = kcu.constraint_name
        and tc.table_schema = kcu.table_schema
      where tc.table_schema = $1 and tc.table_name = $2
        and tc.constraint_type = 'PRIMARY KEY'
      order by kcu.ordinal_position`,
    [schema, table],
  );
  const pkCols = pk.rows
    .map((r) => r.column_name)
    .filter((c) => !excluded.has(c));
  if (pkCols.length > 0) {
    return pkCols;
  }

  // No usable PK (or the PK was the surrogate id): order by all content-bearing columns, ordinal order.
  const cols = await client.query(
    `select column_name from information_schema.columns
      where table_schema = $1 and table_name = $2
      order by ordinal_position`,
    [schema, table],
  );

  return cols.rows.map((r) => r.column_name).filter((c) => !excluded.has(c));
}

async function tableExists(client, schema, table) {
  const r = await client.query(
    `select 1 from information_schema.tables where table_schema = $1 and table_name = $2`,
    [schema, table],
  );

  return r.rows.length > 0;
}

export async function digestStore(connString, schema = 'ponder_sync') {
  const pg = await import('pg');
  const Client = pg.default?.Client ?? pg.Client;
  const client = new Client({ connectionString: connString });
  await client.connect();
  try {
    // deterministic collation for ORDER BY on text keys (fragment_id etc.): pin C collation so the
    // digest is independent of the server's default collation/locale.
    await client.query(`set local synchronous_commit to on`);

    const perTable = [];
    for (const table of DIGEST_TABLES) {
      const exists = await tableExists(client, schema, table);
      if (!exists) {
        throw new Error(
          `digest table "${schema}"."${table}" is ABSENT — schema shape mismatch (fail closed)`,
        );
      }

      const excludedCols = await tableExcludedCols(client, schema, table);
      const orderCols = await tableOrderCols(
        client,
        schema,
        table,
        excludedCols,
      );
      const res = await client.query(
        perTableSql(schema, table, orderCols, excludedCols),
      );
      perTable.push({
        table,
        digest: res.rows[0].digest,
        rows: Number(res.rows[0].rows),
        orderBy: orderCols,
        excluded: excludedCols,
      });
    }

    const { store, lines } = combineDigests(perTable);

    return { store, perTable, lines };
  } finally {
    await client.end();
  }
}

async function main() {
  const args = process.argv.slice(2);
  const conn = args.find((a) => !a.startsWith('--'));
  const schemaIdx = args.indexOf('--schema');
  const schema = schemaIdx >= 0 ? args[schemaIdx + 1] : 'ponder_sync';
  const asJson = args.includes('--json');
  if (!conn) {
    console.error(
      'usage: pg-digest.mjs <connString> [--schema ponder_sync] [--json]',
    );
    process.exit(2);
  }

  try {
    const out = await digestStore(conn, schema);
    if (asJson) {
      process.stdout.write(`${JSON.stringify(out)}\n`);
    } else {
      process.stdout.write(`${out.store}\n`);
    }
    process.exit(0);
  } catch (e) {
    // fail closed — a digest that could not be computed must NEVER be read as a blank/passing value.
    console.error(`pg-digest: ${e?.message ? e.message : e}`);
    process.exit(2);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
