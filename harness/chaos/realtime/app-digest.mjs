// app-digest.mjs - deterministic logical digest for app tables in a Postgres schema.
//
// Mirrors pg-digest.mjs's row-content approach, but discovers application tables at runtime. Identity
// or nextval-backed columns are excluded because they are allocation artifacts, not event content.

import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';

export function combineDigests(perTable) {
  const lines = perTable.map((t) => `${t.table}=${t.digest}:${t.rows}`).sort();
  const h = createHash('md5');
  h.update(lines.join('\n'));

  return { store: h.digest('hex'), lines };
}

function perTableSql(schema, table, orderCols, excludedCols) {
  let logicalJsonb = 'to_jsonb(t)';
  for (const col of excludedCols) {
    logicalJsonb = `(${logicalJsonb} - '${col.replace(/'/g, "''")}')`;
  }

  const orderParts = orderCols.map((col) => `t."${col}"`);
  orderParts.push(`(${logicalJsonb})::text`);

  return `
    select
      md5(coalesce(string_agg(md5(${logicalJsonb}::text), '' order by ${orderParts.join(', ')}), '')) as digest,
      count(*)::bigint as rows
    from "${schema}"."${table}" t`;
}

async function appTables(client, schema) {
  const res = await client.query(
    `select table_name
      from information_schema.tables
      where table_schema = $1
        and table_type = 'BASE TABLE'
        and table_name not in ('kysely_migration', 'kysely_migration_lock')
        and table_name not like '\\_ponder\\_%'
        and table_name not like '\\_reorg\\_\\_%'
      order by table_name`,
    [schema],
  );

  return res.rows.map((r) => r.table_name);
}

async function tableExcludedCols(client, schema, table) {
  const res = await client.query(
    `select column_name
       from information_schema.columns
      where table_schema = $1
        and table_name = $2
        and (is_identity = 'YES' or column_default like 'nextval(%')`,
    [schema, table],
  );

  return res.rows.map((r) => r.column_name);
}

async function tableOrderCols(client, schema, table, excludedCols) {
  const excluded = new Set(excludedCols);
  const pk = await client.query(
    `select kcu.column_name
       from information_schema.table_constraints tc
       join information_schema.key_column_usage kcu
         on tc.constraint_name = kcu.constraint_name
        and tc.table_schema = kcu.table_schema
      where tc.table_schema = $1
        and tc.table_name = $2
        and tc.constraint_type = 'PRIMARY KEY'
      order by kcu.ordinal_position`,
    [schema, table],
  );
  const pkCols = pk.rows
    .map((r) => r.column_name)
    .filter((col) => excluded.has(col) === false);
  if (pkCols.length > 0) return pkCols;

  const cols = await client.query(
    `select column_name
       from information_schema.columns
      where table_schema = $1
        and table_name = $2
      order by ordinal_position`,
    [schema, table],
  );

  return cols.rows
    .map((r) => r.column_name)
    .filter((col) => excluded.has(col) === false);
}

export async function digestApp(connString, schema = 'public') {
  const requireFromCwd = createRequire(`${process.cwd()}/`);
  const pg = requireFromCwd('pg');
  const Client = pg.Client;
  const client = new Client({ connectionString: connString });
  await client.connect();
  try {
    const tables = await appTables(client, schema);
    const perTable = [];
    for (const table of tables) {
      const excluded = await tableExcludedCols(client, schema, table);
      const orderBy = await tableOrderCols(client, schema, table, excluded);
      const res = await client.query(
        perTableSql(schema, table, orderBy, excluded),
      );
      perTable.push({
        table,
        digest: res.rows[0].digest,
        rows: Number(res.rows[0].rows),
        orderBy,
        excluded,
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
  const conn = args.find((arg) => arg.startsWith('--') === false);
  const schemaIdx = args.indexOf('--schema');
  const schema = schemaIdx >= 0 ? args[schemaIdx + 1] : 'public';
  const asJson = args.includes('--json');
  if (conn === undefined) {
    console.error(
      'usage: app-digest.mjs <connString> [--schema public] [--json]',
    );
    process.exit(2);
  }

  try {
    const digest = await digestApp(conn, schema);
    if (asJson) console.log(JSON.stringify(digest, null, 2));
    else console.log(digest.store);
  } catch (error) {
    console.error(`app-digest: ${error.message}`);
    process.exit(2);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
