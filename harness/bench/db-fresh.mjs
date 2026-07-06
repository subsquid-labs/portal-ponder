#!/usr/bin/env node
// db-fresh.mjs — READ-ONLY check that a DATABASE_URL points at a FRESH ponder store (no rows in
// ponder_sync.logs), used by run-flagship.sh's preflight. A re-run against a dirty DB is not a
// reproducible bench and would corrupt the parity comparison, so the driver refuses to start on one.
//
//   node db-fresh.mjs <connString>
//     prints "fresh" if ponder_sync.logs is absent or has 0 rows,
//            "dirty:<n>" if it has n>0 rows,
//            "error:<msg>" if the check could not run (fail closed — the driver treats this as not-fresh).
//   Always exits 0; the caller reads the single stdout token.

async function main() {
  const conn = process.argv[2];
  if (!conn) {
    process.stdout.write('error:no connection string\n');

    return;
  }

  const pg = await import('pg');
  const Client = pg.default?.Client ?? pg.Client;
  const client = new Client({ connectionString: conn });
  try {
    await client.connect();
    await client.query('begin transaction read only');
    const reg = await client.query(
      "select to_regclass('ponder_sync.logs') as t",
    );
    if (!reg.rows[0].t) {
      process.stdout.write('fresh\n');

      return;
    }

    const count = await client.query(
      'select count(*)::bigint as n from ponder_sync.logs',
    );
    const n = Number(count.rows[0].n);
    process.stdout.write(n === 0 ? 'fresh\n' : `dirty:${n}\n`);
  } catch (e) {
    process.stdout.write(`error:${String(e?.message ?? e).slice(0, 80)}\n`);
  } finally {
    await client.end().catch(() => {});
  }
}

main();
