#!/usr/bin/env node
// db-fresh.mjs — READ-ONLY check that a DATABASE_URL points at a FRESH ponder store, used by
// run-flagship.sh's preflight. A re-run against a dirty DB is not a reproducible bench and would
// corrupt the parity comparison, so the driver refuses to start on one.
//
// FRESH means the ponder_sync schema is absent, OR every core ponder_sync table that exists is empty:
// logs AND blocks AND transactions (and the intervals table if present). Checking logs alone was too
// weak — a partial/aborted run can leave blocks or transactions rows with an empty logs table, and that
// store is NOT fresh. The reported row count is the max across the checked tables.
//
//   node db-fresh.mjs <connString>
//     prints "fresh" if ponder_sync is absent or all checked tables have 0 rows,
//            "dirty:<n>" if any checked table has rows (n = the largest table's row count),
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

    // The core ponder_sync tables that a real run populates. `intervals` is checked only if present
    // (its name has varied across ponder versions), so a schema without it is not treated as an error.
    const tables = ['logs', 'blocks', 'transactions', 'intervals'];
    let maxRows = 0;
    let anyPresent = false;
    for (const table of tables) {
      const reg = await client.query('select to_regclass($1) as t', [
        `ponder_sync.${table}`,
      ]);
      if (!reg.rows[0].t) {
        continue;
      }

      anyPresent = true;
      const count = await client.query(
        `select count(*)::bigint as n from ponder_sync.${table}`,
      );
      const n = Number(count.rows[0].n);
      if (n > maxRows) {
        maxRows = n;
      }
    }

    // No ponder_sync core table exists at all → an empty/absent schema, which is fresh.
    if (!anyPresent) {
      process.stdout.write('fresh\n');

      return;
    }

    process.stdout.write(maxRows === 0 ? 'fresh\n' : `dirty:${maxRows}\n`);
  } catch (e) {
    process.stdout.write(`error:${String(e?.message ?? e).slice(0, 80)}\n`);
  } finally {
    await client.end().catch(() => {});
  }
}

main();
