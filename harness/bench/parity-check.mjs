#!/usr/bin/env node
// parity-check.mjs — READ-ONLY equivalence check of a bench DB against a reference DB over the
// authoritative ponder_sync tables. For each chain_id it compares, in ponder_sync.logs:
//   count(*), min(block_number), max(block_number), count(distinct transaction_hash)
// and it compares the whole-store totals count(*) for ponder_sync.blocks and ponder_sync.transactions.
// PASS iff EVERY compared cell matches; ANY difference is a FAIL. Writes parity-report.json and prints a
// table. Queries run inside a READ-ONLY transaction on BOTH connections — the check never writes.
//
//   node parity-check.mjs --bench <benchConnString> --reference <refConnString> \
//                        [--schema ponder_sync] [--out parity-report.json]
//
// exit 0 = PASS (every cell matched), exit 1 = FAIL (a cell differed), exit 2 = usage/connection error.

import { writeFileSync } from 'node:fs';
import { parseArgs } from './anchor-shim.mjs';

// The per-chain aggregate over ponder_sync.logs, grouped by chain_id. Uses aggregate SQL only (no row
// scans) so it is cheap even on a full 15-chain store.
function logsByChainSql(schema) {
  return `
    select
      chain_id::text as chain_id,
      count(*)::bigint as log_count,
      min(block_number)::text as min_block,
      max(block_number)::text as max_block,
      count(distinct transaction_hash)::bigint as distinct_tx
    from "${schema}"."logs"
    group by chain_id
    order by chain_id`;
}

function totalCountSql(schema, table) {
  return `select count(*)::bigint as n from "${schema}"."${table}"`;
}

async function connectReadOnly(connString) {
  const pg = await import('pg');
  const Client = pg.default?.Client ?? pg.Client;
  const client = new Client({ connectionString: connString });
  await client.connect();
  // read-only transaction — a write attempt would error, proving the check cannot mutate either store.
  await client.query('begin transaction read only');

  return client;
}

async function snapshot(connString, schema) {
  const client = await connectReadOnly(connString);
  try {
    const logs = await client.query(logsByChainSql(schema));
    const blocks = await client.query(totalCountSql(schema, 'blocks'));
    const transactions = await client.query(
      totalCountSql(schema, 'transactions'),
    );

    const byChain = new Map();
    for (const row of logs.rows) {
      byChain.set(row.chain_id, {
        chainId: row.chain_id,
        logCount: row.log_count,
        minBlock: row.min_block,
        maxBlock: row.max_block,
        distinctTx: row.distinct_tx,
      });
    }

    return {
      byChain,
      totals: {
        blocks: blocks.rows[0].n,
        transactions: transactions.rows[0].n,
      },
    };
  } finally {
    // end the read-only transaction and disconnect (rollback is a no-op for a read-only txn).
    await client.query('rollback').catch(() => {});
    await client.end().catch(() => {});
  }
}

// Compare two snapshots. Returns { pass, rows: [...], totals: {...} } where rows carry per-(chain,field)
// bench/reference values and a match flag; a chain present in only one store is a mismatch on every cell.
export function compareSnapshots(bench, reference) {
  const chainIds = new Set([
    ...bench.byChain.keys(),
    ...reference.byChain.keys(),
  ]);
  const rows = [];
  let pass = true;

  const fields = ['logCount', 'minBlock', 'maxBlock', 'distinctTx'];
  for (const chainId of [...chainIds].sort((a, b) => Number(a) - Number(b))) {
    const b = bench.byChain.get(chainId);
    const r = reference.byChain.get(chainId);
    for (const field of fields) {
      const bv = b ? b[field] : '<absent>';
      const rv = r ? r[field] : '<absent>';
      const match = String(bv) === String(rv);
      if (!match) {
        pass = false;
      }

      rows.push({
        scope: `logs[chain ${chainId}]`,
        field,
        bench: String(bv),
        reference: String(rv),
        match,
      });
    }
  }

  const totalsRows = [];
  for (const table of ['blocks', 'transactions']) {
    const bv = String(bench.totals[table]);
    const rv = String(reference.totals[table]);
    const match = bv === rv;
    if (!match) {
      pass = false;
    }

    totalsRows.push({
      scope: `${table} total`,
      field: 'count',
      bench: bv,
      reference: rv,
      match,
    });
  }

  return { pass, rows, totals: totalsRows };
}

function printTable(allRows) {
  const header = ['scope', 'field', 'bench', 'reference', 'match'];
  const widths = header.map((h) => h.length);
  for (const row of allRows) {
    header.forEach((h, i) => {
      widths[i] = Math.max(widths[i], String(row[h]).length);
    });
  }

  const line = (cells) =>
    cells.map((c, i) => String(c).padEnd(widths[i])).join('  ');
  console.error(line(header));
  console.error(widths.map((w) => '-'.repeat(w)).join('  '));
  for (const row of allRows) {
    console.error(
      line([
        row.scope,
        row.field,
        row.bench,
        row.reference,
        row.match ? 'ok' : 'DIFF',
      ]),
    );
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const benchConn = args.bench;
  const refConn = args.reference;
  if (!benchConn || benchConn === true || !refConn || refConn === true) {
    console.error(
      'usage: parity-check.mjs --bench <conn> --reference <conn> [--schema ponder_sync] [--out parity-report.json]',
    );
    process.exit(2);
  }

  const schema = typeof args.schema === 'string' ? args.schema : 'ponder_sync';
  const outPath =
    typeof args.out === 'string' ? args.out : 'parity-report.json';

  let benchSnap;
  let refSnap;
  try {
    benchSnap = await snapshot(benchConn, schema);
    refSnap = await snapshot(refConn, schema);
  } catch (e) {
    console.error(`parity-check: ${e?.message ?? e}`);
    process.exit(2);
  }

  const result = compareSnapshots(benchSnap, refSnap);
  const allRows = [...result.rows, ...result.totals];
  printTable(allRows);

  const report = {
    generatedAt: new Date().toISOString(),
    schema,
    pass: result.pass,
    diffs: allRows.filter((r) => !r.match),
    cells: allRows,
  };
  writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`);

  console.error(
    `\nparity-check: ${result.pass ? 'PASS' : 'FAIL'} — ${allRows.length} cells, ${report.diffs.length} diffs → ${outPath}`,
  );
  process.exit(result.pass ? 0 : 1);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(`parity-check: ${e?.message ?? e}`);
    process.exit(2);
  });
}
