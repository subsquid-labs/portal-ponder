// snapshot-coverage-pg.mjs — post-kill store coverage probe for the chaos-3-pg driver (issue #52),
// the native-Postgres analogue of snapshot-coverage.mjs. It reads a chaos store's ponder_sync.intervals
// over a `pg` connection (not a PGlite datadir) and classifies how far the durable state has progressed
// over [from,to]: empty | partial | complete | error — using the EXACT SAME pure classification as the
// v3 PGlite probe (parseRanges / fragmentCoveredHi / coverageVerdict are IMPORTED from it, unchanged),
// so the two backends share one proven coverage verdict.
//
//   node snapshot-coverage-pg.mjs <connString> <from> <to>
//     → prints ONE line of JSON to stdout:
//       {"rows":N,"logs":N,"maxBlock":N,"fragments":M,"coveredBlocks":C,"windowBlocks":W,
//        "coveragePct":P,"coverageClass":"empty|partial|complete"}
//     exit 0 on a readable store (absent ponder_sync ⇒ empty). A hard read/connect error is surfaced
//     as coverageClass:"error" (never masquerading as empty/complete) with exit 0 and an "error" field
//     — the driver treats "error" as a non-neutral store-durability FAIL and freezes. On a CRASH-DURABLE
//     Postgres backend a post-resume store is always readable, so "error" here would itself be a MAJOR
//     finding (the substrate this variant was built to prove).
//
// NOTE on Postgres range rendering: Postgres canonicalizes ponder_sync.intervals.blocks (a
// nummultirange) to CLOSED-upper text like "{[from,to+1]}" (upper_inc=true) — whereas PGlite renders
// the same interval half-open "{[from,to+1)}". parseRanges reads only the numeric bounds and ignores
// bracket inclusivity, and ponder stores `hi = to+1` under BOTH backends, so the imported
// fragmentCoveredHi/coverageVerdict give identical verdicts on either rendering. We pass the raw
// blocks::text through unchanged.

import {
  coverageVerdict,
  fragmentCoveredHi,
  parseRanges,
} from './snapshot-coverage.mjs';

// re-export so a pg-side unit test can exercise the shared pure core through this module too.
export { coverageVerdict, fragmentCoveredHi, parseRanges };

async function main() {
  const [conn, fromArg, toArg] = process.argv.slice(2);
  if (!conn || fromArg === undefined || toArg === undefined) {
    console.error('usage: snapshot-coverage-pg.mjs <connString> <from> <to>');
    process.exit(2);
  }

  const from = Number(fromArg);
  const to = Number(toArg);
  const want = { from, to };

  let client;
  try {
    const pg = await import('pg');
    const Client = pg.default?.Client ?? pg.Client;
    client = new Client({ connectionString: conn });
    await client.connect();

    const present = await client.query(
      `select 1 from information_schema.tables where table_schema='ponder_sync' and table_name='intervals'`,
    );
    if (present.rows.length === 0) {
      // no ponder_sync yet ⇒ the process was killed before any migration ran ⇒ empty (not an error).
      const v = coverageVerdict([], want, { blockCount: 0 });
      v.maxBlock = -1;
      v.logs = 0;
      await client.end();
      process.stdout.write(`${JSON.stringify(v)}\n`);
      process.exit(0);
    }

    const blk = (
      await client.query(
        `select count(*)::int n, coalesce(max(number),-1)::bigint mx from ponder_sync.blocks`,
      )
    ).rows[0];
    const logsRow = (
      await client.query(`select count(*)::int n from ponder_sync.logs`)
    ).rows[0];
    const { rows } = await client.query(
      `select fragment_id, blocks::text as blocks from ponder_sync.intervals`,
    );
    await client.end();

    const blockCount = Number(blk?.n ?? 0);
    const v = coverageVerdict(rows, want, { blockCount });
    v.maxBlock = Number(blk?.mx ?? -1);
    v.logs = Number(logsRow?.n ?? 0);
    process.stdout.write(`${JSON.stringify(v)}\n`);
    process.exit(0);
  } catch (e) {
    // A hard read/connect error must NOT masquerade as a passing (empty/complete) snapshot. Emit an
    // explicit error class the driver treats as a non-neutral store-durability FAIL.
    try {
      if (client) await client.end();
    } catch {}
    const errOut = {
      rows: -1,
      fragments: -1,
      coveredBlocks: -1,
      windowBlocks: want.to + 1 - want.from,
      coveragePct: -1,
      coverageClass: 'error',
      error: String(e?.message ? e.message : e),
    };
    process.stdout.write(`${JSON.stringify(errOut)}\n`);
    process.exit(0);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    process.stdout.write(
      `${JSON.stringify({ coverageClass: 'error', error: String(e?.message ? e.message : e) })}\n`,
    );
    process.exit(0);
  });
}
