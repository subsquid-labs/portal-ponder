// check-intervals.mjs — assert ponder_sync.intervals exactly tiles [from,to] after a resume run:
// every fragment's `blocks` multirange must have coalesced into a SINGLE contiguous range covering
// the requested window (Postgres multiranges auto-merge adjacent/overlapping ranges, so >1 range ⇒
// a real gap, and overlaps are impossible by construction). A gap or short coverage means a kill
// dropped a delegated interval — the exact failure the chaos campaign hunts.
//
//   node harness/chaos/check-intervals.mjs <pgliteDir> <from> <to>   exit 0 = tiled, 1 = gap/short
//
// pglite is imported lazily so the file stays loadable without the dependency present.

// Parse a Postgres (multi)range text like "{[a,b),[c,d)}" or "[a,b)" into [{lo,hi}] (hi exclusive).
export function parseRanges(text) {
  const out = [];
  const re = /[[(]\s*(\d+)?\s*,\s*(\d+)?\s*[\])]/g;
  let m = re.exec(text);
  while (m !== null) {
    out.push({
      lo: m[1] === undefined ? null : Number(m[1]),
      hi: m[2] === undefined ? null : Number(m[2]),
    });
    m = re.exec(text);
  }

  return out;
}

// Pure tiling verdict for one fragment given its parsed ranges and the requested window.
export function tileVerdict(ranges, want) {
  if (ranges.length === 0) {
    return { ok: false, reason: 'no interval rows' };
  }
  if (ranges.length > 1) {
    return { ok: false, reason: `${ranges.length} disjoint ranges → gap` };
  }

  const only = ranges[0];
  // Postgres stores the upper bound exclusive, so a closed window [from,to] tiles as [from, to+1).
  const coversLo = only.lo !== null && only.lo <= want.from;
  const coversHi = only.hi !== null && only.hi >= want.to + 1;
  if (!coversLo || !coversHi) {
    return {
      ok: false,
      reason: `range [${only.lo},${only.hi}) does not cover [${want.from},${want.to + 1})`,
    };
  }

  return { ok: true, reason: `tiled [${only.lo},${only.hi})` };
}

async function main() {
  const [dir, fromArg, toArg] = process.argv.slice(2);
  if (!dir || fromArg === undefined || toArg === undefined) {
    console.error('usage: check-intervals.mjs <pgliteDir> <from> <to>');
    process.exit(2);
  }

  const from = Number(fromArg);
  const to = Number(toArg);
  const { PGlite } = await import('@electric-sql/pglite');
  const db = await PGlite.create(dir);

  const present = await db.query(
    `select 1 from information_schema.tables where table_schema='ponder_sync' and table_name='intervals'`,
  );
  if (present.rows.length === 0) {
    const tables = await db.query(
      `select table_name from information_schema.tables where table_schema='ponder_sync' order by table_name`,
    );
    console.error(
      '✗ ponder_sync.intervals not found. ponder_sync tables:',
      tables.rows.map((r) => r.table_name).join(', '),
    );
    await db.close();
    process.exit(2);
  }

  const { rows } = await db.query(
    `select fragment_id, blocks::text as blocks from ponder_sync.intervals`,
  );
  await db.close();

  let fail = 0;
  console.log(`\nintervals tiling check  dir=${dir}  want=[${from},${to}]\n`);
  for (const row of rows) {
    const v = tileVerdict(parseRanges(row.blocks), { from, to });
    if (!v.ok) {
      fail = 1;
    }

    console.log(
      `  ${v.ok ? '✅' : '❌'} ${String(row.fragment_id).slice(0, 48).padEnd(48)} ${v.reason}`,
    );
  }

  console.log(
    fail
      ? '\n❌ intervals do NOT tile — gap/short coverage\n'
      : '\n✅ every fragment tiles the window exactly\n',
  );
  process.exit(fail);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(e);
    process.exit(2);
  });
}
