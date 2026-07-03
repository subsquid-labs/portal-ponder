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

// Aggregate verdict over every interval row (each `{fragment_id, blocks}`). ZERO rows is a hard FAIL:
// a resume that wrote nothing leaves an empty ponder_sync.intervals, which must NOT be read as a
// vacuous PASS — a non-empty requested window with no interval coverage is the exact gap the chaos
// campaign hunts. `rows` is the raw query result; `blocksOf` extracts the range text of a row.
export function intervalsVerdict(rows, want, blocksOf = (r) => r.blocks) {
  if (rows.length === 0) {
    return {
      ok: false,
      fragments: [],
      reason: `no interval rows for window [${want.from},${want.to}]`,
    };
  }

  const fragments = rows.map((row) => ({
    fragmentId: row.fragment_id,
    ...tileVerdict(parseRanges(blocksOf(row)), want),
  }));
  const ok = fragments.every((f) => f.ok);

  return {
    ok,
    fragments,
    reason: ok ? 'every fragment tiles the window' : 'gap/short coverage',
  };
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

  console.log(`\nintervals tiling check  dir=${dir}  want=[${from},${to}]\n`);

  const verdict = intervalsVerdict(rows, { from, to });
  if (rows.length === 0) {
    console.log(`  ❌ ${verdict.reason}`);
  }

  for (const f of verdict.fragments) {
    console.log(
      `  ${f.ok ? '✅' : '❌'} ${String(f.fragmentId).slice(0, 48).padEnd(48)} ${f.reason}`,
    );
  }

  console.log(
    verdict.ok
      ? '\n✅ every fragment tiles the window exactly\n'
      : '\n❌ intervals do NOT tile — gap/short coverage\n',
  );
  process.exit(verdict.ok ? 0 : 1);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(e);
    process.exit(2);
  });
}
