// check-intervals-pg.mjs — assert ponder_sync.intervals exactly tiles [from,to] after a resume run, on
// native Postgres (issue #52); the pg analogue of harness/chaos/check-intervals.mjs. Every fragment's
// `blocks` multirange must be a SINGLE contiguous range covering the window; >1 range ⇒ a real gap
// (Postgres multiranges auto-coalesce adjacent/overlapping ranges, so a second element is a genuine
// gap), and short coverage ⇒ a kill dropped a delegated interval — the exact failure the chaos
// campaign hunts.
//
//   node check-intervals-pg.mjs <connString> <from> <to>    exit 0 = tiled, 1 = gap/short, 2 = usage/db
//
// The pure verdict core (parseRanges / tileVerdict / intervalsVerdict) is kept byte-equivalent to the
// repo's check-intervals.mjs so the tiling semantics are identical across backends. Postgres renders
// the stored range CLOSED-upper ("[from,to+1]"); parseRanges reads only numeric bounds and ponder
// stores hi=to+1 under both backends, so `hi >= to+1` holds identically.

// Parse a Postgres (multi)range text like "{[a,b),[c,d)}" / "{[a,b]}" / "[a,b)" into [{lo,hi}].
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

// Pure tiling verdict for one fragment. A closed window [from,to] tiles as a single range whose lower
// reaches at/below `from` and whose numeric upper reaches at/above `to+1` (ponder stores the upper as
// to+1 under both backends). >1 range ⇒ gap; 0 ⇒ no rows.
export function tileVerdict(ranges, want) {
  if (ranges.length === 0) {
    return { ok: false, reason: 'no interval rows' };
  }
  if (ranges.length > 1) {
    return { ok: false, reason: `${ranges.length} disjoint ranges → gap` };
  }

  const only = ranges[0];
  const coversLo = only.lo !== null && only.lo <= want.from;
  const coversHi = only.hi !== null && only.hi >= want.to + 1;
  if (!coversLo || !coversHi) {
    return {
      ok: false,
      reason: `range [${only.lo},${only.hi}] does not cover [${want.from},${want.to + 1}]`,
    };
  }

  return { ok: true, reason: `tiled [${only.lo},${only.hi}]` };
}

// Aggregate verdict over every interval row. ZERO rows is a hard FAIL: a resume that wrote nothing
// leaves an empty ponder_sync.intervals, which must NOT read as a vacuous PASS.
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
  const [conn, fromArg, toArg] = process.argv.slice(2);
  if (!conn || fromArg === undefined || toArg === undefined) {
    console.error('usage: check-intervals-pg.mjs <connString> <from> <to>');
    process.exit(2);
  }

  const from = Number(fromArg);
  const to = Number(toArg);

  const pg = await import('pg');
  const Client = pg.default?.Client ?? pg.Client;
  const client = new Client({ connectionString: conn });
  await client.connect();

  const present = await client.query(
    `select 1 from information_schema.tables where table_schema='ponder_sync' and table_name='intervals'`,
  );
  if (present.rows.length === 0) {
    const tables = await client.query(
      `select table_name from information_schema.tables where table_schema='ponder_sync' order by table_name`,
    );
    console.error(
      '✗ ponder_sync.intervals not found. ponder_sync tables:',
      tables.rows.map((r) => r.table_name).join(', '),
    );
    await client.end();
    process.exit(2);
  }

  const { rows } = await client.query(
    `select fragment_id, blocks::text as blocks from ponder_sync.intervals`,
  );
  await client.end();

  console.log(`\nintervals tiling check (pg)  want=[${from},${to}]\n`);

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
