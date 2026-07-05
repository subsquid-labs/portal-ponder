// snapshot-coverage.mjs — post-kill store coverage probe for the chaos-3 driver v3 (Tier-1).
//
// Reads a PGlite chaos store (produced by the driver's ops-local kill loop) and classifies how far
// the durable ponder_sync state has progressed over the requested window [from,to]. This is the
// evidence the v2 campaign lacked: with default 500k chunking the store went 0%->100% atomically
// (issue #50), so a "killed N times" run could be entirely restart-from-zero attempts. Under Tier-1
// chunking (PORTAL_CHUNK_BLOCKS=2000) the store accretes in a staircase, so a kill can land on a
// genuine PARTIAL durable state — and THIS probe attributes it.
//
//   node snapshot-coverage.mjs <pgliteDir> <from> <to>
//     → prints ONE line of JSON to stdout:
//       {"rows":N,"logs":N,"maxBlock":N,"fragments":M,"coveredBlocks":C,"windowBlocks":W,
//        "coveragePct":P,"coverageClass":"empty|partial|complete"}
//     exit 0 always on a readable/absent store (absent or no ponder_sync ⇒ empty); exit 2 only on a
//     usage error. A probe failure must NOT be read as a passing snapshot, so any hard read error is
//     surfaced as coverageClass:"error" with exit 0 and an "error" field (the driver treats "error"
//     as a non-neutral FAIL and freezes).
//
// pglite is imported lazily so the file stays loadable without the dependency present. The driver
// invokes it from a workspace that already has @electric-sql/pglite installed (the probe kit).

// Parse a Postgres (multi)range text like "{[a,b),[c,d)}" or "[a,b)" into [{lo,hi}] (hi EXCLUSIVE).
// Identical semantics to harness/chaos/check-intervals.mjs parseRanges (kept in sync deliberately).
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

// The covered upper bound (exclusive) of one fragment over [from,·]: the max `hi` among ranges whose
// `lo` reaches at or below `from` (a range that starts ABOVE from leaves a gap at the head, so it does
// not extend contiguous coverage from `from`). Multiranges coalesce adjacent ranges, so the common
// case is a single [from, hi) range; a fragment with a head gap returns `from` (0 contiguous coverage).
export function fragmentCoveredHi(ranges, from) {
  let hi = from; // contiguous coverage from `from` starts empty
  // Sort by lo so we can extend contiguously; nulls (unbounded low) treated as -inf ⇒ reach from.
  const sorted = ranges
    .map((r) => ({
      lo: r.lo === null ? Number.NEGATIVE_INFINITY : r.lo,
      hi: r.hi,
    }))
    .sort((a, b) => a.lo - b.lo);
  for (const r of sorted) {
    if (r.hi === null) {
      // unbounded upper ⇒ everything from here up; only useful if it connects to `hi`.
      if (r.lo <= hi) return Number.POSITIVE_INFINITY;

      continue;
    }
    if (r.lo <= hi && r.hi > hi) {
      hi = r.hi;
    }
  }

  return hi;
}

// Pure coverage verdict over the interval rows. `want` is {from,to} (closed window; Postgres stores
// the upper bound EXCLUSIVE, so full coverage means hi >= to+1). Coverage is the MINIMUM contiguous
// covered fraction across all fragments (a resume is only as complete as its least-covered source).
// rows: [{fragment_id, blocks}] ; blocksOf extracts the range text.
export function coverageVerdict(
  rows,
  want,
  { blockCount = 0, blocksOf = (r) => r.blocks } = {},
) {
  const windowBlocks = want.to + 1 - want.from; // inclusive [from,to] ⇒ to+1-from blocks
  if (!Array.isArray(rows) || rows.length === 0) {
    // No interval rows at all ⇒ nothing durable. (blockCount should be 0 here too.)
    return {
      rows: blockCount,
      fragments: 0,
      coveredBlocks: 0,
      windowBlocks,
      coveragePct: 0,
      coverageClass: 'empty',
    };
  }

  let minCoveredHi = Number.POSITIVE_INFINITY;
  for (const row of rows) {
    const hi = fragmentCoveredHi(parseRanges(blocksOf(row)), want.from);
    if (hi < minCoveredHi) minCoveredHi = hi;
  }
  // clamp covered to [from, to+1]
  const cappedHi = Math.min(minCoveredHi, want.to + 1);
  const coveredBlocks = Math.max(0, cappedHi - want.from);
  const coveragePct =
    windowBlocks > 0
      ? Math.round((coveredBlocks / windowBlocks) * 10000) / 100
      : 0;

  let coverageClass;
  if (coveredBlocks <= 0) {
    coverageClass = 'empty';
  } else if (coveragePct >= 100) {
    coverageClass = 'complete';
  } else {
    coverageClass = 'partial';
  }

  return {
    rows: blockCount,
    fragments: rows.length,
    coveredBlocks,
    windowBlocks,
    coveragePct,
    coverageClass,
  };
}

async function main() {
  const [dir, fromArg, toArg] = process.argv.slice(2);
  if (!dir || fromArg === undefined || toArg === undefined) {
    console.error('usage: snapshot-coverage.mjs <pgliteDir> <from> <to>');
    process.exit(2);
  }

  const from = Number(fromArg);
  const to = Number(toArg);
  const want = { from, to };

  const { existsSync } = await import('node:fs');
  // An absent store dir ⇒ the process was killed before it wrote anything ⇒ empty (not an error).
  if (!existsSync(dir)) {
    const v = coverageVerdict([], want, { blockCount: 0 });
    v.maxBlock = -1;
    v.logs = 0;
    process.stdout.write(`${JSON.stringify(v)}\n`);
    process.exit(0);
  }

  let db;
  try {
    const { PGlite } = await import('@electric-sql/pglite');
    db = await PGlite.create(dir);

    const present = await db.query(
      `select 1 from information_schema.tables where table_schema='ponder_sync' and table_name='intervals'`,
    );
    if (present.rows.length === 0) {
      const v = coverageVerdict([], want, { blockCount: 0 });
      v.maxBlock = -1;
      v.logs = 0;
      await db.close();
      process.stdout.write(`${JSON.stringify(v)}\n`);
      process.exit(0);
    }

    const blk = (
      await db.query(
        `select count(*)::int n, coalesce(max(number),-1)::bigint mx from ponder_sync.blocks`,
      )
    ).rows[0];
    const logsRow = (
      await db.query(`select count(*)::int n from ponder_sync.logs`)
    ).rows[0];
    const { rows } = await db.query(
      `select fragment_id, blocks::text as blocks from ponder_sync.intervals`,
    );
    await db.close();

    const blockCount = Number(blk?.n ?? 0);
    const v = coverageVerdict(rows, want, { blockCount });
    v.maxBlock = Number(blk?.mx ?? -1);
    v.logs = Number(logsRow?.n ?? 0);
    process.stdout.write(`${JSON.stringify(v)}\n`);
    process.exit(0);
  } catch (e) {
    // A hard read error must NOT masquerade as a passing (empty/complete) snapshot. Emit an explicit
    // error class the driver treats as a non-neutral FAIL.
    try {
      if (db) await db.close();
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
