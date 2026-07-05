import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  coverageVerdict,
  fragmentCoveredHi,
  parseRanges,
} from './snapshot-coverage-pg.mjs';

// snapshot-coverage-pg.mjs re-exports the shared pure classification core (parseRanges /
// fragmentCoveredHi / coverageVerdict) from snapshot-coverage.mjs so the pg probe and the PGlite probe
// share ONE proven coverage verdict. These tests exercise that core through the pg module, on the
// Postgres CLOSED-upper range rendering. The classification is what the campaign uses to attribute a
// kill as landing at PARTIAL durable coverage (0 < coverage < 100%) — the evidence Tier-0 lacked.

test('coverageVerdict: an empty store (no interval rows) classifies "empty"', () => {
  const v = coverageVerdict(
    [],
    { from: 20529207, to: 20579207 },
    { blockCount: 0 },
  );
  assert.equal(v.coverageClass, 'empty');
  assert.equal(v.coveragePct, 0);
});

test('coverageVerdict: full closed-upper coverage classifies "complete"', () => {
  // Postgres renders full [from,to] coverage as {[from,to+1]}.
  const from = 1000;
  const to = 2000;
  const rows = [{ fragment_id: 'f', blocks: `{[${from},${to + 1}]}` }];
  const v = coverageVerdict(rows, { from, to }, { blockCount: 1001 });
  assert.equal(v.coverageClass, 'complete');
  assert.equal(v.coveragePct, 100);
});

test('coverageVerdict: a partial contiguous prefix classifies "partial" with 0 < pct < 100 (the staircase kill)', () => {
  const from = 1000;
  const to = 2000; // window = 1001 blocks
  // durable state reached block 1500 → range {[1000,1501]} (closed upper) → 501/1001 ≈ 50.05%.
  const rows = [{ fragment_id: 'f', blocks: `{[${from},1501]}` }];
  const v = coverageVerdict(rows, { from, to }, { blockCount: 501 });
  assert.equal(v.coverageClass, 'partial');
  assert.ok(
    v.coveragePct > 0 && v.coveragePct < 100,
    `expected partial pct, got ${v.coveragePct}`,
  );
});

test('coverageVerdict: coverage is the MIN across fragments — a lagging second fragment caps the class', () => {
  const from = 1000;
  const to = 2000;
  const rows = [
    { fragment_id: 'full', blocks: `{[${from},${to + 1}]}` },
    { fragment_id: 'lagging', blocks: `{[${from},1501]}` },
  ];
  const v = coverageVerdict(rows, { from, to }, { blockCount: 900 });
  assert.equal(v.coverageClass, 'partial');
});

test('coverageVerdict: a head-gap fragment (starts above `from`) contributes zero contiguous coverage', () => {
  const from = 1000;
  const to = 2000;
  // a range that begins at 1200, not `from` → no contiguous coverage from `from`.
  const rows = [{ fragment_id: 'headgap', blocks: '{[1200,2001]}' }];
  const v = coverageVerdict(rows, { from, to }, { blockCount: 800 });
  assert.equal(v.coverageClass, 'empty');
});

test('fragmentCoveredHi: contiguous prefix from `from`; closed/half-open bracket parses the same', () => {
  assert.equal(fragmentCoveredHi(parseRanges('{[1000,1501]}'), 1000), 1501);
  assert.equal(fragmentCoveredHi(parseRanges('{[1000,1501)}'), 1000), 1501);
  // a gap at the head yields `from` (zero contiguous coverage).
  assert.equal(fragmentCoveredHi(parseRanges('{[1200,2001]}'), 1000), 1000);
});
