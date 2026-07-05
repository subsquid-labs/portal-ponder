import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  intervalsVerdict,
  parseRanges,
  tileVerdict,
} from './check-intervals-pg.mjs';

// The pg intervals check must give the SAME tiling verdict as the PGlite check-intervals.mjs, across
// Postgres's CLOSED-upper multirange rendering ("{[from,to+1]}") vs PGlite's half-open ("{[from,to+1)}").
// parseRanges reads only numeric bounds (ignores bracket inclusivity) and ponder stores hi=to+1 under
// both backends, so hi >= to+1 holds identically. These tests pin that equivalence.

test('parseRanges: Postgres closed-upper rendering parses to the same numeric bounds as half-open', () => {
  assert.deepEqual(parseRanges('[100,201]'), [{ lo: 100, hi: 201 }]);
  assert.deepEqual(parseRanges('[100,201)'), [{ lo: 100, hi: 201 }]);
});

test('parseRanges: Postgres multirange with a gap yields two ranges (bracket style irrelevant)', () => {
  assert.deepEqual(parseRanges('{[100,150],[160,201]}'), [
    { lo: 100, hi: 150 },
    { lo: 160, hi: 201 },
  ]);
});

test('tileVerdict: one contiguous closed-upper range covering [from,to] passes', () => {
  // Postgres renders full coverage of [100,200] as {[100,201]} — hi=201 >= to+1=201.
  assert.equal(
    tileVerdict([{ lo: 100, hi: 201 }], { from: 100, to: 200 }).ok,
    true,
  );
});

test('tileVerdict: a gap (>1 range) fails; short upper fails; no rows fails', () => {
  assert.equal(
    tileVerdict(
      [
        { lo: 100, hi: 150 },
        { lo: 160, hi: 201 },
      ],
      { from: 100, to: 200 },
    ).ok,
    false,
  );
  assert.equal(
    tileVerdict([{ lo: 100, hi: 190 }], { from: 100, to: 200 }).ok,
    false,
  );
  assert.equal(tileVerdict([], { from: 100, to: 200 }).ok, false);
});

test('intervalsVerdict: ZERO interval rows for a non-empty window is a hard FAIL (not a vacuous pass)', () => {
  const v = intervalsVerdict([], { from: 100, to: 200 });
  assert.equal(v.ok, false);
  assert.match(v.reason, /no interval rows/);
});

test('intervalsVerdict: every fragment must tile — one gap fragment fails the aggregate', () => {
  const good = intervalsVerdict(
    [{ fragment_id: 'f0', blocks: '{[100,201]}' }],
    {
      from: 100,
      to: 200,
    },
  );
  assert.equal(good.ok, true);

  const bad = intervalsVerdict(
    [
      { fragment_id: 'f0', blocks: '{[100,201]}' },
      { fragment_id: 'f1', blocks: '{[100,150],[160,201]}' },
    ],
    { from: 100, to: 200 },
  );
  assert.equal(bad.ok, false);
  assert.equal(bad.fragments[0].ok, true);
  assert.equal(bad.fragments[1].ok, false);
});
