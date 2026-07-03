import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  intervalsVerdict,
  parseRanges,
  tileVerdict,
} from './check-intervals.mjs';

test('parseRanges: single closed/half-open range', () => {
  assert.deepEqual(parseRanges('[100,201)'), [{ lo: 100, hi: 201 }]);
});

test('parseRanges: multirange with a gap yields two ranges', () => {
  assert.deepEqual(parseRanges('{[100,150),[160,201)}'), [
    { lo: 100, hi: 150 },
    { lo: 160, hi: 201 },
  ]);
});

test('tileVerdict: one contiguous range covering [from,to] passes (upper exclusive)', () => {
  const v = tileVerdict([{ lo: 100, hi: 201 }], { from: 100, to: 200 });
  assert.equal(v.ok, true);
});

test('tileVerdict: a gap (>1 range) fails', () => {
  const v = tileVerdict(
    [
      { lo: 100, hi: 150 },
      { lo: 160, hi: 201 },
    ],
    { from: 100, to: 200 },
  );
  assert.equal(v.ok, false);
  assert.match(v.reason, /gap/);
});

test('tileVerdict: short coverage fails; no rows fails', () => {
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

test('intervalsVerdict: a single fully-tiling fragment passes; a gap fragment fails the aggregate', () => {
  const good = intervalsVerdict([{ fragment_id: 'f0', blocks: '[100,201)' }], {
    from: 100,
    to: 200,
  });
  assert.equal(good.ok, true);
  assert.equal(good.fragments.length, 1);

  const bad = intervalsVerdict(
    [
      { fragment_id: 'f0', blocks: '[100,201)' },
      { fragment_id: 'f1', blocks: '{[100,150),[160,201)}' }, // a gap
    ],
    { from: 100, to: 200 },
  );
  assert.equal(bad.ok, false);
  assert.equal(bad.fragments[0].ok, true);
  assert.equal(bad.fragments[1].ok, false);
});
