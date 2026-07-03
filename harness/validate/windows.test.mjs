import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  autoShrink,
  chunkGridWindows,
  deployFloorWindow,
  ETH_FORMAT_ERAS,
  formatEraWindows,
  halveWindow,
  mulberry32,
  resolveWindowEntry,
  resolveWindows,
  seededRandomWindows,
} from './windows.mjs';

test('mulberry32 is deterministic and range-bounded', () => {
  const a = mulberry32(42);
  const b = mulberry32(42);
  const first = [];
  const second = [];
  for (let i = 0; i < 5; i++) {
    first.push(a());
    second.push(b());
  }

  assert.deepEqual(first, second);
  for (const v of first) {
    assert.ok(v >= 0 && v < 1);
  }

  // A different seed diverges.
  assert.notDeepEqual(first, [mulberry32(43)(), mulberry32(43)()].slice(0, 5));
});

test('seededRandomWindows: same seed → identical windows; every window is in-range and sized', () => {
  const params = {
    seed: 1234,
    from: 20_000_000,
    to: 21_000_000,
    count: 8,
    size: 2_000,
  };
  const one = seededRandomWindows(params);
  const two = seededRandomWindows(params);

  assert.deepEqual(one, two, 'seeded generation must be reproducible');
  assert.equal(one.length, 8);
  for (const w of one) {
    assert.equal(w.to - w.from, 2_000, 'window size honoured');
    assert.ok(
      w.from >= params.from && w.to <= params.to,
      'window inside range',
    );
  }

  // A different seed produces a different set.
  const other = seededRandomWindows({ ...params, seed: 5678 });
  assert.notDeepEqual(
    one.map((w) => w.from),
    other.map((w) => w.from),
  );
});

test('seededRandomWindows rejects impossible parameters', () => {
  assert.throws(() =>
    seededRandomWindows({ seed: 1, from: 100, to: 50, count: 1, size: 10 }),
  );
  assert.throws(() =>
    seededRandomWindows({ seed: 1, from: 0, to: 100, count: 1, size: 500 }),
  );
});

test('chunkGridWindows straddles every 500k edge inside the range by ±delta', () => {
  const w = chunkGridWindows({
    from: 19_900_000,
    to: 21_100_000,
    chunk: 500_000,
    delta: 2,
  });
  assert.deepEqual(
    w.map((x) => x.from + 2),
    [20_000_000, 20_500_000, 21_000_000],
    'one window per interior 500k multiple',
  );
  for (const x of w) {
    assert.equal(x.to - x.from, 4, 'window is edge-delta … edge+delta');
  }

  // No edge inside a sub-chunk range → no windows.
  assert.equal(
    chunkGridWindows({ from: 20_000_010, to: 20_000_020 }).length,
    0,
  );
});

test('deployFloorWindow straddles the deploy block and clamps at zero', () => {
  assert.deepEqual(deployFloorWindow({ deploy: 20_529_207, pad: 100 }), {
    from: 20_529_107,
    to: 20_529_307,
    tag: 'deploy@20529207',
  });
  assert.equal(
    deployFloorWindow({ deploy: 40, pad: 100 }).from,
    0,
    'clamped to 0',
  );
});

test('formatEraWindows: eth returns the four eras, non-eth returns none', () => {
  const eras = formatEraWindows({ chainId: 1, span: 50 });
  assert.equal(eras.length, 4);
  assert.deepEqual(
    eras.map((e) => e.from).sort((a, b) => a - b),
    Object.values(ETH_FORMAT_ERAS).sort((a, b) => a - b),
  );
  assert.equal(formatEraWindows({ chainId: 8453 }).length, 0);
});

test('halveWindow keeps the lower half and records the original upper bound', () => {
  assert.deepEqual(halveWindow({ from: 100, to: 200 }), {
    from: 100,
    to: 150,
    halvedFrom: 200,
  });
});

test('autoShrink fires only above threshold and is idempotent in shape', () => {
  const window = { from: 0, to: 4_000 };
  assert.deepEqual(autoShrink({ window, matchedRows: 10, threshold: 50_000 }), {
    window,
    shrunk: false,
  });

  const shrunk = autoShrink({ window, matchedRows: 60_000, threshold: 50_000 });
  assert.equal(shrunk.shrunk, true);
  assert.equal(shrunk.window.to, 2_000, 'halved');
});

test('resolveWindowEntry: literal passes through, frontier marks or resolves', () => {
  assert.deepEqual(resolveWindowEntry({ from: 10, to: 20 }), [
    { from: 10, to: 20, tag: 'literal' },
  ]);

  const marker = resolveWindowEntry({ strategy: 'frontier', span: 30 });
  assert.equal(marker[0].frontier, true);

  const resolved = resolveWindowEntry(
    { strategy: 'frontier', span: 30 },
    { head: 1_000 },
  );
  assert.deepEqual(resolved, [{ from: 971, to: 1_000, tag: 'frontier' }]);
});

test('resolveWindowEntry: full-range marks without head, resolves [from,head] with head', () => {
  const marker = resolveWindowEntry({
    strategy: 'full-range',
    from: 20_529_207,
  });
  assert.equal(marker[0].fullRange, true);
  assert.equal(marker[0].from, 20_529_207);

  const resolved = resolveWindowEntry(
    { strategy: 'full-range', from: 20_529_207 },
    { head: 21_000_000 },
  );
  assert.deepEqual(resolved, [
    { from: 20_529_207, to: 21_000_000, tag: 'full-range' },
  ]);
});

test('resolveWindowEntry rejects an unknown strategy loudly', () => {
  assert.throws(
    () => resolveWindowEntry({ strategy: 'nope' }),
    /unknown strategy/,
  );
});

test('resolveWindows flattens a mixed cell deterministically', () => {
  const cell = {
    chainId: 1,
    windows: [
      { from: 100, to: 200 },
      {
        strategy: 'seeded-random',
        seed: 7,
        from: 1_000_000,
        to: 2_000_000,
        count: 3,
        size: 1_000,
      },
      { strategy: 'deploy-floor', deploy: 1_500_000, pad: 50 },
    ],
  };
  const first = resolveWindows(cell);
  const second = resolveWindows(cell);

  assert.deepEqual(first, second);
  assert.equal(first.length, 5, '1 literal + 3 seeded + 1 deploy-floor');
});
