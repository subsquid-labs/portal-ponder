import fc from 'fast-check';
import { expect, test } from 'vitest';
import {
  chunkRange,
  evictionPlan,
  fetchBounds,
  idxOf,
  readAheadPlan,
  scaleChunkBlocks,
  traceSafeChunkBlocks,
} from './portal-chunks.js';

fc.configureGlobal({ seed: 1337 }); // deterministic CI

const INF = Number.POSITIVE_INFINITY;

test('idxOf / chunkRange: grid-aligned span', () => {
  expect(idxOf(0, 500_000)).toBe(0);
  expect(idxOf(499_999, 500_000)).toBe(0);
  expect(idxOf(500_000, 500_000)).toBe(1);
  expect(chunkRange(0, 500_000, 0, INF)).toEqual([0, 499_999]);
  expect(chunkRange(1, 500_000, 0, INF)).toEqual([500_000, 999_999]);
  // clamped to the backfill window on both sides
  expect(chunkRange(0, 500_000, 100, INF)).toEqual([100, 499_999]);
  expect(chunkRange(1, 500_000, 0, 600_000)).toEqual([500_000, 600_000]);
});

test('#50 fetchBounds: bounded window is ordered and never exceeds desiredTo', () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 1, max: 1_000_000 }),
      (gridFrom, span, quantum) => {
        const desiredTo = gridFrom + span;
        const [from, to] = fetchBounds(gridFrom, desiredTo, undefined, quantum);
        expect(from).toBeLessThanOrEqual(to);
        expect(to).toBeLessThanOrEqual(desiredTo);
      },
    ),
  );
});

test('#50 fetchBounds: covers the clamped need interval', () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 1, max: 1_000_000 }),
      (gridFrom, span, quantum) => {
        const desiredTo = gridFrom + span;
        const needLo = gridFrom + Math.floor(span / 3);
        const needHi = gridFrom + Math.floor((span * 2) / 3);
        const [from, to] = fetchBounds(
          gridFrom,
          desiredTo,
          [needLo, needHi],
          quantum,
        );
        expect(from).toBeLessThanOrEqual(needLo);
        expect(to).toBeGreaterThanOrEqual(needHi);
        expect(to).toBeLessThanOrEqual(desiredTo);
      },
    ),
  );
});

test('#50 fetchBounds: no need fetches at least the quantum-sized prefix when available', () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 1, max: 1_000_000 }),
      (gridFrom, span, quantum) => {
        const desiredTo = gridFrom + span;
        const [from, to] = fetchBounds(gridFrom, desiredTo, undefined, quantum);
        expect(to - from + 1).toBe(Math.min(quantum, span + 1));
      },
    ),
  );
});

test('#50 fetchBounds: infinite quantum preserves the legacy full-chunk identity', () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 0, max: 5_000_000 }),
      (gridFrom, span, needPad) => {
        const desiredTo = gridFrom + span;
        const needLo = Math.max(0, gridFrom - needPad);
        const [from, to] = fetchBounds(
          gridFrom,
          desiredTo,
          [needLo, desiredTo],
          Number.POSITIVE_INFINITY,
        );
        expect([from, to]).toEqual([gridFrom, desiredTo]);
      },
    ),
  );
});

test('INV-2 support: chunks of an interval cover it, are disjoint, and are grid-aligned', () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 1, max: 50 }).map((k) => k * 1000), // chunkBlocks
      fc.integer({ min: 0, max: 5_000_000 }),
      fc.integer({ min: 0, max: 5_000_000 }),
      (cb, a, b) => {
        const lo = Math.min(a, b);
        const hi = Math.max(a, b);
        const startIdx = idxOf(lo, cb);
        const endIdx = idxOf(hi, cb);
        const ranges: [number, number][] = [];
        for (let i = startIdx; i <= endIdx; i++)
          ranges.push(chunkRange(i, cb, 0, INF));
        // coverage: union spans [lo, hi]
        expect(ranges[0]![0]).toBeLessThanOrEqual(lo);
        expect(ranges[ranges.length - 1]![1]).toBeGreaterThanOrEqual(hi);
        // disjoint + contiguous + aligned
        for (let i = 0; i < ranges.length; i++) {
          expect(ranges[i]![0]).toBe((startIdx + i) * cb); // aligned
          if (i > 0) expect(ranges[i]![0]).toBe(ranges[i - 1]![1] + 1); // contiguous, disjoint
        }
      },
    ),
  );
});

test('scaleChunkBlocks: density scaling ×head/25M, capped 25M', () => {
  expect(scaleChunkBlocks(500_000, 20_000_000)).toBe(500_000); // density 1
  expect(scaleChunkBlocks(500_000, 478_000_000)).toBe(9_500_000); // ~19×
  expect(scaleChunkBlocks(500_000, 10_000_000_000)).toBe(25_000_000); // capped
  fc.assert(
    fc.property(fc.integer({ min: 0, max: 2_000_000_000 }), (head) => {
      const v = scaleChunkBlocks(500_000, head);
      expect(v).toBeGreaterThanOrEqual(500_000);
      expect(v).toBeLessThanOrEqual(25_000_000);
    }),
  );
});

test('traceSafeChunkBlocks: caps only when dense sources present and base exceeds cap', () => {
  expect(traceSafeChunkBlocks(500_000, false, 25_000)).toBe(500_000);
  expect(traceSafeChunkBlocks(500_000, true, 25_000)).toBe(25_000);
  expect(traceSafeChunkBlocks(10_000, true, 25_000)).toBe(10_000);
});

test('INV-13: read-ahead plan is bounded (≤ readahead), within raEnd, and depth-1-only when saturated', () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 100 }), // endIdx
      fc.integer({ min: 1, max: 10 }), // readahead
      fc.boolean(), // saturated
      (endIdx, readahead, saturated) => {
        const cb = 1000;
        const raEnd = INF;
        const plan = readAheadPlan(endIdx, cb, raEnd, readahead, saturated);
        expect(plan.length).toBeLessThanOrEqual(readahead);
        // depth-1 is ALWAYS prefetched, even when saturated (exactly 1 — saturation only stops deeper)
        if (saturated) expect(plan.length).toBe(1);
        else expect(plan.length).toBe(readahead); // raEnd = ∞ → the full depth
        // strictly increasing, starting at endIdx+1
        for (const [i, idx] of plan.entries()) expect(idx).toBe(endIdx + 1 + i);
      },
    ),
  );
  // never past raEnd
  expect(readAheadPlan(10, 1000, 11_500, 6, false)).toEqual([11]); // chunk 12 starts at 12000 > 11500
});

test('eviction plan: only chunks whose whole span is behind the cursor', () => {
  expect(evictionPlan([0, 1, 2, 3], 1000, 2500)).toEqual([0, 1]); // chunks 0,1 end < 2500; chunk 2 ends 2999
  expect(evictionPlan([5, 6], 1000, 0)).toEqual([]);
  fc.assert(
    fc.property(
      fc.array(fc.integer({ min: 0, max: 100 })),
      fc.integer({ min: 0, max: 100_000 }),
      (idxs, start) => {
        for (const i of evictionPlan(idxs, 1000, start))
          expect((i + 1) * 1000).toBeLessThanOrEqual(start);
      },
    ),
  );
});
