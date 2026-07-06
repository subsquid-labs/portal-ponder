import fc from 'fast-check';
import { describe, expect, test } from 'vitest';
import {
  type CutoverSyncProgress,
  cutoverProbeIndices,
  isEndCappedAtCutover,
  shouldSkipCutoverRound,
} from './portal-cutover-guard.js';

/** A block-number stub (hex `number`, as `SyncBlock`/`LightBlock` carry it). */
const block = (n: number): { number: string } => ({
  number: `0x${n.toString(16)}`,
});

/** A per-chain `syncProgress` stub with the configured `end` and current `finalized`. */
const sync = (
  end: number | undefined,
  finalized: number,
): { syncProgress: CutoverSyncProgress } => ({
  syncProgress: {
    end: end === undefined ? undefined : block(end),
    finalized: block(finalized),
  },
});

// ─────────────────────────────── isEndCappedAtCutover ───────────────────────────────

describe('INV-18: isEndCappedAtCutover', () => {
  test('end BELOW finalized → end-capped (never probe)', () => {
    expect(
      isEndCappedAtCutover({ end: block(100), finalized: block(200) }),
    ).toBe(true);
  });

  test('end EQUAL to finalized → end-capped (`to = min(finalized, end) = end`)', () => {
    expect(
      isEndCappedAtCutover({ end: block(200), finalized: block(200) }),
    ).toBe(true);
  });

  test('end ABOVE finalized → NOT end-capped (finality-capped: must keep probing)', () => {
    expect(
      isEndCappedAtCutover({ end: block(300), finalized: block(200) }),
    ).toBe(false);
  });

  test('end undefined (unbounded source) → NOT end-capped (must keep probing)', () => {
    expect(
      isEndCappedAtCutover({ end: undefined, finalized: block(200) }),
    ).toBe(false);
  });

  test('large hex block numbers parse correctly (no precision surprise below 2^53)', () => {
    expect(
      isEndCappedAtCutover({
        end: block(19_000_000),
        finalized: block(19_000_001),
      }),
    ).toBe(true);
    expect(
      isEndCappedAtCutover({
        end: block(19_000_002),
        finalized: block(19_000_001),
      }),
    ).toBe(false);
  });
});

// ─────────────────────────────── cutoverProbeIndices ───────────────────────────────

describe('INV-18: cutoverProbeIndices', () => {
  test('excludes end-capped chains, keeps unbounded + finality-capped, preserves order', () => {
    const chains = ['a', 'b', 'c', 'd'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(100, 200)], // end-capped → excluded
        ['b', sync(undefined, 200)], // unbounded → probe
        ['c', sync(300, 200)], // finality-capped (end > finalized) → probe
        ['d', sync(200, 200)], // end == finalized → excluded
      ],
    );

    expect(cutoverProbeIndices([...chains], perChainSync)).toEqual([1, 2]);
  });

  test('all chains end-capped → empty probe set', () => {
    const chains = ['a', 'b'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(50, 200)],
        ['b', sync(200, 200)],
      ],
    );

    expect(cutoverProbeIndices([...chains], perChainSync)).toEqual([]);
  });

  test('regression guard (soaks): NO chain end-capped → EVERY index probes', () => {
    // The two production soak services run unbounded / finality-capped chains; the guard must NEVER
    // exclude them — regressing that would drop the stream-mode clamp the soaks depend on.
    const chains = ['a', 'b', 'c'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(undefined, 200)], // unbounded
        ['b', sync(500, 200)], // end above finalized
        ['c', sync(undefined, 999)], // unbounded
      ],
    );

    expect(cutoverProbeIndices([...chains], perChainSync)).toEqual([0, 1, 2]);
  });

  test('defensive: an UNMAPPED chain is treated as probeable (never silently skipped)', () => {
    const chains = ['a', 'b'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(50, 200)], // end-capped → excluded
        // 'b' is absent from the map → included by default (unknown finality)
      ],
    );

    expect(cutoverProbeIndices([...chains], perChainSync)).toEqual([1]);
  });
});

// ─────────────────────────────── shouldSkipCutoverRound ───────────────────────────────

describe('INV-18: shouldSkipCutoverRound', () => {
  test('ALL chains end-capped → skip the whole round (fully-bounded app: zero refetches)', () => {
    const chains = ['a', 'b'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(100, 200)],
        ['b', sync(200, 200)],
      ],
    );

    expect(shouldSkipCutoverRound([...chains], perChainSync)).toBe(true);
  });

  test('ANY chain not end-capped → do NOT skip (some chain still needs the probe)', () => {
    const chains = ['a', 'b'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(100, 200)], // end-capped
        ['b', sync(undefined, 200)], // unbounded → needs probe
      ],
    );

    expect(shouldSkipCutoverRound([...chains], perChainSync)).toBe(false);
  });

  test('empty chain list → vacuously skip (no chain to probe)', () => {
    expect(shouldSkipCutoverRound([], new Map())).toBe(true);
  });
});

// ─────────────────────────────── properties (fast-check) ───────────────────────────────

describe('INV-18: properties', () => {
  test('predicate ⟺ (end defined ∧ end ≤ finalized)', () => {
    fc.assert(
      fc.property(
        fc.option(fc.integer({ min: 0, max: 2 ** 40 }), { nil: undefined }),
        fc.integer({ min: 0, max: 2 ** 40 }),
        (end, finalized) => {
          const expected = end !== undefined && end <= finalized;

          expect(
            isEndCappedAtCutover({
              end: end === undefined ? undefined : block(end),
              finalized: block(finalized),
            }),
          ).toBe(expected);
        },
      ),
      { seed: 42, numRuns: 500 },
    );
  });

  test('monotonicity: raising `finalized` never un-caps an already end-capped chain', () => {
    // The cutover loop only ever RAISES finalized. An end-capped chain must stay end-capped so the
    // guard never flips it back into the probe set mid-loop.
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 2 ** 40 }),
        fc.integer({ min: 0, max: 2 ** 40 }),
        fc.integer({ min: 0, max: 2 ** 40 }),
        (end, finalized, delta) => {
          fc.pre(end <= finalized); // already end-capped
          const raised = finalized + delta;

          expect(
            isEndCappedAtCutover({ end: block(end), finalized: block(raised) }),
          ).toBe(true);
        },
      ),
      { seed: 43, numRuns: 500 },
    );
  });

  test('shouldSkipCutoverRound ⟺ cutoverProbeIndices is empty', () => {
    const entryArb = fc.record({
      end: fc.option(fc.integer({ min: 0, max: 1000 }), { nil: undefined }),
      finalized: fc.integer({ min: 0, max: 1000 }),
      mapped: fc.boolean(),
    });

    fc.assert(
      fc.property(fc.array(entryArb, { maxLength: 8 }), (entries) => {
        const chains = entries.map((_, i) => `chain-${i}`);
        const perChainSync = new Map<
          string,
          { syncProgress: CutoverSyncProgress }
        >();
        entries.forEach((e, i) => {
          if (e.mapped) perChainSync.set(chains[i]!, sync(e.end, e.finalized));
        });

        const indices = cutoverProbeIndices(chains, perChainSync);

        expect(shouldSkipCutoverRound(chains, perChainSync)).toBe(
          indices.length === 0,
        );
      }),
      { seed: 44, numRuns: 300 },
    );
  });
});
