import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import fc from 'fast-check';
import { describe, expect, test } from 'vitest';
import {
  type CutoverSyncProgress,
  cutoverExcludedIndices,
  cutoverProbeIndices,
  isEndCappedAtCutover,
  omnichainCutoverProbeIndices,
  shouldSkipCutoverRound,
  shouldSkipOmnichainCutoverRound,
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

// ─────────────────────────── shouldSkipOmnichainCutoverRound (finding 1) ───────────────────────────

describe('INV-18: shouldSkipOmnichainCutoverRound (pending-aware break)', () => {
  const allCapped = () => {
    const chains = ['a', 'b'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(100, 200)], // end-capped
        ['b', sync(200, 200)], // end-capped
      ],
    );

    return { chains: [...chains], perChainSync };
  };

  test('all chains end-capped AND pending EMPTY → skip (fully-bounded, nothing parked)', () => {
    const { chains, perChainSync } = allCapped();

    expect(shouldSkipOmnichainCutoverRound(chains, perChainSync, 0)).toBe(true);
  });

  // ── The Finding-1 regression: an unconditional break here would `yield { type: "pending" }` the
  //    parked events to realtime, which skips every end-capped chain — silently DROPPING them. The
  //    break must NOT be taken while pending is non-empty; the probe/catchup path drains it first.
  test('all chains end-capped BUT pending NON-EMPTY → do NOT skip (drain parked events first)', () => {
    const { chains, perChainSync } = allCapped();

    expect(shouldSkipOmnichainCutoverRound(chains, perChainSync, 1)).toBe(
      false,
    );
    expect(shouldSkipOmnichainCutoverRound(chains, perChainSync, 42)).toBe(
      false,
    );
  });

  test('a chain still probeable → do NOT skip regardless of pending count', () => {
    const chains = ['a', 'b'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(100, 200)], // end-capped
        ['b', sync(undefined, 200)], // unbounded → still needs the probe
      ],
    );

    expect(shouldSkipOmnichainCutoverRound([...chains], perChainSync, 0)).toBe(
      false,
    );
    expect(shouldSkipOmnichainCutoverRound([...chains], perChainSync, 5)).toBe(
      false,
    );
  });

  test('empty chain list → skip only when nothing is parked', () => {
    expect(shouldSkipOmnichainCutoverRound([], new Map(), 0)).toBe(true);
    expect(shouldSkipOmnichainCutoverRound([], new Map(), 1)).toBe(false);
  });

  test('property: skip ⟺ (pending === 0 ∧ shouldSkipCutoverRound)', () => {
    const entryArb = fc.record({
      end: fc.option(fc.integer({ min: 0, max: 1000 }), { nil: undefined }),
      finalized: fc.integer({ min: 0, max: 1000 }),
      mapped: fc.boolean(),
    });

    fc.assert(
      fc.property(
        fc.array(entryArb, { maxLength: 6 }),
        fc.nat({ max: 20 }),
        (entries, pendingCount) => {
          const chains = entries.map((_, i) => `chain-${i}`);
          const perChainSync = new Map<
            string,
            { syncProgress: CutoverSyncProgress }
          >();
          entries.forEach((e, i) => {
            if (e.mapped) {
              perChainSync.set(chains[i]!, sync(e.end, e.finalized));
            }
          });

          const expected =
            pendingCount === 0 && shouldSkipCutoverRound(chains, perChainSync);

          expect(
            shouldSkipOmnichainCutoverRound(chains, perChainSync, pendingCount),
          ).toBe(expected);
        },
      ),
      { seed: 45, numRuns: 400 },
    );
  });
});

// ─────────────── omnichainCutoverProbeIndices — PHASE 1 of the two-phase probe (INV-18) ───────────────

describe('INV-18: omnichainCutoverProbeIndices (phase-1 necessity-gated probe)', () => {
  test('pending EMPTY → identical to cutoverProbeIndices (end-capped chains excluded)', () => {
    const chains = ['a', 'b', 'c'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(100, 200)], // end-capped → excluded
        ['b', sync(undefined, 200)], // unbounded → probe
        ['c', sync(200, 200)], // end-capped → excluded
      ],
    );

    expect(omnichainCutoverProbeIndices([...chains], perChainSync, 0)).toEqual(
      cutoverProbeIndices([...chains], perChainSync),
    );
    expect(omnichainCutoverProbeIndices([...chains], perChainSync, 0)).toEqual([
      1,
    ]);
  });

  test('pending NON-EMPTY → probe EVERY chain (already on the drain path)', () => {
    const chains = ['a', 'b'] as const;
    const perChainSync = new Map<string, { syncProgress: CutoverSyncProgress }>(
      [
        ['a', sync(100, 200)], // end-capped
        ['b', sync(200, 200)], // end-capped
      ],
    );

    // With nothing parked the reduced set is empty; once events are parked, probe all.
    expect(cutoverProbeIndices([...chains], perChainSync)).toEqual([]);
    expect(omnichainCutoverProbeIndices([...chains], perChainSync, 1)).toEqual([
      0, 1,
    ]);
    expect(omnichainCutoverProbeIndices([...chains], perChainSync, 42)).toEqual(
      [0, 1],
    );
  });

  test('property: pending>0 ⟺ ALL indices; pending===0 ⟺ cutoverProbeIndices', () => {
    const entryArb = fc.record({
      end: fc.option(fc.integer({ min: 0, max: 1000 }), { nil: undefined }),
      finalized: fc.integer({ min: 0, max: 1000 }),
      mapped: fc.boolean(),
    });

    fc.assert(
      fc.property(
        fc.array(entryArb, { minLength: 1, maxLength: 6 }),
        fc.nat({ max: 20 }),
        (entries, pendingCount) => {
          const chains = entries.map((_, i) => `chain-${i}`);
          const perChainSync = new Map<
            string,
            { syncProgress: CutoverSyncProgress }
          >();
          entries.forEach((e, i) => {
            if (e.mapped) {
              perChainSync.set(chains[i]!, sync(e.end, e.finalized));
            }
          });

          const got = omnichainCutoverProbeIndices(
            chains,
            perChainSync,
            pendingCount,
          );

          if (pendingCount > 0) {
            expect(got).toEqual(chains.map((_, i) => i));
          } else {
            expect(got).toEqual(cutoverProbeIndices(chains, perChainSync));
          }
        },
      ),
      { seed: 46, numRuns: 400 },
    );
  });
});

// ─────────────── cutoverExcludedIndices — PHASE 2 of the two-phase probe (INV-18) ───────────────

describe('INV-18: cutoverExcludedIndices (phase-2 excluded complement)', () => {
  const chains = ['a', 'b', 'c', 'd'] as const;

  test('returns the complement of the phase-1 probe set, in chains order', () => {
    expect(cutoverExcludedIndices([...chains], [1, 2])).toEqual([0, 3]);
    expect(cutoverExcludedIndices([...chains], [0, 1, 2, 3])).toEqual([]);
    expect(cutoverExcludedIndices([...chains], [])).toEqual([0, 1, 2, 3]);
  });

  test('order of the probe set does not matter (result is always ascending chains order)', () => {
    expect(cutoverExcludedIndices([...chains], [2, 1])).toEqual([0, 3]);
    expect(cutoverExcludedIndices([...chains], [3, 0])).toEqual([1, 2]);
  });

  test('property: probeIndices and cutoverExcludedIndices partition [0, chains.length)', () => {
    fc.assert(
      fc.property(
        fc.nat({ max: 8 }),
        fc.array(fc.nat({ max: 7 })),
        (n, rawProbe) => {
          const chainList = Array.from({ length: n }, (_, i) => `chain-${i}`);
          // A valid phase-1 probe set is a subset of [0, n) with no duplicates.
          const probe = [...new Set(rawProbe.filter((i) => i < n))].sort(
            (x, y) => x - y,
          );

          const excluded = cutoverExcludedIndices(chainList, probe);

          // Disjoint.
          expect(probe.some((i) => excluded.includes(i))).toBe(false);
          // Union covers every index exactly once.
          expect([...probe, ...excluded].sort((x, y) => x - y)).toEqual(
            Array.from({ length: n }, (_, i) => i),
          );
        },
      ),
      { seed: 47, numRuns: 400 },
    );
  });
});

// ────────── WHOLE-LOOP behavioral model of getHistoricalEventsOmnichain cutover (INV-18) ──────────
//
// The helper tests above pin the probe SETS. This models the FULL cutover loop across rounds — parking,
// catchup adoption, and the drain-vs-drop decision — parameterised by probe policy, so it reproduces the
// end-to-end silent-drop bug rather than a single round in isolation. It mirrors the grafted
// `getHistoricalEventsOmnichain` tail exactly:
//   omnichainTo = min( min-over-chains(finalized), max-over-chains(end) );   // getOmnichainCheckpoint
//   a chain fetches (cursor, to] where to = min(finalized, end); an event above omnichainTo PARKS;
//   PHASE 1 probes the necessity-gated set; shouldCatchup = ∃ chain: probed−old > finalityBlockCount;
//   PHASE 2 (two-phase only): if shouldCatchup ∧ chains were excluded, probe+adopt the excluded set too;
//   if !shouldCatchup ⇒ the loop BREAKS and yields pendingEvents to realtime, which DROPS every event on
//   an isEnd() (end-capped) chain. A parked event whose checkpoint ≤ a later omnichainTo drains instead.
//
// This is the mechanical whole-loop check from the review, lifted into the suite and wired to the ACTUAL
// guard helpers so a regression in either phase re-opens the drop and fails here.

type LoopChain = {
  end: number | undefined;
  finalized: number;
  /** What a live refetch would return for this chain (≥ finalized). */
  trueFinalized: number;
  finalityBlockCount: number;
  /** Fetch cursor — the highest block already extracted. */
  cursor: number;
  /** Block numbers at which this chain has an event to emit. */
  events: number[];
};

type LoopPolicy = 'base' | 'pending-only' | 'two-phase';

const omnichainTo = (chains: LoopChain[]): number => {
  const minFinalized = Math.min(...chains.map((c) => c.finalized));
  const ends = chains.map((c) => c.end);
  const maxEnd = ends.some((e) => e === undefined)
    ? Number.POSITIVE_INFINITY
    : Math.max(...(ends as number[]));

  return Math.min(minFinalized, maxEnd);
};

/** Run the omnichain cutover loop to termination; return the parked events handed to (and dropped by)
 *  realtime — i.e. events that were never drained historically. `seedPending` models events already
 *  parked by an EARLIER historical round (when the shared omnichainTo was lower), the precondition of the
 *  all-capped + pending fall-through trace. */
const simulateOmnichainCutover = (
  makeChains: () => LoopChain[],
  policy: LoopPolicy,
  seedPending: number[] = [],
): number[] => {
  const chains = makeChains();
  const perChainSync = () =>
    new Map<number, { syncProgress: CutoverSyncProgress }>(
      chains.map((c, i) => [
        i,
        {
          syncProgress: {
            end: c.end === undefined ? undefined : block(c.end),
            finalized: block(c.finalized),
          },
        },
      ]),
    );
  const indices = chains.map((_, i) => i);

  const pending = new Set<number>(seedPending);
  let guard = 0;

  while (guard++ < 100) {
    const oto = omnichainTo(chains);

    // Extract (cursor, to] per chain; an event above the shared omnichainTo parks, else it drains.
    for (const c of chains) {
      const to = Math.min(c.finalized, c.end ?? Number.POSITIVE_INFINITY);
      for (const b of c.events) {
        if (b <= c.cursor || b > to) continue;

        if (b > oto) pending.add(b);
      }
      c.cursor = Math.max(c.cursor, to);
    }

    // Re-filter the parked buffer against the (possibly advanced) omnichainTo.
    for (const b of [...pending]) {
      if (b <= oto) pending.delete(b);
    }

    // Pending-aware all-capped break (shouldSkipOmnichainCutoverRound).
    if (
      shouldSkipOmnichainCutoverRound(indices, perChainSync(), pending.size)
    ) {
      break;
    }

    // PHASE 1 probe set.
    const probeSet =
      policy === 'base'
        ? cutoverProbeIndices(indices, perChainSync())
        : omnichainCutoverProbeIndices(indices, perChainSync(), pending.size);

    const finalizedAfter = chains.map((c, i) =>
      probeSet.includes(i) ? c.trueFinalized : c.finalized,
    );

    const shouldCatchup = chains.some(
      (c, i) => finalizedAfter[i]! - c.finalized > c.finalityBlockCount,
    );

    if (shouldCatchup === false) break;

    // PHASE 2: on a catchup round, adopt main-equivalent finality for the excluded chains too.
    if (policy === 'two-phase') {
      for (const i of cutoverExcludedIndices(indices, probeSet)) {
        finalizedAfter[i] = chains[i]!.trueFinalized;
      }
    }

    chains.forEach((c, i) => {
      c.finalized = finalizedAfter[i]!;
    });
  }

  return [...pending].sort((a, b) => a - b);
};

describe('INV-18: whole-loop omnichain cutover (drop reproduction)', () => {
  // The mixed-case stale-floor counterexample (review round-1). Chain A is end-capped BELOW chain B's
  // end, so A's stale finalized (100) is the min-over-chains floor that caps omnichainTo at 100 — parking
  // chain B's event at 105 forever. On the round that PARKS event 105, pending is still 0, so a pending-
  // only gate never probes A; only two-phase (which adopts A on B's catchup round) lifts the floor to 110
  // and drains it. Base PR and the pending-only gate both DROP; two-phase and upstream main do not.
  const mixedStaleFloor = (): LoopChain[] => [
    {
      end: 90, // end-capped: end 90 ≤ finalized 100
      finalized: 100,
      trueFinalized: 110, // a live probe raises A to 110
      finalityBlockCount: 10,
      cursor: 0,
      events: [],
    },
    {
      end: 110, // active: end 110 > finalized 100
      finalized: 100,
      trueFinalized: 111, // a live probe raises B to 111 (delta 11 > 10 ⇒ triggers catchup)
      finalityBlockCount: 10,
      cursor: 0,
      events: [105], // parks above omnichainTo=100, ≤ B's end 110
    },
  ];

  test('mixed stale-floor: BASE probe DROPS event 105 (the review finding)', () => {
    expect(simulateOmnichainCutover(mixedStaleFloor, 'base')).toEqual([105]);
  });

  test('mixed stale-floor: PENDING-ONLY gate STILL DROPS event 105 (insufficient — parking round has pending=0)', () => {
    expect(simulateOmnichainCutover(mixedStaleFloor, 'pending-only')).toEqual([
      105,
    ]);
  });

  test('mixed stale-floor: TWO-PHASE drains event 105 (no drop — matches upstream main)', () => {
    expect(simulateOmnichainCutover(mixedStaleFloor, 'two-phase')).toEqual([]);
  });

  // The all-capped + pending>0 fall-through trace (review round-2). Both chains are end-capped at loop
  // entry, but chain A's finalized (110) is STALE — its true head is 500 — and it is the min-over-chains
  // floor, so the shared omnichainTo = min(min(110,200), max(110,200)) = 110. An event parked at 150 by an
  // earlier historical round (seedPending) is above that floor. The BASE probe set is EMPTY (all chains
  // end-capped), so no finalized can rise, shouldCatchup stays false, the loop breaks, and realtime skips
  // both end-capped chains — DROP. The necessity gate (phase 1) probes EVERY chain while pending>0, so A
  // rises 110→500, omnichainTo lifts to 200 ≥ 150, and the parked event drains.
  const allCappedPending = (): LoopChain[] => [
    {
      end: 110, // end-capped (end 110 ≤ finalized 110)
      finalized: 110, // STALE — the min-over-chains floor capping omnichainTo at 110
      trueFinalized: 500, // a live probe raises A far past its stale finalized
      finalityBlockCount: 10,
      cursor: 110,
      events: [],
    },
    {
      end: 200, // end-capped (end 200 ≤ finalized 200)
      finalized: 200,
      trueFinalized: 200,
      finalityBlockCount: 10,
      cursor: 200,
      events: [],
    },
  ];

  test('all-capped + pending: BASE probe DROPS the parked event (empty probe set can never lift the floor)', () => {
    expect(simulateOmnichainCutover(allCappedPending, 'base', [150])).toEqual([
      150,
    ]);
  });

  test('all-capped + pending: TWO-PHASE drains the parked event (necessity gate probes every chain)', () => {
    expect(
      simulateOmnichainCutover(allCappedPending, 'two-phase', [150]),
    ).toEqual([]);
  });

  test('steady-state fully-bounded (all end-capped, no advance, nothing parked): no drop, terminates', () => {
    const steady = (): LoopChain[] => [
      {
        end: 100,
        finalized: 100,
        trueFinalized: 100,
        finalityBlockCount: 10,
        cursor: 100,
        events: [],
      },
      {
        end: 200,
        finalized: 200,
        trueFinalized: 200,
        finalityBlockCount: 10,
        cursor: 200,
        events: [],
      },
    ];

    expect(simulateOmnichainCutover(steady, 'two-phase')).toEqual([]);
  });
});

// ─────────────────────────── wiring pins: the grafted historical.ts call sites ───────────────────────────
//
// The guard helpers are pure and unit-tested above, but the FINDINGS live at the call sites in the
// grafted `runtime/historical.ts` (applied by `portal/wiring/{0.16.6,0.15.17}.patch`). These tests read
// the patched file (copied next to `src/` by sync-upstream.sh) and pin the exact wiring so a future
// patch edit that regresses either finding fails here. Both version patches graft the SAME source, so
// one read covers both — the added historical.ts hunks are byte-identical across versions by design.

describe('INV-18: wiring pins (grafted runtime/historical.ts)', () => {
  const historical = readFileSync(
    join(__dirname, '..', 'runtime', 'historical.ts'),
    'utf8',
  );

  // Isolate the omnichain function body so the pins can't accidentally match a multichain hunk.
  const omnichain = historical.slice(
    historical.indexOf('getHistoricalEventsOmnichain'),
    historical.indexOf('getHistoricalEventsMultichain'),
  );
  const multichain = historical.slice(
    historical.indexOf('getHistoricalEventsMultichain'),
    historical.indexOf('getHistoricalEventsIsolated'),
  );

  test('finding 1: omnichain break is pending-aware (shouldSkipOmnichainCutoverRound with pendingEvents.length)', () => {
    // The all-capped break must route through the pending-aware helper AND pass the pending count, so it
    // never fires while parked events would otherwise be dropped to realtime.
    expect(omnichain).toContain('shouldSkipOmnichainCutoverRound(');
    expect(omnichain).toContain('pendingEvents.length');
    // And it must NOT use the plain, unconditional helper for its break (that was the bug).
    expect(omnichain).not.toContain(
      'shouldSkipCutoverRound(params.indexingBuild.chains, params.perChainSync)',
    );
  });

  test('finding 1: multichain break is untouched (plain shouldSkipCutoverRound — no pending mechanism)', () => {
    // Multichain has no pendingEvents; its break semantics must stay exactly as before this fix.
    expect(multichain).toContain(
      'shouldSkipCutoverRound(params.indexingBuild.chains, params.perChainSync)',
    );
    expect(multichain).not.toContain('shouldSkipOmnichainCutoverRound');
  });

  test('two-phase (phase 1): omnichain PROBE SET is the necessity-gated helper with pendingEvents.length', () => {
    // The reduced probe set must route through the necessity-gated helper (phase 1), not the plain
    // pending-blind `cutoverProbeIndices` — otherwise an all-capped+pending round can never lift the floor.
    expect(omnichain).toContain('omnichainCutoverProbeIndices(');
    const gatedAt = omnichain.indexOf(
      'const cutoverProbe = omnichainCutoverProbeIndices(',
    );
    expect(gatedAt).toBeGreaterThan(-1);
    expect(omnichain.slice(gatedAt, gatedAt + 200)).toContain(
      'pendingEvents.length',
    );
    // The omnichain probe set must NOT be computed via the plain (pending-blind) helper.
    expect(omnichain).not.toContain(
      'const cutoverProbe = cutoverProbeIndices(',
    );
  });

  test('two-phase (phase 2): omnichain re-probes the EXCLUDED complement on a catchup round, before adopting', () => {
    // Phase 2 closes the mixed-case stale-floor drop: after `shouldCatchup` fires, the excluded end-capped
    // chains must be re-probed + clamped + adopted so no stale finalized caps omnichainTo. Pin that the
    // phase-2 loop (a) computes the excluded complement, (b) sits AFTER the `if (shouldCatchup === false)
    // break;`, and (c) runs BEFORE the adopt loop that writes syncProgress.finalized.
    expect(omnichain).toContain('cutoverExcludedIndices(');

    const breakAt = omnichain.indexOf('if (shouldCatchup === false) break;');
    const phase2At = omnichain.indexOf(
      'const excludedProbe = cutoverExcludedIndices(',
    );
    const adoptAt = omnichain.indexOf(
      'params.perChainSync.get(chain)!.syncProgress.finalized = finalizedBlock;',
    );

    expect(breakAt).toBeGreaterThan(-1);
    expect(phase2At).toBeGreaterThan(breakAt);
    expect(adoptAt).toBeGreaterThan(phase2At);

    // The phase-2 loop re-probes those indices and clamps them into finalizedBlocks before adoption.
    const phase2Body = omnichain.slice(phase2At, adoptAt);
    expect(phase2Body).toContain('for (const i of excludedProbe)');
    expect(phase2Body).toContain('probeFinalizedBlock(');
    expect(phase2Body).toContain('clampProbedFinalized(');
  });

  test('two-phase: multichain stays single-phase (plain cutoverProbeIndices, no phase-2 re-probe)', () => {
    // Multichain has no parking/pending mechanism, so the two-phase probe is omnichain-only.
    expect(multichain).toContain('const cutoverProbe = cutoverProbeIndices(');
    expect(multichain).not.toContain('omnichainCutoverProbeIndices');
    expect(multichain).not.toContain('cutoverExcludedIndices');
  });

  test('finding 2 (omnichain phase-1 clamp loop): excluded chains bypass the clamp', () => {
    // The clamp is a no-op for excluded end-capped chains by construction, and in stream mode it probes
    // the Portal head and THROWS — so excluded chains must bypass it. Omnichain routes its phase-1 clamp
    // through the shared `clampProbedFinalized(i, …)` closure, guarded by `cutoverProbe.includes(i)`.
    const clampAt = omnichain.indexOf(
      'finalizedBlocks[i] = await clampProbedFinalized(i, finalizedBlocks[i]!);',
    );
    expect(clampAt).toBeGreaterThan(-1);

    const beforeClamp = omnichain.slice(0, clampAt);
    const loopAt = beforeClamp.lastIndexOf(
      'for (let i = 0; i < finalizedBlocks.length; i++)',
    );
    expect(loopAt).toBeGreaterThan(-1);
    expect(omnichain.slice(loopAt, clampAt)).toContain(
      'if (cutoverProbe.includes(i) === false) continue;',
    );
  });

  test('finding 2 (multichain clamp loop): excluded chains bypass the clamp', () => {
    // Multichain is unchanged by two-phase — it still clamps inline via `clampFinalizedToPortalHead({ … })`
    // inside the finalizedBlocks loop, guarded by `cutoverProbe.includes(i)`.
    const clampAt = multichain.indexOf('clampFinalizedToPortalHead({');
    expect(clampAt).toBeGreaterThan(-1);

    const beforeClamp = multichain.slice(0, clampAt);
    const loopAt = beforeClamp.lastIndexOf(
      'for (let i = 0; i < finalizedBlocks.length; i++)',
    );
    expect(loopAt).toBeGreaterThan(-1);
    expect(multichain.slice(loopAt, clampAt)).toContain(
      'if (cutoverProbe.includes(i) === false) continue;',
    );
  });
});
