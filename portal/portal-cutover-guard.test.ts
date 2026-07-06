import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import fc from 'fast-check';
import { describe, expect, test } from 'vitest';
import {
  type CutoverSyncProgress,
  cutoverProbeIndices,
  isEndCappedAtCutover,
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

  test('finding 2: BOTH clamp loops skip excluded (non-probed) chains', () => {
    // The clamp is a no-op for excluded end-capped chains by construction, and in stream mode it probes
    // the Portal head and THROWS — so excluded chains must bypass it (guard: cutoverProbe.includes(i)).
    for (const body of [omnichain, multichain]) {
      const clampAt = body.indexOf('clampFinalizedToPortalHead({');
      expect(clampAt).toBeGreaterThan(-1);

      // The guard `if (cutoverProbe.includes(i) === false) continue;` must sit just before the clamp call
      // inside the finalizedBlocks loop.
      const beforeClamp = body.slice(0, clampAt);
      const loopAt = beforeClamp.lastIndexOf(
        'for (let i = 0; i < finalizedBlocks.length; i++)',
      );
      expect(loopAt).toBeGreaterThan(-1);
      expect(body.slice(loopAt, clampAt)).toContain(
        'if (cutoverProbe.includes(i) === false) continue;',
      );
    }
  });
});
