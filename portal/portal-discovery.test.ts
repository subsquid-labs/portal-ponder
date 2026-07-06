import fc from 'fast-check';
import type { Address } from 'viem';
import { expect, test } from 'vitest';
import type { Factory } from '@/internal/types.js';
import type { PortalClient } from './portal-client.js';
import {
  createDiscovery,
  planDiscovery,
  splitWindows,
} from './portal-discovery.js';
import type { ChildAddresses } from './portal-filters.js';
import { createStats } from './portal-metrics.js';

fc.configureGlobal({ seed: 1337 }); // deterministic CI

const FACTORY_ADDR = '0x29a56a1b8214d9cf7c5561811750d5cbdb45cc8e';
const PROXY_CREATED =
  '0x04e664079117e113faa9684bc14aecb41651cbf098b14eda271248c6d0cda57c';
const factory = (): Factory =>
  ({
    id: 'f',
    type: 'log',
    chainId: 1,
    sourceId: 'EVault',
    address: FACTORY_ADDR,
    eventSelector: PROXY_CREATED as any,
    childAddressLocation: 'topic1',
    fromBlock: undefined,
    toBlock: undefined,
  }) as Factory;

const topicAddr = (addr: string) =>
  `0x${'0'.repeat(24)}${addr.replace(/^0x/, '')}`;
// a factory child "created" at block `bn`
const proxy = (child: string, bn: number) => ({
  header: { number: bn },
  logs: [
    {
      address: FACTORY_ADDR,
      topics: [PROXY_CREATED, topicAddr(child)],
      data: '0x',
    },
  ],
});

// a fake client whose discovery stream serves the given creation events within [lo,hi]; can be made to fail.
const fakeClient = (
  events: { child: string; bn: number }[],
  failWindows: (lo: number, hi: number) => boolean = () => false,
): PortalClient => ({
  finalizedHead: async () => undefined,
  finalizedHeadRetry: async () => undefined,
  async *stream(_q, from, to) {
    if (failWindows(from, to)) throw new Error('scan failed');
    const batch = events
      .filter((e) => e.bn >= from && e.bn <= to)
      .map((e) => proxy(e.child, e.bn));
    if (batch.length) yield batch as any;
  },
});

// ── pure window math ────────────────────────────────────────────────────────────────────────────

test('splitWindows: disjoint, cover [from,to], ≤ discoveryWindows', () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 1000 }),
      fc.integer({ min: 0, max: 5000 }),
      fc.integer({ min: 1, max: 16 }),
      (from, span, P) => {
        const to = from + span;
        const ws = splitWindows(from, to, 500, P);
        expect(ws.length).toBeLessThanOrEqual(P);
        expect(ws[0]![0]).toBe(from);
        expect(ws[ws.length - 1]![1]).toBe(to);
        for (let i = 1; i < ws.length; i++)
          expect(ws[i]![0]).toBe(ws[i - 1]![1] + 1); // disjoint + contiguous
      },
    ),
  );
});

test('planDiscovery: null when no floor or already covered; reaches endHint', () => {
  expect(
    planDiscovery({ floor: -1, through: -1 }, 100, {
      chunkBlocks: 50,
      endHint: 100,
      discoveryWindows: 8,
    }),
  ).toBeNull();
  expect(
    planDiscovery({ floor: 0, through: 100 }, 100, {
      chunkBlocks: 50,
      endHint: 100,
      discoveryWindows: 8,
    }),
  ).toBeNull();
  const p = planDiscovery({ floor: 0, through: -1 }, 100, {
    chunkBlocks: 50,
    endHint: 500,
    discoveryWindows: 8,
  });
  expect(p!.from).toBe(0);
  expect(p!.to).toBe(500); // reaches the endHint in one pass
});

// ── #21 §1: the factory-range gate — scanWindow delegates matching to ponder's isLogFactoryMatched ──
// INV-15's interval-scoped flush (`takePendingInRange`) is lossless ONLY because scanWindow rejects any
// creation log below `factory.fromBlock`: a sub-floor child that leaked into `childAddresses` + the
// pending queue would be suppressed at its in-range creation block by the min-merge guard
// (`prev === undefined || prev > bn`), so it would sit in memory forever — never persisted while its
// factory interval is marked cached — a silent restart loss. scanWindow gets that gate for free by
// delegating to ponder-core's `isLogFactoryMatched` (runtime/filter.ts). This pins the delegation at the
// UNIT level (independent of portal.ts's discovery-floor plumbing): the floor is set BELOW `fromBlock`,
// so scanWindow DOES stream the sub-floor creation log to the matcher — and only the matcher discards it.
// If the seam ever weakens (matcher bypassed, or the fromBlock check dropped), this fails loud.

test('#21 §1: a creation log BELOW factory.fromBlock is neither recorded nor queued/flushed (isLogFactoryMatched gate)', async () => {
  const gated = { ...factory(), fromBlock: 100 } as Factory; // floor gate at 100
  const events = [
    { child: '0xbe10', bn: 50 }, // BELOW fromBlock → the matcher must discard
    { child: '0xab0e', bn: 150 }, // above fromBlock → recorded + queued
  ];
  const childAddresses: ChildAddresses = new Map([
    ['f', new Map<Address, number>()],
  ]);
  const d = createDiscovery({
    client: fakeClient(events),
    childAddresses,
    factories: [gated],
    discoveryWindows: 4,
    stats: createStats(),
  });
  d.setFloor(0); // BELOW fromBlock on purpose: scanWindow streams block 50 to the matcher
  await d.ensure(500, { chunkBlocks: 100, endHint: 500 });

  const rec = childAddresses.get('f')!;
  // the sub-floor child is discarded by isLogFactoryMatched — never recorded, never queued.
  expect(rec.has('0xbe10' as Address)).toBe(false);
  expect(rec.get('0xab0e' as Address)).toBe(150); // the above-floor child is recorded at its creation block

  // AND it is absent from the pending flush over the FULL range (would-be persistence) — pinning that a
  // sub-floor child never reaches insertChildAddresses either. Only the above-floor child flushes.
  const flush = d.takePendingInRange(0, 500);
  const flushed = new Map(flush.flatMap(([, children]) => [...children]));
  expect(flushed.has('0xbe10' as Address)).toBe(false);
  expect(flushed.get('0xab0e' as Address)).toBe(150);
  expect(flushed.size).toBe(1);
});

// ── INV-4: earliest-creation convergence under shuffled/overlapping windows ─────────────────────────

test('INV-4: shuffled/overlapping discovery windows converge to the same earliest-creation map', async () => {
  const events = [
    { child: '0xaaa', bn: 5 },
    { child: '0xbbb', bn: 250 },
    { child: '0xaaa', bn: 400 },
  ]; // 0xaaa first seen at 5
  const childAddresses: ChildAddresses = new Map([
    ['f', new Map<Address, number>()],
  ]);
  const d = createDiscovery({
    client: fakeClient(events),
    childAddresses,
    factories: [factory()],
    discoveryWindows: 4,
    stats: createStats(),
  });
  d.setFloor(0);
  await d.ensure(500, { chunkBlocks: 100, endHint: 500 });
  const rec = childAddresses.get('f')!;
  expect(rec.get('0xaaa' as Address)).toBe(5); // earliest, not 400
  expect(rec.get('0xbbb' as Address)).toBe(250);
});

// ── INV-3: discovery-before-data + failure/recovery (fixes G2) ──────────────────────────────────────

test('INV-3/G2: a failed scan rolls the watermark back; a later ensure recovers', async () => {
  const events = [{ child: '0xaaa', bn: 42 }];
  let failing = true;
  const childAddresses: ChildAddresses = new Map([
    ['f', new Map<Address, number>()],
  ]);
  const d = createDiscovery({
    client: fakeClient(events, () => failing),
    childAddresses,
    factories: [factory()],
    discoveryWindows: 2,
    stats: createStats(),
  });
  d.setFloor(0);

  await expect(
    d.ensure(500, { chunkBlocks: 100, endHint: 500 }),
  ).rejects.toThrow(/scan failed/);
  expect(d.through()).toBe(-1); // watermark rolled back to the last good value (never advanced on failure)

  failing = false; // recover
  await d.ensure(500, { chunkBlocks: 100, endHint: 500 });
  expect(d.through()).toBe(500);
  expect(childAddresses.get('f')!.get('0xaaa' as Address)).toBe(42); // discovered on retry
});

test('INV-3: dedup — a second ensure within the watermark returns without re-scanning', async () => {
  let scans = 0;
  const client: PortalClient = {
    finalizedHead: async () => undefined,
    finalizedHeadRetry: async () => undefined,
    // yields one empty batch (counts the scans; discovers nothing)
    async *stream(_q, _from, _to) {
      scans++;
      yield [];
    },
  };
  const d = createDiscovery({
    client,
    childAddresses: new Map([['f', new Map()]]),
    factories: [factory()],
    discoveryWindows: 1,
    stats: createStats(),
  });
  d.setFloor(0);
  await d.ensure(200, { chunkBlocks: 1000, endHint: 200 }); // one window
  const after = scans;
  await d.ensure(100, { chunkBlocks: 1000, endHint: 200 }); // covered → no new scan
  expect(scans).toBe(after);
});

test('no factories → ensure is a no-op', async () => {
  const d = createDiscovery({
    client: fakeClient([]),
    childAddresses: new Map(),
    factories: [],
    discoveryWindows: 4,
    stats: createStats(),
  });
  d.setFloor(0);
  await d.ensure(1000, { chunkBlocks: 100, endHint: 1000 });
  expect(d.through()).toBe(-1);
});

// ── INV-3/G2: the interleaving hole (empirically reproduced in review) ──────────────────────────────
// A successor extension planned WHILE a predecessor scan is in flight only scans [through+1..to]. If
// the predecessor then FAILS, the successor must reject too — were the failure swallowed, the successor
// would confirm coverage over the predecessor's unscanned gap and a child created inside it (block 50
// here) would be permanently invisible: `planDiscovery` would return null forever and the INV-3 runtime
// assert would PASS while data silently lacks the child's logs.

test('INV-3/G2 interleaving: a successor extension rejects when its in-flight predecessor fails — never confirms over the gap', async () => {
  const CHILD_AT = 50; // created inside the FIRST (failing) scan's range
  let release: (() => void) | undefined;
  let failFirst = true;
  const client: PortalClient = {
    finalizedHead: async () => undefined,
    finalizedHeadRetry: async () => undefined,
    async *stream(_q, from, to) {
      if (failFirst && from <= CHILD_AT && to >= CHILD_AT) {
        failFirst = false;
        await new Promise<void>((r) => {
          release = r; // hold this scan open so a successor can be planned meanwhile
        });
        throw new Error('scan failed');
      }
      if (from <= CHILD_AT && to >= CHILD_AT)
        yield [proxy('0xaaa', CHILD_AT)] as any;
    },
  };
  const childAddresses: ChildAddresses = new Map([
    ['f', new Map<Address, number>()],
  ]);
  const d = createDiscovery({
    client,
    childAddresses,
    factories: [factory()],
    discoveryWindows: 1,
    stats: createStats(),
  });
  d.setFloor(0);

  const p1 = d.ensure(100, { chunkBlocks: 1000, endHint: 100 }); // scans [0,100], held open
  const p2 = d.ensure(200, { chunkBlocks: 1000, endHint: 200 }); // extension [101,200], chained on p1
  while (release === undefined) await new Promise((r) => setTimeout(r, 1));
  release();

  await expect(p1).rejects.toThrow('scan failed');
  await expect(p2).rejects.toThrow('scan failed'); // the successor MUST reject — no swallowed gap
  expect(d.through()).toBe(-1); // the watermark never advanced past the hole
  expect(childAddresses.get('f')!.has('0xaaa' as Address)).toBe(false);

  // recovery: a later ensure replans contiguously from the floor and discovers the child
  await d.ensure(200, { chunkBlocks: 1000, endHint: 200 });
  expect(d.through()).toBe(200);
  expect(childAddresses.get('f')!.get('0xaaa' as Address)).toBe(CHILD_AT);
});

// ── reset() generation guard: a scan invalidated by a mid-scan reset never advances the watermark ────
// (issue #9) reset() forgets the floor + watermark (a dense-source grid reset). A scan that was already
// in flight must not, on its late completion, advance `confirmed` past the reset — because a subsequent
// failure would then roll `through` up to that stale watermark, certifying coverage the new grid never
// scanned. Latent today (reset only fires before any scan exists); the generation stamp closes it.

test('reset() during an in-flight scan: the stale completion never advances the confirmed watermark (issue #9)', async () => {
  let release: (() => void) | undefined;
  let failNext = false;
  const client: PortalClient = {
    finalizedHead: async () => undefined,
    finalizedHeadRetry: async () => undefined,
    async *stream(_q, from, to) {
      if (failNext) throw new Error('scan failed');

      // hold the FIRST scan open until the test releases it (so reset() can fire mid-scan)
      if (release === undefined)
        await new Promise<void>((r) => {
          release = r;
        });

      if (from <= 42 && to >= 42) yield [proxy('0xaaa', 42)] as any;
    },
  };
  const childAddresses: ChildAddresses = new Map([
    ['f', new Map<Address, number>()],
  ]);
  const d = createDiscovery({
    client,
    childAddresses,
    factories: [factory()],
    discoveryWindows: 1,
    stats: createStats(),
  });
  d.setFloor(0);

  const p = d.ensure(500, { chunkBlocks: 1000, endHint: 500 }); // scans [0,500], held open
  while (release === undefined) await new Promise((r) => setTimeout(r, 1));

  d.reset(); // a grid reset fires WHILE the scan is in flight
  expect(d.snapshot().through).toBe(-1); // reset cleared the optimistic watermark
  release!(); // the stale scan now completes AFTER the reset
  await p;

  // The exact corruption issue #9 describes: were the stale completion allowed to set `confirmed = 500`,
  // this later FAILING scan would roll `through` up to 500 — certifying [0,500] covered when it was not.
  failNext = true;
  d.setFloor(0);
  await expect(
    d.ensure(300, { chunkBlocks: 1000, endHint: 300 }),
  ).rejects.toThrow('scan failed');
  expect(d.through()).toBe(-1); // fixed: -1 (stale completion ignored); buggy: 500 (rolled up to stale)
});

test('INV-3 property: random ensures + injected window failures → coverage below through() is always complete; retry converges', async () => {
  await fc.assert(
    fc.asyncProperty(
      fc.array(fc.integer({ min: 0, max: 200 }), {
        minLength: 1,
        maxLength: 6,
      }), // ensure targets
      fc.array(
        fc.record({
          child: fc.integer({ min: 1, max: 8 }),
          bn: fc.integer({ min: 0, max: 200 }),
        }),
        { maxLength: 8 },
      ),
      fc.array(fc.boolean(), { minLength: 32, maxLength: 32 }), // per-stream failure schedule
      async (targets, childSpecs, failures) => {
        const events = childSpecs.map(({ child, bn }) => ({
          child: `0x${child.toString(16).padStart(4, '0')}`,
          bn,
        }));
        // the earliest creation block per child — the model the map must converge to (INV-4)
        const earliest = new Map<string, number>();
        for (const e of events) {
          const prev = earliest.get(e.child);
          if (prev === undefined || prev > e.bn) earliest.set(e.child, e.bn);
        }
        let call = 0;
        const client: PortalClient = {
          finalizedHead: async () => undefined,
          finalizedHeadRetry: async () => undefined,
          async *stream(_q, from, to) {
            // schedule-driven failures; once the schedule is exhausted every scan succeeds (convergence)
            const fail = call < failures.length && failures[call] === true;
            call++;
            if (fail) throw new Error('scan failed');
            const batch = events
              .filter((e) => e.bn >= from && e.bn <= to)
              .map((e) => proxy(e.child, e.bn));
            if (batch.length) yield batch as any;
          },
        };
        const childAddresses: ChildAddresses = new Map([
          ['f', new Map<Address, number>()],
        ]);
        const d = createDiscovery({
          client,
          childAddresses,
          factories: [factory()],
          discoveryWindows: 2,
          stats: createStats(),
        });
        d.setFloor(0);

        const rec = childAddresses.get('f')!;
        const checkNoHoleBelowThrough = () => {
          const t = d.through();
          if (t < 0) return;
          for (const [child, bn] of earliest) {
            if (bn <= t) expect(rec.get(child as Address)).toBe(bn); // no child below the watermark missing
          }
        };

        for (const target of targets) {
          await d
            .ensure(target, { chunkBlocks: 50, endHint: target })
            .catch(() => {});
          checkNoHoleBelowThrough();
        }
        // convergence: retry until the max target is covered (the schedule can't fail forever)
        const maxTarget = Math.max(...targets);
        for (let i = 0; i < 40 && d.through() < maxTarget; i++) {
          await d
            .ensure(maxTarget, { chunkBlocks: 50, endHint: maxTarget })
            .catch(() => {});
        }
        expect(d.through()).toBeGreaterThanOrEqual(maxTarget);
        checkNoHoleBelowThrough();
      },
    ),
    { numRuns: 30 },
  );
});
