/**
 * portal-discovery.ts — the factory child-discovery state machine (pure core + a runner shell).
 *
 * Correctness for factory sources rests on decoupling the DISCOVERY timeline from the DATA timeline: a
 * data chunk may only fetch once factory discovery is complete THROUGH its last block, so no child event
 * is missed even though data chunks are fetched out of order (INV-3). Discovery over [factoryStart, head]
 * is split into disjoint windows fetched CONCURRENTLY (the Portal serializes one stream in block order,
 * so parallelism comes from issuing disjoint requests). Children are min-merged to their EARLIEST
 * creation block (INV-4).
 *
 * The watermark `through` advances ONLY when a scan SUCCEEDS; a failed scan rolls it back to the last
 * good value and the rejected promise is replaced, so a later interval retries instead of replaying a
 * cached rejection (INV-3 / fixes G2). The window math (`splitWindows`, `planDiscovery`) is pure.
 */

import type { Address } from 'viem';
import type { Factory, SyncLog } from '@/internal/types.js';
import { getChildAddress, isLogFactoryMatched } from '@/runtime/filter.js';
import type { PortalClient } from './portal-client.js';
import type { ChildAddresses } from './portal-filters.js';
import type { PortalStats } from './portal-metrics.js';
import { hx } from './portal-transform.js';

export type DiscoveryStatus = 'idle' | 'scanning' | 'failed';
export type DiscoveryState = Readonly<{
  floor: number;
  through: number;
  status: DiscoveryStatus;
}>;

/** Split [from, to] into ≤ `discoveryWindows` disjoint windows sized by `chunkBlocks`. Pure. */
export function splitWindows(
  from: number,
  to: number,
  chunkBlocks: number,
  discoveryWindows: number,
): [number, number][] {
  const span = to - from + 1;
  const P = Math.max(
    1,
    Math.min(discoveryWindows, Math.ceil(span / chunkBlocks)),
  );
  const w = Math.ceil(span / P);
  const windows: [number, number][] = [];
  for (let i = 0; i < P; i++) {
    const lo = from + i * w;
    if (lo > to) break;
    windows.push([lo, Math.min(to, lo + w - 1)]);
  }
  return windows;
}

export type DiscoveryPlanOpts = {
  chunkBlocks: number;
  endHint: number;
  discoveryWindows: number;
};

/**
 * The next extension to scan to reach `needTo`, or null when already covered / no floor. Pure. `from`
 * continues past the current watermark; `to` reaches as far as the backfill will need (`endHint` —
 * usually the whole span at once) so discovery typically completes in one pass.
 */
export function planDiscovery(
  state: DiscoveryState,
  needTo: number,
  opts: DiscoveryPlanOpts,
): { from: number; to: number; windows: [number, number][] } | null {
  if (state.floor < 0) return null;
  if (needTo <= state.through) return null;
  const from = state.through < 0 ? state.floor : state.through + 1;
  const to = Math.max(needTo, opts.endHint);
  return {
    from,
    to,
    windows: splitWindows(from, to, opts.chunkBlocks, opts.discoveryWindows),
  };
}

/** Children flushed per interval: factory object (identity preserved for the store) → child → creation block. */
export type PendingFlush = [Factory, Map<Address, number>][];

export interface Discovery {
  /** Set the discovery floor (factory-start block). The shell clamps DOWNWARD (C4) before calling. */
  setFloor(floorBlock: number): void;
  /** Grid reset: forget floor + watermark (dense-source chunk cap). Pending children stay queued. */
  reset(): void;
  /** Ensure discovery is complete through `needTo` (awaited before a data fetch). */
  ensure(
    needTo: number,
    opts: { chunkBlocks: number; endHint: number },
  ): Promise<void>;
  /** The current confirmed/optimistic watermark (for the INV-3 post-await assert). */
  through(): number;
  /**
   * INV-15: drain the newly-discovered children whose creation block ∈ [lo, hi] — the caller persists
   * them via syncStore.insertChildAddresses inside the SAME syncBlockRangeData whose transaction marks
   * this interval's factory intervals cached. Children outside the range stay queued for their owning
   * interval (a whole-queue flush could commit another interval's children in this interval's
   * transaction and lose them if it rolls back after the lower interval committed as cached).
   */
  takePendingInRange(lo: number, hi: number): PendingFlush;
  /** Restore a failed flush (min-merge) so nothing is silently dropped — the interval fails loud. */
  restorePending(flush: PendingFlush): void;
  snapshot(): DiscoveryState;
}

export type DiscoveryDeps = {
  client: PortalClient;
  childAddresses: ChildAddresses;
  factories: readonly Factory[];
  discoveryWindows: number;
  stats: PortalStats;
};

export function createDiscovery(deps: DiscoveryDeps): Discovery {
  const { client, childAddresses, factories, discoveryWindows, stats } = deps;
  let floor = -1;
  let through = -1; // optimistic watermark (dedup + `from`)
  let confirmed = -1; // last SUCCESSFUL watermark (rollback target)
  let status: DiscoveryStatus = 'idle';
  let inflight: Promise<void> = Promise.resolve();
  // Bumped by reset(). Each scan captures the generation at plan time; a scan that resolves OR rejects
  // after a reset changed the generation must NOT touch the watermark. Otherwise a stale success advances
  // `confirmed` past the reset, and a later failure rolls `through` up to that stale watermark — certifying
  // coverage over a range the post-reset grid never scanned (issue #9). Latent today (reset fires only
  // before any scan exists), guarded cheaply so a future caller that resets mid-scan stays correct.
  let generation = 0;
  // INV-15: children discovered by the wide scan but NOT yet persisted. Ponder's core marks
  // requiredFactoryIntervals cached after EVERY interval and on startup loads children ONLY from the
  // store (no re-derivation from logs) — so a factory interval marked cached MUST have its children in
  // the store. Children pre-loaded from the store (restart) re-discover at prevBn ≤ bn and are NOT
  // re-queued, so nothing is double-inserted. Drained per interval by the shell (takePendingInRange).
  const pendingChildren = new Map<Factory, Map<Address, number>>();

  // One window's scan: every factory, min-merging children to their earliest creation block (INV-4).
  const scanWindow = async (lo: number, hi: number): Promise<void> => {
    for (const factory of factories) {
      const needsData = factory.childAddressLocation.startsWith('offset');
      const address = factory.address
        ? (Array.isArray(factory.address)
            ? factory.address
            : [factory.address]
          ).map((a) => a.toLowerCase())
        : undefined;
      const q = {
        type: 'evm' as const,
        fields: {
          block: { number: true },
          log: { address: true, topics: true, data: needsData },
        },
        logs: [{ address, topic0: [factory.eventSelector.toLowerCase()] }],
      };
      const rec = childAddresses.get(factory.id)!;
      for await (const blocks of client.stream(q, lo, hi)) {
        for (const bl of blocks)
          for (const raw of bl.logs ?? []) {
            const sl = {
              address: (raw.address as string)?.toLowerCase(),
              topics: raw.topics ?? [],
              data: raw.data ?? '0x',
              blockNumber: hx(bl.header.number),
            } as unknown as SyncLog;
            if (isLogFactoryMatched({ factory, log: sl })) {
              const child = getChildAddress({
                log: sl,
                factory,
              }).toLowerCase() as Address;
              const bn = bl.header.number as number;
              const prev = rec.get(child);
              if (prev === undefined || prev > bn) {
                rec.set(child, bn);
                // queue for persistence (INV-15). Store-preloaded children have prev ≤ bn → not re-queued.
                let pend = pendingChildren.get(factory);
                if (pend === undefined) {
                  pend = new Map();
                  pendingChildren.set(factory, pend);
                }
                pend.set(child, bn);
              }
            }
          }
      }
    }
  };

  const takePendingInRange = (lo: number, hi: number): PendingFlush => {
    const flush: PendingFlush = [];
    for (const [factory, children] of pendingChildren) {
      let inRange: Map<Address, number> | undefined;
      for (const [child, block] of children) {
        if (block >= lo && block <= hi) {
          if (inRange === undefined) inRange = new Map();
          inRange.set(child, block);
        }
      }
      if (inRange === undefined) continue;

      flush.push([factory, inRange]);
      for (const child of inRange.keys()) children.delete(child);
      if (children.size === 0) pendingChildren.delete(factory);
    }

    return flush;
  };

  const restorePending = (flush: PendingFlush): void => {
    for (const [factory, children] of flush) {
      let pend = pendingChildren.get(factory);
      if (pend === undefined) {
        pend = new Map();
        pendingChildren.set(factory, pend);
      }
      for (const [child, block] of children) {
        const prev = pend.get(child);
        if (prev === undefined || prev > block) pend.set(child, block);
      }
    }
  };

  const ensure = (
    needTo: number,
    opts: { chunkBlocks: number; endHint: number },
  ): Promise<void> => {
    if (factories.length === 0) return Promise.resolve();
    const plan = planDiscovery({ floor, through, status }, needTo, {
      chunkBlocks: opts.chunkBlocks,
      endHint: opts.endHint,
      discoveryWindows,
    });
    if (plan === null) return inflight; // no floor yet OR already covered
    const { to, windows } = plan;
    const earlier = inflight;
    const gen = generation; // stamp: a reset() after this point invalidates this scan
    through = to; // optimistic advance so concurrent ensures dedup onto one scan
    status = 'scanning';
    const p = (async () => {
      // G2/INV-3: a predecessor extension's failure MUST propagate. This extension only scans
      // [through+1..to] on the assumption the predecessor covered everything below it; swallowing the
      // failure and then confirming `to` would certify coverage over the predecessor's unscanned gap —
      // permanently losing any child created inside it. By rethrowing, this extension rejects BEFORE
      // scanning, its catch below rolls `through` back to `confirmed`, and the awaiting data chunks
      // reject (G1-evicted) → a later interval replans contiguously from confirmed+1 / the floor.
      await earlier;
      stats.discChunks += windows.length;
      await Promise.all(windows.map(([lo, hi]) => scanWindow(lo, hi)));
      if (gen !== generation) return; // a reset() invalidated this scan — never confirm over it (issue #9)

      confirmed = to; // INV-3: advance the confirmed watermark ONLY on success (never past a gap)
      status = 'idle';
    })();
    inflight = p;
    // On failure roll the optimistic watermark back to the last good one and drop the rejected promise so
    // a later ensure re-plans (the awaiting data chunk still sees `p` reject and retries).
    p.catch(() => {
      if (gen !== generation) return; // invalidated by reset() — leave the post-reset state untouched (issue #9)

      through = confirmed;
      status = 'failed';
      if (inflight === p) inflight = Promise.resolve();
    });
    return p;
  };

  return {
    setFloor: (floorBlock: number) => {
      floor = floorBlock;
    },
    // pendingChildren survives a grid reset: re-discovery re-finds known children at prev === bn (not
    // re-queued), and still-unflushed children flush with their owning interval.
    reset: () => {
      floor = -1;
      through = -1;
      confirmed = -1;
      status = 'idle';
      inflight = Promise.resolve();
      generation++; // invalidate any in-flight scan stamped with the previous generation (issue #9)
    },
    ensure,
    through: () => through,
    takePendingInRange,
    restorePending,
    snapshot: () => ({ floor, through, status }),
  };
}
