/**
 * portal-chunks.ts — chunk-grid math (pure).
 *
 * Ponder feeds small intervals; the Portal is latency-bound per request but has huge parallel
 * bandwidth. So the historical sync fetches large aligned CHUNKS (indexed by `idx = ⌊block /
 * chunkBlocks⌋`) and serves every interval from cache, prefetching ahead. This module owns all
 * the grid arithmetic as pure functions returning PLANS — the imperative shell (portal.ts) then
 * executes them. Keeping it pure makes the tiling/coverage guarantees (INV-2 support, INV-13)
 * unit-testable in isolation.
 */
import type { Interval } from '@/utils/interval.js';

/** The chunk index a block falls in. */
export const idxOf = (blockNumber: number, chunkBlocks: number): number =>
  Math.floor(blockNumber / chunkBlocks);

/**
 * A chunk's grid-aligned [from, to] clamped to the real backfill window — `backfillStart` on the
 * low side, `end` (explicit toBlock, else the finalized head, else +∞) on the high side. Bounds
 * fetch on BOTH sides so a small/bounded range isn't widened to the full grid.
 */
export const chunkRange = (
  idx: number,
  chunkBlocks: number,
  backfillStart: number,
  end: number,
): [number, number] => [
  Math.max(idx * chunkBlocks, backfillStart),
  Math.min(idx * chunkBlocks + chunkBlocks - 1, end),
];

/**
 * #50: the quantum-bounded fetch window. Always covers `need` when given; low edge trimmed to
 * the requesting interval (the durable frontier); span is at least min(quantum, available).
 */
export const fetchBounds = (
  gridFrom: number,
  desiredTo: number,
  need: Interval | undefined,
  quantum: number,
): [number, number] => {
  const from = need === undefined ? gridFrom : Math.max(gridFrom, need[0]);
  const to = Math.min(
    desiredTo,
    Math.max(need === undefined ? 0 : need[1], from + quantum - 1),
  );

  return [from, to];
};

/**
 * Scale the base chunk width by the chain's block density. High-block-rate chains (Arbitrum
 * ~478M blocks ≈ 19× Ethereum) otherwise need 19× more latency-bound round-trips; CU is charged
 * per Portal data-chunk (data-density based), so larger BLOCK-chunks don't cost more CU — they
 * just cut round-trips. Capped at 25M blocks.
 */
export const scaleChunkBlocks = (base: number, head: number): number => {
  const density = Math.max(1, Math.round(head / 25_000_000));
  return Math.min(base * density, 25_000_000);
};

/**
 * Traces are ~100× denser than logs; buffering a wide chunk's worth over a busy contract OOMs.
 * For trace-index parity we fetch the FULL (unfiltered) trace set, denser still, so the default
 * cap is conservative. When a chain has trace (or includeAllBlocks) sources, cap the chunk to a
 * trace-safe width (`PORTAL_TRACE_CHUNK_BLOCKS`, default 2k).
 *
 * The cap always comes from the caller (`cfg.traceChunkBlocks` — env is parsed ONCE into the
 * validated PortalConfig, INV-14); the old env-reading default parameter here was dead in
 * production and reintroduced the exact silent-NaN shape INV-14 was written to kill.
 */
export const traceSafeChunkBlocks = (
  base: number,
  needTraces: boolean,
  cap: number,
): number => (needTraces && base > cap ? cap : base);

/**
 * Read-ahead plan: which chunk idxs to prefetch after serving up to `endIdx`. Always prefetch
 * lead-1 (so this chain's next chunk is ready and indexing never awaits a fetch), and go deeper
 * (up to `readahead`) only while the shared row buffer isn't `saturated`. Never past `raEnd`
 * (bounded toBlock or finalized head) so the tail doesn't waste CU / hit 204s (INV-13).
 */
export const readAheadPlan = (
  endIdx: number,
  chunkBlocks: number,
  raEnd: number,
  readahead: number,
  saturated: boolean,
): number[] => {
  const plan: number[] = [];
  for (let d = 1; d <= readahead; d++) {
    if ((endIdx + d) * chunkBlocks > raEnd) break;
    if (d > 1 && saturated) break;
    plan.push(endIdx + d);
  }
  return plan;
};

/** Eviction plan: cached chunk idxs whose whole span lies strictly behind `intervalStart`. */
export const evictionPlan = (
  cachedIdxs: Iterable<number>,
  chunkBlocks: number,
  intervalStart: number,
): number[] => {
  const out: number[] = [];
  for (const idx of cachedIdxs)
    if ((idx + 1) * chunkBlocks <= intervalStart) out.push(idx);
  return out;
};
