/**
 * portal.ts — the Portal-backed historical sync: the imperative ORCHESTRATION SHELL.
 *
 * Ponder feeds small intervals; the Portal is latency-bound per request but has huge parallel bandwidth.
 * So we fetch large aligned CHUNKS (portal-chunks) and serve every interval from cache, prefetching ahead
 * (read-ahead depth bounded by a shared memory budget). Factory correctness is preserved by decoupling
 * discovery from data (portal-discovery): a data chunk fetches only once discovery is complete through its
 * range (INV-3). All the domain logic lives in the pure modules; this file only WIRES them and holds the
 * mutable shell state (chunk cache, stash, delegation) with invariant checks at the seam boundaries.
 *
 *   config  → portal-config      client   → portal-client     filters/spec → portal-filters
 *   gate    → portal-gate        chunks   → portal-chunks      discovery    → portal-discovery
 *   assemble→ portal-assemble    metrics  → portal-metrics     transforms   → portal-transform
 *
 * The public seam is FROZEN: `createPortalHistoricalSync({common, chain, rpc, childAddresses,
 * eventCallbacks}) : HistoricalSync`. See portal/INVARIANTS.md for the invariant catalog (INV-1…INV-14).
 */
import type { Common } from "@/internal/common.js";
import type {
  Chain,
  Filter,
  SyncBlockHeader,
  SyncTransaction,
} from "@/internal/types.js";
import type { Rpc } from "@/rpc/index.js";
import type { Interval } from "@/utils/interval.js";
import { type HistoricalSync, createHistoricalSync } from "./index.js";
import { type AssembledRange, type ChunkData, assembleRange, createChunkData } from "./portal-assemble.js";
import { chunkRange, evictionPlan, idxOf, readAheadPlan, scaleChunkBlocks, traceSafeChunkBlocks } from "./portal-chunks.js";
import { createPortalClient } from "./portal-client.js";
import { loadPortalConfig } from "./portal-config.js";
import { createDiscovery } from "./portal-discovery.js";
import { type ChildAddresses, compileFetchSpec } from "./portal-filters.js";
import { sharedGate } from "./portal-gate.js";
import { invariant, setCheckMode } from "./portal-invariant.js";
import { createStats, startGateLog, writeMetrics } from "./portal-metrics.js";
import { isBlockFilterMatched } from "@/runtime/filter.js";
import { isFinalityGap } from "./portal-transform.js";

type CreateHistoricalSyncParameters = {
  common: Common;
  chain: Chain;
  rpc: Rpc;
  childAddresses: ChildAddresses;
  // FULL per-chain filter set (runtime: params.eventCallbacks). The fetch-spec is resolved from THIS,
  // once — never from per-call requiredIntervals — so every idx-keyed chunk is filter-complete (INV-1).
  eventCallbacks: { filter: Filter }[];
};

type StashEntry = Omit<AssembledRange, "logs">;

export const createPortalHistoricalSync = (args: CreateHistoricalSyncParameters): HistoricalSync => {
  const cfg = loadPortalConfig();
  setCheckMode(cfg.checks);
  const log = args.common.logger;
  const chain = args.chain;
  const portalUrl = chain.portal!.replace(/\/$/, "");
  const headers: Record<string, string> = { "content-type": "application/json", "accept-encoding": "gzip", ...(cfg.apiKey ? { "x-api-key": cfg.apiKey } : {}) };

  const stats = createStats();
  const gate = sharedGate(cfg);
  startGateLog(gate, cfg.gateLog);
  const client = createPortalClient({ portalUrl, headers, gate, stats, bufferSize: cfg.bufferSize, chainName: chain.name, logDebug: (msg) => log.debug({ service: "portal", msg }) });
  const spec = compileFetchSpec(args.eventCallbacks ?? [], args.childAddresses);
  const discovery = createDiscovery({ client, childAddresses: args.childAddresses, factories: spec.factories, discoveryWindows: cfg.discoveryWindows, stats });

  // ── mutable shell state ──────────────────────────────────────────────────────────────────────────
  const dataCache = new Map<number, Promise<ChunkData>>(); // idx → chunk (INV-1: filter-complete)
  const chunkSpecId = new Map<number, symbol>(); //          idx → fetch-spec identity (INV-1)
  const chunkRows = new Map<number, number>(); //            idx → registered rows (freed on evict/reject)
  const stash = new Map<string, StashEntry>(); //            interval → block-data, consumed by syncBlockData
  const delegated = new Set<string>(); //                    interval keys routed to RPC (finality gap)
  let chunkBlocks = cfg.chunkBlocks;
  let chunkSizeP: Promise<void> | undefined;
  let discStartIdx: number | undefined; // factory-deploy chunk = discovery floor (C4: clamps DOWNWARD)
  let portalHead: number | undefined = cfg.finalizedHead;
  let startTime = 0;

  const ikey = (i: Interval): string => `${i[0]}-${i[1]}`;
  const dataEnd = (): number => spec.backfillEnd ?? portalHead ?? Number.POSITIVE_INFINITY;

  // finality-gap fallback: Portal serves only finalized data, and its finalized head can (rarely) lag
  // Ponder's target. Any interval reaching past the head is delegated whole to the stock RPC sync.
  let rpcFallbackInstance: HistoricalSync | undefined;
  const rpcFallback = (): HistoricalSync => (rpcFallbackInstance ??= createHistoricalSync(args));
  // stream-realtime mode: the recent region is served by the Portal /stream (portal-realtime-wire), so
  // historical never targets past the head and this RPC finality-gap fallback is neither needed nor wanted.
  const STREAM_REALTIME = Boolean(chain.portal) && cfg.realtime === "stream";

  const refreshPortalHead = async (): Promise<number | undefined> => {
    if (cfg.finalizedHead !== undefined) return (portalHead = cfg.finalizedHead);
    for (let attempt = 0; attempt < 3; attempt++) {
      const h = await client.finalizedHead();
      if (h !== undefined) return (portalHead = h);
      await new Promise((r) => setTimeout(r, 200 * (attempt + 1)));
    }
    return portalHead; // may be a kept-prior value, or undefined if never probed successfully
  };

  // Scale chunk size by the chain's block density (once); also seeds the finality head (C3 dedupe).
  const ensureChunkSize = (): Promise<void> =>
    (chunkSizeP ??= (async () => {
      if (cfg.chunkFixed) return;
      const h = await client.finalizedHead();
      if (h !== undefined) { portalHead = h; chunkBlocks = scaleChunkBlocks(cfg.chunkBlocks, h); log.debug({ service: "portal", msg: `Portal ${chain.name}: head=${h} → chunkBlocks=${chunkBlocks}` }); }
    })());

  // ── one data chunk: gated on discovery-through-this-chunk, then the source streams ──────────────────
  const dataChunk = (idx: number): Promise<ChunkData> => {
    const cached = dataCache.get(idx);
    if (cached) {
      stats.cacheHits++;
      invariant("INV-1", chunkSpecId.get(idx) === spec.id, "cached chunk built under a different fetch-spec", () => ({ idx }));
      return cached;
    }
    const p = (async (): Promise<ChunkData> => {
      const [from, to] = chunkRange(idx, chunkBlocks, spec.backfillStart, dataEnd());
      await discovery.ensure(to, { chunkBlocks, endHint: spec.backfillEnd ?? portalHead ?? to }); // children ≤ this chunk are known
      invariant("INV-3", spec.factories.length === 0 || discStartIdx === undefined || discovery.through() >= to, "data fetch under a stale discovery watermark", () => ({ idx, through: discovery.through(), to }));
      stats.dataChunks++;
      const cd = createChunkData();
      const neededMissing = new Set<string>();
      const onRows = (n: number): void => { chunkRows.set(idx, (chunkRows.get(idx) ?? 0) + n); gate.addRows(n); }; // G3: register as batches arrive

      const lq = spec.logQuery();
      if (lq) for await (const blocks of client.stream(lq, from, to, { neededMissing, onRows })) {
        for (const b of blocks) if (b.logs?.length) {
          cd.headers.set(b.header.number, b.header);
          cd.logs.set(b.header.number, (cd.logs.get(b.header.number) ?? []).concat(b.logs));
          if (b.transactions?.length) cd.txs.set(b.header.number, (cd.txs.get(b.header.number) ?? []).concat(b.transactions));
          stats.logs += b.logs.length;
        }
      }
      const tq = spec.traceQuery();
      if (tq) for await (const blocks of client.stream(tq, from, to, { neededMissing, onRows })) {
        for (const b of blocks) if (b.traces?.length) {
          const ex = cd.traceBlocks.get(b.header.number);
          if (ex) { ex.traces.push(...b.traces); if (b.transactions) ex.txs.push(...b.transactions); }
          else cd.traceBlocks.set(b.header.number, { header: b.header, traces: b.traces, txs: b.transactions ?? [] });
        }
      }
      const bq = spec.blockQuery();
      if (bq) for await (const blocks of client.stream(bq, from, to, { neededMissing, onRows })) {
        for (const b of blocks) { const bn = b.header.number; if (spec.blockFilters.some((f) => isBlockFilterMatched({ filter: f, block: { number: BigInt(bn) } as never }))) cd.blockHeaders.set(bn, b.header); }
      }
      const txq = spec.txQuery();
      if (txq) for await (const blocks of client.stream(txq, from, to, { neededMissing, onRows })) {
        for (const b of blocks) if (b.transactions?.length) {
          const ex = cd.txBlocks.get(b.header.number);
          if (ex) ex.txs.push(...b.transactions);
          else cd.txBlocks.set(b.header.number, { header: b.header, txs: b.transactions });
        }
      }
      // A NEEDED field the dataset lacked on THIS chunk: crash ONLY IF the chunk yielded MATCHED data —
      // an event the indexer processes would be incomplete. Event-less (old/irrelevant) ranges proceed.
      if (neededMissing.size && (cd.logs.size || cd.traceBlocks.size || cd.txBlocks.size || cd.blockHeaders.size)) {
        throw new Error(`Portal dataset for ${chain.name} is missing [${[...neededMissing].join(", ")}] on blocks [${from},${to}], which contain matched data your indexer needs — a Portal dataset-completeness gap. Failing fast rather than serving incomplete data; report the gap to SQD, or start your indexer past the affected range.`);
      }
      return cd;
    })();
    chunkSpecId.set(idx, spec.id);
    dataCache.set(idx, p);
    // G1 (INV-13): a rejected chunk promise is evicted immediately (and its registered rows freed) so a
    // later interval RETRIES rather than replaying the cached rejection forever.
    p.catch(() => {
      if (dataCache.get(idx) === p) { dataCache.delete(idx); chunkSpecId.delete(idx); }
      gate.freeRows(chunkRows.get(idx) ?? 0); chunkRows.delete(idx);
    });
    return p;
  };

  const evictBehind = (intervalStart: number): void => {
    for (const i of evictionPlan(dataCache.keys(), chunkBlocks, intervalStart)) {
      dataCache.delete(i); chunkSpecId.delete(i); gate.freeRows(chunkRows.get(i) ?? 0); chunkRows.delete(i);
    }
  };

  return {
    async syncBlockRangeData(params) {
      const { interval, requiredFactoryIntervals, syncStore } = params;
      if (!startTime) startTime = Date.now();
      // finality gap: an interval past Portal's finalized head (or an unknown head) is delegated whole to
      // the authoritative RPC (INV-9). Re-confirm first (the Portal advances). The head can only be
      // stale-LOW (monotonic upstream), which errs safe.
      if (portalHead === undefined) await refreshPortalHead();
      if (portalHead === undefined || isFinalityGap(interval[1], portalHead)) {
        await refreshPortalHead();
        if (portalHead === undefined || isFinalityGap(interval[1], portalHead)) {
          if (STREAM_REALTIME) { log.warn({ service: "portal", msg: `Portal ${chain.name} [${interval[0]},${interval[1]}] past/unknown finalized head in stream mode → RPC fallback suppressed (realtime /stream covers the gap)` }); return []; }
          delegated.add(ikey(interval)); stats.rpcFallback++;
          log.debug({ service: "portal", msg: `Portal ${chain.name} [${interval[0]},${interval[1]}] ${portalHead === undefined ? "head unknown" : `past finalized head ${portalHead}`} → RPC fallback` });
          return rpcFallback().syncBlockRangeData(params);
        }
      }
      await ensureChunkSize(); // scale chunk to chain block-density before any idxOf()

      // cap the chunk grid for DENSE sources (traces fetch every trace; block sources includeAllBlocks-
      // scan the whole range) — bounds memory + overfetch. A change resets the grid.
      const capped = traceSafeChunkBlocks(chunkBlocks, spec.needTraces || spec.needBlocks, cfg.traceChunkBlocks);
      if (capped !== chunkBlocks) {
        chunkBlocks = capped;
        dataCache.clear(); chunkSpecId.clear();
        for (const r of chunkRows.values()) gate.freeRows(r); chunkRows.clear();
        discStartIdx = undefined; discovery.reset();
        log.debug({ service: "portal", msg: `Portal ${chain.name}: dense sources → chunkBlocks capped to ${chunkBlocks} (grid reset)` });
      }

      // pin the discovery floor at the factory's real start (NOT block 0). C4: clamp DOWNWARD only.
      if (requiredFactoryIntervals.length > 0) {
        const floor = idxOf(Math.min(...requiredFactoryIntervals.map((r) => r.interval[0]).concat(interval[0])), chunkBlocks);
        discStartIdx = discStartIdx === undefined ? floor : Math.min(discStartIdx, floor);
        discovery.setFloor(discStartIdx * chunkBlocks);
      }

      const startIdx = idxOf(interval[0], chunkBlocks), endIdx = idxOf(interval[1], chunkBlocks);
      const idxs: number[] = [];
      for (let i = startIdx; i <= endIdx; i++) idxs.push(i);
      const data = await Promise.all(idxs.map(dataChunk));

      // PARALLEL read-ahead: prefetch ahead (never past the backfill end) — depth bounded by the shared
      // memory budget, not a fixed count (always lead-1; deeper only while unsaturated).
      for (const d of readAheadPlan(endIdx, chunkBlocks, dataEnd(), cfg.readahead, gate.saturated())) void dataChunk(d).catch(() => {});

      const tXform = Date.now(); // decode/transform time: Portal NDJSON → Ponder Sync* shapes
      const assembled = assembleRange(data, interval, spec, args.childAddresses);
      stats.transformMs += Date.now() - tXform;

      evictBehind(interval[0]); // free chunks fully behind the cursor + their memory budget

      await syncStore.insertLogs({ logs: assembled.logs, chainId: chain.id });
      invariant("INV-12", !stash.has(ikey(interval)) && !delegated.has(ikey(interval)), "stash entry created twice / for a delegated interval", () => ({ key: ikey(interval) }));
      stash.set(ikey(interval), { blocks: assembled.blocks, txs: assembled.txs, receipts: assembled.receipts, traces: assembled.traces, closest: assembled.closest });

      log.debug({ service: "portal", msg: `Portal ${chain.name} [${interval[0]},${interval[1]}]: ${assembled.logs.length} logs (dataChunks=${stats.dataChunks} discChunks=${stats.discChunks} http=${stats.http} hits=${stats.cacheHits} inflight=${stats.maxInflight} err=${stats.errors})` });
      return assembled.logs;
    },

    async syncBlockData(params) {
      const { interval, syncStore } = params;
      const key = ikey(interval);
      if (delegated.has(key)) { delegated.delete(key); return rpcFallback().syncBlockData(params); }
      const s = stash.get(key);
      stash.delete(key); // INV-12: consumed exactly once
      if (!s) return undefined;
      const chainId = chain.id;
      // merge log blocks/txs with trace blocks/txs (a trace-only block isn't in the log set)
      const blocks = new Map<string, SyncBlockHeader>();
      for (const b of s.blocks) blocks.set(b.number as unknown as string, b);
      const txs = new Map<string, SyncTransaction>();
      for (const t of s.txs) txs.set(t.hash as unknown as string, t);
      for (const { block, transaction } of s.traces) {
        blocks.set((block as { number: string }).number, block as unknown as SyncBlockHeader);
        const th = (transaction as { hash?: string }).hash;
        if (th) txs.set(th, transaction as unknown as SyncTransaction);
      }
      const blockArr = [...blocks.values()];
      if (blockArr.length === 0) return s.closest;
      await syncStore.insertBlocks({ blocks: blockArr, chainId });
      if (txs.size) await syncStore.insertTransactions({ transactions: [...txs.values()], chainId });
      if (s.receipts.length) await syncStore.insertTransactionReceipts({ transactionReceipts: s.receipts, chainId });
      if (s.traces.length) await syncStore.insertTraces({ traces: s.traces, chainId });
      stats.blocks += blockArr.length; stats.txs += txs.size; stats.receipts += s.receipts.length; stats.traces += s.traces.length;
      writeMetrics({ metricsFile: cfg.metricsFile, chain, stats, chunkBlocks, portalHead, gate, startTime });
      return s.closest;
    },
  };
};
