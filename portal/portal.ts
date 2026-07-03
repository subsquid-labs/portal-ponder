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
import type { Common } from '@/internal/common.js';
import type {
  Chain,
  Filter,
  SyncBlockHeader,
  SyncTransaction,
} from '@/internal/types.js';
import type { Rpc } from '@/rpc/index.js';
import { isBlockFilterMatched } from '@/runtime/filter.js';
import type { Interval } from '@/utils/interval.js';
import { createHistoricalSync, type HistoricalSync } from './index.js';
import {
  type AssembledRange,
  assembleRange,
  type ChunkData,
  createChunkData,
} from './portal-assemble.js';
import {
  chunkRange,
  evictionPlan,
  idxOf,
  readAheadPlan,
  scaleChunkBlocks,
  traceSafeChunkBlocks,
} from './portal-chunks.js';
import { createPortalClient } from './portal-client.js';
import { loadPortalConfig } from './portal-config.js';
import { createDiscovery } from './portal-discovery.js';
import { type ChildAddresses, compileFetchSpec } from './portal-filters.js';
import { sharedGate } from './portal-gate.js';
import {
  invariant,
  invariantStrict,
  setCheckMode,
} from './portal-invariant.js';
import { createStats, startGateLog, writeMetrics } from './portal-metrics.js';
import { isFinalityGap } from './portal-transform.js';

type CreateHistoricalSyncParameters = {
  common: Common;
  chain: Chain;
  rpc: Rpc;
  childAddresses: ChildAddresses;
  // FULL per-chain filter set (runtime: params.eventCallbacks). The fetch-spec is resolved from THIS,
  // once — never from per-call requiredIntervals — so every idx-keyed chunk is filter-complete (INV-1).
  eventCallbacks: { filter: Filter }[];
};

type StashEntry = Omit<AssembledRange, 'logs'>;

export const createPortalHistoricalSync = (
  args: CreateHistoricalSyncParameters,
): HistoricalSync => {
  const cfg = loadPortalConfig();
  setCheckMode(cfg.checks);
  const log = args.common.logger;
  const chain = args.chain;
  const portalUrl = chain.portal!.replace(/\/$/, '');
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    'accept-encoding': 'gzip',
    ...(cfg.apiKey ? { 'x-api-key': cfg.apiKey } : {}),
  };

  const stats = createStats();
  const gate = sharedGate(cfg);
  startGateLog(gate, cfg.gateLog);
  const client = createPortalClient({
    portalUrl,
    headers,
    gate,
    stats,
    bufferSize: cfg.bufferSize,
    chainName: chain.name,
    logDebug: (msg) => log.debug({ service: 'portal', msg }),
  });
  const spec = compileFetchSpec(args.eventCallbacks ?? [], args.childAddresses);
  const discovery = createDiscovery({
    client,
    childAddresses: args.childAddresses,
    factories: spec.factories,
    discoveryWindows: cfg.discoveryWindows,
    stats,
  });

  // ── mutable shell state ──────────────────────────────────────────────────────────────────────────
  // One cache entry per chunk idx. `token` keys the row accounting to THIS fetch (not the idx): a stale
  // in-flight fetch evicted and replaced at the same idx can neither register rows into nor free rows
  // from the replacement's budget — its own token is freed exactly once (idempotent). `coveredTo` is the
  // upper bound this fetch actually covered (head-clamped at fetch time), revalidated on every hit
  // (INV-13: a chunk truncated at a then-lower finalized head is refetched, never served stale).
  type RowToken = { rows: number; freed: boolean };
  type CacheEntry = {
    promise: Promise<ChunkData>;
    specId: symbol;
    coveredTo: number;
    token: RowToken;
  };
  const dataCache = new Map<number, CacheEntry>();
  const freeToken = (t: RowToken): void => {
    if (!t.freed) {
      t.freed = true;
      gate.freeRows(t.rows);
    }
  };
  const stash = new Map<string, StashEntry>(); // interval → block-data, consumed by syncBlockData
  const delegated = new Set<string>(); //        interval keys routed to RPC (finality gap)
  let chunkBlocks = cfg.chunkBlocks;
  let chunkSizeP: Promise<void> | undefined;
  let discStartIdx: number | undefined; // factory-deploy chunk = discovery floor (C4: clamps DOWNWARD)
  let portalHead: number | undefined = cfg.finalizedHead;
  let startTime = 0;

  const ikey = (i: Interval): string => `${i[0]}-${i[1]}`;
  const dataEnd = (): number =>
    spec.backfillEnd ?? portalHead ?? Number.POSITIVE_INFINITY;

  // finality-gap fallback: Portal serves only finalized data, and its finalized head can (rarely) lag
  // Ponder's target. Any interval reaching past the head is delegated whole to the stock RPC sync.
  let rpcFallbackInstance: HistoricalSync | undefined;
  const rpcFallback = (): HistoricalSync => {
    rpcFallbackInstance ??= createHistoricalSync(args);
    return rpcFallbackInstance;
  };
  // stream-realtime mode: the recent region is served by the Portal /stream (portal-realtime-wire), so
  // historical never targets past the head and this RPC finality-gap fallback is neither needed nor wanted.
  const STREAM_REALTIME = Boolean(chain.portal) && cfg.realtime === 'stream';

  const refreshPortalHead = async (): Promise<number | undefined> => {
    if (cfg.finalizedHead !== undefined) {
      portalHead = cfg.finalizedHead;
      return portalHead;
    }
    const h = await client.finalizedHeadRetry(3); // retry lives in the client (injectable sleep)
    if (h !== undefined) portalHead = h;

    return portalHead; // may be a kept-prior value, or undefined if never probed successfully
  };

  // Scale chunk size by the chain's block density (once); also seeds the finality head (C3 dedupe).
  const ensureChunkSize = (): Promise<void> => {
    chunkSizeP ??= (async () => {
      if (cfg.chunkFixed) return;

      const h = await client.finalizedHead();
      if (h !== undefined) {
        portalHead = h;
        chunkBlocks = scaleChunkBlocks(cfg.chunkBlocks, h);
        log.debug({
          service: 'portal',
          msg: `Portal ${chain.name}: head=${h} → chunkBlocks=${chunkBlocks}`,
        });
      }
    })();
    return chunkSizeP;
  };

  // Stream this chunk's four source shapes over [from, to] and MERGE into `cd`. Append-only: the
  // extend path calls this AGAIN for a disjoint tail (coveredTo, desiredTo], so it must add to — never
  // reset — the maps. Each call raises its own crash on a needed-but-missing field within ITS range.
  // Rows register against `token` per arriving batch (G3); the token guard means a stale stream that
  // outlives its eviction cannot register orphaned rows (S1: accounting is per-fetch, not per-idx).
  const runStreams = async (
    cd: ChunkData,
    from: number,
    to: number,
    token: RowToken,
  ): Promise<void> => {
    const neededMissing = new Set<string>();
    const onRows = (n: number): void => {
      if (token.freed) return;

      token.rows += n;
      gate.addRows(n);
    };

    const lq = spec.logQuery();
    if (lq)
      for await (const blocks of client.stream(lq, from, to, {
        neededMissing,
        onRows,
      })) {
        for (const b of blocks)
          if (b.logs?.length) {
            cd.headers.set(b.header.number, b.header);
            cd.logs.set(
              b.header.number,
              (cd.logs.get(b.header.number) ?? []).concat(b.logs),
            );
            if (b.transactions?.length)
              cd.txs.set(
                b.header.number,
                (cd.txs.get(b.header.number) ?? []).concat(b.transactions),
              );
            stats.logs += b.logs.length;
          }
      }
    const tq = spec.traceQuery();
    if (tq)
      for await (const blocks of client.stream(tq, from, to, {
        neededMissing,
        onRows,
      })) {
        for (const b of blocks)
          if (b.traces?.length) {
            const ex = cd.traceBlocks.get(b.header.number);
            if (ex) {
              ex.traces.push(...b.traces);
              if (b.transactions) ex.txs.push(...b.transactions);
            } else
              cd.traceBlocks.set(b.header.number, {
                header: b.header,
                traces: b.traces,
                txs: b.transactions ?? [],
              });
          }
      }
    const bq = spec.blockQuery();
    if (bq)
      for await (const blocks of client.stream(bq, from, to, {
        neededMissing,
        onRows,
      })) {
        for (const b of blocks) {
          const bn = b.header.number;
          if (
            spec.blockFilters.some((f) =>
              isBlockFilterMatched({
                filter: f,
                block: { number: BigInt(bn) } as never,
              }),
            )
          )
            cd.blockHeaders.set(bn, b.header);
        }
      }
    const txq = spec.txQuery();
    if (txq)
      for await (const blocks of client.stream(txq, from, to, {
        neededMissing,
        onRows,
      })) {
        for (const b of blocks)
          if (b.transactions?.length) {
            const ex = cd.txBlocks.get(b.header.number);
            if (ex) ex.txs.push(...b.transactions);
            else
              cd.txBlocks.set(b.header.number, {
                header: b.header,
                txs: b.transactions,
              });
          }
      }
    // A NEEDED field the dataset lacked on THIS range: crash ONLY IF the chunk yielded MATCHED data —
    // an event the indexer processes would be incomplete. Event-less (old/irrelevant) ranges proceed.
    if (
      neededMissing.size &&
      (cd.logs.size ||
        cd.traceBlocks.size ||
        cd.txBlocks.size ||
        cd.blockHeaders.size)
    ) {
      throw new Error(
        `Portal dataset for ${chain.name} is missing [${[...neededMissing].join(', ')}] on blocks [${from},${to}], which contain matched data your indexer needs — a Portal dataset-completeness gap. Failing fast rather than serving incomplete data; report the gap to SQD, or start your indexer past the affected range.`,
      );
    }
  };

  // ── one data chunk: gated on discovery-through-this-chunk, then the source streams ──────────────────
  const dataChunk = (idx: number): Promise<ChunkData> => {
    // Desired coverage RIGHT NOW: grid end clamped to the live backfill end / Portal head. For a fully
    // finalized (or bounded-backfill) chunk this equals the grid end and never grows; for the FRONTIER
    // chunk it grows as the Portal head advances — which drives the extend path below (INV-13).
    const [gridFrom, desiredTo] = chunkRange(
      idx,
      chunkBlocks,
      spec.backfillStart,
      dataEnd(),
    );
    // INV-9: a Portal data request never targets past the known finalized head (an explicit backfill
    // toBlock may exceed it by configuration — the delegation branch already guards served intervals).
    invariant(
      'INV-9',
      portalHead === undefined ||
        desiredTo <= portalHead ||
        spec.backfillEnd !== undefined,
      'data request targets past the Portal finalized head',
      () => ({ idx, desiredTo, portalHead }),
    );
    const ensureOpts = {
      chunkBlocks,
      endHint: spec.backfillEnd ?? portalHead ?? desiredTo,
    };

    const cached = dataCache.get(idx);
    if (cached) {
      invariant(
        'INV-1',
        cached.specId === spec.id,
        'cached chunk built under a different fetch-spec',
        () => ({ idx }),
      );
      if (desiredTo <= cached.coveredTo) {
        stats.cacheHits++;
        return cached.promise;
      }
      // Head-boundary staleness (INV-13, bug found relative to main and fixed by open PR #5 — same fix,
      // this architecture): the FRONTIER chunk was fetched TRUNCATED at the then-finalized head; the
      // head has since advanced. Serving the stale cache would mark the interval synced over a silent
      // gap — EXTEND instead: stream ONLY the newly finalized tail (coveredTo, desiredTo] and merge.
      const extendFrom = cached.coveredTo + 1;
      cached.coveredTo = desiredTo; // optimistic high-water so concurrent callers don't double-extend
      const prev = cached.promise;
      const extended = (async (): Promise<ChunkData> => {
        const cd = await prev;
        await discovery.ensure(desiredTo, ensureOpts); // discovery must reach the extended tail too
        invariant(
          'INV-3',
          spec.factories.length === 0 ||
            discStartIdx === undefined ||
            discovery.through() >= desiredTo,
          'chunk extend under a stale discovery watermark',
          () => ({ idx, through: discovery.through(), desiredTo }),
        );
        await runStreams(cd, extendFrom, desiredTo, cached.token);

        return cd;
      })();
      cached.promise = extended;
      // A FAILED extend must not leave the optimistic high-water in place (the G2 lesson). Evicting the
      // whole entry is the strongest rollback and matches G1's contract: the awaiting caller sees the
      // rejection, and a later interval refetches the chunk fresh under the current head.
      extended.catch(() => {
        if (dataCache.get(idx) === cached) dataCache.delete(idx);
        freeToken(cached.token);
      });
      return extended;
    }

    const token: RowToken = { rows: 0, freed: false };
    const p = (async (): Promise<ChunkData> => {
      await discovery.ensure(desiredTo, ensureOpts); // children ≤ this chunk are known (INV-3)
      invariant(
        'INV-3',
        spec.factories.length === 0 ||
          discStartIdx === undefined ||
          discovery.through() >= desiredTo,
        'data fetch under a stale discovery watermark',
        () => ({ idx, through: discovery.through(), desiredTo }),
      );
      stats.dataChunks++;
      const cd = createChunkData();
      await runStreams(cd, gridFrom, desiredTo, token);

      return cd;
    })();
    const entry: CacheEntry = {
      promise: p,
      specId: spec.id,
      coveredTo: desiredTo,
      token,
    };
    dataCache.set(idx, entry);
    // G1 (INV-13): a rejected chunk promise is evicted immediately (and its registered rows freed — via
    // ITS token, exactly once) so a later interval RETRIES rather than replaying the cached rejection.
    p.catch(() => {
      if (dataCache.get(idx) === entry) dataCache.delete(idx);
      freeToken(token);
    });
    return p;
  };

  const evictBehind = (intervalStart: number): void => {
    for (const i of evictionPlan(
      dataCache.keys(),
      chunkBlocks,
      intervalStart,
    )) {
      const entry = dataCache.get(i)!;
      dataCache.delete(i);
      freeToken(entry.token);
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
        if (
          portalHead === undefined ||
          isFinalityGap(interval[1], portalHead)
        ) {
          if (STREAM_REALTIME) {
            log.warn({
              service: 'portal',
              msg: `Portal ${chain.name} [${interval[0]},${interval[1]}] past/unknown finalized head in stream mode → RPC fallback suppressed (realtime /stream covers the gap)`,
            });
            return [];
          }
          delegated.add(ikey(interval));
          stats.rpcFallback++;
          log.debug({
            service: 'portal',
            msg: `Portal ${chain.name} [${interval[0]},${interval[1]}] ${portalHead === undefined ? 'head unknown' : `past finalized head ${portalHead}`} → RPC fallback`,
          });
          return rpcFallback().syncBlockRangeData(params);
        }
      }
      await ensureChunkSize(); // scale chunk to chain block-density before any idxOf()

      // cap the chunk grid for DENSE sources (traces fetch every trace; block sources includeAllBlocks-
      // scan the whole range) — bounds memory + overfetch. A change resets the grid.
      const capped = traceSafeChunkBlocks(
        chunkBlocks,
        spec.needTraces || spec.needBlocks,
        cfg.traceChunkBlocks,
      );
      if (capped !== chunkBlocks) {
        chunkBlocks = capped;
        for (const entry of dataCache.values()) freeToken(entry.token);
        dataCache.clear();
        discStartIdx = undefined;
        discovery.reset();
        log.debug({
          service: 'portal',
          msg: `Portal ${chain.name}: dense sources → chunkBlocks capped to ${chunkBlocks} (grid reset)`,
        });
      }

      // pin the discovery floor at the factory's real start (NOT block 0). C4: clamp DOWNWARD only.
      if (requiredFactoryIntervals.length > 0) {
        const floor = idxOf(
          Math.min(
            ...requiredFactoryIntervals
              .map((r) => r.interval[0])
              .concat(interval[0]),
          ),
          chunkBlocks,
        );
        discStartIdx =
          discStartIdx === undefined ? floor : Math.min(discStartIdx, floor);
        discovery.setFloor(discStartIdx * chunkBlocks);
      }

      const startIdx = idxOf(interval[0], chunkBlocks);
      const endIdx = idxOf(interval[1], chunkBlocks);
      const idxs: number[] = [];
      for (let i = startIdx; i <= endIdx; i++) idxs.push(i);
      const data = await Promise.all(idxs.map(dataChunk));

      // INV-15: persist the children discovered within THIS interval's range NOW — dataChunk awaited
      // discovery through this interval, and this call's syncStore is the transaction ponder's core
      // commits together with insertIntervals (which marks THIS interval's factory intervals cached).
      // Interval-scoped on purpose (see Discovery.takePendingInRange); a failed flush restores the
      // queue and fails the interval loud (core rolls back — nothing is silently dropped).
      const flush = discovery.takePendingInRange(interval[0], interval[1]);
      if (flush.length > 0) {
        try {
          await Promise.all(
            flush.map(([factory, children]) =>
              syncStore.insertChildAddresses({
                factory,
                childAddresses: children,
                chainId: chain.id,
              }),
            ),
          );
        } catch (err) {
          discovery.restorePending(flush);
          throw err;
        }
      }

      // PARALLEL read-ahead: prefetch ahead (never past the backfill end) — depth bounded by the shared
      // memory budget, not a fixed count (always lead-1; deeper only while unsaturated).
      for (const d of readAheadPlan(
        endIdx,
        chunkBlocks,
        dataEnd(),
        cfg.readahead,
        gate.saturated(),
      )) {
        void dataChunk(d).catch(() => {});
      }

      const tXform = Date.now(); // decode/transform time: Portal NDJSON → Ponder Sync* shapes
      const assembled = assembleRange(
        data,
        interval,
        spec,
        args.childAddresses,
      );
      stats.transformMs += Date.now() - tXform;

      evictBehind(interval[0]); // free chunks fully behind the cursor + their memory budget

      await syncStore.insertLogs({ logs: assembled.logs, chainId: chain.id });
      // INV-12: a stash entry is created then consumed exactly once. An upstream retry can legitimately
      // re-issue a range, so production (`on`) keeps the pre-refactor OVERWRITE semantics with a debug
      // log; `strict` (tests/CI) makes the double-set loud.
      invariantStrict(
        'INV-12',
        () => !stash.has(ikey(interval)) && !delegated.has(ikey(interval)),
        'stash entry created twice / for a delegated interval',
        () => ({ key: ikey(interval) }),
      );
      if (stash.has(ikey(interval)))
        log.debug({
          service: 'portal',
          msg: `Portal ${chain.name} [${interval[0]},${interval[1]}]: stash entry overwritten (upstream range retry)`,
        });
      stash.set(ikey(interval), {
        blocks: assembled.blocks,
        txs: assembled.txs,
        receipts: assembled.receipts,
        traces: assembled.traces,
        closest: assembled.closest,
      });

      log.debug({
        service: 'portal',
        msg: `Portal ${chain.name} [${interval[0]},${interval[1]}]: ${assembled.logs.length} logs (dataChunks=${stats.dataChunks} discChunks=${stats.discChunks} http=${stats.http} hits=${stats.cacheHits} inflight=${stats.maxInflight} err=${stats.errors})`,
      });
      return assembled.logs;
    },

    async syncBlockData(params) {
      const { interval, syncStore } = params;
      const key = ikey(interval);
      if (delegated.has(key)) {
        delegated.delete(key);
        return rpcFallback().syncBlockData(params);
      }
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
        blocks.set(
          (block as { number: string }).number,
          block as unknown as SyncBlockHeader,
        );
        const th = (transaction as { hash?: string }).hash;
        if (th) txs.set(th, transaction as unknown as SyncTransaction);
      }
      const blockArr = [...blocks.values()];
      if (blockArr.length === 0) return s.closest;
      await syncStore.insertBlocks({ blocks: blockArr, chainId });
      if (txs.size)
        await syncStore.insertTransactions({
          transactions: [...txs.values()],
          chainId,
        });
      if (s.receipts.length)
        await syncStore.insertTransactionReceipts({
          transactionReceipts: s.receipts,
          chainId,
        });
      if (s.traces.length)
        await syncStore.insertTraces({ traces: s.traces, chainId });
      stats.blocks += blockArr.length;
      stats.txs += txs.size;
      stats.receipts += s.receipts.length;
      stats.traces += s.traces.length;
      writeMetrics({
        metricsFile: cfg.metricsFile,
        chain,
        stats,
        chunkBlocks,
        portalHead,
        gate,
        startTime,
      });
      return s.closest;
    },
  };
};
