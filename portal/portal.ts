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
 * eventCallbacks}) : HistoricalSync`. See portal/INVARIANTS.md for the invariant catalog (INV-1…INV-17).
 */
import type { Address } from 'viem';
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
  buildRawLogMatcher,
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
import { createDiscovery, type PendingFlush } from './portal-discovery.js';
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
    requestTimeoutMs: cfg.requestTimeout,
    idleTimeoutMs: cfg.idleTimeout,
    logDebug: (msg) => log.debug({ service: 'portal', msg }),
    logWarn: (msg) => log.warn({ service: 'portal', msg }),
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
  let portalHead: number | undefined = cfg.finalizedHead;
  let startTime = 0;

  // FIX 2 (INV-3/INV-15): the discovery floor is the earliest block ANY factory could create a child —
  // `min` over the compiled spec's factories of `fromBlock ?? 0` (undefined ⇒ genesis). It is a property
  // of the SPEC, not of the intervals ponder happens to still need, so it is pinned at CONSTRUCTION (and
  // re-pinned per call, since chunkBlocks can scale) — see `pinDiscoveryFloor`. `discFloorBlock` is the
  // downward-clamped floor BLOCK (grid-independent, so it survives a rescale); requiredFactoryIntervals /
  // the interval start only REFINE it downward (C4/INV-4).
  const specFloorBlock =
    spec.factories.length > 0
      ? Math.min(...spec.factories.map((f) => f.fromBlock ?? 0))
      : undefined;
  let discFloorBlock = specFloorBlock;

  const ikey = (i: Interval): string => `${i[0]}-${i[1]}`;
  // FIX 1 (INV-9/INV-13): the data ceiling is the LOWER of the configured backfill end and the live Portal
  // head — clamp BOTH. With every source bounded (`backfillEnd` defined) the old `backfillEnd ?? portalHead`
  // ignored the head, so a frontier chunk's `desiredTo`/`coveredTo` extended PAST the head; the Portal
  // 204s/truncates above its head, `coveredTo` recorded phantom coverage, and once the head advanced later
  // intervals blind-hit the stale cache and were marked synced EMPTY (permanent silent gap). Clamping here
  // flows to desiredTo, coveredTo, endHint and raEnd, and re-arms the INV-13 extend as the head advances.
  const dataEnd = (): number =>
    Math.min(
      spec.backfillEnd ?? Number.POSITIVE_INFINITY,
      portalHead ?? Number.POSITIVE_INFINITY,
    );

  // FIX 2: snap `discFloorBlock` to the current grid and install it as the discovery floor. Idempotent —
  // called at construction and again on every syncBlockRangeData before any fetch (chunkBlocks may have
  // scaled since). No-op when there are no factories.
  const pinDiscoveryFloor = (): void => {
    if (discFloorBlock === undefined) return;

    discovery.setFloor(idxOf(discFloorBlock, chunkBlocks) * chunkBlocks);
  };

  // finality-gap fallback: Portal serves only finalized data, and its finalized head can (rarely) lag
  // Ponder's target. Any interval reaching past the head is delegated whole to the stock RPC sync.
  let rpcFallbackInstance: HistoricalSync | undefined;
  const rpcFallback = (): HistoricalSync => {
    rpcFallbackInstance ??= createHistoricalSync(args);
    return rpcFallbackInstance;
  };
  // stream-realtime mode: the recent region is served by the Portal /stream (portal-realtime-wire), so
  // historical never targets past the head and this RPC finality-gap fallback is neither needed nor wanted.
  // (this constructor is only selected for chains WITH a portal source, so the flag alone decides)
  const STREAM_REALTIME = cfg.realtime === 'stream';

  const refreshPortalHead = async (): Promise<number | undefined> => {
    if (cfg.finalizedHead !== undefined) {
      portalHead = cfg.finalizedHead;
      return portalHead;
    }
    // MONOTONIC cache: the Portal's finalized head only advances upstream, but load-balanced replicas
    // answer probes independently, so a LATER probe can return a LOWER number. Adopting it regressed the
    // cached head; in stream mode that reopened the G4/C11 silent gap (an interval at/below the true head
    // read as "past the head"). Every observation is ≤ the true head, so keeping the max stays
    // finality-safe. (wave 4 review)
    const h = await client.finalizedHeadRetry(3); // retry lives in the client (injectable sleep)
    if (h !== undefined) {
      portalHead = portalHead === undefined ? h : Math.max(portalHead, h);
    }

    return portalHead; // may be a kept-prior value, or undefined if never probed successfully
  };

  // Scale chunk size by the chain's block density (once); also seeds the finality head (C3 dedupe).
  const ensureChunkSize = (): Promise<void> => {
    chunkSizeP ??= (async () => {
      if (cfg.chunkFixed) return;

      const h = await client.finalizedHead();
      if (h !== undefined) {
        // FIX 5: a live probe drives chunk SCALING, but it must NOT overwrite an explicit
        // PORTAL_FINALIZED_HEAD pin — the pin is authoritative for the finality/delegation decision, and
        // clobbering it here (which happened whenever the pin was set but PORTAL_CHUNK_FIXED was not) let
        // intervals above the pin but below the live head be served instead of delegated. Only adopt the
        // probe as the head when there is no pin; scaling may still use the live `h`. Monotonic, same as
        // refreshPortalHead: a stale-LOW replica answer must not regress the cache. (wave 4 review)
        if (cfg.finalizedHead === undefined) {
          portalHead = portalHead === undefined ? h : Math.max(portalHead, h);
        }
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
    // FIX 3 + wave-4 log re-match: the needed-field crash check must consider ONLY the rows THIS call adds
    // that assembly will actually KEEP. On a frontier EXTEND `cd` already carries the base chunk's data; the
    // tail streams the disjoint range (coveredTo, desiredTo] whose block numbers are all > coveredTo (new
    // map keys), so a size delta over the append-only trace/tx/block maps captures exactly the tail's added
    // rows (inspecting the whole accumulated `cd`, as before FIX 3, let a data-bearing base + an event-less
    // tail whose dataset lacks a needed column throw fatally → evict → crash-loop). LOGS need more: the
    // Portal's server-side log filter over-returns rows assembly re-matches AWAY — a factory child's
    // pre-creation logs and a bounded filter's out-of-range logs — so a raw `cd.logs.size` delta would count
    // logs the indexer never keeps and arm the same false fatal (the exact class of #20's trace/transfer
    // residual, created here by the new re-match boundary). Count only logs surviving the SAME re-match
    // assembly applies; discovery is complete through this range (asserted below), so the seam agrees.
    const logMatchedRaw = spec.logFilters.length
      ? buildRawLogMatcher(spec, args.childAddresses)
      : undefined;
    let matchedLogsAdded = 0;
    const otherMatchedSize = (): number =>
      cd.traceBlocks.size + cd.txBlocks.size + cd.blockHeaders.size;
    const otherMatchedBefore = otherMatchedSize();
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
            if (logMatchedRaw)
              matchedLogsAdded += b.logs.filter((raw) =>
                logMatchedRaw(raw, b.header, b.header.number),
              ).length;
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
    // A NEEDED field the dataset lacked on THIS range: crash ONLY IF this call added MATCHED data — an
    // event the indexer processes would be incomplete. Logs count post-re-match (kept-only, wave 4);
    // trace/tx/block sources by size delta. An event-less (old/irrelevant) range — or an event-less /
    // all-re-match-dropped EXTEND tail over a data-bearing base (FIX 3) — proceeds.
    if (
      neededMissing.size &&
      (matchedLogsAdded > 0 || otherMatchedSize() > otherMatchedBefore)
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
    // INV-9: a Portal data request never targets past the known finalized head. `dataEnd()` now clamps to
    // the head (FIX 1), so `desiredTo <= portalHead` holds BY CONSTRUCTION whenever the head is known — the
    // former `spec.backfillEnd !== undefined` escape (which let a bounded backfill over-reach) is gone and
    // this assert is a strictly stronger tripwire. Intervals past the head are already delegated to RPC.
    invariant(
      'INV-9',
      portalHead === undefined || desiredTo <= portalHead,
      'data request targets past the Portal finalized head',
      () => ({ idx, desiredTo, portalHead }),
    );
    // Discovery scans as far as the backfill will need in one pass; head-clamped (FIX 1) via dataEnd().
    const de = dataEnd();
    const ensureOpts = {
      chunkBlocks,
      endHint: Number.isFinite(de) ? de : desiredTo,
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
      stats.extends++;
      const extendFrom = cached.coveredTo + 1;
      cached.coveredTo = desiredTo; // optimistic high-water so concurrent callers don't double-extend
      const prev = cached.promise;
      const extended = (async (): Promise<ChunkData> => {
        const cd = await prev;
        await discovery.ensure(desiredTo, ensureOpts); // discovery must reach the extended tail too
        // FIX 2: the floor is pinned from the spec at construction, so a factory sync always has a floor —
        // the former `discStartIdx === undefined` escape (which silently disabled this check on the very
        // chunks that fetched before requiredFactoryIntervals arrived) is gone.
        invariant(
          'INV-3',
          spec.factories.length === 0 || discovery.through() >= desiredTo,
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
      // FIX 2: floor pinned from the spec at construction ⇒ no `discStartIdx === undefined` escape (see extend).
      invariant(
        'INV-3',
        spec.factories.length === 0 || discovery.through() >= desiredTo,
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

  // FIX 2: pin the discovery floor from the spec NOW, before the first fetch (re-pinned per call once
  // chunkBlocks is finalized). A no-op when there are no factory sources.
  pinDiscoveryFloor();

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
            // Stream mode does NOT delegate to RPC — the Portal `/stream` covers (portal-head → tip]. Head
            // UNKNOWN (probe persistently failing) means we can't locate the historical↔realtime boundary:
            // returning [] would mark this interval synced with NO data while realtime streams only ABOVE
            // the head we can't find — a permanent silent gap. Fail loud. (finding 6 / C11 / INV-9)
            if (portalHead === undefined)
              throw new Error(
                `Portal ${chain.name}: /finalized-head probe failed in stream mode (PORTAL_REALTIME=stream) — cannot establish the historical/realtime boundary for [${interval[0]},${interval[1]}]. Refusing to mark the range synced with no data. Check Portal connectivity for ${portalUrl}.`,
              );

            // A KNOWN head below the interval is ALSO fatal in stream mode (wave 4 review; this used to
            // debug + return [] as "realtime /stream covers it"). It never legitimately fires:
            // clampFinalizedToPortalHead bounds every historical interval at the boundary head, and
            // realtime streams only ABOVE that boundary — so an interval "past the head" here means OUR
            // probe is stale-LOW relative to the boundary's (replica lag), and returning [] would mark
            // the interval synced while NO path ever delivers its data: the exact G4/C11 silent gap. The
            // head cache is monotonic and was just re-probed (retry ×3, twice), so reaching this line
            // means the lag persisted through all retries — the same "boundary cannot be located"
            // condition as an unknown head. Fail loud; a restart re-probes cleanly.
            throw new Error(
              `Portal ${chain.name}: interval [${interval[0]},${interval[1]}] ends past the probed finalized head ${portalHead} in stream mode (PORTAL_REALTIME=stream). Historical intervals are bounded at the finality boundary, so this head is stale-LOW (a lagging Portal replica). Refusing to mark the range synced with no data — realtime streams only above the boundary. Check ${portalUrl} replica consistency, or pin PORTAL_FINALIZED_HEAD.`,
            );
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
        discovery.reset(); // discFloorBlock (block-space) survives; re-pinned to the new grid just below
        log.debug({
          service: 'portal',
          msg: `Portal ${chain.name}: dense sources → chunkBlocks capped to ${chunkBlocks} (grid reset)`,
        });
      }

      // FIX 2: (re-)pin the discovery floor BEFORE any fetch. Base floor = the spec's earliest factory
      // start (`discFloorBlock`, seeded at construction); requiredFactoryIntervals and the interval start
      // only REFINE it downward (C4/INV-4). Applied on EVERY call (not just when ponder hands over
      // requiredFactoryIntervals) so an early spanning-chunk fetch never runs without discovery.
      if (discFloorBlock !== undefined) {
        discFloorBlock = Math.min(
          discFloorBlock,
          interval[0],
          ...requiredFactoryIntervals.map((r) => r.interval[0]),
        );
      }
      pinDiscoveryFloor();

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
          // INV-17 (write-side idempotence, #53): dedupe each factory's flush against the store's
          // already-persisted rows BEFORE inserting. Upstream's `insertChildAddresses` is a plain
          // INSERT with no ON CONFLICT and `factory_addresses` has no UNIQUE on (factory, chain,
          // address) — so a resumed/re-run writer that re-flushes an already-persisted child set would
          // durably DUPLICATE those rows. This is the write-side analogue of the read side's min-merge
          // in `getChildAddresses` (LEAST semantics): keep a child only if it is not yet persisted OR
          // is re-discovered at a STRICTLY LOWER creation block (whose lower row wins the min-merge on
          // read). Re-flushes at an equal/higher block become no-ops, so re-inserting the same set
          // cannot grow the table. `getChildAddresses` returns the store's min-merged map, and the
          // fork owns only this call site (the store method is upstream), so the guard lives here.
          const deduped: PendingFlush = [];
          for (const [factory, children] of flush) {
            const persisted = await syncStore.getChildAddresses({ factory });
            let toInsert: Map<Address, number> | undefined;
            for (const [address, block] of children) {
              const prev = persisted.get(address);
              if (prev !== undefined && prev <= block) continue;

              if (toInsert === undefined) toInsert = new Map();
              toInsert.set(address, block);
            }
            if (toInsert !== undefined) deduped.push([factory, toInsert]);
          }

          if (deduped.length > 0) {
            await Promise.all(
              deduped.map(([factory, children]) =>
                syncStore.insertChildAddresses({
                  factory,
                  childAddresses: children,
                  chainId: chain.id,
                }),
              ),
            );
          }
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
        msg: `Portal ${chain.name} [${interval[0]},${interval[1]}]: ${assembled.logs.length} logs (dataChunks=${stats.dataChunks} extends=${stats.extends} discChunks=${stats.discChunks} http=${stats.http} hits=${stats.cacheHits} inflight=${stats.maxInflight} err=${stats.errors})`,
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
