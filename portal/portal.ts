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
  buildRawTraceMatcher,
  buildRawTxMatcher,
  type ChunkData,
  createChunkData,
} from './portal-assemble.js';
import { dedupeChildAddressesAgainstStore } from './portal-child-dedupe.js';
import {
  chunkRange,
  evictionPlan,
  fetchBounds,
  idxOf,
  readAheadPlan,
  scaleChunkBlocks,
  traceSafeChunkBlocks,
} from './portal-chunks.js';
import { createPortalClient } from './portal-client.js';
import { loadPortalConfig } from './portal-config.js';
import { createDiscovery, type PendingFlush } from './portal-discovery.js';
import {
  type ChildAddresses,
  compileFetchSpec,
  RECEIPT_FIELDS,
} from './portal-filters.js';
import { sharedGate } from './portal-gate.js';
import {
  invariant,
  invariantStrict,
  setCheckMode,
} from './portal-invariant.js';
import {
  createCompletionSummary,
  createStats,
  startGateLog,
  startProgressLog,
  writeMetrics,
} from './portal-metrics.js';
import { isFinalityGap } from './portal-transform.js';

// #94: the table-qualified field keys of the receipt columns (RECEIPT_FIELDS ride the `transaction`
// projection). A `neededMissing` entry is `"${fieldKey} (${tag})"` with fieldKey like `transaction.logsBloom`
// (portal-client `colToFieldKey`), so a missing column is a RECEIPT field iff its fieldKey is one of these.
// `block.logsBloom` is a BLOCK_FIELD (fieldKey `block.logsBloom`) and is deliberately NOT here — a block's
// bloom is non-receipt and must arm on any matched row.
const RECEIPT_FIELD_KEYS = new Set(
  Object.keys(RECEIPT_FIELDS).map((f) => `transaction.${f}`),
);

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
  log.info({
    service: 'portal',
    msg: `Portal backfill active for ${chain.name}: ${portalUrl}`,
  });
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
  // #94: mirror `assembleRange`'s trace/transfer receipt-emit gate EXACTLY — a matched trace/transfer emits
  // a receipt iff ANY trace OR transfer filter wants receipts (not the specific filter that matched it).
  const needTraceReceipts =
    spec.traceFilters.some((f) => f.hasTransactionReceipt) ||
    spec.transferFilters.some((f) => f.hasTransactionReceipt);
  const discovery = createDiscovery({
    client,
    childAddresses: args.childAddresses,
    factories: spec.factories,
    discoveryWindows: cfg.discoveryWindows,
    warmupBlocks: cfg.warmupBlocks,
    stats,
  });
  const stopProgressLog = startProgressLog({
    chainName: chain.name,
    stats,
    intervalMs: cfg.progressInterval,
    startTime: () => startTime,
    discovery: () => discovery.snapshot(),
    logInfo: (entry) => log.info(entry),
  });
  const completeOnce = createCompletionSummary({
    chainName: chain.name,
    stats,
    startTime: () => startTime,
    logInfo: (entry) => log.info(entry),
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
    coveredFrom: number;
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
  // Per-interval record of the last non-empty INV-15 flush, keyed by ikey(interval). Ponder's core runs
  // the whole interval callback inside a RETRYING transaction (queryBuilder re-runs it on transient DB
  // failures — connection drops, deadlocks, even a failed COMMIT). A failure anywhere AFTER the flush
  // rolls the inserted children back while the pending queue stays drained — the retry would then flush
  // EMPTY and commit the factory interval cached WITHOUT its children (a permanent INV-15 violation:
  // restarts load children only from the store). syncBlockRangeData restores an entry into the queue when
  // the SAME interval re-enters (core's retry); a spurious restore after a COMMITTED attempt is safe (the
  // INV-17 store dedupe no-ops the re-flush).
  //
  // KEYED, not a single slot, because core PIPELINES intervals: in runtime/historical.ts (`syncInterval`)
  // the NEXT interval is dispatched INSIDE the current interval's still-open transaction — right after its
  // syncBlockRangeData resolves, BEFORE syncBlockData/insertIntervals/COMMIT. So a sibling interval B can
  // enter syncBlockRangeData and record its OWN flush while A's transaction is still failable. A single
  // slot let B's entry evict A's, so A's retry found nothing to restore and committed EMPTY — the very
  // loss this guards. With a Map only a MATCHING re-entry consumes its own entry; a sibling never touches
  // another's. (Same-interval CONCURRENT re-entry — a retry re-dispatching the next interval with the same
  // promise — degenerates to the documented INV-17 two-transaction TOCTOU residual: both txs read absence,
  // both insert; only a DB UNIQUE truly closes that, so no extra handling is needed here.) Core gives no
  // "committed" signal, so committed entries are pruned by a bounded cap: the LIVE pipeline depth is small
  // (the next interval is only queued after the current one's range-data resolves), so the cap sits far
  // above any set of still-uncommitted entries and the pruned oldest is always long-committed.
  const MAX_PENDING_FLUSHES = 32;
  const pendingFlushes = new Map<string, PendingFlush>();
  let chunkBlocks = cfg.chunkBlocks;
  // #50: per-process fetch quantum. Starts small for bounded time-to-first-commit, then converges
  // to chunkBlocks; PORTAL_WARMUP_BLOCKS=0 disables the warmup and restores the legacy fetch shape.
  let fetchQuantum =
    cfg.warmupBlocks === 0 ? Number.POSITIVE_INFINITY : cfg.warmupBlocks;
  let chunkSizeP: Promise<void> | undefined;
  let portalHead: number | undefined = cfg.finalizedHead;
  let startTime = 0;

  // FIX 2 (INV-3/INV-15): the discovery floor is the earliest block ANY factory could create a child —
  // `min` over the compiled spec's factories of `fromBlock ?? 0` (undefined ⇒ genesis). It is a property
  // of the SPEC alone, not of the intervals ponder happens to still need, so it is pinned ONCE at
  // CONSTRUCTION (grid-independent, so it survives a chunkBlocks rescale — see `pinDiscoveryFloor`).
  //
  // #21: no per-call downward refinement. Such a refinement is dead for correctness — the floor only has
  // to sit AT OR BELOW every real matched child-creation block, and `isLogFactoryMatched` discards any
  // creation log below `factory.fromBlock`, so every child that matters for factory `f` is created at
  // block ≥ `f.fromBlock ≥ discFloorBlock`. Neither `interval[0]` nor a `requiredFactoryIntervals` start
  // can ever lower a real child below `discFloorBlock` (each factory required-interval start is
  // `intervalIntersection(params.interval, [factory.fromBlock, factory.toBlock])`-bounded, so ≥
  // `factory.fromBlock ≥ discFloorBlock`). But `interval[0]` CAN drag the floor BELOW `discFloorBlock`:
  // in a mixed config (plain log filter from 0 + factory from ~15M) the first data interval starts at 0,
  // so the old `min(..., interval[0])` pulled the floor to 0 and the first `ensure()` streamed ~15M blocks
  // of factory-query results the matcher then discarded — a one-time-per-process overscan. Dropping it
  // keeps only the tight, sufficient construction-time floor.
  const discFloorBlock =
    spec.factories.length > 0
      ? Math.min(...spec.factories.map((f) => f.fromBlock ?? 0))
      : undefined;

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

  const maybeComplete = (interval: Interval): void => {
    const end = dataEnd();
    if (!Number.isFinite(end) || interval[1] < end) return;

    if (completeOnce()) stopProgressLog();
  };

  const growFetchQuantum = (): void => {
    if (cfg.warmupBlocks === 0) return;

    fetchQuantum = Math.min(fetchQuantum * 2, chunkBlocks);
  };

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
    // FIX 3 + wave-4 log re-match + #20 trace/tx re-match: the needed-field crash check must consider ONLY
    // the rows THIS call adds that assembly will actually KEEP. On a frontier EXTEND `cd` already carries the
    // base chunk's data; the tail streams the disjoint range (coveredTo, desiredTo] whose block numbers are
    // all > coveredTo (new map keys). Inspecting the whole accumulated `cd` (as before FIX 3) let a
    // data-bearing base + an event-less tail whose dataset lacks a needed column throw fatally → evict →
    // crash-loop, so the check is EXTEND-LOCAL. But a per-call size delta over a source's raw map is only
    // sound where that map holds ONLY matched rows: the Portal over-returns on every re-matched source. LOGS
    // (server log filter is the merged set, no per-child/per-range floor), TRACES/TRANSFERS (the trace query
    // is server-side UNFILTERED by INV-5 design — every trace fetched, client-matched only at assembly), and
    // account TXS (the tx query pushes merged from/to sets with no per-filter block range) all return rows
    // assembly re-matches AWAY. A raw size delta over any of them would count rows the indexer never keeps
    // and arm the same false fatal (#20). So each re-matched source is counted POST-re-match via its
    // build*Matcher, mirroring assembly EXACTLY; discovery is complete through this range (asserted below),
    // so the seam agrees. Only BLOCK headers stay a raw size delta — `cd.blockHeaders` is filtered inline at
    // fetch time (below) to hold ONLY BlockFilter-matched headers, so its size IS the matched count.
    const logMatchedRaw = spec.logFilters.length
      ? buildRawLogMatcher(spec, args.childAddresses)
      : undefined;
    let matchedLogsAdded = 0;
    const traceMatchedRaw = spec.needTraces
      ? buildRawTraceMatcher(spec, args.childAddresses)
      : undefined;
    let matchedTracesAdded = 0;
    const txMatchedRaw = spec.needTxFilter
      ? buildRawTxMatcher(spec, args.childAddresses)
      : undefined;
    let matchedTxsAdded = 0;
    const otherMatchedSize = (): number => cd.blockHeaders.size;
    const otherMatchedBefore = otherMatchedSize();
    const onRows = (n: number): void => {
      if (token.freed) return;

      token.rows += n;
      gate.addRows(n);
    };

    // #194: a factory whose child set overflows one 256KiB Portal body is streamed as MULTIPLE
    // byte-budgeted shards (disjoint address subsets, IDENTICAL fields/topics). Below the wall this is
    // exactly ONE shard byte-identical to the un-sharded logQuery() — a no-op. The completeness gate
    // (INV-1/INV-3/INV-11) is preserved BY CONSTRUCTION: this loop is INSIDE runStreams, so the chunk
    // promise resolves (→ dataCache.set by idx / interval-cached) ONLY after EVERY shard has drained
    // into `cd`. A shard throw propagates out of runStreams → rejects the chunk promise → G1 evict →
    // the WHOLE chunk (all shards) retries fresh. There is NO partial-commit window between shards.
    for (const lq of spec.logQueryShards())
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
            if (traceMatchedRaw)
              matchedTracesAdded += b.traces.filter((raw) =>
                traceMatchedRaw(raw, b.header.number),
              ).length;
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
            if (txMatchedRaw)
              matchedTxsAdded += b.transactions.filter((raw) =>
                txMatchedRaw(raw, b.header, b.header.number),
              ).length;
          }
      }
    // A NEEDED field the dataset lacked on THIS range: crash ONLY IF this call added MATCHED data — an
    // event the indexer processes would be incomplete. Logs, traces/transfers, and account txs each count
    // POST-re-match (kept-only — wave 4 for logs, #20 for traces/txs), because each source over-returns
    // rows assembly drops. Only BLOCK headers are a raw size delta (already matched-only at fetch time). An
    // event-less (old/irrelevant) range — or an event-less / all-re-match-dropped EXTEND tail over a
    // data-bearing base (FIX 3) — proceeds.
    //
    // #94: the missing field must be scoped to the rows that ACTUALLY consume it, not any matched row.
    // RECEIPT_FIELDS (transaction.logsBloom/status/gasUsed/…) are projected SPEC-WIDE onto the log, trace,
    // AND tx queries whenever ANY filter on the spec wants receipts (portal-filters `needReceipts` →
    // `txFields()`), but a receipt row is EMITTED per-source and only lands those columns in the store via
    // `toSyncReceipt`. So a matched row on a source that emits NO receipt cannot corrupt a missing receipt
    // column, yet before #94 any matched log/trace/tx armed the fatal on a spec-wide-projected receipt field
    // it never persisted → false-fatal → G1 evict → crash-loop. Split the check by field category and arm
    // each from ONLY the rows that consume it:
    //   • NON-receipt fields (LOG_FIELDS/TX_FIELDS/TRACE_FIELDS/BLOCK_FIELDS) are projected PER-SOURCE-TYPE,
    //     so any matched log/trace/tx/block genuinely needs them → armed exactly as before (no change).
    //   • RECEIPT fields arm ONLY from a receipt-EMITTING matched row, mirroring assembly's per-source emit
    //     gate EXACTLY so the seam and `assembleRange` agree by construction (the FIX-3/wave-4/#20 discipline):
    //       – logs:   assembly emits a receipt for a kept log's parent tx iff `spec.needReceipts` (SPEC-WIDE,
    //         portal-assemble log branch) — NOT the matched log's own filter flag. So gate matchedLogsAdded
    //         by `spec.needReceipts`. (Per-log-filter would UNDER-fire: a non-receipt log filter's kept log
    //         still gets a spec-wide receipt row → would silently drop a real gap. Never under-fire.)
    //       – traces: assembly wires `onReceipt` iff `needTraceReceipts` (any trace OR transfer filter wants
    //         receipts), then emits for EVERY matched trace regardless of which filter matched it. So gate
    //         matchedTracesAdded by `needTraceReceipts` (same reason per-filter would under-fire).
    //       – txs:    every txFilterMatched tx gets a receipt UNCONDITIONALLY (upstream types
    //         TransactionFilter.hasTransactionReceipt as literal `true`) → arm on any matchedTxsAdded.
    //   • BLOCK headers never emit a receipt → never arm a receipt field (block.* columns are non-receipt
    //     and armed by the non-receipt branch). Same for `block.logsBloom`: it is a BLOCK_FIELD, table-keyed
    //     `block.…`, so it is NOT in the receipt set below — a missing block bloom still fatals on any match.
    if (neededMissing.size) {
      // Each entry is `"${fieldKey} (${tag})"` (portal-client) — the table-qualified fieldKey is the head
      // up to the first space (fieldKeys never contain one). A RECEIPT field is `transaction.<X>` with
      // <X> ∈ RECEIPT_FIELDS.
      const receiptMissing = [...neededMissing].filter((entry) => {
        const spaceAt = entry.indexOf(' ');
        const fieldKey = spaceAt === -1 ? entry : entry.slice(0, spaceAt);

        return RECEIPT_FIELD_KEYS.has(fieldKey);
      });
      const nonReceiptMissing = neededMissing.size - receiptMissing.length;

      const nonReceiptArmed =
        matchedLogsAdded > 0 ||
        matchedTracesAdded > 0 ||
        matchedTxsAdded > 0 ||
        otherMatchedSize() > otherMatchedBefore;
      const receiptArmed =
        (spec.needReceipts && matchedLogsAdded > 0) ||
        (needTraceReceipts && matchedTracesAdded > 0) ||
        matchedTxsAdded > 0;

      if (
        (nonReceiptMissing > 0 && nonReceiptArmed) ||
        (receiptMissing.length > 0 && receiptArmed)
      ) {
        throw new Error(
          `Portal dataset for ${chain.name} is missing [${[...neededMissing].join(', ')}] on blocks [${from},${to}], which contain matched data your indexer needs — a Portal dataset-completeness gap. Failing fast rather than serving incomplete data; report the gap to SQD, or start your indexer past the affected range.`,
        );
      }
    }
  };

  // ── one data chunk: gated on discovery-through-the-fetch-window, then the source streams ────────────
  const dataChunk = (idx: number, need?: Interval): Promise<ChunkData> => {
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

    const activeNeed = cfg.warmupBlocks === 0 ? undefined : need;
    const needTo = activeNeed === undefined ? desiredTo : activeNeed[1];
    const quantum = Math.min(fetchQuantum, chunkBlocks);
    const de = dataEnd();
    const legacyEndHint = Number.isFinite(de) ? de : desiredTo;
    const ensureOpts = {
      chunkBlocks,
      endHint: legacyEndHint,
    };
    const discoveryEnsureTarget = (target: number): number => {
      if (spec.factories.length === 0) return target;

      const { floor, through } = discovery.snapshot();
      if (target < floor && through < floor) return floor;

      return target;
    };
    const discoveryReady = (target: number): boolean => {
      if (spec.factories.length === 0) return true;

      const { floor, through } = discovery.snapshot();
      return target < floor || through >= target;
    };
    const assertNeedCoveredFrom = (cached: CacheEntry): void => {
      if (activeNeed === undefined) return;

      invariant(
        'INV-19',
        activeNeed[0] >= cached.coveredFrom,
        'cache entry starts after requested interval',
        () => ({
          idx,
          needFrom: activeNeed[0],
          needTo,
          coveredFrom: cached.coveredFrom,
          coveredTo: cached.coveredTo,
        }),
      );
    };

    const cached = dataCache.get(idx);
    if (cached) {
      invariant(
        'INV-1',
        cached.specId === spec.id,
        'cached chunk built under a different fetch-spec',
        () => ({ idx }),
      );
      if (
        activeNeed !== undefined &&
        activeNeed[0] < cached.coveredFrom &&
        activeNeed[1] >= cached.coveredFrom
      ) {
        stats.extends++;
        const prefixFrom = activeNeed[0];
        const prefixTo = cached.coveredFrom - 1;
        const prev = cached.promise;
        cached.coveredFrom = prefixFrom; // optimistic low-water so concurrent callers dedup onto this repair
        const repaired = (async (): Promise<ChunkData> => {
          const cd = await prev;
          const ensureTo = discoveryEnsureTarget(prefixTo);
          await discovery.ensure(ensureTo, ensureOpts);
          invariant(
            'INV-3',
            discoveryReady(prefixTo),
            'chunk prefix repair under a stale discovery watermark',
            () => ({ idx, ...discovery.snapshot(), desiredTo, prefixTo }),
          );
          await runStreams(cd, prefixFrom, prefixTo, cached.token);
          growFetchQuantum();

          return cd;
        })();
        cached.promise = repaired;
        repaired.catch(() => {
          if (dataCache.get(idx) === cached) dataCache.delete(idx);
          freeToken(cached.token);
        });
      }
      assertNeedCoveredFrom(cached);
      if (needTo <= cached.coveredTo) {
        stats.cacheHits++;
        return cached.promise;
      }
      // Head-boundary staleness (INV-13, bug found relative to main and fixed by open PR #5 — same fix,
      // this architecture): the FRONTIER chunk was fetched TRUNCATED at the then-finalized head; the
      // head has since advanced. Serving the stale cache would mark the interval synced over a silent
      // gap — EXTEND instead: stream ONLY the newly finalized tail (coveredTo, desiredTo] and merge.
      stats.extends++;
      const extendFrom = cached.coveredTo + 1;
      const extendTo = Math.min(
        desiredTo,
        Math.max(needTo, cached.coveredTo + quantum),
      );
      cached.coveredTo = extendTo; // optimistic high-water so concurrent callers don't double-extend
      const prev = cached.promise;
      const extended = (async (): Promise<ChunkData> => {
        const cd = await prev;
        const ensureTo = discoveryEnsureTarget(extendTo);
        await discovery.ensure(ensureTo, ensureOpts); // discovery must reach the extended tail too
        // FIX 2: the floor is pinned from the spec at construction, so a factory sync always has a floor —
        // the former `discStartIdx === undefined` escape (which silently disabled this check on the very
        // chunks that fetched before requiredFactoryIntervals arrived) is gone.
        invariant(
          'INV-3',
          discoveryReady(extendTo),
          'chunk extend under a stale discovery watermark',
          () => ({ idx, ...discovery.snapshot(), desiredTo, extendTo }),
        );
        await runStreams(cd, extendFrom, extendTo, cached.token);
        growFetchQuantum();

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
    const [from, to] = fetchBounds(gridFrom, desiredTo, activeNeed, quantum);
    const p = (async (): Promise<ChunkData> => {
      const ensureTo = discoveryEnsureTarget(to);
      await discovery.ensure(ensureTo, ensureOpts); // children ≤ this fetch window are known (INV-3)
      // FIX 2: floor pinned from the spec at construction ⇒ no `discStartIdx === undefined` escape (see extend).
      invariant(
        'INV-3',
        discoveryReady(to),
        'data fetch under a stale discovery watermark',
        () => ({ idx, ...discovery.snapshot(), desiredTo, to }),
      );
      stats.dataChunks++;
      const cd = createChunkData();
      await runStreams(cd, from, to, token);
      growFetchQuantum();

      return cd;
    })();
    const entry: CacheEntry = {
      promise: p,
      specId: spec.id,
      coveredFrom: from,
      coveredTo: to,
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

  // INV-15: persist the children discovered within an interval's range — the caller's syncStore is the
  // transaction ponder's core commits together with insertIntervals (which marks the interval's factory
  // intervals cached). Interval-scoped on purpose (see Discovery.takePendingInRange); a failed flush
  // restores the queue and fails the interval loud (core rolls back — nothing is silently dropped).
  // Called on BOTH serving paths — the Portal path (after its chunks resolve) and the RPC finality-
  // delegation path — because core caches the factory intervals either way, and the RPC fallback cannot
  // be trusted to persist these children itself: it shares `args.childAddresses`, and upstream's
  // syncAddressFactory persists only children NOT already in that record, while the wide Portal scan
  // (endHint = dataEnd) has usually already recorded them far past the interval being served.
  const persistPendingChildren = async (
    interval: Interval,
    syncStore: Parameters<HistoricalSync['syncBlockRangeData']>[0]['syncStore'],
  ): Promise<void> => {
    const flush = discovery.takePendingInRange(interval[0], interval[1]);
    if (flush.length === 0) return;

    // Survives a post-flush rollback: restored into the queue when THIS SAME interval re-enters (core's
    // transaction retry — see `pendingFlushes`). Keyed, so a pipelined sibling interval cannot evict it.
    const key = ikey(interval);
    pendingFlushes.set(key, flush);
    // No commit signal from core, so prune the oldest (long-committed) entry once the map outgrows the
    // small live pipeline depth. Map iteration is insertion-ordered → the first key is the oldest.
    if (pendingFlushes.size > MAX_PENDING_FLUSHES) {
      const oldest = pendingFlushes.keys().next().value as string;
      pendingFlushes.delete(oldest);
    }
    try {
      // INV-17 (write-side idempotence, #53): dedupe each factory's flush against the store's
      // already-persisted rows BEFORE inserting. Upstream's `insertChildAddresses` is a plain
      // INSERT with no ON CONFLICT and `factory_addresses` has no UNIQUE on (factory, chain,
      // address) — so a resumed/re-run writer that re-flushes an already-persisted child set would
      // durably DUPLICATE those rows. This is the write-side analogue of the read side's min-merge
      // in `getChildAddresses` (LEAST semantics): keep a child only if it is not yet persisted OR
      // is re-discovered at a STRICTLY LOWER creation block (whose lower row wins the min-merge on
      // read). Re-flushes at an equal/higher block become no-ops, so re-inserting the same set
      // cannot grow the table. The dedupe (group by store identity, read `getChildAddresses`,
      // case-normalize, LEAST-keep) is the SHARED `dedupeChildAddressesAgainstStore` core — the
      // realtime finalize path (runtime/realtime.ts, `dedupeFinalizeChildAddresses`) runs the same
      // logic, so both sync modes are byte-identically idempotent (see portal-child-dedupe.ts).
      //
      // The read→dedupe→insert→insertIntervals sequence all runs inside ONE store transaction (the
      // syncStore is created from the tx handle in runtime/historical.ts), so for a SINGLE writer the
      // guard is transactional — no interleaving read/insert can slip a duplicate past it. (Two
      // concurrent writers are two transactions: both can read absence and INSERT — only a DB UNIQUE
      // closes that; documented residual on INV-17.)
      const deduped: PendingFlush = await dedupeChildAddressesAgainstStore(
        flush,
        ({ factory }) => syncStore.getChildAddresses({ factory }),
      );

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
      pendingFlushes.delete(key); // restored NOW — a same-interval retry must not restore it a second time

      throw err;
    }
  };

  // FIX 2: pin the discovery floor from the spec NOW, before the first fetch (re-pinned per call once
  // chunkBlocks is finalized). A no-op when there are no factory sources.
  pinDiscoveryFloor();

  // #140: the SINGLE source of truth for the per-chain metrics file. `writeMetrics` overwrites
  // `${metricsFile}.${chain.id}` on every call (last-write-wins), and reads the live cumulative
  // `stats` plus the current `chunkBlocks`/`portalHead`, so this closure must be invoked on BOTH
  // syncBlockData exits — the normal insert path AND the 0-block early return. A chain that streams 0
  // blocks in its window still ran; emitting a `blocks=0 logs=0 …` file makes it observable and gate-
  // able (issue #140) instead of leaving the harness with no file to read.
  const emitMetrics = (): void => {
    writeMetrics({
      metricsFile: cfg.metricsFile,
      chain,
      stats,
      chunkBlocks,
      portalHead,
      gate,
      startTime,
    });
  };

  return {
    async syncBlockRangeData(params) {
      const { interval, syncStore } = params;
      if (!startTime) startTime = Date.now();
      // A SAME-interval re-entry is ponder's transaction retry (the interval callback re-runs after a
      // rollback): restore THIS interval's previous flush — those children were rolled back with the
      // transaction, and leaving the queue drained would commit the retry cached WITHOUT them (see
      // `pendingFlushes`). Keyed by ikey, so this consumes ONLY this interval's entry: a pipelined sibling
      // that entered while this transaction was still open left its own entry untouched, and this
      // interval's entry likewise survives that sibling for its own retry. (persistPendingChildren
      // re-records the entry below if this attempt also flushes non-empty, so a repeated retry is covered.)
      const rkey = ikey(interval);
      const reentry = pendingFlushes.get(rkey);
      if (reentry !== undefined) {
        discovery.restorePending(reentry);
        pendingFlushes.delete(rkey);
      }
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
          // INV-15 × INV-9: children already discovered by the wide Portal scan and pending in this
          // range MUST be flushed by THIS path. The fallback shares `args.childAddresses`, so upstream's
          // syncAddressFactory dedupe-skips exactly the pre-discovered children (it persists only ones
          // NOT already in the record) — while core still marks the factory interval cached in this same
          // transaction. Skipping the flush here stranded them: persisted by NEITHER path, permanently
          // lost on the next restart.
          await persistPendingChildren(interval, syncStore);

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

      // FIX 2 (#21): (re-)pin the discovery floor BEFORE any fetch. The floor is the spec's earliest
      // factory start (`discFloorBlock`, fixed at construction). Re-pinned here — not refined — only
      // because `chunkBlocks` may have scaled since construction, which moves the grid snap. No per-call
      // downward refinement from `interval[0]` / `requiredFactoryIntervals`: those terms are dead for
      // correctness (each is ≥ `discFloorBlock`, so they never lower a real matched child below the
      // floor), and the `interval[0]` term caused a one-time sub-floor discovery overscan in mixed
      // configs (see the construction-time note). Applied on EVERY call so an early spanning-chunk fetch
      // never runs without discovery.
      pinDiscoveryFloor();
      if (cfg.warmupBlocks !== 0 && spec.factories.length > 0) {
        discovery.seed(interval[0] - 1);
      }

      const startIdx = idxOf(interval[0], chunkBlocks);
      const endIdx = idxOf(interval[1], chunkBlocks);
      const idxs: number[] = [];
      for (let i = startIdx; i <= endIdx; i++) idxs.push(i);
      const data = await Promise.all(
        idxs.map((i) =>
          dataChunk(i, [
            Math.max(interval[0], i * chunkBlocks),
            Math.min(interval[1], i * chunkBlocks + chunkBlocks - 1),
          ]),
        ),
      );

      // INV-15: persist the children discovered within THIS interval's range NOW — dataChunk awaited
      // discovery through this interval (shared with the RPC-delegation path, see persistPendingChildren).
      await persistPendingChildren(interval, syncStore);

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
      // #143: count logs ACTUALLY inserted (post-re-match), not the raw streamed `stats.logs`. A window
      // whose over-returned logs are all re-match-dropped inserts 0 here, so `inserted.logs` reads 0
      // rather than contradicting `inserted.blocks=0`.
      stats.insertedLogs += assembled.logs.length;
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
        const closest = await rpcFallback().syncBlockData(params);
        maybeComplete(interval);

        return closest;
      }
      const s = stash.get(key);
      stash.delete(key); // INV-12: consumed exactly once
      if (!s) {
        maybeComplete(interval);

        return undefined;
      }
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
      if (blockArr.length === 0) {
        // #140: a 0-block window still ran — emit the per-chain metrics file (blocks=0 …) so it is
        // present and gate-able, then take the same early-out (nothing to insert for 0 blocks).
        emitMetrics();
        maybeComplete(interval);

        return s.closest;
      }
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
      emitMetrics();
      maybeComplete(interval);

      return s.closest;
    },
  };
};
