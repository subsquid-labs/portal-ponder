import { writeFileSync } from "node:fs";
import type { Common } from "@/internal/common.js";
import type {
  Chain,
  FactoryId,
  Filter,
  LogFilter,
  SyncBlock,
  SyncBlockHeader,
  SyncLog,
  SyncTrace,
  SyncTransaction,
  SyncTransactionReceipt,
} from "@/internal/types.js";
import {
  getChildAddress,
  getFilterFactories,
  isAddressFactory,
  isAddressMatched,
  isBlockFilterMatched,
  isLogFactoryMatched,
  isTraceFilterMatched,
  isTransactionFilterMatched,
  isTransferFilterMatched,
} from "@/runtime/filter.js";
import type { Rpc } from "@/rpc/index.js";
import type { Interval } from "@/utils/interval.js";
import { type Address, type Hex } from "viem";
import { type HistoricalSync, createHistoricalSync } from "./index.js";
import { type RawHeader, hx, isFinalityGap, toSyncLog, toSyncBlockHeader, toSyncTransaction, toSyncReceipt, parityToCallFrame, cmpTraceAddr, traceSafeChunkBlocks } from "./portal-transform.js";

/**
 * Portal-backed historical sync with a PARALLEL read-ahead chunk buffer.
 *
 * Ponder feeds small intervals; Portal is latency-bound per request but has huge
 * parallel bandwidth. So we fetch large aligned CHUNKS and serve every interval
 * from cache — and we fetch chunks IN PARALLEL (read-ahead depth N) so the
 * Portal's per-request latency overlaps instead of serializing.
 *
 * Correctness for factory sources: the discovery timeline is decoupled from the
 * data timeline. Each chunk's children are discovered independently (clamped to
 * the factory's real start block), and a data chunk only fetches once discovery
 * is complete THROUGH its own block range — so no child event is missed even
 * though data chunks are fetched out of order.
 *
 * Tunables: PORTAL_CHUNK_BLOCKS (default 500k), PORTAL_READAHEAD (default 6).
 * Selected at runtime/historical.ts when `chain.portal` is set; realtime → rpc.
 */

type CreateHistoricalSyncParameters = {
  common: Common;
  chain: Chain;
  rpc: Rpc;
  childAddresses: Map<FactoryId, Map<Address, number>>;
  // FULL per-chain filter set (runtime: params.eventCallbacks). The fetch-spec is resolved from
  // THIS, once, not from per-call requiredIntervals (which is only the subset still needed and
  // shrinks as fragments cache) — so every idx-keyed chunk is filter-complete. (C1)
  eventCallbacks: { filter: Filter }[];
};

type PortalLogRequest = { address?: string[]; topic0?: string[]; topic1?: string[]; topic2?: string[]; topic3?: string[]; transaction?: boolean };
type ChunkData = {
  headers: Map<number, RawHeader>;
  logs: Map<number, any[]>;
  txs: Map<number, any[]>;
  // for trace/transfer sources: full block + all its traces + its txs, by block number
  traceBlocks: Map<number, { header: RawHeader; traces: any[]; txs: any[] }>;
  // for block-interval sources: headers of blocks matching a BlockFilter (interval/offset)
  blockHeaders: Map<number, RawHeader>;
  // for account transaction sources: blocks + their from/to-matched txs, by block number
  txBlocks: Map<number, { header: RawHeader; txs: any[] }>;
};

const PORTAL_MAX_ADDRESSES = 1000;
// Portal rejects any request whose raw body exceeds this (sqd-network transport/src/protocol.rs:
// `MAX_RAW_QUERY_SIZE = 256 * 1024`) with 400 "Query is too large". The body is dominated by filter
// address lists (factory children in log/tx filters). We keep under it by merging per-event log
// filters + batching addresses; a body that still overflows fails loud (see fetchBatch).
const MAX_RAW_QUERY_SIZE = 256 * 1024;
const CHUNK_BLOCKS = Number(process.env.PORTAL_CHUNK_BLOCKS ?? 500_000);
const READAHEAD = Number(process.env.PORTAL_READAHEAD ?? 6);
// The Portal fans ONE stream request out across up to `buffer_size` chunk-workers concurrently
// (default 10, clamped to 1000) at ZERO extra CU. Without it a wide/sparse scan runs ~10-wide and
// the head-of-line front chunk stalls → the stream truncates (verified: an empty [0,5M] factory scan
// terminates at 60s with the default vs completes in 17.5s at 100). Set high; the Portal's own
// download window (~500) is the real ceiling, and CU is charged per chunk touched regardless.
const BUFFER_SIZE = Number(process.env.PORTAL_BUFFER_SIZE ?? 100);
// Discovery splits [deploy, head] into this many DISJOINT windows fetched CONCURRENTLY (separate
// streams — the Portal serializes one stream in block order, so parallelism comes from disjoint
// requests). Bounded so tiny ranges aren't over-split.
const DISCOVERY_WINDOWS = Number(process.env.PORTAL_DISCOVERY_WINDOWS ?? 8);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ── Global adaptive Portal controller — module scope, SHARED across every per-chain sync ──────────
// All chains stream from the SAME Portal endpoint, so request concurrency, CU/throttle headroom and
// buffered memory are ONE shared budget, not per-chain (15 chains each running a private read-ahead
// is what OOMs and gets CU-throttled). Portal mode ≠ RPC: it prefers a FEW long, data-heavy requests
// over RPC's aggressive low-latency fan-out, so the objective is not max RPS but to keep every chain's
// read-ahead buffer FULL while bounding total memory — so indexing is bottlenecked by local
// decode/write and NEVER by awaiting a fetch (the whole promise of the Portal). Two controls, both
// zero-config + self-tuning (the client can't know the endpoint's limits, which drift over time):
//   • AIMD concurrency — start low, ramp one slot per clean generation, halve on 429/503/timeout.
//     Discovers the endpoint's LIVE capacity and re-adapts (mirrors Ponder's native RPC AIMD,
//     rpc/index.ts), but GLOBAL because the endpoint is shared.
//   • Rows-in-memory budget — read-ahead prefetches until the shared buffer reaches it, then
//     backpressures. Caps memory regardless of chain count; consumption drains + evicts → refills.
const portalGate = (() => {
  // Concurrency must feed however many chains share the endpoint, so the floor is generous (a
  // multichain app parks one in-flight request per chain plus read-ahead). Memory — not concurrency —
  // is the OOM guard (the rows budget below), so a higher concurrency ceiling is safe. AIMD still
  // discovers the true ceiling: ramp while clean, halve on throttle. All zero-config.
  const MIN = Number(process.env.PORTAL_MIN_CONCURRENCY ?? 8);
  const MAX = Number(process.env.PORTAL_MAX_CONCURRENCY ?? 48);
  const START = Number(process.env.PORTAL_START_CONCURRENCY ?? 16);
  // Backpressure threshold on buffered rows (log/tx/trace/block records held across all chains'
  // read-ahead). A buffered record costs ~5-10 KB live in V8 once ponder's derived copies are
  // counted, so 250k ≈ 1.5-2.5 GB — it must engage BEFORE the heap dies. The prior 1.2M was dead
  // code: a 4 GB heap OOMs at ~450k rows, so the cap never fired. Scale up with --max-old-space-size.
  const MAX_ROWS = Number(process.env.PORTAL_MAX_ROWS_IN_MEM ?? 250_000);
  let limit = START, active = 0, ok = 0, rows = 0;
  const waiters: (() => void)[] = [];
  const pump = () => { while (active < limit && waiters.length > 0) { active++; waiters.shift()!(); } };
  return {
    acquire: (): Promise<void> => new Promise<void>((r) => { waiters.push(r); pump(); }),
    release: () => { active = Math.max(0, active - 1); pump(); },
    onOk: () => { if (++ok >= 8 && limit < MAX) { limit = Math.min(MAX, limit + 2); ok = 0; pump(); } }, // additive ramp (+2 / 8 clean)
    onThrottle: () => { limit = Math.max(MIN, Math.floor(limit / 2)); ok = 0; }, // multiplicative back-off
    addRows: (n: number) => { rows += n; },
    freeRows: (n: number) => { rows = Math.max(0, rows - n); },
    saturated: () => rows >= MAX_ROWS, // memory backpressure for read-ahead (never gates the needed chunk)
    snapshot: () => ({ limit, active, rows }),
  };
})();
// opt-in observability: watch the AIMD concurrency + memory backpressure adapt live.
if (process.env.PORTAL_GATE_LOG) setInterval(() => { const s = portalGate.snapshot(); console.log(`[portalGate] concurrency_limit=${s.limit} active=${s.active} buffered_rows=${s.rows}`); }, 20_000).unref();

const asArr = (t: Hex | readonly Hex[] | null | undefined): string[] | undefined => {
  if (t === null || t === undefined) return undefined;
  return (Array.isArray(t) ? t : [t]).map((x) => (x as string).toLowerCase());
};

export const createPortalHistoricalSync = (
  args: CreateHistoricalSyncParameters,
): HistoricalSync => {
  const portalUrl = args.chain.portal!.replace(/\/$/, "");
  const log = args.common.logger;
  const baseHeaders: Record<string, string> = { "content-type": "application/json", "accept-encoding": "gzip" };
  if (process.env.PORTAL_API_KEY) baseHeaders["x-api-key"] = process.env.PORTAL_API_KEY;

  const stats = { dataChunks: 0, discChunks: 0, http: 0, logs: 0, errors: 0, retries: 0, bytes: 0, cacheHits: 0, inflight: 0, maxInflight: 0, blocks: 0, txs: 0, receipts: 0, traces: 0, rpcFallback: 0, gateWaitMs: 0, fetchMs: 0, transformMs: 0 };
  const dataCache = new Map<number, Promise<ChunkData>>(); // keyed by chunk index
  const chunkRows = new Map<number, number>(); // idx → buffered row count, for the global memory budget
  let discoveredThrough = -1; // high-water block covered by the single wide factory-discovery scan
  let discoveryP: Promise<void> = Promise.resolve(); // the (lazily extended) discovery scan promise
  const stash = new Map<string, { blocks: SyncBlockHeader[]; txs: SyncTransaction[]; receipts: SyncTransactionReceipt[]; traces: { trace: SyncTrace; block: SyncBlock; transaction: SyncTransaction }[]; closest: SyncBlock | undefined }>();
  const ikey = (i: Interval) => `${i[0]}-${i[1]}`;
  let chunkBlocks = CHUNK_BLOCKS;
  let chunkSizeP: Promise<void> | undefined;
  const idxOf = (n: number) => Math.floor(n / chunkBlocks);
  let discStartIdx: number | undefined; // factory deploy chunk — discovery floor (fixes from-0 scan)

  // finality-gap fallback: Portal serves only finalized data, and its finalized head can
  // (rarely) lag Ponder's target. Any interval reaching past Portal's head is delegated
  // whole to the stock RPC historical sync. PORTAL_FINALIZED_HEAD overrides for tests/ops.
  let portalHead: number | undefined = process.env.PORTAL_FINALIZED_HEAD ? Number(process.env.PORTAL_FINALIZED_HEAD) : undefined;
  let rpcFallbackInstance: HistoricalSync | undefined;
  const rpcFallback = (): HistoricalSync => (rpcFallbackInstance ??= createHistoricalSync(args));
  const delegated = new Set<string>(); // interval keys routed to RPC
  // Portal-native realtime (PORTAL_REALTIME="stream"): the recent region [portal-head → tip] is served by
  // the Portal `/stream` in runtime/realtime.ts, and `clampFinalizedToPortalHead` lowers ponder's finalized
  // block to the Portal head — so historical never targets past the head and this RPC finality-gap fallback
  // is neither needed nor wanted (it's the single-thread stall this mode removes). Skip it here.
  const STREAM_REALTIME = Boolean(args.chain.portal) && process.env.PORTAL_REALTIME === "stream";
  const refreshPortalHead = async (): Promise<number | undefined> => {
    if (process.env.PORTAL_FINALIZED_HEAD) return (portalHead = Number(process.env.PORTAL_FINALIZED_HEAD));
    // retry: the head probe is cheap, and a valid head is load-bearing for the finality-gap decision.
    // On persistent failure portalHead stays undefined → the caller treats "head unknown" conservatively.
    for (let attempt = 0; attempt < 3; attempt++) {
      try { const h = await fetch(`${portalUrl}/finalized-head`, { headers: baseHeaders }).then((r) => r.json()); if (typeof h?.number === "number") return (portalHead = h.number); } catch { /* retry */ }
      await new Promise((r) => setTimeout(r, 200 * (attempt + 1)));
    }
    return portalHead; // may be a kept-prior value, or undefined if never probed successfully
  };
  // instrumentation: per-chain backfill metrics → PORTAL_METRICS_FILE.<chainId> (for the bench harness)
  const METRICS_FILE = process.env.PORTAL_METRICS_FILE;
  let startTime = 0;
  const writeMetrics = () => {
    if (!METRICS_FILE) return;
    try {
      writeFileSync(`${METRICS_FILE}.${args.chain.id}`, JSON.stringify({
        chain: args.chain.name, chainId: args.chain.id, wallMs: startTime ? Date.now() - startTime : 0,
        chunkBlocks, portalFinalizedHead: portalHead ?? null,
        fetch: { dataChunks: stats.dataChunks, discChunks: stats.discChunks, http: stats.http, bytes: stats.bytes, errors: stats.errors, retries: stats.retries, cacheHits: stats.cacheHits, maxInflight: stats.maxInflight },
        // saturation breakdown (cumulative ms across all requests of this chain): gate-wait = time
        // blocked on the global concurrency budget; fetch = Portal I/O (POST+stream drain); transform
        // = NDJSON→Sync* decode. DB-write time lives in Ponder (per-range log timing), not here.
        timing: { gateWaitMs: Math.round(stats.gateWaitMs), fetchMs: Math.round(stats.fetchMs), transformMs: Math.round(stats.transformMs) },
        portalGate: portalGate.snapshot(),
        inserted: { logs: stats.logs, blocks: stats.blocks, txs: stats.txs, receipts: stats.receipts, traces: stats.traces },
        rpcFallbackIntervals: stats.rpcFallback,
      }));
    } catch { /* best-effort */ }
  };

  // Scale chunk size by the chain's block density. High-block-rate chains (Arbitrum
  // ~478M blocks ≈ 19× Ethereum) otherwise need 19× more 500k-block chunks = 19× more
  // latency-bound round-trips. CU is charged per Portal data-chunk (data-density based),
  // so larger BLOCK-chunks don't cost more CU — they just cut round-trips. PORTAL_CHUNK_FIXED=1 disables.
  const ensureChunkSize = (): Promise<void> =>
    (chunkSizeP ??= (async () => {
      if (process.env.PORTAL_CHUNK_FIXED) return;
      try {
        const h = await fetch(`${portalUrl}/finalized-head`, { headers: baseHeaders }).then((r) => r.json());
        if (typeof h?.number === "number") portalHead = h.number; // dedupe the probe: seed the finality head (C3)
        const density = Math.max(1, Math.round((h.number as number) / 25_000_000));
        chunkBlocks = Math.min(CHUNK_BLOCKS * density, 25_000_000);
        log.debug({ service: "portal", msg: `Portal ${args.chain.name}: head=${h.number} → chunkBlocks=${chunkBlocks} (${density}× density)` });
      } catch { /* keep default */ }
    })());

  // transient = retry: HTTP 503/529/429 AND network/socket errors (parallel load
  // makes "other side closed" / ECONNRESET / fetch failed routine).
  const isNetworkError = (err: any): boolean => {
    const m = `${err?.message ?? ""} ${err?.cause?.message ?? ""} ${err?.cause?.code ?? ""}`.toLowerCase();
    return /socket|closed|econnreset|fetch failed|terminated|timeout|network|epipe|und_err/.test(m) || err?.name === "AbortError";
  };

  // one POST+drain; returns blocks or "done" (204); throws (with .retryAfterMs on 503-class).
  async function fetchBatch(body: string, cursor: number): Promise<{ blocks: { header: RawHeader; logs?: any[]; transactions?: any[]; traces?: any[] }[]; last: number } | "done"> {
    // Proactive, uniform size guard — covers EVERY request type (logs/traces/txs/discovery) at the one
    // POST choke point. A body over MAX_RAW_QUERY_SIZE would 400; surface it explicitly with the real
    // driver instead. Euler's worst (eth: 897 children × 24 topics) is ~41KB, so this never fires here;
    // it protects indexers with pathological filtered-address counts (esp. unbatched tx from/to sets).
    if (body.length > MAX_RAW_QUERY_SIZE) {
      const q = (() => { try { return JSON.parse(body); } catch { return {}; } })();
      const nLog = (q.logs ?? []).reduce((s: number, r: any) => s + (r.address?.length ?? 0), 0);
      const nTx = (q.transactions ?? []).reduce((s: number, r: any) => s + (r.from?.length ?? 0) + (r.to?.length ?? 0), 0);
      throw new Error(
        `Portal request body ${(body.length / 1024).toFixed(1)}KB exceeds MAX_RAW_QUERY_SIZE ${MAX_RAW_QUERY_SIZE / 1024}KB @ ${cursor}. ` +
        `Filter addresses in this request: ${nLog} log + ${nTx} tx(from/to). ` +
        `Log filters are already merged+batched (PORTAL_MAX_ADDRESSES=${PORTAL_MAX_ADDRESSES}); if this is a tx filter, its from/to set is too large to fit one request and cannot be safely split — narrow the filter.`,
      );
    }
    const tAcq = Date.now(); await portalGate.acquire(); stats.gateWaitMs += Date.now() - tAcq; // gate-wait = concurrency back-pressure
    const tFetch = Date.now();
    stats.inflight++; stats.maxInflight = Math.max(stats.maxInflight, stats.inflight);
    try {
      const res = await fetch(`${portalUrl}/finalized-stream?buffer_size=${BUFFER_SIZE}`, { method: "POST", headers: baseHeaders, body });
      stats.http++;
      if (res.status === 204) { portalGate.onOk(); return "done"; }
      // Transient, retry with back-off (never crash the app on one bad response): 429/529 = explicit
      // throttle; ALL 5xx (500/502/503/504…) = gateway/proxy/server hiccups that return an HTML error
      // page mid-backfill; 409 on the FINALIZED stream = a gateway "conflict" (finalized data doesn't
      // reorg, so it's not the reorg JSON). Backing off on any of these keeps the AIMD honest.
      if (res.status >= 500 || res.status === 429 || res.status === 409) {
        await res.body?.cancel().catch(() => {});
        const ra = Number(res.headers.get("retry-after"));
        const e: any = new Error(`Portal ${res.status}`); e.retryAfterMs = Number.isFinite(ra) ? ra * 1000 : undefined;
        portalGate.onThrottle(); // treat as congestion → halve global concurrency
        throw e;
      }
      if (!res.ok) {
        const text = (await res.text()).slice(0, 300);
        // a dataset that lacks a requested column (e.g. Monad has no accessList) → the whole
        // request 400s. Surface the column so stream() can drop the field and retry.
        const m = res.status === 400 && text.match(/column '([a-z0-9_]+)' is not found in '([a-z_]+)'/i);
        if (m) { const e: any = new Error(`Portal 400: unsupported column ${m[1]} in ${m[2]}`); e.unsupportedColumn = m[1]; e.unsupportedTable = m[2]; throw e; }
        // OTHER schema shape: a dataset whose schema doesn't know the field at all → query PARSE
        // error ("unknown field `accessList`, expected one of ..."). Find which fields-block we put
        // it in (block/transaction/log/trace) so stream() can drop that field key and retry.
        const u = res.status === 400 && text.match(/unknown field `([a-zA-Z0-9_]+)`/);
        if (u && u[1]) {
          const fn = u[1]; let table = "transaction";
          try { const q = JSON.parse(body); for (const t of ["transaction", "block", "log", "trace"]) if (q?.fields?.[t] && q.fields[t][fn] !== undefined) { table = t; break; } } catch { /* default transaction */ }
          const e: any = new Error(`Portal 400: unknown field ${fn} in ${table}`); e.unsupportedField = fn; e.unsupportedFieldTable = table; throw e;
        }
        // a dataset that doesn't begin at genesis (e.g. TAC starts at block 1) 400s when queried
        // below its first block. Surface the start so stream() can clamp the cursor forward.
        const s = res.status === 400 && text.match(/dataset starts (?:from|at) block (\d+)/i);
        if (s) { const e: any = new Error(`Portal 400: dataset starts at block ${s[1]}`); e.datasetStartsAt = Number(s[1]); throw e; }
        // a dense range (many child addresses × many event topics × wide chunk) can exceed the
        // Portal's per-query size/work estimate → 400 "Query is too large". Signal stream() to
        // bisect the block range and retry (adaptive; no client tuning).
        if (res.status === 400 && /query is too large/i.test(text)) { const e: any = new Error(`Portal 400: query too large @ ${cursor}`); e.tooLarge = true; throw e; }
        throw new Error(`Portal ${res.status} @ ${cursor}: ${text}`);
      }
      const reader = res.body!.getReader();
      const dec = new TextDecoder();
      let buf = "", last = cursor;
      const blocks: { header: RawHeader; logs?: any[]; transactions?: any[]; traces?: any[] }[] = [];
      const onLine = (line: string) => { if (!line) return; const b = JSON.parse(line); blocks.push(b); if (b.header?.number > last) last = b.header.number; };
      for (;;) { const { done, value } = await reader.read(); if (done) break; stats.bytes += value.byteLength; buf += dec.decode(value, { stream: true }); let nl: number; while ((nl = buf.indexOf("\n")) >= 0) { onLine(buf.slice(0, nl)); buf = buf.slice(nl + 1); } }
      buf += dec.decode(); if (buf) onLine(buf);
      portalGate.onOk(); // clean full response → a generation of these ramps concurrency up
      return { blocks, last };
    } catch (err: any) {
      if (isNetworkError(err)) portalGate.onThrottle(); // dropped/timed-out connections under load = congestion
      throw err;
    } finally { stats.fetchMs += Date.now() - tFetch; portalGate.release(); stats.inflight--; }
  }

  // Fields the TARGET dataset doesn't have (per-dataset schema varies — e.g. Monad's transactions
  // have no accessList). Discovered from a "column not found" 400, then stripped from every request
  // so the fork degrades gracefully instead of crashing. Keyed "<fieldsKey>.<field>".
  // Portal reports a missing COLUMN in a plural TABLE; map back to the field key we requested.
  const TABLE_TO_KEY: Record<string, string> = { transactions: "transaction", blocks: "block", logs: "log", traces: "trace" };
  const COL_SPECIAL: Record<string, string> = { access_list_size: "accessList", access_list: "accessList" }; // portal's derived column ≠ snake(field)
  const colToFieldKey = (col: string, table: string): string => {
    const key = TABLE_TO_KEY[table] ?? table;
    const field = COL_SPECIAL[col] ?? col.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase()); // snake_case → camelCase
    return `${key}.${field}`;
  };
  const stripFields = (q: any, dropped: Set<string>): any => {
    if (dropped.size === 0 || !q.fields) return q;
    const fields = JSON.parse(JSON.stringify(q.fields));
    for (const tf of dropped) { const i = tf.indexOf("."); const t = tf.slice(0, i), f = tf.slice(i + 1); if (fields[t]) delete fields[t][f]; }
    return { ...q, fields };
  };

  // PER-STREAM (per block-range) field degradation. A dataset can lack a column on only SOME
  // (e.g. old) chunks, so drops are LOCAL — a chunk that has the column keeps it. When a NEEDED
  // field is missing we DON'T crash here (this range might be event-less/irrelevant); we drop it to
  // fetch, and record it in `neededMissing` so the caller can crash ONLY IF the range yields matched
  // data (see dataChunk). Unused nullable fields are dropped silently.
  async function* stream(query: object, from: number, to: number, neededMissing?: Set<string>) {
    let cursor = from;
    const dropped = new Set<string>(), triedCols = new Set<string>();
    while (cursor <= to) {
      let attempt = 0;
      let batch: Awaited<ReturnType<typeof fetchBatch>> | undefined;
      while (batch === undefined) {
        const body = JSON.stringify({ ...stripFields(query, dropped), fromBlock: cursor, toBlock: to });
        try { batch = await fetchBatch(body, cursor); }
        catch (err: any) {
          if (err?.tooLarge) {
            // Portal caps request BYTES (MAX_RAW_QUERY_SIZE), not range — so bisecting blocks can't
            // help. mergeLogRequests already de-dups addresses across event filters; if a body still
            // exceeds the cap the address batch itself is too big → fail loud with the actual lever.
            throw new Error(`Portal query body exceeds MAX_RAW_QUERY_SIZE even after merging event filters — lower PORTAL_MAX_ADDRESSES (currently ${PORTAL_MAX_ADDRESSES}) to shrink the address batch. @ ${cursor}`);
          }
          if (err?.datasetStartsAt !== undefined) {
            // dataset begins after this chunk's start (doesn't reach genesis) → skip the missing
            // prefix. If the whole chunk precedes the dataset, there's nothing to fetch here.
            if (err.datasetStartsAt > to) return;
            if (err.datasetStartsAt > cursor) { cursor = err.datasetStartsAt; continue; }
            throw err; // start ≤ cursor yet still 400 ⇒ not a below-start issue; surface it
          }
          // a dataset that can't serve a requested field — either the column is absent from the
          // parquet ("column not found") or the schema doesn't know the field ("unknown field").
          // Both are handled the same way: drop that field for this chunk and retry.
          if (err?.unsupportedColumn || err?.unsupportedField) {
            const tag = (err.unsupportedColumn ?? err.unsupportedField) as string; // unique id → bounds retries
            if (triedCols.has(tag)) throw err; // dropping its field didn't help → real error
            triedCols.add(tag);
            const field = err.unsupportedColumn ? colToFieldKey(err.unsupportedColumn, err.unsupportedTable) : `${err.unsupportedFieldTable}.${err.unsupportedField}`;
            dropped.add(field); // drop for THIS chunk's retries only (chunks that have it keep it)
            // non-load-bearing nullable field → drop silently; anything else → crash IF matched (dataChunk).
            if (!DROPPABLE_FIELDS.has(field)) neededMissing?.add(`${field} (${tag})`);
            else log.debug({ service: "portal", msg: `Portal ${args.chain.name} [${from},${to}]: dataset can't serve '${tag}' → skipping non-load-bearing field ${field}` });
            continue;
          }
          const retryable = err?.retryAfterMs !== undefined || isNetworkError(err);
          if (!retryable || attempt++ >= 10) throw err;
          stats.errors++; stats.retries++;
          await sleep(err?.retryAfterMs !== undefined ? Math.min(err.retryAfterMs, 30_000) : Math.min(500 * 2 ** attempt, 30_000));
        }
      }
      if (batch === "done") return;
      yield batch.blocks;
      if (batch.last < cursor) throw new Error(`Portal no progress @ ${cursor}`);
      cursor = batch.last + 1;
    }
  }

  function logRequestsFor(filter: LogFilter): PortalLogRequest[] {
    const base: PortalLogRequest = {};
    if (filter.topic0) base.topic0 = asArr(filter.topic0);
    if (filter.topic1) base.topic1 = asArr(filter.topic1 as any);
    if (filter.topic2) base.topic2 = asArr(filter.topic2 as any);
    if (filter.topic3) base.topic3 = asArr(filter.topic3 as any);
    let addresses: Address[] | undefined;
    if (isAddressFactory(filter.address)) {
      addresses = Array.from(args.childAddresses.get(filter.address.id)?.keys() ?? []);
      if (addresses.length === 0) return [];
    } else if (filter.address === undefined) return [base];
    else addresses = (Array.isArray(filter.address) ? filter.address : [filter.address]).map((a) => a.toLowerCase() as Address);
    const out: PortalLogRequest[] = [];
    for (let i = 0; i < addresses.length; i += PORTAL_MAX_ADDRESSES) out.push({ ...base, address: addresses.slice(i, i + PORTAL_MAX_ADDRESSES) });
    return out;
  }

  // Ponder emits ONE filter per event, so an N-event contract (e.g. the 24-event EVault) produces N
  // log requests that each repeat the SAME (possibly large) child-address list with a different
  // topic0. Concatenated into one query body they can exceed the Portal's raw query-size limit
  // (400 "Query is too large" — it caps request BYTES, not range). Collapse requests that share the
  // same address set + topic1..3 into one, unioning topic0 — identical result set, ~N× smaller body.
  function mergeLogRequests(reqs: PortalLogRequest[]): PortalLogRequest[] {
    const groups = new Map<string, PortalLogRequest>();
    for (const r of reqs) {
      const key = JSON.stringify([r.address ? [...r.address].sort() : null, r.topic1 ?? null, r.topic2 ?? null, r.topic3 ?? null]);
      const g = groups.get(key);
      if (!g) { groups.set(key, { ...r, topic0: r.topic0 ? [...new Set(r.topic0)] : undefined }); continue; }
      if (g.topic0 === undefined || r.topic0 === undefined) g.topic0 = undefined; // one wants ALL topic0 → keep the broadest
      else { const s = new Set(g.topic0); for (const t of r.topic0) s.add(t); g.topic0 = [...s]; }
    }
    return [...groups.values()];
  }

  // FILTER/PROJECTION STRATEGY (max Portal leverage): every row filter is pushed to
  // Portal's native server-side filters — logs by address+topics (logRequestsFor),
  // traces by callTo/callFrom/callSighash (tracePortalRequests), account txs by from/to
  // (txPortalRequests). Field projection below requests exactly the columns the sync
  // store persists and no more. The only client-side row filter is block-interval
  // (Portal has no modulo filter), and receipt fields are added only on demand.
  const REQUIRED_BLOCK_FIELDS = ["number", "hash", "parentHash", "timestamp", "logsBloom", "miner", "gasUsed", "gasLimit", "stateRoot", "receiptsRoot", "transactionsRoot", "size", "difficulty", "extraData"];
  const NULLABLE_BLOCK_FIELDS = ["baseFeePerGas", "nonce", "mixHash", "sha3Uncles", "totalDifficulty"];
  const LOG_FIELDS = { address: true, topics: true, data: true, transactionHash: true, transactionIndex: true, logIndex: true };
  // Ponder's event profiler probes event.transaction.hash, so we pull each matched
  // log's parent transaction (Portal `transaction` relation) and store it.
  const TX_FIELDS = { transactionIndex: true, hash: true, from: true, to: true, input: true, value: true, nonce: true, gas: true, gasPrice: true, maxFeePerGas: true, maxPriorityFeePerGas: true, type: true, r: true, s: true, v: true, yParity: true, accessList: true };
  // receipt fields ride on Portal's transaction object (no separate receipt entity)
  const RECEIPT_FIELDS = { status: true, cumulativeGasUsed: true, effectiveGasPrice: true, gasUsed: true, contractAddress: true, logsBloom: true };
  let needReceipts = false; // set from filters on first syncBlockRangeData (stable per chain)
  // trace fields: request both flattened selectors (some Portal builds) AND rely on
  // nested action/result in the response — the transform reads whichever is present.
  const TRACE_FIELDS = {
    transactionIndex: true, traceAddress: true, type: true, subtraces: true, error: true, revertReason: true,
    callFrom: true, callTo: true, callValue: true, callGas: true, callInput: true, callSighash: true, callCallType: true, callResultGasUsed: true, callResultOutput: true,
    createFrom: true, createValue: true, createGas: true, createInit: true, createResultGasUsed: true, createResultCode: true, createResultAddress: true,
    suicideAddress: true, suicideRefundAddress: true, suicideBalance: true,
  };
  let needTraces = false;
  let traceFilters: any[] = [];
  let transferFilters: any[] = [];
  let needBlocks = false;
  let blockFilters: any[] = [];
  let needTxFilter = false;
  let transactionFilters: any[] = [];
  let logFilters: LogFilter[] = [];
  let allFactories: any[] = [];
  let backfillStartBlock = 0;
  let backfillEndBlock: number | undefined; // undefined ⇒ unbounded (backfill to the finalized head)
  // Fields that are NULLABLE in Ponder's sync-store AND non-load-bearing — Ponder never uses them
  // internally and they're legitimately absent on some chains (accessList on non-typed txs; nonce/
  // mixHash on PoS; baseFeePerGas pre-1559; totalDifficulty post-merge). Safe to store as null when a
  // dataset lacks them. NOTE: Ponder's per-filter `include` is a STATIC default that always lists
  // EVERY standard field incl. accessList (runtime/filter.ts defaultTransactionInclude), so it can't
  // tell us what a handler actually reads — we classify by field. Anything NOT here, missing ⇒ crash
  // (a NOT-NULL / bloom-load-bearing / core column whose absence would corrupt or silently gut data).
  const DROPPABLE_FIELDS = new Set(["transaction.accessList", "block.baseFeePerGas", "block.nonce", "block.mixHash", "block.sha3Uncles", "block.totalDifficulty"]);
  // Resolve the COMPLETE chain-wide fetch-spec ONCE from args.eventCallbacks (the FULL per-chain
  // filter set), NOT from per-call requiredIntervals (only the subset Ponder still needs, which
  // shrinks as fragments cache). Chunks are cached by idx ALONE, so every chunk MUST be filter-
  // complete — else a filter that first needs an already-cached chunk is never streamed, yet its
  // interval is marked done → permanent silent gap. (C1)
  let specReady = false;
  const initSpec = () => {
    if (specReady) return;
    specReady = true;
    const fs = (args.eventCallbacks ?? []).map((e) => e.filter);
    logFilters = fs.filter((f) => f.type === "log") as LogFilter[];
    allFactories = [...new Map(fs.flatMap(getFilterFactories).map((f: any) => [f.id, f])).values()];
    needReceipts = fs.some((f) => (f as any).hasTransactionReceipt === true);
    blockFilters = fs.filter((f) => f.type === "block"); needBlocks = blockFilters.length > 0;
    transactionFilters = fs.filter((f) => f.type === "transaction"); needTxFilter = transactionFilters.length > 0;
    traceFilters = fs.filter((f) => f.type === "trace");
    transferFilters = fs.filter((f) => f.type === "transfer");
    needTraces = traceFilters.length + transferFilters.length > 0;
    // the chain's actual backfill window, from the filters — used to bound chunk fetches so a
    // bounded backfill (or the backfill tail) never over-fetches. Fully automatic; no client tuning.
    const froms = fs.map((f) => (f as any).fromBlock).filter((b) => b != null);
    backfillStartBlock = froms.length ? Math.min(...froms) : 0;
    const tos = fs.map((f) => (f as any).toBlock);
    backfillEndBlock = tos.length && tos.every((t) => t != null) ? Math.max(...tos) : undefined;
  };
  // a chunk's grid-aligned [from,to] clamped to the real backfill window (end ⇒ explicit toBlock,
  // else the finalized head). Bounds fetch on BOTH sides so a small/bounded range isn't widened to
  // the 500k grid — the reason the diff harness needs no PORTAL_CHUNK_* tuning.
  const chunkRange = (idx: number): [number, number] => {
    const end = backfillEndBlock ?? portalHead ?? Number.POSITIVE_INFINITY;
    return [Math.max(idx * chunkBlocks, backfillStartBlock), Math.min(idx * chunkBlocks + chunkBlocks - 1, end)];
  };
  const blockFieldsFor = (filters: Filter[]): Record<string, boolean> => {
    const inc = new Set<string>();
    for (const f of filters) for (const i of f.include ?? []) if (i.startsWith("block.")) inc.add(i.slice(6));
    const fields: Record<string, boolean> = {};
    for (const k of REQUIRED_BLOCK_FIELDS) fields[k] = true;
    // always fetch the nullable header fields too — they're cheap and keep stored blocks
    // byte-identical with the RPC path (which always has nonce/mixHash/sha3Uncles/totalDifficulty).
    for (const k of NULLABLE_BLOCK_FIELDS) fields[k] = true;
    void inc;
    return fields;
  };

  // ---- discovery: wide factory scan over [factoryStart, head], split into PARALLEL disjoint windows ----
  // A factory scan can't be pruned (logs are block-ordered, the address is scattered), so its cost is
  // ~the log volume of the range and irreducible — but fully parallelizable. The Portal serializes ONE
  // stream in block order (a slow front chunk truncates it), so parallelism comes from issuing DISJOINT
  // windows concurrently; each stream additionally fans out `buffer_size` chunk-workers. A single
  // sequential [0,head] scan was the slow start; N concurrent windows divide the wall-clock by N.
  function ensureDiscoveredThrough(idx: number, factories: any[]): Promise<unknown> {
    if (factories.length === 0 || discStartIdx === undefined) return Promise.resolve();
    const need = chunkRange(idx)[1];
    if (need <= discoveredThrough) return discoveryP; // already scanned this far
    const from = discoveredThrough < 0 ? discStartIdx * chunkBlocks : discoveredThrough + 1;
    const to = Math.max(need, backfillEndBlock ?? portalHead ?? need); // reach as far as the backfill will need — usually the whole span at once
    discoveredThrough = to;
    const earlier = discoveryP;
    discoveryP = (async () => {
      await earlier; // serialize extensions so children accumulate deterministically
      const span = to - from + 1;
      const P = Math.max(1, Math.min(DISCOVERY_WINDOWS, Math.ceil(span / chunkBlocks)));
      const w = Math.ceil(span / P);
      const windows: [number, number][] = [];
      for (let i = 0; i < P; i++) { const lo = from + i * w; if (lo > to) break; windows.push([lo, Math.min(to, lo + w - 1)]); }
      stats.discChunks += windows.length;
      await Promise.all(windows.map(async ([lo, hi]) => {
        for (const factory of factories) {
          const needsData = factory.childAddressLocation.startsWith("offset");
          const q = { type: "evm", fields: { block: { number: true }, log: { address: true, topics: true, data: needsData } }, logs: [{ address: factory.address ? (Array.isArray(factory.address) ? factory.address : [factory.address]).map((addr: string) => addr.toLowerCase()) : undefined, topic0: [factory.eventSelector.toLowerCase()] }] };
          const rec = args.childAddresses.get(factory.id)!;
          for await (const blocks of stream(q, lo, hi)) {
            for (const bl of blocks) for (const raw of bl.logs ?? []) {
              const sl = { address: (raw.address as string)?.toLowerCase(), topics: raw.topics ?? [], data: raw.data ?? "0x", blockNumber: hx(bl.header.number) } as unknown as SyncLog;
              if (isLogFactoryMatched({ factory, log: sl })) {
                const child = getChildAddress({ log: sl, factory }).toLowerCase() as Address;
                const bn = bl.header.number; const prevBn = rec.get(child);
                if (prevBn === undefined || prevBn > bn) rec.set(child, bn);
              }
            }
          }
        }
      }));
    })();
    return discoveryP;
  }

  // ---- data chunk: gated on discovery-through-this-chunk, then ONE big data stream ----
  function dataChunk(idx: number, factories: any[], filters: LogFilter[]): Promise<ChunkData> {
    let p = dataCache.get(idx);
    if (p) { stats.cacheHits++; return p; }
    p = (async () => {
      await ensureDiscoveredThrough(idx, factories); // correctness: children ≤ this chunk are known
      stats.dataChunks++;
      const [from, to] = chunkRange(idx);
      const logRequests = mergeLogRequests(filters.flatMap((f) => logRequestsFor(f))).map((r) => ({ ...r, transaction: true }));
      const data: ChunkData = { headers: new Map(), logs: new Map(), txs: new Map(), traceBlocks: new Map(), blockHeaders: new Map(), txBlocks: new Map() };
      const neededMissing = new Set<string>(); // needed fields the dataset lacked on THIS chunk
      if (logRequests.length > 0) {
        const q = { type: "evm", fields: { block: blockFieldsFor(filters), log: LOG_FIELDS, transaction: needReceipts ? { ...TX_FIELDS, ...RECEIPT_FIELDS } : TX_FIELDS }, logs: logRequests };
        for await (const blocks of stream(q, from, to, neededMissing)) {
          for (const b of blocks) if (b.logs?.length) {
            data.headers.set(b.header.number, b.header);
            data.logs.set(b.header.number, (data.logs.get(b.header.number) ?? []).concat(b.logs));
            if (b.transactions?.length) data.txs.set(b.header.number, (data.txs.get(b.header.number) ?? []).concat(b.transactions));
            stats.logs += b.logs.length;
          }
        }
      }
      if (needTraces) {
        const tq = { type: "evm", fields: { block: blockFieldsFor(filters), trace: TRACE_FIELDS, transaction: needReceipts ? { ...TX_FIELDS, ...RECEIPT_FIELDS } : TX_FIELDS }, traces: tracePortalRequests() };
        for await (const blocks of stream(tq, from, to, neededMissing)) {
          for (const b of blocks) if (b.traces?.length) {
            const ex = data.traceBlocks.get(b.header.number);
            if (ex) { ex.traces.push(...b.traces); if (b.transactions) ex.txs.push(...b.transactions); }
            else data.traceBlocks.set(b.header.number, { header: b.header, traces: b.traces, txs: b.transactions ?? [] });
          }
        }
      }
      // block-interval sources: includeAllBlocks range-scan (Portal has no modulo filter),
      // keep only headers matching a BlockFilter's interval/offset.
      if (needBlocks) {
        const bq = { type: "evm", includeAllBlocks: true, fields: { block: blockFieldsFor(blockFilters) } };
        for await (const blocks of stream(bq, from, to, neededMissing)) {
          for (const b of blocks) {
            const bn = b.header.number;
            if (blockFilters.some((f) => isBlockFilterMatched({ filter: f, block: { number: BigInt(bn) } }))) data.blockHeaders.set(bn, b.header);
          }
        }
      }
      // account transaction sources: Portal transactions[] from/to filter pushed server-side
      if (needTxFilter) {
        const txReqs = txPortalRequests();
        if (txReqs.length) {
          const tq = { type: "evm", fields: { block: blockFieldsFor(transactionFilters), transaction: needReceipts ? { ...TX_FIELDS, ...RECEIPT_FIELDS } : TX_FIELDS }, transactions: txReqs };
          for await (const blocks of stream(tq, from, to, neededMissing)) {
            for (const b of blocks) if (b.transactions?.length) {
              const ex = data.txBlocks.get(b.header.number);
              if (ex) ex.txs.push(...b.transactions);
              else data.txBlocks.set(b.header.number, { header: b.header, txs: b.transactions });
            }
          }
        }
      }
      // The dataset lacked a NEEDED field on THIS chunk. Crash ONLY IF the chunk yielded MATCHED
      // data — an event the indexer processes would be incomplete. If the chunk is event-less
      // (old/irrelevant range), the gap is harmless, so proceed. (silent bug ≫ crash, but only
      // when it actually affects the indexer's data.)
      if (neededMissing.size && (data.logs.size || data.traceBlocks.size || data.txBlocks.size || data.blockHeaders.size)) {
        throw new Error(`Portal dataset for ${args.chain.name} is missing [${[...neededMissing].join(", ")}] on blocks [${from},${to}], which contain matched data your indexer needs — a Portal dataset-completeness gap. Failing fast rather than serving incomplete data; report the gap to SQD, or start your indexer past the affected range.`);
      }
      // register this chunk's buffered size with the GLOBAL memory budget (freed when evicted).
      let rc = data.blockHeaders.size;
      for (const a of data.logs.values()) rc += a.length;
      for (const a of data.txs.values()) rc += a.length;
      for (const b of data.traceBlocks.values()) rc += b.traces.length + b.txs.length;
      for (const b of data.txBlocks.values()) rc += b.txs.length;
      chunkRows.set(idx, rc); portalGate.addRows(rc);
      return data;
    })();
    dataCache.set(idx, p);
    return p;
  }


  const factoryAddrOk = (filterAddr: any, addr: string | undefined, bn: number): boolean =>
    !isAddressFactory(filterAddr) || isAddressMatched({ address: addr as Address, blockNumber: bn, childAddresses: args.childAddresses.get(filterAddr.id)! });
  const traceMatched = (frame: any, bn: number): boolean => {
    const blk = { number: BigInt(bn) } as any;
    for (const f of transferFilters) if (isTransferFilterMatched({ filter: f, trace: frame, block: blk }) && factoryAddrOk(f.fromAddress, frame.from, bn) && factoryAddrOk(f.toAddress, frame.to, bn)) return true;
    for (const f of traceFilters) if (isTraceFilterMatched({ filter: f, trace: frame, block: blk }) && factoryAddrOk(f.fromAddress, frame.from, bn) && factoryAddrOk(f.toAddress, frame.to, bn)) return true;
    return false;
  };
  const buildTraces = (cd: ChunkData, lo: number, hi: number): { trace: SyncTrace; block: SyncBlock; transaction: SyncTransaction }[] => {
    const out: { trace: SyncTrace; block: SyncBlock; transaction: SyncTransaction }[] = [];
    for (const [bn, tb] of cd.traceBlocks) {
      if (bn < lo || bn > hi || !tb.traces?.length) continue;
      const block = toSyncBlockHeader(tb.header) as unknown as SyncBlock; // encodeTrace only reads block.number
      const txByIdx = new Map<number, any>();
      for (const tx of tb.txs ?? []) txByIdx.set(tx.transactionIndex, tx);
      const byTx = new Map<number, any[]>();
      // callTracer has no block-reward frames; skip reward/no-tx traces so `?? 0` can't fold them
      // into tx 0 and shift its DFS ranks (now that we fetch the full, unfiltered trace set).
      for (const t of tb.traces) { if (t.transactionIndex == null || t.type === "reward") continue; const k = t.transactionIndex; if (!byTx.has(k)) byTx.set(k, []); byTx.get(k)!.push(t); }
      for (const [txIndex, traces] of byTx) {
        traces.sort((x, y) => cmpTraceAddr(x.traceAddress ?? [], y.traceAddress ?? []));
        const rawTx = txByIdx.get(txIndex);
        traces.forEach((t, i) => {
          const frame = parityToCallFrame(t, i);
          if (!frame || !traceMatched(frame, bn)) return;
          out.push({ trace: { trace: frame, transactionHash: rawTx?.hash } as unknown as SyncTrace, block, transaction: rawTx ? toSyncTransaction(rawTx, tb.header) : ({ transactionIndex: hx(txIndex) } as unknown as SyncTransaction) });
        });
      }
    }
    return out;
  };
  // Trace-index parity with the RPC path: Ponder assigns `trace_index` as the PRE-ORDER DFS rank
  // over each tx's FULL call tree (rpc/actions.ts dfs(), which numbers EVERY frame, THEN filters —
  // so a matched trace keeps its full-tree position). Pushing callTo/callFrom/callSighash would make
  // Portal return only the matched SUBSET, so buildTraces' per-tx rank would be filter-local (a lone
  // deep match → 0) instead of its true position (e.g. 7). So fetch EVERY trace and let buildTraces
  // client-filter (traceMatched) AFTER ranking. Covers trace AND transfer sources. The cost is real
  // (no server-side trace filter → ~all traces of the chunk); bounded by PORTAL_TRACE_CHUNK_BLOCKS.
  const tracePortalRequests = (): any[] => [{ transaction: true }];
  // push account TransactionFilters (from/to) to Portal's transactions[] (server-side row filter)
  const txPortalRequests = (): any[] => {
    const reqs: any[] = [];
    const addrsOf = (a: any): string[] | undefined => {
      if (a === undefined) return undefined;
      if (isAddressFactory(a)) return Array.from(args.childAddresses.get(a.id)?.keys() ?? []);
      return (Array.isArray(a) ? a : [a]).map((x: string) => x.toLowerCase());
    };
    for (const f of transactionFilters) {
      const req: any = {};
      const from = addrsOf(f.fromAddress); if (from?.length) req.from = from;
      const to = addrsOf(f.toAddress); if (to?.length) req.to = to;
      if (req.from || req.to) reqs.push(req); // skip match-all (never fetch every tx)
    }
    return reqs;
  };

  return {
    async syncBlockRangeData(params) {
      const { interval, requiredFactoryIntervals, syncStore } = params;
      if (!startTime) startTime = Date.now();
      // finality gap: if this interval reaches past Portal's finalized head, re-confirm
      // (Portal advances) and, if still beyond, delegate the whole interval to RPC.
      if (portalHead === undefined) await refreshPortalHead();
      // C3: head UNKNOWN (probe persistently failing) OR interval past the head → don't risk silently
      // under-serving the tip from Portal (it would 204 the missing tail and mark it synced).
      // Re-confirm (Portal advances), then delegate the whole interval to the authoritative RPC.
      if (portalHead === undefined || isFinalityGap(interval[1], portalHead)) {
        await refreshPortalHead();
        if (portalHead === undefined || isFinalityGap(interval[1], portalHead)) {
          // Stream-realtime mode: do NOT delegate to RPC — the Portal `/stream` covers [portal-head → tip].
          // With clampFinalizedToPortalHead this branch is unreachable (finalized ≤ portal-head), so it only
          // fires if the head probe is failing (portalHead === undefined) — a loud degradation, not silent.
          if (STREAM_REALTIME) {
            log.warn({ service: "portal", msg: `Portal ${args.chain.name} [${interval[0]},${interval[1]}] past/unknown finalized head in stream mode → RPC fallback suppressed (realtime /stream covers the gap)` });
            return [];
          }
          delegated.add(ikey(interval)); stats.rpcFallback++;
          log.debug({ service: "portal", msg: `Portal ${args.chain.name} [${interval[0]},${interval[1]}] ${portalHead === undefined ? "head unknown" : `past finalized head ${portalHead}`} → RPC fallback` });
          return rpcFallback().syncBlockRangeData(params);
        }
      }
      await ensureChunkSize(); // scale chunk to chain block-density before any idxOf()
      initSpec(); // freeze the COMPLETE filter/factory set once → every cached chunk is filter-complete (C1)
      const filters = logFilters;
      const factories = allFactories;
      // cap the chunk grid BEFORE any idxOf() for DENSE sources (traces fetch every trace;
      // block sources includeAllBlocks-scan the WHOLE chunk range) — bounds memory + overfetch.
      const capped = traceSafeChunkBlocks(chunkBlocks, needTraces || needBlocks);
      if (capped !== chunkBlocks) {
        chunkBlocks = capped; dataCache.clear(); for (const r of chunkRows.values()) portalGate.freeRows(r); chunkRows.clear(); discStartIdx = undefined; discoveredThrough = -1; discoveryP = Promise.resolve();
        log.debug({ service: "portal", msg: `Portal ${args.chain.name}: dense sources → chunkBlocks capped to ${chunkBlocks} (grid reset)` });
      }

      // pin the discovery floor at the factory's real start (NOT block 0), after any chunk cap.
      // C4: clamp DOWNWARD only — a later call whose required factory interval starts earlier must
      // LOWER the floor, never stay latched too high (which would skip early child discovery).
      if (requiredFactoryIntervals.length > 0) {
        const floor = idxOf(Math.min(...requiredFactoryIntervals.map((r) => r.interval[0]).concat(interval[0])));
        discStartIdx = discStartIdx === undefined ? floor : Math.min(discStartIdx, floor);
      }

      const startIdx = idxOf(interval[0]), endIdx = idxOf(interval[1]);
      const idxs: number[] = [];
      for (let i = startIdx; i <= endIdx; i++) idxs.push(i);
      const data = await Promise.all(idxs.map((i) => dataChunk(i, factories, filters)));
      // PARALLEL read-ahead: prefetch the next chunks concurrently — but never past the backfill end
      // (bounded toBlock or finalized head), so the tail doesn't waste CU / hit 204s. Depth is bounded
      // by the GLOBAL memory budget, not a fixed count: always prefetch lead-1 (so this chain's next
      // chunk is ready and indexing never awaits a fetch), and go deeper only while the shared buffer
      // isn't saturated — so a fast Portal keeps every chain fed while total memory stays capped.
      const raEnd = backfillEndBlock ?? portalHead ?? Number.POSITIVE_INFINITY;
      for (let d = 1; d <= READAHEAD; d++) { if ((endIdx + d) * chunkBlocks > raEnd) break; if (d > 1 && portalGate.saturated()) break; void dataChunk(endIdx + d, factories, filters).catch(() => {}); }

      const tXform = Date.now(); // decode/transform time: Portal NDJSON → Ponder Sync* shapes
      const syncLogs: SyncLog[] = [];
      const blocksByNumber = new Map<number, SyncBlockHeader>();
      const syncTxs: SyncTransaction[] = [];
      const syncReceipts: SyncTransactionReceipt[] = [];
      const seenTx = new Set<string>();
      for (const cd of data) for (const [bn, hdr] of cd.headers) {
        if (bn < interval[0] || bn > interval[1]) continue;
        const logs = cd.logs.get(bn) ?? [];
        if (logs.length) {
          blocksByNumber.set(bn, toSyncBlockHeader(hdr));
          for (const raw of logs) syncLogs.push(toSyncLog(raw, hdr));
          for (const tx of cd.txs.get(bn) ?? []) if (!seenTx.has(tx.hash)) {
            seenTx.add(tx.hash);
            syncTxs.push(toSyncTransaction(tx, hdr));
            if (needReceipts) syncReceipts.push(toSyncReceipt(tx, hdr));
          }
        }
      }
      // block-interval sources: ensure each matched block is in the blocks table
      if (needBlocks) for (const cd of data) for (const [bn, hdr] of cd.blockHeaders) {
        if (bn >= interval[0] && bn <= interval[1] && !blocksByNumber.has(bn)) blocksByNumber.set(bn, toSyncBlockHeader(hdr));
      }
      // account transaction sources: re-match Portal's from/to-filtered txs (+ factory + range), insert tx/receipt/block
      if (needTxFilter) for (const cd of data) for (const [bn, tb] of cd.txBlocks) {
        if (bn < interval[0] || bn > interval[1]) continue;
        for (const raw of tb.txs) {
          if (seenTx.has(raw.hash)) continue;
          const tx = toSyncTransaction(raw, tb.header);
          if (!transactionFilters.some((f) => isTransactionFilterMatched({ filter: f, transaction: tx }) && factoryAddrOk(f.fromAddress, tx.from, bn) && factoryAddrOk(f.toAddress, (tx.to ?? undefined) as any, bn))) continue;
          seenTx.add(raw.hash);
          blocksByNumber.set(bn, toSyncBlockHeader(tb.header));
          syncTxs.push(tx);
          if (needReceipts) syncReceipts.push(toSyncReceipt(raw, tb.header));
        }
      }
      for (const i of dataCache.keys()) if ((i + 1) * chunkBlocks <= interval[0]) { dataCache.delete(i); portalGate.freeRows(chunkRows.get(i) ?? 0); chunkRows.delete(i); } // evict behind + free its memory budget

      const syncTraces = needTraces ? data.flatMap((cd) => buildTraces(cd, interval[0], interval[1])) : [];
      stats.transformMs += Date.now() - tXform;

      // C9: highest block with data — a loop, NOT Math.max(...spread) which RangeErrors on ~100k+
      // keys — and INCLUDING trace-only blocks (a block with only matched traces isn't in
      // blocksByNumber) so `closest` doesn't understate the synced tip.
      let closest: SyncBlock | undefined;
      let maxBn = -1;
      for (const [bn, hdr] of blocksByNumber) if (bn > maxBn) { maxBn = bn; closest = hdr as unknown as SyncBlock; }
      for (const t of syncTraces) { const bn = Number((t.block as any).number); if (bn > maxBn) { maxBn = bn; closest = t.block; } }
      await syncStore.insertLogs({ logs: syncLogs, chainId: args.chain.id });
      stash.set(ikey(interval), { blocks: [...blocksByNumber.values()], txs: syncTxs, receipts: syncReceipts, traces: syncTraces, closest });

      log.debug({ service: "portal", msg: `Portal ${args.chain.name} [${interval[0]},${interval[1]}]: ${syncLogs.length} logs (dataChunks=${stats.dataChunks} discChunks=${stats.discChunks} http=${stats.http} hits=${stats.cacheHits} inflight=${stats.maxInflight} err=${stats.errors})` });
      return syncLogs;
    },

    async syncBlockData(params) {
      const { interval, syncStore } = params;
      if (delegated.has(ikey(interval))) { delegated.delete(ikey(interval)); return rpcFallback().syncBlockData(params); }
      const s = stash.get(ikey(interval));
      stash.delete(ikey(interval));
      if (!s) return undefined;
      const chainId = args.chain.id;
      // merge log blocks/txs with trace blocks/txs (a trace-only block isn't in the log set)
      const blocks = new Map<string, SyncBlockHeader>();
      for (const b of s.blocks) blocks.set(b.number as unknown as string, b);
      const txs = new Map<string, SyncTransaction>();
      for (const t of s.txs) txs.set(t.hash as unknown as string, t);
      for (const { block, transaction } of s.traces) {
        blocks.set((block as any).number, block as unknown as SyncBlockHeader);
        if ((transaction as any)?.hash) txs.set((transaction as any).hash, transaction);
      }
      const blockArr = [...blocks.values()];
      if (blockArr.length === 0) return s.closest;
      await syncStore.insertBlocks({ blocks: blockArr, chainId });
      if (txs.size) await syncStore.insertTransactions({ transactions: [...txs.values()], chainId });
      if (s.receipts.length) await syncStore.insertTransactionReceipts({ transactionReceipts: s.receipts, chainId });
      if (s.traces.length) await syncStore.insertTraces({ traces: s.traces, chainId });
      stats.blocks += blockArr.length; stats.txs += txs.size; stats.receipts += s.receipts.length; stats.traces += s.traces.length;
      writeMetrics();
      return s.closest;
    },
  };
};
