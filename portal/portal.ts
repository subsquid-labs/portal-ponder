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
const CHUNK_BLOCKS = Number(process.env.PORTAL_CHUNK_BLOCKS ?? 500_000);
const READAHEAD = Number(process.env.PORTAL_READAHEAD ?? 6);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

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

  const stats = { dataChunks: 0, discChunks: 0, http: 0, logs: 0, errors: 0, retries: 0, bytes: 0, cacheHits: 0, inflight: 0, maxInflight: 0, blocks: 0, txs: 0, receipts: 0, traces: 0, rpcFallback: 0 };
  const dataCache = new Map<number, Promise<ChunkData>>(); // keyed by chunk index
  const discCache = new Map<number, Promise<void>>(); // keyed by chunk index
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
  const refreshPortalHead = async (): Promise<number> => {
    if (process.env.PORTAL_FINALIZED_HEAD) return (portalHead = Number(process.env.PORTAL_FINALIZED_HEAD));
    try { const h = await fetch(`${portalUrl}/finalized-head`, { headers: baseHeaders }).then((r) => r.json()); if (typeof h?.number === "number") portalHead = h.number; } catch { /* keep prior */ }
    return portalHead ?? Number.POSITIVE_INFINITY;
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
    stats.inflight++; stats.maxInflight = Math.max(stats.maxInflight, stats.inflight);
    try {
      const res = await fetch(`${portalUrl}/finalized-stream`, { method: "POST", headers: baseHeaders, body });
      stats.http++;
      if (res.status === 204) return "done";
      if (res.status === 503 || res.status === 529 || res.status === 429) {
        await res.body?.cancel().catch(() => {});
        const ra = Number(res.headers.get("retry-after"));
        const e: any = new Error(`Portal ${res.status}`); e.retryAfterMs = Number.isFinite(ra) ? ra * 1000 : undefined;
        throw e;
      }
      if (!res.ok) throw new Error(`Portal ${res.status} @ ${cursor}: ${(await res.text()).slice(0, 200)}`);
      const reader = res.body!.getReader();
      const dec = new TextDecoder();
      let buf = "", last = cursor;
      const blocks: { header: RawHeader; logs?: any[]; transactions?: any[]; traces?: any[] }[] = [];
      const onLine = (line: string) => { if (!line) return; const b = JSON.parse(line); blocks.push(b); if (b.header?.number > last) last = b.header.number; };
      for (;;) { const { done, value } = await reader.read(); if (done) break; stats.bytes += value.byteLength; buf += dec.decode(value, { stream: true }); let nl: number; while ((nl = buf.indexOf("\n")) >= 0) { onLine(buf.slice(0, nl)); buf = buf.slice(nl + 1); } }
      buf += dec.decode(); if (buf) onLine(buf);
      return { blocks, last };
    } finally { stats.inflight--; }
  }

  async function* stream(query: object, from: number, to: number) {
    let cursor = from;
    while (cursor <= to) {
      const body = JSON.stringify({ ...query, fromBlock: cursor, toBlock: to });
      let attempt = 0;
      let batch: Awaited<ReturnType<typeof fetchBatch>> | undefined;
      while (batch === undefined) {
        try { batch = await fetchBatch(body, cursor); }
        catch (err: any) {
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

  // ---- discovery: one sparse stream per chunk, accumulating children (memoized) ----
  function discoverChunk(idx: number, factories: any[]): Promise<void> {
    let p = discCache.get(idx);
    if (p) return p;
    p = (async () => {
      stats.discChunks++;
      const from = idx * chunkBlocks, to = from + chunkBlocks - 1;
      for (const factory of factories) {
        const needsData = factory.childAddressLocation.startsWith("offset");
        const q = { type: "evm", fields: { block: { number: true }, log: { address: true, topics: true, data: needsData } }, logs: [{ address: factory.address ? (Array.isArray(factory.address) ? factory.address : [factory.address]).map((a: string) => a.toLowerCase()) : undefined, topic0: [factory.eventSelector.toLowerCase()] }] };
        const rec = args.childAddresses.get(factory.id)!;
        for await (const blocks of stream(q, from, to)) {
          for (const b of blocks) for (const raw of b.logs ?? []) {
            const sl = { address: (raw.address as string)?.toLowerCase(), topics: raw.topics ?? [], data: raw.data ?? "0x", blockNumber: hx(b.header.number) } as unknown as SyncLog;
            if (isLogFactoryMatched({ factory, log: sl })) {
              const child = getChildAddress({ log: sl, factory }).toLowerCase() as Address;
              const bn = b.header.number; const prev = rec.get(child);
              if (prev === undefined || prev > bn) rec.set(child, bn);
            }
          }
        }
      }
    })();
    discCache.set(idx, p);
    return p;
  }
  // discovery complete THROUGH chunk idx (children of all chunks ≤ idx are known)
  function ensureDiscoveredThrough(idx: number, factories: any[]): Promise<unknown> {
    if (factories.length === 0 || discStartIdx === undefined) return Promise.resolve();
    const ps: Promise<void>[] = [];
    for (let i = discStartIdx; i <= idx; i++) ps.push(discoverChunk(i, factories));
    return Promise.all(ps);
  }

  // ---- data chunk: gated on discovery-through-this-chunk, then ONE big data stream ----
  function dataChunk(idx: number, factories: any[], filters: LogFilter[]): Promise<ChunkData> {
    let p = dataCache.get(idx);
    if (p) { stats.cacheHits++; return p; }
    p = (async () => {
      await ensureDiscoveredThrough(idx, factories); // correctness: children ≤ this chunk are known
      stats.dataChunks++;
      const from = idx * chunkBlocks, to = from + chunkBlocks - 1;
      const logRequests = filters.flatMap((f) => logRequestsFor(f)).map((r) => ({ ...r, transaction: true }));
      const data: ChunkData = { headers: new Map(), logs: new Map(), txs: new Map(), traceBlocks: new Map(), blockHeaders: new Map(), txBlocks: new Map() };
      if (logRequests.length > 0) {
        const q = { type: "evm", fields: { block: blockFieldsFor(filters), log: LOG_FIELDS, transaction: needReceipts ? { ...TX_FIELDS, ...RECEIPT_FIELDS } : TX_FIELDS }, logs: logRequests };
        for await (const blocks of stream(q, from, to)) {
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
        for await (const blocks of stream(tq, from, to)) {
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
        for await (const blocks of stream(bq, from, to)) {
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
          for await (const blocks of stream(tq, from, to)) {
            for (const b of blocks) if (b.transactions?.length) {
              const ex = data.txBlocks.get(b.header.number);
              if (ex) ex.txs.push(...b.transactions);
              else data.txBlocks.set(b.header.number, { header: b.header, txs: b.transactions });
            }
          }
        }
      }
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
      for (const t of tb.traces) { const k = t.transactionIndex ?? 0; if (!byTx.has(k)) byTx.set(k, []); byTx.get(k)!.push(t); }
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
  // push the trace/transfer filters' addresses to Portal (resolving factories → children)
  const tracePortalRequests = (): any[] => {
    const reqs: any[] = [];
    const addrsOf = (a: any): string[] | undefined => {
      if (a === undefined) return undefined;
      if (isAddressFactory(a)) return Array.from(args.childAddresses.get(a.id)?.keys() ?? []);
      return (Array.isArray(a) ? a : [a]).map((x: string) => x.toLowerCase());
    };
    for (const f of [...traceFilters, ...transferFilters]) {
      const req: any = {};
      const to = addrsOf(f.toAddress); if (to?.length) req.callTo = to;
      const from = addrsOf(f.fromAddress); if (from?.length) req.callFrom = from;
      if (f.functionSelector) req.callSighash = (Array.isArray(f.functionSelector) ? f.functionSelector : [f.functionSelector]).map((s: string) => s.toLowerCase());
      req.transaction = true; reqs.push(req);
    }
    return reqs.length ? reqs : [{ transaction: true }];
  };
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
      const { interval, requiredIntervals, requiredFactoryIntervals, syncStore } = params;
      if (!startTime) startTime = Date.now();
      // finality gap: if this interval reaches past Portal's finalized head, re-confirm
      // (Portal advances) and, if still beyond, delegate the whole interval to RPC.
      if (portalHead === undefined) await refreshPortalHead();
      if (isFinalityGap(interval[1], portalHead)) {
        await refreshPortalHead(); // Portal advances — re-confirm before falling back
        if (isFinalityGap(interval[1], portalHead)) {
          delegated.add(ikey(interval)); stats.rpcFallback++;
          log.debug({ service: "portal", msg: `Portal ${args.chain.name} [${interval[0]},${interval[1]}] past finalized head ${portalHead} → RPC fallback` });
          return rpcFallback().syncBlockRangeData(params);
        }
      }
      await ensureChunkSize(); // scale chunk to chain block-density before any idxOf()
      const filters = requiredIntervals.map((r) => r.filter).filter((f) => f.type === "log") as LogFilter[];
      const factories = [...new Map(requiredFactoryIntervals.map((r) => [r.factory.id, r.factory])).values()];

      needReceipts ||= requiredIntervals.some((r) => (r.filter as any).hasTransactionReceipt === true);
      if (!needBlocks && requiredIntervals.some((r) => r.filter.type === "block")) {
        blockFilters = requiredIntervals.filter((r) => r.filter.type === "block").map((r) => r.filter);
        needBlocks = blockFilters.length > 0;
      }
      if (!needTxFilter && requiredIntervals.some((r) => r.filter.type === "transaction")) {
        transactionFilters = requiredIntervals.filter((r) => r.filter.type === "transaction").map((r) => r.filter);
        needTxFilter = transactionFilters.length > 0;
      }
      if (!needTraces && requiredIntervals.some((r) => r.filter.type === "trace" || r.filter.type === "transfer")) {
        traceFilters = requiredIntervals.filter((r) => r.filter.type === "trace").map((r) => r.filter);
        transferFilters = requiredIntervals.filter((r) => r.filter.type === "transfer").map((r) => r.filter);
        needTraces = traceFilters.length + transferFilters.length > 0;
      }
      // cap the chunk grid BEFORE any idxOf() for DENSE sources (traces fetch every trace;
      // block sources includeAllBlocks-scan the WHOLE chunk range) — bounds memory + overfetch.
      const capped = traceSafeChunkBlocks(chunkBlocks, needTraces || needBlocks);
      if (capped !== chunkBlocks) {
        chunkBlocks = capped; dataCache.clear(); discCache.clear(); discStartIdx = undefined;
        log.debug({ service: "portal", msg: `Portal ${args.chain.name}: dense sources → chunkBlocks capped to ${chunkBlocks} (grid reset)` });
      }

      // pin the discovery floor at the factory's real start (NOT block 0) — after any chunk cap
      if (discStartIdx === undefined && factories.length > 0) {
        const starts = requiredFactoryIntervals.map((r) => r.interval[0]).concat(interval[0]);
        discStartIdx = idxOf(Math.min(...starts));
      }

      const startIdx = idxOf(interval[0]), endIdx = idxOf(interval[1]);
      const idxs: number[] = [];
      for (let i = startIdx; i <= endIdx; i++) idxs.push(i);
      const data = await Promise.all(idxs.map((i) => dataChunk(i, factories, filters)));
      // PARALLEL read-ahead: prefetch the next READAHEAD chunks concurrently
      for (let d = 1; d <= READAHEAD; d++) void dataChunk(endIdx + d, factories, filters).catch(() => {});

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
      for (const i of dataCache.keys()) if ((i + 1) * chunkBlocks <= interval[0]) dataCache.delete(i); // evict behind

      const syncTraces = needTraces ? data.flatMap((cd) => buildTraces(cd, interval[0], interval[1])) : [];

      let closest: SyncBlock | undefined;
      if (blocksByNumber.size > 0) closest = blocksByNumber.get(Math.max(...blocksByNumber.keys())) as unknown as SyncBlock;
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
