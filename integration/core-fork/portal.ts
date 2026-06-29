import type { Common } from "@/internal/common.js";
import type {
  Chain,
  FactoryId,
  Filter,
  LogFilter,
  SyncBlock,
  SyncBlockHeader,
  SyncLog,
  SyncTransaction,
} from "@/internal/types.js";
import {
  getChildAddress,
  isAddressFactory,
  isLogFactoryMatched,
} from "@/runtime/filter.js";
import type { Interval } from "@/utils/interval.js";
import { type Address, type Hex, numberToHex, toHex } from "viem";
import type { HistoricalSync } from "./index.js";

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
  childAddresses: Map<FactoryId, Map<Address, number>>;
};

type PortalLogRequest = { address?: string[]; topic0?: string[]; topic1?: string[]; topic2?: string[]; topic3?: string[]; transaction?: boolean };
type RawHeader = Record<string, any> & { number: number };
type ChunkData = { headers: Map<number, RawHeader>; logs: Map<number, any[]>; txs: Map<number, any[]> };

const PORTAL_MAX_ADDRESSES = 1000;
const CHUNK_BLOCKS = Number(process.env.PORTAL_CHUNK_BLOCKS ?? 500_000);
const READAHEAD = Number(process.env.PORTAL_READAHEAD ?? 6);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const hx = (v: unknown): Hex => {
  if (typeof v === "string") return (v.startsWith("0x") ? v : toHex(BigInt(v))) as Hex;
  if (typeof v === "number" || typeof v === "bigint") return numberToHex(v);
  return "0x0";
};
const opt = (v: unknown): Hex | undefined => (v === undefined || v === null ? undefined : hx(v));
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

  const stats = { dataChunks: 0, discChunks: 0, http: 0, logs: 0, errors: 0, retries: 0, bytes: 0, cacheHits: 0, inflight: 0, maxInflight: 0 };
  const dataCache = new Map<number, Promise<ChunkData>>(); // keyed by chunk index
  const discCache = new Map<number, Promise<void>>(); // keyed by chunk index
  const stash = new Map<string, { blocks: SyncBlockHeader[]; txs: SyncTransaction[]; closest: SyncBlock | undefined }>();
  const ikey = (i: Interval) => `${i[0]}-${i[1]}`;
  let chunkBlocks = CHUNK_BLOCKS;
  let chunkSizeP: Promise<void> | undefined;
  const idxOf = (n: number) => Math.floor(n / chunkBlocks);
  let discStartIdx: number | undefined; // factory deploy chunk — discovery floor (fixes from-0 scan)

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
  async function fetchBatch(body: string, cursor: number): Promise<{ blocks: { header: RawHeader; logs?: any[]; transactions?: any[] }[]; last: number } | "done"> {
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
      const blocks: { header: RawHeader; logs?: any[]; transactions?: any[] }[] = [];
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

  const REQUIRED_BLOCK_FIELDS = ["number", "hash", "parentHash", "timestamp", "logsBloom", "miner", "gasUsed", "gasLimit", "stateRoot", "receiptsRoot", "transactionsRoot", "size", "difficulty", "extraData"];
  const NULLABLE_BLOCK_FIELDS = ["baseFeePerGas", "nonce", "mixHash", "sha3Uncles", "totalDifficulty"];
  const LOG_FIELDS = { address: true, topics: true, data: true, transactionHash: true, transactionIndex: true, logIndex: true };
  // Ponder's event profiler probes event.transaction.hash, so we pull each matched
  // log's parent transaction (Portal `transaction` relation) and store it.
  const TX_FIELDS = { transactionIndex: true, hash: true, from: true, to: true, input: true, value: true, nonce: true, gas: true, gasPrice: true, maxFeePerGas: true, maxPriorityFeePerGas: true, type: true, r: true, s: true, v: true, yParity: true };
  const blockFieldsFor = (filters: Filter[]): Record<string, boolean> => {
    const inc = new Set<string>();
    for (const f of filters) for (const i of f.include ?? []) if (i.startsWith("block.")) inc.add(i.slice(6));
    const fields: Record<string, boolean> = {};
    for (const k of REQUIRED_BLOCK_FIELDS) fields[k] = true;
    for (const k of NULLABLE_BLOCK_FIELDS) if (inc.has(k)) fields[k] = true;
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
      const data: ChunkData = { headers: new Map(), logs: new Map(), txs: new Map() };
      if (logRequests.length > 0) {
        const q = { type: "evm", fields: { block: blockFieldsFor(filters), log: LOG_FIELDS, transaction: TX_FIELDS }, logs: logRequests };
        for await (const blocks of stream(q, from, to)) {
          for (const b of blocks) if (b.logs?.length) {
            data.headers.set(b.header.number, b.header);
            data.logs.set(b.header.number, (data.logs.get(b.header.number) ?? []).concat(b.logs));
            if (b.transactions?.length) data.txs.set(b.header.number, (data.txs.get(b.header.number) ?? []).concat(b.transactions));
            stats.logs += b.logs.length;
          }
        }
      }
      return data;
    })();
    dataCache.set(idx, p);
    return p;
  }

  const toSyncLog = (l: any, h: RawHeader): SyncLog => ({ address: (l.address as string).toLowerCase(), topics: l.topics ?? [], data: l.data ?? "0x", blockNumber: hx(h.number), blockHash: h.hash, transactionHash: l.transactionHash, transactionIndex: hx(l.transactionIndex), logIndex: hx(l.logIndex), removed: false }) as SyncLog;
  const toSyncBlockHeader = (h: RawHeader): SyncBlockHeader => ({ number: hx(h.number), hash: h.hash, parentHash: h.parentHash, timestamp: hx(h.timestamp), logsBloom: h.logsBloom, miner: h.miner, gasUsed: opt(h.gasUsed), gasLimit: opt(h.gasLimit), baseFeePerGas: opt(h.baseFeePerGas), nonce: h.nonce, mixHash: h.mixHash, stateRoot: h.stateRoot, receiptsRoot: h.receiptsRoot, transactionsRoot: h.transactionsRoot, sha3Uncles: h.sha3Uncles, size: opt(h.size), difficulty: opt(h.difficulty), totalDifficulty: opt(h.totalDifficulty), extraData: h.extraData, transactions: undefined }) as unknown as SyncBlockHeader;
  const toSyncTransaction = (tx: any, h: RawHeader): SyncTransaction => ({
    blockHash: h.hash, blockNumber: hx(h.number),
    from: (tx.from as string)?.toLowerCase(), to: tx.to ? (tx.to as string).toLowerCase() : null,
    gas: hx(tx.gas), hash: tx.hash, input: tx.input ?? "0x", nonce: hx(tx.nonce ?? 0),
    transactionIndex: hx(tx.transactionIndex), value: hx(tx.value ?? 0), type: hx(tx.type ?? 0),
    gasPrice: opt(tx.gasPrice), maxFeePerGas: opt(tx.maxFeePerGas), maxPriorityFeePerGas: opt(tx.maxPriorityFeePerGas),
    v: opt(tx.v), r: tx.r, s: tx.s, yParity: tx.yParity !== undefined ? hx(tx.yParity) : undefined, accessList: tx.accessList,
  }) as unknown as SyncTransaction;

  return {
    async syncBlockRangeData({ interval, requiredIntervals, requiredFactoryIntervals, syncStore }) {
      await ensureChunkSize(); // scale chunk to chain block-density before any idxOf()
      const filters = requiredIntervals.map((r) => r.filter).filter((f) => f.type === "log") as LogFilter[];
      const factories = [...new Map(requiredFactoryIntervals.map((r) => [r.factory.id, r.factory])).values()];
      // pin the discovery floor at the factory's real start (NOT block 0)
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
      const seenTx = new Set<string>();
      for (const cd of data) for (const [bn, hdr] of cd.headers) {
        if (bn < interval[0] || bn > interval[1]) continue;
        const logs = cd.logs.get(bn) ?? [];
        if (logs.length) {
          blocksByNumber.set(bn, toSyncBlockHeader(hdr));
          for (const raw of logs) syncLogs.push(toSyncLog(raw, hdr));
          for (const tx of cd.txs.get(bn) ?? []) if (!seenTx.has(tx.hash)) { seenTx.add(tx.hash); syncTxs.push(toSyncTransaction(tx, hdr)); }
        }
      }
      for (const i of dataCache.keys()) if ((i + 1) * chunkBlocks <= interval[0]) dataCache.delete(i); // evict behind

      let closest: SyncBlock | undefined;
      if (blocksByNumber.size > 0) closest = blocksByNumber.get(Math.max(...blocksByNumber.keys())) as unknown as SyncBlock;
      await syncStore.insertLogs({ logs: syncLogs, chainId: args.chain.id });
      stash.set(ikey(interval), { blocks: [...blocksByNumber.values()], txs: syncTxs, closest });

      log.debug({ service: "portal", msg: `Portal ${args.chain.name} [${interval[0]},${interval[1]}]: ${syncLogs.length} logs (dataChunks=${stats.dataChunks} discChunks=${stats.discChunks} http=${stats.http} hits=${stats.cacheHits} inflight=${stats.maxInflight} err=${stats.errors})` });
      return syncLogs;
    },

    async syncBlockData({ interval, syncStore }) {
      const s = stash.get(ikey(interval));
      stash.delete(ikey(interval));
      if (!s || s.blocks.length === 0) return undefined;
      await syncStore.insertBlocks({ blocks: s.blocks, chainId: args.chain.id });
      if (s.txs.length) await syncStore.insertTransactions({ transactions: s.txs, chainId: args.chain.id });
      return s.closest;
    },
  };
};
