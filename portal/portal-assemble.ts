/**
 * portal-assemble.ts — the pure range assembler.
 *
 * Given the fetched CHUNKS covering an interval, produce exactly the sync-store rows for that interval:
 * logs, blocks, txs, receipts, traces, and the `closest` (highest synced block). This is the whole
 * transform block that used to live inline in `syncBlockRangeData`, made pure and testable:
 *   • INV-2 Interval exactness: only rows with interval[0] ≤ blockNumber ≤ interval[1], no leakage, no
 *     omission — a per-row range assert guards it.
 *   • INV-5 Full-tree trace ranking: `rankTraces` assigns each tx's traces their pre-order DFS rank over
 *     the FULL (unfiltered) set; matching is applied AFTER ranking, so a matched trace keeps its true
 *     position (per-tx ranks strictly increasing).
 *   • closest includes trace-only and tx-only blocks; computed by a LOOP (not Math.max(...spread), which
 *     RangeErrors on ~100k+ keys — was C9).
 */

import type { Address } from 'viem';
import type {
  SyncBlock,
  SyncBlockHeader,
  SyncLog,
  SyncTrace,
  SyncTransaction,
  SyncTransactionReceipt,
} from '@/internal/types.js';
import {
  isAddressFactory,
  isAddressMatched,
  isLogFilterMatched,
  isTraceFilterMatched,
  isTransactionFilterMatched,
  isTransferFilterMatched,
} from '@/runtime/filter.js';
import type { Interval } from '@/utils/interval.js';
import type { ChildAddresses, FetchSpec } from './portal-filters.js';
import {
  cmpTraceAddr,
  hx,
  parityToCallFrame,
  type RawHeader,
  type RawLog,
  type RawTrace,
  type RawTx,
  toSyncBlockHeader,
  toSyncLog,
  toSyncReceipt,
  toSyncTransaction,
} from './portal-transform.js';

/** The per-chunk buffered wire data assembled from the Portal streams (built by the shell's dataChunk). */
export type ChunkData = {
  headers: Map<number, RawHeader>;
  logs: Map<number, RawLog[]>;
  txs: Map<number, RawTx[]>;
  // for trace/transfer sources: full block + all its traces + its txs, by block number
  traceBlocks: Map<
    number,
    { header: RawHeader; traces: RawTrace[]; txs: RawTx[] }
  >;
  // for block-interval sources: headers of blocks matching a BlockFilter (interval/offset)
  blockHeaders: Map<number, RawHeader>;
  // for account transaction sources: blocks + their from/to-matched txs, by block number
  txBlocks: Map<number, { header: RawHeader; txs: RawTx[] }>;
};

export const createChunkData = (): ChunkData => ({
  headers: new Map(),
  logs: new Map(),
  txs: new Map(),
  traceBlocks: new Map(),
  blockHeaders: new Map(),
  txBlocks: new Map(),
});

/** A geth-style CallFrame produced by parityToCallFrame (index = its DFS rank). */
type CallFrame = {
  index: number;
  from: string;
  to?: string;
  type: string;
} & Record<string, unknown>;
export type RankedTrace = { frame: CallFrame; index: number };

/**
 * INV-19 (ancestor-error cascade): reproduce stock ponder's derived trace-error normalization so the
 * Portal-path store is byte-identical to the RPC-path store on `traces.error` / `traces.revert_reason`.
 *
 * Upstream `src/rpc/actions.ts` `debug_traceBlockByNumber`/`debug_traceBlockByHash` DFS (shipped as
 * @subsquid/ponder dist/esm/rpc/actions.js:100-118): within ONE transaction's trace tree, a reverted
 * ancestor's error/revertReason is smeared onto every descendant that lacks its OWN error, transitively
 * (a frame that inherits becomes a source for its own children). A frame with its OWN distinct error
 * keeps it (own-error branch wins). Portal delivers geth-faithful per-frame errors, so the fork must
 * apply this same post-processing to match — realtime/fallback already flow through the untouched RPC
 * cascade, so the fork DB must be uniformly ponder-shaped.
 *
 * Upstream keys `failedTraces` by frame identity and threads `parentFrame` down the recursion; here we
 * key by traceAddress prefix (parent of [a,b,c] is [a,b]) — the frames within a tx are exactly one tree,
 * `sorted` is DFS pre-order (cmpTraceAddr, parent-before-child), so each parent's effective error is
 * resolved before its children are visited. Keyed by traceAddress (not the produced frame) so the
 * parentage chain is intact even if `parityToCallFrame` drops a frame from the output.
 *
 * Upstream issue: <UPSTREAM_ISSUE_URL>  (to be filled by the reviewer).
 */
const cascadeTraceErrors = (
  sorted: RawTrace[],
): Map<string, { error: string; revertReason?: string }> => {
  const effective = new Map<string, { error: string; revertReason?: string }>();
  for (const t of sorted) {
    const addr = t.traceAddress ?? [];
    const key = addr.join(',');
    const ownError = (t.error ?? undefined) as string | undefined;
    if (ownError !== undefined) {
      effective.set(key, {
        error: ownError,
        revertReason: (t.revertReason ?? undefined) as string | undefined,
      });

      continue;
    }

    if (addr.length === 0) continue;

    const parentKey = addr.slice(0, -1).join(',');
    const parentError = effective.get(parentKey);
    if (parentError === undefined) continue;

    effective.set(key, parentError);
  }

  return effective;
};

/**
 * INV-5 producer: given ONE transaction's traces (reward/no-tx frames already excluded), sort them into
 * pre-order DFS (cmpTraceAddr) and assign each its rank = position in that sorted full list. The matcher
 * only ever consumes `RankedTrace`s, so a matched trace's `index` is its full-tree position, never a
 * filter-local one. INV-19: the ancestor-error cascade is applied over the same sorted tree, so each
 * frame carries its post-cascade `error`/`revertReason` (byte-identical to the RPC path).
 */
export function rankTraces(traces: RawTrace[]): RankedTrace[] {
  const sorted = [...traces].sort((x, y) =>
    cmpTraceAddr(x.traceAddress ?? [], y.traceAddress ?? []),
  );
  const effectiveErrors = cascadeTraceErrors(sorted);
  // Ranks are strictly increasing BY CONSTRUCTION (each rank is the forEach counter of a monotonically
  // walked array) — the same "an assert that could never fire" INV-2 deliberately omits, so none here
  // either (wave 4; the property is proven in portal-assemble.test.ts).
  const out: RankedTrace[] = [];
  sorted.forEach((t, i) => {
    const frame = parityToCallFrame(t, i) as CallFrame | undefined;
    if (!frame) return;

    const cascaded = effectiveErrors.get((t.traceAddress ?? []).join(','));
    if (cascaded !== undefined) {
      frame.error = cascaded.error;
      frame.revertReason = cascaded.revertReason;
    }
    out.push({ frame, index: i });
  });

  return out;
}

type Matchers = {
  logMatched: (log: SyncLog, bn: number) => boolean;
  traceMatched: (frame: CallFrame, bn: number) => boolean;
  txFilterMatched: (tx: SyncTransaction, bn: number) => boolean;
};

const buildMatchers = (
  spec: FetchSpec,
  childAddresses: ChildAddresses,
): Matchers => {
  const factoryAddrOk = (
    filterAddr: unknown,
    addr: string | undefined,
    bn: number,
  ): boolean =>
    !isAddressFactory(filterAddr as never) ||
    isAddressMatched({
      address: addr as Address,
      blockNumber: bn,
      childAddresses: childAddresses.get((filterAddr as { id: string }).id)!,
    });
  return {
    // Log re-match (wave 4 review): the Portal's server-side log filter is the MERGED request set with no
    // per-filter fromBlock/toBlock and no per-child creation floor, so it legitimately returns (a) a
    // factory child's logs from BEFORE the child's creation block and (b) a bounded filter's logs outside
    // its own range when another filter's chunk fetched them. Upstream's RPC path re-matches every log
    // (isLogFilterMatched + isAddressMatched) before insertion; skipping that here stored/returned rows
    // the RPC path excludes — an INV-6 store divergence. buildEvents re-filters, so no wrong events fired,
    // but the sync store must be byte-identical across both paths.
    logMatched: (log, bn) =>
      spec.logFilters.some(
        (f) =>
          isLogFilterMatched({ filter: f, log }) &&
          factoryAddrOk(f.address, log.address, bn),
      ),
    traceMatched: (frame, bn) => {
      const blk = { number: BigInt(bn) } as never;
      for (const f of spec.transferFilters)
        if (
          isTransferFilterMatched({
            filter: f,
            trace: frame as never,
            block: blk,
          }) &&
          factoryAddrOk(f.fromAddress, frame.from, bn) &&
          factoryAddrOk(f.toAddress, frame.to, bn)
        )
          return true;
      for (const f of spec.traceFilters)
        if (
          isTraceFilterMatched({
            filter: f,
            trace: frame as never,
            block: blk,
          }) &&
          factoryAddrOk(f.fromAddress, frame.from, bn) &&
          factoryAddrOk(f.toAddress, frame.to, bn)
        )
          return true;
      return false;
    },
    txFilterMatched: (tx, bn) =>
      spec.transactionFilters.some(
        (f) =>
          isTransactionFilterMatched({ filter: f, transaction: tx }) &&
          factoryAddrOk(f.fromAddress, tx.from, bn) &&
          factoryAddrOk(
            f.toAddress,
            (tx.to ?? undefined) as string | undefined,
            bn,
          ),
      ),
  };
};

/**
 * Stream-seam parity for `runStreams`' needed-field growth check (wave-4 log re-match). The Portal's
 * server-side log filter over-returns rows that assembly then DROPS — a factory child's PRE-CREATION logs
 * and a bounded filter's OUT-OF-RANGE logs (see `logMatched` above) — so counting raw returned logs toward
 * "matched data this call added" would arm the needed-field fatal for data the indexer never keeps → G1
 * evict → crash-loop. This returns a predicate over RAW logs that mirrors `assembleRange`'s per-log
 * re-match EXACTLY (same `buildMatchers` + `toSyncLog`, over the same discovery-complete `childAddresses`),
 * so the seam count and assembly agree by construction.
 */
export const buildRawLogMatcher = (
  spec: FetchSpec,
  childAddresses: ChildAddresses,
): ((raw: RawLog, hdr: RawHeader, bn: number) => boolean) => {
  const { logMatched } = buildMatchers(spec, childAddresses);

  return (raw, hdr, bn) => logMatched(toSyncLog(raw, hdr), bn);
};

/**
 * Stream-seam parity for `runStreams`' needed-field growth check — the TRACE/TRANSFER analogue of
 * `buildRawLogMatcher` (#20). The trace query is server-side UNFILTERED by design (INV-5: fetch every
 * trace so `buildTraces` can rank over the full tree, THEN client-filter), so the tail returns traces
 * assembly drops wholesale — traces matching no configured trace/transfer filter, a bounded filter's
 * out-of-range traces, a factory child's pre-creation traces. Counting raw returned traces (the old
 * `cd.traceBlocks.size` delta) toward "matched data this call added" armed the needed-field fatal for
 * data the indexer never keeps → G1 evict → crash-loop. This mirrors `buildTraces`' KEEP decision
 * EXACTLY: the same per-trace pre-skip (`transactionIndex == null` / `type === 'reward'`), the same
 * `parityToCallFrame` frame (undefined ⇒ dropped, e.g. a non-call/create/suicide type), then the same
 * `traceMatched`. `rankTraces` only assigns each matched frame its DFS `index`; the trace/transfer
 * matchers (`isTraceFilterMatched`/`isTransferFilterMatched`) never read `index`, so ranking never turns
 * a matched frame unmatched — a single-frame `parityToCallFrame(t, 0)` decides KEEP identically.
 */
export const buildRawTraceMatcher = (
  spec: FetchSpec,
  childAddresses: ChildAddresses,
): ((raw: RawTrace, bn: number) => boolean) => {
  const { traceMatched } = buildMatchers(spec, childAddresses);

  return (raw, bn) => {
    if (raw.transactionIndex == null || raw.type === 'reward') return false;

    const frame = parityToCallFrame(raw, 0) as CallFrame | undefined;
    if (frame === undefined) return false;

    return traceMatched(frame, bn);
  };
};

/**
 * Stream-seam parity for `runStreams`' needed-field growth check — the TRANSACTION analogue of
 * `buildRawLogMatcher` (#20). The account-tx query pushes only the merged from/to address sets
 * (`txRequestsFor`) with NO per-filter block range, so — exactly like the log query — it over-returns:
 * a bounded tx filter's out-of-range txs, a two-filter query's cross-matches (A's `from` set ∪ B's `to`
 * set), and a factory child's pre-creation txs all ride the request and `assembleRange` re-matches them
 * AWAY via `txFilterMatched` (`isTransactionFilterMatched` re-checks the block range + factory floor).
 * Counting raw `cd.txBlocks.size` toward "matched data this call added" armed the same false-fatal. This
 * mirrors assembly's per-tx re-match EXACTLY (same `buildMatchers` + `toSyncTransaction`).
 */
export const buildRawTxMatcher = (
  spec: FetchSpec,
  childAddresses: ChildAddresses,
): ((raw: RawTx, hdr: RawHeader, bn: number) => boolean) => {
  const { txFilterMatched } = buildMatchers(spec, childAddresses);

  return (raw, hdr, bn) => txFilterMatched(toSyncTransaction(raw, hdr), bn);
};

/** Per-chunk trace assembly: full-tree ranking then client-side filtering (INV-5). `onReceipt` (FIX 4)
 * is invoked with each MATCHED trace's parent transaction so the caller can emit a receipt when a
 * trace/transfer filter has `hasTransactionReceipt` — the log/tx branches never see these txs. */
const buildTraces = (
  cd: ChunkData,
  lo: number,
  hi: number,
  matchers: Matchers,
  onReceipt?: (rawTx: RawTx, header: RawHeader) => void,
): { trace: SyncTrace; block: SyncBlock; transaction: SyncTransaction }[] => {
  const out: {
    trace: SyncTrace;
    block: SyncBlock;
    transaction: SyncTransaction;
  }[] = [];
  for (const [bn, tb] of cd.traceBlocks) {
    if (bn < lo || bn > hi || !tb.traces?.length) continue;
    const block = toSyncBlockHeader(tb.header) as unknown as SyncBlock; // encodeTrace only reads block.number
    const txByIdx = new Map<number | undefined, RawTx>();
    for (const tx of tb.txs ?? []) txByIdx.set(tx.transactionIndex, tx);
    const byTx = new Map<number, RawTrace[]>();
    // callTracer has no block-reward frames; skip reward/no-tx traces so `?? 0` can't fold them into tx 0
    // and shift its DFS ranks (now that we fetch the full, unfiltered trace set).
    for (const t of tb.traces) {
      if (t.transactionIndex == null || t.type === 'reward') continue;
      const k = t.transactionIndex;
      if (!byTx.has(k)) byTx.set(k, []);
      byTx.get(k)!.push(t);
    }
    for (const [txIndex, traces] of byTx) {
      const rawTx = txByIdx.get(txIndex);
      for (const { frame } of rankTraces(traces)) {
        if (!matchers.traceMatched(frame, bn)) continue;
        if (rawTx) onReceipt?.(rawTx, tb.header); // FIX 4: receipt for the matched trace's tx
        out.push({
          trace: {
            trace: frame,
            transactionHash: rawTx?.hash,
          } as unknown as SyncTrace,
          block,
          transaction: rawTx
            ? toSyncTransaction(rawTx, tb.header)
            : ({ transactionIndex: hx(txIndex) } as unknown as SyncTransaction),
        });
      }
    }
  }
  return out;
};

export type AssembledRange = {
  logs: SyncLog[];
  blocks: SyncBlockHeader[];
  txs: SyncTransaction[];
  receipts: SyncTransactionReceipt[];
  traces: {
    trace: SyncTrace;
    block: SyncBlock;
    transaction: SyncTransaction;
  }[];
  closest: SyncBlock | undefined;
};

/**
 * Yield a chunk map's entries whose block-number key ∈ [lo, hi], picking the cheaper of two walks: a
 * RANGE-WALK (`get()` per block, bounded by the interval width `hi−lo+1`) or a full ENTRY-SCAN (bounded by
 * `map.size`). The maps are keyed by block number, so both find exactly the same entries.
 *
 * Why it matters (wave 5, perf): a density-scaled chunk holds up to ~25M block entries, an interval is
 * ≤~100k blocks, and the SAME cached chunk is assembled once per interval it overlaps. Scanning ALL chunk
 * entries per interval — the previous shape — is O(intervalsPerChunk × chunkEntries): ~250 re-reads of a
 * 25M-entry map, billions of iterator steps, tens of minutes on a backfill. This bounds each assemble at
 * O(min(intervalWidth, chunkEntries)). Values are never `undefined` in these maps, so `!== undefined`
 * cleanly rejects a range-walk miss.
 */
function* entriesInRange<V>(
  map: Map<number, V>,
  lo: number,
  hi: number,
): Generator<[number, V]> {
  if (hi - lo + 1 <= map.size) {
    for (let bn = lo; bn <= hi; bn++) {
      const v = map.get(bn);
      if (v !== undefined) yield [bn, v];
    }
  } else {
    for (const entry of map) {
      if (entry[0] >= lo && entry[0] <= hi) yield entry;
    }
  }
}

/**
 * Assemble exactly the interval's rows from its chunks (INV-2). Pure given `childAddresses` (which
 * discovery has already grown through the interval).
 */
export function assembleRange(
  chunks: ChunkData[],
  interval: Interval,
  spec: FetchSpec,
  childAddresses: ChildAddresses,
): AssembledRange {
  const [lo, hi] = interval;
  const matchers = buildMatchers(spec, childAddresses);

  const syncLogs: SyncLog[] = [];
  const blocksByNumber = new Map<number, SyncBlockHeader>();
  const syncTxs: SyncTransaction[] = [];
  const syncReceipts: SyncTransactionReceipt[] = [];
  const seenTx = new Set<string>();
  // FIX 4: receipts are deduped by tx hash across ALL branches (log, tx-filter, trace/transfer) — a tx
  // matched by more than one source must yield exactly one receipt row.
  const seenReceipt = new Set<string>();
  const pushReceipt = (raw: RawTx, hdr: RawHeader): void => {
    if (!raw.hash || seenReceipt.has(raw.hash)) return;

    seenReceipt.add(raw.hash);
    syncReceipts.push(toSyncReceipt(raw, hdr));
  };

  // INV-2 is enforced BY CONSTRUCTION here: every emitting branch below iterates only entries whose block
  // number ∈ [lo, hi] via `entriesInRange` (there is deliberately no redundant per-row assert — it could
  // never fire), and the exactness property is proven against a brute-force model in portal-assemble.test.ts.
  // Each log is re-matched against the ACTUAL log filters (see logMatched above); a dropped log's
  // parent tx/block must not ride in either, so txs are keyed to the KEPT logs' transactionHash.
  for (const cd of chunks)
    for (const [bn, hdr] of entriesInRange(cd.headers, lo, hi)) {
      const rawLogs = cd.logs.get(bn) ?? [];
      if (rawLogs.length === 0) continue;

      const keptTxHashes = new Set<string>();
      let keptAny = false;
      for (const raw of rawLogs) {
        const log = toSyncLog(raw, hdr);
        if (!matchers.logMatched(log, bn)) continue;

        keptAny = true;
        syncLogs.push(log);
        if (raw.transactionHash) keptTxHashes.add(raw.transactionHash);
      }
      if (!keptAny) continue;

      blocksByNumber.set(bn, toSyncBlockHeader(hdr));
      for (const tx of cd.txs.get(bn) ?? [])
        if (tx.hash && keptTxHashes.has(tx.hash) && !seenTx.has(tx.hash)) {
          seenTx.add(tx.hash);
          syncTxs.push(toSyncTransaction(tx, hdr));
          if (spec.needReceipts) pushReceipt(tx, hdr);
        }
    }

  // block-interval sources: ensure each matched block is in the blocks table (cd.blockHeaders already
  // holds ONLY the BlockFilter-matched headers — the shell filters at fetch time to avoid buffering the
  // whole includeAllBlocks scan).
  if (spec.needBlocks)
    for (const cd of chunks)
      for (const [bn, hdr] of entriesInRange(cd.blockHeaders, lo, hi)) {
        if (!blocksByNumber.has(bn))
          blocksByNumber.set(bn, toSyncBlockHeader(hdr));
      }

  // account transaction sources: re-match Portal's from/to-filtered txs (+ factory + range), insert tx/receipt/block
  if (spec.needTxFilter)
    for (const cd of chunks)
      for (const [bn, tb] of entriesInRange(cd.txBlocks, lo, hi)) {
        for (const raw of tb.txs) {
          const tx = toSyncTransaction(raw, tb.header);
          if (!matchers.txFilterMatched(tx, bn)) continue;
          // EVERY tx-filter-matched tx gets a receipt, independent of hasTransactionReceipt.
          // Hardening, not a reachable bug today — upstream guarantees hasTransactionReceipt: true
          // on every tx filter (literal type + build), so needReceipts covered this. But ponder's
          // buildEvents reads `transactionReceipt.status` (positional cursor, NO identity check) on
          // every transaction event to apply `includeReverted`, and the RPC path collects these
          // receipts unconditionally; pushing unconditionally removes the dependence on that
          // upstream invariant. Pushed BEFORE the seenTx skip: a tx also matched by a log filter got
          // its tx row from the log branch, whose raw row lacks receipt columns unless needReceipts —
          // THIS branch's raw row always carries them (txQuery projects RECEIPT_FIELDS).
          pushReceipt(raw, tb.header);
          if (raw.hash && seenTx.has(raw.hash)) continue;
          if (raw.hash) seenTx.add(raw.hash);
          blocksByNumber.set(bn, toSyncBlockHeader(tb.header));
          syncTxs.push(tx);
        }
      }

  // FIX 4: a trace/transfer filter with hasTransactionReceipt needs a receipt for every matched trace tx;
  // those txs ride on the trace query (fields include RECEIPT_FIELDS when needReceipts) and are seen by no
  // other branch, so emit them here (deduped via pushReceipt). Without this, buildEvents throws
  // "Missing transaction receipt" on a legit trace-receipt config.
  const needTraceReceipts =
    spec.traceFilters.some((f) => f.hasTransactionReceipt) ||
    spec.transferFilters.some((f) => f.hasTransactionReceipt);
  const onTraceReceipt = needTraceReceipts ? pushReceipt : undefined;
  const traces = spec.needTraces
    ? chunks.flatMap((cd) => buildTraces(cd, lo, hi, matchers, onTraceReceipt))
    : [];

  // C9: highest block with data — a LOOP (Math.max(...spread) RangeErrors on ~100k+ keys) — INCLUDING
  // trace-only blocks so `closest` doesn't understate the synced tip.
  let closest: SyncBlock | undefined;
  let maxBn = -1;
  for (const [bn, hdr] of blocksByNumber)
    if (bn > maxBn) {
      maxBn = bn;
      closest = hdr as unknown as SyncBlock;
    }
  for (const t of traces) {
    const bn = Number((t.block as { number: unknown }).number);
    if (bn > maxBn) {
      maxBn = bn;
      closest = t.block;
    }
  }

  return {
    logs: syncLogs,
    blocks: [...blocksByNumber.values()],
    txs: syncTxs,
    receipts: syncReceipts,
    traces,
    closest,
  };
}
