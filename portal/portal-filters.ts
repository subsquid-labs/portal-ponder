/**
 * portal-filters.ts — the per-chain Portal fetch-spec (pure).
 *
 * SINGLE source of truth for how Ponder filters become Portal requests + field projections, shared by
 * the historical sync (portal.ts) AND the realtime wire (portal-realtime-wire.ts) — deleting the former
 * duplication so the two paths can never drift.
 *
 * FILTER/PROJECTION STRATEGY (max Portal leverage): every row filter is pushed to Portal's native
 * server-side filters — logs by address+topics (`logRequestsFor`), account txs by from/to (`txRequests`).
 * Traces are the exception (see `traceRequests` / INV-5). Field projection requests exactly the columns
 * the sync store persists and no more.
 *
 * `compileFetchSpec` freezes the COMPLETE chain-wide spec ONCE from `eventCallbacks` (the FULL per-chain
 * filter set), NOT from per-call `requiredIntervals` (only the subset Ponder still needs, which shrinks
 * as fragments cache). Chunks are cached by idx ALONE, so every chunk MUST be filter-complete — else a
 * filter that first needs an already-cached chunk is never streamed yet its interval is marked done
 * (INV-1). The spec's request builders read the LIVE `childAddresses` map (discovery grows it over
 * time); its filter set + field projections are immutable.
 */

import type { Address, Hex } from 'viem';
import type {
  BlockFilter,
  Factory,
  FactoryId,
  Filter,
  LogFilter,
  TraceFilter,
  TransactionFilter,
  TransferFilter,
} from '@/internal/types.js';
import { getFilterFactories, isAddressFactory } from '@/runtime/filter.js';
import { invariant } from './portal-invariant.js';

export type ChildAddresses = Map<FactoryId, Map<Address, number>>;

export type PortalLogRequest = {
  address?: string[];
  topic0?: string[];
  topic1?: string[];
  topic2?: string[];
  topic3?: string[];
  transaction?: boolean;
};
export type TxRequest = { from?: string[]; to?: string[] };
export type TraceRequest = { transaction?: boolean };

type FieldMap = Record<string, boolean>;
export type PortalQuery = {
  type: 'evm';
  includeAllBlocks?: boolean;
  fields: Record<string, FieldMap>;
  logs?: PortalLogRequest[];
  traces?: TraceRequest[];
  transactions?: TxRequest[];
};

// Portal rejects any request whose raw body exceeds this (sqd-network transport/src/protocol.rs:
// `MAX_RAW_QUERY_SIZE = 256 * 1024`) with 400 "Query is too large". The body is dominated by filter
// address lists (factory children). We keep under it by merging per-event log filters + batching
// addresses; a body that still overflows fails loud (portal-client).
export const MAX_RAW_QUERY_SIZE = 256 * 1024;
export const PORTAL_MAX_ADDRESSES = 1000;

// #194 factory sharding. A factory whose child set is large enough (~5.8k children) blows a single
// logQuery() body past MAX_RAW_QUERY_SIZE — a permanent hard stop. `logQueryShards()` partitions the
// address union into multiple bodies, each strictly UNDER the cap, that portal.ts streams sequentially
// and UNIONs (INV-11: disjoint address subsets, additive rows). SHARD_BODY_BUDGET is the per-shard
// byte ceiling — held below MAX_RAW_QUERY_SIZE by SHARD_SAFETY_MARGIN so a shard never 400s
// server-side. The margin absorbs (a) the `{...query, fromBlock, toBlock}` envelope client.stream adds
// per POST (portal-client.ts:668 — a few tens of bytes) and (b) any server-side accounting that counts
// slightly more than our JSON.stringify estimate. A few KiB is generous: the dominant term is the
// address strings, and one shard boundary costs at most one under-budget shard, never data.
export const SHARD_SAFETY_MARGIN = 8 * 1024;
export const SHARD_BODY_BUDGET = MAX_RAW_QUERY_SIZE - SHARD_SAFETY_MARGIN;

const REQUIRED_BLOCK_FIELDS = [
  'number',
  'hash',
  'parentHash',
  'timestamp',
  'logsBloom',
  'miner',
  'gasUsed',
  'gasLimit',
  'stateRoot',
  'receiptsRoot',
  'transactionsRoot',
  'size',
  'difficulty',
  'extraData',
];
const NULLABLE_BLOCK_FIELDS = [
  'baseFeePerGas',
  'nonce',
  'mixHash',
  'sha3Uncles',
  'totalDifficulty',
];
export const LOG_FIELDS: FieldMap = {
  address: true,
  topics: true,
  data: true,
  transactionHash: true,
  transactionIndex: true,
  logIndex: true,
};
// Ponder's event profiler probes event.transaction.hash, so we pull each matched log's parent
// transaction (Portal `transaction` relation) and store it.
export const TX_FIELDS: FieldMap = {
  transactionIndex: true,
  hash: true,
  from: true,
  to: true,
  input: true,
  value: true,
  nonce: true,
  gas: true,
  gasPrice: true,
  maxFeePerGas: true,
  maxPriorityFeePerGas: true,
  type: true,
  r: true,
  s: true,
  v: true,
  yParity: true,
  accessList: true,
};
// receipt fields ride on Portal's transaction object (no separate receipt entity)
export const RECEIPT_FIELDS: FieldMap = {
  status: true,
  cumulativeGasUsed: true,
  effectiveGasPrice: true,
  gasUsed: true,
  contractAddress: true,
  logsBloom: true,
};
// trace fields: request both flattened selectors (some Portal builds) AND rely on nested action/result
// in the response — the transform reads whichever is present.
export const TRACE_FIELDS: FieldMap = {
  transactionIndex: true,
  traceAddress: true,
  type: true,
  subtraces: true,
  error: true,
  revertReason: true,
  callFrom: true,
  callTo: true,
  callValue: true,
  callGas: true,
  callInput: true,
  callSighash: true,
  callCallType: true,
  callResultGasUsed: true,
  callResultOutput: true,
  createFrom: true,
  createValue: true,
  createGas: true,
  createInit: true,
  createResultGasUsed: true,
  createResultCode: true,
  createResultAddress: true,
  suicideAddress: true,
  suicideRefundAddress: true,
  suicideBalance: true,
};
// Block header field set requested on EVERY query: the RPC-path-equivalent columns, so stored blocks
// are byte-identical with the RPC path (which always has nonce/mixHash/sha3Uncles/totalDifficulty).
// Shared by the historical queries and the realtime wire.
export const BLOCK_FIELDS: FieldMap = Object.fromEntries(
  [...REQUIRED_BLOCK_FIELDS, ...NULLABLE_BLOCK_FIELDS].map((k) => [k, true]),
);

// Fields that are NULLABLE in Ponder's sync-store AND non-load-bearing — Ponder never uses them
// internally and they're legitimately absent on some chains (accessList on non-typed txs; nonce/
// mixHash on PoS; baseFeePerGas pre-1559; totalDifficulty post-merge). Safe to store as null when a
// dataset lacks them. Anything NOT here, missing ⇒ crash (a NOT-NULL / bloom-load-bearing / core
// column whose absence would corrupt or silently gut data).
export const DROPPABLE_FIELDS = new Set([
  'transaction.accessList',
  'block.baseFeePerGas',
  'block.nonce',
  'block.mixHash',
  'block.sha3Uncles',
  'block.totalDifficulty',
]);

const asArr = (
  t: Hex | readonly Hex[] | null | undefined,
): string[] | undefined => {
  if (t === null || t === undefined) return undefined;
  return (Array.isArray(t) ? t : [t]).map((x) => (x as string).toLowerCase());
};

/** Log-filter → Portal log requests. Factory-address filters expand to the currently-known children
 * (empty ⇒ [], never a match-all); plain-address filters batch by PORTAL_MAX_ADDRESSES. */
export function logRequestsFor(
  filter: LogFilter,
  childAddresses: ChildAddresses,
): PortalLogRequest[] {
  const base: PortalLogRequest = {};
  if (filter.topic0) base.topic0 = asArr(filter.topic0);
  if (filter.topic1) base.topic1 = asArr(filter.topic1);
  if (filter.topic2) base.topic2 = asArr(filter.topic2);
  if (filter.topic3) base.topic3 = asArr(filter.topic3);
  let addresses: string[] | undefined;
  if (isAddressFactory(filter.address)) {
    addresses = Array.from(childAddresses.get(filter.address.id)?.keys() ?? []);
    if (addresses.length === 0) return [];
  } else if (filter.address === undefined) {
    return [base];
  } else {
    addresses = (
      Array.isArray(filter.address) ? filter.address : [filter.address]
    ).map((a) => a.toLowerCase());
  }
  const out: PortalLogRequest[] = [];
  for (let i = 0; i < addresses.length; i += PORTAL_MAX_ADDRESSES)
    out.push({
      ...base,
      address: addresses.slice(i, i + PORTAL_MAX_ADDRESSES),
    });
  return out;
}

/**
 * Ponder emits ONE filter per event, so an N-event contract produces N log requests each repeating the
 * SAME (possibly large) child-address list with a different topic0. Concatenated into one body they can
 * exceed the Portal's raw query-size limit. Collapse requests sharing the same address set + topic1..3
 * into one, unioning topic0 (undefined-absorbs-all) — identical result set (INV-11), ~N× smaller body.
 */
export function mergeLogRequests(reqs: PortalLogRequest[]): PortalLogRequest[] {
  const groups = new Map<string, PortalLogRequest>();
  for (const r of reqs) {
    const key = JSON.stringify([
      r.address ? [...r.address].sort() : null,
      r.topic1 ?? null,
      r.topic2 ?? null,
      r.topic3 ?? null,
    ]);
    const g = groups.get(key);
    if (!g) {
      groups.set(key, {
        ...r,
        topic0: r.topic0 ? [...new Set(r.topic0)] : undefined,
      });
      continue;
    }
    if (g.topic0 === undefined || r.topic0 === undefined)
      g.topic0 = undefined; // one wants ALL topic0 → keep the broadest
    else {
      const s = new Set(g.topic0);
      for (const t of r.topic0) s.add(t);
      g.topic0 = [...s];
    }
  }
  return [...groups.values()];
}

/**
 * #194: partition a merged `logs: PortalLogRequest[]` array into byte-budgeted shards. Each element is
 * already ≤ PORTAL_MAX_ADDRESSES (1000) addresses (`logRequestsFor` batches at that width ≈ 45 KiB),
 * so a single element always fits SHARD_BODY_BUDGET — the partition is over ARRAY ELEMENTS, never
 * inside one. Greedy bin-pack in the merged order: keep the current shard's serialized body under
 * budget; a bare `type/fields/logs` envelope (`envelope`) is measured with each candidate `logs`
 * subset. Guarantees: (a) EVERY shard body < SHARD_BODY_BUDGET < MAX_RAW_QUERY_SIZE; (b) the shards
 * partition `logs` (disjoint, order-preserving, union == input); (c) when the WHOLE array's envelope
 * is < SHARD_BODY_BUDGET the result is EXACTLY ONE shard whose `logs` IS the input array (same objects,
 * same order) — so wrapping it in `envelope` is byte-identical to the un-sharded query (the no-op). NB:
 * the no-op is scoped to < SHARD_BODY_BUDGET, NOT < MAX_RAW_QUERY_SIZE — a body in the SHARD_SAFETY_MARGIN
 * band (SHARD_BODY_BUDGET ≤ body < MAX_RAW_QUERY_SIZE) still shards into >1 shard (completeness-preserving,
 * just not a single-body no-op). The validated corpus (≤872 children ≈ 40KB) is far below budget, so it is
 * provably a byte-identical no-op for the entire validated corpus.
 */
function shardLogs(
  logs: PortalLogRequest[],
  envelope: (logs: PortalLogRequest[]) => PortalQuery,
): PortalQuery[] {
  if (logs.length === 0) return [];

  // A shard with ALL of `logs` — the common (below-the-wall) case. One serialize; if it fits, the
  // single shard is byte-identical to the un-sharded query (same `logs` reference, same order).
  const whole = envelope(logs);
  if (JSON.stringify(whole).length < SHARD_BODY_BUDGET) return [whole];

  const shards: PortalQuery[] = [];
  let current: PortalLogRequest[] = [];
  for (const entry of logs) {
    // Guarantee (a) has teeth: a shard NEVER starts smaller than one element, so if a single element's
    // own envelope is already ≥ budget the bin-pack would silently emit an oversized shard (both the
    // first-element push and the reset-to-singleton below). Not reachable today — each element is ≤
    // PORTAL_MAX_ADDRESSES (1000) addresses ≈ 45KiB — but nothing in this function enforced it. Check
    // every element ONCE, up front, so an oversized element is an attributable plan-build-time throw,
    // not a 400 at stream time. It never fires below the wall (that path returned `[whole]` already).
    const entrySize = JSON.stringify(envelope([entry])).length;
    invariant(
      '#194',
      entrySize < SHARD_BODY_BUDGET,
      `#194: a single merged log-request element (${entrySize}B) exceeds SHARD_BODY_BUDGET — reduce PORTAL_MAX_ADDRESSES or narrow the filter's topic0 union`,
      () => ({ entrySize, budget: SHARD_BODY_BUDGET }),
    );

    if (current.length === 0) {
      current.push(entry);
      continue;
    }

    const withEntry = [...current, entry];
    if (JSON.stringify(envelope(withEntry)).length < SHARD_BODY_BUDGET) {
      current = withEntry;
      continue;
    }

    shards.push(envelope(current));
    current = [entry];
  }
  if (current.length > 0) shards.push(envelope(current));

  return shards;
}

/**
 * #196: split ONE over-cap `TxRequest` into fitting pieces by CHUNKING its dominant address side.
 *
 * Unlike log requests — which `logRequestsFor` pre-batches into ≤PORTAL_MAX_ADDRESSES-address
 * `PortalLogRequest` elements so a single element always fits — `txRequestsFor` packs ALL factory-child
 * addresses into ONE `TxRequest.from` (or `.to`) array with no batching. So the over-cap lives INSIDE
 * one TxRequest's from[]/to[] array, not across the `TxRequest[]` array. To restore the "single element
 * always fits" invariant the bin-packer relies on, we split the DOMINANT (larger) side — `from` XOR
 * `to` — into fixed-width disjoint batches of PORTAL_MAX_ADDRESSES (mirroring `logRequestsFor`'s
 * `for (i += PORTAL_MAX_ADDRESSES)` batching), each batch keeping the OTHER side WHOLE.
 *
 * COMPLETE + NON-DUP BY CONSTRUCTION: a tx matches a TxRequest iff `from ∈ req.from AND to ∈ req.to`
 * (AND within a request). Chunking the dominant side into disjoint batches Fi (other side whole) means a
 * tx's from lands in exactly ONE Fi → it matches exactly one chunk; the union over chunks == the
 * original request, disjoint on the chunked side. (Client re-match — `txFilterMatched`,
 * portal-assemble.ts:180/238/314 — drops non-matches so over-fetch is harmless, and assemble's seenTx /
 * seenReceipt hash-sets, portal-assemble.ts:446-482, drop any tx/receipt seen across shards.)
 *
 * both-sides-huge corner: when a fixed PORTAL_MAX_ADDRESSES-wide chunk of the dominant side PLUS the
 * whole other side STILL overflows the budget, fixed-width chunking of one side does not help (NEITHER
 * side need individually exceed budget for this to bite — e.g. from=5000 + to=4800 leaves a first chunk
 * `{from: 1000, to: 4800}` over-cap). Curing it needs adaptive sub-PORTAL_MAX_ADDRESSES chunking (or a
 * grid partition, which would blow up shard COUNT) — a throughput enhancement, deliberately out of scope
 * and field-unreachable today. We do NOT sub-chunk here; the caller's per-element fail-loud invariant
 * fires (mirroring shardLogs:296 and portal-client.ts:509's "cannot be safely split").
 */
function chunkTxRequest(
  req: TxRequest,
  envelope: (transactions: TxRequest[]) => PortalQuery,
): TxRequest[] {
  // Below the per-element wall (the common case incl. the whole validated corpus): keep the request
  // WHOLE — return the SAME `req` object we received (by reference), so no chunk boundary is introduced.
  if (JSON.stringify(envelope([req])).length < SHARD_BODY_BUDGET) return [req];

  // Chunk the DOMINANT side (the larger address array); keep the other side whole in every chunk.
  const fromLen = req.from?.length ?? 0;
  const toLen = req.to?.length ?? 0;
  const chunkFrom = fromLen >= toLen;
  const side = chunkFrom ? req.from : req.to;
  if (!side || side.length === 0) return [req]; // no address side to split → let the caller fail loud

  const out: TxRequest[] = [];
  for (let i = 0; i < side.length; i += PORTAL_MAX_ADDRESSES) {
    const batch = side.slice(i, i + PORTAL_MAX_ADDRESSES);
    if (chunkFrom) {
      out.push(req.to ? { from: batch, to: req.to } : { from: batch });
    } else {
      out.push(req.from ? { from: req.from, to: batch } : { to: batch });
    }
  }

  return out;
}

/**
 * #196: partition a `transactions: TxRequest[]` array into byte-budgeted shards (chunk-then-binpack).
 *
 * STEP 1 — chunk: each TxRequest whose own envelope is ≥ SHARD_BODY_BUDGET is split (chunkTxRequest)
 * into disjoint fixed-width batches of its dominant side, restoring the "single element always fits"
 * invariant the bin-pack below relies on. Below the wall this leaves the array UNTOUCHED (same objects).
 *
 * STEP 2 — bin-pack: greedy pack the chunked `TxRequest[]` into shards under SHARD_BODY_BUDGET (a
 * shardLogs-shaped packer: keep the current shard's serialized body under budget; else start a new
 * shard). Guarantees, mirroring shardLogs: (a) EVERY shard body < SHARD_BODY_BUDGET < MAX_RAW_QUERY_SIZE;
 * (b) the shards partition the chunked array (order-preserving, union == the chunked input); (c) when the
 * WHOLE array's envelope is < SHARD_BODY_BUDGET the result is EXACTLY ONE shard whose `transactions` IS
 * this function's `transactions` arg (same objects, same order); wrapped in `envelope` its serialized body
 * is byte-identical to txQuery()'s (the #194-style no-op). Cross-method this is byte-identity, NOT `===`:
 * txQuery() and txQueryShards() each re-derive their TxRequest[] from txRequestsFor independently, so the
 * arrays are distinct instances with identical serialized bodies. NB: the no-op is scoped to <
 * SHARD_BODY_BUDGET, NOT < MAX_RAW_QUERY_SIZE — a body in the SHARD_SAFETY_MARGIN band still shards
 * (completeness-preserving, just not a single-body no-op).
 *
 * both-sides-huge fail-loud: after chunking, a TxRequest whose OWN envelope is still ≥ SHARD_BODY_BUDGET
 * (a fixed PORTAL_MAX_ADDRESSES-wide chunk of the dominant side plus the whole other side still overflows —
 * see chunkTxRequest; NEITHER side need individually exceed budget) fires an attributable
 * `invariant('#196', …)` plan-build throw, NOT a Portal 400 at stream time. This mirrors shardLogs:296
 * and portal-client.ts:509's pre-existing "cannot be safely split" hard stop.
 */
function shardTxRequests(
  transactions: TxRequest[],
  envelope: (transactions: TxRequest[]) => PortalQuery,
): PortalQuery[] {
  if (transactions.length === 0) return [];

  // A shard with ALL of `transactions` — the common (below-the-wall) case. One serialize; if it fits,
  // the single shard wraps the SAME `transactions` array (by reference, order preserved) — so its
  // serialized body is byte-identical to what txQuery() produces from the same TxRequest[] value (NOT
  // the same `===` object: txQuery() re-derives its own array from txRequestsFor; see the FetchSpec doc).
  const whole = envelope(transactions);
  if (JSON.stringify(whole).length < SHARD_BODY_BUDGET) return [whole];

  // STEP 1 — chunk over-cap requests so every element fits (restores "single element always fits").
  const chunked = transactions.flatMap((req) => chunkTxRequest(req, envelope));

  const shards: PortalQuery[] = [];
  let current: TxRequest[] = [];
  for (const entry of chunked) {
    // Guarantee (a) has teeth: a shard never starts smaller than one element, so an element whose own
    // envelope is still ≥ budget (both-sides-huge, chunkTxRequest could not reduce it) would silently
    // emit an oversized shard. Fail loud, attributably, at plan-build — NOT a 400 at stream time. Mirrors
    // shardLogs:296 and portal-client.ts:509. Never fires below the wall (that returned `[whole]` above).
    const entrySize = JSON.stringify(envelope([entry])).length;
    invariant(
      '#196',
      entrySize < SHARD_BODY_BUDGET,
      `#196: a single tx-filter request (${entrySize}B) exceeds SHARD_BODY_BUDGET (${SHARD_BODY_BUDGET}B) after chunking — a PORTAL_MAX_ADDRESSES-sized chunk of the dominant from/to side PLUS the whole other side still overflows the budget (adaptive sub-PORTAL_MAX_ADDRESSES chunking is deliberately out of scope — a throughput enhancement, field-unreachable today); narrow or split the filter`,
      () => ({ entrySize, budget: SHARD_BODY_BUDGET }),
    );

    if (current.length === 0) {
      current.push(entry);
      continue;
    }

    const withEntry = [...current, entry];
    if (JSON.stringify(envelope(withEntry)).length < SHARD_BODY_BUDGET) {
      current = withEntry;
      continue;
    }

    shards.push(envelope(current));
    current = [entry];
  }
  if (current.length > 0) shards.push(envelope(current));

  return shards;
}

/** The unique factories referenced by any filter (deduped by id). */
export const uniqueFactories = (
  eventCallbacks: { filter: Filter }[],
): Factory[] => [
  ...new Map(
    eventCallbacks
      .flatMap((e) => getFilterFactories(e.filter))
      .map((f) => [f.id, f]),
  ).values(),
];

/**
 * Build the merged Portal `/stream` log filter for a chain's REALTIME: every log filter's
 * address+topics PLUS a discovery request per factory (factory address + child-event selector), so new
 * children are streamed and pruned/matched downstream. Historical discovery is separate (portal-discovery).
 */
export function buildPortalLogRequests(
  eventCallbacks: { filter: Filter }[],
  childAddresses: ChildAddresses,
): PortalLogRequest[] {
  const reqs: PortalLogRequest[] = [];
  for (const e of eventCallbacks)
    if (e.filter.type === 'log')
      reqs.push(...logRequestsFor(e.filter, childAddresses));
  for (const factory of uniqueFactories(eventCallbacks)) {
    reqs.push({
      address: asArr(factory.address),
      topic0: [factory.eventSelector.toLowerCase()],
    });
  }
  return mergeLogRequests(reqs);
}

/**
 * The frozen per-chain fetch-spec. `id` is a stable identity (INV-1): every cached chunk records it and
 * a cache hit asserts the chunk was built under THIS spec, never a per-call subset. The `*Query()`
 * builders read the live `childAddresses` map, so factory-child expansion reflects discovery.
 */
export type FetchSpec = Readonly<{
  id: symbol;
  logFilters: readonly LogFilter[];
  factories: readonly Factory[];
  traceFilters: readonly TraceFilter[];
  transferFilters: readonly TransferFilter[];
  blockFilters: readonly BlockFilter[];
  transactionFilters: readonly TransactionFilter[];
  needReceipts: boolean;
  needBlocks: boolean;
  needTxFilter: boolean;
  needTraces: boolean;
  backfillStart: number;
  backfillEnd: number | undefined;
  /** Log-data query (matched logs + parent transactions); undefined when no log requests expand.
   * Equals `logQueryShards()[0]` when the whole body is < SHARD_BODY_BUDGET; kept as the single-body
   * view for callers/tests that don't shard. */
  logQuery(): PortalQuery | undefined;
  /** #194: byte-budgeted partition of `logQuery()`'s address union into ≥1 shards, each a full
   * PortalQuery with IDENTICAL fields/topics and a DISJOINT address subset whose serialized body is
   * strictly < SHARD_BODY_BUDGET < MAX_RAW_QUERY_SIZE. Empty ⇒ [] (same as logQuery undefined).
   * When the whole body is < SHARD_BODY_BUDGET it yields EXACTLY ONE shard byte-identical to logQuery()
   * — the no-op. A body in the SHARD_SAFETY_MARGIN band (SHARD_BODY_BUDGET ≤ body < MAX_RAW_QUERY_SIZE)
   * still shards into >1 shard (completeness-preserving, not a single-body no-op). portal.ts streams
   * these sequentially and UNIONs the rows (INV-11). */
  logQueryShards(): PortalQuery[];
  /** Full-trace query (fetch-all, ranked+filtered client-side — INV-5); undefined when !needTraces. */
  traceQuery(): PortalQuery | undefined;
  /** includeAllBlocks header scan for block-interval sources; undefined when !needBlocks. */
  blockQuery(): PortalQuery | undefined;
  /** Account-transaction from/to query; undefined when !needTxFilter or no from/to sets.
   * Byte-identical to `txQueryShards()[0]` when the whole body is < SHARD_BODY_BUDGET (each method
   * re-derives its `transactions` from `txRequestsFor` independently — a distinct array instance whose
   * serialized body is identical, NOT the same `===` reference); kept as the single-body view for
   * callers/tests that don't shard. */
  txQuery(): PortalQuery | undefined;
  /** #196: byte-budgeted partition of `txQuery()`'s tx-filter body into ≥1 shards, each a full
   * PortalQuery with IDENTICAL fields and a subset of the `transactions: TxRequest[]` array whose
   * serialized body is strictly < SHARD_BODY_BUDGET < MAX_RAW_QUERY_SIZE. Empty ⇒ [] (same as txQuery
   * undefined). When the whole body is < SHARD_BODY_BUDGET it yields EXACTLY ONE shard byte-identical to
   * txQuery() — the no-op (the #194-style guarantee for the whole validated corpus). Above the wall, a
   * TxRequest whose own envelope overflows is first CHUNKED (its dominant from/to side split into
   * disjoint PORTAL_MAX_ADDRESSES batches, the other side kept whole) so every resulting element fits,
   * then the elements are bin-packed into byte-budgeted shards. The chunked side is DISJOINT across the
   * chunks of one request and their union == the original side, so a tx matches exactly one chunk (AND
   * semantics + client re-match + assemble's seenTx dedup make the shard union complete + non-dup by
   * construction — see shardTxRequests). portal.ts streams these sequentially and UNIONs the rows. */
  txQueryShards(): PortalQuery[];
}>;

/** Trace-index parity (INV-5): fetch EVERY trace (no server-side trace filter) so buildTraces ranks over
 * the FULL tree, THEN client-filters — a matched trace keeps its true DFS position. */
const traceRequests = (): TraceRequest[] => [{ transaction: true }];

const txRequestsFor = (
  transactionFilters: readonly TransactionFilter[],
  childAddresses: ChildAddresses,
): TxRequest[] => {
  const addrsOf = (
    a: TransactionFilter['fromAddress'],
  ): string[] | undefined => {
    if (a === undefined) return undefined;
    if (isAddressFactory(a))
      return Array.from(childAddresses.get(a.id)?.keys() ?? []);
    return (Array.isArray(a) ? a : [a]).map((x) => x.toLowerCase());
  };
  const reqs: TxRequest[] = [];
  for (const f of transactionFilters) {
    const req: TxRequest = {};
    const from = addrsOf(f.fromAddress);
    if (from?.length) req.from = from;
    const to = addrsOf(f.toAddress);
    if (to?.length) req.to = to;
    if (req.from || req.to) reqs.push(req); // skip match-all (never fetch every tx)
  }
  return reqs;
};

export function compileFetchSpec(
  eventCallbacks: { filter: Filter }[],
  childAddresses: ChildAddresses,
): FetchSpec {
  const fs = eventCallbacks.map((e) => e.filter);
  const logFilters = fs.filter((f): f is LogFilter => f.type === 'log');
  const factories = uniqueFactories(eventCallbacks);
  const needReceipts = fs.some((f) => f.hasTransactionReceipt === true);
  const blockFilters = fs.filter((f): f is BlockFilter => f.type === 'block');
  const transactionFilters = fs.filter(
    (f): f is TransactionFilter => f.type === 'transaction',
  );
  const traceFilters = fs.filter((f): f is TraceFilter => f.type === 'trace');
  const transferFilters = fs.filter(
    (f): f is TransferFilter => f.type === 'transfer',
  );
  const needBlocks = blockFilters.length > 0;
  const needTxFilter = transactionFilters.length > 0;
  const needTraces = traceFilters.length + transferFilters.length > 0;

  // the chain's actual backfill window, from the filters — used to bound chunk fetches so a bounded
  // backfill (or the tail) never over-fetches. Fully automatic; no client tuning.
  // A source with NO fromBlock starts at genesis (ponder's runtime reads `filter.fromBlock ?? 0`); if
  // ANY source omits it the floor MUST be 0 — symmetric with backfillEnd's undefined-⇒-unbounded rule
  // below. Taking Math.min over only the DEFINED fromBlocks would clamp every chunk fetch past the
  // earliest DEFINED start (chunkRange, portal-chunks.ts) and mark the [0, min) prefix of an unbounded
  // source synced over a permanent silent gap — the whole pre-min history lost. (INV-16 / C10)
  const froms = fs.map((f) => f.fromBlock);
  const backfillStart =
    froms.length && froms.every((b) => b != null)
      ? Math.min(...(froms as number[]))
      : 0;
  const tos = fs.map((f) => f.toBlock);
  const backfillEnd =
    tos.length && tos.every((t) => t != null)
      ? Math.max(...(tos as number[]))
      : undefined;

  const txFields = (): FieldMap =>
    needReceipts ? { ...TX_FIELDS, ...RECEIPT_FIELDS } : TX_FIELDS;

  // The full merged+batched log-request array (reads the LIVE childAddresses map, so factory expansion
  // reflects discovery). logQuery() wraps it whole; logQueryShards() partitions it byte-budgeted. Both
  // build from THIS single source so the single-shard case is byte-identical to logQuery() (#194 no-op).
  const mergedLogRequests = (): PortalLogRequest[] =>
    mergeLogRequests(
      logFilters.flatMap((f) => logRequestsFor(f, childAddresses)),
    ).map((r) => ({ ...r, transaction: true }));

  const logEnvelope = (logs: PortalLogRequest[]): PortalQuery => ({
    type: 'evm',
    fields: {
      block: BLOCK_FIELDS,
      log: LOG_FIELDS,
      transaction: txFields(),
    },
    logs,
  });

  // The tx-filter query envelope. txQuery() wraps the whole TxRequest[] via this; txQueryShards()
  // partitions it byte-budgeted through the SAME envelope so the single-shard case is byte-identical to
  // txQuery() (#196 no-op) — exactly how logQuery()/logQueryShards() share logEnvelope. Receipt columns
  // ALWAYS ride the tx query (see the note in txQuery below).
  const txEnvelope = (transactions: TxRequest[]): PortalQuery => ({
    type: 'evm',
    fields: {
      block: BLOCK_FIELDS,
      transaction: { ...TX_FIELDS, ...RECEIPT_FIELDS },
    },
    transactions,
  });

  const spec: FetchSpec = {
    id: Symbol('portal-fetch-spec'),
    logFilters,
    factories,
    traceFilters,
    transferFilters,
    blockFilters,
    transactionFilters,
    needReceipts,
    needBlocks,
    needTxFilter,
    needTraces,
    backfillStart,
    backfillEnd,
    logQuery: () => {
      const logs = mergedLogRequests();
      if (logs.length === 0) return undefined;

      return logEnvelope(logs);
    },
    logQueryShards: () => shardLogs(mergedLogRequests(), logEnvelope),
    traceQuery: () => {
      if (!needTraces) return undefined;
      return {
        type: 'evm',
        fields: {
          block: BLOCK_FIELDS,
          trace: TRACE_FIELDS,
          transaction: txFields(),
        },
        traces: traceRequests(),
      };
    },
    blockQuery: () => {
      if (!needBlocks) return undefined;
      return {
        type: 'evm',
        includeAllBlocks: true,
        fields: { block: BLOCK_FIELDS },
      };
    },
    txQuery: () => {
      if (!needTxFilter) return undefined;
      const transactions = txRequestsFor(transactionFilters, childAddresses);
      if (transactions.length === 0) return undefined;
      // Receipt columns ALWAYS ride the tx query, regardless of hasTransactionReceipt — hardening,
      // not a reachable bug today: upstream types TransactionFilter.hasTransactionReceipt as the
      // literal `true` and builds every account tx filter with it set, so needReceipts already
      // projected these columns whenever a tx filter existed. But ponder's buildEvents dereferences
      // `transactionReceipt.status` (a positional cursor over the stored receipts, NO identity
      // check) on EVERY transaction event to apply `includeReverted`, and the RPC path collects
      // these receipts unconditionally — if upstream ever relaxed that literal-true invariant, a tx
      // row without its receipt row would crash buildEvents (no receipts at all) or silently
      // mis-match a neighbor's receipt (sparse receipts). Projecting unconditionally makes the tx
      // query correct on its own, without depending on that upstream invariant. Built from txEnvelope
      // so it is byte-identical to txQueryShards()[0] below the wall (#196 no-op).
      return txEnvelope(transactions);
    },
    txQueryShards: () => {
      if (!needTxFilter) return [];
      const transactions = txRequestsFor(transactionFilters, childAddresses);

      return transactions.length === 0
        ? []
        : shardTxRequests(transactions, txEnvelope);
    },
  };
  return Object.freeze(spec);
}
