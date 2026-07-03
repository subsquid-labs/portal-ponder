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
  /** Log-data query (matched logs + parent transactions); undefined when no log requests expand. */
  logQuery(): PortalQuery | undefined;
  /** Full-trace query (fetch-all, ranked+filtered client-side — INV-5); undefined when !needTraces. */
  traceQuery(): PortalQuery | undefined;
  /** includeAllBlocks header scan for block-interval sources; undefined when !needBlocks. */
  blockQuery(): PortalQuery | undefined;
  /** Account-transaction from/to query; undefined when !needTxFilter or no from/to sets. */
  txQuery(): PortalQuery | undefined;
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
      const logs = mergeLogRequests(
        logFilters.flatMap((f) => logRequestsFor(f, childAddresses)),
      ).map((r) => ({ ...r, transaction: true }));
      if (logs.length === 0) return undefined;
      return {
        type: 'evm',
        fields: {
          block: BLOCK_FIELDS,
          log: LOG_FIELDS,
          transaction: txFields(),
        },
        logs,
      };
    },
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
      return {
        type: 'evm',
        fields: { block: BLOCK_FIELDS, transaction: txFields() },
        transactions,
      };
    },
  };
  return Object.freeze(spec);
}
