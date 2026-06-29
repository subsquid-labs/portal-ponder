/**
 * Ponder filter set  →  ONE union Portal query.
 *
 * The proxy path's fatal flaw was ×17 amplification: Ponder issues one
 * eth_getLogs *per event topic per contract group*, and the proxy can only see
 * them one at a time. Here we have the whole picture for an interval up front,
 * so every log/trace/tx filter becomes an OR'd element of a single Portal query
 * = one stream. Address sets beyond Portal's 1000/filter cap split into extra
 * OR'd `logs[]` entries (still one stream). Fields are projected to exactly what
 * the sync-store needs.
 */
import type { PortalEvmQuery, PortalLogRequest, PortalTraceRequest, PortalFieldSelection } from "./portal-types.ts";

export type Topic = string | string[] | null | undefined;

/** Ponder-shaped log filter. */
export type LogFilter = {
  address?: string | string[];
  topic0?: Topic;
  topic1?: Topic;
  topic2?: Topic;
  topic3?: Topic;
  /** also fetch the parent transaction + receipt for matches */
  includeTransaction?: boolean;
};

export type TraceFilter = {
  fromAddress?: string[];
  toAddress?: string[];
  functionSelector?: string[];
};

export type Interval = [number, number];

export type BuildOptions = {
  /** project receipt fields onto transactions (Ponder includeTransactionReceipts) */
  receipts?: boolean;
  /** include traces query */
  traces?: TraceFilter[];
  /** Portal address-per-filter cap */
  maxAddresses?: number;
};

const PORTAL_MAX_ADDRESSES = 1000;

const asArray = (t: Topic): string[] | undefined => {
  if (t === null || t === undefined) return undefined;
  return Array.isArray(t) ? t : [t];
};

const lower = (s: string) => s.toLowerCase();

/** Full field projections matching Ponder's sync-store columns. */
export const FIELDS = {
  block: {
    number: true, hash: true, parentHash: true, timestamp: true, logsBloom: true,
    miner: true, gasUsed: true, gasLimit: true, baseFeePerGas: true, nonce: true,
    mixHash: true, stateRoot: true, receiptsRoot: true, transactionsRoot: true,
    sha3Uncles: true, size: true, difficulty: true, totalDifficulty: true, extraData: true,
  } as Record<string, boolean>,
  log: { address: true, topics: true, data: true, transactionHash: true, transactionIndex: true, logIndex: true } as Record<string, boolean>,
  transactionBase: {
    transactionIndex: true, hash: true, from: true, to: true, input: true, value: true,
    nonce: true, gas: true, gasPrice: true, maxFeePerGas: true, maxPriorityFeePerGas: true,
    type: true, r: true, s: true, v: true, yParity: true,
  } as Record<string, boolean>,
  receipt: {
    status: true, gasUsed: true, cumulativeGasUsed: true, effectiveGasPrice: true,
    contractAddress: true, logsBloom: true,
  } as Record<string, boolean>,
  trace: {
    transactionIndex: true, traceAddress: true, subtraces: true, type: true, error: true, revertReason: true,
    callFrom: true, callTo: true, callValue: true, callGas: true, callInput: true, callSighash: true, callType: true,
    callResultGasUsed: true, callResultOutput: true,
    createFrom: true, createValue: true, createGas: true, createInit: true,
    createResultGasUsed: true, createResultCode: true, createResultAddress: true,
  } as Record<string, boolean>,
};

/** Split one log filter into >=1 Portal logs[] entries respecting the address cap. */
const expandLogFilter = (f: LogFilter, maxAddresses: number): PortalLogRequest[] => {
  const base: PortalLogRequest = {};
  const t0 = asArray(f.topic0); if (t0) base.topic0 = t0.map(lower);
  const t1 = asArray(f.topic1); if (t1) base.topic1 = t1.map(lower);
  const t2 = asArray(f.topic2); if (t2) base.topic2 = t2.map(lower);
  const t3 = asArray(f.topic3); if (t3) base.topic3 = t3.map(lower);
  if (f.includeTransaction) base.transaction = true;

  if (f.address === undefined) return [base];
  const addrs = (Array.isArray(f.address) ? f.address : [f.address]).map(lower);
  if (addrs.length === 0) return []; // factory with no children yet → contributes nothing
  const out: PortalLogRequest[] = [];
  for (let i = 0; i < addrs.length; i += maxAddresses) {
    out.push({ ...base, address: addrs.slice(i, i + maxAddresses) });
  }
  return out;
};

const expandTraceFilter = (f: TraceFilter): PortalTraceRequest => {
  const r: PortalTraceRequest = {};
  if (f.fromAddress?.length) r.callFrom = f.fromAddress.map(lower);
  if (f.toAddress?.length) r.callTo = f.toAddress.map(lower);
  if (f.functionSelector?.length) r.callSighash = f.functionSelector.map(lower);
  r.transaction = true;
  return r;
};

/**
 * Build the single union query for an interval. `logFilters` is the full set
 * for the chain (each source's discovery + child + singleton filters), already
 * resolved with current child addresses.
 */
export const buildPortalQuery = (
  interval: Interval,
  logFilters: LogFilter[],
  opts: BuildOptions = {},
): PortalEvmQuery => {
  const maxAddresses = opts.maxAddresses ?? PORTAL_MAX_ADDRESSES;

  const logs: PortalLogRequest[] = logFilters.flatMap((f) => expandLogFilter(f, maxAddresses));

  const needTx = logFilters.some((f) => f.includeTransaction) || (opts.traces?.length ?? 0) > 0;

  const fields: PortalFieldSelection = { block: FIELDS.block, log: FIELDS.log };
  if (needTx) {
    fields.transaction = opts.receipts ? { ...FIELDS.transactionBase, ...FIELDS.receipt } : { ...FIELDS.transactionBase };
  }

  const query: PortalEvmQuery = {
    type: "evm",
    fromBlock: interval[0],
    toBlock: interval[1],
    fields,
  };
  if (logs.length > 0) query.logs = logs;

  if (opts.traces?.length) {
    query.traces = opts.traces.map(expandTraceFilter);
    fields.trace = FIELDS.trace;
  }

  return query;
};
