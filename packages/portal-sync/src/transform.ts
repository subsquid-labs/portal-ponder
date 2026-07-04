/**
 * Portal NDJSON  →  Ponder Sync* rows (viem RPC shapes).
 *
 * Two normalizations, both verified live against ethereum-mainnet block 21M:
 *  1. Portal returns small ints decimal (number, timestamp, status, type,
 *     transactionIndex, logIndex) and big values hex — viem wants all hex.
 *  2. Traces arrive Parity/trace_block-style (action/result + traceAddress);
 *     Ponder wants geth callTracer `CallFrame`. We convert type/callType and
 *     assign a DFS traceIndex + subcalls. CREATE vs CREATE2 is unresolvable in
 *     the Parity model (emit CREATE) — but Ponder's trace filters ignore type
 *     (runtime/filter.ts:280), so indexing is unaffected.
 */
import { numberToHex } from 'viem';
import type { PortalBlock } from './portal-types.ts';

type Hex = `0x${string}`;

/** number|hex|bigint → 0x-hex; pass through existing hex; undefined stays undefined. */
const hx = (v: unknown): Hex | undefined => {
  if (v === undefined || v === null) return undefined;
  if (typeof v === 'string')
    return (v.startsWith('0x') ? v : numberToHex(BigInt(v))) as Hex;
  if (typeof v === 'number' || typeof v === 'bigint')
    return numberToHex(v) as Hex;
  return undefined;
};
const hxN = (v: unknown): Hex => hx(v) ?? '0x0';

export type SyncLog = {
  address: Hex;
  topics: Hex[];
  data: Hex;
  blockNumber: Hex;
  blockHash: Hex;
  transactionHash: Hex;
  transactionIndex: Hex;
  logIndex: Hex;
  removed: false;
};

export type SyncBlock = Record<string, unknown> & {
  number: Hex;
  hash: Hex;
  transactions: unknown[];
};

export const toSyncBlock = (b: PortalBlock): SyncBlock => {
  const h = b.header;
  return {
    number: hxN(h.number),
    hash: (h.hash as Hex) ?? '0x',
    parentHash: (h.parentHash as Hex) ?? '0x',
    timestamp: hxN(h.timestamp),
    nonce: h.nonce,
    logsBloom: h.logsBloom,
    miner: h.miner,
    gasUsed: hx(h.gasUsed),
    gasLimit: hx(h.gasLimit),
    baseFeePerGas: hx(h.baseFeePerGas),
    size: hx(h.size),
    stateRoot: h.stateRoot,
    receiptsRoot: h.receiptsRoot,
    transactionsRoot: h.transactionsRoot,
    sha3Uncles: h.sha3Uncles,
    mixHash: h.mixHash,
    difficulty: hx(h.difficulty),
    totalDifficulty: hx(h.totalDifficulty),
    extraData: h.extraData,
    transactions: [],
  };
};

export const toSyncLog = (log: any, header: PortalBlock['header']): SyncLog => {
  const topics = (log.topics ?? []) as Hex[];
  return {
    address: (log.address as string).toLowerCase() as Hex,
    topics,
    data: (log.data as Hex) ?? '0x',
    blockNumber: hxN(header.number),
    blockHash: (header.hash as Hex) ?? '0x',
    transactionHash: log.transactionHash as Hex,
    transactionIndex: hxN(log.transactionIndex),
    logIndex: hxN(log.logIndex),
    removed: false,
  };
};

export const toSyncTransaction = (tx: any, header: PortalBlock['header']) => ({
  blockNumber: hxN(header.number),
  blockHash: (header.hash as Hex) ?? '0x',
  transactionIndex: hxN(tx.transactionIndex),
  hash: tx.hash as Hex,
  from: (tx.from as string)?.toLowerCase(),
  to: tx.to ? (tx.to as string).toLowerCase() : null,
  input: tx.input,
  value: hx(tx.value),
  nonce: typeof tx.nonce === 'number' ? tx.nonce : Number(tx.nonce),
  gas: hx(tx.gas),
  gasPrice: hx(tx.gasPrice),
  maxFeePerGas: hx(tx.maxFeePerGas),
  maxPriorityFeePerGas: hx(tx.maxPriorityFeePerGas),
  type: hx(tx.type),
  r: tx.r,
  s: tx.s,
  v: hx(tx.v),
  yParity: tx.yParity !== undefined ? hx(tx.yParity) : undefined,
});

export const toSyncReceipt = (tx: any, header: PortalBlock['header']) => ({
  blockNumber: hxN(header.number),
  blockHash: (header.hash as Hex) ?? '0x',
  transactionIndex: hxN(tx.transactionIndex),
  transactionHash: tx.hash as Hex,
  from: (tx.from as string)?.toLowerCase(),
  to: tx.to ? (tx.to as string).toLowerCase() : null,
  contractAddress: tx.contractAddress
    ? (tx.contractAddress as string).toLowerCase()
    : null,
  status: tx.status === 1 || tx.status === '0x1' ? '0x1' : '0x0',
  gasUsed: hx(tx.gasUsed),
  cumulativeGasUsed: hx(tx.cumulativeGasUsed),
  effectiveGasPrice: hx(tx.effectiveGasPrice),
  logsBloom: tx.logsBloom,
  type: hx(tx.type),
});

/** geth callTracer CallFrame, flattened to a Ponder trace row. */
export type SyncTrace = {
  trace: {
    type: string;
    from: Hex;
    to?: Hex;
    value?: Hex;
    gas: Hex;
    gasUsed: Hex;
    input: Hex;
    output?: Hex;
    error?: string;
    revertReason?: string;
    index: number;
    subcalls: number;
  };
  transactionHash: Hex;
};

const CALL_TYPE: Record<string, string> = {
  call: 'CALL',
  delegatecall: 'DELEGATECALL',
  staticcall: 'STATICCALL',
  callcode: 'CALLCODE',
};

// lexicographic, parent-before-child: [] < [0] < [0,0] < [1]
const cmpTraceAddr = (a: number[], b: number[]): number => {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i]! - b[i]!;
  return a.length - b.length;
};

/** Convert all Parity traces of a block into flat callTracer rows (per tx, DFS-indexed). */
export const toSyncTraces = (block: PortalBlock): SyncTrace[] => {
  const raw = (block.traces ?? []) as any[];
  if (raw.length === 0) return [];
  const txHashByIndex = new Map<number, Hex>();
  for (const tx of block.transactions ?? [])
    txHashByIndex.set((tx as any).transactionIndex, (tx as any).hash);

  const byTx = new Map<number, any[]>();
  for (const t of raw) {
    const k = t.transactionIndex ?? 0;
    if (!byTx.has(k)) byTx.set(k, []);
    byTx.get(k)!.push(t);
  }

  const out: SyncTrace[] = [];
  for (const [txIndex, traces] of byTx) {
    traces.sort((a, b) =>
      cmpTraceAddr(a.traceAddress ?? [], b.traceAddress ?? []),
    );
    traces.forEach((t, i) => {
      const frame = parityToCallFrame(t, i);
      if (frame)
        out.push({
          trace: frame,
          transactionHash: txHashByIndex.get(txIndex) ?? ('0x' as Hex),
        });
    });
  }
  return out;
};

const parityToCallFrame = (
  t: any,
  index: number,
): SyncTrace['trace'] | undefined => {
  const a = t.action ?? {};
  const r = t.result ?? {};
  let type: string;
  let to: Hex | undefined, input: Hex, output: Hex | undefined;
  if (t.type === 'call') {
    type = CALL_TYPE[a.type as string] ?? 'CALL';
    to = a.to;
    input = a.input ?? '0x';
    output = r.output;
  } else if (t.type === 'create') {
    type = 'CREATE'; // CREATE2 indistinguishable in Parity model (Ponder ignores trace type in filters)
    to = r.address;
    input = a.init ?? '0x';
    output = r.code;
  } else if (t.type === 'suicide') {
    type = 'SELFDESTRUCT';
    to = a.refundAddress;
    input = '0x';
  } else {
    return undefined; // "reward" has no callTracer equivalent
  }
  return {
    type,
    from: (a.from as string)?.toLowerCase() as Hex,
    to: to ? ((to as string).toLowerCase() as Hex) : undefined,
    value: hx(a.value),
    gas: hxN(a.gas),
    gasUsed: hxN(r.gasUsed),
    input,
    output,
    error: t.error ?? undefined,
    revertReason: t.revertReason ?? undefined,
    index,
    subcalls: t.subtraces ?? 0,
  };
};
