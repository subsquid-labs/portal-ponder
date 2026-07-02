/**
 * Pure transforms: SQD Portal NDJSON → Ponder Sync* / geth callTracer shapes.
 *
 * Portal uses a SPLIT encoding (verified live, block 21,000,000): small bounded
 * values are DECIMAL numbers (status, type, transactionIndex, nonce, chainId,
 * yParity, traceAddress[], subtraces, block number/timestamp) while 256-bit /
 * byte quantities are HEX strings (gas*, value, effectiveGasPrice, v/r/s, all
 * addresses/hashes, and ALL trace action/result quantities). So e.g. `status` is
 * `1` not `"0x1"`, but `gasUsed` is `"0x317fa"`. These functions normalise to
 * viem RPC conventions. Kept pure + exported so they're unit-testable against
 * real captured fixtures.
 */

import { type Address, type Hex, numberToHex, toHex } from "viem";
import type {
  SyncBlockHeader,
  SyncLog,
  SyncTransaction,
  SyncTransactionReceipt,
} from "@/internal/types.js";

export type RawHeader = Record<string, any> & { number: number };

/** Traces are ~100x denser than logs; buffering a wide chunk's worth over a busy contract OOMs.
 * For trace-index parity we fetch the FULL (unfiltered) trace set, which is denser still, so the
 * default cap is conservative. When a chain has trace sources, cap the chunk to a trace-safe width
 * (PORTAL_TRACE_CHUNK_BLOCKS, default 2k). Pure so it's unit-testable. */
export const traceSafeChunkBlocks = (
  base: number,
  needTraces: boolean,
  cap = Number(process.env.PORTAL_TRACE_CHUNK_BLOCKS ?? 2_000),
): number => (needTraces && base > cap ? cap : base);

/** An interval reaching past Portal's finalized head must fall back to RPC for the gap.
 * (undefined head = not yet known → treat as no gap.) Pure so it's unit-testable. */
export const isFinalityGap = (
  intervalEnd: number,
  portalHead: number | undefined,
): boolean => portalHead !== undefined && intervalEnd > portalHead;

/** number|decimal-string|hex → 0x-hex; passes existing hex through. */
export const hx = (v: unknown): Hex => {
  if (typeof v === "string") {
    if (v === "0x" || v === "") return "0x0"; // empty quantity → 0 (never the invalid "0x")
    return (v.startsWith("0x") ? v : toHex(BigInt(v))) as Hex;
  }
  if (typeof v === "number" || typeof v === "bigint") return numberToHex(v);
  return "0x0";
};
export const opt = (v: unknown): Hex | undefined =>
  v === undefined || v === null ? undefined : hx(v);

export const toSyncLog = (l: any, h: RawHeader): SyncLog =>
  ({
    address: (l.address as string).toLowerCase(),
    topics: l.topics ?? [],
    data: l.data ?? "0x",
    blockNumber: hx(h.number),
    blockHash: h.hash,
    transactionHash: l.transactionHash,
    transactionIndex: hx(l.transactionIndex),
    logIndex: hx(l.logIndex),
    removed: false,
  }) as unknown as SyncLog;

export const toSyncBlockHeader = (h: RawHeader): SyncBlockHeader =>
  ({
    number: hx(h.number),
    hash: h.hash,
    parentHash: h.parentHash,
    timestamp: hx(h.timestamp),
    logsBloom: h.logsBloom,
    miner: h.miner,
    gasUsed: opt(h.gasUsed),
    gasLimit: opt(h.gasLimit),
    baseFeePerGas: opt(h.baseFeePerGas),
    nonce: h.nonce,
    mixHash: h.mixHash,
    stateRoot: h.stateRoot,
    receiptsRoot: h.receiptsRoot,
    transactionsRoot: h.transactionsRoot,
    sha3Uncles: h.sha3Uncles,
    size: opt(h.size),
    difficulty: opt(h.difficulty),
    totalDifficulty: opt(h.totalDifficulty),
    extraData: h.extraData,
    transactions: undefined,
  }) as unknown as SyncBlockHeader;

export const toSyncTransaction = (tx: any, h: RawHeader): SyncTransaction =>
  ({
    blockHash: h.hash,
    blockNumber: hx(h.number),
    from: (tx.from as string)?.toLowerCase(),
    to: tx.to ? (tx.to as string).toLowerCase() : null,
    gas: hx(tx.gas),
    hash: tx.hash,
    input: tx.input ?? "0x",
    nonce: hx(tx.nonce ?? 0),
    transactionIndex: hx(tx.transactionIndex),
    value: hx(tx.value ?? 0),
    type: hx(tx.type ?? 0),
    gasPrice: opt(tx.gasPrice),
    maxFeePerGas: opt(tx.maxFeePerGas),
    maxPriorityFeePerGas: opt(tx.maxPriorityFeePerGas),
    v: opt(tx.v),
    r: tx.r,
    s: tx.s,
    yParity:
      tx.yParity !== undefined && tx.yParity !== null
        ? hx(tx.yParity)
        : undefined,
    // accessList exists only on typed txs (EIP-2930/1559/4844 → type ≥ 1); legacy (type 0) has none.
    // Portal returns [] regardless, so normalize to match the RPC path: legacy → undefined (null).
    accessList: Number(tx.type) >= 1 ? (tx.accessList ?? []) : undefined,
  }) as unknown as SyncTransaction;

/** receipt fields ride on Portal's transaction object; status/type are DECIMAL → hex. */
export const toSyncReceipt = (tx: any, h: RawHeader): SyncTransactionReceipt =>
  ({
    blockNumber: hx(h.number),
    blockHash: h.hash,
    transactionIndex: hx(tx.transactionIndex),
    transactionHash: tx.hash,
    from: (tx.from as string)?.toLowerCase(),
    to: tx.to ? (tx.to as string).toLowerCase() : null,
    contractAddress: tx.contractAddress
      ? (tx.contractAddress as string).toLowerCase()
      : null,
    // logsBloom is NOT NULL + bloom-load-bearing; never substitute. A dataset that lacks it fails
    // loudly at fetch (stream() field-degradation), so this is a straight passthrough.
    logsBloom: tx.logsBloom,
    gasUsed: hx(tx.gasUsed),
    cumulativeGasUsed: hx(tx.cumulativeGasUsed),
    effectiveGasPrice: hx(tx.effectiveGasPrice),
    status:
      tx.status === 1 || tx.status === "0x1" || tx.status === true
        ? "0x1"
        : "0x0",
    type: hx(tx.type ?? 0),
  }) as unknown as SyncTransactionReceipt;

export const CALL_TYPE: Record<string, string> = {
  call: "CALL",
  delegatecall: "DELEGATECALL",
  staticcall: "STATICCALL",
  callcode: "CALLCODE",
};

/** lexicographic, parent-before-child: [] < [0] < [0,0] < [1] — i.e. DFS pre-order. */
export const cmpTraceAddr = (a: number[], b: number[]): number => {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i]! - b[i]!;
  return a.length - b.length;
};

/** Portal Parity trace → geth callTracer CallFrame (+ index/subcalls). Reads nested
 * action/result; callType lives at action.callType. CREATE2 is indistinguishable. */
export const parityToCallFrame = (t: any, index: number): any | undefined => {
  const a = t.action ?? {},
    r = t.result ?? {};
  let type: string,
    from: any,
    to: any,
    value: any,
    gas: any,
    gasUsed: any,
    input: any,
    output: any;
  if (t.type === "call") {
    type =
      CALL_TYPE[
        (a.callType ?? a.type ?? t.callCallType ?? t.callType) as string
      ] ?? "CALL";
    from = a.from ?? t.callFrom;
    to = a.to ?? t.callTo;
    value = a.value ?? t.callValue;
    gas = a.gas ?? t.callGas;
    gasUsed = r.gasUsed ?? t.callResultGasUsed;
    input = a.input ?? t.callInput ?? "0x";
    output = r.output ?? t.callResultOutput;
  } else if (t.type === "create") {
    type = "CREATE";
    from = a.from ?? t.createFrom;
    to = r.address ?? t.createResultAddress;
    value = a.value ?? t.createValue;
    gas = a.gas ?? t.createGas;
    gasUsed = r.gasUsed ?? t.createResultGasUsed;
    input = a.init ?? t.createInit ?? "0x";
    output = r.code ?? t.createResultCode;
  } else if (t.type === "suicide") {
    type = "SELFDESTRUCT";
    from = a.address ?? t.suicideAddress;
    to = a.refundAddress ?? t.suicideRefundAddress;
    value = a.balance ?? t.suicideBalance;
    gas = 0;
    gasUsed = 0;
    input = "0x";
  } else return undefined; // "reward" has no callTracer equivalent
  return {
    type,
    from: (from as string)?.toLowerCase() as Address,
    to: to ? ((to as string).toLowerCase() as Address) : undefined,
    value: opt(value),
    gas: hx(gas ?? 0),
    gasUsed: hx(gasUsed ?? 0),
    input: input ?? "0x",
    output: output ?? undefined,
    error: t.error ?? undefined,
    revertReason: t.revertReason ?? undefined,
    index,
    subcalls: t.subtraces ?? 0,
  };
};

/** State diffs — Portal has them; Ponder has no state-diff source today, so this is
 * engine-level only (available via the standalone client, not wired into HistoricalSync). */
export type StateDiff = {
  transactionIndex: number;
  address: Address;
  key: string;
  kind: "=" | "+" | "*" | "-";
  prev: Hex | null;
  next: Hex | null;
};
export const toStateDiff = (d: any): StateDiff => ({
  transactionIndex: d.transactionIndex,
  address: (d.address as string)?.toLowerCase() as Address,
  key: d.key,
  kind: d.kind,
  prev: d.prev ?? null,
  next: d.next ?? null,
});
