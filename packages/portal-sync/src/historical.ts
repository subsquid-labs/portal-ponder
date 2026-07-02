/**
 * createPortalHistoricalSync — a Portal-backed implementation of Ponder's
 * `HistoricalSync` interface (sync-historical/index.ts:70). Drops into
 * packages/core/src/sync-historical/portal.ts and is selected at
 * runtime/historical.ts:1224 when `chain.portal` is set; realtime stays on RPC.
 *
 * Why this seam (not a viem Transport): the interface is RANGE-oriented
 * (`syncBlockRangeData({interval})`), so one interval = one self-paced Portal
 * stream carrying the UNION of every filter — no per-request fan-out, no ×17
 * per-topic amplification, and it never touches Ponder's latency/backoff
 * machinery. The runtime's adaptive estimator grows intervals toward 100k
 * blocks, which only makes Portal faster.
 */

import type { PortalMetrics } from "./metrics.ts";
import { PortalClient } from "./portal-client.ts";
import {
  buildPortalQuery,
  type Interval,
  type LogFilter,
  type TraceFilter,
} from "./query.ts";
import {
  type SyncBlock,
  type SyncLog,
  type SyncTrace,
  toSyncBlock,
  toSyncLog,
  toSyncReceipt,
  toSyncTraces,
  toSyncTransaction,
} from "./transform.ts";

/** Subset of Ponder's SyncStore the Portal path writes (same method names/shapes). */
export interface SyncStore {
  insertLogs(p: { logs: SyncLog[]; chainId: number }): Promise<void> | void;
  insertBlocks(p: {
    blocks: SyncBlock[];
    chainId: number;
  }): Promise<void> | void;
  insertTransactions(p: {
    transactions: unknown[];
    chainId: number;
  }): Promise<void> | void;
  insertTransactionReceipts(p: {
    transactionReceipts: unknown[];
    chainId: number;
  }): Promise<void> | void;
  insertTraces(p: {
    traces: SyncTrace[];
    chainId: number;
  }): Promise<void> | void;
}

/** A factory whose children are discovered from a log and reused on later intervals. */
export type FactoryDiscovery = {
  name: string;
  factory: string;
  discoveryTopic0: string;
  /** topic index or data word that holds the child address */
  child: { kind: "topic"; index: number } | { kind: "data"; word: number };
  /** topic0s of the child contract's events to index */
  childTopic0s: string[];
};

export type PortalSources = {
  /** static log filters (non-factory contracts, singletons like EVC) */
  logFilters: LogFilter[];
  /** factory sources (discovery + child events) */
  factories: FactoryDiscovery[];
  traceFilters: TraceFilter[];
  /** also fetch blocks/txs/receipts for matched logs */
  includeReceipts: boolean;
};

export type CreatePortalHistoricalSyncParams = {
  chainId: number;
  dataset: string;
  baseUrl?: string;
  metrics?: PortalMetrics;
  sources: PortalSources;
  /** live, grows as factories discover children (mirrors Ponder's childAddresses) */
  childAddresses?: Map<string, Set<string>>;
};

export type HistoricalSync = {
  syncBlockRangeData(p: {
    interval: Interval;
    syncStore: SyncStore;
  }): Promise<SyncLog[]>;
  syncBlockData(p: {
    interval: Interval;
    logs: SyncLog[];
    syncStore: SyncStore;
  }): Promise<SyncBlock | undefined>;
};

const extractChild = (
  rule: FactoryDiscovery["child"],
  log: any,
): string | undefined => {
  if (rule.kind === "topic") {
    const t = log.topics?.[rule.index];
    return t ? ("0x" + t.slice(26)).toLowerCase() : undefined;
  }
  const data: string = log.data ?? "0x";
  const w = data.slice(2 + rule.word * 64, 2 + rule.word * 64 + 64);
  return w.length === 64 ? ("0x" + w.slice(24)).toLowerCase() : undefined;
};

export const createPortalHistoricalSync = (
  params: CreatePortalHistoricalSyncParams,
): HistoricalSync => {
  const client = new PortalClient({
    dataset: params.dataset,
    baseUrl: params.baseUrl,
    metrics: params.metrics,
  });
  const childAddresses =
    params.childAddresses ?? new Map<string, Set<string>>();
  for (const f of params.sources.factories)
    if (!childAddresses.has(f.name)) childAddresses.set(f.name, new Set());

  /** Resolve the chain's full filter set for an interval, with current children. */
  const logFiltersFor = (): LogFilter[] => {
    const filters: LogFilter[] = [...params.sources.logFilters];
    for (const f of params.sources.factories) {
      // discovery filter (so children created in this interval are caught)
      filters.push({ address: [f.factory], topic0: [f.discoveryTopic0] });
      // child-data filter over children discovered so far
      const addrs = [...childAddresses.get(f.name)!];
      if (addrs.length && f.childTopic0s.length)
        filters.push({
          address: addrs,
          topic0: f.childTopic0s,
          includeTransaction: params.sources.includeReceipts,
        });
    }
    return filters;
  };

  return {
    async syncBlockRangeData({ interval, syncStore }) {
      const query = buildPortalQuery(interval, logFiltersFor(), {
        receipts: params.sources.includeReceipts,
        traces: params.sources.traceFilters.length
          ? params.sources.traceFilters
          : undefined,
      });

      const syncLogs: SyncLog[] = [];
      const blocks: SyncBlock[] = [];
      const transactions: unknown[] = [];
      const receipts: unknown[] = [];
      const traces: SyncTrace[] = [];

      for await (const batch of client.streamFinalized(query)) {
        for (const b of batch.blocks) {
          if (b.logs?.length || b.transactions?.length || b.traces?.length)
            blocks.push(toSyncBlock(b));
          for (const log of b.logs ?? []) {
            syncLogs.push(toSyncLog(log, b.header));
            // factory child discovery
            for (const f of params.sources.factories) {
              if (
                (log.address as string).toLowerCase() === f.factory &&
                log.topics?.[0] === f.discoveryTopic0
              ) {
                const c = extractChild(f.child, log);
                if (c) childAddresses.get(f.name)!.add(c);
              }
            }
          }
          for (const tx of b.transactions ?? []) {
            transactions.push(toSyncTransaction(tx, b.header));
            if (params.sources.includeReceipts)
              receipts.push(toSyncReceipt(tx, b.header));
          }
          traces.push(...toSyncTraces(b));
        }
      }

      await syncStore.insertLogs({ logs: syncLogs, chainId: params.chainId });
      if (blocks.length)
        await syncStore.insertBlocks({ blocks, chainId: params.chainId });
      if (transactions.length)
        await syncStore.insertTransactions({
          transactions,
          chainId: params.chainId,
        });
      if (receipts.length)
        await syncStore.insertTransactionReceipts({
          transactionReceipts: receipts,
          chainId: params.chainId,
        });
      if (traces.length)
        await syncStore.insertTraces({ traces, chainId: params.chainId });

      return syncLogs;
    },

    // blocks/txs/receipts/traces are fetched in the same range stream above
    // (Portal returns them inline), so this is a light finalizer that reports
    // the closest-to-tip block. In core, the runtime's split is honored by
    // moving the block/tx/receipt writes here if preferred.
    async syncBlockData({ logs }) {
      if (logs.length === 0) return undefined;
      const last = logs[logs.length - 1]!;
      return {
        number: last.blockNumber,
        hash: last.blockHash,
        transactions: [],
      };
    },
  };
};
