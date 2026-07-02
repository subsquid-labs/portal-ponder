/**
 * Wiring glue: Portal-native realtime → Ponder's realtime runtime.
 *
 * `portal-realtime.ts` produces `PortalRealtimeEvent`s from the Portal `/stream`. THIS module adapts them
 * into ponder's own `RealtimeSyncEvent`s and drops into `runtime/realtime.ts` as an alternative to the
 * `rpc.subscribe`+`createRealtimeSync` path — so the downstream indexing/checkpointing is UNCHANGED.
 *
 * Everything here is inert unless a chain has a Portal source AND `PORTAL_REALTIME==="stream"`. When the
 * flag is unset/"rpc", `isPortalRealtime()` is false and `clampFinalizedToPortalHead()` is a pass-through,
 * so the RPC realtime path (the "A-path") is byte-for-byte unchanged.
 *
 * WHY the finality clamp: ponder computes `syncProgress.finalized` from RPC (`latest−finalityBlockCount`),
 * which can sit ABOVE the Portal's finalized head. If realtime streamed from below that finalized block it
 * would silently regress ponder's persisted checkpoints; if historical skipped its RPC finality-gap
 * fallback it would mark the gap "synced" with no data (a permanent silent hole). Both are avoided by
 * making the Portal's finalized head the finality boundary in stream mode: `clampFinalizedToPortalHead()`
 * lowers `syncProgress.finalized` to the Portal head, so (a) historical stops exactly at the Portal head
 * (no gap, the RPC fallback never triggers) and (b) realtime streams `[portal-head+1 → tip]` — every block
 * strictly ABOVE `finalized`, and every `finalize` monotonically at/above it.
 */
import type { Common } from "@/internal/common.js";
import type {
  Chain,
  EventCallback,
  Factory,
  FactoryId,
  Filter,
  LightBlock,
  LogFilter,
  SyncLog,
} from "@/internal/types.js";
import {
  getChildAddress,
  getFilterFactories,
  isAddressFactory,
  isLogFactoryMatched,
} from "@/runtime/filter.js";
import { eth_getBlockByNumber } from "@/rpc/actions.js";
import type { Rpc } from "@/rpc/index.js";
import type { RealtimeSyncEvent } from "@/sync-realtime/index.js";
import { type Address, hexToNumber, numberToHex } from "viem";
import {
  type Light,
  type PortalRealtimeEvent,
  portalRealtimeEvents,
} from "./portal-realtime.js";
import { hx } from "./portal-transform.js";

// ─────────────────────────────── flag / detection ───────────────────────────────

/** True when this chain should use the Portal `/stream` for realtime instead of ponder's RPC path. */
export const isPortalRealtime = (chain: { portal?: string | undefined }): boolean =>
  typeof chain.portal === "string" && process.env.PORTAL_REALTIME === "stream";

const portalHeaders = (): Record<string, string> => {
  const h: Record<string, string> = { "content-type": "application/json", "accept-encoding": "gzip" };
  if (process.env.PORTAL_API_KEY) h["x-api-key"] = process.env.PORTAL_API_KEY;
  return h;
};

const cleanUrl = (portal: string): string => portal.replace(/\/$/, "");

// ─────────────────────────────── Portal finalized head + finality clamp ───────────────────────────────

/** Poll the Portal `/finalized-head` (reused by both the historical finality-gap decision and here). */
export async function portalFinalizedHead(
  portalUrl: string,
  headers: Record<string, string>,
  fetchImpl: typeof fetch = fetch,
): Promise<number | undefined> {
  try {
    const h = await fetchImpl(`${cleanUrl(portalUrl)}/finalized-head`, { headers }).then((r) => r.json());
    if (typeof h?.number === "number") return h.number;
  } catch {
    /* head unknown → caller stays conservative */
  }
  return undefined;
}

/**
 * In stream mode, lower `finalizedBlock` to the Portal's finalized head (when the Portal lags ponder's
 * RPC-derived finalized block). Pass-through for the A-path (no portal / flag off) and when the Portal is
 * already at/ahead of the RPC finalized block — so it never RAISES the boundary. Called at every site that
 * sets `syncProgress.finalized`: the initial `getLocalSyncProgress` and the backfill-cutover refetch.
 */
export async function clampFinalizedToPortalHead(params: {
  chain: Chain;
  rpc: Rpc;
  finalizedBlock: LightBlock;
  common?: Common;
  fetchImpl?: typeof fetch;
}): Promise<LightBlock> {
  const { chain, rpc, finalizedBlock } = params;
  if (isPortalRealtime(chain) === false) return finalizedBlock;

  const portalUrl = cleanUrl(chain.portal!);
  const head = await portalFinalizedHead(portalUrl, portalHeaders(), params.fetchImpl);
  // Head unknown → stay conservative and keep the RPC finalized block (historical's own RPC finality-gap
  // fallback remains the safety net). Portal at/ahead of RPC finalized → nothing to clamp.
  if (head === undefined || head >= hexToNumber(finalizedBlock.number)) return finalizedBlock;

  const clamped = (await eth_getBlockByNumber(rpc, [numberToHex(head), false], {
    retryNullBlockRequest: true,
  })) as unknown as LightBlock;
  params.common?.logger.debug({
    service: "portal",
    msg: `Portal ${chain.name}: clamped realtime finalized ${hexToNumber(finalizedBlock.number)} → Portal head ${head} (stream mode)`,
  });
  return clamped;
}

// ─────────────────────────────── log-request construction (mirrors portal.ts) ───────────────────────────────

/** Portal `/stream` log filter — same shape the historical sync uses. */
export type PortalLogRequest = {
  address?: string[];
  topic0?: string[];
  topic1?: string[];
  topic2?: string[];
  topic3?: string[];
};

const PORTAL_MAX_ADDRESSES = 1000;
const asArr = <T,>(v: T | T[]): T[] => (Array.isArray(v) ? v : [v]);
const lc = (a: string): string => a.toLowerCase();

/** The unique factories referenced by any filter (deduped by id). */
export const uniqueFactories = (eventCallbacks: { filter: Filter }[]): Factory[] =>
  [
    ...new Map(
      eventCallbacks.flatMap((e) => getFilterFactories(e.filter)).map((f) => [f.id, f]),
    ).values(),
  ];

/** Log-filter → Portal log requests. Factory-address filters expand to the currently-known children. */
function logRequestsFor(
  filter: LogFilter,
  childAddresses: Map<FactoryId, Map<Address, number>>,
): PortalLogRequest[] {
  const base: PortalLogRequest = {};
  if (filter.topic0) base.topic0 = asArr(filter.topic0);
  if (filter.topic1) base.topic1 = asArr(filter.topic1 as any);
  if (filter.topic2) base.topic2 = asArr(filter.topic2 as any);
  if (filter.topic3) base.topic3 = asArr(filter.topic3 as any);
  let addresses: string[] | undefined;
  if (isAddressFactory(filter.address)) {
    addresses = Array.from(childAddresses.get(filter.address.id)?.keys() ?? []);
    if (addresses.length === 0) return []; // no children yet → nothing to request for this filter
  } else if (filter.address === undefined) {
    return [base];
  } else {
    addresses = asArr(filter.address).map(lc);
  }
  const out: PortalLogRequest[] = [];
  for (let i = 0; i < addresses.length; i += PORTAL_MAX_ADDRESSES)
    out.push({ ...base, address: addresses.slice(i, i + PORTAL_MAX_ADDRESSES) });
  return out;
}

/** Collapse requests sharing the same address-set + topic1..3, unioning topic0 (keeps the body small). */
function mergeLogRequests(reqs: PortalLogRequest[]): PortalLogRequest[] {
  const groups = new Map<string, PortalLogRequest>();
  for (const r of reqs) {
    const key = JSON.stringify([r.address ? [...r.address].sort() : null, r.topic1 ?? null, r.topic2 ?? null, r.topic3 ?? null]);
    const g = groups.get(key);
    if (!g) { groups.set(key, { ...r, topic0: r.topic0 ? [...new Set(r.topic0)] : undefined }); continue; }
    if (g.topic0 === undefined || r.topic0 === undefined) g.topic0 = undefined;
    else { const s = new Set(g.topic0); for (const t of r.topic0) s.add(t); g.topic0 = [...s]; }
  }
  return [...groups.values()];
}

/**
 * Build the merged Portal `/stream` log filter for a chain's realtime: every log filter's
 * address+topics PLUS a discovery request per factory (factory address + ProxyCreated selector), so new
 * children are streamed and pruned/matched downstream. Mirrors `portal.ts` so the realtime and historical
 * fetch-specs agree.
 */
export function buildPortalLogRequests(
  eventCallbacks: { filter: Filter }[],
  childAddresses: Map<FactoryId, Map<Address, number>>,
): PortalLogRequest[] {
  const filters = eventCallbacks.map((e) => e.filter);
  const reqs: PortalLogRequest[] = [];
  for (const f of filters) if (f.type === "log") reqs.push(...logRequestsFor(f as LogFilter, childAddresses));
  for (const factory of uniqueFactories(eventCallbacks)) {
    const address = factory.address ? asArr(factory.address).map(lc) : undefined;
    reqs.push({ address, topic0: [factory.eventSelector.toLowerCase()] });
  }
  return mergeLogRequests(reqs);
}

// ─────────────────────────────── factory child discovery ───────────────────────────────

/**
 * Discover factory children in a block's logs — reuses ponder's own factory logic (`isLogFactoryMatched`
 * + `getChildAddress`). For the euler `ProxyCreated(address indexed proxy,…)` factory the child is the
 * `proxy` in topic1. Returns the per-factory children found in THIS block (keyed by `Factory`, the shape
 * `handleRealtimeSyncEvent` finalizes into the sync-store).
 */
export function discoverChildAddresses(
  logs: SyncLog[],
  factories: Factory[],
): Map<Factory, Set<Address>> {
  const out = new Map<Factory, Set<Address>>();
  for (const factory of factories) {
    for (const log of logs) {
      if (isLogFactoryMatched({ factory, log }) === false) continue;
      let address: Address;
      try {
        address = getChildAddress({ log, factory }).toLowerCase() as Address;
      } catch {
        continue; // ABI mismatch on a factory with no fixed address → skip (mirrors createRealtimeSync)
      }
      if (out.has(factory) === false) out.set(factory, new Set<Address>());
      out.get(factory)!.add(address);
    }
  }
  return out;
}

/**
 * Fold this block's discovered children into the RUNNING `childAddresses` map (the one `buildEvents` reads
 * to match factory-child logs) — so a child created in this block is matched for its own logs in the SAME
 * block, exactly like `createRealtimeSync`. Returns true if any NEW child was added.
 */
function applyDiscovered(
  discovered: Map<Factory, Set<Address>>,
  childAddresses: Map<FactoryId, Map<Address, number>>,
  blockNumber: number,
): boolean {
  let added = false;
  for (const [factory, addresses] of discovered) {
    let rec = childAddresses.get(factory.id);
    if (rec === undefined) { rec = new Map<Address, number>(); childAddresses.set(factory.id, rec); }
    for (const address of addresses) {
      if (rec.has(address) === false) { rec.set(address, blockNumber); added = true; }
    }
  }
  return added;
}

// ─────────────────────────────── event conversion ───────────────────────────────

/** Portal `Light` (decimal number/timestamp) → ponder `LightBlock` (hex). */
export const lightToLightBlock = (l: Light): LightBlock => ({
  number: hx(l.number),
  hash: l.hash as LightBlock["hash"],
  parentHash: l.parentHash as LightBlock["parentHash"],
  timestamp: hx(l.timestamp),
});

/**
 * PortalRealtimeEvent → ponder RealtimeSyncEvent. `block` becomes a log-only BlockWithEventData (no
 * txs/receipts/traces — euler is log-indexed); `reorg`/`finalize` pass through with hex LightBlocks.
 */
export function toRealtimeSyncEvent(
  ev: PortalRealtimeEvent,
  childAddresses: Map<Factory, Set<Address>>,
): RealtimeSyncEvent {
  switch (ev.type) {
    case "block":
      return {
        type: "block",
        hasMatchedFilter: ev.hasMatchedFilter,
        block: ev.block,
        logs: ev.logs,
        transactions: [],
        transactionReceipts: [],
        traces: [],
        childAddresses,
        blockCallback: undefined, // no rpc.subscribe backpressure hook in the stream path (optional-chained downstream)
      };
    case "reorg":
      return { type: "reorg", block: lightToLightBlock(ev.block), reorgedBlocks: ev.reorgedBlocks.map(lightToLightBlock) };
    case "finalize":
      return { type: "finalize", block: lightToLightBlock(ev.block) };
  }
}

// ─────────────────────────────── drop-in realtime generator ───────────────────────────────

/**
 * Drop-in replacement for `getRealtimeEventGenerator` when `isPortalRealtime(chain)`. Yields the SAME
 * `{chain, event: RealtimeSyncEvent}` stream, sourced from the Portal `/stream` instead of
 * `rpc.subscribe`+`createRealtimeSync`. Callers (omni/multi/isolated) are unchanged.
 */
export async function* getPortalRealtimeEventGenerator(params: {
  common: Common;
  chain: Chain;
  rpc: Rpc;
  eventCallbacks: EventCallback[];
  syncProgress: { finalized: LightBlock; end?: LightBlock | undefined };
  childAddresses: Map<FactoryId, Map<Address, number>>;
  fetchImpl?: typeof fetch; // injected for tests
  finalizePollMs?: number; // injected for tests (prod: portal-realtime.ts default cadence)
}) {
  const { common, chain, eventCallbacks, syncProgress, childAddresses } = params;
  const portalUrl = cleanUrl(chain.portal!);
  const headers = portalHeaders();
  const factories = uniqueFactories(eventCallbacks);

  const startupFinalized = hexToNumber(syncProgress.finalized.number);
  const fromBlock = startupFinalized + 1; // finalized == Portal head (clamped) → stream (portal-head, tip]
  const endBlock = syncProgress.end ? hexToNumber(syncProgress.end.number) : undefined;

  // Mutable: rebuilt (in place) whenever a new child is discovered so the next stream reconnection filters
  // the new child's logs too (portal-realtime.ts re-reads this array when it re-opens the stream).
  const logs = buildPortalLogRequests(eventCallbacks, childAddresses);

  let childCount = 0;
  for (const [, m] of childAddresses) childCount += m.size;
  common.logger.info({
    service: "portal",
    msg: "Started live indexing (Portal /stream)",
    chain: chain.name,
    chain_id: chain.id,
    finalized_block: startupFinalized,
    factory_address_count: childCount,
  });

  const controller = new AbortController();
  let lastFinalized = startupFinalized;
  try {
    for await (const ev of portalRealtimeEvents({
      portalUrl,
      headers,
      fromBlock,
      logs,
      blockFields: BLOCK_FIELDS,
      logFields: LOG_FIELDS,
      finalizedHead: () => portalFinalizedHead(portalUrl, headers, params.fetchImpl),
      finalizePollMs: params.finalizePollMs,
      signal: controller.signal,
      fetchImpl: params.fetchImpl,
    })) {
      if (ev.type === "finalize") {
        // Q3 safety: never regress ponder's finalized/safe checkpoints. Suppress a finalize at/below the
        // startup boundary or below one already emitted (Portal head only ever advances, so this is defensive).
        const n = ev.block.number;
        if (n <= startupFinalized || n <= lastFinalized) continue;
        lastFinalized = n;
        yield { chain, event: toRealtimeSyncEvent(ev, new Map()) };
        continue;
      }

      if (ev.type === "reorg") {
        yield { chain, event: toRealtimeSyncEvent(ev, new Map()) };
        continue;
      }

      // block
      const discovered = discoverChildAddresses(ev.logs, factories);
      const blockNumber = hexToNumber(ev.block.number);
      if (applyDiscovered(discovered, childAddresses, blockNumber)) {
        const next = buildPortalLogRequests(eventCallbacks, childAddresses);
        logs.length = 0;
        logs.push(...next); // mutate in place — picked up on the next `/stream` reconnection
      }

      yield { chain, event: toRealtimeSyncEvent(ev, discovered) };

      if (endBlock !== undefined && blockNumber >= endBlock) {
        common.logger.info({
          service: "portal",
          msg: "Completed live indexing (chain end block has been indexed)",
          chain: chain.name,
          chain_id: chain.id,
          end_block: endBlock,
        });
        return;
      }
    }
  } finally {
    controller.abort();
  }
}

// Block header fields — the RPC-path-equivalent set (kept in sync with portal.ts) so stored realtime
// blocks are byte-consistent with the historical Portal backfill.
const BLOCK_FIELDS: Record<string, boolean> = {
  number: true, hash: true, parentHash: true, timestamp: true, logsBloom: true, miner: true,
  gasUsed: true, gasLimit: true, stateRoot: true, receiptsRoot: true, transactionsRoot: true,
  size: true, difficulty: true, extraData: true, baseFeePerGas: true, nonce: true, mixHash: true,
  sha3Uncles: true, totalDifficulty: true,
};
const LOG_FIELDS: Record<string, boolean> = {
  address: true, topics: true, data: true, transactionHash: true, transactionIndex: true, logIndex: true,
};
