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

import { type Address, hexToNumber, numberToHex } from 'viem';
import type { Common } from '@/internal/common.js';
import type {
  Chain,
  EventCallback,
  Factory,
  FactoryId,
  Filter,
  LightBlock,
  SyncLog,
} from '@/internal/types.js';
import { eth_getBlockByNumber } from '@/rpc/actions.js';
import type { Rpc } from '@/rpc/index.js';
import { getChildAddress, isLogFactoryMatched } from '@/runtime/filter.js';
import type { RealtimeSyncEvent } from '@/sync-realtime/index.js';
// Log-request construction + field projections are the SINGLE source in portal-filters — shared with the
// historical sync so realtime and backfill fetch-specs can never drift. Re-exported for callers/tests.
import {
  BLOCK_FIELDS,
  buildPortalLogRequests,
  LOG_FIELDS,
  uniqueFactories,
} from './portal-filters.js';
import {
  type Light,
  type PortalRealtimeEvent,
  portalRealtimeEvents,
} from './portal-realtime.js';
import { hx } from './portal-transform.js';

export type { PortalLogRequest } from './portal-filters.js';
export { buildPortalLogRequests, uniqueFactories } from './portal-filters.js';

// ─────────────────────────────── flag / detection ───────────────────────────────

/** True when this chain should use the Portal `/stream` for realtime instead of ponder's RPC path. */
export const isPortalRealtime = (chain: {
  portal?: string | undefined;
}): boolean =>
  typeof chain.portal === 'string' && process.env.PORTAL_REALTIME === 'stream';

const portalHeaders = (): Record<string, string> => {
  const h: Record<string, string> = {
    'content-type': 'application/json',
    'accept-encoding': 'gzip',
  };
  if (process.env.PORTAL_API_KEY) h['x-api-key'] = process.env.PORTAL_API_KEY;
  return h;
};

const cleanUrl = (portal: string): string => portal.replace(/\/$/, '');

const sleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

// ─────────────────────────────── Portal finalized head + finality clamp ───────────────────────────────

/** Poll the Portal `/finalized-head` (reused by both the historical finality-gap decision and here). */
export async function portalFinalizedHead(
  portalUrl: string,
  headers: Record<string, string>,
  fetchImpl: typeof fetch = fetch,
): Promise<number | undefined> {
  try {
    const h = await fetchImpl(`${cleanUrl(portalUrl)}/finalized-head`, {
      headers,
    }).then((r) => r.json());
    if (typeof h?.number === 'number') return h.number;
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
  // The Portal finalized head IS the finality boundary in stream mode — load-bearing, so retry the cheap
  // probe (transient blips are routine under multichain load) before deciding.
  let head: number | undefined;
  for (let attempt = 0; attempt < 3; attempt++) {
    head = await portalFinalizedHead(
      portalUrl,
      portalHeaders(),
      params.fetchImpl,
    );
    if (head !== undefined) break;

    await sleep(200 * (attempt + 1));
  }
  // Head UNKNOWN after retries: FATAL in stream mode. The old behavior passed the RPC finalized block
  // through, which leaves historical targeting (portalHead, rpcFinalized] — a range stream mode's
  // historical seam serves nothing for — while realtime starts ABOVE it: a permanent silent gap. The RPC
  // finality-gap fallback is SUPPRESSED in stream mode, so there is no safe conservative default; fail loud
  // at startup rather than run with a hole. (finding 6 / C11 / G4)
  if (head === undefined)
    throw new Error(
      `Portal ${chain.name}: /finalized-head probe failed in stream mode (PORTAL_REALTIME=stream) — cannot establish the finality boundary. Check Portal connectivity for ${portalUrl}.`,
    );
  // Portal at/ahead of RPC finalized → nothing to clamp (never RAISE the boundary).
  if (head >= hexToNumber(finalizedBlock.number)) return finalizedBlock;

  const clamped = (await eth_getBlockByNumber(rpc, [numberToHex(head), false], {
    retryNullBlockRequest: true,
  })) as unknown as LightBlock;
  params.common?.logger.debug({
    service: 'portal',
    msg: `Portal ${chain.name}: clamped realtime finalized ${hexToNumber(finalizedBlock.number)} → Portal head ${head} (stream mode)`,
  });
  return clamped;
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
    if (rec === undefined) {
      rec = new Map<Address, number>();
      childAddresses.set(factory.id, rec);
    }
    for (const address of addresses) {
      if (rec.has(address) === false) {
        rec.set(address, blockNumber);
        added = true;
      }
    }
  }
  return added;
}

// ─────────────────────────────── event conversion ───────────────────────────────

/** Portal `Light` (decimal number/timestamp) → ponder `LightBlock` (hex). */
export const lightToLightBlock = (l: Light): LightBlock => ({
  number: hx(l.number),
  hash: l.hash as LightBlock['hash'],
  parentHash: l.parentHash as LightBlock['parentHash'],
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
    case 'block':
      return {
        type: 'block',
        hasMatchedFilter: ev.hasMatchedFilter,
        block: ev.block,
        logs: ev.logs,
        transactions: [],
        transactionReceipts: [],
        traces: [],
        childAddresses,
        blockCallback: undefined, // no rpc.subscribe backpressure hook in the stream path (optional-chained downstream)
      };
    case 'reorg':
      return {
        type: 'reorg',
        block: lightToLightBlock(ev.block),
        reorgedBlocks: ev.reorgedBlocks.map(lightToLightBlock),
      };
    case 'finalize':
      return { type: 'finalize', block: lightToLightBlock(ev.block) };
  }
}

// ─────────────────────────────── stream-mode capability gate ───────────────────────────────

/**
 * Stream mode only emits `block` events carrying LOGS — no transactions, receipts, or traces (see
 * `toRealtimeSyncEvent`). So a non-log source (trace/transfer/transaction/block filter) would receive NO
 * realtime events, yet ponder still finalizes its intervals as cached → a permanent, SILENT gap. Likewise
 * a log source that requested transaction receipts (`hasTransactionReceipt`) can't be served. Refuse to
 * start rather than corrupt. The historical Portal backfill supports every source type up to the finalized
 * head, so this rejects only PORTAL_REALTIME=stream — not the chain. (finding 5)
 */
export function assertStreamModeSupported(
  filters: Filter[],
  chainName: string,
): void {
  const nonLog = [
    ...new Set(filters.filter((f) => f.type !== 'log').map((f) => f.type)),
  ].sort();
  if (nonLog.length > 0)
    throw new Error(
      `Portal ${chainName}: PORTAL_REALTIME=stream serves only log sources, but this chain has ${nonLog.join(', ')} source(s). Realtime would silently skip their events while marking the range synced. Use the default RPC realtime (unset PORTAL_REALTIME) or remove the non-log sources.`,
    );

  const needsReceipts = filters.some((f) => f.hasTransactionReceipt === true);

  if (needsReceipts)
    throw new Error(
      `Portal ${chainName}: PORTAL_REALTIME=stream cannot serve transaction receipts (a log source sets hasTransactionReceipt), so realtime would drop them. Use the default RPC realtime (unset PORTAL_REALTIME).`,
    );
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
  const { common, chain, eventCallbacks, syncProgress, childAddresses } =
    params;
  // Belt-and-braces (the wiring's getLocalSyncProgress asserts this at process start too): fail loud BEFORE
  // streaming if this chain has sources stream mode can't serve (non-log / receipt-requiring) — else their
  // events are silently skipped while their intervals are marked synced. (finding 5)
  assertStreamModeSupported(
    eventCallbacks.map((e) => e.filter),
    chain.name,
  );

  const portalUrl = cleanUrl(chain.portal!);
  const headers = portalHeaders();
  const factories = uniqueFactories(eventCallbacks);

  const startupFinalized = hexToNumber(syncProgress.finalized.number);
  const fromBlock = startupFinalized + 1; // finalized == Portal head (clamped) → stream (portal-head, tip]
  const endBlock = syncProgress.end
    ? hexToNumber(syncProgress.end.number)
    : undefined;

  // Mutable: rebuilt (in place) whenever a new child is discovered so the next stream reconnection filters
  // the new child's logs too (portal-realtime.ts re-reads this array when it re-opens the stream).
  const logs = buildPortalLogRequests(eventCallbacks, childAddresses);
  // Bumps on every `logs` rebuild; streamHotBlocks watches it and re-opens the /stream the moment it
  // advances so the widened server-side filter takes effect on the next block. (finding 4)
  let logsRevision = 0;

  let childCount = 0;
  for (const [, m] of childAddresses) childCount += m.size;
  common.logger.info({
    service: 'portal',
    msg: 'Started live indexing (Portal /stream)',
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
      getLogsRevision: () => logsRevision,
      finalizedHead: () =>
        portalFinalizedHead(portalUrl, headers, params.fetchImpl),
      finalizePollMs: params.finalizePollMs,
      signal: controller.signal,
      fetchImpl: params.fetchImpl,
    })) {
      if (ev.type === 'finalize') {
        // Q3 safety: never regress ponder's finalized/safe checkpoints. Suppress a finalize at/below the
        // startup boundary or below one already emitted (Portal head only ever advances, so this is defensive).
        const n = ev.block.number;
        if (n <= startupFinalized || n <= lastFinalized) continue;
        lastFinalized = n;
        yield { chain, event: toRealtimeSyncEvent(ev, new Map()) };
        continue;
      }

      if (ev.type === 'reorg') {
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
        logsRevision++; // force streamHotBlocks to re-open with the widened filter now (finding 4)
      }

      yield { chain, event: toRealtimeSyncEvent(ev, discovered) };

      if (endBlock !== undefined && blockNumber >= endBlock) {
        common.logger.info({
          service: 'portal',
          msg: 'Completed live indexing (chain end block has been indexed)',
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
