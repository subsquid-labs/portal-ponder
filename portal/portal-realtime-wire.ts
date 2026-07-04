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
  TX_FIELDS,
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

/** Poll the Portal `/finalized-head` (reused by both the historical finality-gap decision and here).
 * Carries the canonical hash when the endpoint provides one — it arms portalRealtimeEvents' wrong-fork
 * finalize guard (a local block finalized by NUMBER must match the canonical hash at that height). */
export async function portalFinalizedHead(
  portalUrl: string,
  headers: Record<string, string>,
  fetchImpl: typeof fetch = fetch,
): Promise<{ number: number; hash?: string } | undefined> {
  try {
    const h = await fetchImpl(`${cleanUrl(portalUrl)}/finalized-head`, {
      headers,
    }).then((r) => r.json());
    if (typeof h?.number === 'number')
      return {
        number: h.number,
        hash: typeof h?.hash === 'string' ? h.hash : undefined,
      };
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
  // An explicit PORTAL_FINALIZED_HEAD pin is AUTHORITATIVE for the finality boundary — the historical
  // seam (portal.ts refreshPortalHead, FIX 5) already treats it that way. Probing the live head here
  // while portal.ts honors the pin made the two seams disagree: with pin < live head, this clamp set the
  // boundary at the live head, historical intervals in (pin, liveHead] hit portal.ts's "realtime /stream
  // covers it" branch (its head IS the pin) and were marked synced EMPTY, while realtime streamed from
  // liveHead+1 — the exact G4/C11 silent gap the stream-mode throws were added to close.
  const pinRaw = process.env.PORTAL_FINALIZED_HEAD;
  const pin = pinRaw ? Number(pinRaw) : undefined;
  let head: number | undefined;
  if (pin !== undefined && Number.isInteger(pin) && pin >= 0) {
    head = pin;
  } else {
    // The Portal finalized head IS the finality boundary in stream mode — load-bearing, so retry the
    // cheap probe (transient blips are routine under multichain load) before deciding.
    for (let attempt = 0; attempt < 3; attempt++) {
      const probed = await portalFinalizedHead(
        portalUrl,
        portalHeaders(),
        params.fetchImpl,
      );
      if (probed !== undefined) {
        head = probed.number;
        break;
      }

      await sleep(200 * (attempt + 1));
    }
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
 * PortalRealtimeEvent → ponder RealtimeSyncEvent. `block` carries the matched logs AND their parent
 * transactions (the stream projects TX_FIELDS via the log requests' `transaction: true` — same relation
 * as the historical logQuery, so `event.transaction` works and the finalize-time store insert matches
 * the backfill's rows). Receipts/traces stay unserved — `assertStreamModeSupported` refuses those
 * configs up front. `reorg`/`finalize` pass through with hex LightBlocks.
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
        transactions: ev.transactions,
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
 * Stream mode only emits `block` events carrying LOGS and their parent TRANSACTIONS — no receipts or
 * traces (see `toRealtimeSyncEvent`). So a non-log source (trace/transfer/transaction/block filter) would
 * receive NO realtime events, yet ponder still finalizes its intervals as cached → a permanent, SILENT
 * gap. Likewise a log source that requested transaction receipts (`hasTransactionReceipt`) can't be
 * served. Refuse to start rather than corrupt. The historical Portal backfill supports every source type
 * up to the finalized head, so this rejects only PORTAL_REALTIME=stream — not the chain. (finding 5)
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
  /**
   * Watchdog for a redelivery that never lands: after suppressing block N for its same-block child
   * redelivery, streamHotBlocks re-opens FROM N and we await the complete N. On a HALTED chain (or any
   * stream that never re-serves N) that await stalls SILENTLY forever. This bounds it — if the redelivery
   * doesn't arrive within the timeout, fail loud (diagnosable) instead. Default 5 min; small values
   * injected in tests. (recommended: redelivery watchdog)
   */
  redeliveryTimeoutMs?: number;
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
  // Redelivery handshake (same-block child discovery): when block N discovers a NEW child, its own
  // same-block logs were filtered out server-side by the connection N arrived on. We suppress N's
  // incomplete event, widen the filter (logsRevision++), and streamHotBlocks re-opens FROM N; the
  // re-delivered N (now complete) reconciles as a duplicate that `shouldRedeliver` marks awaited, and
  // only THAT one is forwarded to ponder. While awaiting, a finalize at/above N is held back (ponder
  // hasn't seen N — finalizing it would mark the interval cached without N's data; the next poll
  // re-emits it) and a reorg that removes N clears the wait.
  let awaiting: { hash: string; number: number } | undefined;
  // Redelivery watchdog: a redelivery that never lands (halted chain — the reopened stream 204s forever, so
  // no event ever reaches this loop to trip a per-event check) would stall SILENTLY. Arm a timer whenever we
  // start awaiting; on expiry, record a loud fatal and ABORT the stream so the generator unwinds and rethrows
  // it (below). Disarmed the instant the wait clears. (recommended: redelivery watchdog)
  const redeliveryTimeoutMs = params.redeliveryTimeoutMs ?? 300_000;
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  let watchdogError: Error | undefined;
  const setAwaiting = (
    v: { hash: string; number: number } | undefined,
  ): void => {
    awaiting = v;
    if (watchdog !== undefined) {
      clearTimeout(watchdog);
      watchdog = undefined;
    }
    if (v !== undefined) {
      watchdog = setTimeout(() => {
        watchdogError = new Error(
          `Portal realtime: block ${v.number} (${v.hash}) was suppressed for its same-block child redelivery but the /stream never re-delivered it within ${redeliveryTimeoutMs}ms — the chain may be halted or the Portal is not re-serving it. Restart to re-sync from the finalized head.`,
        );
        controller.abort();
      }, redeliveryTimeoutMs);
      watchdog.unref?.();
    }
  };
  // A finalize that arrives WHILE awaiting a redelivery covers a block ponder hasn't received yet, so it
  // cannot be forwarded immediately. But portalRealtimeEvents has ALREADY applied it (anchor advanced,
  // window cleared) — dropping it means no later poll re-emits it, so at endBlock=N or on a halted chain
  // ponder would never finalize N (a liveness stall, the interval never cached). Stash it here and emit it
  // right after the redelivered block N is forwarded (ordering: block N, then finalize N). (review B2)
  let heldFinalize:
    | Extract<PortalRealtimeEvent, { type: 'finalize' }>
    | undefined;
  // Emit a finalize with the Q3 safety checks (never regress ponder's finalized/safe checkpoints: skip a
  // finalize at/below the startup boundary or below one already emitted — Portal head only ever advances,
  // so this is defensive). Shared by the live finalize branch and the held-finalize drain. (review B2)
  function* emitFinalize(
    ev: Extract<PortalRealtimeEvent, { type: 'finalize' }>,
  ): Generator<{ chain: Chain; event: RealtimeSyncEvent }> {
    const n = ev.block.number;
    if (n <= startupFinalized || n <= lastFinalized) return;
    lastFinalized = n;
    yield { chain, event: toRealtimeSyncEvent(ev, new Map()) };
  }
  const rebuildLogs = (): void => {
    const next = buildPortalLogRequests(eventCallbacks, childAddresses);
    logs.length = 0;
    logs.push(...next); // mutate in place — picked up on the next `/stream` (re)connection
    logsRevision++; // force streamHotBlocks to re-open with the changed filter now (finding 4)
  };
  try {
    for await (const ev of portalRealtimeEvents({
      portalUrl,
      headers,
      fromBlock,
      logs,
      blockFields: BLOCK_FIELDS,
      logFields: LOG_FIELDS,
      txFields: TX_FIELDS,
      getLogsRevision: () => logsRevision,
      // the reconcile anchor: the startup finalized block — an empty window appends ONLY a child of it
      anchor: {
        number: startupFinalized,
        hash: syncProgress.finalized.hash as string,
        parentHash: syncProgress.finalized.parentHash as string,
        timestamp: hexToNumber(syncProgress.finalized.timestamp),
      },
      shouldRedeliver: (hash) => awaiting?.hash === hash,
      finalizedHead: () =>
        portalFinalizedHead(portalUrl, headers, params.fetchImpl),
      finalizePollMs: params.finalizePollMs,
      signal: controller.signal,
      fetchImpl: params.fetchImpl,
    })) {
      if (ev.type === 'finalize') {
        // Hold back a finalize covering a block ponder hasn't received yet (suppressed for redelivery):
        // handleRealtimeSyncEvent would mark its interval cached WITHOUT its data. portalRealtimeEvents has
        // already consumed this finalize (window cleared, anchor advanced) — dropping it would lose it, so
        // STASH it and drain it right after the redelivered block lands (see the block branch). A later
        // finalize supersedes an earlier stash (finality is monotonic → keep the highest). (review B2)
        if (awaiting !== undefined && ev.block.number >= awaiting.number) {
          if (
            heldFinalize === undefined ||
            ev.block.number > heldFinalize.block.number
          )
            heldFinalize = ev;

          continue;
        }
        for (const out of emitFinalize(ev)) yield out;

        continue;
      }

      if (ev.type === 'reorg') {
        // The awaited block was reorged away before its redelivery — stop waiting for it. Ponder never
        // saw it (suppressed), and a rollback to the common ancestor is a no-op for blocks it never had.
        // Drop any finalize stashed for it too: it will never be redelivered, and finality would re-derive
        // it from the new fork on the next poll if it truly finalized. (review B2)
        if (
          awaiting !== undefined &&
          ev.reorgedBlocks.some((b) => b.hash === awaiting!.hash)
        ) {
          setAwaiting(undefined);
          heldFinalize = undefined;
        }
        // Prune reorged-out factory children from the RUNNING map (stock createRealtimeSync does this via
        // childAddressesPerBlock): a child whose creation block was reorged away must stop matching —
        // otherwise every later log from that address is indexed as a phantom child event until restart.
        // Only stream-discovered children can sit above the common ancestor (historical children are at/
        // below finality), so this never touches the preloaded map entries. Narrow the server filter too.
        const ancestor = ev.block.number;
        let pruned = false;
        for (const [, rec] of childAddresses)
          for (const [address, creationBlock] of rec)
            if (creationBlock > ancestor) {
              rec.delete(address);
              pruned = true;
            }
        if (pruned) rebuildLogs();

        yield { chain, event: toRealtimeSyncEvent(ev, new Map()) };
        continue;
      }

      // block
      const blockNumber = hexToNumber(ev.block.number);
      if (awaiting !== undefined) {
        // The only block event that may arrive while awaiting is the redelivery itself; anything else
        // means the stream skipped past the suppressed block — ponder would silently miss it. Fail loud.
        if (ev.block.hash !== awaiting.hash)
          throw new Error(
            `Portal realtime: awaiting redelivery of block ${awaiting.number} (${awaiting.hash}) after child discovery, but received ${blockNumber} (${ev.block.hash}) — the suppressed block would be silently skipped. Restart to re-sync from the finalized head.`,
          );
        setAwaiting(undefined);
      }
      const discovered = discoverChildAddresses(ev.logs, factories);
      if (applyDiscovered(discovered, childAddresses, blockNumber)) {
        rebuildLogs();
        // NEW children in THIS block: its own logs from them were server-side filtered out on the
        // connection it arrived on. Suppress the incomplete event and await the complete redelivery
        // (streamHotBlocks re-opens from this block; converges — the child set only grows).
        setAwaiting({ hash: ev.block.hash as string, number: blockNumber });
        continue;
      }

      yield { chain, event: toRealtimeSyncEvent(ev, discovered) };

      // Drain a finalize held back during this block's redelivery, now that ponder has the block: block N
      // is forwarded above, then finalize N here. Otherwise the finalize (already consumed by
      // portalRealtimeEvents) would be lost — at endBlock=N or a halted chain, ponder never finalizes N.
      // (review B2)
      if (heldFinalize !== undefined) {
        const held = heldFinalize;
        heldFinalize = undefined;
        for (const out of emitFinalize(held)) yield out;
      }

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
    // The stream ended. If the redelivery watchdog fired (it aborted the stream to unwind this loop), rethrow
    // its loud fatal — a silent stall on a never-redelivering (e.g. halted) chain becomes diagnosable.
    if (watchdogError !== undefined) throw watchdogError;
  } finally {
    if (watchdog !== undefined) clearTimeout(watchdog);
    controller.abort();
  }
}
