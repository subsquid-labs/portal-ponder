/**
 * Portal-native realtime — serve `[portal-finalized-head → tip]` from the Portal `/stream` (fork-aware
 * hot-blocks) instead of RPC.
 *
 * WHY: in realtime mode the fork currently backfills `[deploy → portal-finalized-head]` from the Portal,
 * then fetches `[portal-finalized-head → chain-finalized-head]` over RPC (the "finality gap") and runs
 * ponder's RPC RealtimeSync for the tip. For dense chains under multichain load that RPC gap-fetch stalls
 * — ponder's single-thread global ordering blocks on the slowest chain's gap. The Portal already holds
 * these hot-blocks; streaming them removes the RPC gap at its source.
 *
 * HOW: `/stream` with `includeAllBlocks:true` yields EVERY block header (a consecutive parentHash chain,
 * for reorg tracking) plus the filtered euler logs, in one request. We reconcile each block into a local
 * unfinalized chain and emit ponder's own `RealtimeSyncEvent`s (`block` / `reorg` / `finalize`) reusing
 * `portal-transform` — so the downstream indexing + checkpointing in runtime/realtime.ts is UNCHANGED.
 *
 * The reorg/finalize reconciliation is pure + unit-tested; the `/stream` read and `/finalized-head` poll
 * are the I/O shell.
 */

import { type Hex, hexToNumber } from "viem";
import type { SyncBlockHeader, SyncLog } from "@/internal/types.js";
import {
  hx,
  type RawHeader,
  toSyncBlockHeader,
  toSyncLog,
} from "./portal-transform.js";

// ─────────────────────────────── pure reorg / finalize core (unit-tested) ───────────────────────────────

/** The minimum a block needs for chain reconciliation. `number` is a decimal string (Portal-native). */
export type Light = {
  number: number;
  hash: string;
  parentHash: string;
  timestamp: number;
};

export type Reconcile =
  | { kind: "append" } //   extends the tip (normal case)
  | { kind: "duplicate" } // already the tip (idempotent re-delivery)
  | { kind: "reorg"; commonAncestor: Light; reorgedBlocks: Light[] } // forks off an earlier block
  | { kind: "gap" }; //     parent is unknown (beyond our window / a skipped block) → caller must re-sync

/**
 * Reconcile a newly-streamed block against the local unfinalized chain (oldest→newest, linked
 * parentHash→hash). Pure. With `includeAllBlocks` the happy path is always `append`; a reorg re-streams
 * from the fork point, so the new block's `parentHash` matches an EARLIER block — everything after that
 * common ancestor is reorged.
 */
export function reconcile(unfinalized: Light[], next: Light): Reconcile {
  if (unfinalized.length === 0) return { kind: "append" };
  const tip = unfinalized[unfinalized.length - 1]!;
  if (next.hash === tip.hash) return { kind: "duplicate" };
  if (next.parentHash === tip.hash) return { kind: "append" };
  const idx = unfinalized.findIndex((b) => b.hash === next.parentHash);
  if (idx === -1) return { kind: "gap" };
  return {
    kind: "reorg",
    commonAncestor: unfinalized[idx]!,
    reorgedBlocks: unfinalized.slice(idx + 1),
  };
}

/** Split the unfinalized chain at a newly-finalized block number. Pure. */
export function takeFinalized(
  unfinalized: Light[],
  finalizedNumber: number,
): { finalizedTip: Light | undefined; remaining: Light[] } {
  let finalizedTip: Light | undefined;
  const remaining: Light[] = [];
  for (const b of unfinalized) {
    if (b.number <= finalizedNumber) finalizedTip = b;
    else remaining.push(b);
  }
  return { finalizedTip, remaining };
}

export const toLight = (h: RawHeader): Light => ({
  number: h.number,
  hash: h.hash,
  parentHash: h.parentHash,
  timestamp: h.timestamp,
});

// ─────────────────────────────── /stream I/O shell ───────────────────────────────

export type PortalRealtimeArgs = {
  portalUrl: string;
  headers: Record<string, string>;
  /** first block to stream (exclusive of already-indexed finalized head): syncProgress.finalized.number + 1 */
  fromBlock: number;
  /** euler log filters (address/topics), already merged — passed straight into the Portal query */
  logs: Array<Record<string, unknown>>;
  /** the block header fields ponder needs */
  blockFields?: Record<string, boolean>;
  logFields?: Record<string, boolean>;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
};

/**
 * Stream fork-aware hot-blocks with `includeAllBlocks` (every header + filtered logs). Yields raw Portal
 * batch objects `{ header, logs }`. Re-opens the stream from the last seen block on disconnect so it runs
 * continuously; a reorg simply re-streams from the fork point (reconcile() detects it).
 */
export async function* streamHotBlocks(
  args: PortalRealtimeArgs,
): AsyncGenerator<{ header: RawHeader; logs: any[] }> {
  const fetchImpl = args.fetchImpl ?? fetch;
  let cursor = args.fromBlock;
  for (;;) {
    if (args.signal?.aborted) return;
    const body = JSON.stringify({
      type: "evm",
      fromBlock: cursor,
      includeAllBlocks: true,
      fields: {
        block: args.blockFields ?? {
          number: true,
          hash: true,
          parentHash: true,
          timestamp: true,
        },
        log: args.logFields ?? {
          address: true,
          topics: true,
          data: true,
          logIndex: true,
          transactionHash: true,
          transactionIndex: true,
        },
      },
      logs: args.logs,
    });
    let res: Response;
    try {
      res = await fetchImpl(`${args.portalUrl}/stream`, {
        method: "POST",
        headers: { "content-type": "application/json", ...args.headers },
        body,
        signal: args.signal,
      });
    } catch {
      await sleep(1000, args.signal);
      continue;
    }
    if (res.status === 204 || !res.body) {
      await sleep(500, args.signal);
      continue;
    } // no hot data yet; re-poll
    if (!res.ok) {
      await sleep(1000, args.signal);
      continue;
    }
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let buf = "";
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let nl: number;
        while ((nl = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, nl);
          buf = buf.slice(nl + 1);
          if (!line) continue;
          const batch = JSON.parse(line);
          if (batch?.header?.number != null) {
            cursor = batch.header.number + 1; // resume past this block on reconnect
            yield { header: batch.header, logs: batch.logs ?? [] };
          }
        }
      }
    } catch {
      /* stream cut — reconnect from cursor */
    }
    await sleep(200, args.signal);
  }
}

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) return resolve();
    const t = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        resolve();
      },
      { once: true },
    );
  });

// ─────────────────────────────── event producer ───────────────────────────────
// Emits ponder RealtimeSyncEvent-shaped objects. `finalizedHead()` polls the Portal finalized head so we
// can emit `finalize` events (the RPC RealtimeSync derives finality from confirmations; the Portal tells
// us directly). NB: the exact ponder BlockWithEventData construction (transactions/receipts/traces/
// childAddresses) is completed at the wiring step in runtime/realtime.ts, which already has the factory
// child-address maps + buildEvents; here we surface the block header + logs + reorg/finalize control flow.

export type PortalRealtimeEvent =
  | {
      type: "block";
      block: SyncBlockHeader;
      logs: SyncLog[];
      hasMatchedFilter: boolean;
    }
  | { type: "reorg"; block: Light; reorgedBlocks: Light[] }
  | { type: "finalize"; block: Light };

export async function* portalRealtimeEvents(
  args: PortalRealtimeArgs & {
    finalizedHead: () => Promise<number | undefined>;
    finalizePollMs?: number;
  },
): AsyncGenerator<PortalRealtimeEvent> {
  const unfinalized: Light[] = [];
  let lastFinalizePoll = 0;
  const pollMs = args.finalizePollMs ?? 4000;

  for await (const { header, logs } of streamHotBlocks(args)) {
    const light = toLight(header);
    const r = reconcile(unfinalized, light);
    if (r.kind === "duplicate") continue;
    if (r.kind === "gap") {
      // parent unknown: our window is behind a deeper reorg. Drop to a resync by clearing and continuing
      // from this block (the finalized floor still protects already-committed data).
      unfinalized.length = 0;
    } else if (r.kind === "reorg") {
      // trim the local chain and surface the rollback to the common ancestor
      while (
        unfinalized.length &&
        unfinalized[unfinalized.length - 1]!.number > r.commonAncestor.number
      )
        unfinalized.pop();
      yield {
        type: "reorg",
        block: r.commonAncestor,
        reorgedBlocks: r.reorgedBlocks,
      };
    }
    unfinalized.push(light);

    const block = toSyncBlockHeader(header);
    const syncLogs = logs.map((l) => toSyncLog(l, header));
    yield {
      type: "block",
      block,
      logs: syncLogs,
      hasMatchedFilter: syncLogs.length > 0,
    };

    // finalize on a cadence (cheap head probe), not every block
    const now = Date.now();
    if (now - lastFinalizePoll >= pollMs) {
      lastFinalizePoll = now;
      const fh = await args.finalizedHead().catch(() => undefined);
      if (fh !== undefined) {
        const { finalizedTip, remaining } = takeFinalized(unfinalized, fh);
        if (finalizedTip) {
          unfinalized.length = 0;
          unfinalized.push(...remaining);
          yield { type: "finalize", block: finalizedTip };
        }
      }
    }
  }
}

export type { Hex, SyncBlockHeader, SyncLog };
export { hexToNumber, hx as _hx };
