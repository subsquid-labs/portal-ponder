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
import type {
  SyncBlockHeader,
  SyncLog,
  SyncTransaction,
} from '@/internal/types.js';
import {
  ndjsonLines,
  parseSchemaFieldError,
  readTextWithIdle,
} from './portal-client.js';
import { DROPPABLE_FIELDS } from './portal-filters.js';
import { invariant } from './portal-invariant.js';
import {
  type RawHeader,
  type RawLog,
  type RawTx,
  toSyncBlockHeader,
  toSyncLog,
  toSyncTransaction,
} from './portal-transform.js';

// ─────────────────────────────── pure reorg / finalize core (unit-tested) ───────────────────────────────

/** The minimum a block needs for chain reconciliation. `number` is a decimal string (Portal-native). */
export type Light = {
  number: number;
  hash: string;
  parentHash: string;
  timestamp: number;
};

export type Reconcile =
  | { kind: 'append' } //   extends the tip (normal case)
  | { kind: 'duplicate' } // already the tip (idempotent re-delivery)
  | { kind: 'reorg'; commonAncestor: Light; reorgedBlocks: Light[] } // forks off an earlier block
  | { kind: 'gap' }; //     parent unknown (beyond our window / a skipped block) → FATAL; restart to re-sync (finding 7)

/**
 * Reconcile a newly-streamed block against the local unfinalized chain (oldest→newest, linked
 * parentHash→hash). Pure. With `includeAllBlocks` the happy path is always `append`; a reorg re-streams
 * from the fork point, so the new block's `parentHash` matches an EARLIER block — everything after that
 * common ancestor is reorged.
 *
 * `anchor` is the last FINALIZED block (startup boundary, then each finalize's tip). It closes two holes
 * the bare window had:
 *   • An EMPTY window (routine right after a finalize, and at startup) used to blind-`append` ANYTHING —
 *     a skipped block or a wrong-fork block at exactly that boundary was undetectable. With the anchor,
 *     only a block extending it appends; the anchor block itself re-delivered is a `duplicate`; anything
 *     else is a `gap` (fatal — a fork below finality has no safe recovery).
 *   • A depth-1 fork at the boundary (new tip whose parent IS the finalized block, forking off the sole
 *     window entry) used to read as a fatal `gap` even though the common ancestor is known-safe. With the
 *     anchor it reconciles as a normal `reorg` off the anchor, reorging the whole window.
 * `anchor === undefined` (unknown at startup) keeps the old blind-append behavior.
 */
export function reconcile(
  unfinalized: Light[],
  next: Light,
  anchor?: Light,
): Reconcile {
  if (unfinalized.length === 0) {
    if (anchor === undefined) return { kind: 'append' };
    if (next.hash === anchor.hash) return { kind: 'duplicate' };
    if (next.parentHash === anchor.hash) return { kind: 'append' };

    return { kind: 'gap' };
  }
  const tip = unfinalized[unfinalized.length - 1]!;
  if (next.hash === tip.hash) return { kind: 'duplicate' };
  if (next.parentHash === tip.hash) return { kind: 'append' };
  const idx = unfinalized.findIndex((b) => b.hash === next.parentHash);
  if (idx === -1) {
    // fork at the finality boundary: the parent is the last-finalized anchor, so the common ancestor is
    // known-safe and the WHOLE window is the reorged suffix — a normal reorg, not a fatal gap.
    if (anchor !== undefined && next.parentHash === anchor.hash)
      return {
        kind: 'reorg',
        commonAncestor: anchor,
        reorgedBlocks: [...unfinalized],
      };

    return { kind: 'gap' };
  }

  return {
    kind: 'reorg',
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

// Realtime headers always carry hash/parentHash/timestamp (BLOCK_FIELDS requests them); the RawHeader
// type keeps them optional because other queries project fewer fields — assert presence here.
export const toLight = (h: RawHeader): Light => ({
  number: h.number,
  hash: h.hash as string,
  parentHash: h.parentHash as string,
  timestamp: h.timestamp as number,
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
  /**
   * Current logs-filter revision. The Portal filters `/stream` SERVER-side, so a `logs` change after a
   * connection opens (a newly-discovered factory child) can't reach THAT connection. streamHotBlocks
   * snapshots this at open and re-opens with the widened filter the moment it advances, so the child's
   * logs are caught on the next block instead of only after an unrelated reconnect. Absent ⇒ constant 0
   * (never reopens). (finding 4)
   */
  getLogsRevision?: () => number;
  /**
   * Transaction fields to project. When set, every log request carries `transaction: true` so the Portal
   * includes each matched log's PARENT transaction in the block batch — the same relation the historical
   * logQuery uses. Absent ⇒ no transactions requested (batches carry `transactions: []`).
   */
  txFields?: Record<string, boolean>;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
};

/**
 * Stream fork-aware hot-blocks with `includeAllBlocks` (every header + filtered logs + the matched logs'
 * parent transactions when `txFields` is set). Yields raw Portal batch objects `{ header, logs,
 * transactions }`. Re-opens the stream from the last seen block on disconnect so it runs continuously; a
 * reorg simply re-streams from the fork point (reconcile() detects it).
 */
export async function* streamHotBlocks(
  args: PortalRealtimeArgs,
): AsyncGenerator<{
  header: RawHeader;
  logs: RawLog[];
  transactions: RawTx[];
}> {
  const fetchImpl = args.fetchImpl ?? fetch;
  let cursor = args.fromBlock;
  // Tx fields the dataset can't serve, dropped across reconnects. TX_FIELDS includes `accessList`, which is
  // DROPPABLE (non-typed txs lack it) and which the HISTORICAL client degrades on a schema-field 400 — so a
  // dataset historical handles fine (e.g. no access_list) must not make stream mode refuse to start. We
  // degrade the SAME droppable tx fields here; a genuinely-unserveable (non-droppable) field stays fatal.
  // (review B3)
  const droppedTxFields = new Set<string>();
  for (;;) {
    if (args.signal?.aborted) return;
    // strip any dropped tx field from the projection before building the body (the same fields the
    // historical `stripFields` removes — keyed `transaction.<field>` in DROPPABLE_FIELDS).
    let txFields = args.txFields;
    if (txFields && droppedTxFields.size > 0) {
      txFields = { ...txFields };
      for (const key of droppedTxFields) {
        const field = key.slice(key.indexOf('.') + 1);
        delete txFields[field];
      }
    }
    const body = JSON.stringify({
      type: 'evm',
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
        ...(txFields ? { transaction: txFields } : {}),
      },
      logs: args.txFields
        ? args.logs.map((r) => ({ ...r, transaction: true }))
        : args.logs,
    });
    let res: Response;
    try {
      res = await fetchImpl(`${args.portalUrl}/stream`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...args.headers },
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
      // A 4xx (except 429) is DETERMINISTIC — the same body will 400 forever. Retrying it every second is a
      // silent, permanent tip outage; fail loud UNLESS it's a droppable-field 400 the historical path also
      // degrades. 429/5xx are load — retry. (review B3)
      if (res.status >= 400 && res.status < 500 && res.status !== 429) {
        // Read the body (bounded, own-the-lock cancel-on-stall — a stalled 400 body can't hang forever) and
        // parse it the SAME way the historical client does. A droppable tx field (e.g. `transaction.accessList`
        // on a dataset with no access_list) → drop it and retry, mirroring the backfill; anything else is a
        // genuinely-unserveable config → fatal.
        const text = (
          await readTextWithIdle(res, 10_000).catch(() => '')
        ).slice(0, 300);
        const schemaErr = parseSchemaFieldError(res.status, text, body);
        if (
          schemaErr &&
          DROPPABLE_FIELDS.has(schemaErr.fieldKey) &&
          schemaErr.tableKey === 'transaction' &&
          droppedTxFields.has(schemaErr.fieldKey) === false
        ) {
          droppedTxFields.add(schemaErr.fieldKey);
          continue; // retry immediately without the dropped field (no backoff — a config fix, not load)
        }

        throw new Error(
          `Portal /stream rejected the realtime query (HTTP ${res.status})${schemaErr ? ` — dataset cannot serve ${schemaErr.fieldKey}` : ''} — deterministic, not retried. PORTAL_REALTIME=stream cannot serve this configuration; unset it to use RPC realtime.`,
        );
      }
      // 429/5xx: load — cancel the body so the socket doesn't leak, then retry with backoff.
      void res.body?.cancel().catch(() => {});
      await sleep(1000, args.signal);
      continue;
    }
    // Snapshot the filter revision at open; if it advances while streaming (a child was discovered), break
    // to re-open with the widened server-side filter NOW rather than waiting for an unrelated reconnect.
    // Breaking early cancels the ndjsonLines reader (its finally), closing this connection. (finding 4)
    const openedRev = args.getLogsRevision?.() ?? 0;
    let reopen = false;
    try {
      for await (const line of ndjsonLines(res.body)) {
        const batch = JSON.parse(line);
        if (batch?.header?.number != null) {
          cursor = batch.header.number + 1; // resume past this block on reconnect
          yield {
            header: batch.header,
            logs: batch.logs ?? [],
            transactions: batch.transactions ?? [],
          };
          if ((args.getLogsRevision?.() ?? 0) !== openedRev) {
            // The filter widened while THIS block was being consumed — a factory child was discovered in
            // it, and the child's own same-block logs were filtered out server-side by the filter this
            // connection opened with. Re-open from THIS block (not past it): the new connection
            // re-delivers it under the widened filter, and the consumer accepts exactly the duplicate it
            // awaits (the redelivery handshake in portalRealtimeEvents / the wire). Resuming from
            // `number + 1` here permanently lost the child's block-N logs — ponder marked the interval
            // cached on finalize, so the gap survived restarts.
            cursor = batch.header.number;
            reopen = true;
            break;
          }
        }
      }
    } catch {
      /* stream cut — reconnect from cursor */
    }
    if (!reopen) await sleep(200, args.signal); // filter changed ⇒ re-open immediately, no backoff
  }
}

// `{ once: true }` removes the abort listener only when `abort` FIRES — on the normal timer path it was
// never removed, so streamHotBlocks leaked one listener per poll (~3600/h) onto the long-lived per-chain
// signal (MaxListenersExceededWarning storms + a real memory leak). Remove it when the timer fires normally.
// Exported for the leak regression test. (issue #28)
export const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    if (signal?.aborted) return resolve();
    const onAbort = (): void => {
      clearTimeout(t);
      resolve();
    };
    const t = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });

// ─────────────────────────────── event producer ───────────────────────────────
// Emits ponder RealtimeSyncEvent-shaped objects. `finalizedHead()` polls the Portal finalized head so we
// can emit `finalize` events (the RPC RealtimeSync derives finality from confirmations; the Portal tells
// us directly). NB: the exact ponder BlockWithEventData construction (transactions/receipts/traces/
// childAddresses) is completed at the wiring step in runtime/realtime.ts, which already has the factory
// child-address maps + buildEvents; here we surface the block header + logs + reorg/finalize control flow.

export type PortalRealtimeEvent =
  | {
      type: 'block';
      block: SyncBlockHeader;
      logs: SyncLog[];
      transactions: SyncTransaction[];
      hasMatchedFilter: boolean;
    }
  | { type: 'reorg'; block: Light; reorgedBlocks: Light[] }
  | { type: 'finalize'; block: Light };

/** The finalized-head poll result: the canonical hash (when the endpoint carries one) arms the
 * wrong-fork finalize guard below. A bare number keeps the old behavior (no hash check). */
export type FinalizedHead = number | { number: number; hash?: string };

export async function* portalRealtimeEvents(
  args: PortalRealtimeArgs & {
    finalizedHead: () => Promise<FinalizedHead | undefined>;
    finalizePollMs?: number;
    /**
     * Upper bound (ms) on how long the B1 hash-unverifiable finalize deferral may persist before it is
     * declared fatal. The defer branch below skips a poll whose canonical finalized head sits ABOVE the
     * local window tip (no local hash to verify it against). Under a Portal brownout — /stream delivery
     * below the chain's block rate — that head can stay ABOVE the window forever, so EVERY poll defers,
     * the anchor never advances, and `unfinalized` grows without bound while ponder's finalized checkpoint
     * silently freezes. When the deferral has run for this whole bound we THROW instead: a restart re-syncs
     * from the finalized head. Injectable for tests; production default 10 min. (delta review B1)
     */
    finalizeDeferMaxMs?: number;
    /**
     * The last FINALIZED block at startup (syncProgress.finalized as a Light) — the reconcile anchor.
     * Advanced to each finalize's tip. Absent ⇒ the empty-window blind-append behavior (legacy).
     */
    anchor?: Light;
    /**
     * Redelivery handshake (same-block child discovery): when the consumer discovers a factory child in
     * block N, it suppresses N's incomplete event, widens the filter, and streamHotBlocks re-opens FROM
     * N. The re-delivered N reconciles as `duplicate`; this predicate (owned by the consumer) marks it
     * as awaited, so it is RE-EMITTED with the widened filter's logs instead of skipped.
     */
    shouldRedeliver?: (hash: string) => boolean;
  },
): AsyncGenerator<PortalRealtimeEvent> {
  const unfinalized: Light[] = [];
  let anchor = args.anchor;
  let lastFinalizePoll = 0;
  const pollMs = args.finalizePollMs ?? 4000;
  // B1 defer-streak watchdog: `deferStreakStart` is the Date.now() of the FIRST poll in an unbroken run of
  // hash-unverifiable-finalize deferrals (undefined ⇒ not armed). Set when a poll defers and the streak is
  // not already armed; cleared whenever a finalize poll does NOT defer (finalize emitted, no finalizedTip,
  // or no probe result). If it ever runs longer than the bound the deferral is fatal — see the branch.
  // (delta review B1)
  const deferMaxMs = args.finalizeDeferMaxMs ?? 600_000;
  let deferStreakStart: number | undefined;

  for await (const { header, logs, transactions } of streamHotBlocks(args)) {
    const light = toLight(header);
    const r = reconcile(unfinalized, light, anchor);
    const redelivered =
      r.kind === 'duplicate' && (args.shouldRedeliver?.(light.hash) ?? false);
    if (r.kind === 'duplicate' && !redelivered) continue;

    if (r.kind === 'gap') {
      // Parent unknown: the streamed block doesn't attach to our unfinalized window — a reorg deeper than
      // the window (e.g. one that landed while the stream was disconnected, past our resume cursor). We
      // can't locate the fork point, so we can't emit a correct rollback; silently clearing the window (the
      // prior INV-10 "gap resets" behavior) left the already-indexed UNFINALIZED blocks on the WRONG fork
      // with no reorg event, and dropped the skipped span. Fail loud instead — the finalized floor protects
      // only COMMITTED data; a restart re-derives the window cleanly from historical. NOTE (finding 4 × 7):
      // a forced reopen on child discovery widens the reconnect window in which a concurrent deep reorg
      // surfaces here as a fatal gap rather than a handled reorg — accepted, a crash beats silent
      // corruption. (finding 7 / G5; a walk-parents-and-refetch auto-recovery is a follow-up.)
      throw new Error(
        `Portal realtime: streamed block ${light.number} (${light.hash}) has an unknown parent ${light.parentHash} — a reorg deeper than the unfinalized window, or a skipped block. Cannot reconcile safely; restart to re-sync from the finalized head.`,
      );
    }

    if (r.kind === 'reorg') {
      // trim the local chain and surface the rollback to the common ancestor
      while (
        unfinalized.length &&
        unfinalized[unfinalized.length - 1]!.number > r.commonAncestor.number
      )
        unfinalized.pop();
      yield {
        type: 'reorg',
        block: r.commonAncestor,
        reorgedBlocks: r.reorgedBlocks,
      };
    }
    // INV-10 tripwire: the unfinalized chain stays strictly increasing and parentHash-linked on every
    // append. `reconcile` guarantees this (append/reorg restore linkage; a gap is now fatal above), so this
    // O(1) check can only fire on a reconcile regression. A redelivered duplicate is ALREADY the tip —
    // no append, no check.
    if (!redelivered) {
      if (unfinalized.length > 0) {
        const tip = unfinalized[unfinalized.length - 1]!;
        invariant(
          'INV-10',
          light.parentHash === tip.hash && light.number > tip.number,
          'unfinalized chain link broken on append',
          () => ({
            tip: { number: tip.number, hash: tip.hash },
            next: { number: light.number, parentHash: light.parentHash },
          }),
        );
      }
      unfinalized.push(light);
    }

    const block = toSyncBlockHeader(header);
    const syncLogs = logs.map((l) => toSyncLog(l, header));
    // Dedupe parent txs by hash before emit — parity with the historical assembly's `seenTx` set (INV-2):
    // two matched logs sharing a parent tx, or overlapping log requests, could each carry the same tx, and
    // ponder's finalize insert must store exactly one row per hash. (review B4)
    const seenTx = new Set<string>();
    const syncTxs: SyncTransaction[] = [];
    for (const t of transactions ?? []) {
      if (t.hash !== undefined && seenTx.has(t.hash)) continue;

      if (t.hash !== undefined) seenTx.add(t.hash);
      syncTxs.push(toSyncTransaction(t, header));
    }
    yield {
      type: 'block',
      block,
      logs: syncLogs,
      transactions: syncTxs,
      hasMatchedFilter: syncLogs.length > 0,
    };

    // finalize on a cadence (cheap head probe), not every block
    const now = Date.now();
    if (now - lastFinalizePoll >= pollMs) {
      lastFinalizePoll = now;
      const fh = await args.finalizedHead().catch(() => undefined);
      const fhNumber = typeof fh === 'number' ? fh : fh?.number;
      const fhHash = typeof fh === 'object' ? fh?.hash : undefined;
      // No probe result this poll: the streak of deferrals (if any) is broken — clear it so a later,
      // hash-unverifiable deferral times its OWN run, not a run interleaved with probe outages. (B1)
      if (fhNumber === undefined) deferStreakStart = undefined;

      if (fhNumber !== undefined) {
        const { finalizedTip, remaining } = takeFinalized(
          unfinalized,
          fhNumber,
        );
        // No block at/below the finalized height yet — nothing to finalize, and not a hash-unverifiable
        // deferral either. This poll did NOT defer, so clear the streak. (B1)
        if (!finalizedTip) deferStreakStart = undefined;

        if (finalizedTip) {
          // Wrong-fork finalize guard: `takeFinalized` splits by NUMBER, so the finalizedTip is only
          // hash-VERIFIABLE against the probe when it sits at the probe's exact height. Two cases when the
          // probe carries the canonical hash:
          //   • finalizedTip.number === fhNumber — our local block at that height must equal the canonical
          //     hash, else the window is on a fork that lost to a finalized competitor; persisting it as
          //     finalized would commit wrong-fork data with no rollback event. Fail loud (review B1 keeps
          //     this).
          //   • finalizedTip.number  <  fhNumber — the probe references a block ABOVE our local tip, so we
          //     have NO canonical hash for finalizedTip's height and cannot confirm it descends from the
          //     canonical finalized block. Finalizing it by number alone would persist a possibly-losing
          //     fork below finality (the exact hole this guard closes). DEFER: skip this poll and let the
          //     window catch up to a hash-verifiable boundary (finality is monotonic and live chains reach
          //     fhNumber within a poll or two, at which point the === case verifies or fatals). (review B1)
          if (fhHash !== undefined && finalizedTip.number === fhNumber) {
            if (finalizedTip.hash !== fhHash)
              throw new Error(
                `Portal realtime: local block ${finalizedTip.number} (${finalizedTip.hash}) diverges from the canonical finalized block (${fhHash}) — the unfinalized window is on a losing fork at/below finality. Cannot finalize safely; restart to re-sync from the finalized head.`,
              );
          } else if (fhHash !== undefined && finalizedTip.number < fhNumber) {
            // Hash-unverifiable finalize: the canonical head sits ABOVE our window tip, so we cannot
            // confirm the local finalizedTip descends from it. Deferring is correct for a window that is
            // merely catching up — but a persistent Portal brownout (delivery below the chain rate) keeps
            // the head ABOVE the window forever, so EVERY poll defers, the anchor never advances, and
            // `unfinalized` grows without bound while ponder's finalized checkpoint silently freezes. Bound
            // the streak: arm it on the first deferral, and if it has run for the whole bound, fail loud —
            // number-only fallback would reopen the wrong-fork hole this branch exists to close. A restart
            // re-syncs from the finalized head. (delta review B1)
            if (deferStreakStart === undefined) deferStreakStart = now;
            else if (now - deferStreakStart >= deferMaxMs)
              throw new Error(
                `Portal realtime: the unfinalized window has lagged the hash-carrying finalized head (${fhNumber}, ${fhHash}) for ${now - deferStreakStart}ms (local tip ${finalizedTip.number}) — /stream delivery is below the chain's finalization rate, so finality cannot be hash-verified and the finalized checkpoint has stopped advancing. Cannot finalize safely; restart to re-sync from the finalized head.`,
              );

            continue; // hash-unverifiable finalize deferred until the window reaches fhNumber
          }
          // This poll finalizes (or applies a number-only head) — it did NOT defer, so the streak is
          // broken; clear it. (B1)
          deferStreakStart = undefined;

          anchor = finalizedTip; // the reconcile anchor advances with finality
          unfinalized.length = 0;
          unfinalized.push(...remaining);
          yield { type: 'finalize', block: finalizedTip };
        }
      }
    }
  }
}
