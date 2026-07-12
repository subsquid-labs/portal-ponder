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
import {
  BLOCK_FIELDS,
  DROPPABLE_FIELDS,
  LOG_FIELDS,
} from './portal-filters.js';
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
 * `anchor` is the last FINALIZED block (startup boundary, then each finalize's tip) and is REQUIRED
 * (wave 4 — the optional legacy mode preserved exactly the blind-append hole this closes; production
 * always had an anchor). It closes two holes the bare window had:
 *   • An EMPTY window (routine right after a finalize, and at startup) used to blind-`append` ANYTHING —
 *     a skipped block or a wrong-fork block at exactly that boundary was undetectable. With the anchor,
 *     only a block extending it appends; the anchor block itself re-delivered is a `duplicate`; anything
 *     else is a `gap` (fatal — a fork below finality has no safe recovery).
 *   • A depth-1 fork at the boundary (new tip whose parent IS the finalized block, forking off the sole
 *     window entry) used to read as a fatal `gap` even though the common ancestor is known-safe. With the
 *     anchor it reconciles as a normal `reorg` off the anchor, reorging the whole window.
 */
export function reconcile(
  unfinalized: Light[],
  next: Light,
  anchor: Light,
): Reconcile {
  if (unfinalized.length === 0) {
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
    if (next.parentHash === anchor.hash)
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

/**
 * A `{number, hash}` seen on the wire — the shape of a `/stream` 409's `previousBlocks` entries AND of the
 * delivered-hash ring's records. `number` decimal (Portal-native).
 */
export type NumberedHash = { number: number; hash: string };

/**
 * Live diagnostic snapshot of the /stream I/O shell, mutated by `streamHotBlocks` and read by the fatal
 * paths (the `gap` fatal in `portalRealtimeEvents`, the 409-exhausted fatal here) so both dumps carry the
 * ring/connection state that pins the mechanism of a future instance. In-memory only, zero steady-state
 * cost — the objects are re-pointed, never accumulated. (issue #33 diagnostic dump)
 */
export type StreamDiag = {
  /** the last ~8 delivered ring entries (oldest→newest), for the dump */
  ring: NumberedHash[];
  /** the cursor the failing connection opened at */
  cursor: number;
  /** the `parentBlockHash` the failing connection sent (undefined ⇒ none/first-boot before seed) */
  parentBlockHashSent: string | undefined;
  /** blocks delivered on the current connection since it opened */
  blocksDeliveredThisConn: number;
  /** the previousBlocks of the last 409 seen, if any */
  lastPreviousBlocks: NumberedHash[] | undefined;
};

/** Render a NumberedHash list compactly for a fatal message. */
export const fmtNumberedHashes = (xs: NumberedHash[]): string =>
  `[${xs.map((x) => `${x.number}:${x.hash}`).join(', ')}]`;

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
  /**
   * The startup finality anchor `{number → hash}`, used to SEED the delivered-hash ring so the FIRST
   * `/stream` request (resuming at `fromBlock = anchor.number + 1`) can carry `parentBlockHash =
   * anchor.hash` — opting the client into the Portal's fork negotiation from the very first connection.
   * Absent ⇒ no seed, and the first request carries no `parentBlockHash` (legacy pre-#33 behavior, kept
   * for the unit-test call sites that don't wire it). (issue #33)
   */
  seedRing?: NumberedHash;
  /**
   * The current finalized floor (decimal). streamHotBlocks prunes the delivered-hash ring below it and
   * bounds the 409 rewind/step-down at it: a fork point below finality has no safe recovery, so it is
   * FATAL rather than rewound. The wire advances this with each finalize. Absent ⇒ the floor is
   * `fromBlock − 1` (the startup boundary — safe, since nothing below it was ever streamed). (issue #33)
   */
  getFinalizedFloor?: () => number;
  /**
   * Live diagnostic snapshot the shell keeps current so the fatal paths can dump the ring/connection
   * state. Absent ⇒ the shell keeps its ring internally with no external mirror (unit-test call sites).
   * (issue #33)
   */
  diag?: StreamDiag;
  /** No-data repoll sleep and tick spacing (204 / bodyless-200). Default 500 (legacy timing). */
  tickSleepMs?: number;
  /**
   * Transient-error retry sleep (fetch throw, 429/5xx). Default 1000 (legacy timing); portalRealtimeEvents
   * may pass a smaller value when finalizePollMs < 2000 to preserve the heartbeat cadence bound.
   */
  errorSleepMs?: number;
  /**
   * Idle bound (ms) on the OPEN /stream body read (RT-G11). A wedged connection — headers OK, body never
   * delivers, no FIN/RST — would otherwise hang the NDJSON read FOREVER: no blocks, no reconnect, no ticks
   * to drive finalize. `ndjsonLines`' per-chunk idle guard re-arms on every received chunk (a slow-but-alive
   * stream is never cut) and, after `idleMs` of CUMULATIVE silence, throws — which the loop catches and
   * reconnects from `cursor` (routine, cheap, NOT fatal). Default 120_000; portalRealtimeEvents/wire resolve
   * the env-tunable `PORTAL_STREAM_IDLE_MS`. The bound must comfortably exceed normal inter-block quiet.
   * Independent of the tick-transparent line-wait below: ticks keep finalize alive DURING the idle window;
   * this only bounds how long a silent-but-open connection is held before it is recycled. (RT-1 SC1)
   */
  idleMs?: number;
  /**
   * Called once when the OPEN-body read hits its `idleMs` bound and the connection is recycled (RT-G11) — a
   * seam for the logger-free shell to surface a debug line at the wire (where `common.logger` lives), so an
   * idle reconnect is distinguishable from an ordinary stream cut. Absent ⇒ silent recycle (unit call sites).
   */
  onIdleReconnect?: () => void;
  /**
   * Called on each transient `/stream` fetch THROW (the E1 retry site) — the read never opened, so this
   * round delivers nothing and the loop yields a `{kind:'tick'}` + backs off. A logger-free seam (mirrors
   * `onIdleReconnect`) letting the shell surface a RATE-LIMITED warn so a silent retry storm is visible at
   * the wire without this module importing a logger. Absent ⇒ silent (unit call sites). (RT-1 SC3)
   */
  onFetchError?: () => void;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
};

/**
 * Max delivered-hash ring entries kept (per height). ~2048 covers far more than any unfinalized window.
 * CANDOR (F5): with an unfinalized window DEEPER than RING_CAP — a fast chain under a max finalize-defer
 * streak, so the anchor (and thus the pruning floor) hasn't advanced while the tip has climbed >2048 blocks
 * above it — a deep 409 step-down could reach an EVICTED ring height and find no confirming entry, ending at
 * the floor fatal. That is an availability edge (a loud restart, never wrong data): the step-down simply
 * fails to confirm a fork point whose ring hash was capped out, exactly as a fresh restart would re-derive
 * the window from the finalized head. The B1 finalize-defer watchdog bounds how long that window can grow.
 */
export const RING_CAP = 2048;
/**
 * Max consecutive 409 fork-negotiation rounds WITHOUT cursor progress before failing loud (F2). This is an
 * OSCILLATION guard, NOT a per-409 counter: a legitimate no-match negotiation steps the cursor DOWN one
 * height per 409 (SQD's docs describe the 409 `previousBlocks` as a SAMPLE and warn clients to expect
 * several repeated 409s), and a deep fork can descend far more than 10 heights — that monotonic descent is
 * bounded separately by the floor fatal (cursor − floor is finite). Only rounds that make NO progress (the
 * cursor did not strictly decrease — a rewind/re-409 stuck at the same spot) count toward this cap.
 */
export const MAX_CONSECUTIVE_409 = 10;

export type HotBatch = {
  kind: 'block';
  header: RawHeader;
  logs: RawLog[];
  transactions: RawTx[];
};
export type HotTick = { kind: 'tick' };
export type HotItem = HotBatch | HotTick;

/**
 * Stream fork-aware hot-blocks with `includeAllBlocks` (every header + filtered logs + the matched logs'
 * parent transactions when `txFields` is set). Yields raw Portal batch objects `{ header, logs,
 * transactions }`. Re-opens the stream from the last seen block on disconnect so it runs continuously; a
 * reorg simply re-streams from the fork point (reconcile() detects it).
 */
export async function* streamHotBlocks(
  args: PortalRealtimeArgs,
): AsyncGenerator<HotItem> {
  const fetchImpl = args.fetchImpl ?? fetch;
  const tickSleepMs = args.tickSleepMs ?? 500;
  const errorSleepMs = args.errorSleepMs ?? 1000;
  const idleMs = args.idleMs ?? 120_000;
  let cursor = args.fromBlock;
  // Delivered-hash ring: height → last-delivered hash at that height. Its semantics is "what is my
  // parentBlockHash if I resume at h+1" — so `ring.get(cursor − 1)` is sent on EVERY /stream request. It
  // opts the client into the Portal's fork negotiation: on a fork the server answers 409 with the canonical
  // `previousBlocks` (the replacement chain) instead of blindly serving 200. Seeded with the startup anchor
  // so the FIRST request already carries a parentBlockHash. Overwrite-per-height (a redelivered/reorged
  // height replaces its hash); pruned below the finalized floor (bounded). (issue #33)
  const ring = new Map<number, string>();
  // ARMED only when a seed was wired: production ALWAYS seeds via the startup anchor (the wire passes it at
  // portal-realtime-wire.ts anchor construction → portalRealtimeEvents seeds streamHotBlocks), so the ring
  // is armed and `parentBlockHash` is sent on every request incl. the first. Legacy/unit call sites that
  // pass no seed stay UNARMED: they send no `parentBlockHash` and a missing ring entry is not fatal — the
  // pre-#33 number-only behavior, so those hermetic tests keep their exact wire shape. (issue #33)
  const armed = args.seedRing !== undefined;
  if (args.seedRing !== undefined)
    ring.set(args.seedRing.number, args.seedRing.hash);

  const floor = (): number => args.getFinalizedFloor?.() ?? args.fromBlock - 1;

  // Prune ring heights strictly below the finalized floor: those blocks are committed, never re-negotiated
  // (a fork below finality is fatal, not rewound), so their hashes can be dropped. Also cap the size.
  const pruneRing = (): void => {
    const lo = floor();
    for (const [n] of ring) {
      if (n < lo) ring.delete(n);
    }
    if (ring.size > RING_CAP) {
      const heights = [...ring.keys()].sort((a, b) => a - b);
      const drop = heights.length - RING_CAP;
      for (let i = 0; i < drop; i++) ring.delete(heights[i]!);
    }
  };

  // Keep the shared diagnostic mirror current (in-memory; the fatal paths read it). `lastPreviousBlocks`
  // survives until the next 409 so a subsequent `gap` fatal can still cite it.
  const syncDiag = (
    parentBlockHashSent: string | undefined,
    blocksDeliveredThisConn: number,
    lastPreviousBlocks?: NumberedHash[],
  ): void => {
    if (args.diag === undefined) return;

    const heights = [...ring.keys()].sort((a, b) => a - b).slice(-8);
    args.diag.ring = heights.map((n) => ({ number: n, hash: ring.get(n)! }));
    args.diag.cursor = cursor;
    args.diag.parentBlockHashSent = parentBlockHashSent;
    args.diag.blocksDeliveredThisConn = blocksDeliveredThisConn;
    if (lastPreviousBlocks !== undefined)
      args.diag.lastPreviousBlocks = lastPreviousBlocks;
  };

  // Fields the dataset can't serve, dropped across reconnects — keyed `table.field`, exactly the keys in
  // DROPPABLE_FIELDS. The projections include droppable fields on TWO tables: `transaction.accessList`
  // (non-typed txs lack it) and five nullable block columns (`block.mixHash`, `block.nonce`, …) that the
  // HISTORICAL client degrades on a schema-field 400 via `stripFields` — so a dataset historical handles
  // fine must not make stream mode refuse to start. Restricting this to `transaction.*` (the original B3
  // fix) left every droppable BLOCK field fatal: a mix_hash-less dataset backfilled fine, then the chain
  // was down at the tip on every restart. Degrade ANY droppable field, whichever table it lives on; a
  // genuinely-unserveable (non-droppable) field stays fatal. (review B3; completed in wave 4)
  const droppedFields = new Set<string>();
  // Strip dropped fields for `table` from a projection (the same removal the historical `stripFields`
  // applies). Returns the input object untouched when nothing is dropped for that table.
  const projectFields = (
    fields: Record<string, boolean> | undefined,
    table: string,
  ): Record<string, boolean> | undefined => {
    if (fields === undefined || droppedFields.size === 0) return fields;

    let out = fields;
    for (const key of droppedFields) {
      if (!key.startsWith(`${table}.`)) continue;

      if (out === fields) {
        out = { ...fields };
      }
      delete out[key.slice(table.length + 1)];
    }

    return out;
  };
  // Consecutive 409 fork-negotiations. Reset on any successful delivery; a runaway (>MAX) is a bug (the
  // server keeps rejecting a chain the ring believes canonical) → fail loud rather than spin. (issue #33)
  let consecutive409 = 0;
  // Cache the serialized /stream request body. Its `logs` is O(total children) — up to ~100k filter rows on
  // a busy factory chain (~4.6MB serialized) — and the loop re-enters body construction on EVERY iteration:
  // each 204 re-poll (~500ms), each 409 round, each reopen on a new child. Re-`JSON.stringify`ing + re-
  // uploading that on every poll is ~110MB of upload per block on a caught-up 12s chain. The body is a pure
  // function of (cursor, parentBlockHash, logs-filter revision, dropped-field set), so rebuild only when one
  // of those changes and re-POST the identical bytes otherwise. (wave 5)
  let cachedBody: string | undefined;
  let cachedKey: string | undefined;
  for (;;) {
    if (args.signal?.aborted) return;

    // The parentBlockHash for this request: the ring's hash at cursor−1. When ARMED, a MISSING entry is an
    // invariant violation — the ring is seeded with the anchor and updated on every delivery, and the cursor
    // only ever sits at a height whose predecessor we've delivered (fromBlock−1 = anchor; number+1 after a
    // block; number after a redelivery reopen; a rewound 409 target that matched the ring). If it's absent
    // the client would send NO parentBlockHash and silently re-open the fork-negotiation hole this fix
    // closes. Fail loud with the diagnostic dump. When UNARMED (no seed), send no parentBlockHash (legacy).
    // (issue #33)
    const parentBlockHash = ring.get(cursor - 1);
    if (armed && parentBlockHash === undefined) {
      syncDiag(undefined, 0);

      throw new Error(
        `Portal realtime: no delivered-hash ring entry for the resume parent at ${cursor - 1} (cursor ${cursor}, floor ${floor()}) — cannot send parentBlockHash, which would re-open the /stream fork-negotiation hole. ${diagDump(args.diag)} Restart to re-sync from the finalized head.`,
      );
    }
    // Rebuild the body only when an input changed; otherwise reuse the last serialization (see above). The
    // key is O(1) to compute — a change in the logs filter bumps `getLogsRevision`, a schema-degrade grows
    // `droppedFields`, and a resume/reorg moves `cursor`/`parentBlockHash`.
    const bodyKey = `${cursor}|${args.getLogsRevision?.() ?? 0}|${droppedFields.size}|${parentBlockHash ?? ''}`;
    let body: string;
    if (bodyKey === cachedKey && cachedBody !== undefined) {
      body = cachedBody; // inputs unchanged — reuse the last serialization
    } else {
      const txFields = projectFields(args.txFields, 'transaction');
      // Defaults are the SHARED portal-filters projections — the wire passes the same constants, so a
      // caller omitting them can no longer drift from the single source (wave 4; the old inline literals
      // were a second copy of exactly what the module header disclaims).
      const blockFields = projectFields(
        args.blockFields ?? BLOCK_FIELDS,
        'block',
      );
      body = JSON.stringify({
        type: 'evm',
        fromBlock: cursor,
        // omit the key entirely when we have no hash, so an unarmed legacy request is byte-identical to pre-#33
        ...(parentBlockHash !== undefined ? { parentBlockHash } : {}),
        includeAllBlocks: true,
        fields: {
          block: blockFields,
          log: args.logFields ?? LOG_FIELDS,
          ...(txFields ? { transaction: txFields } : {}),
        },
        logs: args.txFields
          ? args.logs.map((r) => ({ ...r, transaction: true }))
          : args.logs,
      });
      cachedBody = body;
      cachedKey = bodyKey;
    }
    let res: Response;
    try {
      res = await fetchImpl(`${args.portalUrl}/stream`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...args.headers },
        body,
        signal: args.signal,
      });
    } catch {
      // E1: transient fetch throw — the read never opened, so this round delivers NOTHING. Surface it on
      // the logger-free seam (the shell rate-limits the warn) so a silent retry storm is diagnosable; the
      // yielded tick is the same non-delivery heartbeat the delivery watchdog already counts. (RT-1 SC3)
      args.onFetchError?.();
      yield { kind: 'tick' };
      await sleep(errorSleepMs, args.signal);
      continue;
    }
    if (res.status === 204) {
      yield { kind: 'tick' };
      await sleep(tickSleepMs, args.signal);
      continue;
    } // no hot data yet; re-poll
    // NB: only a 204 short-circuits to a re-poll here. The old guard ALSO re-polled on `!res.body` BEFORE
    // the 409/4xx branches below — so a bodyless 409 (or 4xx) silently re-polled forever, a quiet permanent
    // negotiation stall / tip outage. The bodyless re-poll now applies only to a bodyless 200 (its original
    // intent — a "no hot data yet" empty OK), gated just before ndjsonLines consumes res.body. 409 and 4xx
    // tolerate a null body: parsePreviousBlocks / readTextWithIdle read '' from a bodyless response, which
    // drives the 409 step-down/floor path and the 4xx not-a-droppable-400 fatal respectively. (F1)
    // 409 fork negotiation (BEFORE the deterministic-4xx branch, which would otherwise treat it as a fatal
    // config). The server saw our parentBlockHash orphaned and returned the canonical replacement chain in
    // `previousBlocks` (ending at fromBlock−1). Rewind the cursor to the highest previousBlocks entry that
    // MATCHES our ring at/above the floor — that block is the confirmed fork point; re-opening there re-serves
    // the canonical chain, which reconcile() surfaces as a normal reorg + appends. If nothing matches, step
    // DOWN one block per 409 (each retry re-sends the ring hash at the new cursor−1) until a match or the
    // floor. A fork point below the finalized floor has no safe recovery (finalized data can't be rolled
    // back) → fatal. An OSCILLATING 409 loop that never lowers the cursor → fatal (the no-progress cap
    // below); a monotonic descent is bounded by the floor, not the cap. (issue #33)
    if (res.status === 409) {
      const prev = await parsePreviousBlocks(res).catch(() => undefined);
      syncDiag(parentBlockHash, 0, prev ?? []);
      // OSCILLATION guard (NOT a per-409 counter). A legitimate no-match negotiation steps the cursor DOWN
      // one height per 409, and a deep fork can sit far more than 10 heights above the floor — SQD's public
      // docs describe the 409 `previousBlocks` as a SAMPLE and warn clients to expect several repeated 409s.
      // Counting every 409 would fatal such a descent BEFORE it reached the floor, contradicting the
      // "until a match or the floor" contract. So we count only consecutive 409 rounds that made NO cursor
      // PROGRESS (progress = the cursor STRICTLY DECREASED this round, via a rewind that lands lower or a
      // step-down). Monotonic descent is separately bounded by the floor fatal below (cursor − floor is
      // finite), so termination and loudness are preserved: a genuine oscillation (a rewind/re-409 that
      // never lowers the cursor) trips this cap; a real descent reaches the floor. (F2)
      const cursorBefore = cursor;
      const lo = floor();
      // Highest previousBlocks entry whose {number,hash} matches our ring, at/above the floor.
      let rewindTo: number | undefined;
      for (const pb of prev ?? []) {
        if (pb.number < lo) continue;
        if (ring.get(pb.number) !== pb.hash) continue;
        if (rewindTo === undefined || pb.number > rewindTo)
          rewindTo = pb.number;
      }
      if (rewindTo !== undefined) {
        cursor = rewindTo + 1; // re-open just above the confirmed common ancestor
      } else {
        // No previousBlocks entry matches the ring. If the server named a fork point below the floor, or the
        // step-down has reached the floor, recovery is unsafe (below finality) → fatal. Otherwise step the
        // cursor down one block and re-negotiate with the ring hash at the new cursor−1.
        const belowFloor = (prev ?? []).some((pb) => pb.number < lo);
        if (belowFloor || cursor - 1 <= lo) {
          throw new Error(
            `Portal realtime: /stream fork point is at or below the finalized floor ${lo} (cursor ${cursor}) — no safe recovery below finality. ${diagDump(args.diag)} Restart to re-sync from the finalized head.`,
          );
        }
        cursor -= 1;
      }
      // Did this round make progress? A rewind that lands the cursor at the same height or HIGHER (the server
      // named a confirmed block at/above where we already are) is NOT progress — it's the shape a stuck
      // oscillation takes (re-open, get the same 409, rewind to the same spot). Only a strict decrease resets.
      if (cursor < cursorBefore) {
        consecutive409 = 0;
      } else {
        consecutive409 += 1;
        if (consecutive409 > MAX_CONSECUTIVE_409) {
          throw new Error(
            `Portal realtime: ${consecutive409} consecutive /stream 409 fork-negotiations without cursor progress (cursor ${cursor}, floor ${lo}) — the Portal keeps rejecting the chain the ring believes canonical without the negotiation descending. ${diagDump(args.diag)} Restart to re-sync from the finalized head.`,
          );
        }
      }

      continue;
    }
    if (!res.ok) {
      // A 4xx (except 429) is DETERMINISTIC — the same body will 400 forever. Retrying it every second is a
      // silent, permanent tip outage; fail loud UNLESS it's a droppable-field 400 the historical path also
      // degrades. 429/5xx are load — retry. (review B3)
      if (res.status >= 400 && res.status < 500 && res.status !== 429) {
        // Read the body (bounded, own-the-lock cancel-on-stall — a stalled 400 body can't hang forever) and
        // parse it the SAME way the historical client does. ANY droppable field (`transaction.accessList`
        // on a dataset with no access_list, `block.mixHash` on one with no mix_hash, …) → drop it and
        // retry, mirroring the backfill's stripFields; anything else is a genuinely-unserveable config →
        // fatal. No tableKey restriction: DROPPABLE_FIELDS itself is the whitelist.
        const text = (
          await readTextWithIdle(res, 10_000).catch(() => '')
        ).slice(0, 300);
        const schemaErr = parseSchemaFieldError(res.status, text, body);
        if (
          schemaErr &&
          DROPPABLE_FIELDS.has(schemaErr.fieldKey) &&
          droppedFields.has(schemaErr.fieldKey) === false
        ) {
          droppedFields.add(schemaErr.fieldKey);
          continue; // retry immediately without the dropped field (no backoff — a config fix, not load)
        }

        throw new Error(
          `Portal /stream rejected the realtime query (HTTP ${res.status})${schemaErr ? ` — dataset cannot serve ${schemaErr.fieldKey}` : ''} — deterministic, not retried. PORTAL_REALTIME=stream cannot serve this configuration; unset it to use RPC realtime.`,
        );
      }
      // 429/5xx: load — cancel the body so the socket doesn't leak, then retry with backoff.
      void res.body?.cancel().catch(() => {});
      yield { kind: 'tick' };
      await sleep(errorSleepMs, args.signal);
      continue;
    }
    // A 200 means the fork negotiation (if any) resolved — the chain from `cursor` links to our ring. Clear
    // the streak so a later, unrelated fork negotiates fresh from the 10-cap. (issue #33)
    consecutive409 = 0;
    // A bodyless 200 (empty OK — "no hot data yet") re-polls, preserving the old `!res.body` semantics but
    // now ONLY on the OK path, after the 409/4xx branches have had their turn. (F1)
    if (!res.body) {
      yield { kind: 'tick' };
      await sleep(tickSleepMs, args.signal);
      continue;
    }
    // Snapshot the filter revision at open; if it advances while streaming (a child was discovered), break
    // to re-open with the widened server-side filter NOW rather than waiting for an unrelated reconnect.
    // Breaking early cancels the ndjsonLines reader (its finally), closing this connection. (finding 4)
    const openedRev = args.getLogsRevision?.() ?? 0;
    let reopen = false;
    let deliveredThisConn = 0;
    // Drive the NDJSON reader MANUALLY (not a plain `for await`) so the wait for the next line is
    // TICK-TRANSPARENT (G2b): while suspended waiting for a line, we yield `{ kind: 'tick' }` every
    // `tickSleepMs` of silence WITHOUT touching the connection, so finalize/B1 keep polling on a
    // silent-but-OPEN connection (the one no-delivery state the loop-turn ticks cannot reach). The
    // `idleMs` guard armed on `ndjsonLines` still re-arms per received chunk (a slow-but-alive stream is
    // never cut) and, after `idleMs` of CUMULATIVE silence, REJECTS the read → caught below → reconnect.
    // Correctness: a single pending `readerP` is held across heartbeat races and NEVER re-requested until
    // it settles, so a heartbeat tick can neither consume nor reorder a line; the heartbeat timer is
    // always cleared (raceHeartbeat's finally) so no timer accrues across ticks. (RT-1 SC1 / R1)
    const it = ndjsonLines(res.body, undefined, idleMs);
    let idleExpired = false;
    try {
      let readerP = it.next();
      // DO NOT REMOVE — a readerP abandoned before its next re-race (abort/return at the yield, or the
      // loop-top break on an already-aborted signal) would else unhandled-reject on a signal-aborted body:
      // the /stream fetch is made with `signal: args.signal`, so on abort the in-flight `reader.read()`
      // inside this pending `readerP` rejects. This `.catch` is a SEPARATE handler on the same promise — a
      // promise may carry many independent handlers, so it does NOT consume the rejection observed by
      // raceHeartbeat's own `readerP.then(...)` on the re-race (idle timeout → onIdleReconnect + reconnect;
      // stream cut → reconnect); it only guarantees no abandoned readerP unhandled-rejects. (RT-1 SC1 / B1)
      void readerP.catch(() => {});
      for (;;) {
        if (args.signal?.aborted) break;

        const r = await raceHeartbeat(readerP, tickSleepMs, args.signal);
        if (r.tick) {
          // On abort the heartbeat wins immediately and forever (the read never settles) — break to the
          // outer loop, whose top `return`s on the aborted signal, rather than spin ticks post-teardown.
          if (args.signal?.aborted) break;

          yield { kind: 'tick' };
          continue; // re-race the SAME pending read — no line consumed, no new read requested
        }

        const step = r.value!;
        if (step.done) break;

        const line = step.value;
        readerP = it.next(); // this line settled — request the next before processing (order preserved)
        // DO NOT REMOVE — same B1 guard as the initial readerP above: this prefetched next-read is created
        // BEFORE we parse/yield the current block, so if the consumer abandons the generator while it is
        // paused at the yield below (abort / .return() / throw), this readerP gets no further re-race and
        // would unhandled-reject on the signal-aborted body (or the idle-timeout reject). Separate handler,
        // does not mask the re-race's observation. (RT-1 SC1 / B1)
        void readerP.catch(() => {});
        const batch = JSON.parse(line);
        if (batch?.header?.number != null) {
          const num = batch.header.number as number;
          // Record this height's hash in the ring so the NEXT resume (at num+1) carries it as
          // parentBlockHash. Overwrite-per-height (a reorged/redelivered height replaces its prior hash).
          ring.set(num, batch.header.hash as string);
          deliveredThisConn += 1;
          pruneRing();
          cursor = num + 1; // resume past this block on reconnect
          syncDiag(parentBlockHash, deliveredThisConn);
          yield {
            kind: 'block',
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
            // cached on finalize, so the gap survived restarts. The reopen at `num` sends
            // `parentBlockHash = ring.get(num − 1)` — the ring holds num−1 from its earlier delivery.
            cursor = num;
            reopen = true;
            break;
          }
        }
      }
    } catch (err) {
      // The idle guard rejects with a distinct message after `idleMs` of cumulative silence; anything else
      // is an ordinary stream cut. Both reconnect from `cursor` (the tick + backoff sleep run below); the
      // idle case additionally fires the debug seam so an idle recycle is distinguishable at the wire.
      if (err instanceof Error && err.message.includes('idle timeout')) {
        idleExpired = true;
      }
    } finally {
      // Cancel the reader's finally on early break/reopen/error — closing the body even when we stopped
      // mid-stream without draining (`for await` did this implicitly; the manual driver must do it too).
      void it.return?.(undefined).catch(() => {});
    }
    // On abort the inner driver broke out (loop-top guard or the heartbeat-win abort re-check). Return NOW,
    // before the finalize-cadence tick + backoff below: the outer loop top would only `return` on the same
    // aborted signal anyway, so emitting one more `{ kind: 'tick' }` (and sleeping) post-teardown is a leak
    // of a tick past abort. The normal reopen / reconnect / idle-tick paths are untouched — this fires only
    // when the signal is already aborted. (codex NIT)
    if (args.signal?.aborted) return;

    if (idleExpired) args.onIdleReconnect?.();
    if (!reopen) {
      yield { kind: 'tick' };
      await sleep(Math.min(200, tickSleepMs), args.signal);
    } // filter changed ⇒ re-open immediately, no backoff
  }
}

/**
 * Parse a /stream 409's `previousBlocks` (the canonical replacement chain). Reads the body via
 * `readTextWithIdle` (bounded, cancel-on-stall) then extracts `{number, hash}` entries. Tolerant: a
 * malformed/absent body yields `[]` rather than throwing (the caller's step-down fallback still terminates
 * at the floor). (issue #33)
 */
async function parsePreviousBlocks(res: Response): Promise<NumberedHash[]> {
  const text = await readTextWithIdle(res, 10_000);
  const parsed = JSON.parse(text) as { previousBlocks?: unknown };
  const raw = Array.isArray(parsed?.previousBlocks)
    ? parsed.previousBlocks
    : [];
  const out: NumberedHash[] = [];
  for (const e of raw) {
    if (
      e != null &&
      typeof (e as NumberedHash).number === 'number' &&
      typeof (e as NumberedHash).hash === 'string'
    ) {
      out.push({
        number: (e as NumberedHash).number,
        hash: (e as NumberedHash).hash,
      });
    }
  }
  return out;
}

/** Render the shared StreamDiag as a single-line fatal-message fragment. Empty when no diag is wired. */
export function diagDump(diag: StreamDiag | undefined): string {
  if (diag === undefined) return '';

  const prev =
    diag.lastPreviousBlocks !== undefined
      ? fmtNumberedHashes(diag.lastPreviousBlocks)
      : 'none';

  return `[diag: cursor=${diag.cursor}, parentBlockHashSent=${diag.parentBlockHashSent ?? 'none'}, blocksDeliveredThisConn=${diag.blocksDeliveredThisConn}, ring(last8)=${fmtNumberedHashes(diag.ring)}, last409.previousBlocks=${prev}]`;
}

/**
 * Render the unfinalized window + anchor for a `gap` fatal (issue #33): the window size, its first/tip
 * `{number,hash}`, the entry at `parentHeight` (`next.number − 1` — present/absent + hash pins whether an
 * orphaned sibling occupied the local N−1), and the anchor. In-memory only.
 */
export function windowDump(
  unfinalized: Light[],
  anchor: Light | undefined,
  parentHeight: number,
): string {
  const first = unfinalized[0];
  const tip = unfinalized[unfinalized.length - 1];
  const at = unfinalized.find((b) => b.number === parentHeight);
  const atStr = at !== undefined ? `present ${at.hash}` : 'absent';
  const firstStr =
    first !== undefined ? `${first.number}:${first.hash}` : 'none';
  const tipStr = tip !== undefined ? `${tip.number}:${tip.hash}` : 'none';
  const anchorStr =
    anchor !== undefined ? `${anchor.number}:${anchor.hash}` : 'none';

  return `[window: size=${unfinalized.length}, first=${firstStr}, tip=${tipStr}, at(${parentHeight})=${atStr}, anchor=${anchorStr}]`;
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

/**
 * Race a PENDING NDJSON read (`readerP`) against a `tickSleepMs` heartbeat and `signal` abort — the engine
 * of the tick-transparent line-wait (G2b, RT-1 SC1). Returns `{ tick: true }` when the heartbeat (or abort)
 * wins so the caller yields a `{ kind: 'tick' }` and RE-RACES THE SAME `readerP` (the read is never consumed
 * nor re-requested on a tick, so no NDJSON line can be dropped or reordered), or `{ tick: false, value }`
 * carrying the settled read when the reader wins. Leak-safety: exactly one heartbeat timer per race, always
 * cleared in `finally`; the abort listener is `{ once: true }` and removed on the timer path — so no timer or
 * listener accrues across the many ticks of a long silent-open window. `readerP` rejecting (the `idleMs`
 * guard firing, or a stream cut) propagates as a rejection the caller's try/catch handles. (RT-1 SC1)
 */
export async function raceHeartbeat<T>(
  readerP: Promise<T>,
  tickSleepMs: number,
  signal?: AbortSignal,
): Promise<{ tick: true } | { tick: false; value: T }> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  const read = readerP.then((value) => ({ tick: false as const, value }));
  // If the heartbeat wins this race, `read` (derived from the pending `readerP`) is left unsettled; should
  // `readerP` later reject (idle guard / stream cut) its rejection would be UNOBSERVED on this wrapper and
  // surface as an unhandledRejection. The caller re-races the SAME `readerP`, where the rejection IS
  // observed (or its try/catch awaits it) — so swallow here on the derived wrapper only, never on readerP.
  read.catch(() => {});
  const beat = new Promise<{ tick: true }>((resolve) => {
    if (signal?.aborted) return resolve({ tick: true });

    onAbort = (): void => {
      resolve({ tick: true });
    };
    timer = setTimeout(() => resolve({ tick: true }), tickSleepMs);
    signal?.addEventListener('abort', onAbort, { once: true });
  });

  try {
    return await Promise.race([read, beat]);
  } finally {
    // Clear the heartbeat timer whoever wins — a reader-win must not leave a dangling timer to fire later
    // (property 2: no timer accrues across a silent-open window). The abort listener is removed here too;
    // `{ once: true }` covers the fired-abort case, this covers the reader-win / timer-win cases.
    if (timer !== undefined) clearTimeout(timer);
    if (onAbort !== undefined) signal?.removeEventListener('abort', onAbort);
  }
}

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
 * wrong-fork finalize guard below. Absent hash ⇒ number-only finality (no hash check). */
export type FinalizedHead = { number: number; hash?: string };

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
     * Delivery-progress watchdog (RT-G10, INV-24). Upper bound (ms) on how long the probed finalized head
     * may advance while ZERO blocks are delivered before the stall is declared fatal. Complements the B1
     * defer watchdog — B1 bounds a window that DELIVERS but can't hash-verify finality; this bounds the
     * dual case where NOTHING is delivered at all (204/bodyless-200/reconnect churn or the silent fetch-
     * error retry loop) yet the chain is demonstrably ALIVE because its finalized head keeps climbing. A
     * quiet/halted chain (head static) never trips it — it is PROGRESS-conditioned. Injectable for tests;
     * production default 600_000 (10 min, aligned with the B1 bound). (RT-1 SC3)
     */
    deliveryProgressMaxMs?: number;
    /**
     * Delivery-progress watchdog head-advance threshold (blocks, RT-G10, INV-24). The watchdog is fatal
     * only when the probed finalized head has advanced by AT LEAST this many blocks past the delivery
     * baseline (highest delivered block number, floored at the first observed finalized head — RT-G10)
     * WHILE delivery was zero for the whole bound. A single-block finality lag (head
     * ticked forward once while a block is momentarily in flight) must NOT trip it, so the default is a
     * comfortable multiple of one. Injectable for tests; production default 16 (see the resolver comment
     * for the rationale). (RT-1 SC3)
     */
    deliveryProgressThreshold?: number;
    /**
     * Non-delivery signal from the producer's fetch-error retry loop (E1 site). streamHotBlocks yields a
     * `{kind:'tick'}` on a transient fetch throw (the read never opened), which the delivery watchdog
     * already counts as non-delivery; this callback lets the LOGGER-BEARING shell surface a rate-limited
     * warn so a silent retry storm is diagnosable at the wire (same seam pattern as `onIdleReconnect`).
     * Rate-limiting is the shell's responsibility — this fires once per error round. Absent ⇒ silent (unit
     * call sites). (RT-1 SC3)
     */
    onFetchError?: () => void;
    /**
     * The last FINALIZED block at startup (syncProgress.finalized as a Light) — the reconcile anchor.
     * Advanced to each finalize's tip. REQUIRED (wave 4): startup is always anchored.
     */
    anchor: Light;
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
  const half = Math.max(1, Math.floor(pollMs / 2));
  // Wire the /stream fork-negotiation state (issue #33): SEED the delivered-hash ring with the startup
  // anchor so the first request carries its hash, and expose the FINALIZED FLOOR (the anchor's number,
  // advanced on each finalize) so the ring prunes and the 409 rewind bounds correctly. `diag` mirrors the
  // shell's ring/connection state so the `gap` fatal below can dump it alongside the window/anchor. When
  // there is no anchor (legacy unit-test call sites), the floor defaults to fromBlock−1 and no seed is set.
  const diag: StreamDiag = {
    ring: [],
    cursor: args.fromBlock,
    parentBlockHashSent: undefined,
    blocksDeliveredThisConn: 0,
    lastPreviousBlocks: undefined,
  };
  const streamArgs: PortalRealtimeArgs = {
    ...args,
    seedRing:
      anchor !== undefined
        ? { number: anchor.number, hash: anchor.hash }
        : args.seedRing,
    getFinalizedFloor: () => anchor?.number ?? args.fromBlock - 1,
    diag,
    tickSleepMs: Math.min(500, half),
    errorSleepMs: Math.min(1000, half),
  };
  // B1 defer-streak watchdog: `deferStreakStart` is the Date.now() of the FIRST poll in an unbroken run of
  // hash-unverifiable-finalize deferrals (undefined ⇒ not armed). Set when a poll defers and the streak is
  // not already armed; cleared whenever a finalize poll does NOT defer (finalize emitted, no finalizedTip,
  // or no probe result). If it ever runs longer than the bound the deferral is fatal — see the branch.
  // (delta review B1)
  const deferMaxMs = args.finalizeDeferMaxMs ?? 600_000;
  let deferStreakStart: number | undefined;

  // Delivery-progress watchdog (RT-G10, INV-24). PROGRESS-CONDITIONED: fatal ONLY when the probed finalized
  // head has advanced ≥ `deliveryProgressThreshold` blocks past the HIGHEST DELIVERED block number WHILE
  // ZERO blocks were delivered for ≥ `deliveryProgressMaxMs`. It bounds the no-delivery case B1 does not:
  // B1 fires when the window DELIVERS but the finalized head sits above it hash-unverifiably; this fires when
  // NOTHING is delivered at all (204/bodyless-200/reconnect churn, or the silent E1 fetch-error retry loop)
  // yet the chain is provably ALIVE because it has FINALIZED blocks ABOVE everything we delivered. A
  // quiet/halted chain (finality static, or ≤ threshold past the delivered tip) NEVER trips it — normal
  // idling, parity with RPC realtime.
  //   • WHY DELIVERED HEIGHT, NOT FINALIZED-HEAD-AT-DELIVERY (RT-G10 correctness fix): the /stream delivers
  //     UNFINALIZED tip blocks that run FAR AHEAD of the finalized head (steady state: delivered tip 500,
  //     finalized head 100). The old baseline was the finalized head observed AT the last delivery, so a
  //     chain-TIP freeze (an L2 sequencer pause — nothing NEW to deliver) while FINALITY merely caught up
  //     its backlog over blocks 100→116 that were ALREADY delivered read as a ≥-threshold advance with zero
  //     deliveries and FALSE-FATAL'd — it used FINALITY progress as a proxy for NEW-BLOCK production, and the
  //     two diverge on a tip-freeze. Baselining on the highest DELIVERED block makes `finalized ≤ delivered ⇒
  //     no trip` hold BY CONSTRUCTION (finality catching up over delivered blocks can never trip), while a
  //     genuine post-delivery stall — finality climbing ≥ threshold BEYOND the highest delivered block,
  //     provably new blocks we are not delivering — still fatals.
  //   • `lastDeliveryMs` is stamped Date.now() on every delivered block AND INITIALIZED to loop entry (not
  //     0/epoch): a fresh start that begins INTO an ongoing outage — zero deliveries ever — must arm the
  //     bound from process start, exactly as the first delivery would have; epoch-0 would make the very first
  //     poll's `now - lastDeliveryMs` astronomically exceed any bound and false-fatal a healthy startup.
  //   • `deliveryBaseline` is `max(first-observed finalized head, highest delivered block number)` — the
  //     block height the advance is measured against. Undefined until the first probe: with no height ever
  //     observed there is nothing to measure against, so the first successful poll ADOPTS its finalized head
  //     as the baseline WITHOUT resetting the clock (already armed at loop entry — preserving the
  //     outage-from-start guarantee). Adopting the FIRST PROBED HEAD (not the resume anchor) is deliberate: a
  //     process that RESUMES after downtime sees a finalized head already far above its anchor and must be
  //     allowed to backfill that pre-existing deficit for the whole bound without a false fatal — the anchor
  //     as baseline would fatal a legitimately-catching-up resume. Every delivery then RAISES the baseline to
  //     its own block number, never LOWERS it (a resume that adopts a high head then delivers lower catch-up
  //     blocks must not regress the baseline below the head), so the trip only ever measures finality's climb
  //     PAST the furthest thing we have delivered or provably know finalized. The block arm never probes
  //     (ticks must not touch the network or the window; the tick is the clock, wall time is the data).
  // Fatal = a loud throw with the same diagDump + "restart" tail as the gap/B1 fatals; NEVER an RPC fallback
  // (ruling Q2: a watchdog fatal-restarts, it never silently switches transport). (RT-1 SC3)
  const deliveryProgressMaxMs = args.deliveryProgressMaxMs ?? 600_000;
  const deliveryProgressThreshold = args.deliveryProgressThreshold ?? 16;
  let lastDeliveryMs = Date.now();
  let deliveryBaseline: number | undefined;

  // Finalize-poll cadence + B1 defer watchdog, relocated from the inline gate.
  // The hash-unverifiable defer branch returns because this helper is the final step for each turn.
  async function* runFinalizeCadence(): AsyncGenerator<PortalRealtimeEvent> {
    // finalize on a cadence (cheap head probe), not every block
    const now = Date.now();
    if (now - lastFinalizePoll >= pollMs) {
      lastFinalizePoll = now;
      const fh = await args.finalizedHead().catch(() => undefined);
      const fhNumber = fh?.number;
      const fhHash = fh?.hash;
      // No probe result this poll: the streak of deferrals (if any) is broken — clear it so a later,
      // hash-unverifiable deferral times its OWN run, not a run interleaved with probe outages. (B1)
      if (fhNumber === undefined) deferStreakStart = undefined;

      // Delivery-progress watchdog (RT-G10, INV-24): evaluate on the head THIS poll already probed — no
      // extra probe, no window touch. A failed probe (fhNumber undefined) carries no head signal, so it can
      // neither advance the baseline nor trip the watchdog; skip it. Otherwise:
      //   • if a baseline was ever established, fatal when the finalized head has climbed ≥ threshold PAST the
      //     highest DELIVERED block (the baseline) WHILE zero blocks were delivered for the whole bound —
      //     finality provably above everything we delivered + delivery flatlined = the chain has NEW blocks
      //     the /stream is starving us of. Loud restart, never a silent transport switch (Q2). Finality
      //     merely catching up OVER already-delivered blocks (finalized ≤ delivered) can never trip.
      //   • first observed head with no prior baseline: adopt it as the baseline without touching the clock
      //     (already armed at loop entry), so an outage from process start still times from start.
      if (fhNumber !== undefined) {
        if (deliveryBaseline === undefined) {
          deliveryBaseline = fhNumber;
        } else if (
          fhNumber - deliveryBaseline >= deliveryProgressThreshold &&
          now - lastDeliveryMs >= deliveryProgressMaxMs
        ) {
          throw new Error(
            `Portal realtime: the finalized head advanced to ${fhNumber}, ${fhNumber - deliveryBaseline} blocks PAST the highest delivered block ${deliveryBaseline}, while /stream delivered ZERO blocks for ${now - lastDeliveryMs}ms — the chain has finalized blocks the stream never delivered (endless 204s/reconnects or a transient-error retry loop), so indexing has silently stalled. ${diagDump(diag)} Restart to re-sync from the finalized head.`,
          );
        }
      }

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

            return; // hash-unverifiable finalize deferred until the window reaches fhNumber
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

  for await (const item of streamHotBlocks(streamArgs)) {
    if (item.kind === 'tick') {
      yield* runFinalizeCadence();
      continue;
    }

    const { header, logs, transactions } = item;
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
      // Diagnostic dump (issue #33): the window shape + the ENTRY AT next.number−1 (the parent height —
      // present/absent + hash pins whether an orphaned sibling was the local N−1), the anchor, and the
      // shell's ring/connection state (whether a parentBlockHash was sent, blocks delivered, the last 409).
      // In-memory only — a future instance is self-identifying without a code change.
      throw new Error(
        `Portal realtime: streamed block ${light.number} (${light.hash}) has an unknown parent ${light.parentHash} — a reorg deeper than the unfinalized window, or a skipped block. ${windowDump(unfinalized, anchor, light.number - 1)} ${diagDump(diag)} Cannot reconcile safely; restart to re-sync from the finalized head.`,
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
    // Delivery-progress watchdog stamp (RT-G10, INV-24): this is the ONE point a real block is delivered
    // (a skipped `duplicate && !redelivered` returned above; a `gap` threw). Re-baseline the no-delivery
    // clock to now, and RAISE the delivered-height baseline to THIS block's number — so the NEXT stall
    // measures finality's climb against the furthest block we have actually delivered, never below it. Use a
    // max (never lower): a resume that adopted a high first-probed finalized head then delivers lower
    // catch-up blocks must not regress the baseline beneath that head. A delivery before any poll has run
    // leaves `deliveryBaseline` undefined here and adopts `light.number` directly. No probe — the block arm
    // never touches the network or the window. (RT-1 SC3)
    lastDeliveryMs = Date.now();
    deliveryBaseline = Math.max(deliveryBaseline ?? light.number, light.number);
    yield {
      type: 'block',
      block,
      logs: syncLogs,
      transactions: syncTxs,
      hasMatchedFilter: syncLogs.length > 0,
    };
    yield* runFinalizeCadence();
  }
}
