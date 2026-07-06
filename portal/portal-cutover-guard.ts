/**
 * portal-cutover-guard.ts — skip the historical→realtime cutover refetch for fully-bounded chains.
 *
 * After ponder's historical backfill drains, its cutover loop live-probes EVERY chain's head
 * (`eth_getBlockByNumber("latest")` → a finalized-target fetch, under
 * `logger.child({ action: "refetch_finalized_block" })`) inside a `Promise.all` across all chains,
 * and this probe sits ON THE READINESS CRITICAL PATH — `/ready` only flips 200 once the generator
 * returns. For a chain with a configured historical end block that is already at or below the current
 * finalized block, that probe is provably RANGE-NEUTRAL: the backfill's upper bound is
 * `to = min(finalized, end)`, a refetch can only RAISE `finalized`, and `min(higher, end) = end` is
 * unchanged — no interval is added or removed. Realtime is skipped anyway for such a chain
 * (`syncProgress.isEnd()` is true, so `getRealtimeEventGenerator` returns early). Yet a flaky RPC that
 * makes the probe reject (e.g. JSON-RPC "Missing or invalid parameters" / -32602, which
 * `rpc/index.ts` `shouldRetry` treats as retryable → 10 attempts with backoff) delays readiness for
 * minutes and, on retry exhaustion, escapes the fire-and-forget runner as an unhandledRejection that
 * kills a FULLY-INDEXED app with exit code 75 before `/ready` ever serves 200. With omnichain ordering
 * the probe is UNCONDITIONAL (no 30s freshness gate), so a permanently-failing RPC blocks completion
 * across restarts.
 *
 * The guard (INV-18): compute `isEndCapped := end !== undefined && end.number <= finalized.number`
 * per chain, EXCLUDE end-capped chains from the cutover probe set, and skip the round entirely when
 * ALL chains are end-capped (a fully-bounded app performs ZERO cutover refetches). Unbounded and
 * finality-capped chains (`end === undefined`, or `end` still ABOVE finalized) keep today's behavior
 * EXACTLY — including the fork's stream-mode clamp that the wiring patch applies right after the probe.
 *
 * The predicate is monotone: `end <= finalized` holds at start and `finalized` only ever rises across
 * cutover iterations, so an end-capped chain stays end-capped — the guard never flips a chain back into
 * the probe set mid-loop.
 *
 * Pure w.r.t. I/O and env: it reads only block numbers, so it is unit-testable without any RPC/network.
 */

/** The minimal block shape the guard reads — a hex block number, as carried by `SyncBlock`/`LightBlock`. */
export type BlockLike = { number: string };

/** The minimal per-chain sync shape the guard reads at a cutover site. */
export type CutoverSyncProgress = {
  /** Configured historical end block (`SyncBlock`/`LightBlock`), or `undefined` when unbounded. */
  end: BlockLike | undefined;
  /** Current finalized block for the chain (`SyncBlock`/`LightBlock`). */
  finalized: BlockLike;
};

/** Parse a hex block number (`"0x…"`) to a JS number. Local to keep the guard dependency-free. */
const blockNumber = (block: BlockLike): number =>
  Number.parseInt(block.number, 16);

/**
 * True when this chain's configured historical end block is at or below its current finalized block —
 * i.e. the backfill's upper bound `to = min(finalized, end)` equals `end` and a cutover refetch (which
 * can only raise `finalized`) cannot change it. Such a chain must NEVER be probed at cutover.
 *
 * `end === undefined` (an unbounded source) is NOT end-capped. An `end` strictly ABOVE `finalized`
 * (finality-capped for now) is NOT end-capped either — it keeps probing so the backfill can advance as
 * finality catches up.
 */
export const isEndCappedAtCutover = (
  syncProgress: CutoverSyncProgress,
): boolean =>
  syncProgress.end !== undefined &&
  blockNumber(syncProgress.end) <= blockNumber(syncProgress.finalized);

/**
 * The indices (into `chains`) whose head must be REFETCHED at cutover: every chain that is not
 * end-capped. Reading each chain's live `syncProgress` from `perChainSync`, this drops the end-capped
 * chains. An empty result means the whole round can be skipped (see `shouldSkipCutoverRound`) — a
 * fully-bounded app issues zero cutover refetches.
 *
 * The returned indices are in `chains` order, so a caller mapping over `chains` by index can test
 * membership and pass an end-capped chain's existing finalized block through untouched instead of
 * probing it.
 */
export const cutoverProbeIndices = <TChain>(
  chains: readonly TChain[],
  perChainSync: Map<TChain, { syncProgress: CutoverSyncProgress }>,
): number[] => {
  const indices: number[] = [];
  for (let i = 0; i < chains.length; i++) {
    const entry = perChainSync.get(chains[i]!);
    if (entry === undefined) {
      // Defensive: an unmapped chain has no known finality — treat it as probeable (today's behavior),
      // never silently skip it.
      indices.push(i);
      continue;
    }

    if (isEndCappedAtCutover(entry.syncProgress)) continue;

    indices.push(i);
  }

  return indices;
};

/**
 * True when NO chain needs a cutover refetch — every chain is end-capped, so the whole probe round
 * (and the `shouldCatchup` re-scan that follows it) is a no-op and the cutover loop should exit.
 */
export const shouldSkipCutoverRound = <TChain>(
  chains: readonly TChain[],
  perChainSync: Map<TChain, { syncProgress: CutoverSyncProgress }>,
): boolean => cutoverProbeIndices(chains, perChainSync).length === 0;

/**
 * Omnichain-only refinement of `shouldSkipCutoverRound` (INV-18): the all-capped `break` in
 * `getHistoricalEventsOmnichain` must ALSO require that no fetched-but-parked events remain.
 *
 * The omnichain generator parks events whose checkpoint exceeds the omnichain finalized checkpoint
 * (`omnichainTo`) — events between that shared boundary and their own chain's higher boundary — in
 * `pendingEvents`, to preserve cross-chain ordering. Upstream drains them via the catchup path: the
 * cutover probe adopts a raised finalized, `isCatchup` flips true, and the next loop iteration
 * re-filters `pendingEvents` against the advanced `omnichainTo`. Breaking out unconditionally when
 * every chain is end-capped skips that drain; the trailing `yield { type: "pending", result: … }`
 * then hands the parked events to realtime — which skips every end-capped chain (`syncProgress.isEnd()`),
 * so with all chains capped those events are silently DROPPED. This bites when one chain's end
 * checkpoint exceeds the min-over-chains finalized checkpoint (a recently-capped end alongside lagging
 * finality on another chain).
 *
 * So take the all-capped break ONLY when `pendingCount === 0`. With pending non-empty, fall through to
 * the upstream probe/catchup path unchanged — it converges: finalized advances, `omnichainTo` rises,
 * `pendingEvents` drains, and a later round (now with empty pending) breaks.
 *
 * `pendingCount` is passed in (rather than reading the buffer here) to keep this helper pure and the
 * decision unit-testable at the seam. Multichain has no pending-events mechanism and keeps plain
 * `shouldSkipCutoverRound`.
 */
export const shouldSkipOmnichainCutoverRound = <TChain>(
  chains: readonly TChain[],
  perChainSync: Map<TChain, { syncProgress: CutoverSyncProgress }>,
  pendingCount: number,
): boolean =>
  pendingCount === 0 && shouldSkipCutoverRound(chains, perChainSync);
