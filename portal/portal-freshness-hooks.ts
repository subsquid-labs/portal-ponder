/**
 * Freshness hook emission — env-gated, OFF by default. When `PORTAL_FRESHNESS_HOOKS === '1'`, each
 * batch-recv and post-commit site writes ONE NDJSON line to stderr so a journald-following collector
 * can parse them. `durable-commit-ack` means a block was durably committed; `durable-commit-reject`
 * means the post-commit callback rejected the block. A module-level injectable sink lets tests capture
 * hooks without scraping stderr.
 *
 * Flag unset/falsy ⇒ zero output, zero side effects (byte-identical to baseline).
 */

type FreshnessHookBase = {
  chainId: number;
  from: number;
  to: number;
  batchId: number;
  batchSize: number;
  /**
   * Process-monotonic high-resolution timestamp in ms (`performance.now()`) — the delta base for
   * intra-process latency (e.g. batch-recv → durable-commit-ack). NOT wall-clock and NOT comparable
   * across processes; pair with `wall` for absolute time.
   */
  mono: number;
  /** Absolute wall-clock time (`new Date().toISOString()`) — for human/cross-process correlation. */
  wall: string;
};

/** The shape of a freshness hook line (NDJSON on stderr). */
export type FreshnessHook =
  | (FreshnessHookBase & { evt: 'batch-recv' })
  | (FreshnessHookBase & {
      evt: 'durable-commit-ack';
      blockHash: string;
      blockTimestamp: number;
    })
  | (FreshnessHookBase & {
      evt: 'durable-commit-reject';
      blockHash: string;
      blockTimestamp: number;
    });

/**
 * Test-injectable collector. When set, `emitFreshnessHook` pushes the hook here INSTEAD OF (not in
 * addition to) writing to stderr — so tests capture deterministically without scraping stderr, and the
 * production path stays a single stderr write. Reset to `undefined` after each test via
 * `setFreshnessHookCollector(undefined)`.
 */
let freshnessHookCollector: ((hook: FreshnessHook) => void) | undefined;

export function setFreshnessHookCollector(
  fn: ((hook: FreshnessHook) => void) | undefined,
): void {
  freshnessHookCollector = fn;
}

/**
 * Emit a single freshness hook line. When `PORTAL_FRESHNESS_HOOKS === '1'`:
 *   • If a test collector is installed, invoke it (no stderr write — tests capture deterministically).
 *   • Otherwise write `JSON.stringify(hook) + '\n'` to stderr (the production sink — NDJSON on a
 *     journald-followable stream).
 *
 * When the flag is unset/falsy: zero output, zero side effects.
 */
export function emitFreshnessHook(hook: FreshnessHook): void {
  if (process.env.PORTAL_FRESHNESS_HOOKS !== '1') return;

  if (freshnessHookCollector !== undefined) {
    freshnessHookCollector(hook);

    return;
  }

  process.stderr.write(JSON.stringify(hook) + '\n');
}

/**
 * Build a durable-commit-ack callback for a single block batch. Returns a function
 * `(isAccepted: boolean) => void` that fires AT MOST ONCE (defensive idempotence — core
 * guarantees exactly-once invocation) and emits exactly one post-commit hook: `durable-commit-ack`
 * when `isAccepted === true`, otherwise `durable-commit-reject`. The ack event name is reserved for
 * durably committed blocks.
 *
 * The post-commit hook is emitted via the same env-gated `emitFreshnessHook` + injectable collector
 * as the batch-recv hook, so env-OFF ⇒ zero output and the callback is inert. `mono`
 * and `wall` are sampled AT INVOCATION (post-commit) so the latency delta
 * `ack.mono − recv.mono` measures true durability latency, not yield-resume.
 */
export function makeDurableCommitAck(meta: {
  chainId: number;
  from: number;
  to: number;
  batchId: number;
  batchSize: number;
  blockHash: string;
  blockTimestamp: number;
}): (isAccepted: boolean) => void {
  let fired = false;

  return (isAccepted: boolean): void => {
    if (fired) return;

    fired = true;

    const evt =
      isAccepted === true ? 'durable-commit-ack' : 'durable-commit-reject';

    emitFreshnessHook({
      evt,
      chainId: meta.chainId,
      from: meta.from,
      to: meta.to,
      batchId: meta.batchId,
      batchSize: meta.batchSize,
      blockHash: meta.blockHash,
      blockTimestamp: meta.blockTimestamp,
      mono: performance.now(),
      wall: new Date().toISOString(),
    });
  };
}
