/**
 * Freshness hook emission — env-gated, OFF by default. When `PORTAL_FRESHNESS_HOOKS === '1'`, each
 * batch-recv (and later durable-commit-ack) site writes ONE NDJSON line to stderr so a journald-following
 * collector can parse them. A module-level injectable sink lets tests capture hooks without scraping
 * stderr.
 *
 * Flag unset/falsy ⇒ zero output, zero side effects (byte-identical to baseline).
 */

/** The shape of a freshness hook line (NDJSON on stderr). */
export type FreshnessHook = {
  evt: 'batch-recv' | 'durable-commit-ack';
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
