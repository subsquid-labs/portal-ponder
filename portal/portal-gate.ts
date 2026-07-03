/**
 * portal-gate.ts — the shared adaptive Portal controller: AIMD request concurrency + a buffered-row
 * budget, as a PURE reducer (`gateReduce`) wrapped by a thin async shell (`createGate`).
 *
 * All chains stream from the SAME Portal endpoint, so request concurrency, CU/throttle headroom and
 * buffered memory are ONE shared budget, not per-chain (15 chains each running a private read-ahead is
 * what OOMs and gets CU-throttled). Two self-tuning controls:
 *   • AIMD concurrency — start low, ramp +2 every 8 clean generations, halve on 429/503/timeout.
 *     Discovers the endpoint's LIVE capacity (mirrors Ponder's native RPC AIMD) but GLOBAL.
 *   • Rows-in-memory budget — read-ahead prefetches until the shared buffer reaches the cap, then
 *     backpressures. Caps memory regardless of chain count.
 *
 * The reducer is the whole state machine (INV-8 AIMD bounds; INV-7 rows ≥ 0); the shell only owns the
 * FIFO waiter queue and pumps it. No module-scope side effects (the PORTAL_GATE_LOG ticker lives in
 * portal-metrics and is started/stopped by the shell).
 */
import type { PortalConfig } from './portal-config.js';
import { invariant } from './portal-invariant.js';

export type GateLimits = Readonly<{
  min: number;
  max: number;
  maxRows: number;
}>;

export type GateState = Readonly<{
  limit: number; // current AIMD concurrency limit
  active: number; // in-flight requests
  ok: number; // clean generations since the last ramp/back-off
  rows: number; // buffered records across all chains' read-ahead
  limits: GateLimits;
}>;

export type GateEvent =
  | { type: 'admit' } //           a waiter is granted a slot (active++)
  | { type: 'release' } //         an in-flight request finished (active--)
  | { type: 'ok' } //              a clean full response (ramp toward MAX)
  | { type: 'throttle' } //        429/5xx/timeout (halve toward MIN)
  | { type: 'addRows'; n: number } // NDJSON batch arrived
  | { type: 'freeRows'; n: number }; // chunk evicted / failed

/** Initial state; `start` is clamped into [min, max] so INV-8 holds from the first event. */
export const gateInit = (limits: GateLimits, start: number): GateState => ({
  limit: Math.min(limits.max, Math.max(limits.min, start)),
  active: 0,
  ok: 0,
  rows: 0,
  limits,
});

/** A slot is available iff fewer requests are in flight than the current limit. */
export const canAdmit = (s: GateState): boolean => s.active < s.limit;

const RAMP_EVERY = 8; // clean generations per +RAMP_STEP
const RAMP_STEP = 2;

/**
 * The pure AIMD/row-budget transition. Enforces INV-8 (MIN ≤ limit ≤ MAX, active ≥ 0) and INV-7
 * (rows ≥ 0) on every produced state.
 */
export const gateReduce = (s: GateState, e: GateEvent): GateState => {
  const { min, max } = s.limits;
  let next: GateState;
  switch (e.type) {
    case 'admit':
      next = { ...s, active: s.active + 1 };
      break;
    case 'release':
      next = { ...s, active: Math.max(0, s.active - 1) };
      break;
    case 'ok': {
      const ok = s.ok + 1;
      if (ok >= RAMP_EVERY && s.limit < max)
        next = { ...s, limit: Math.min(max, s.limit + RAMP_STEP), ok: 0 };
      else next = { ...s, ok };
      break;
    }
    case 'throttle':
      next = { ...s, limit: Math.max(min, Math.floor(s.limit / 2)), ok: 0 };
      break;
    case 'addRows':
      next = { ...s, rows: s.rows + e.n };
      break;
    case 'freeRows':
      next = { ...s, rows: Math.max(0, s.rows - e.n) };
      break;
  }
  invariant(
    'INV-8',
    next.limit >= min && next.limit <= max,
    'AIMD limit out of [MIN, MAX]',
    () => ({ limit: next.limit, min, max }),
  );
  invariant('INV-8', next.active >= 0, 'AIMD active went negative', () => ({
    active: next.active,
  }));
  invariant('INV-7', next.rows >= 0, 'buffered rows went negative', () => ({
    rows: next.rows,
  }));
  return next;
};

export interface Gate {
  /** Await a concurrency slot (FIFO). */
  acquire(): Promise<void>;
  /** Return a slot and pump the queue. */
  release(): void;
  /** A clean full response → ramp concurrency. */
  onOk(): void;
  /** Congestion → back off concurrency. */
  onThrottle(): void;
  /** Register `n` buffered rows against the shared budget. */
  addRows(n: number): void;
  /** Free `n` previously-registered rows. */
  freeRows(n: number): void;
  /** True when the row budget is reached (read-ahead should stop going deeper). */
  saturated(): boolean;
  /** Current { limit, active, rows } for metrics/logging. */
  snapshot(): { limit: number; active: number; rows: number };
}

/**
 * The async shell around the reducer. Owns the FIFO waiter queue; every mutation goes through
 * `gateReduce`, then `pump` drains as many waiters as capacity allows (FIFO — no starvation while
 * capacity exists, INV-8).
 */
export const createGate = (cfg: PortalConfig): Gate => {
  const limits: GateLimits = {
    min: cfg.minConcurrency,
    max: cfg.maxConcurrency,
    maxRows: cfg.maxRowsInMem,
  };
  let state = gateInit(limits, cfg.startConcurrency);
  const waiters: Array<() => void> = [];
  const pump = (): void => {
    while (canAdmit(state) && waiters.length > 0) {
      state = gateReduce(state, { type: 'admit' });
      waiters.shift()!();
    }
  };
  return {
    acquire: () =>
      new Promise<void>((resolve) => {
        waiters.push(resolve);
        pump();
      }),
    release: () => {
      state = gateReduce(state, { type: 'release' });
      pump();
    },
    onOk: () => {
      state = gateReduce(state, { type: 'ok' });
      pump();
    },
    onThrottle: () => {
      state = gateReduce(state, { type: 'throttle' });
    },
    addRows: (n) => {
      state = gateReduce(state, { type: 'addRows', n });
    },
    freeRows: (n) => {
      state = gateReduce(state, { type: 'freeRows', n });
    },
    saturated: () => state.rows >= state.limits.maxRows,
    snapshot: () => ({
      limit: state.limit,
      active: state.active,
      rows: state.rows,
    }),
  };
};

// ── process-shared instance ─────────────────────────────────────────────────────────────────────
// All per-chain syncs share ONE gate (the endpoint is shared). Lazily created from the first sync's
// config — no module-scope construction/side effects. `__resetSharedGate` is test-only.
let shared: Gate | undefined;
export const sharedGate = (cfg: PortalConfig): Gate => {
  shared ??= createGate(cfg);
  return shared;
};
export const __resetSharedGate = (): void => {
  shared = undefined;
};
