/**
 * portal-config.ts — parse + validate ALL `PORTAL_*` env ONCE into a frozen `PortalConfig`.
 *
 * Contract (INV-14): the environment is read exactly once per sync instance and validated
 * eagerly; a garbage value fails fast with an actionable message instead of silently
 * poisoning downstream math (today `Number("abc") → NaN` flows into the chunk grid). The
 * result is frozen and injectable — tests build a config directly rather than mutating
 * `process.env`.
 *
 * Every knob below is documented API (names, defaults, semantics are frozen). One new knob:
 * `PORTAL_CHECKS=off|on|strict` (default `on`) selects the runtime invariant mode.
 *
 * Pure: the only impurity is the default argument `process.env`; pass an explicit `env` for
 * deterministic tests.
 */
import type { CheckMode } from './portal-invariant.js';

/** Thrown when a `PORTAL_*` value is malformed or out of range. */
export class PortalConfigError extends Error {
  constructor(msg: string) {
    super(`Invalid Portal configuration: ${msg}`);
    this.name = 'PortalConfigError';
  }
}

export type PortalConfig = Readonly<{
  /** PORTAL_API_KEY — x-api-key header for authenticated Portal datasets. */
  apiKey: string | undefined;
  /** PORTAL_CHUNK_BLOCKS (500000) — base width of an aligned data chunk, pre density scaling. */
  chunkBlocks: number;
  /** PORTAL_READAHEAD (6) — max parallel prefetch depth beyond the in-service chunk. */
  readahead: number;
  /** PORTAL_BUFFER_SIZE (100) — Portal `buffer_size` fan-out per stream request. */
  bufferSize: number;
  /** PORTAL_DISCOVERY_WINDOWS (8) — disjoint concurrent windows a factory scan splits into. */
  discoveryWindows: number;
  /** PORTAL_MIN_CONCURRENCY (8) — AIMD floor for the shared request concurrency limit. */
  minConcurrency: number;
  /** PORTAL_MAX_CONCURRENCY (48) — AIMD ceiling. */
  maxConcurrency: number;
  /** PORTAL_START_CONCURRENCY (16) — AIMD initial limit. */
  startConcurrency: number;
  /** PORTAL_MAX_ROWS_IN_MEM (250000) — buffered-row budget that backpressures read-ahead. */
  maxRowsInMem: number;
  /** PORTAL_CHUNK_FIXED — when set, disables block-density chunk scaling. */
  chunkFixed: boolean;
  /** PORTAL_TRACE_CHUNK_BLOCKS (2000) — trace-safe chunk cap for dense (trace/block) sources. */
  traceChunkBlocks: number;
  /** PORTAL_FINALIZED_HEAD — test/ops override for the Portal finalized head. */
  finalizedHead: number | undefined;
  /** PORTAL_REALTIME — "stream" enables Portal-native realtime (else RPC realtime). */
  realtime: string | undefined;
  /** PORTAL_METRICS_FILE — per-chain backfill metrics are written to `<file>.<chainId>`. */
  metricsFile: string | undefined;
  /** PORTAL_GATE_LOG — periodically log the AIMD/backpressure snapshot. */
  gateLog: boolean;
  /** PORTAL_CHECKS (on) — runtime invariant mode. */
  checks: CheckMode;
}>;

type Env = Record<string, string | undefined>;

/** Parse a required-integer knob, defaulting when unset, validating range when present. */
const intKnob = (
  env: Env,
  name: string,
  def: number,
  {
    min = Number.MIN_SAFE_INTEGER,
    max = Number.MAX_SAFE_INTEGER,
  }: { min?: number; max?: number } = {},
): number => {
  const raw = env[name];
  if (raw === undefined) return def;
  const n = Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n)) {
    throw new PortalConfigError(`${name}="${raw}" must be an integer`);
  }
  if (n < min || n > max) {
    throw new PortalConfigError(
      `${name}=${n} out of range [${min}, ${max === Number.MAX_SAFE_INTEGER ? '∞' : max}]`,
    );
  }
  return n;
};

const parseChecks = (raw: string | undefined): CheckMode => {
  if (raw === undefined || raw === '') return 'on';
  if (raw === 'off' || raw === 'on' || raw === 'strict') return raw;
  throw new PortalConfigError(
    `PORTAL_CHECKS="${raw}" must be one of off|on|strict`,
  );
};

/** Load + validate the frozen `PortalConfig` from `env` (defaults to `process.env`). */
export function loadPortalConfig(env: Env = process.env): PortalConfig {
  const minConcurrency = intKnob(env, 'PORTAL_MIN_CONCURRENCY', 8, { min: 1 });
  const maxConcurrency = intKnob(env, 'PORTAL_MAX_CONCURRENCY', 48, {
    min: minConcurrency,
  });
  const startConcurrency = intKnob(env, 'PORTAL_START_CONCURRENCY', 16, {
    min: 1,
  });

  const finalizedRaw = env.PORTAL_FINALIZED_HEAD;

  const cfg: PortalConfig = {
    apiKey: env.PORTAL_API_KEY || undefined,
    chunkBlocks: intKnob(env, 'PORTAL_CHUNK_BLOCKS', 500_000, { min: 1 }),
    readahead: intKnob(env, 'PORTAL_READAHEAD', 6, { min: 0 }),
    bufferSize: intKnob(env, 'PORTAL_BUFFER_SIZE', 100, { min: 1 }),
    discoveryWindows: intKnob(env, 'PORTAL_DISCOVERY_WINDOWS', 8, { min: 1 }),
    minConcurrency,
    maxConcurrency,
    startConcurrency,
    maxRowsInMem: intKnob(env, 'PORTAL_MAX_ROWS_IN_MEM', 250_000, { min: 1 }),
    chunkFixed: Boolean(env.PORTAL_CHUNK_FIXED),
    traceChunkBlocks: intKnob(env, 'PORTAL_TRACE_CHUNK_BLOCKS', 2_000, {
      min: 1,
    }),
    finalizedHead: finalizedRaw
      ? intKnob(env, 'PORTAL_FINALIZED_HEAD', 0, { min: 0 })
      : undefined,
    realtime: env.PORTAL_REALTIME,
    metricsFile: env.PORTAL_METRICS_FILE || undefined,
    gateLog: Boolean(env.PORTAL_GATE_LOG),
    checks: parseChecks(env.PORTAL_CHECKS),
  };
  return Object.freeze(cfg);
}
