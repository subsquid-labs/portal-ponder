/**
 * portal-metrics.ts — the per-chain backfill stats shape, the metrics-file writer, and the optional
 * AIMD gate-log ticker.
 *
 * The metrics-file JSON shape is FROZEN: the bench harness parses it. `writeMetrics` reproduces exactly
 * the fields the historical sync wrote before this refactor. The gate-log ticker moved here out of module
 * scope (no import-time side effects) and is started once per process by the shell when PORTAL_GATE_LOG
 * is set.
 */
import { writeFileSync } from 'node:fs';
import type { Gate } from './portal-gate.js';

export type PortalProgressDiscovery = Readonly<{
  floor: number;
  through: number;
}>;

type PortalInfoLog = (entry: { service: 'portal'; msg: string }) => void;

/** Mutable per-chain counters accumulated across a backfill. */
export type PortalStats = {
  dataChunks: number;
  /** frontier chunks re-fetched over a newly-finalized tail (partial, not a fresh chunk) */
  extends: number;
  discChunks: number;
  http: number;
  logs: number;
  errors: number;
  retries: number;
  bytes: number;
  cacheHits: number;
  inflight: number;
  maxInflight: number;
  blocks: number;
  txs: number;
  receipts: number;
  traces: number;
  rpcFallback: number;
  gateWaitMs: number;
  fetchMs: number;
  transformMs: number;
};

export const createStats = (): PortalStats => ({
  dataChunks: 0,
  extends: 0,
  discChunks: 0,
  http: 0,
  logs: 0,
  errors: 0,
  retries: 0,
  bytes: 0,
  cacheHits: 0,
  inflight: 0,
  maxInflight: 0,
  blocks: 0,
  txs: 0,
  receipts: 0,
  traces: 0,
  rpcFallback: 0,
  gateWaitMs: 0,
  fetchMs: 0,
  transformMs: 0,
});

/** Write `<metricsFile>.<chainId>` (best-effort). Field shape is load-bearing for the bench harness. */
export function writeMetrics(args: {
  metricsFile: string | undefined;
  chain: { id: number; name: string };
  stats: PortalStats;
  chunkBlocks: number;
  portalHead: number | undefined;
  gate: Gate;
  startTime: number;
}): void {
  const {
    metricsFile,
    chain,
    stats,
    chunkBlocks,
    portalHead,
    gate,
    startTime,
  } = args;
  if (!metricsFile) return;
  try {
    writeFileSync(
      `${metricsFile}.${chain.id}`,
      JSON.stringify({
        chain: chain.name,
        chainId: chain.id,
        wallMs: startTime ? Date.now() - startTime : 0,
        chunkBlocks,
        portalFinalizedHead: portalHead ?? null,
        fetch: {
          dataChunks: stats.dataChunks,
          extends: stats.extends,
          discChunks: stats.discChunks,
          http: stats.http,
          bytes: stats.bytes,
          errors: stats.errors,
          retries: stats.retries,
          cacheHits: stats.cacheHits,
          maxInflight: stats.maxInflight,
        },
        // saturation breakdown (cumulative ms across all requests of this chain): gate-wait = time blocked
        // on the global concurrency budget; fetch = Portal I/O (POST+stream drain); transform = NDJSON→
        // Sync* decode. DB-write time lives in Ponder (per-range log timing), not here.
        timing: {
          gateWaitMs: Math.round(stats.gateWaitMs),
          fetchMs: Math.round(stats.fetchMs),
          transformMs: Math.round(stats.transformMs),
        },
        portalGate: gate.snapshot(),
        inserted: {
          logs: stats.logs,
          blocks: stats.blocks,
          txs: stats.txs,
          receipts: stats.receipts,
          traces: stats.traces,
        },
        rpcFallbackIntervals: stats.rpcFallback,
      }),
    );
  } catch {
    /* best-effort */
  }
}

// ── opt-in gate observability ─────────────────────────────────────────────────────────────────────
// One ticker per process (the gate is process-shared). Starting it is idempotent so multiple per-chain
// syncs don't each spawn one.
let ticker: ReturnType<typeof setInterval> | undefined;

/** Start the PORTAL_GATE_LOG ticker once (no-op if already running or `enabled` is false). */
export function startGateLog(
  gate: Gate,
  enabled: boolean,
  intervalMs = 20_000,
): void {
  if (!enabled || ticker !== undefined) return;
  ticker = setInterval(() => {
    const s = gate.snapshot();
    console.log(
      `[portalGate] concurrency_limit=${s.limit} active=${s.active} buffered_rows=${s.rows}`,
    );
  }, intervalMs);
  ticker.unref();
}

/** Stop the ticker (test-only / graceful shutdown). */
export function stopGateLog(): void {
  if (ticker !== undefined) {
    clearInterval(ticker);
    ticker = undefined;
  }
}

const formatMb = (bytes: number): string => (bytes / (1024 * 1024)).toFixed(2);

const formatRate = (blocks: number, elapsedMs: number): string => {
  if (elapsedMs <= 0) return '0.0';

  return (blocks / (elapsedMs / 1000)).toFixed(1);
};

const discoveryScannedBlocks = (d: PortalProgressDiscovery): number => {
  if (d.floor < 0 || d.through < d.floor) return 0;

  return d.through - d.floor + 1;
};

const progressFingerprint = (
  stats: PortalStats,
  discovery: PortalProgressDiscovery,
): string =>
  [
    stats.blocks,
    stats.bytes,
    stats.dataChunks,
    stats.extends,
    stats.discChunks,
    stats.logs,
    stats.txs,
    stats.receipts,
    stats.traces,
    stats.rpcFallback,
    discoveryScannedBlocks(discovery),
  ].join(':');

export function startProgressLog(args: {
  chainName: string;
  stats: PortalStats;
  intervalMs: number;
  startTime: () => number;
  discovery: () => PortalProgressDiscovery;
  logInfo: PortalInfoLog;
  now?: () => number;
}): () => void {
  const {
    chainName,
    stats,
    intervalMs,
    startTime,
    discovery,
    logInfo,
    now = Date.now,
  } = args;
  if (intervalMs === 0) return () => {};

  const createdAt = now();
  let last = progressFingerprint(stats, discovery());
  const t = setInterval(() => {
    try {
      const d = discovery();
      const next = progressFingerprint(stats, d);
      if (next === last) return;

      last = next;
      const startedAt = startTime() || createdAt;
      const elapsedMs = Math.max(0, now() - startedAt);
      logInfo({
        service: 'portal',
        msg: `Portal ${chainName} progress: blocks_streamed=${stats.blocks} mb_streamed=${formatMb(stats.bytes)} blocks_per_s=${formatRate(stats.blocks, elapsedMs)} discChunks=${stats.discChunks} scanned=${discoveryScannedBlocks(d)}`,
      });
    } catch {
      /* progress logging is best-effort */
    }
  }, intervalMs);
  t.unref?.();

  return () => clearInterval(t);
}

export function createCompletionSummary(args: {
  chainName: string;
  stats: PortalStats;
  startTime: () => number;
  logInfo: PortalInfoLog;
  now?: () => number;
}): () => boolean {
  const { chainName, stats, startTime, logInfo, now = Date.now } = args;
  let done = false;

  return () => {
    if (done) return false;

    done = true;
    try {
      const startedAt = startTime() || now();
      const elapsedMs = Math.max(0, now() - startedAt);
      // Data-plane provenance, driven by the rpcFallback counter (portal.ts: stats.rpcFallback++ runs
      // once per historical block range the Portal path hands off to the RPC HistoricalSync). So
      // rpcFallback === 0 ⟺ every historical range was Portal-served with zero JSON-RPC fallback.
      // This speaks ONLY to the historical sync data plane (blocks/logs/txs/receipts); user-land
      // readContract state reads still hit the RPC and are out of scope for this line.
      const provenance =
        stats.rpcFallback === 0
          ? 'served entirely by the SQD Portal (0 JSON-RPC for history)'
          : `${stats.rpcFallback} block range(s) fell back to JSON-RPC`;
      logInfo({
        service: 'portal',
        msg: `Portal ${chainName} complete: blocks=${stats.blocks} logs=${stats.logs} txs=${stats.txs} receipts=${stats.receipts} mb_streamed=${formatMb(stats.bytes)} elapsed=${(elapsedMs / 1000).toFixed(1)}s avg_blocks_per_s=${formatRate(stats.blocks, elapsedMs)} rpc_fallback=${stats.rpcFallback} — ${provenance}`,
      });
    } catch {
      /* completion logging is best-effort */
    }

    return true;
  };
}
