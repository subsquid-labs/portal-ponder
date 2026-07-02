/**
 * Portal-specific observability.
 *
 * Native Ponder metrics are RPC-bucket-centric (`ponder_rpc_request_duration`,
 * `ponder_rpc_request_error_total`) and cannot see the dimensions that decide
 * whether a Portal backfill is healthy: how many *streams* we open, how many
 * blocks each scan covers, the per-dataset worker pressure, and the CU we spend.
 * This module records exactly those.
 *
 * CU model (from cloned subsquid/sqd-portal + worker-rs):
 *   cost per worker query = active_blocks / chunk_blocks  (∈ (0,1])
 *   total CU for a scan   ≈ blocks_scanned / chunk_blocks  (≈ number of chunks)
 * chunk_blocks is data-density driven and not exposed, so `cuEstimate` is
 * reported against a configurable `assumedChunkBlocks` and clearly labelled.
 */

export type StatusKey =
  | "200"
  | "204"
  | "409"
  | "429"
  | "500"
  | "503"
  | "529"
  | "other";

export type DatasetMetrics = {
  dataset: string;
  /** High-level stream() calls — one per interval/union-query the fork issues. */
  logicalStreams: number;
  /** Actual HTTP POSTs, including mid-range continuation re-issues. */
  httpRequests: number;
  status: Record<StatusKey, number>;
  /** Sum of (toBlock-fromBlock+1) the fork *asked* to cover. */
  blocksRequested: number;
  /** Highest block the server actually advanced us to minus first requested. */
  forwardProgressBlocks: number;
  /** Block objects received (matching blocks + range boundaries). */
  blocksEmitted: number;
  logs: number;
  transactions: number;
  traces: number;
  /** Decoded NDJSON bytes (proxy for data volume). */
  bytes: number;
  streamMillis: number;
  retries: number;
  retryAfterWaitMillis: number;
  firstBlock: number | undefined;
  lastBlock: number | undefined;
};

const emptyStatus = (): Record<StatusKey, number> => ({
  "200": 0,
  "204": 0,
  "409": 0,
  "429": 0,
  "500": 0,
  "503": 0,
  "529": 0,
  other: 0,
});

const statusKey = (status: number): StatusKey => {
  const k = String(status) as StatusKey;
  return (
    ["200", "204", "409", "429", "500", "503", "529"] as StatusKey[]
  ).includes(k)
    ? k
    : "other";
};

export class PortalMetrics {
  datasets: Map<string, DatasetMetrics> = new Map();
  startedAt: number;
  /** CU estimate assumption; override per dataset density if known. */
  assumedChunkBlocks: number;
  /** rolling per-request latencies (ms) for live p50/p90/p99. */
  recentLat: number[] = [];

  pct(p: number): number {
    if (this.recentLat.length === 0) return 0;
    const a = [...this.recentLat].sort((x, y) => x - y);
    return a[Math.min(a.length - 1, Math.floor((p / 100) * a.length))]!;
  }

  constructor(opts?: { assumedChunkBlocks?: number; now?: number }) {
    this.assumedChunkBlocks = opts?.assumedChunkBlocks ?? 50_000;
    this.startedAt = opts?.now ?? Date.now();
  }

  private ds(dataset: string): DatasetMetrics {
    let d = this.datasets.get(dataset);
    if (d === undefined) {
      d = {
        dataset,
        logicalStreams: 0,
        httpRequests: 0,
        status: emptyStatus(),
        blocksRequested: 0,
        forwardProgressBlocks: 0,
        blocksEmitted: 0,
        logs: 0,
        transactions: 0,
        traces: 0,
        bytes: 0,
        streamMillis: 0,
        retries: 0,
        retryAfterWaitMillis: 0,
        firstBlock: undefined,
        lastBlock: undefined,
      };
      this.datasets.set(dataset, d);
    }
    return d;
  }

  onLogicalStream(dataset: string, fromBlock: number, toBlock: number): void {
    const d = this.ds(dataset);
    d.logicalStreams++;
    d.blocksRequested += Math.max(0, toBlock - fromBlock + 1);
    if (d.firstBlock === undefined || fromBlock < d.firstBlock)
      d.firstBlock = fromBlock;
  }

  onHttpResponse(
    dataset: string,
    r: {
      status: number;
      bytes: number;
      durationMs: number;
      blocks?: number;
      logs?: number;
      transactions?: number;
      traces?: number;
      lastBlock?: number;
    },
  ): void {
    const d = this.ds(dataset);
    d.httpRequests++;
    d.status[statusKey(r.status)]++;
    d.bytes += r.bytes;
    d.streamMillis += r.durationMs;
    this.recentLat.push(r.durationMs);
    if (this.recentLat.length > 3000) this.recentLat.shift();
    d.blocksEmitted += r.blocks ?? 0;
    d.logs += r.logs ?? 0;
    d.transactions += r.transactions ?? 0;
    d.traces += r.traces ?? 0;
    if (r.lastBlock !== undefined) {
      if (d.lastBlock === undefined || r.lastBlock > d.lastBlock)
        d.lastBlock = r.lastBlock;
      if (d.firstBlock !== undefined) {
        d.forwardProgressBlocks = Math.max(
          d.forwardProgressBlocks,
          r.lastBlock - d.firstBlock + 1,
        );
      }
    }
  }

  onRetry(dataset: string, waitMs: number): void {
    const d = this.ds(dataset);
    d.retries++;
    d.retryAfterWaitMillis += waitMs;
  }

  /** Derived, human-facing snapshot. */
  snapshot(now = Date.now()) {
    const wallMs = now - this.startedAt;
    const perDataset = [...this.datasets.values()].map((d) => {
      const clientFacingErrors =
        d.status["503"] + d.status["529"] + d.status["500"];
      const httpPerLogicalStream = d.logicalStreams
        ? d.httpRequests / d.logicalStreams
        : 0;
      const blocks = d.forwardProgressBlocks || d.blocksRequested;
      return {
        dataset: d.dataset,
        logicalStreams: d.logicalStreams,
        httpRequests: d.httpRequests,
        httpPerLogicalStream: round(httpPerLogicalStream, 2),
        // comparability with the proxy's "portal_streams_per_window" (1000-block windows)
        streamsPer1000Blocks: blocks
          ? round((d.httpRequests / blocks) * 1000, 3)
          : 0,
        forwardProgressBlocks: d.forwardProgressBlocks,
        blocksPerSec: d.streamMillis
          ? Math.round((d.forwardProgressBlocks / d.streamMillis) * 1000)
          : 0,
        blocksEmitted: d.blocksEmitted,
        logs: d.logs,
        transactions: d.transactions,
        traces: d.traces,
        mib: round(d.bytes / 1024 / 1024, 2),
        status: d.status,
        clientFacingErrors,
        retries: d.retries,
        retryAfterWaitMs: d.retryAfterWaitMillis,
        cuEstimate: round(blocks / this.assumedChunkBlocks, 1),
        range:
          d.firstBlock !== undefined ? [d.firstBlock, d.lastBlock] : undefined,
      };
    });
    const totals = perDataset.reduce(
      (a, p) => ({
        logicalStreams: a.logicalStreams + p.logicalStreams,
        httpRequests: a.httpRequests + p.httpRequests,
        forwardProgressBlocks:
          a.forwardProgressBlocks + p.forwardProgressBlocks,
        logs: a.logs + p.logs,
        traces: a.traces + p.traces,
        mib: round(a.mib + p.mib, 2),
        clientFacingErrors: a.clientFacingErrors + p.clientFacingErrors,
        cuEstimate: round(a.cuEstimate + p.cuEstimate, 1),
      }),
      {
        logicalStreams: 0,
        httpRequests: 0,
        forwardProgressBlocks: 0,
        logs: 0,
        traces: 0,
        mib: 0,
        clientFacingErrors: 0,
        cuEstimate: 0,
      },
    );
    return {
      wallSeconds: round(wallMs / 1000, 1),
      assumedChunkBlocks: this.assumedChunkBlocks,
      totals,
      perDataset,
    };
  }

  /** Prometheus exposition text — drop-in alongside Ponder's /metrics. */
  prometheus(): string {
    const lines: string[] = [];
    const g = (name: string, help: string, type: string) => {
      lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`);
    };
    g(
      "portal_logical_streams_total",
      "High-level Portal stream() calls (one per fork interval-query)",
      "counter",
    );
    g(
      "portal_http_requests_total",
      "Actual Portal HTTP POSTs incl. continuations, by status",
      "counter",
    );
    g(
      "portal_blocks_scanned_total",
      "Forward-progress blocks scanned per dataset",
      "counter",
    );
    g("portal_logs_total", "Logs returned by Portal per dataset", "counter");
    g("portal_bytes_total", "Decoded NDJSON bytes per dataset", "counter");
    g(
      "portal_client_facing_errors_total",
      "503/529/500 surfaced to the client",
      "counter",
    );
    g(
      "portal_cu_estimate",
      "Estimated compute units (blocks/assumedChunkBlocks)",
      "gauge",
    );
    for (const d of this.datasets.values()) {
      const l = `dataset="${d.dataset}"`;
      lines.push(`portal_logical_streams_total{${l}} ${d.logicalStreams}`);
      for (const [s, n] of Object.entries(d.status)) {
        lines.push(`portal_http_requests_total{${l},status="${s}"} ${n}`);
      }
      lines.push(
        `portal_blocks_scanned_total{${l}} ${d.forwardProgressBlocks}`,
      );
      lines.push(`portal_logs_total{${l}} ${d.logs}`);
      lines.push(`portal_bytes_total{${l}} ${d.bytes}`);
      lines.push(
        `portal_client_facing_errors_total{${l}} ${d.status["503"] + d.status["529"] + d.status["500"]}`,
      );
      lines.push(
        `portal_cu_estimate{${l}} ${round((d.forwardProgressBlocks || d.blocksRequested) / this.assumedChunkBlocks, 2)}`,
      );
    }
    return lines.join("\n") + "\n";
  }
}

const round = (n: number, dp: number): number => {
  const f = 10 ** dp;
  return Math.round(n * f) / f;
};
