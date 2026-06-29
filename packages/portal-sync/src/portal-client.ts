/**
 * Bounded, self-pacing Portal stream client.
 *
 * This is the antithesis of the JSON-RPC-emulation path: it issues ONE logical
 * stream per query and walks the whole range via Portal's NDJSON continuation
 * protocol (re-issue from last_block+1), letting Portal's server-side sliding
 * buffer do look-ahead. It treats multi-second range-scan latency as NORMAL and
 * honors Retry-After on 503/529 cooperatively — so it never trips the kind of
 * latency/throttle penalty that strangles the proxy path. Concurrency across
 * datasets/intervals is bounded by the caller, per Portal's CU/worker economics
 * ("a small number of concurrent streams, each over a large contiguous range").
 */
import type { PortalEvmQuery, PortalBlock, BlockRef } from "./portal-types.ts";
import type { PortalMetrics } from "./metrics.ts";

export type PortalClientOptions = {
  baseUrl?: string;
  dataset: string;
  metrics?: PortalMetrics;
  requestTimeoutMs?: number;
  maxRetries?: number;
  /** hard cap on Retry-After we'll honor (ms) */
  maxRetryAfterMs?: number;
  /** dedicated-portal API key (sent as x-api-key) */
  apiKey?: string;
};

export type StreamBatch = {
  blocks: PortalBlock[];
  fromBlock: number;
  toBlock: number;
  logs: number;
  transactions: number;
  traces: number;
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export class PortalClient {
  baseUrl: string;
  dataset: string;
  metrics: PortalMetrics | undefined;
  requestTimeoutMs: number;
  maxRetries: number;
  maxRetryAfterMs: number;
  headers: Record<string, string>;

  constructor(opts: PortalClientOptions) {
    this.baseUrl = (opts.baseUrl ?? "https://portal.sqd.dev/datasets").replace(/\/$/, "");
    this.dataset = opts.dataset;
    this.metrics = opts.metrics;
    this.requestTimeoutMs = opts.requestTimeoutMs ?? 60_000;
    this.maxRetries = opts.maxRetries ?? 8;
    this.maxRetryAfterMs = opts.maxRetryAfterMs ?? 30_000;
    this.headers = { "accept-encoding": "gzip" };
    if (opts.apiKey) this.headers["x-api-key"] = opts.apiKey;
  }

  private url(path: string): string {
    return `${this.baseUrl}/${this.dataset}/${path}`;
  }

  async getHead(): Promise<BlockRef | undefined> {
    return this.getRef("head");
  }
  async getFinalizedHead(): Promise<BlockRef | undefined> {
    return this.getRef("finalized-head");
  }
  private async getRef(path: string): Promise<BlockRef | undefined> {
    const res = await fetch(this.url(path), { headers: this.headers });
    if (res.status === 204 || res.status === 404) return undefined;
    if (!res.ok) throw new Error(`Portal ${path} ${res.status}: ${await res.text()}`);
    return (await res.json()) as BlockRef;
  }
  async getMetadata(): Promise<any> {
    const res = await fetch(this.url("metadata"), { headers: { "accept-encoding": "gzip" } });
    if (!res.ok) throw new Error(`Portal metadata ${res.status}`);
    return res.json();
  }

  /**
   * Stream a finalized query over its full [fromBlock,toBlock] range, yielding
   * one batch per HTTP response. Finalized stream never 409s (no reorg handling
   * needed for backfill).
   */
  async *streamFinalized(query: PortalEvmQuery): AsyncGenerator<StreamBatch> {
    const target = query.toBlock;
    if (target === undefined) throw new Error("backfill stream requires toBlock");
    this.metrics?.onLogicalStream(this.dataset, query.fromBlock, target);

    let cursor = query.fromBlock;
    while (cursor <= target) {
      const body = JSON.stringify({ ...query, fromBlock: cursor });
      const batch = await this.postOnce(body, cursor, target);
      if (batch === "above-head") return; // 204
      yield batch;
      if (batch.toBlock < cursor) {
        // server made no progress — guard against an infinite loop
        throw new Error(`Portal made no forward progress at block ${cursor} (got ${batch.toBlock})`);
      }
      cursor = batch.toBlock + 1;
    }
  }

  /** One HTTP POST + NDJSON drain, with Retry-After-aware retries. */
  private async postOnce(body: string, fromBlock: number, target: number): Promise<StreamBatch | "above-head"> {
    let attempt = 0;
    while (true) {
      const started = Date.now();
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), this.requestTimeoutMs);
      try {
        const res = await fetch(this.url("finalized-stream"), {
          method: "POST",
          headers: { ...this.headers, "content-type": "application/json" },
          body,
          signal: ctrl.signal,
        });

        if (res.status === 204) {
          this.metrics?.onHttpResponse(this.dataset, { status: 204, bytes: 0, durationMs: Date.now() - started });
          return "above-head";
        }

        if (res.status === 503 || res.status === 529 || res.status === 429) {
          await res.body?.cancel().catch(() => {});
          const waitMs = this.retryAfterMs(res, attempt);
          this.metrics?.onHttpResponse(this.dataset, { status: res.status, bytes: 0, durationMs: Date.now() - started });
          if (attempt++ >= this.maxRetries) {
            throw new Error(`Portal overloaded (${res.status}) at block ${fromBlock} after ${attempt} attempts`);
          }
          this.metrics?.onRetry(this.dataset, waitMs);
          await sleep(waitMs);
          continue;
        }

        if (!res.ok) {
          const text = await res.text().catch(() => "");
          this.metrics?.onHttpResponse(this.dataset, { status: res.status, bytes: text.length, durationMs: Date.now() - started });
          throw new Error(`Portal stream ${res.status} at block ${fromBlock}: ${text.slice(0, 200)}`);
        }

        // 200 — drain NDJSON
        const drained = await this.drainNdjson(res, fromBlock);
        this.metrics?.onHttpResponse(this.dataset, {
          status: 200, bytes: drained.bytes, durationMs: Date.now() - started,
          blocks: drained.blocks.length, logs: drained.logs, transactions: drained.transactions,
          traces: drained.traces, lastBlock: drained.lastBlock,
        });
        return {
          blocks: drained.blocks,
          fromBlock,
          toBlock: Math.min(drained.lastBlock ?? target, target),
          logs: drained.logs, transactions: drained.transactions, traces: drained.traces,
        };
      } catch (err) {
        const aborted = (err as Error)?.name === "AbortError";
        if (aborted && attempt++ < this.maxRetries) {
          // a timeout is a transient, not a poison pill — back off and retry the cursor
          this.metrics?.onHttpResponse(this.dataset, { status: 500, bytes: 0, durationMs: Date.now() - started });
          const waitMs = Math.min(500 * 2 ** attempt, this.maxRetryAfterMs);
          this.metrics?.onRetry(this.dataset, waitMs);
          await sleep(waitMs);
          continue;
        }
        throw err;
      } finally {
        clearTimeout(timer);
      }
    }
  }

  private retryAfterMs(res: Response, attempt: number): number {
    const h = res.headers.get("retry-after");
    if (h) {
      const secs = Number(h);
      if (Number.isFinite(secs)) return Math.min(secs * 1000, this.maxRetryAfterMs);
    }
    return Math.min(500 * 2 ** attempt, this.maxRetryAfterMs);
  }

  private async drainNdjson(res: Response, fromBlock: number) {
    const blocks: PortalBlock[] = [];
    let bytes = 0, logs = 0, transactions = 0, traces = 0;
    let lastBlock: number | undefined;

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    const handleLine = (line: string) => {
      if (line.length === 0) return;
      const b = JSON.parse(line) as PortalBlock;
      blocks.push(b);
      const n = b.header?.number;
      if (typeof n === "number" && (lastBlock === undefined || n > lastBlock)) lastBlock = n;
      if (b.logs) logs += b.logs.length;
      if (b.transactions) transactions += b.transactions.length;
      if (b.traces) traces += b.traces.length;
    };

    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      buf += decoder.decode(value, { stream: true });
      let nl: number;
      while ((nl = buf.indexOf("\n")) >= 0) {
        handleLine(buf.slice(0, nl));
        buf = buf.slice(nl + 1);
      }
    }
    buf += decoder.decode();
    if (buf.length > 0) handleLine(buf);

    return { blocks, bytes, logs, transactions, traces, lastBlock };
  }
}
