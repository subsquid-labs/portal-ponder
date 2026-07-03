/**
 * Realtime source helpers + endpoint instrumentation.
 *
 * Ponder owns realtime and reorg handling natively (RealtimeSync: parent-hash tracking + rollback to the
 * common ancestor, bounded by the finalized block). The fork only swaps the HISTORICAL sync to the Portal.
 * So the realtime source is purely a `rpc` config choice — ponder polls whatever transport(s) you give it
 * and reorg-handles the result identically. The lib is RPC-agnostic: an authenticated RPC (an x-api-key
 * header, a keyed proxy, …) is just `http(url, { fetchOptions: { headers } })` — nothing special.
 *
 *   import { http, fallback } from "viem";
 *   // a plain list (Ponder-style), or latency-ranked for fastest-tip:
 *   rpc: [process.env.RPC_1!, process.env.RPC_1B!],
 *   rpc: fallback([http(process.env.RPC_1!), http(process.env.RPC_1B!)], { rank: true }),
 *   // an authenticated RPC + fallback:
 *   rpc: fallback([ http(process.env.RPC_URL!, { fetchOptions: { headers: { "x-api-key": process.env.RPC_KEY! } } }), http(process.env.RPC_1!) ]),
 *
 * `fallback` gives smooth failover; `{ rank: true }` probes latency and routes to the fastest tip. Either
 * way ponder's reorg safety comes for free.
 */
import { fallback, http, type Transport } from 'viem';

/**
 * Compose realtime RPC(s) into a latency-ranked fallback so the fastest tip lands. Accepts URL strings or
 * viem Transports — pass a Transport for an authenticated RPC: `http(url, { fetchOptions: { headers } })`.
 */
export function rpcRealtime(
  rpcs: Array<string | Transport>,
  opts?: { rank?: boolean },
): Transport {
  return fallback(
    rpcs.map((r) => (typeof r === 'string' ? http(r) : r)),
    { rank: opts?.rank ?? true },
  );
}

// ─────────────────────────────── endpoint instrumentation ───────────────────────────────
// Periodically probe each configured endpoint (eth_blockNumber) to evaluate, per endpoint:
//   • latency   — request round-trip (ms), p50/p95/mean
//   • freshness — how many blocks behind the FRESHEST endpoint for that chain (tip lag)
//   • responsiveness — error rate + last error
// This is a side-probe (consistent, independent of ponder's request pattern), written to the metrics suite.

export type Endpoint = {
  name: string;
  url: string;
  headers?: Record<string, string>;
};
export type ChainEndpoints = { chainId: number; endpoints: Endpoint[] };
export type ProbeSample = {
  chainId: number;
  name: string;
  ok: boolean;
  latencyMs: number;
  tip: number | null;
  error?: string;
  lag?: number | null;
};

/** Probe every endpoint once. Pure w.r.t. state — takes a `fetchImpl` for testability. */
export async function probeOnce(
  chains: ChainEndpoints[],
  fetchImpl: typeof fetch = fetch,
  timeoutMs = 8_000,
): Promise<ProbeSample[]> {
  const out: ProbeSample[] = [];
  await Promise.all(
    chains.flatMap((c) =>
      c.endpoints.map(async (e) => {
        const t0 = Date.now();
        try {
          const res = await fetchImpl(e.url, {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              ...(e.headers ?? {}),
            },
            body: JSON.stringify({
              jsonrpc: '2.0',
              id: 1,
              method: 'eth_blockNumber',
              params: [],
            }),
            signal: AbortSignal.timeout(timeoutMs),
          });
          const latencyMs = Date.now() - t0;
          if (!res.ok) {
            out.push({
              chainId: c.chainId,
              name: e.name,
              ok: false,
              latencyMs,
              tip: null,
              error: `HTTP ${res.status}`,
            });
            return;
          }
          const j = await res.json();
          const tip = typeof j?.result === 'string' ? Number(j.result) : null;
          out.push({
            chainId: c.chainId,
            name: e.name,
            ok: tip !== null && Number.isFinite(tip),
            latencyMs,
            tip,
            error: j?.error ? JSON.stringify(j.error).slice(0, 80) : undefined,
          });
        } catch (err: any) {
          out.push({
            chainId: c.chainId,
            name: e.name,
            ok: false,
            latencyMs: Date.now() - t0,
            tip: null,
            error: String(err?.message ?? err).slice(0, 80),
          });
        }
      }),
    ),
  );
  return out;
}

/** Freshness lag (blocks behind the freshest endpoint for the same chain). Pure. */
export function freshnessLag(
  samples: ProbeSample[],
): Map<string, number | null> {
  const maxTip = new Map<number, number>();
  for (const s of samples)
    if (s.ok && s.tip !== null)
      maxTip.set(s.chainId, Math.max(maxTip.get(s.chainId) ?? 0, s.tip));
  const lag = new Map<string, number | null>();
  for (const s of samples) {
    const key = `${s.chainId}:${s.name}`;
    lag.set(
      key,
      s.ok && s.tip !== null && maxTip.has(s.chainId)
        ? maxTip.get(s.chainId)! - s.tip
        : null,
    );
  }
  return lag;
}

const pct = (arr: number[], p: number): number => {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] ?? 0;
};

/** Roll a window of samples into per-endpoint stats. Pure. */
export function summarize(window: ProbeSample[]): Record<
  string,
  {
    chainId: number;
    name: string;
    n: number;
    okRate: number;
    latP50: number;
    latP95: number;
    latMean: number;
    avgLag: number | null;
    lastError?: string;
  }
> {
  const byKey: Record<string, ProbeSample[]> = {};
  for (const s of window) {
    const key = `${s.chainId}:${s.name}`;
    let bucket = byKey[key];
    if (bucket === undefined) {
      bucket = [];
      byKey[key] = bucket;
    }
    bucket.push(s);
  }
  const res: Record<string, any> = {};
  for (const [key, ss] of Object.entries(byKey)) {
    const ok = ss.filter((s) => s.ok);
    const lats = ok.map((s) => s.latencyMs);
    const lags = ok
      .map((s) => s.lag)
      .filter((x): x is number => typeof x === 'number');
    res[key] = {
      chainId: ss[0]!.chainId,
      name: ss[0]!.name,
      n: ss.length,
      okRate: ss.length ? ok.length / ss.length : 0,
      latP50: pct(lats, 50),
      latP95: pct(lats, 95),
      latMean: lats.length
        ? Math.round(lats.reduce((a, b) => a + b, 0) / lats.length)
        : 0,
      avgLag: lags.length
        ? Math.round(
            lags.reduce((a: number, b: number) => a + b, 0) / lags.length,
          )
        : null,
      lastError: [...ss].reverse().find((s) => s.error)?.error,
    };
  }
  return res;
}

/** Start the periodic endpoint probe. Returns a stop() fn. Writes `<metricsFile>` (JSON) each tick. */
export function startEndpointProbe(
  chains: ChainEndpoints[],
  opts: {
    intervalMs?: number;
    windowTicks?: number;
    metricsFile?: string;
    fetchImpl?: typeof fetch;
    onSummary?: (s: ReturnType<typeof summarize>) => void;
    log?: (m: string) => void;
  } = {},
): () => void {
  const intervalMs = opts.intervalMs ?? 10_000;
  const windowTicks = opts.windowTicks ?? 30; // rolling window
  const fetchImpl = opts.fetchImpl ?? fetch;
  const ring: ProbeSample[] = [];
  let stopped = false;
  const tick = async () => {
    if (stopped) return;
    const samples = await probeOnce(chains, fetchImpl);
    const lag = freshnessLag(samples);
    for (const s of samples) s.lag = lag.get(`${s.chainId}:${s.name}`) ?? null;
    ring.push(...samples);
    while (
      ring.length >
      windowTicks * chains.reduce((n, c) => n + c.endpoints.length, 0)
    )
      ring.shift();
    const summary = summarize(ring);
    opts.onSummary?.(summary);
    if (opts.metricsFile) {
      try {
        (await import('node:fs')).writeFileSync(
          opts.metricsFile,
          JSON.stringify(
            { ts: Date.now(), intervalMs, endpoints: summary },
            null,
            2,
          ),
        );
      } catch {
        /* best-effort */
      }
    }
    if (opts.log)
      for (const v of Object.values(summary))
        opts.log(
          `[probe] ${v.chainId}:${v.name} lat p50=${v.latP50}ms p95=${v.latP95}ms ok=${(v.okRate * 100).toFixed(0)}% lag=${v.avgLag ?? '?'}blk`,
        );
  };
  const timer = setInterval(tick, intervalMs);
  void tick();
  return () => {
    stopped = true;
    clearInterval(timer);
  };
}
