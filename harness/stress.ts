/**
 * Portal stress test — ramping multi-chain Euler load against a dedicated Portal,
 * with a live observability dashboard to see WHAT and WHEN starts breaking.
 *
 * Workload: real Euler union query (factory discovery + EVC status + vault events)
 * per chain, tiled into windows over [deploy, head], processed by a worker pool
 * whose concurrency RAMPS over time. We push until errors (503/529/429) appear or
 * latency degrades, recording a per-second time-series.
 *
 * Env: PORTAL_API_KEY, PORTAL_BASE (default dedicated), CHAINS (default eth,base,arb),
 *      DURATION_MIN (30), WINDOW (1_000_000), RAMP_BASE/RAMP_STEP/RAMP_EVERY_S/RAMP_MAX,
 *      DASH_PORT (8080).
 */

import { appendFileSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import { PortalMetrics } from '../packages/portal-sync/src/metrics.ts';
import { PortalClient } from '../packages/portal-sync/src/portal-client.ts';
import {
  buildPortalQuery,
  type LogFilter,
} from '../packages/portal-sync/src/query.ts';
import { DASHBOARD_HTML } from './dashboard.ts';
import { extractChild, loadEulerChain } from './euler/load-sources.ts';

const API_KEY = process.env.PORTAL_API_KEY ?? '';
const BASE = process.env.PORTAL_BASE ?? 'https://portal.sqd.dev/datasets';
const CHAINS = (process.env.CHAINS ?? '1,8453,42161').split(',').map(Number);
const DURATION_MS = Number(process.env.DURATION_MIN ?? 30) * 60_000;
const WINDOW = Number(process.env.WINDOW ?? 1_000_000);
const EULER_TS = Number(process.env.EULER_TS ?? 1_722_470_400); // 2024-08-01
const DISCOVER_WINDOW = Number(process.env.DISCOVER_WINDOW ?? 1_500_000);
const RAMP_BASE = Number(process.env.RAMP_BASE ?? 6);
const RAMP_STEP = Number(process.env.RAMP_STEP ?? 6);
const RAMP_EVERY_S = Number(process.env.RAMP_EVERY_S ?? 75);
const RAMP_MAX = Number(process.env.RAMP_MAX ?? 240);
const DASH_PORT = Number(process.env.DASH_PORT ?? 8080);
const JSONL = '/tmp/stress-metrics.jsonl';

const metrics = new PortalMetrics({ assumedChunkBlocks: 50_000 });
const blockAtTs = async (slug: string, ts: number): Promise<number> => {
  const r = await fetch(`${BASE}/${slug}/timestamps/${ts}/block`, {
    headers: { 'x-api-key': API_KEY },
  });
  return r.ok ? ((await r.json()).block_number ?? 0) : 0;
};

type ChainState = {
  chainId: number;
  name: string;
  dataset: string;
  client: PortalClient;
  logRequests: LogFilter[];
  deploy: number;
  head: number;
  blocksScanned: number;
  windowsDone: number;
  vaults: number;
};
const states = new Map<number, ChainState>();
type Task = { chainId: number; from: number; to: number };
const queue: Task[] = [];

// ---------------- setup: discover children + build union filters + tile windows ----------------
async function setupChain(chainId: number) {
  const c = loadEulerChain(chainId);
  if (!c.dataset) {
    console.log(`skip ${chainId}: no dataset`);
    return;
  }
  const client = new PortalClient({
    baseUrl: BASE,
    dataset: c.dataset,
    apiKey: API_KEY,
    metrics,
    requestTimeoutMs: 120_000,
  });
  const head = (await client.getFinalizedHead())!.number;
  const deploy =
    chainId === 143 ? 30_858_573 : await blockAtTs(c.dataset, EULER_TS);

  // quick bounded discovery to seed a realistic child set
  const children = new Map<string, Set<string>>();
  for (const f of c.factories) children.set(f.name, new Set());
  const discFilters: LogFilter[] = c.factories.map((f) => ({
    address: [f.factory],
    topic0: [f.discoveryTopic0],
  }));
  const discTo = Math.min(deploy + DISCOVER_WINDOW, head);
  for await (const batch of client.streamFinalized(
    buildPortalQuery([deploy, discTo], discFilters),
  )) {
    for (const b of batch.blocks)
      for (const log of b.logs ?? []) {
        const f = c.factories.find(
          (x) =>
            x.factory === (log.address as string).toLowerCase() &&
            log.topics?.[0] === x.discoveryTopic0,
        );
        if (f) {
          const ch = extractChild(f.childRule, log);
          if (ch) children.get(f.name)!.add(ch.toLowerCase());
        }
      }
  }
  const totalVaults = [...children.values()].reduce((a, s) => a + s.size, 0);

  // union Euler filter: factory discovery + EVC singletons + vault events on children
  const logRequests: LogFilter[] = [];
  for (const f of c.factories) {
    logRequests.push({ address: [f.factory], topic0: [f.discoveryTopic0] });
    const addrs = [...children.get(f.name)!];
    if (addrs.length && f.childTopic0s.length)
      logRequests.push({ address: addrs, topic0: f.childTopic0s });
  }
  for (const s of c.singletons)
    if (s.topic0s.length)
      logRequests.push({ address: [s.address], topic0: s.topic0s });

  states.set(chainId, {
    chainId,
    name: c.eulerName ?? String(chainId),
    dataset: c.dataset,
    client,
    logRequests,
    deploy,
    head,
    blocksScanned: 0,
    windowsDone: 0,
    vaults: totalVaults,
  });

  for (let from = deploy; from < head; from += WINDOW)
    queue.push({ chainId, from, to: Math.min(from + WINDOW - 1, head) });
  console.log(
    `[setup] ${c.eulerName ?? chainId} (${c.dataset}): deploy=${deploy} head=${head} → ${Math.ceil((head - deploy) / WINDOW)} windows, ${totalVaults} vaults seeded`,
  );
}

async function setup() {
  await Promise.all(
    CHAINS.map((id) =>
      setupChain(id).catch((e) =>
        console.log(`setup ${id} failed: ${e.message}`),
      ),
    ),
  );
  console.log(
    `[setup] total ${queue.length} window tasks across ${states.size} chains`,
  );
}

// ---------------- worker pool with ramping concurrency ----------------
let running = true;
let launched = 0;
let taskIdx = 0;
const startedAt = Date.now();
const targetConcurrency = () => {
  const steps = Math.floor((Date.now() - startedAt) / 1000 / RAMP_EVERY_S);
  return Math.min(RAMP_MAX, RAMP_BASE + steps * RAMP_STEP);
};

async function worker() {
  while (running) {
    if (queue.length === 0) {
      await new Promise((r) => setTimeout(r, 500));
      continue;
    }
    const task = queue[taskIdx++ % queue.length]!; // round-robin, loop to sustain load
    const st = states.get(task.chainId)!;
    try {
      const q = buildPortalQuery([task.from, task.to], st.logRequests, {
        receipts: false,
      });
      for await (const batch of st.client.streamFinalized(q)) {
        st.blocksScanned += batch.toBlock - batch.fromBlock + 1;
        if (!running) break;
      }
      st.windowsDone++;
    } catch (e) {
      // errors are recorded in metrics (status counts/retries); swallow to keep pushing
    }
  }
}

// ---------------- time-series sampler ----------------
type Point = {
  t: number;
  concurrency: number;
  blocksPerSec: number;
  reqPerSec: number;
  mbPerSec: number;
  errPerSec: number;
  okPerSec: number;
  p50: number;
  p90: number;
  p99: number;
  cumErrors: number;
  cumBlocks: number;
  cumReq: number;
  cumMb: number;
  status: Record<string, number>;
  chains: Record<
    string,
    {
      blocksPerSec: number;
      reqPerSec: number;
      errPerSec: number;
      vaults: number;
    }
  >;
};
const series: Point[] = [];
let breakingAt: number | undefined;
let prev = {
  blocks: 0,
  req: 0,
  bytes: 0,
  err: 0,
  ok: 0,
  t: Date.now(),
  perChain: new Map<number, { blocks: number; req: number; err: number }>(),
};

function sample() {
  const now = Date.now();
  const dt = (now - prev.t) / 1000 || 1;
  const snap = metrics.snapshot(now);
  let req = 0;
  let bytes = 0;
  let err = 0;
  let ok = 0;
  const statusAgg: Record<string, number> = {};
  for (const d of snap.perDataset) {
    req += d.httpRequests;
    bytes += Math.round(d.mib * 1024 * 1024);
    for (const [k, v] of Object.entries(d.status))
      statusAgg[k] = (statusAgg[k] ?? 0) + v;
  }
  err =
    (statusAgg['503'] ?? 0) +
    (statusAgg['529'] ?? 0) +
    (statusAgg['429'] ?? 0) +
    (statusAgg['500'] ?? 0);
  ok = statusAgg['200'] ?? 0;
  const blocks = [...states.values()].reduce((a, s) => a + s.blocksScanned, 0);

  const chains: Point['chains'] = {};
  for (const s of states.values()) {
    const p = prev.perChain.get(s.chainId) ?? { blocks: 0, req: 0, err: 0 };
    const dPerDs = snap.perDataset.find((x) => x.dataset === s.dataset);
    const cReq = dPerDs?.httpRequests ?? 0;
    const cErr = dPerDs
      ? dPerDs.status['503'] +
        dPerDs.status['529'] +
        dPerDs.status['429'] +
        dPerDs.status['500']
      : 0;
    chains[s.name] = {
      blocksPerSec: Math.round((s.blocksScanned - p.blocks) / dt),
      reqPerSec: +((cReq - p.req) / dt).toFixed(1),
      errPerSec: +((cErr - p.err) / dt).toFixed(2),
      vaults: s.vaults,
    };
    prev.perChain.set(s.chainId, {
      blocks: s.blocksScanned,
      req: cReq,
      err: cErr,
    });
  }

  const errPerSec = +((err - prev.err) / dt).toFixed(2);
  const pt: Point = {
    t: Math.round((now - startedAt) / 1000),
    concurrency: launched,
    blocksPerSec: Math.round((blocks - prev.blocks) / dt),
    reqPerSec: +((req - prev.req) / dt).toFixed(1),
    mbPerSec: +((bytes - prev.bytes) / dt / 1024 / 1024).toFixed(2),
    errPerSec,
    okPerSec: +((ok - prev.ok) / dt).toFixed(1),
    p50: metrics.pct(50),
    p90: metrics.pct(90),
    p99: metrics.pct(99),
    cumErrors: err,
    cumBlocks: blocks,
    cumReq: req,
    cumMb: +(bytes / 1024 / 1024).toFixed(0),
    status: statusAgg,
    chains,
  };
  series.push(pt);
  appendFileSync(JSONL, JSON.stringify(pt) + '\n');
  if (breakingAt === undefined && errPerSec > 0.5 && pt.t > 5) {
    breakingAt = pt.t;
    console.log(
      `\n⚠️  BREAKING at T+${pt.t}s: errPerSec=${errPerSec} concurrency=${launched} status=${JSON.stringify(statusAgg)}\n`,
    );
  }
  prev = { blocks, req, bytes, err, ok, t: now, perChain: prev.perChain };
  const c = pt.chains;
  process.stdout.write(
    `T+${String(pt.t).padStart(4)}s conc=${String(launched).padStart(3)} | ${String(pt.blocksPerSec).padStart(8)} blk/s | ${String(pt.reqPerSec).padStart(5)} req/s | ${String(pt.mbPerSec).padStart(6)} MB/s | p90=${String(pt.p90).padStart(5)}ms | err/s=${pt.errPerSec} | ${Object.entries(
      c,
    )
      .map(([n, v]) => `${n}:${v.blocksPerSec}`)
      .join(' ')}\n`,
  );
}

// ---------------- dashboard server ----------------
function serve() {
  http
    .createServer((req, res) => {
      if (req.url === '/data') {
        res.writeHead(200, {
          'content-type': 'application/json',
          'access-control-allow-origin': '*',
        });
        res.end(
          JSON.stringify({
            series,
            breakingAt,
            durationMs: DURATION_MS,
            rampMax: RAMP_MAX,
            chains: [...states.values()].map((s) => ({
              name: s.name,
              dataset: s.dataset,
              vaults: s.vaults,
              windows: Math.ceil((s.head - s.deploy) / WINDOW),
              windowsDone: s.windowsDone,
            })),
          }),
        );
      } else {
        res.writeHead(200, { 'content-type': 'text/html' });
        res.end(DASHBOARD_HTML);
      }
    })
    .listen(DASH_PORT, () =>
      console.log(`\n📊 Dashboard: http://localhost:${DASH_PORT}\n`),
    );
}

// ---------------- run ----------------
writeFileSync(JSONL, '');
serve(); // dashboard is up IMMEDIATELY (shows "setting up" until data flows)
console.log(
  `[run] discovering children (DISCOVER_WINDOW=${DISCOVER_WINDOW})… load starts per-chain as each is ready`,
);
setup().then(() =>
  console.log(
    `[setup] all ${states.size} chains ready, ${queue.length} windows queued`,
  ),
);
const ramp = setInterval(() => {
  const t = targetConcurrency();
  while (launched < t && running) {
    launched++;
    worker();
  }
}, 1000);
const sampler = setInterval(sample, 3000);
setTimeout(() => {
  running = false;
  clearInterval(ramp);
  clearInterval(sampler);
  sample();
  const peak = series.reduce(
    (a, p) => (p.blocksPerSec > a.blocksPerSec ? p : a),
    series[0]!,
  );
  console.log(`\n===== STRESS SUMMARY =====`);
  console.log(
    `peak throughput: ${peak.blocksPerSec.toLocaleString()} blk/s @ conc=${peak.concurrency} (T+${peak.t}s), ${peak.mbPerSec} MB/s, ${peak.reqPerSec} req/s`,
  );
  console.log(
    `breaking point: ${breakingAt ? `T+${breakingAt}s` : 'none (no sustained errors)'}`,
  );
  console.log(
    `total: ${(prev.blocks / 1e6).toFixed(1)}M blocks scanned, ${prev.req.toLocaleString()} requests, ${(prev.bytes / 1e9).toFixed(1)} GB, ${prev.err} errors`,
  );
  console.log(
    `time-series: ${JSONL} (${series.length} points) | dashboard stays up — Ctrl-C to exit`,
  );
}, DURATION_MS);
