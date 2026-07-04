/**
 * Multi-chain concurrency demo.
 *
 * Runs Euler factory discovery across many chains CONCURRENTLY — the dimension
 * that broke the proxy path (10 instances × 8 chains × 256 concurrency ≈ 20k
 * in-flight, saturating Portal's shared worker pool). Here each chain is one
 * self-paced stream over a DISJOINT dataset/worker-set ("spread, don't stack"),
 * so N chains = N streams, not 20k requests. Shared metrics; aggregate gate.
 *
 * Env: CHAINS (csv, default "1,8453,43114,42161,143"), EULER_TS, WINDOW.
 */

import { PortalMetrics } from '../packages/portal-sync/src/metrics.ts';
import { PortalClient } from '../packages/portal-sync/src/portal-client.ts';
import {
  buildPortalQuery,
  type LogFilter,
} from '../packages/portal-sync/src/query.ts';
import { extractChild, loadEulerChain } from './euler/load-sources.ts';

const CHAINS = (process.env.CHAINS ?? '1,8453,43114,42161,143')
  .split(',')
  .map(Number);
const EULER_TS = Number(process.env.EULER_TS ?? 1_722_470_400); // 2024-08-01
const WINDOW = Number(process.env.WINDOW ?? 3_000_000);

const metrics = new PortalMetrics();

const blockAtTs = async (slug: string, ts: number): Promise<number> => {
  const r = await fetch(
    `https://portal.sqd.dev/datasets/${slug}/timestamps/${ts}/block`,
  );
  if (!r.ok) return 0;
  return (await r.json()).block_number ?? 0;
};

const runChain = async (chainId: number) => {
  const chain = loadEulerChain(chainId);
  if (!chain.dataset) return { chainId, skipped: 'no Portal dataset' };
  const client = new PortalClient({ dataset: chain.dataset, metrics });
  const head = await client.getFinalizedHead();
  if (!head) return { chainId, skipped: 'no finalized head' };

  const start =
    chainId === 143 ? 30_858_573 : await blockAtTs(chain.dataset, EULER_TS);
  const to = Math.min(start + WINDOW, head.number);

  const discoveryFilters: LogFilter[] = chain.factories.map((f) => ({
    address: [f.factory],
    topic0: [f.discoveryTopic0],
  }));
  const vaults = new Set<string>();
  const t0 = Date.now();
  for await (const batch of client.streamFinalized(
    buildPortalQuery([start, to], discoveryFilters),
  )) {
    for (const b of batch.blocks)
      for (const log of b.logs ?? []) {
        const f = chain.factories.find(
          (x) =>
            x.factory === (log.address as string).toLowerCase() &&
            log.topics?.[0] === x.discoveryTopic0,
        );
        if (f) {
          const c = extractChild(f.childRule, log);
          if (c) vaults.add(c.toLowerCase());
        }
      }
  }
  const res = {
    chainId,
    dataset: chain.dataset,
    eulerName: chain.eulerName,
    range: [start, to],
    blocks: to - start,
    vaults: vaults.size,
    seconds: +((Date.now() - t0) / 1000).toFixed(1),
  };
  console.log(
    `  ✓ ${String(chain.eulerName ?? chainId).padEnd(11)} ${chain.dataset.padEnd(20)} ${(to - start).toLocaleString().padStart(11)} blk → ${String(vaults.size).padStart(4)} vaults  (${res.seconds}s)`,
  );
  return res;
};

console.log(
  `\n================  MULTI-CHAIN EULER DISCOVERY (concurrent)  ================`,
);
console.log(
  `chains=${CHAINS.join(',')} | window=${WINDOW.toLocaleString()} blocks from ${new Date(EULER_TS * 1000).toISOString().slice(0, 10)}\n`,
);

const t0 = Date.now();
const results = await Promise.all(CHAINS.map(runChain)); // ALL chains concurrently
const wall = ((Date.now() - t0) / 1000).toFixed(1);

console.log(`=== PER-CHAIN ===`);
for (const r of results as any[]) {
  if (r.skipped) {
    console.log(`  chain ${r.chainId}: skipped (${r.skipped})`);
    continue;
  }
  console.log(
    `  ${String(r.eulerName ?? r.chainId).padEnd(11)} ${r.dataset.padEnd(20)} ${r.blocks.toLocaleString().padStart(12)} blk → ${String(r.vaults).padStart(4)} vaults  (${r.seconds}s)`,
  );
}

const snap = metrics.snapshot();
console.log(`\n=== AGGREGATE PORTAL METRICS (all chains, concurrent) ===`);
console.log(
  `  wall: ${wall}s | concurrent logical streams: ${snap.totals.logicalStreams} | total HTTP: ${snap.totals.httpRequests}`,
);
console.log(
  `  blocks scanned: ${snap.totals.forwardProgressBlocks.toLocaleString()} | vaults: ${(results as any[]).reduce((a, r) => a + (r.vaults ?? 0), 0)}`,
);
console.log(
  `  client-facing 503/529: ${snap.totals.clientFacingErrors} | est CU: ${snap.totals.cuEstimate}`,
);
console.log(
  `\n  vs proxy path on the same multi-chain shape: ~20,000 requests in flight → worker-pool exhaustion`,
);

const errs = snap.totals.clientFacingErrors;
console.log(
  `\n=== GATE === ${errs === 0 ? '✅ PASS' : '❌ FAIL'} (client-facing 503/529 = ${errs}, expect 0)`,
);

const fs = await import('node:fs');
fs.writeFileSync(
  '/Users/dz/Projects/portal-ponder/harness/results.multichain.json',
  JSON.stringify(
    { chains: results, aggregate: snap, wallSeconds: +wall },
    null,
    2,
  ),
);
process.exit(errs === 0 ? 0 : 1);
