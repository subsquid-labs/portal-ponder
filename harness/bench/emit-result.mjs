#!/usr/bin/env node
// emit-result.mjs — scrape a running ponder app's /metrics, summarize the flagship bench run, and write
// the machine-readable result JSON. Thin CLI around the pure summarizeMetrics() (metrics-parse.mjs);
// the run driver (run-flagship.sh) calls this once completion is detected.
//
//   node emit-result.mjs --metrics-url http://127.0.0.1:42069/metrics \
//                        --chains ethereum,polygon,... \
//                        --unit-start-ms <epoch-ms the process started> \
//                        --ready-ms <epoch-ms /ready first went 200> \
//                        --out bench.result.json
//
// The result carries: allComplete, historical start/end (unix s) + derived wallSeconds, the separate
// unit-start→ready duration, per-chain completed/total blocks + complete flag, and rpc {requests,errors}
// (errors MUST be 0 for a clean run). Exit 0 always writes the file; exit code reflects whether the run
// looks CLEAN (complete + zero errors) so the driver can gate on it.

import { writeFileSync } from 'node:fs';
import { parseArgs } from './anchor-shim.mjs';
import { parsePrometheus, summarizeMetrics } from './metrics-parse.mjs';

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const metricsUrl = args['metrics-url'];
  if (!metricsUrl || metricsUrl === true) {
    console.error('emit-result: --metrics-url is required');
    process.exit(2);
  }

  const chains =
    typeof args.chains === 'string'
      ? args.chains
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean)
      : null;
  const outPath = typeof args.out === 'string' ? args.out : 'bench.result.json';
  const unitStartMs = args['unit-start-ms']
    ? Number(args['unit-start-ms'])
    : null;
  const readyMs = args['ready-ms'] ? Number(args['ready-ms']) : null;

  const res = await fetch(metricsUrl, { signal: AbortSignal.timeout(15_000) });
  if (!res.ok) {
    console.error(`emit-result: /metrics returned HTTP ${res.status}`);
    process.exit(2);
  }

  const body = await res.text();
  const summary = summarizeMetrics(parsePrometheus(body), chains);

  const startToReadySeconds =
    unitStartMs !== null && readyMs !== null
      ? Math.round((readyMs - unitStartMs) / 100) / 10
      : null;

  const clean = summary.allComplete && summary.rpc.errors === 0;
  const result = {
    generatedAt: new Date().toISOString(),
    clean,
    allComplete: summary.allComplete,
    historicalStartTs: summary.historicalStart,
    historicalEndTs: summary.historicalEnd,
    wallSeconds: summary.wallSeconds,
    startToReadySeconds,
    rpc: summary.rpc,
    chainCount: summary.perChain.length,
    perChain: summary.perChain,
  };

  writeFileSync(outPath, `${JSON.stringify(result, null, 2)}\n`);

  const wall = summary.wallSeconds;
  const mmss =
    wall !== null
      ? `${Math.floor(wall / 60)}m${String(wall % 60).padStart(2, '0')}s`
      : 'n/a';
  console.error(
    `emit-result: wrote ${outPath} — ${clean ? 'CLEAN' : 'NOT CLEAN'} ` +
      `wall=${mmss} chains=${summary.perChain.length} ` +
      `complete=${summary.perChain.filter((c) => c.complete).length} ` +
      `rpc.requests=${summary.rpc.requests} rpc.errors=${summary.rpc.errors}`,
  );

  process.exit(clean ? 0 : 1);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(`emit-result: ${e?.message ?? e}`);
    process.exit(2);
  });
}
