/**
 * Full real-Euler backfill, the fork's way, against live Portal.
 *
 * Pass 1 (discovery): ONE union stream over all factory discovery events →
 *                     accumulate child addresses per factory.
 * Pass 2 (data):      ONE union stream over every child event + EVC singleton
 *                     events, transformed to Ponder SyncLog rows.
 * = TWO logical streams for the entire Euler backfill of a chain, vs the
 *   proxy path's 34,241 eth_getLogs (which died at 0% progress).
 *
 * Env: CHAIN (default 143/Monad), START, TO (default finalized head).
 */

import { writeFileSync } from 'node:fs';
import { PortalMetrics } from '../packages/portal-sync/src/metrics.ts';
import { PortalClient } from '../packages/portal-sync/src/portal-client.ts';
import {
  buildPortalQuery,
  type LogFilter,
} from '../packages/portal-sync/src/query.ts';
import { toSyncLog } from '../packages/portal-sync/src/transform.ts';
import { extractChild, loadEulerChain } from './euler/load-sources.ts';

const CHAIN = Number(process.env.CHAIN ?? 143);
const chain = loadEulerChain(CHAIN);
if (!chain.dataset) throw new Error(`chain ${CHAIN} has no Portal dataset`);

const metrics = new PortalMetrics();
const client = new PortalClient({ dataset: chain.dataset, metrics });
const head = await client.getFinalizedHead();
if (!head) throw new Error(`no finalized head for ${chain.dataset}`);

const START = Number(process.env.START ?? (CHAIN === 143 ? 30_858_573 : 0));
const TO = Number(process.env.TO ?? head.number);
console.log(
  `\n================  EULER BACKFILL — ${chain.eulerName ?? chain.chainId} (${chain.dataset})  ================`,
);
console.log(
  `range [${START.toLocaleString()}, ${TO.toLocaleString()}] = ${(TO - START).toLocaleString()} blocks | finalizedHead=${head.number}`,
);
console.log(
  `sources: ${chain.factories.map((f) => f.name).join(', ')}${chain.singletons.length ? ' + ' + chain.singletons.map((s) => s.name).join(',') : ''}`,
);

// ---------- PASS 1: discovery ----------
console.log(`\n[pass 1] discovery — one stream, all factory discovery events`);
const discoveryFilters: LogFilter[] = chain.factories.map((f) => ({
  address: [f.factory],
  topic0: [f.discoveryTopic0],
}));
const children = new Map<string, Set<string>>(); // factory name -> child addresses
for (const f of chain.factories) children.set(f.name, new Set());

const t1 = Date.now();
let p1http = 0;
for await (const batch of client.streamFinalized(
  buildPortalQuery([START, TO], discoveryFilters),
)) {
  p1http++;
  for (const b of batch.blocks) {
    for (const log of b.logs ?? []) {
      const f = chain.factories.find(
        (x) =>
          x.factory === (log.address as string).toLowerCase() &&
          log.topics?.[0] === x.discoveryTopic0,
      );
      if (!f) continue;
      const child = extractChild(f.childRule, log);
      if (child) children.get(f.name)!.add(child.toLowerCase());
    }
  }
}
const p1s = ((Date.now() - t1) / 1000).toFixed(1);
for (const f of chain.factories)
  console.log(
    `  ${f.name}: ${children.get(f.name)!.size} children (${p1http} http, ${p1s}s)`,
  );

// ---------- PASS 2: child + singleton data ----------
console.log(`\n[pass 2] data — one union stream over all child events + EVC`);
const dataFilters: LogFilter[] = [];
for (const f of chain.factories) {
  const addrs = [...children.get(f.name)!];
  if (addrs.length && f.childTopic0s.length)
    dataFilters.push({ address: addrs, topic0: f.childTopic0s });
}
for (const s of chain.singletons)
  if (s.topic0s.length)
    dataFilters.push({ address: [s.address], topic0: s.topic0s });

const topicLabel = new Map<string, string>();
for (const f of chain.factories) {
  f.childTopic0s.forEach((t, i) => {
    topicLabel.set(t, `${f.name}.${f.childEventNames[i]}`);
  });
}
for (const s of chain.singletons) {
  s.topic0s.forEach((t, i) => {
    topicLabel.set(t, `${s.name}.${s.eventNames[i]}`);
  });
}

const perEvent = new Map<string, number>();
let sampleRow: unknown;
const t2 = Date.now();
let p2http = 0;
let totalLogs = 0;
for await (const batch of client.streamFinalized(
  buildPortalQuery([START, TO], dataFilters),
)) {
  p2http++;
  for (const b of batch.blocks) {
    for (const log of b.logs ?? []) {
      totalLogs++;
      const label = topicLabel.get(log.topics?.[0]) ?? 'other';
      perEvent.set(label, (perEvent.get(label) ?? 0) + 1);
      if (!sampleRow) sampleRow = toSyncLog(log, b.header); // prove transform on real data
    }
  }
  if (p2http % 25 === 0)
    console.log(
      `  …http#${p2http} block ${batch.toBlock} | ${totalLogs.toLocaleString()} logs`,
    );
}
const p2s = ((Date.now() - t2) / 1000).toFixed(1);
console.log(
  `  ${totalLogs.toLocaleString()} child/singleton logs in ${p2http} http, ${p2s}s`,
);

// ---------- report ----------
console.log(`\n=== EVENTS INDEXED (per source) ===`);
for (const [label, n] of [...perEvent.entries()].sort((a, b) => b[1] - a[1]))
  console.log(`  ${label.padEnd(28)} ${n.toLocaleString()}`);

console.log(`\n=== SAMPLE SyncLog (Portal NDJSON → Ponder row) ===`);
console.dir(sampleRow, { depth: 3 });

const snap = metrics.snapshot();
console.log(
  `\n=== PORTAL METRICS — full Euler backfill (2 logical streams) ===`,
);
console.dir(snap, { depth: 4 });

// ---------- inline gate ----------
const d = snap.perDataset[0]!;
const gate = {
  client_facing_503_529: { value: d.clientFacingErrors, max: 0 },
  logical_streams: { value: d.logicalStreams, max: 2 },
  streams_per_1000_blocks: { value: d.streamsPer1000Blocks, max: 1.5 },
  forward_progress_blocks: { value: d.forwardProgressBlocks, min: TO - START },
};
console.log(`\n=== EVAL GATE ===`);
let pass = true;
for (const [k, g] of Object.entries(gate)) {
  const ok =
    ('max' in g ? (g as any).value <= (g as any).max : true) &&
    ('min' in g ? (g as any).value >= (g as any).min : true);
  pass &&= ok;
  console.log(
    `  [${ok ? 'PASS' : 'FAIL'}] ${k} = ${(g as any).value} (${'max' in g ? '≤' + (g as any).max : '≥' + (g as any).min})`,
  );
}
console.log(`\n  GATE: ${pass ? '✅ PASS' : '❌ FAIL'}`);

const out = `/Users/dz/Projects/portal-ponder/harness/results.${chain.dataset}.json`;
writeFileSync(
  out,
  JSON.stringify(
    {
      chain: chain.chainId,
      dataset: chain.dataset,
      range: [START, TO],
      perEvent: Object.fromEntries(perEvent),
      metrics: snap,
      gate,
      pass,
    },
    null,
    2,
  ),
);
console.log(`\nwrote ${out}`);
process.exit(pass ? 0 : 1);
