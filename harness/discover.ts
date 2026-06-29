/**
 * Real-Portal proof: run the EVault factory child-discovery scan (the first
 * half of what Ponder's factory() source does during backfill) the fork's way —
 * ONE Portal stream over the whole range — and report Portal-specific metrics.
 *
 * Env: DATASET (default monad-mainnet), FACTORY, TOPIC0, FROM, TO (default head).
 */
import { getAddress } from "viem";
import { PortalClient } from "../packages/portal-sync/src/portal-client.ts";
import { PortalMetrics } from "../packages/portal-sync/src/metrics.ts";
import type { PortalEvmQuery } from "../packages/portal-sync/src/portal-types.ts";

const DATASET = process.env.DATASET ?? "monad-mainnet";
const FACTORY = (process.env.FACTORY ?? "0xba4Dd672062dE8FeeDb665DD4410658864483f1E").toLowerCase();
// ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)
const TOPIC0 = process.env.TOPIC0 ?? "0x04e664079117e113faa9684bc14aecb41651cbf098b14eda271248c6d0cda57c";
const FROM = Number(process.env.FROM ?? 30_858_573);

const metrics = new PortalMetrics();
const client = new PortalClient({ dataset: DATASET, metrics });

const head = await client.getFinalizedHead();
if (head === undefined) throw new Error(`no finalized head for ${DATASET}`);
const TO = Number(process.env.TO ?? head.number);
console.log(`[discover] dataset=${DATASET} finalizedHead=${head.number}`);
console.log(`[discover] EVault factory ${getAddress(FACTORY)} ProxyCreated over [${FROM}, ${TO}] = ${(TO - FROM).toLocaleString()} blocks`);

const query: PortalEvmQuery = {
  type: "evm",
  fromBlock: FROM,
  toBlock: TO,
  fields: { block: { number: true, hash: true, timestamp: true }, log: { address: true, topics: true } },
  logs: [{ address: [FACTORY], topic0: [TOPIC0] }],
};

const children = new Map<string, number>(); // address -> creation block
let httpSeen = 0;
const t0 = Date.now();

for await (const batch of client.streamFinalized(query)) {
  httpSeen++;
  for (const b of batch.blocks) {
    for (const log of b.logs ?? []) {
      const proxyTopic = log.topics?.[1];
      if (proxyTopic) children.set(getAddress("0x" + proxyTopic.slice(26)), b.header.number);
    }
  }
  if (httpSeen % 5 === 0 || batch.toBlock >= TO) {
    const dt = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`  …http#${httpSeen} advanced to block ${batch.toBlock} | ${children.size} vaults | ${dt}s`);
  }
}

console.log("\n=== DISCOVERED VAULTS ===");
console.log(`${children.size} EVault proxies created in range`);
const sample = [...children.entries()].slice(0, 5);
for (const [addr, blk] of sample) console.log(`  ${addr}  @ block ${blk}`);

console.log("\n=== PORTAL METRICS (fork-style: one stream, self-paced) ===");
console.dir(metrics.snapshot(), { depth: 4 });

// persist discovered children for the backfill phase
const fs = await import("node:fs");
const out = `/Users/dz/Projects/portal-ponder/harness/euler/vaults.${DATASET}.json`;
fs.writeFileSync(out, JSON.stringify({ dataset: DATASET, factory: getAddress(FACTORY), from: FROM, to: TO, children: Object.fromEntries(children) }, null, 2));
console.log(`\nwrote ${children.size} vault addresses → ${out}`);
