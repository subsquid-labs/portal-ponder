/**
 * Portal compatibility report for a Ponder project — per-chain & per-range aware.
 *
 *   cd <client-ponder-project>
 *   PORTAL_API_KEY=<key> node <path>/report.ts ./ponder.config.ts [--differential <rpcUrl>]
 *
 * Imports the resolved config, checks each source against (1) what our backfill
 * implements and (2) what Portal actually SERVES for that chain — including a live
 * probe of trace availability per block-range (Arbitrum/Polygon lack ancient traces).
 *
 * Env: PORTAL_BASE (default https://portal.sqd.dev/datasets), PORTAL_API_KEY.
 */
import { pathToFileURL } from "node:url";
import { type ChainFeatures, analyzeConfig, type CompatReport } from "./analyze.ts";
import { ALL_NETWORKS_DOCS, fetchCatalog } from "./datasets.ts";
import { probeChainFeatures } from "./probe.ts";

const BASE = process.env.PORTAL_BASE ?? "https://portal.sqd.dev/datasets";
const API_KEY = process.env.PORTAL_API_KEY;
const ICON: Record<string, string> = { READY: "✅", NO_DATASET: "⛔", NEEDS_RECEIPTS: "🟡", NEEDS_TRACES: "🟡", NEEDS_BLOCK_FILTER: "🟡", NEEDS_ACCOUNT_SOURCES: "🟡" };

const finalizedHead = async (dataset: string): Promise<number> => {
  const headers: Record<string, string> = {}; if (API_KEY) headers["x-api-key"] = API_KEY;
  const r = await fetch(`${BASE}/${dataset}/finalized-head`, { headers }).catch(() => null);
  return r?.ok ? ((await r.json()).number ?? 0) : 0;
};

function fmtFeatures(f?: ChainFeatures): string {
  if (!f) return "";
  const t = f.traces ? (f.tracesFromBlock ? `from ~${f.tracesFromBlock.toLocaleString()}` : "yes") : "no";
  return ` | traces:${t} receipts:${f.receipts ? "yes" : "no"} stateDiffs:${f.stateDiffs ? "yes" : "no"}`;
}

function print(report: CompatReport) {
  console.log(`\n=================  PORTAL COMPATIBILITY REPORT  =================`);
  console.log(`overall: ${report.overall}   (${report.ready} ready / ${report.blocked} blocked of ${report.sources.length} sources)\n`);
  console.log(`CHAINS  (traces/receipts/stateDiffs probed live; full matrix: ${ALL_NETWORKS_DOCS})`);
  for (const c of report.chains) {
    const ds = c.dataset ? `${c.dataset}${c.realTime === false ? " (not real-time)" : ""}` : "⛔ NO PORTAL DATASET";
    console.log(`  ${String(c.name).padEnd(12)} id=${String(c.chainId).padEnd(8)} → ${ds}${fmtFeatures(c.features)}`);
  }
  console.log(`\nSOURCES`);
  for (const s of report.sources) {
    console.log(`  ${ICON[s.verdict] ?? "•"} ${s.source} @ ${s.chain}  [${s.verdict}]`);
    console.log(`      needs: ${s.needs.join(", ")}${s.startBlock !== undefined ? ` | startBlock ${s.startBlock.toLocaleString()}` : ""}`);
    for (const b of s.blockers) console.log(`      ⚠ ${b}`);
  }
  console.log(`\nVERDICT`);
  if (report.overall === "READY") console.log(`  ✅ READY NOW — Portal serves every source (on these chains & ranges). Enable the backfill boost.`);
  else if (report.overall === "PARTIAL") console.log(`  🟡 PARTIAL — boost the ready sources now; the rest are blocked by an unimplemented feature OR a per-chain/per-range Portal gap (see ⚠).`);
  else console.log(`  ⛔ BLOCKED — see blockers.`);
}

const configPath = process.argv[2];
if (!configPath) { console.error("usage: report.ts <ponder.config.ts> [--differential <rpcUrl>]"); process.exit(1); }
const config: any = (await import(pathToFileURL(configPath).href)).default;
const catalog = await fetchCatalog(BASE, API_KEY);

// pass 1 (static) to discover which chains have trace/receipt sources + their startBlocks
const pre = analyzeConfig(config, catalog);
const features = new Map<number, ChainFeatures>();
for (const c of pre.chains) {
  if (!c.dataset) continue;
  const onChain = pre.sources.filter((s) => s.chainId === c.chainId);
  const traceSrc = onChain.filter((s) => s.needs.includes("traces"));
  const receiptSrc = onChain.filter((s) => s.needs.includes("receipts"));
  const head = await finalizedHead(c.dataset);
  if (head === 0) continue;
  const probeStart = traceSrc.length ? Math.min(...traceSrc.map((s) => s.startBlock ?? head)) : head;
  process.stderr.write(`  probing ${c.dataset}…\n`);
  features.set(c.chainId, await probeChainFeatures(BASE, API_KEY, c.dataset, probeStart, head, { traces: traceSrc.length > 0, receipts: receiptSrc.length > 0 }));
}

// pass 2 with live per-chain/per-range features
const report = analyzeConfig(config, catalog, features);
print(report);
process.exit(report.overall === "BLOCKED" ? 1 : 0);
