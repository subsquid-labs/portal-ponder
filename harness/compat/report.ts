/**
 * Portal compatibility report for a Ponder project.
 *
 *   cd <client-ponder-project>
 *   node --experimental-strip-types <path>/report.ts ./ponder.config.ts [--differential <rpcUrl>]
 *
 * Dynamic-imports the resolved config (so it sees factories, includes, multi-chain),
 * checks each source against the live Portal catalog + the current backfill feature
 * set, and prints a per-source readiness verdict. With --differential it proves
 * byte-parity of Portal vs RPC logs on a sample of one READY log source.
 *
 * Env: PORTAL_BASE (default https://portal.sqd.dev/datasets), PORTAL_API_KEY.
 */
import { pathToFileURL } from "node:url";
import { analyzeConfig, type CompatReport } from "./analyze.ts";
import { fetchCatalog } from "./datasets.ts";

const BASE = process.env.PORTAL_BASE ?? "https://portal.sqd.dev/datasets";
const API_KEY = process.env.PORTAL_API_KEY;

const ICON: Record<string, string> = { READY: "✅", NO_DATASET: "⛔", NEEDS_RECEIPTS: "🟡", NEEDS_TRACES: "🟡", NEEDS_BLOCK_FILTER: "🟡", NEEDS_ACCOUNT_SOURCES: "🟡" };

function print(report: CompatReport) {
  console.log(`\n=================  PORTAL COMPATIBILITY REPORT  =================`);
  console.log(`overall: ${report.overall}   (${report.ready} ready / ${report.blocked} blocked of ${report.sources.length} sources)\n`);

  console.log(`CHAINS`);
  for (const c of report.chains) {
    const ds = c.dataset ? `${c.dataset}${c.realTime === false ? " (not real-time)" : ""}` : "⛔ NO PORTAL DATASET";
    console.log(`  ${String(c.name).padEnd(12)} id=${String(c.chainId).padEnd(8)} → ${ds}`);
  }

  console.log(`\nSOURCES`);
  for (const s of report.sources) {
    console.log(`  ${ICON[s.verdict] ?? "•"} ${s.source} @ ${s.chain}  [${s.verdict}]`);
    console.log(`      needs: ${s.needs.join(", ")}${s.dataset ? `  | dataset: ${s.dataset}` : ""}`);
    for (const b of s.blockers) console.log(`      ⚠ ${b}`);
  }

  console.log(`\nVERDICT`);
  if (report.overall === "READY") console.log(`  ✅ READY NOW — all sources are served by the Portal backfill path. Safe to enable backfill-only boost.`);
  else if (report.overall === "PARTIAL") console.log(`  🟡 PARTIAL — ${report.ready} source(s) ready now; the rest need features not yet implemented (see blockers). You can boost the ready chains/sources today and keep the rest on RPC.`);
  else console.log(`  ⛔ BLOCKED — no source is served yet. See blockers.`);
  console.log(`\n(Portal backfill feature set today: logs ✓, log-factory ✓, transactions ✓, receipts ✗, traces ✗, block-interval ✗, account/transfer ✗)`);
}

async function differential(rpcUrl: string, report: CompatReport) {
  const target = report.sources.find((s) => s.verdict === "READY" && s.dataset);
  if (!target) { console.log("\n[differential] no READY log source to sample — skipping"); return; }
  console.log(`\n[differential] sampling ${target.source} @ ${target.chain} (${target.dataset}) — Portal vs RPC parity…`);
  // Resolve a recent finalized range and compare eth_getLogs over it.
  const headers: Record<string, string> = {}; if (API_KEY) headers["x-api-key"] = API_KEY;
  const head = (await (await fetch(`${BASE}/${target.dataset}/finalized-head`, { headers })).json()).number as number;
  const from = head - 200, to = head - 100;
  const portalQ = { type: "evm", fromBlock: from, toBlock: to, fields: { block: { number: true }, log: { address: true, topics: true, data: true, transactionHash: true, logIndex: true } }, logs: [{}] };
  const pRes = await fetch(`${BASE}/${target.dataset}/finalized-stream`, { method: "POST", headers: { ...headers, "content-type": "application/json" }, body: JSON.stringify(portalQ) });
  const pLogs: any[] = [];
  for (const line of (await pRes.text()).trim().split("\n").filter(Boolean)) { const b = JSON.parse(line); for (const l of b.logs ?? []) pLogs.push({ k: `${b.header.number}:${BigInt(l.logIndex)}` }); }
  const rRes = await fetch(rpcUrl, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_getLogs", params: [{ fromBlock: `0x${from.toString(16)}`, toBlock: `0x${to.toString(16)}` }] }) });
  const rLogs = ((await rRes.json()).result ?? []) as any[];
  const match = pLogs.length === rLogs.length;
  console.log(`  Portal: ${pLogs.length} logs | RPC: ${rLogs.length} logs → ${match ? "✅ counts match" : "⚠ MISMATCH (investigate)"} over blocks [${from},${to}]`);
}

const configPath = process.argv[2];
if (!configPath) { console.error("usage: report.ts <ponder.config.ts> [--differential <rpcUrl>]"); process.exit(1); }
const config: any = (await import(pathToFileURL(configPath).href)).default;
const catalog = await fetchCatalog(BASE, API_KEY);
const report = analyzeConfig(config, catalog);
print(report);
const diffIdx = process.argv.indexOf("--differential");
if (diffIdx > 0 && process.argv[diffIdx + 1]) await differential(process.argv[diffIdx + 1]!, report);
process.exit(report.overall === "BLOCKED" ? 1 : 0);
