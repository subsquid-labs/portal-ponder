/**
 * Run the indexer benchmark base and print a comparable table.
 *   PORTAL_API_KEY=… node --experimental-strip-types harness/bench/bench-all.ts [name-filter]
 *
 * Each spec lives in benches.ts. Priorities surfaced in the table: STABILITY (ok? errors,
 * retries, peak RSS) and BACKFILL SPEED (wall-clock, events/sec), plus Portal efficiency.
 */
import { writeFileSync } from "node:fs";
import { BENCHES } from "./benches.ts";
import { type BenchResult, runBench } from "./run-bench.ts";

const filter = process.argv[2]; // comma-separated substrings, e.g. "uniswap,traces,blocks"
const specs = BENCHES.filter((b) => !filter || filter.split(",").some((f) => b.name.includes(f.trim())));
if (specs.length === 0) { console.error(`no benches match '${filter}'. available: ${BENCHES.map((b) => b.name).join(", ")}`); process.exit(1); }

const pad = (s: string | number, n: number) => String(s).padEnd(n);
const padL = (s: string | number, n: number) => String(s).padStart(n);

const results: BenchResult[] = [];
for (const spec of specs) {
  process.stderr.write(`\n▶ ${spec.name}  [${spec.start.toLocaleString()},${spec.end.toLocaleString()}] …\n`);
  const r = await runBench(spec);
  results.push(r);
  process.stderr.write(`  ${r.ok ? "✅" : "❌ " + r.error}  ${r.wallSec}s  ${r.events.toLocaleString()} events  ${r.eventsPerSec.toLocaleString()}/s  rss ${r.peakRssMB}MB` +
    (r.portal ? `  | portal: http ${r.portal.http} ${r.portal.mb.toFixed(1)}MB chunks ${r.portal.dataChunks} err ${r.portal.errors} retry ${r.portal.retries} fallback ${r.portal.rpcFallback}` : "") + "\n");
}

// table
const H = ["indexer", "ok", "wall(s)", "events", "ev/s", "rssMB", "http", "MB", "chunks", "err", "retry", "fallbck"];
const W = [26, 3, 8, 10, 9, 6, 6, 8, 7, 5, 6, 8];
const line = (cells: (string | number)[]) => cells.map((c, i) => (i <= 1 ? pad(c, W[i]) : padL(c, W[i]))).join(" ");
const rows = results.map((r) => line([
  r.name.slice(0, 26), r.ok ? "✓" : "✗", r.wallSec, r.events, r.eventsPerSec, r.peakRssMB,
  r.portal?.http ?? "-", r.portal ? r.portal.mb.toFixed(1) : "-", r.portal?.dataChunks ?? "-",
  r.portal?.errors ?? "-", r.portal?.retries ?? "-", r.portal?.rpcFallback ?? "-",
]));
const table = [line(H), "-".repeat(W.reduce((a, b) => a + b + 1, 0)), ...rows].join("\n");
console.log("\n" + table + "\n");

const ts = process.env.BENCH_TS ?? "unstamped"; // pass BENCH_TS=$(date) to stamp
writeFileSync(new URL("./results.json", import.meta.url), JSON.stringify({ ts, results }, null, 2));
writeFileSync(new URL("./results.md", import.meta.url), "# Portal backfill benchmark base\n\n_run: " + ts + "_\n\n```\n" + table + "\n```\n");
console.log("→ wrote harness/bench/results.{json,md}");
