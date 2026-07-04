/**
 * Portal compatibility report for a Ponder project.
 *
 *   cd <client-ponder-project>
 *   PORTAL_API_KEY=<key> PORTAL_BASE=<the portal you'll use> \
 *     node <path>/harness/compat/report.ts ./ponder.config.ts
 *
 * Per source it checks: (1) does our backfill implement the type, (2) does the TARGET
 * portal serve that chain's dataset (live /datasets — per-portal!), (3) does the
 * network have the data the source needs (traces) per the authoritative docs matrix.
 *
 * Env: PORTAL_BASE (default https://portal.sqd.dev/datasets) — set this to the portal
 * the client will actually use, since different portals serve different datasets.
 */
import { pathToFileURL } from 'node:url';
import { analyzeConfig, type CompatReport } from './analyze.ts';
import { ALL_NETWORKS_DOCS, fetchCatalog } from './datasets.ts';

const BASE = process.env.PORTAL_BASE ?? 'https://portal.sqd.dev/datasets';
const API_KEY = process.env.PORTAL_API_KEY;
const ICON: Record<string, string> = {
  READY: '✅',
  NO_DATASET: '⛔',
  NEEDS_RECEIPTS: '🟡',
  NEEDS_TRACES: '🟡',
  NEEDS_BLOCK_FILTER: '🟡',
  NEEDS_ACCOUNT_SOURCES: '🟡',
};

function print(report: CompatReport) {
  console.log(
    `\n=================  PORTAL COMPATIBILITY REPORT  =================`,
  );
  console.log(`portal:  ${BASE}`);
  console.log(
    `overall: ${report.overall}   (${report.ready} ready / ${report.blocked} blocked of ${report.sources.length} sources)\n`,
  );
  console.log(
    `CHAINS   (existence = this portal's /datasets; caps = docs matrix ${ALL_NETWORKS_DOCS})`,
  );
  for (const c of report.chains) {
    if (!c.dataset) {
      console.log(
        `  ${String(c.name).padEnd(12)} id=${String(c.chainId).padEnd(9)} ⛔ not in the docs network matrix`,
      );
      continue;
    }
    const served =
      c.servedByPortal === false
        ? 'NOT served by this portal'
        : c.servedByPortal === true
          ? 'served'
          : 'served? (catalog unavailable)';
    const caps = c.caps
      ? `traces:${c.caps.traces ? 'yes' : 'no'} stateDiffs:${c.caps.stateDiffs ? 'yes' : 'no'} realtime:${c.caps.realtime ? 'yes' : 'no'}`
      : '';
    console.log(
      `  ${String(c.name).padEnd(12)} id=${String(c.chainId).padEnd(9)} → ${c.dataset} [${served}] ${caps}`,
    );
    if (c.caps?.note) console.log(`      ⓘ ${c.caps.note}`);
  }
  console.log(`\nSOURCES`);
  for (const s of report.sources) {
    console.log(
      `  ${ICON[s.verdict] ?? '•'} ${s.source} @ ${s.chain}  [${s.verdict}]`,
    );
    console.log(
      `      needs: ${s.needs.join(', ')}${s.startBlock !== undefined ? ` | startBlock ${s.startBlock.toLocaleString()}` : ''}`,
    );
    for (const b of s.blockers) console.log(`      ⚠ ${b}`);
    for (const n of s.notes) console.log(`      ⓘ ${n}`);
  }
  console.log(`\nVERDICT`);
  if (report.overall === 'READY')
    console.log(
      `  ✅ READY NOW — enable the backfill boost. (Check any ⓘ block-range notes against your startBlock.)`,
    );
  else if (report.overall === 'PARTIAL')
    console.log(
      `  🟡 PARTIAL — boost the ready sources now; the rest are blocked by an unimplemented feature, a per-portal gap, or a missing capability (see ⚠).`,
    );
  else console.log(`  ⛔ BLOCKED — see blockers.`);
}

const configPath = process.argv[2];
if (!configPath) {
  console.error('usage: report.ts <ponder.config.ts>');
  process.exit(1);
}
const config: any = (await import(pathToFileURL(configPath).href)).default;
const catalog = await fetchCatalog(BASE, API_KEY); // per-portal existence
print(analyzeConfig(config, catalog));
process.exit(0);
