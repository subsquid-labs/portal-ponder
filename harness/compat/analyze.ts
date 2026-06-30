/**
 * Compatibility analyzer: given a resolved Ponder config, work out which data
 * types each source needs and whether the Portal backfill path serves them today.
 *
 * Update SUPPORT as features land in createPortalHistoricalSync.
 */
import { datasetForChain, type DatasetInfo } from "./datasets.ts";

/** Current implementation state of the Portal HistoricalSync. */
export const SUPPORT = {
  logs: "yes",
  logFactory: "yes",
  transactions: "yes",
  receipts: "todo", // not wired to insertTransactionReceipts yet
  traces: "todo", // Parity→callTracer transform prototyped, not wired into core
  blockInterval: "todo", // block filters not handled
  accountTx: "todo", // transaction/transfer (from/to) filters not handled
} as const;

export type Need = keyof typeof SUPPORT;
export type Verdict = "READY" | "NEEDS_RECEIPTS" | "NEEDS_TRACES" | "NEEDS_BLOCK_FILTER" | "NEEDS_ACCOUNT_SOURCES" | "NO_DATASET";

export type SourceReport = {
  source: string;
  chain: string;
  chainId: number;
  dataset: string | null;
  datasetRealTime: boolean | null;
  needs: Need[];
  verdict: Verdict;
  blockers: string[];
};

export type CompatReport = {
  overall: "READY" | "PARTIAL" | "BLOCKED";
  ready: number;
  blocked: number;
  sources: SourceReport[];
  chains: { name: string; chainId: number; dataset: string | null; realTime: boolean | null }[];
};

const isFactory = (addr: any): boolean =>
  addr != null && typeof addr === "object" && !Array.isArray(addr) && ("event" in addr || "parameter" in addr || "childAddressLocation" in addr);

/** Expand a contract/account's `chain` (string | { name: {...} }) into [{chainName, override}]. */
function chainEntries(src: any): { chain: string; override: any }[] {
  if (typeof src.chain === "string") return [{ chain: src.chain, override: src }];
  if (src.chain && typeof src.chain === "object") return Object.entries(src.chain).map(([chain, override]) => ({ chain, override: { ...src, ...(override as any) } }));
  return [];
}

function verdictFor(needs: Need[], dataset: string | null): { verdict: Verdict; blockers: string[] } {
  if (!dataset) return { verdict: "NO_DATASET", blockers: ["no SQD Portal dataset for this chain"] };
  const blockers: string[] = [];
  for (const n of needs) if (SUPPORT[n] === "todo") blockers.push(`${n} not yet implemented in Portal backfill`);
  if (blockers.length === 0) return { verdict: "READY", blockers };
  if (needs.includes("receipts")) return { verdict: "NEEDS_RECEIPTS", blockers };
  if (needs.includes("traces")) return { verdict: "NEEDS_TRACES", blockers };
  if (needs.includes("blockInterval")) return { verdict: "NEEDS_BLOCK_FILTER", blockers };
  return { verdict: "NEEDS_ACCOUNT_SOURCES", blockers };
}

export function analyzeConfig(config: any, catalog: Map<string, DatasetInfo>): CompatReport {
  const chainId = (name: string): number => config.chains?.[name]?.id ?? 0;
  const datasetOf = (cid: number): { dataset: string | null; realTime: boolean | null } => {
    const slug = datasetForChain(cid);
    if (!slug) return { dataset: null, realTime: null };
    const info = catalog.get(slug);
    // if catalog unreachable, trust the static map
    return catalog.size === 0 ? { dataset: slug, realTime: null } : info ? { dataset: info.dataset, realTime: info.realTime } : { dataset: null, realTime: null };
  };

  const sources: SourceReport[] = [];
  const push = (name: string, chain: string, baseNeeds: Need[], override: any) => {
    const cid = chainId(chain);
    const { dataset, realTime } = datasetOf(cid);
    const needs = new Set<Need>(baseNeeds);
    if (isFactory(override.address)) needs.add("logFactory");
    if (override.includeTransactionReceipts === true) needs.add("receipts");
    if (override.includeCallTraces === true) needs.add("traces");
    const needsArr = [...needs];
    const { verdict, blockers } = verdictFor(needsArr, dataset);
    sources.push({ source: name, chain, chainId: cid, dataset, datasetRealTime: realTime, needs: needsArr, verdict, blockers });
  };

  for (const [name, c] of Object.entries(config.contracts ?? {})) for (const { chain, override } of chainEntries(c)) push(name, chain, ["logs", "transactions"], override);
  for (const [name, a] of Object.entries(config.accounts ?? {})) for (const { chain, override } of chainEntries(a)) push(`account:${name}`, chain, ["transactions", "accountTx"], override);
  for (const [name, b] of Object.entries(config.blocks ?? {})) for (const { chain, override } of chainEntries(b)) push(`block:${name}`, chain, ["blockInterval"], override);

  const ready = sources.filter((s) => s.verdict === "READY").length;
  const blocked = sources.length - ready;
  const overall = blocked === 0 ? "READY" : ready === 0 ? "BLOCKED" : "PARTIAL";
  const chains = Object.entries(config.chains ?? {}).map(([name, c]: any) => {
    const { dataset, realTime } = datasetOf(c.id);
    return { name, chainId: c.id, dataset, realTime };
  });
  return { overall, ready, blocked, sources, chains };
}
