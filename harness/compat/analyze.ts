/**
 * Compatibility analyzer. Two independent gates per source:
 *  1. SUPPORT — does OUR Portal backfill implement this source type at all?
 *  2. ChainFeatures — does Portal actually SERVE this data for THIS chain, and
 *     (for traces) for this source's block RANGE? Trace coverage is per-chain and
 *     per-range — e.g. Arbitrum/Polygon have no traces for ancient blocks. These
 *     come from live probes (report.ts); absent → fall back to SUPPORT only.
 */
import { datasetForChain, type DatasetInfo } from "./datasets.ts";

/** Implementation state of OUR Portal HistoricalSync (update as features land). */
export const SUPPORT = {
  logs: "yes",
  logFactory: "yes",
  transactions: "yes",
  receipts: "yes",
  traces: "yes",
  blockInterval: "todo",
  accountTx: "todo",
} as const;

export type Need = keyof typeof SUPPORT;
export type Verdict = "READY" | "NEEDS_RECEIPTS" | "NEEDS_TRACES" | "NEEDS_BLOCK_FILTER" | "NEEDS_ACCOUNT_SOURCES" | "NO_DATASET";

/** What Portal actually serves for a given chain (probed live). */
export type ChainFeatures = { traces: boolean; tracesFromBlock?: number; receipts: boolean; stateDiffs: boolean };

export type SourceReport = {
  source: string; chain: string; chainId: number; dataset: string | null; datasetRealTime: boolean | null;
  startBlock?: number; needs: Need[]; verdict: Verdict; blockers: string[];
};

export type CompatReport = {
  overall: "READY" | "PARTIAL" | "BLOCKED";
  ready: number; blocked: number; sources: SourceReport[];
  chains: { name: string; chainId: number; dataset: string | null; realTime: boolean | null; features?: ChainFeatures }[];
};

const isFactory = (addr: any): boolean =>
  addr != null && typeof addr === "object" && !Array.isArray(addr) && ("event" in addr || "parameter" in addr || "childAddressLocation" in addr);

function chainEntries(src: any): { chain: string; override: any }[] {
  if (typeof src.chain === "string") return [{ chain: src.chain, override: src }];
  if (src.chain && typeof src.chain === "object") return Object.entries(src.chain).map(([chain, override]) => ({ chain, override: { ...src, ...(override as any) } }));
  return [];
}

const startBlockOf = (override: any): number | undefined => {
  if (typeof override.startBlock === "number") return override.startBlock;
  if (isFactory(override.address) && typeof override.address.startBlock === "number") return override.address.startBlock;
  return undefined;
};

function verdictFor(needs: Need[], dataset: string | null, feat: ChainFeatures | undefined, startBlock: number | undefined): { verdict: Verdict; blockers: string[] } {
  if (!dataset) return { verdict: "NO_DATASET", blockers: ["no SQD Portal dataset for this chain"] };
  const blockers: string[] = [];
  for (const n of needs) {
    if (SUPPORT[n] === "todo") { blockers.push(`${n} not yet implemented in the Portal backfill`); continue; }
    if (!feat) continue; // no live probe → trust SUPPORT only
    if (n === "traces") {
      if (!feat.traces) blockers.push("Portal does not serve traces on this chain");
      else if (feat.tracesFromBlock !== undefined && startBlock !== undefined && startBlock < feat.tracesFromBlock)
        blockers.push(`Portal traces on this chain begin ~block ${feat.tracesFromBlock.toLocaleString()}; this source's startBlock ${startBlock.toLocaleString()} is in the trace-less range`);
    }
    if (n === "receipts" && !feat.receipts) blockers.push("Portal does not serve receipts on this chain");
  }
  if (blockers.length === 0) return { verdict: "READY", blockers };
  if (blockers.some((b) => b.includes("trace"))) return { verdict: "NEEDS_TRACES", blockers };
  if (needs.includes("receipts")) return { verdict: "NEEDS_RECEIPTS", blockers };
  if (needs.includes("blockInterval")) return { verdict: "NEEDS_BLOCK_FILTER", blockers };
  return { verdict: "NEEDS_ACCOUNT_SOURCES", blockers };
}

export function analyzeConfig(config: any, catalog: Map<string, DatasetInfo>, features: Map<number, ChainFeatures> = new Map()): CompatReport {
  const chainId = (name: string): number => config.chains?.[name]?.id ?? 0;
  const datasetOf = (cid: number): { dataset: string | null; realTime: boolean | null } => {
    const slug = datasetForChain(cid);
    if (!slug) return { dataset: null, realTime: null };
    const info = catalog.get(slug);
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
    const startBlock = startBlockOf(override);
    const { verdict, blockers } = verdictFor(needsArr, dataset, features.get(cid), startBlock);
    sources.push({ source: name, chain, chainId: cid, dataset, datasetRealTime: realTime, startBlock, needs: needsArr, verdict, blockers });
  };

  for (const [name, c] of Object.entries(config.contracts ?? {})) for (const { chain, override } of chainEntries(c)) push(name, chain, ["logs", "transactions"], override);
  for (const [name, a] of Object.entries(config.accounts ?? {})) for (const { chain, override } of chainEntries(a)) push(`account:${name}`, chain, ["transactions", "accountTx"], override);
  for (const [name, b] of Object.entries(config.blocks ?? {})) for (const { chain, override } of chainEntries(b)) push(`block:${name}`, chain, ["blockInterval"], override);

  const ready = sources.filter((s) => s.verdict === "READY").length;
  const blocked = sources.length - ready;
  const overall = blocked === 0 ? "READY" : ready === 0 ? "BLOCKED" : "PARTIAL";
  const chains = Object.entries(config.chains ?? {}).map(([name, c]: any) => {
    const { dataset, realTime } = datasetOf(c.id);
    return { name, chainId: c.id, dataset, realTime, features: features.get(c.id) };
  });
  return { overall, ready, blocked, sources, chains };
}
