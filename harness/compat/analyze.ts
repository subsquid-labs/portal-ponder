/**
 * Compatibility analyzer. Three gates per source:
 *  1. SUPPORT      — does OUR Portal backfill implement this source type at all?
 *  2. EXISTENCE    — does the TARGET portal serve this chain's dataset? (per-portal,
 *                    from the live /datasets catalog — different portals differ)
 *  3. CAPABILITY   — does the network have the data this source needs? (traces) —
 *                    from the authoritative docs matrix (networks.json), incl. the
 *                    block-range caveat note (e.g. Optimism traces from Bedrock).
 */
import { capsForChain, type DatasetInfo, type NetworkCaps } from "./datasets.ts";

/** Implementation state of OUR Portal HistoricalSync (update as features land). */
export const SUPPORT = {
  logs: "yes",
  logFactory: "yes",
  transactions: "yes",
  receipts: "yes",
  traces: "yes",
  blockInterval: "yes",
  accountTx: "todo",
} as const;

export type Need = keyof typeof SUPPORT;
export type Verdict = "READY" | "NEEDS_RECEIPTS" | "NEEDS_TRACES" | "NEEDS_BLOCK_FILTER" | "NEEDS_ACCOUNT_SOURCES" | "NO_DATASET";

export type SourceReport = {
  source: string; chain: string; chainId: number; dataset: string | null; datasetRealTime: boolean | null;
  startBlock?: number; needs: Need[]; verdict: Verdict; blockers: string[]; notes: string[];
};

export type ChainSummary = { name: string; chainId: number; dataset: string | null; servedByPortal: boolean | null; caps?: NetworkCaps };

export type CompatReport = {
  overall: "READY" | "PARTIAL" | "BLOCKED";
  ready: number; blocked: number; sources: SourceReport[]; chains: ChainSummary[];
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

export function analyzeConfig(config: any, catalog: Map<string, DatasetInfo> = new Map()): CompatReport {
  const chainId = (name: string): number => config.chains?.[name]?.id ?? 0;

  // existence is PER-PORTAL: caps give the canonical slug; the catalog says if THIS portal serves it.
  const resolve = (cid: number): { dataset: string | null; served: boolean | null; caps?: NetworkCaps } => {
    const caps = capsForChain(cid);
    if (!caps) return { dataset: null, served: null };
    const served = catalog.size === 0 ? null : catalog.has(caps.slug);
    return { dataset: caps.slug, served, caps };
  };

  const sources: SourceReport[] = [];
  const push = (name: string, chain: string, baseNeeds: Need[], override: any) => {
    const cid = chainId(chain);
    const { dataset, served, caps } = resolve(cid);
    const needs = new Set<Need>(baseNeeds);
    if (isFactory(override.address)) needs.add("logFactory");
    if (override.includeTransactionReceipts === true) needs.add("receipts");
    if (override.includeCallTraces === true) needs.add("traces");
    const needsArr = [...needs];
    const startBlock = startBlockOf(override);

    const blockers: string[] = [];
    const notes: string[] = [];
    let verdict: Verdict;
    if (!dataset) { verdict = "NO_DATASET"; blockers.push("no SQD Portal dataset for this chain (not in the docs network matrix)"); }
    else if (served === false) { verdict = "NO_DATASET"; blockers.push(`this portal does not serve '${dataset}' (per its /datasets); a different portal may`); }
    else {
      for (const n of needsArr) if (SUPPORT[n] === "todo") blockers.push(`${n} not yet implemented in the Portal backfill`);
      if (needs.has("traces") && caps && !caps.traces) blockers.push(`Portal has no traces for ${dataset}`);
      if (caps?.note && (needs.has("traces") || needs.has("transactions"))) notes.push(caps.note); // e.g. "traces from Bedrock block …"
      if (blockers.length === 0) verdict = "READY";
      else if (blockers.some((b) => b.includes("trace"))) verdict = "NEEDS_TRACES";
      else if (needsArr.includes("receipts")) verdict = "NEEDS_RECEIPTS";
      else if (needsArr.includes("blockInterval")) verdict = "NEEDS_BLOCK_FILTER";
      else verdict = "NEEDS_ACCOUNT_SOURCES";
    }
    sources.push({ source: name, chain, chainId: cid, dataset, datasetRealTime: caps?.realtime ?? null, startBlock, needs: needsArr, verdict, blockers, notes });
  };

  for (const [name, c] of Object.entries(config.contracts ?? {})) for (const { chain, override } of chainEntries(c)) push(name, chain, ["logs", "transactions"], override);
  for (const [name, a] of Object.entries(config.accounts ?? {})) for (const { chain, override } of chainEntries(a)) push(`account:${name}`, chain, ["transactions", "accountTx"], override);
  for (const [name, b] of Object.entries(config.blocks ?? {})) for (const { chain, override } of chainEntries(b)) push(`block:${name}`, chain, ["blockInterval"], override);

  const ready = sources.filter((s) => s.verdict === "READY").length;
  const blocked = sources.length - ready;
  const overall = blocked === 0 ? "READY" : ready === 0 ? "BLOCKED" : "PARTIAL";
  const chains: ChainSummary[] = Object.entries(config.chains ?? {}).map(([name, c]: any) => {
    const { dataset, served, caps } = resolve(c.id);
    return { name, chainId: c.id, dataset, servedByPortal: served, caps };
  });
  return { overall, ready, blocked, sources, chains };
}
