/**
 * Live Portal probes for per-chain / per-range feature availability.
 * Trace coverage especially is per-chain AND per-range (Arbitrum/Polygon lack
 * traces for ancient blocks), so we probe rather than assume.
 */
import type { ChainFeatures } from "./analyze.ts";

const SPAN = 4000; // probe window; dense chains always have txs in this many blocks

async function probeRange(base: string, dataset: string, apiKey: string | undefined, kind: "traces" | "stateDiffs" | null, from: number): Promise<{ hasKind: boolean; hasTxs: boolean }> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (apiKey) headers["x-api-key"] = apiKey;
  const fields: any = { transaction: { hash: true, status: true } };
  const body: any = { type: "evm", fromBlock: from, toBlock: from + SPAN, fields, transactions: [{}] };
  if (kind === "traces") { fields.trace = { type: true }; body.traces = [{}]; }
  if (kind === "stateDiffs") { fields.stateDiff = { kind: true }; body.stateDiffs = [{}]; }
  const res = await fetch(`${base}/${dataset}/finalized-stream`, { method: "POST", headers, body: JSON.stringify(body) }).catch(() => null);
  if (!res || res.status === 204) return { hasKind: false, hasTxs: false };
  if (!res.ok) { await res.body?.cancel().catch(() => {}); return { hasKind: false, hasTxs: false }; }
  let hasKind = false, hasTxs = false;
  for (const line of (await res.text()).trim().split("\n").filter(Boolean)) {
    const b = JSON.parse(line);
    if (b.transactions?.length) hasTxs = true;
    if (kind === "traces" && b.traces?.length) hasKind = true;
    if (kind === "stateDiffs" && b.stateDiffs?.length) hasKind = true;
  }
  return { hasKind, hasTxs };
}

/** Binary-search the first block where Portal serves traces, in [lo, hi]. ~14 probes. */
async function firstTraceBlock(base: string, dataset: string, apiKey: string | undefined, lo: number, hi: number): Promise<number> {
  for (let i = 0; i < 14 && hi - lo > SPAN * 2; i++) {
    const mid = Math.floor((lo + hi) / 2);
    const { hasKind, hasTxs } = await probeRange(base, dataset, apiKey, "traces", mid);
    if (hasKind) hi = mid;
    else if (hasTxs) lo = mid; // activity but no traces → cutoff is later
    else lo = mid; // quiet region → move up
  }
  return hi;
}

/** Probe a chain for trace/receipt/stateDiff availability. `needTraceCutoff` triggers
 * the (more expensive) binary search only when a trace source exists on this chain. */
export async function probeChainFeatures(
  base: string, apiKey: string | undefined, dataset: string,
  startBlock: number, head: number, opts: { traces: boolean; receipts: boolean } = { traces: true, receipts: true },
): Promise<ChainFeatures> {
  const recent = Math.max(startBlock, head - SPAN - 10);
  let traces = false, tracesFromBlock: number | undefined;
  if (opts.traces) {
    const atRecent = await probeRange(base, dataset, apiKey, "traces", recent);
    traces = atRecent.hasKind;
    if (traces) {
      // do traces also exist at the source's start? if not, find the cutoff.
      const atStart = await probeRange(base, dataset, apiKey, "traces", startBlock);
      if (!atStart.hasKind && atStart.hasTxs) tracesFromBlock = await firstTraceBlock(base, dataset, apiKey, startBlock, recent);
    }
  }
  const receipts = opts.receipts ? (await probeRange(base, dataset, apiKey, null, recent)).hasTxs : true;
  const stateDiffs = (await probeRange(base, dataset, apiKey, "stateDiffs", recent)).hasKind;
  return { traces, tracesFromBlock, receipts, stateDiffs };
}
