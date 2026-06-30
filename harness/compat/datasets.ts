/**
 * Two authoritative sources, kept separate on purpose:
 *
 * 1. CAPABILITIES (which networks have traces / stateDiffs / realtime, + block-range
 *    caveats) come from the SQD docs matrix, snapshotted in networks.json. The Portal
 *    API does NOT expose these flags. Refresh with fetch-networks.ts.
 *
 * 2. EXISTENCE (which datasets a portal actually serves) is PER-PORTAL — different
 *    portals serve different subsets — so we query the TARGET portal's /datasets live
 *    (fetchCatalog) rather than assume the docs list is what this portal has.
 */
import { createRequire } from "node:module";

const snapshot = createRequire(import.meta.url)("./networks.json") as { _source: string; networks: Record<string, NetworkCaps> };

export const ALL_NETWORKS_DOCS = snapshot._source;

export type NetworkCaps = { name: string; slug: string; type: string; portal: boolean; realtime: boolean; traces: boolean; stateDiffs: boolean; note?: string };

/** chainId → docs capabilities (authoritative; from networks.json). */
export const NETWORKS: Record<number, NetworkCaps> = snapshot.networks as any;

export const datasetForChain = (chainId: number): string | undefined => NETWORKS[chainId]?.slug;
export const capsForChain = (chainId: number): NetworkCaps | undefined => NETWORKS[chainId];

export type DatasetInfo = { dataset: string; realTime: boolean; aliases: string[] };

/** Live per-portal catalog — what THIS portal serves (existence is per-portal). */
export async function fetchCatalog(baseUrl: string, apiKey?: string): Promise<Map<string, DatasetInfo>> {
  const headers: Record<string, string> = {};
  if (apiKey) headers["x-api-key"] = apiKey;
  const res = await fetch(`${baseUrl.replace(/\/datasets$/, "")}/datasets`, { headers }).catch(() => null);
  const map = new Map<string, DatasetInfo>();
  if (res?.ok) {
    for (const d of (await res.json()) as any[]) {
      const info = { dataset: d.dataset, realTime: !!d.real_time, aliases: d.aliases ?? [] };
      map.set(d.dataset, info);
      for (const a of d.aliases ?? []) map.set(a, info);
    }
  }
  return map;
}
