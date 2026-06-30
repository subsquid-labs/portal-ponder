/** chainId → SQD Portal dataset slug. Verified live via the /datasets catalog at runtime. */
export const CHAIN_TO_DATASET: Record<number, string> = {
  1: "ethereum-mainnet",
  10: "optimism-mainnet",
  56: "binance-mainnet",
  130: "unichain-mainnet",
  137: "polygon-mainnet",
  143: "monad-mainnet",
  146: "sonic-mainnet",
  239: "tac-mainnet",
  250: "fantom-mainnet",
  480: "worldchain-mainnet",
  999: "hyperliquid-mainnet",
  8453: "base-mainnet",
  9745: "plasma-mainnet",
  42161: "arbitrum-one",
  42220: "celo-mainnet",
  43114: "avalanche-mainnet",
  59144: "linea-mainnet",
  60808: "bob-mainnet",
  80094: "berachain-mainnet",
  534352: "scroll-mainnet",
  11155111: "ethereum-sepolia",
  84532: "base-sepolia",
  421614: "arbitrum-sepolia",
};

export type DatasetInfo = { dataset: string; realTime: boolean; aliases: string[] };

/** Fetch the live catalog so we report actual availability (not a stale map). */
export async function fetchCatalog(baseUrl: string, apiKey?: string): Promise<Map<string, DatasetInfo>> {
  const headers: Record<string, string> = {};
  if (apiKey) headers["x-api-key"] = apiKey;
  const res = await fetch(`${baseUrl.replace(/\/datasets$/, "")}/datasets`, { headers }).catch(() => null);
  const map = new Map<string, DatasetInfo>();
  if (res?.ok) {
    for (const d of (await res.json()) as any[]) {
      map.set(d.dataset, { dataset: d.dataset, realTime: !!d.real_time, aliases: d.aliases ?? [] });
      for (const a of d.aliases ?? []) map.set(a, { dataset: d.dataset, realTime: !!d.real_time, aliases: d.aliases ?? [] });
    }
  }
  return map;
}

export const datasetForChain = (chainId: number): string | undefined => CHAIN_TO_DATASET[chainId];
