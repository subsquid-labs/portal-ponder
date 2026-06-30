/**
 * portalTransport — a viem Transport that serves Ponder's backfill `eth_getLogs`
 * and `eth_getBlockByNumber` from SQD Portal, and falls back to a real RPC for
 * everything else (receipts, traces, state reads, realtime/by-hash).
 *
 * This is the CONFIG-ONLY delivery (rung 2 of the migration): no fork, no patch,
 * works on any Ponder version, just
 *
 *   chains: { mainnet: { id: 1, rpc: portalTransport({
 *     dataset: "https://portal.sqd.dev/datasets/ethereum-mainnet",
 *     fallbackRpc: process.env.PONDER_RPC_URL_1!,
 *   }) } }
 *
 * Trade-off vs the native `createPortalHistoricalSync` seam: the orchestrator
 * still drives per-request granularity, so this can't reach the native ~8×. But
 * a read-ahead range cache (one Portal stream over a getLogs window also warms
 * the per-block getBlockByNumber lookups it implies) recovers most of it, and
 * unlike the eRPC+rust shim it runs in-process with nothing to deploy.
 */
import { custom, numberToHex } from "viem";
import { PortalClient } from "./portal-client.ts";
import { buildPortalQuery, type LogFilter } from "./query.ts";
import { toSyncLog, toSyncBlock, toSyncTransaction } from "./transform.ts";

export type PortalTransportOptions = {
  /** full dataset URL, e.g. https://portal.sqd.dev/datasets/ethereum-mainnet */
  dataset: string;
  /** real JSON-RPC endpoint for everything Portal doesn't serve + realtime */
  fallbackRpc: string;
  apiKey?: string;
  /** cap on cached blocks (LRU). Default 50k. */
  maxCachedBlocks?: number;
};

const hexToNum = (h: string | number): number => (typeof h === "number" ? h : Number.parseInt(h, 16));

export function portalTransport(opts: PortalTransportOptions) {
  const { baseUrl, slug } = splitDatasetUrl(opts.dataset);
  const client = new PortalClient({ baseUrl, dataset: slug, apiKey: opts.apiKey });
  const cap = opts.maxCachedBlocks ?? 50_000;
  const blockCache = new Map<number, { block: any; txs: any[] }>();
  let finalizedHead = -1;

  const rpc = async (method: string, params: any[]) => {
    const res = await fetch(opts.fallbackRpc, {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    });
    const json = await res.json();
    if (json.error) throw Object.assign(new Error(json.error.message), { code: json.error.code });
    return json.result;
  };

  const getFinalizedHead = async () => {
    if (finalizedHead < 0) finalizedHead = (await client.getFinalizedHead())?.number ?? 0;
    return finalizedHead;
  };

  const cacheBlock = (n: number, block: any, txs: any[]) => {
    if (blockCache.size >= cap) blockCache.delete(blockCache.keys().next().value!);
    blockCache.set(n, { block, txs });
  };

  const handlers: Record<string, (params: any[]) => Promise<any>> = {
    // logs: serve the range from Portal; warm the block/tx cache for the matched blocks
    async eth_getLogs([f]) {
      if (f.blockHash) return rpc("eth_getLogs", [f]); // by-hash → reorg tail, not Portal's job
      const from = hexToNum(f.fromBlock ?? "0x0");
      const to = hexToNum(f.toBlock ?? numberToHex(await getFinalizedHead()));
      if (to > (await getFinalizedHead())) return rpc("eth_getLogs", [f]); // above finalized → RPC
      const filter: LogFilter = { address: f.address, topic0: f.topics?.[0], topic1: f.topics?.[1], topic2: f.topics?.[2], topic3: f.topics?.[3], includeTransaction: true };
      const query = buildPortalQuery([from, to], [filter], { receipts: false });
      const out: any[] = [];
      for await (const batch of client.streamFinalized(query)) {
        for (const b of batch.blocks) {
          if (b.transactions?.length || b.logs?.length) cacheBlock(b.header.number, b, b.transactions ?? []);
          for (const log of b.logs ?? []) out.push(toSyncLog(log, b.header));
        }
      }
      return out;
    },
    // block by number: serve from the cache warmed by eth_getLogs; else fall back
    async eth_getBlockByNumber([tag, fullTx]) {
      if (typeof tag !== "string" || !tag.startsWith("0x")) return rpc("eth_getBlockByNumber", [tag, fullTx]); // latest/finalized → RPC
      const n = hexToNum(tag);
      const hit = blockCache.get(n);
      if (!hit) return rpc("eth_getBlockByNumber", [tag, fullTx]);
      const block: any = toSyncBlock(hit.block);
      block.transactions = fullTx ? hit.txs.map((t) => toSyncTransaction(t, hit.block.header)) : hit.txs.map((t) => t.hash);
      return block;
    },
  };

  const provider = {
    request: async ({ method, params }: { method: string; params?: any[] }) => {
      const h = handlers[method];
      try {
        return h ? await h(params ?? []) : await rpc(method, params ?? []);
      } catch (err) {
        // any Portal hiccup → degrade to RPC rather than failing the sync
        if (h) return rpc(method, params ?? []);
        throw err;
      }
    },
  };
  return custom(provider, { key: "sqd-portal", name: "SQD Portal", retryCount: 0 });
}

function splitDatasetUrl(url: string): { baseUrl: string; slug: string } {
  const u = url.replace(/\/$/, "");
  const i = u.lastIndexOf("/");
  return { baseUrl: u.slice(0, i), slug: u.slice(i + 1) };
}
