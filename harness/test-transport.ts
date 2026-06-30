import { portalTransport } from "../packages/portal-sync/src/transport.ts";

const t = portalTransport({
  dataset: "https://portal.sqd.dev/datasets/base-mainnet",
  fallbackRpc: "https://mainnet.base.org",
});
const { request } = (t as any)({}); // instantiate the viem transport

// eth_getLogs over a finalized range → served by Portal
const WETH = "0x4200000000000000000000000000000000000006";
const TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const logs = await request({ method: "eth_getLogs", params: [{ address: WETH, topics: [TRANSFER], fromBlock: "0x1c9c380", toBlock: "0x1c9c38a" }] });
console.log(`eth_getLogs(base WETH Transfer, [30000000,30000010]) via portalTransport → ${logs.length} logs`);
console.log(`  sample:`, JSON.stringify(logs[0]).slice(0, 160));

// eth_getBlockByNumber for a matched block → served from the cache warmed above (no RPC)
const bn = logs[0].blockNumber;
const block = await request({ method: "eth_getBlockByNumber", params: [bn, false] });
console.log(`eth_getBlockByNumber(${bn}, false) → block ${block.number}, hash ${String(block.hash).slice(0, 14)}…, ${block.transactions.length} txs (cache hit)`);

// a non-Portal method → transparently falls back to RPC
const chainId = await request({ method: "eth_chainId", params: [] });
console.log(`eth_chainId (fallback RPC) → ${chainId}`);
console.log(logs.length === 380 ? "\n✅ matches the differential (380 logs) — Portal-served getLogs is correct" : `\n⚠ expected 380, got ${logs.length}`);
