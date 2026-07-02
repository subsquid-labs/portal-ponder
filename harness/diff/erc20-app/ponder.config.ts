import { createConfig } from "@subsquid/ponder";
import { parseAbiItem } from "abitype";

// High-volume SINGLE-contract log indexer: USDC Transfer + receipts. Tests dense per-block log
// volume + the receipt path (no factory), a different shape than the factory benches.
const transfer = parseAbiItem(
  "event Transfer(address indexed from, address indexed to, uint256 value)",
);

export default createConfig({
  database: {
    kind: "pglite",
    directory: process.env.PGLITE_DIR ?? "./.ponder/pglite",
  },
  chains: {
    mainnet: {
      id: 1,
      rpc: (process.env.PONDER_RPC_URL_1 ?? "").includes(",")
        ? process.env.PONDER_RPC_URL_1.split(",")
            .map((x) => x.trim())
            .filter(Boolean)
        : process.env.PONDER_RPC_URL_1,
      portal: process.env.PORTAL_URL_1 || undefined,
    },
  },
  contracts: {
    USDC: {
      abi: [transfer] as const,
      chain: "mainnet",
      address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
      includeTransactionReceipts: true,
      startBlock: Number(process.env.PONDER_START ?? 22_200_000),
      endBlock: process.env.PONDER_END
        ? Number(process.env.PONDER_END)
        : undefined,
    },
  },
});
