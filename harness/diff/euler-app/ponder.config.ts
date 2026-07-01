import { parseAbiItem } from "abitype";
import { createConfig, factory } from "@subsquid/ponder";
import { EVaultAbi } from "./abis/EVault";

// Larger byte-identity + wall-clock diff app: the real Euler V2 factory (eVaultFactory
// ProxyCreated → child EVaults) + all 6 indexed EVault events + receipts. No-op handlers (below)
// so the run measures PURE backfill (factory discovery + multi-source child logs + txs + receipts),
// not the readContract indexing the full example does. Env-driven so run.sh can dual-run it.
const proxyCreated = parseAbiItem(
  "event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)",
);

export default createConfig({
  database: { kind: "pglite", directory: process.env.PGLITE_DIR ?? "./.ponder/pglite" },
  chains: { mainnet: { id: Number(process.env.CHAIN_ID ?? 1), rpc: (process.env.PONDER_RPC_URL_1 ?? "").includes(",") ? process.env.PONDER_RPC_URL_1.split(",").map((x)=>x.trim()).filter(Boolean) : process.env.PONDER_RPC_URL_1, portal: process.env.PORTAL_URL_1 || undefined } },
  contracts: {
    EVault: {
      abi: EVaultAbi,
      chain: "mainnet",
      address: factory({ address: (process.env.EULER_FACTORY ?? "0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e") as `0x${string}`, event: proxyCreated, parameter: "proxy" }),
      includeTransactionReceipts: process.env.INCLUDE_RECEIPTS !== "false", // also diff the receipt path at scale
      startBlock: Number(process.env.PONDER_START ?? 20_529_207),
      endBlock: process.env.PONDER_END ? Number(process.env.PONDER_END) : undefined,
    },
  },
});
