import { parseAbiItem } from "abitype";
import { createConfig, factory } from "ponder";
import { EVaultAbi } from "./abis/EVault";

// Euler V2 subgraph → Ponder. The subgraph's GenericFactory template (ProxyCreated → EVault)
// becomes a Ponder log-factory; backfill is routed through SQD Portal via `portal:`.
const proxyCreated = parseAbiItem(
  "event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)",
);

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1, // realtime + the readContract vault-metadata calls
      portal: process.env.PORTAL_URL_1, // ← historical backfill from Portal (drop-in @subsquid/ponder)
    },
  },
  contracts: {
    EVault: {
      abi: EVaultAbi,
      chain: "mainnet",
      address: factory({
        // GenericFactory (eVaultFactory) — the subgraph's `EulerVaultFactory` data source
        address: "0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e",
        event: proxyCreated,
        parameter: "proxy", // child EVault address (subgraph: event.params.proxy)
      }),
      startBlock: Number(process.env.PONDER_START ?? 20_529_207), // subgraph mainnet startBlock
      endBlock: process.env.PONDER_END ? Number(process.env.PONDER_END) : undefined,
    },
  },
});
