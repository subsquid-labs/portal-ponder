import { createConfig, factory } from '@subsquid/ponder';
import { parseAbiItem } from 'abitype';
import { EVaultAbi } from './abis/EVault';

// Euler V2 subgraph → Ponder. The subgraph's GenericFactory template (ProxyCreated → EVault)
// becomes a Ponder log-factory; backfill is routed through SQD Portal via `portal:`.
const proxyCreated = parseAbiItem(
  'event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)',
);

// Zero-config defaults so `npm run dev` works from a fresh clone with no .env:
//  - portal: defaults to the free public Portal (historical backfill). Leaving this unset would
//    SILENTLY fall back to stock RPC historical sync — the demo would not use the Portal at all.
//  - rpc: defaults to a keyless *archive* public RPC (realtime tip + the readContract
//    vault-metadata calls, which read historical state). Rate-limits under load; set
//    PONDER_RPC_URL_1 to your own for real work.
//  - endBlock: defaults to a short window (~91k blocks) so the demo finishes in ~1-2 min. Set
//    PONDER_END to backfill further.
const START = Number(process.env.PONDER_START ?? 20_529_207); // subgraph mainnet startBlock
const END = process.env.PONDER_END
  ? Number(process.env.PONDER_END)
  : START + 91_000;

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      // realtime tip + the readContract vault-metadata calls. Defaults to a keyless *archive*
      // public RPC (drpc.org) — the vault reads happen at historical blocks, so a non-archive
      // node rejects them. Rate-limited under load; set PONDER_RPC_URL_1 to your own for real work.
      rpc: process.env.PONDER_RPC_URL_1 ?? 'https://eth.drpc.org',
      // ← historical backfill from the free public Portal (drop-in @subsquid/ponder)
      portal:
        process.env.PORTAL_URL_1 ??
        'https://portal.sqd.dev/datasets/ethereum-mainnet',
    },
  },
  contracts: {
    EVault: {
      abi: EVaultAbi,
      chain: 'mainnet',
      address: factory({
        // GenericFactory (eVaultFactory) — the subgraph's `EulerVaultFactory` data source
        address: '0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e',
        event: proxyCreated,
        parameter: 'proxy', // child EVault address (subgraph: event.params.proxy)
      }),
      startBlock: START,
      endBlock: END,
    },
  },
});
