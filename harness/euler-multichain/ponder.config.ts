import { parseAbiItem } from "abitype";
import { createConfig, factory } from "@subsquid/ponder";
import { EVaultAbi } from "./abis/EVault";
import chainsData from "./chains.json";

// SINGLE multichain Euler app — every Portal-supported Euler chain in ONE indexer, exactly how
// Euler runs in production. eVaultFactory (ProxyCreated → child EVaults) + the 6 EVault events,
// full history [0, finalized head] per chain. Portal does the backfill; the RPC (SQD-first where
// available, then a round-robin of keyless public RPCs) is only for chain setup / the finality tail.
//
// No secrets in the repo: SQD_RPC_KEY + PORTAL_API_KEY come from the environment. chains.json holds
// only public data (dataset name, factory, head, SQD slug, keyless public RPCs).
const proxyCreated = parseAbiItem(
  "event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)",
);

type ChainRow = { id: number; name: string; ds: string; factory: string; head: number; sqdSlug: string | null; freeRpcs: string[] };
const rows = chainsData as ChainRow[];
const SQD_KEY = process.env.SQD_RPC_KEY;

const rpcFor = (c: ChainRow): string[] =>
  [c.sqdSlug && SQD_KEY ? `https://rpc.subsquid.io/${c.sqdSlug}/${SQD_KEY}` : null, ...c.freeRpcs].filter(Boolean) as string[];

// optional subset: EULER_CHAINS=ethereum,base limits the run (default = all)
const only = (process.env.EULER_CHAINS ?? "").split(",").map((s) => s.trim()).filter(Boolean);
const active = only.length ? rows.filter((c) => only.includes(c.name)) : rows;

export default createConfig({
  // Postgres (production shape, separate process → own memory) when DATABASE_URL is set; else pglite
  // (in-process dev DB — its write backlog shares the Node heap, so a fast 15-chain backfill can OOM it).
  database: process.env.DATABASE_URL
    ? { kind: "postgres", connectionString: process.env.DATABASE_URL }
    : { kind: "pglite", directory: process.env.PGLITE_DIR ?? "./.ponder/pglite" },
  chains: Object.fromEntries(active.map((c) => [c.name, { id: c.id, rpc: rpcFor(c), portal: `https://sqd.portal.sqd.dev/datasets/${c.ds}` }])),
  contracts: {
    EVault: {
      abi: EVaultAbi,
      includeTransactionReceipts: process.env.INCLUDE_RECEIPTS === "true",
      chain: Object.fromEntries(
        active.map((c) => [
          c.name,
          { address: factory({ address: c.factory as `0x${string}`, event: proxyCreated, parameter: "proxy" }), startBlock: 0, endBlock: c.head },
        ]),
      ),
    },
  },
});
