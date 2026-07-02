import { parseAbiItem } from "abitype";
import { createConfig, factory } from "@subsquid/ponder";
import { http, fallback } from "viem";
import { EVaultAbi } from "./abis/EVault";
import chainsData from "./chains.json";

// SINGLE multichain Euler app — every Portal-supported Euler chain in ONE indexer, exactly how Euler
// runs in production: eVaultFactory (ProxyCreated → child EVaults) + the full 24-event EVault superset.
//
// Historical backfill is ALWAYS the Portal. The realtime source is a config choice:
//   • bench mode (default): bounded [deploy, head] — a clean fixed-range benchmark; RPC only for setup.
//   • EULER_REALTIME=true:  unbounded → live. The Portal-backed RPC (euler.portal.sqd.dev/rpc/v1/evm —
//     the realtime service offered to Euler alongside Portal + Ponder, Portal-backed under the hood)
//     LEADS realtime, with public RPCs as fallback for downtime / tip-lag. Ponder owns reorg handling
//     either way (RealtimeSync: parent-hash tracking + rollback to the finalized common ancestor).
//
// The plain `rpc` list is GENERIC — rpc.subsquid.io/<slug> is just a fast keyed RPC (like Alchemy),
// nothing special. The Portal-backed RPC is the distinct, Portal-served product.
//
// No secrets in the repo: PORTAL_API_KEY (backfill), PORTAL_RPC_KEY (realtime), SQD_RPC_KEY come from env.
const proxyCreated = parseAbiItem(
  "event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)",
);

type ChainRow = { id: number; name: string; ds: string; factory: string; head: number; deploy: number; sqdSlug: string | null; freeRpcs: string[] };
const rows = chainsData as ChainRow[];
const SQD_KEY = process.env.SQD_RPC_KEY;
const PORTAL_RPC_KEY = process.env.PORTAL_RPC_KEY;
const REALTIME = process.env.EULER_REALTIME === "true";

// Chains served by the Portal-backed RPC (euler.portal.sqd.dev/rpc/v1/evm). Realtime leads with it here.
const PORTAL_RPC_CHAINS = new Set([1, 42161, 8453, 43114, 137, 56, 9745, 143]);
const portalRpc = (chainId: number) =>
  http(`https://euler.portal.sqd.dev/rpc/v1/evm/${chainId}`, { fetchOptions: { headers: { "x-api-key": PORTAL_RPC_KEY ?? "" } } });

// generic RPC list: a fast keyed proxy (if configured) + keyless public RPCs — for setup / finality tail,
// and as the realtime fallback behind the Portal-backed RPC.
const genericRpcs = (c: ChainRow): string[] =>
  [c.sqdSlug && SQD_KEY ? `https://rpc.subsquid.io/${c.sqdSlug}/${SQD_KEY}` : null, ...c.freeRpcs].filter(Boolean) as string[];

// realtime → Portal-backed RPC preferred + generic fallback; bench → the plain generic list.
const rpcFor = (c: ChainRow) =>
  REALTIME && PORTAL_RPC_KEY && PORTAL_RPC_CHAINS.has(c.id)
    ? fallback([portalRpc(c.id), ...genericRpcs(c).map((u) => http(u))])
    : genericRpcs(c);

// optional subset: EULER_CHAINS=ethereum,base limits the run. In realtime mode with no explicit subset,
// default to the Portal-backed-RPC chains (the 8 that have a realtime source).
const only = (process.env.EULER_CHAINS ?? "").split(",").map((s) => s.trim()).filter(Boolean);
let active = only.length ? rows.filter((c) => only.includes(c.name)) : rows;
if (REALTIME && !only.length) active = rows.filter((c) => PORTAL_RPC_CHAINS.has(c.id));

export default createConfig({
  // Postgres (production shape, separate process → own memory) when DATABASE_URL is set; else pglite.
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
          // realtime: no endBlock → backfill to finalized head, then go live on the realtime source.
          { address: factory({ address: c.factory as `0x${string}`, event: proxyCreated, parameter: "proxy" }), startBlock: c.deploy, ...(REALTIME ? {} : { endBlock: c.head }) },
        ]),
      ),
    },
  },
});
