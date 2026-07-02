import { createConfig, factory } from "@subsquid/ponder";
import { parseAbiItem } from "abitype";
import { http } from "viem";
import { EVaultAbi } from "./abis/EVault";
import chainsData from "./chains.json";

// SINGLE multichain Euler app — every Portal-supported Euler chain in ONE indexer, exactly how Euler
// runs in production: eVaultFactory (ProxyCreated → child EVaults) + the full 24-event EVault superset.
//
// Historical backfill is ALWAYS the Portal. The realtime source is a config choice:
//   • bench mode (default): bounded [deploy, head] — a clean fixed-range benchmark; RPC only for setup.
//   • EULER_REALTIME=true:  unbounded → live, realtime served by an authenticated RPC (URL + x-api-key
//     from env). Ponder owns reorg handling and is agnostic to what the RPC is or how it's implemented.
//
// Portal base URLs are deployment config (from env); relative to a base the documented Portal API paths
// apply — /datasets/<>, /metadata, … (see docs.sqd.dev). Public examples default to the public Portal.
//   PORTAL_URL        = Portal base (default https://portal.sqd.dev — the public, rate-limited Portal)
//   REALTIME_RPC_URL  = authenticated realtime RPC base (with REALTIME_RPC_KEY, from env)
// One config serves any deployment by swapping env. Keyless public RPCs (chains.json `freeRpcs`) are only
// for setup / finality tail — excluded from the realtime path (403/timeout cascades stall it).
//
// No secrets/tenancy in the repo: PORTAL_API_KEY, PORTAL_URL, REALTIME_RPC_KEY, REALTIME_RPC_URL come from env.
const proxyCreated = parseAbiItem(
  "event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)",
);

type ChainRow = {
  id: number;
  name: string;
  ds: string;
  factory: string;
  head: number;
  deploy: number;
  sqdSlug: string | null;
  freeRpcs: string[];
};
const rows = chainsData as ChainRow[];
const PORTAL_URL = process.env.PORTAL_URL ?? "https://portal.sqd.dev";
const REALTIME_RPC_KEY = process.env.REALTIME_RPC_KEY;
const REALTIME_RPC_URL = process.env.REALTIME_RPC_URL; // realtime RPC base (from env; provisioned per client)
const REALTIME = process.env.EULER_REALTIME === "true";

// Chains served by the realtime RPC. Realtime uses it (alone) on these.
const REALTIME_CHAINS = new Set([1, 42161, 8453, 43114, 137, 56, 9745, 143]);
const realtimeRpc = (chainId: number) =>
  http(`${REALTIME_RPC_URL}/${chainId}`, {
    fetchOptions: { headers: { "x-api-key": REALTIME_RPC_KEY ?? "" } },
  });

// realtime → the realtime RPC ALONE (the endpoint under evaluation): no generic proxy, no flaky
// keyless public fallback (their 403/timeout cascades stall the single-thread). bench → public RPCs only.
const rpcFor = (c: ChainRow) =>
  REALTIME && REALTIME_RPC_KEY && REALTIME_RPC_URL && REALTIME_CHAINS.has(c.id)
    ? realtimeRpc(c.id)
    : c.freeRpcs;

// optional subset: EULER_CHAINS=ethereum,base limits the run. In realtime mode with no explicit subset,
// default to the realtime-RPC chains (the 8 that have a realtime source).
const only = (process.env.EULER_CHAINS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
let active = only.length ? rows.filter((c) => only.includes(c.name)) : rows;
if (REALTIME && !only.length)
  active = rows.filter((c) => REALTIME_CHAINS.has(c.id));

export default createConfig({
  // Postgres (production shape, separate process → own memory) when DATABASE_URL is set; else pglite.
  database: process.env.DATABASE_URL
    ? { kind: "postgres", connectionString: process.env.DATABASE_URL }
    : {
        kind: "pglite",
        directory: process.env.PGLITE_DIR ?? "./.ponder/pglite",
      },
  chains: Object.fromEntries(
    active.map((c) => [
      c.name,
      { id: c.id, rpc: rpcFor(c), portal: `${PORTAL_URL}/datasets/${c.ds}` },
    ]),
  ),
  contracts: {
    EVault: {
      abi: EVaultAbi,
      includeTransactionReceipts: process.env.INCLUDE_RECEIPTS === "true",
      chain: Object.fromEntries(
        active.map((c) => [
          c.name,
          // realtime: no endBlock → backfill to finalized head, then go live on the realtime RPC.
          {
            address: factory({
              address: c.factory as `0x${string}`,
              event: proxyCreated,
              parameter: "proxy",
            }),
            startBlock: c.deploy,
            ...(REALTIME ? {} : { endBlock: c.head }),
          },
        ]),
      ),
    },
  },
});
