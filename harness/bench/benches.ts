/**
 * The benchmark base: real/representative Ponder indexers, each run as a bounded Portal
 * backfill. Apps live in the patched-ponder workspace (so they link the Portal-fork core);
 * point PONDER_EXAMPLES at that workspace's examples/ dir. Secrets (PORTAL_API_KEY, SQD RPC
 * URLs) come from the environment at run time — never hard-coded here. PORTAL_URL datasets
 * are public hostnames and safe to list.
 *
 * The runner sizes each chunk to the bench range, so numbers are the PURE backfill of that
 * range (chunk over-fetch — which amortizes over a full multi-interval backfill — is removed).
 */
import { join } from "node:path";
import type { BenchSpec } from "./run-bench.ts";

const EX =
  process.env.PONDER_EXAMPLES ??
  "/private/tmp/claude-501/-Users-dz-Projects/13199370-953e-47b4-8270-46346fb77a26/scratchpad/ponder/examples";
const ETH = "https://portal.sqd.dev/datasets/ethereum-mainnet";
const BASE = "https://portal.sqd.dev/datasets/base-mainnet";

export const BENCHES: BenchSpec[] = [
  // synthetic all-in-one: the only bench exercising all five source types (logs + receipts
  // + traces + block-interval + accounts) — real Uniswap contracts, one chain.
  {
    name: "uniswap-portal (all-sources)",
    dir: join(EX, "uniswap-portal"),
    schema: "bench_uni",
    port: 42201,
    start: 22_200_000,
    end: 22_205_000,
    chainIds: [1],
    env: { PORTAL_URL: ETH },
  },
  // 10x range — heavy/dense (V3 USDC/WETH swaps + V2 Router traces are always busy): scaling + memory-under-load
  {
    name: "uniswap-portal HEAVY (50k blk)",
    dir: join(EX, "uniswap-portal"),
    schema: "bench_uniheavy",
    port: 42211,
    start: 22_200_000,
    end: 22_250_000,
    chainIds: [1],
    env: { PORTAL_URL: ETH },
    timeoutMin: 12,
  },

  // first-party per-source-type benches (Ponder's own examples), Portal-routed
  {
    name: "feature-call-traces (traces)",
    dir: join(EX, "feature-call-traces"),
    schema: "bench_traces",
    port: 42202,
    start: 22_200_000,
    end: 22_205_000,
    chainIds: [1],
    env: { PORTAL_URL_1: ETH },
  },
  {
    name: "feature-blocks (block-interval)",
    dir: join(EX, "feature-blocks"),
    schema: "bench_blocks",
    port: 42203,
    start: 22_200_000,
    end: 22_210_000,
    chainIds: [1],
    env: { PORTAL_URL_1: ETH },
  },
  // real lending protocol via log-factory: Euler v2 EVault proxies discovered from the
  // factory's ProxyCreated, indexed from deploy (dense child activity right after).
  {
    name: "euler-mainnet (factory+lending)",
    dir: join(EX, "euler-mainnet"),
    schema: "bench_euler",
    port: 42204,
    start: 20_429_973,
    end: 20_490_000,
    chainIds: [1],
    env: { PORTAL_URL_1: ETH },
  },

  // real third-party production indexer: Uniswap v4 on Base (singleton swaps + token
  // metadata readContract). Bounded; reveals whether reads (RPC) bottleneck the backfill.
  {
    name: "v4-ponder (uniswap v4, base)",
    dir: join(EX, "v4-ponder"),
    schema: "bench_v4",
    port: 42208,
    start: 25_350_988,
    end: 25_400_000,
    chainIds: [8453],
    env: { PORTAL_URL_8453: BASE },
    timeoutMin: 12,
  },
];
