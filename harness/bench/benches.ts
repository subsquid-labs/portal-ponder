/**
 * The benchmark base: real/representative Ponder indexers, each run as a bounded Portal
 * backfill. Apps live in the patched-ponder workspace (so they link the Portal-fork core);
 * point PONDER_EXAMPLES at that workspace's examples/ dir. Secrets (PORTAL_API_KEY, SQD RPC
 * URLs) come from the environment at run time — never hard-coded here. PORTAL_URL datasets
 * are public hostnames and safe to list.
 */
import { join } from "node:path";
import type { BenchSpec } from "./run-bench.ts";

const EX = process.env.PONDER_EXAMPLES ?? "/private/tmp/claude-501/-Users-dz-Projects/13199370-953e-47b4-8270-46346fb77a26/scratchpad/ponder/examples";
const ETH = "https://sqd.portal.sqd.dev/datasets/ethereum-mainnet";
const BASE = "https://sqd.portal.sqd.dev/datasets/base-mainnet";
const SEPOLIA = "https://sqd.portal.sqd.dev/datasets/ethereum-sepolia";

export const BENCHES: BenchSpec[] = [
  // all five source types in one app (logs + receipts + traces + block-interval + accounts)
  { name: "uniswap-portal (all-sources)", dir: join(EX, "uniswap-portal"), schema: "bench_uni", port: 42201,
    start: 22_200_000, end: 22_205_000, chainIds: [1], env: { PORTAL_URL: ETH } },

  // first-party per-source-type benches (Ponder's own examples), Portal-routed
  { name: "feature-call-traces", dir: join(EX, "feature-call-traces"), schema: "bench_traces", port: 42202,
    start: 22_200_000, end: 22_205_000, chainIds: [1], env: { PORTAL_URL_1: ETH } },
  { name: "feature-blocks", dir: join(EX, "feature-blocks"), schema: "bench_blocks", port: 42203,
    start: 22_200_000, end: 22_220_000, chainIds: [1], env: { PORTAL_URL_1: ETH } },
  { name: "feature-accounts", dir: join(EX, "feature-accounts"), schema: "bench_accts", port: 42204,
    start: 22_200_000, end: 22_205_000, chainIds: [1], env: { PORTAL_URL_1: ETH } },
  { name: "feature-factory (sepolia)", dir: join(EX, "feature-factory"), schema: "bench_factory", port: 42205,
    start: 4_121_269, end: 4_200_000, chainIds: [11155111], env: { PORTAL_URL_11155111: SEPOLIA } },

  // real production apps
  { name: "euler-portal (lending)", dir: join(EX, "euler-portal"), schema: "bench_euler", port: 42206,
    start: 20_500_000, end: 20_560_000, chainIds: [1], env: { PORTAL_URL: ETH } },
  { name: "friendtech (base factory)", dir: join(EX, "project-friendtech"), schema: "bench_ft", port: 42207,
    start: 2_430_000, end: 2_460_000, chainIds: [8453], env: { PORTAL_URL_8453: BASE } },
];
