import assert from "node:assert";
import { test } from "node:test";
import { getPortalDataset, withPortal } from "../../packages/portal-sync/src/config.ts";
import { analyzeConfig } from "./analyze.ts";

// Fixture: one config exercising every verdict branch. Empty catalog → static chain map trusted.
const FIXTURE = {
  chains: { eth: { id: 1 }, weird: { id: 999_999 } },
  contracts: {
    Factory: { chain: "eth", address: { event: {}, parameter: "proxy" } }, // log factory
    WithReceipts: { chain: "eth", address: "0xabc", includeTransactionReceipts: true },
    WithTraces: { chain: "eth", address: "0xabc", includeCallTraces: true },
    NoDataset: { chain: "weird", address: "0xabc" },
  },
  blocks: { Blk: { chain: "eth", interval: 100 } },
};

test("analyzeConfig: verdict per source type", () => {
  const r = analyzeConfig(FIXTURE, new Map());
  const v = (name: string) => r.sources.find((s) => s.source === name)?.verdict;
  assert.equal(v("Factory"), "READY"); // logs + tx + factory
  assert.equal(v("WithReceipts"), "READY"); // receipts now supported
  assert.equal(v("WithTraces"), "READY"); // traces now supported
  assert.equal(v("NoDataset"), "NO_DATASET");
  assert.equal(v("block:Blk"), "NEEDS_BLOCK_FILTER"); // block-interval still todo
  assert.equal(r.overall, "PARTIAL");
  assert.equal(r.ready, 3);
});

test("analyzeConfig: factory detected on the contract address", () => {
  const r = analyzeConfig(FIXTURE, new Map());
  assert.ok(r.sources.find((s) => s.source === "Factory")!.needs.includes("logFactory"));
});

test("per-range: trace source in the chain's trace-less range is flagged (Arbitrum ancient-blocks case)", () => {
  const cfg = { chains: { arbitrum: { id: 42161 } }, contracts: { OldTraced: { chain: "arbitrum", address: "0xabc", includeCallTraces: true, startBlock: 1_000_000 } } };
  const features = new Map([[42161, { traces: true, tracesFromBlock: 200_000_000, receipts: true, stateDiffs: false }]]);
  const s = analyzeConfig(cfg, new Map(), features as any).sources[0]!;
  assert.equal(s.verdict, "NEEDS_TRACES");
  assert.ok(s.blockers.some((b) => b.includes("trace-less range")));
});

test("per-range: same source ABOVE the trace cutoff is READY", () => {
  const cfg = { chains: { arbitrum: { id: 42161 } }, contracts: { NewTraced: { chain: "arbitrum", address: "0xabc", includeCallTraces: true, startBlock: 300_000_000 } } };
  const features = new Map([[42161, { traces: true, tracesFromBlock: 200_000_000, receipts: true, stateDiffs: false }]]);
  assert.equal(analyzeConfig(cfg, new Map(), features as any).sources[0]!.verdict, "READY");
});

test("per-chain: a chain where Portal serves no traces flags trace sources", () => {
  const cfg = { chains: { c: { id: 1 } }, contracts: { T: { chain: "c", address: "0xabc", includeCallTraces: true } } };
  const features = new Map([[1, { traces: false, receipts: true, stateDiffs: false }]]);
  const s = analyzeConfig(cfg, new Map(), features as any).sources[0]!;
  assert.equal(s.verdict, "NEEDS_TRACES");
  assert.ok(s.blockers.some((b) => b.includes("does not serve traces")));
});

test("withPortal: extracts portal into registry and strips it from the chain", () => {
  const cfg: any = { chains: { mainnet: { id: 1, rpc: "x", portal: "https://portal.sqd.dev/datasets/ethereum-mainnet" } } };
  withPortal(cfg);
  assert.equal(getPortalDataset(1), "https://portal.sqd.dev/datasets/ethereum-mainnet");
  assert.equal(cfg.chains.mainnet.portal, undefined); // Ponder never sees it
  assert.equal(cfg.chains.mainnet.rpc, "x"); // rpc preserved for realtime
});
