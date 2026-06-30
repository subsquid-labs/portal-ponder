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
  assert.equal(v("Factory"), "READY"); // logs + tx + factory all supported
  assert.equal(v("WithReceipts"), "NEEDS_RECEIPTS");
  assert.equal(v("WithTraces"), "NEEDS_TRACES");
  assert.equal(v("NoDataset"), "NO_DATASET");
  assert.equal(v("block:Blk"), "NEEDS_BLOCK_FILTER");
  assert.equal(r.overall, "PARTIAL");
  assert.equal(r.ready, 1);
});

test("analyzeConfig: factory detected on the contract address", () => {
  const r = analyzeConfig(FIXTURE, new Map());
  assert.ok(r.sources.find((s) => s.source === "Factory")!.needs.includes("logFactory"));
});

test("withPortal: extracts portal into registry and strips it from the chain", () => {
  const cfg: any = { chains: { mainnet: { id: 1, rpc: "x", portal: "https://portal.sqd.dev/datasets/ethereum-mainnet" } } };
  withPortal(cfg);
  assert.equal(getPortalDataset(1), "https://portal.sqd.dev/datasets/ethereum-mainnet");
  assert.equal(cfg.chains.mainnet.portal, undefined); // Ponder never sees it
  assert.equal(cfg.chains.mainnet.rpc, "x"); // rpc preserved for realtime
});
