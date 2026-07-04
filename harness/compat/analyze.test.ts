import assert from 'node:assert';
import { test } from 'node:test';
import { getPortalDataset, withPortal } from '../../portal/config.ts';
import { analyzeConfig } from './analyze.ts';
import type { DatasetInfo } from './datasets.ts';

// caps come from the committed docs snapshot (networks.json): eth=traces+stateDiffs,
// arbitrum=traces only, flare(14)=no traces, optimism(10)=traces w/ Bedrock note.
const FIXTURE = {
  chains: { eth: { id: 1 }, weird: { id: 999_999 } },
  contracts: {
    Factory: { chain: 'eth', address: { event: {}, parameter: 'proxy' } },
    WithReceipts: {
      chain: 'eth',
      address: '0xabc',
      includeTransactionReceipts: true,
    },
    WithTraces: { chain: 'eth', address: '0xabc', includeCallTraces: true },
    NoDataset: { chain: 'weird', address: '0xabc' },
  },
  blocks: { Blk: { chain: 'eth', interval: 100 } },
};

test('verdict per source type (empty catalog → trust docs caps)', () => {
  const r = analyzeConfig(FIXTURE, new Map());
  const v = (name: string) => r.sources.find((s) => s.source === name)?.verdict;
  assert.equal(v('Factory'), 'READY');
  assert.equal(v('WithReceipts'), 'READY');
  assert.equal(v('WithTraces'), 'READY'); // eth has traces per docs
  assert.equal(v('NoDataset'), 'NO_DATASET'); // chain 999999 not in docs matrix
  assert.equal(v('block:Blk'), 'READY'); // block-interval now supported
  assert.equal(r.overall, 'PARTIAL'); // only NoDataset blocked
  assert.equal(r.ready, 4);
});

test('capability: a chain with traces:false (per docs) flags trace sources', () => {
  const cfg = {
    chains: { flare: { id: 14 } },
    contracts: {
      T: { chain: 'flare', address: '0xabc', includeCallTraces: true },
    },
  };
  const s = analyzeConfig(cfg, new Map()).sources[0]!;
  assert.equal(s.verdict, 'NEEDS_TRACES');
  assert.ok(s.blockers.some((b) => b.includes('no traces')));
});

test('capability: Arbitrum HAS traces per docs → trace source READY (the docs are authoritative)', () => {
  const cfg = {
    chains: { arb: { id: 42161 } },
    contracts: {
      T: { chain: 'arb', address: '0xabc', includeCallTraces: true },
    },
  };
  assert.equal(analyzeConfig(cfg, new Map()).sources[0]!.verdict, 'READY');
});

test('account source (transactions from/to) → READY where the dataset is served', () => {
  const cfg = {
    chains: { eth: { id: 1 } },
    accounts: { Wallet: { chain: 'eth', address: '0xabc' } },
  };
  const s = analyzeConfig(cfg, new Map()).sources[0]!;
  assert.equal(s.source, 'account:Wallet');
  assert.ok(s.needs.includes('accountTx'));
  assert.equal(s.verdict, 'READY');
});

test("per-portal existence: a portal that doesn't serve the dataset → NO_DATASET", () => {
  const catalog = new Map<string, DatasetInfo>([
    [
      'some-other-chain',
      { dataset: 'some-other-chain', realTime: true, aliases: [] },
    ],
  ]);
  const cfg = {
    chains: { eth: { id: 1 } },
    contracts: { C: { chain: 'eth', address: '0xabc' } },
  };
  const s = analyzeConfig(cfg, catalog).sources[0]!;
  assert.equal(s.verdict, 'NO_DATASET');
  assert.ok(s.blockers.some((b) => b.includes('does not serve')));
});

test('block-range caveat note surfaced (Optimism Bedrock)', () => {
  const cfg = {
    chains: { op: { id: 10 } },
    contracts: {
      T: { chain: 'op', address: '0xabc', includeCallTraces: true },
    },
  };
  const s = analyzeConfig(cfg, new Map()).sources[0]!;
  assert.equal(s.verdict, 'READY');
  assert.ok(s.notes.some((n) => n.includes('Bedrock')));
});

test('withPortal: extracts portal into registry and strips it from the chain', () => {
  const cfg: any = {
    chains: {
      mainnet: {
        id: 1,
        rpc: 'x',
        portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
      },
    },
  };
  withPortal(cfg);
  assert.equal(
    getPortalDataset(1),
    'https://portal.sqd.dev/datasets/ethereum-mainnet',
  );
  assert.equal(cfg.chains.mainnet.portal, undefined);
  assert.equal(cfg.chains.mainnet.rpc, 'x');
});
