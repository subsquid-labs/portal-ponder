import fc from 'fast-check';
import { expect, test } from 'vitest';
import {
  assembleRange,
  createChunkData,
  rankTraces,
} from './portal-assemble.js';
import { compileFetchSpec } from './portal-filters.js';
import { setCheckMode } from './portal-invariant.js';
import { cmpTraceAddr } from './portal-transform.js';

fc.configureGlobal({ seed: 1337 }); // deterministic CI

setCheckMode('strict');

const header = (n: number) => ({
  number: n,
  hash: '0x' + n.toString(16).padStart(64, '0'),
  parentHash: '0x0',
  timestamp: 1_700_000_000 + n,
});
const rawLog = (addr: string, tx: string) => ({
  address: addr,
  topics: ['0xdead'],
  data: '0x',
  transactionHash: tx,
  transactionIndex: 0,
  logIndex: 0,
});
const rawTx = (hash: string) => ({
  transactionIndex: 0,
  hash,
  from: '0xfrom',
  to: '0xto',
  input: '0x',
  value: '0x0',
  nonce: 0,
  gas: '0x1',
  type: 0,
});

const logFilter: any = {
  type: 'log',
  chainId: 1,
  sourceId: 's',
  address: undefined,
  topic0: '0xdead',
  topic1: null,
  topic2: null,
  topic3: null,
  fromBlock: undefined,
  toBlock: undefined,
  hasTransactionReceipt: false,
  include: [],
};

// ── INV-2: interval exactness vs a brute-force model ────────────────────────────────────────────────

test('INV-2: assembleRange returns exactly the in-range logs (vs brute-force filter)', () => {
  fc.assert(
    fc.property(
      fc.array(fc.integer({ min: 0, max: 30 }), {
        minLength: 1,
        maxLength: 15,
      }), // block numbers with logs
      fc.integer({ min: 0, max: 30 }),
      fc.integer({ min: 0, max: 30 }),
      (blockNums, a, b) => {
        const lo = Math.min(a, b);
        const hi = Math.max(a, b);
        const cd = createChunkData();
        const model: number[] = []; // expected log block numbers (with multiplicity)
        for (const bn of new Set(blockNums)) {
          const nLogs = (bn % 3) + 1;
          cd.headers.set(bn, header(bn));
          cd.logs.set(
            bn,
            Array.from({ length: nLogs }, (_, i) =>
              rawLog('0xVault', '0xtx' + bn + '_' + i),
            ),
          );
          if (bn >= lo && bn <= hi)
            for (let i = 0; i < nLogs; i++) model.push(bn);
        }
        const spec = compileFetchSpec([{ filter: logFilter }], new Map());
        const out = assembleRange([cd], [lo, hi], spec, new Map());
        expect(out.logs.length).toBe(model.length);
        for (const l of out.logs) {
          const bn = Number(BigInt((l as any).blockNumber));
          expect(bn).toBeGreaterThanOrEqual(lo);
          expect(bn).toBeLessThanOrEqual(hi);
        }
      },
    ),
  );
});

test('seenTx dedupe: two logs sharing a tx insert the tx once', () => {
  const cd = createChunkData();
  cd.headers.set(5, header(5));
  cd.logs.set(5, [rawLog('0xV', '0xSAME'), rawLog('0xV', '0xSAME')]);
  cd.txs.set(5, [rawTx('0xSAME'), rawTx('0xSAME')]);
  const spec = compileFetchSpec([{ filter: logFilter }], new Map());
  const out = assembleRange([cd], [0, 10], spec, new Map());
  expect(out.logs).toHaveLength(2);
  expect(out.txs).toHaveLength(1); // deduped
});

// ── tx-filter receipts: every matched account tx carries its receipt ─────────────────────────────────

const receiptTx = (hash: string) => ({
  ...rawTx(hash),
  // receipt fields (the tx query always projects RECEIPT_FIELDS) — toSyncReceipt reads these
  status: '0x1',
  cumulativeGasUsed: '0x5208',
  gasUsed: '0x5208',
  effectiveGasPrice: '0x1',
  logsBloom: '0x' + '00'.repeat(256),
  contractAddress: null,
});

// `hasTransactionReceipt: false` is unconstructible upstream (literal-true type, always set by the
// build) — the `any`-forced shape pins the HARDENED contract: receipts must flow even if upstream
// ever relaxes that invariant.
const txFilter: any = {
  type: 'transaction',
  chainId: 1,
  sourceId: 'acct',
  fromAddress: '0xfrom',
  toAddress: undefined,
  includeReverted: false,
  fromBlock: undefined,
  toBlock: undefined,
  hasTransactionReceipt: false, // ← receipts must be emitted anyway
  include: [],
};

test('a tx-filter-matched tx yields its receipt even without hasTransactionReceipt', () => {
  // ponder's buildEvents dereferences `transactionReceipt.status` on EVERY transaction event to apply
  // `includeReverted` (positional cursor, no identity check) — a matched tx stored without its receipt
  // would crash buildEvents, or with a sparse receipt set silently consult a NEIGHBOR's receipt.
  const cd = createChunkData();
  cd.txBlocks.set(5, { header: header(5), txs: [receiptTx('0xACCT')] });
  const spec = compileFetchSpec([{ filter: txFilter }], new Map());
  const out = assembleRange([cd], [0, 10], spec, new Map());
  expect(out.txs).toHaveLength(1);
  expect(out.receipts).toHaveLength(1);
  expect((out.receipts[0] as any).transactionHash).toBe('0xACCT');
});

test('a tx matched by BOTH a log filter and a tx filter keeps one tx row AND gets its receipt', () => {
  // the log branch wins the seenTx dedupe (its raw row lacks receipt columns unless needReceipts), so
  // the tx-filter branch must push the receipt BEFORE its seenTx skip — from ITS raw row, which always
  // carries the receipt fields.
  const cd = createChunkData();
  cd.headers.set(5, header(5));
  cd.logs.set(5, [rawLog('0xV', '0xBOTH')]);
  cd.txs.set(5, [rawTx('0xBOTH')]);
  cd.txBlocks.set(5, { header: header(5), txs: [receiptTx('0xBOTH')] });
  const spec = compileFetchSpec(
    [{ filter: logFilter }, { filter: txFilter }],
    new Map(),
  );
  const out = assembleRange([cd], [0, 10], spec, new Map());
  expect(out.txs).toHaveLength(1); // still deduped
  expect(out.receipts).toHaveLength(1); // but the receipt survives the dedupe
  expect((out.receipts[0] as any).transactionHash).toBe('0xBOTH');
});

// ── INV-5: trace ranking = index in the cmpTraceAddr-sorted full list ────────────────────────────────

const arbTraceAddr = fc.array(fc.integer({ min: 0, max: 3 }), { maxLength: 4 });
// encode the traceAddress into `input` (passed through by parityToCallFrame) so we can verify the sort
const mkTrace = (traceAddress: number[]) => ({
  traceAddress,
  type: 'call',
  subtraces: 0,
  action: {
    from: '0xfrom',
    to: '0xto',
    value: '0x0',
    gas: '0x1',
    input: '0x' + traceAddress.join(','),
    callType: 'call',
  },
  result: { gasUsed: '0x1', output: '0x' },
});

test('INV-5: rankTraces sorts into pre-order DFS and assigns strictly-increasing full-tree ranks', () => {
  fc.assert(
    // fc.uniqueArray yields arbitrary orderings across runs, so rankTraces' internal sort is exercised
    fc.property(
      fc.uniqueArray(arbTraceAddr, {
        comparator: (a, b) => cmpTraceAddr(a, b) === 0,
        maxLength: 8,
      }),
      (addrs) => {
        const ranked = rankTraces(addrs.map(mkTrace));
        const expectedOrder = [...addrs].sort(cmpTraceAddr);
        expect(ranked.map((r) => r.index)).toEqual(
          expectedOrder.map((_, i) => i),
        ); // ranks are 0..n-1, strictly increasing
        // the k-th ranked frame is the k-th trace in cmpTraceAddr order (verified via the encoded input)
        expect(ranked.map((r) => (r.frame as any).input)).toEqual(
          expectedOrder.map((ta) => '0x' + ta.join(',')),
        );
      },
    ),
  );
});

// ── closest includes trace-only + tx-only blocks (C9) ───────────────────────────────────────────────

test('closest includes a trace-only block above the highest log block', () => {
  const TARGET = '0x000000000000000000000000000000000000dead';
  const traceFilter: any = {
    type: 'trace',
    chainId: 1,
    sourceId: 't',
    fromAddress: undefined,
    toAddress: TARGET,
    functionSelector: undefined,
    callType: undefined,
    includeReverted: false,
    fromBlock: 0,
    toBlock: 1000,
    hasTransactionReceipt: false,
    include: [],
  };
  const cd = createChunkData();
  cd.headers.set(10, header(10));
  cd.logs.set(10, [rawLog('0xV', '0xtx10')]); // a log block at 10
  cd.traceBlocks.set(20, {
    header: header(20),
    traces: [
      {
        transactionIndex: 0,
        traceAddress: [],
        type: 'call',
        subtraces: 0,
        action: {
          from: '0xother',
          to: TARGET,
          value: '0x0',
          gas: '0x1',
          input: '0x',
          callType: 'call',
        },
        result: { gasUsed: '0x1', output: '0x' },
      },
    ],
    txs: [rawTx('0xtx20')],
  });
  const spec = compileFetchSpec(
    [{ filter: logFilter }, { filter: traceFilter }],
    new Map(),
  );
  const out = assembleRange([cd], [0, 100], spec, new Map());
  expect(out.traces).toHaveLength(1);
  expect(Number(BigInt((out.closest as any).number))).toBe(20); // trace-only block 20 wins over log block 10
});

test("INV-2: trace assembly respects the interval's UPPER bound (a trace block at hi+1 is excluded)", () => {
  const TARGET = '0x000000000000000000000000000000000000dead';
  const traceFilter: any = {
    type: 'trace',
    chainId: 1,
    sourceId: 't',
    fromAddress: undefined,
    toAddress: TARGET,
    functionSelector: undefined,
    callType: undefined,
    includeReverted: false,
    fromBlock: 0,
    toBlock: 1000,
    hasTransactionReceipt: false,
    include: [],
  };
  const mkTraceBlock = (bn: number) => ({
    header: header(bn),
    traces: [
      {
        transactionIndex: 0,
        traceAddress: [],
        type: 'call',
        subtraces: 0,
        action: {
          from: '0xother',
          to: TARGET,
          value: '0x0',
          gas: '0x1',
          input: '0x',
          callType: 'call',
        },
        result: { gasUsed: '0x1', output: '0x' },
      },
    ],
    txs: [rawTx(`0xtx${bn}`)],
  });
  const cd = createChunkData();
  const HI = 50;
  cd.traceBlocks.set(HI, mkTraceBlock(HI)); //     at the bound → included
  cd.traceBlocks.set(HI + 1, mkTraceBlock(HI + 1)); // one past → excluded
  const spec = compileFetchSpec([{ filter: traceFilter }], new Map());
  const out = assembleRange([cd], [0, HI], spec, new Map());
  expect(out.traces).toHaveLength(1);
  expect(Number(BigInt((out.traces[0]!.block as any).number))).toBe(HI);
});
