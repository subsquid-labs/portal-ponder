import { readFileSync } from 'node:fs';
import { join } from 'node:path';
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

// a block-interval filter — drives `needBlocks`, so assembleRange range-walks `cd.blockHeaders`
const blockFilter: any = {
  type: 'block',
  chainId: 1,
  sourceId: 'blk',
  interval: 1,
  offset: 0,
  fromBlock: undefined,
  toBlock: undefined,
  hasTransactionReceipt: false,
  include: [],
};

// an account-transaction filter — drives `needTxFilter`, so assembleRange range-walks `cd.txBlocks`
const txFilterAll: any = {
  type: 'transaction',
  chainId: 1,
  sourceId: 'acctAll',
  fromAddress: '0xfrom',
  toAddress: undefined,
  includeReverted: false,
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

test('wave 5 (perf): assembleRange returns exactly the in-range logs whether the interval is NARROWER than the chunk (range-walk) or WIDER (entry-scan)', () => {
  // The fix walks min(intervalWidth, chunkEntries) instead of every chunk entry per interval. Both
  // branches must yield the identical in-range set — this pins that equivalence at the two extremes.
  const spec = compileFetchSpec([{ filter: logFilter }], new Map());
  const bns = (logs: any[]) =>
    logs.map((l) => Number(BigInt(l.blockNumber))).sort((x, y) => x - y);

  // (a) RANGE-WALK branch: a WIDE chunk (1000 block entries) with a NARROW interval [500,502]
  // (hi−lo+1 = 3 ≤ 1000). Pre-fix scanned all 1000 entries; now it get()s only the 3 in-range blocks.
  const wide = createChunkData();
  for (let bn = 0; bn < 1000; bn++) {
    wide.headers.set(bn, header(bn));
    wide.logs.set(bn, [rawLog('0xVault', '0xtx' + bn)]);
  }
  expect(bns(assembleRange([wide], [500, 502], spec, new Map()).logs)).toEqual([
    500, 501, 502,
  ]);

  // (b) ENTRY-SCAN branch: a SPARSE chunk (3 far-apart entries) with a huge interval (hi−lo+1 ≫ size) —
  // range-walking a million blocks would be absurd, so the scan branch iterates the 3 entries instead.
  const sparse = createChunkData();
  for (const bn of [10, 5_000, 999_999]) {
    sparse.headers.set(bn, header(bn));
    sparse.logs.set(bn, [rawLog('0xVault', '0xtx' + bn)]);
  }
  expect(
    bns(assembleRange([sparse], [0, 1_000_000], spec, new Map()).logs),
  ).toEqual([10, 5_000, 999_999]);
  // a block just OUTSIDE the interval is excluded by both branches (10 < 11; 999_999 > 5_000)
  expect(
    bns(assembleRange([sparse], [11, 5_000], spec, new Map()).logs),
  ).toEqual([5_000]);

  // (b′) ENTRY-SCAN lo-INCLUSIVE bound: a block sitting EXACTLY at `lo` must be kept. The scan branch's
  // guard is `entry[0] >= lo` — an off-by-one to `> lo` would silently DROP the boundary block, and no
  // other sub-case here has an entry at `lo` (the [11,5000] case's `lo=11` holds no block). This is the
  // scan-branch twin of the range-walk's inclusive `bn = lo` start. (PR #66 review, non-blocking finding 2.)
  // Interval [10, 1_000_000] over the size-3 map → hi−lo+1 = 999_991 > 3, so the ENTRY-SCAN branch runs;
  // block 10 lies exactly at lo and must survive.
  expect(
    bns(assembleRange([sparse], [10, 1_000_000], spec, new Map()).logs),
  ).toEqual([10, 5_000, 999_999]);
});

// An operation-counting Map: tallies `get()` lookups and every FULL-map iteration (`for…of` / `.entries()`
// / spread — all route through `[Symbol.iterator]`). It lets a test OBSERVE which walk `entriesInRange`
// took, pinning the range-walk's PRESENCE — not merely its output.
class CountingMap<K, V> extends Map<K, V> {
  gets = 0;
  iterations = 0;
  get(key: K): V | undefined {
    this.gets += 1;

    return super.get(key);
  }
  [Symbol.iterator](): MapIterator<[K, V]> {
    this.iterations += 1;

    return super[Symbol.iterator]();
  }
  entries(): MapIterator<[K, V]> {
    this.iterations += 1;

    return super.entries();
  }
}

test('wave 5 follow-up (perf presence): a narrow interval over a large chunk RANGE-WALKS all three call sites (bounded get()s, zero full-map iteration) — the pre-#66 full-scan shape is rejected', () => {
  // Pins the PRESENCE of the range-walk, not just its equivalence: reverting the three `entriesInRange`
  // call sites (log-branch headers, needBlocks blockHeaders, needTxFilter txBlocks) to the pre-#66
  // `for (const [bn, …] of cd.<map>) { if (bn < lo || bn > hi) continue; … }` full-scan shape re-introduces
  // O(intervalsPerChunk × chunkEntries) iteration. Under that revert each instrumented map is ITERATED (≥1
  // Symbol.iterator hit) and get()ed zero times — both arms below then fail; on the #66 range-walk the
  // maps are get()ed (≤ interval width) and never iterated. (PR #66 review, non-blocking finding 1.)
  const SIZE = 10_000;
  const headers = new CountingMap<number, ReturnType<typeof header>>();
  const blockHeaders = new CountingMap<number, ReturnType<typeof header>>();
  const txBlocks = new CountingMap<
    number,
    { header: ReturnType<typeof header>; txs: ReturnType<typeof rawTx>[] }
  >();
  const cd = createChunkData();
  cd.headers = headers;
  cd.blockHeaders = blockHeaders;
  cd.txBlocks = txBlocks;
  for (let bn = 0; bn < SIZE; bn++) {
    headers.set(bn, header(bn));
    cd.logs.set(bn, [rawLog('0xVault', '0xtx' + bn)]);
    cd.txs.set(bn, [rawTx('0xtx' + bn)]);
    blockHeaders.set(bn, header(bn));
    txBlocks.set(bn, { header: header(bn), txs: [rawTx('0xtxb' + bn)] });
  }

  // a NARROW interval [5000,5004] (width 5 ≪ SIZE): the range-walk get()s only the 5 interval blocks per
  // map and NEVER iterates it; a full-scan would iterate all 10_000 entries once per call-site map.
  const lo = 5_000;
  const hi = 5_004;
  const width = hi - lo + 1; // 5
  const spec = compileFetchSpec(
    [{ filter: logFilter }, { filter: blockFilter }, { filter: txFilterAll }],
    new Map(),
  );
  const out = assembleRange([cd], [lo, hi], spec, new Map());

  // correctness still holds — exactly the 5 in-range log blocks
  const got = out.logs
    .map((l: any) => Number(BigInt(l.blockNumber)))
    .sort((x, y) => x - y);
  expect(got).toEqual([5_000, 5_001, 5_002, 5_003, 5_004]);

  // PRESENCE: each of the three call-site maps was range-walked — get() count is bounded BY THE INTERVAL
  // WIDTH (not the chunk size) and the map was NEVER full-iterated.
  for (const m of [headers, blockHeaders, txBlocks]) {
    expect(m.iterations).toBe(0); // never full-scanned
    expect(m.gets).toBeGreaterThan(0); // it DID range-walk (a get() per in-range block)
    expect(m.gets).toBeLessThanOrEqual(width); // ≤ interval width, INDEPENDENT of SIZE
  }
  // a full-scan would have touched ≥ SIZE entries; the range-walk touched ≤ width per map.
  expect(headers.gets).toBeLessThan(SIZE);
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

// ── log re-match: parity with the RPC path's per-filter matching (wave 4) ───────────────────────────

test("wave 4: a factory child's PRE-CREATION log is excluded — and its parent tx/block with it (parity with isAddressMatched)", () => {
  // The Portal log request carries the child address with NO per-address creation floor, so a child that
  // emitted filter-matching logs BEFORE its creation event comes back "matched". Upstream's RPC path
  // stores a child's logs only from its creation block on (isAddressMatched: creation ≤ blockNumber);
  // skipping the re-match stored the pre-creation log + its tx + its block — an INV-6 store divergence.
  const CHILD = '0x000000000000000000000000000000000c0ffee1';
  const factory: any = {
    id: 'f1',
    type: 'log',
    chainId: 1,
    sourceId: 's',
    address: '0x00000000000000000000000000000000000fac70',
    eventSelector: '0xsel',
    childAddressLocation: 'topic1',
    fromBlock: undefined,
    toBlock: undefined,
  };
  const factoryLogFilter: any = { ...logFilter, address: factory };
  const childAddresses = new Map([['f1', new Map([[CHILD, 100]])]]); // created at block 100
  const cd = createChunkData();
  cd.headers.set(50, header(50));
  cd.logs.set(50, [rawLog(CHILD, '0xtxPRE')]); // emitted BEFORE creation → must be dropped
  cd.txs.set(50, [rawTx('0xtxPRE')]);
  cd.headers.set(150, header(150));
  cd.logs.set(150, [rawLog(CHILD, '0xtxPOST')]); // after creation → kept
  cd.txs.set(150, [rawTx('0xtxPOST')]);
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter }],
    childAddresses as any,
  );
  const out = assembleRange([cd], [0, 200], spec, childAddresses as any);
  expect(out.logs).toHaveLength(1);
  expect(Number(BigInt((out.logs[0] as any).blockNumber))).toBe(150);
  expect(out.txs).toHaveLength(1); // the dropped log's parent tx must not ride in
  expect((out.txs[0] as any).hash).toBe('0xtxPOST');
  expect(out.blocks.map((b) => Number(BigInt((b as any).number)))).toEqual([
    150,
  ]); // block 50 held ONLY the dropped log → not stored
});

test("wave 4: a bounded filter's below-fromBlock log is excluded even when another filter's chunk fetched it (per-filter range re-match)", () => {
  // Chunk fetches are bounded by the SPEC-global backfillStart/dataEnd, so with filter A unbounded and
  // filter B fromBlock:200, chunks below 200 are fetched with B's topics too (merged server request).
  // Upstream's isLogFilterMatched rejects a B-matching log below 200; without the re-match it was
  // stored and returned as matched — an INV-6 store divergence.
  const filterA: any = { ...logFilter, topic0: '0xaaaa' }; // unbounded
  const filterB: any = { ...logFilter, topic0: '0xbbbb', fromBlock: 200 };
  const logWithTopic = (topic0: string, tx: string) => ({
    ...rawLog('0xV', tx),
    topics: [topic0],
  });
  const cd = createChunkData();
  cd.headers.set(100, header(100));
  // block 100: an A log (kept) and a B log BELOW B's fromBlock (dropped)
  cd.logs.set(100, [
    logWithTopic('0xaaaa', '0xtxA100'),
    logWithTopic('0xbbbb', '0xtxB100'),
  ]);
  cd.txs.set(100, [rawTx('0xtxA100'), rawTx('0xtxB100')]);
  cd.headers.set(250, header(250));
  cd.logs.set(250, [logWithTopic('0xbbbb', '0xtxB250')]); // in B's range → kept
  cd.txs.set(250, [rawTx('0xtxB250')]);
  const spec = compileFetchSpec(
    [{ filter: filterA }, { filter: filterB }],
    new Map(),
  );
  const out = assembleRange([cd], [0, 300], spec, new Map());
  const keptBlocks = out.logs.map((l) =>
    Number(BigInt((l as any).blockNumber)),
  );
  expect(keptBlocks.sort((x, y) => x - y)).toEqual([100, 250]);
  expect(out.logs.map((l) => (l as any).topics[0]).sort()).toEqual([
    '0xaaaa',
    '0xbbbb',
  ]); // the below-range B log at 100 is gone
  expect(out.txs.map((t) => (t as any).hash).sort()).toEqual([
    '0xtxA100',
    '0xtxB250',
  ]); // 0xtxB100 (parent of the dropped log) must not be inserted
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

// ── INV-20: ancestor-error cascade (byte-identity with the stock-ponder RPC path) ────────────────────
//
// Stock ponder's `debug_traceBlockByNumber` DFS (src/rpc/actions.ts) smears a reverted ancestor's
// error/revertReason onto every descendant lacking its OWN error, transitively; a frame with its own
// error keeps it. Portal delivers geth-faithful per-frame errors, so `rankTraces` must reproduce that
// post-processing for the store to match. These are ORACLE tests: the expected error strings come from
// the real RPC-store output / from the cascade rule, never hand-derived per frame.

// index a tx's ranked frames by their source traceAddress (encoded into `input` for synthetic cases,
// or read from the real fixture below).
const byAddr = (traces: any[]): Map<string, any> => {
  const m = new Map<string, any>();
  const sorted = [...traces].sort((x, y) =>
    cmpTraceAddr(x.traceAddress ?? [], y.traceAddress ?? []),
  );
  const ranked = rankTraces(traces);
  // rankTraces returns one frame per SURVIVING sorted trace in cmpTraceAddr order (parityToCallFrame can
  // drop a frame — e.g. a non-call/create/suicide type). Every fixture here is type:'call', so nothing
  // drops; assert that so the positional zip below stays a valid oracle rather than silently misaligning
  // if a future fixture introduces a droppable type.
  expect(ranked.length).toBe(sorted.length);
  sorted.forEach((t, i) => {
    m.set((t.traceAddress ?? []).join(','), ranked[i]!.frame);
  });

  return m;
};

const mkErrTrace = (
  traceAddress: number[],
  error?: string,
  revertReason?: string,
) => ({
  transactionIndex: 0,
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
  ...(error !== undefined ? { error } : {}),
  ...(revertReason !== undefined ? { revertReason } : {}),
});

test('INV-20: real reverted tx — the whole reverted subtree inherits the ancestor error (oracle: RPC store)', () => {
  // Real geth callTracer output for eth mainnet block 20351061, tx index 26 (a reverted swap-router call),
  // reshaped into Portal's Parity-flat RawTrace with each frame's OWN error preserved verbatim (Portal's
  // geth-faithful input shape): frames [], [0], [0,2] carry error; [0,1] and its whole subtree are null.
  const fixture = JSON.parse(
    readFileSync(
      join(__dirname, '__fixtures__', 'trace-error-cascade.json'),
      'utf8',
    ),
  );
  const frames = byAddr(fixture.traces);

  // The [0,1] frame (to=V2 router 0x7a250d..., input 0x38ed1739) has NO own error in the input, but its
  // parent [0] is reverted, so the RPC store carries "execution reverted"/"Too little received" on it.
  const swap = frames.get('0,1');
  expect(swap.to).toBe('0x7a250d5630b4cf539739df2c5dacb4c659f2488d');
  expect((swap.input as string).startsWith('0x38ed1739')).toBe(true);
  expect(swap.error).toBe('execution reverted');
  expect(swap.revertReason).toBe('Too little received');

  // The root reverted, so EVERY frame in the tx ends up carrying the same error (full-subtree smear),
  // including the deep descendants of the succeeded-in-geth [0,1] subtree.
  for (const [, f] of frames) {
    expect(f.error).toBe('execution reverted');
    expect(f.revertReason).toBe('Too little received');
  }
});

test('INV-20 (a): a child with its OWN distinct error SURVIVES (own-error branch wins over the parent)', () => {
  const frames = byAddr([
    mkErrTrace([], 'execution reverted', 'parent revert'),
    mkErrTrace([0]), // null → inherits parent
    mkErrTrace([1], 'out of gas', 'own revert'), // own error → keeps it
    mkErrTrace([1, 0]), // null → inherits [1]'s OWN error, not the root's
  ]);

  expect(frames.get('').error).toBe('execution reverted');
  expect(frames.get('0').error).toBe('execution reverted');
  expect(frames.get('0').revertReason).toBe('parent revert');

  expect(frames.get('1').error).toBe('out of gas'); // survived
  expect(frames.get('1').revertReason).toBe('own revert');
  expect(frames.get('1,0').error).toBe('out of gas'); // inherited the OWN error, not the root's
  expect(frames.get('1,0').revertReason).toBe('own revert');
});

test('INV-20 (b): ≥3-deep nesting inherits the ancestor error transitively', () => {
  const frames = byAddr([
    mkErrTrace([], 'execution reverted', 'deep revert'),
    mkErrTrace([0]),
    mkErrTrace([0, 0]),
    mkErrTrace([0, 0, 0]),
    mkErrTrace([0, 0, 0, 0]),
  ]);

  for (const key of ['', '0', '0,0', '0,0,0', '0,0,0,0']) {
    expect(frames.get(key).error).toBe('execution reverted');
    expect(frames.get(key).revertReason).toBe('deep revert');
  }
});

test('INV-20 (c): a non-reverted tx keeps every frame error/revertReason null', () => {
  const frames = byAddr([
    mkErrTrace([]),
    mkErrTrace([0]),
    mkErrTrace([0, 0]),
    mkErrTrace([1]),
  ]);

  for (const [, f] of frames) {
    expect(f.error).toBeUndefined();
    expect(f.revertReason).toBeUndefined();
  }
});

test('INV-20 (d): an inheriting child of an ancestor that has an error but NO revertReason gets error set AND revertReason === undefined (unconditional assignment)', () => {
  // Ancestor reverted with an `error` but no `revertReason` (revertReason omitted → undefined). The child
  // has neither of its own, so it inherits. Upstream `rpc/actions.ts` assigns BOTH fields unconditionally
  // (`frame.error = error.error; frame.revertReason = error.revertReason`), so the child ends up with the
  // ancestor's error AND revertReason === undefined (present-as-undefined).
  const frames = byAddr([
    mkErrTrace([], 'execution reverted'), // error set, revertReason omitted (undefined)
    mkErrTrace([0]), // no own error → inherits [ ]'s error + undefined revertReason
  ]);

  expect(frames.get('').error).toBe('execution reverted');
  expect(frames.get('').revertReason).toBeUndefined();

  // This PINS upstream-exact behavior: the cascade assigns revertReason UNCONDITIONALLY. Do NOT add a
  // guard skipping the undefined-revertReason assignment — that would diverge the Portal store from the
  // RPC store, whose actions.ts writes revertReason on every inheriting frame regardless of its value.
  expect(frames.get('0').error).toBe('execution reverted'); // inherited the ancestor's error
  expect(frames.get('0').revertReason).toBeUndefined(); // and its undefined revertReason, unconditionally
});

// ── INV-21: root-frame gas parity (byte-identity with the stock-ponder RPC path) ─────────────────────
//
// The RPC path stores geth callTracer's top-frame `gas`, which is the transaction's FULL gasLimit. Portal
// serves Parity-style traces whose ROOT `action.gas` is gasLimit MINUS the EIP-2028 intrinsic (21000 base +
// 16/nonzero + 4/zero calldata byte, + access-list + creation costs). The diff-harness surfaced exactly
// this: 133/133 divergences were gas-only, at the root frame (traceAddress []), with gas_used byte-identical
// and RPC.gas − Portal.gas == the tx's intrinsic — and on every root frame RPC.gas == the tx gasLimit
// UNCONDITIONALLY (grounded on a real captured Portal-vs-RPC store diff, eth mainnet). So `buildTraces`
// overrides the root frame's gas with the parent tx's gasLimit to make the Portal store byte-identical to
// the stock RPC realtime path (intra-deployment determinism — a fork DB is Portal-backfill + stock-RPC-
// realtime and must be uniformly ponder-shaped). Non-root frames already match and are left untouched.
//
// ORACLE: the expected root gas comes from the REAL RPC-store top-frame value (== the tx's real gasLimit)
// captured for a real eth-mainnet tx (block 19068568, tx index 7 — a Uniswap V2 router swap); the pre-fix
// Portal `action.gas` (gasLimit − intrinsic) and the real non-root child gas are captured verbatim too. The
// fixture is a real Portal Parity-flat block, not synthetic.
const gasFixture = JSON.parse(
  readFileSync(join(__dirname, '__fixtures__', 'trace-root-gas.json'), 'utf8'),
);

// a trace filter matching the fixture tx's `to` (the router) so both the root and its child frame are kept.
const rootGasTraceFilter: any = {
  type: 'trace',
  chainId: 1,
  sourceId: 'g',
  fromAddress: undefined,
  toAddress: gasFixture.oracle.toAddr,
  functionSelector: undefined,
  callType: undefined,
  includeReverted: false,
  fromBlock: 0,
  toBlock: 1_000_000_000,
  hasTransactionReceipt: false,
  include: [],
};

const assembleGasFixture = (block: any) => {
  const cd = createChunkData();
  const bn = Number(block.header.number);
  cd.traceBlocks.set(bn, {
    header: block.header,
    traces: block.traces,
    txs: block.transactions,
  });
  const spec = compileFetchSpec([{ filter: rootGasTraceFilter }], new Map());
  return assembleRange([cd], [bn, bn], spec, new Map());
};

// frame at a given traceAddress from an assembled range (matches by the input suffix we carry through).
const frameGasByAddr = (out: any) => {
  const m = new Map<string, string>();
  for (const t of out.traces) {
    const f = t.trace.trace;
    m.set(String(f.index), f.gas as string);
  }
  return m;
};

test('INV-21: real diff-root tx — the root frame carries the tx gasLimit (oracle: RPC store), non-root untouched', () => {
  const { oracle } = gasFixture;
  // Sanity that the fixture pins the real production signature: the pre-fix Portal action.gas is the
  // intrinsic-reduced value, and it DIFFERS from the tx gasLimit (else the test proves nothing), while the
  // RPC store's root gas equals the tx gasLimit and the non-root child already matches.
  expect(oracle.rootPortalActionGas).not.toBe(oracle.txGasLimit); // Parity action.gas ≠ gasLimit
  expect(oracle.rootRpcGas).toBe(oracle.txGasLimit); // geth top-frame gas == gasLimit
  expect(oracle.childPortalGas).toBe(oracle.childRpcGas); // non-root already byte-identical

  const out = assembleGasFixture(gasFixture.block);
  expect(out.traces).toHaveLength(2); // root + child both matched the router filter

  const gasByIndex = frameGasByAddr(out);
  // index 0 = the DFS-first (root) frame, index 1 = its child (INV-5 ranking).
  const rootGas = gasByIndex.get('0');
  const childGas = gasByIndex.get('1');

  // THE FIX: the stored root gas equals the tx gasLimit (== the RPC store's top-frame gas), NOT Portal's
  // intrinsic-reduced action.gas.
  expect(rootGas).toBe(oracle.rootRpcGas);
  expect(rootGas).toBe(oracle.txGasLimit);
  expect(rootGas).not.toBe(oracle.rootPortalActionGas); // MUTATION SENTINEL: the OLD `a.gas ?? t.callGas`
  // mapping (Parity action.gas at root) fails here — revert the buildTraces override and this line breaks.

  // Non-root child is UNTOUCHED — its stored gas is still Portal's action.gas (which already matches RPC).
  expect(childGas).toBe(oracle.childPortalGas);
  expect(childGas).toBe(oracle.childRpcGas);
});

test('INV-21 (a): an already-matching root (Portal action.gas == gasLimit) is unchanged — idempotent', () => {
  // A root whose Parity action.gas HAPPENS to equal the tx gasLimit (31/164 root frames in the grounded
  // window): the override sets gas := gasLimit, i.e. the same value — a no-op. Prove it stays byte-equal.
  const block = structuredClone(gasFixture.block);
  const gl = gasFixture.oracle.txGasLimit;
  block.traces = [block.traces[0]]; // root only
  block.traces[0].action.gas = gl; // Portal already served the full gasLimit
  const out = assembleGasFixture(block);
  expect(out.traces).toHaveLength(1);
  expect(out.traces[0].trace.trace.gas).toBe(gl);
});

test('INV-21 (b): a rootless trace chunk (only a deep frame, its root not in this chunk) is NOT rewritten', () => {
  // The override is keyed on traceAddress.length === 0, NOT on the DFS rank — so a chunk holding only a deep
  // frame (traceAddress [0], which sorts FIRST here and gets DFS index 0) must keep its own action.gas, never
  // the tx gasLimit. This guards against a rank-based root heuristic silently corrupting a partial tree.
  const block = structuredClone(gasFixture.block);
  const child = block.traces[1]; // the traceAddress:[0] frame
  block.traces = [child]; // root [] absent from this chunk
  const out = assembleGasFixture(block);
  expect(out.traces).toHaveLength(1);
  // gas stays the child's own action.gas; NOT overwritten to the tx gasLimit.
  expect(out.traces[0].trace.trace.gas).toBe(gasFixture.oracle.childPortalGas);
  expect(out.traces[0].trace.trace.gas).not.toBe(gasFixture.oracle.txGasLimit);
});
