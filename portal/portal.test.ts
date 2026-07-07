import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, expect, test } from 'vitest';
import { createPortalHistoricalSync } from './portal.js';

/**
 * Range-END cursor anchor for an IN-RANGE `/finalized-stream` window (issue #47). The real Portal
 * terminates an in-range stream by serving the range-end block HEADER as the cursor anchor — it does NOT
 * 204 an in-range window (a 204 strictly means `fromBlock` is above the SERVING replica's finalized head).
 * A mid-range 204 now fails closed (PortalIncompleteRangeError → retry-to-budget → throw), so a mock that
 * models an in-range served-through terminal must emit this header-only record (NO logs/txs/traces → it
 * registers no matched rows and asserts nothing) so the stream ends by REACHING `to` instead of 204-ing
 * the served-through tail. Mirrors portal-shell.test.ts's `anchor`/`streamRes` semantics.
 */
const anchorHeader = (num: number) => ({
  number: num,
  hash: `0x${num.toString(16).padStart(64, '0')}`,
  parentHash: `0x${'00'.repeat(32)}`,
  timestamp: 1_700_000_000 + num,
  logsBloom: `0x${'00'.repeat(256)}`,
  miner: `0x${'99'.repeat(20)}`,
  gasUsed: '0x1',
  gasLimit: '0x1c9c380',
  stateRoot: `0x${'22'.repeat(32)}`,
  receiptsRoot: `0x${'33'.repeat(32)}`,
  transactionsRoot: `0x${'44'.repeat(32)}`,
  size: '0x500',
  difficulty: '0x0',
  extraData: '0x',
});

/**
 * Terminate an in-range window at its range-end anchor: serve `blocks` (may be empty) PLUS a header-only
 * anchor at `min(to, head)` — unless a served block already reaches that end (the Portal emits each block
 * exactly once, so a duplicate would double-count). This is the in-range terminal; a 204 (fromBlock above
 * the served head) stays a bare 204 at its own call site. `head` defaults to `to`.
 */
const anchorRes = (
  res: http.ServerResponse,
  to: number,
  blocks: unknown[] = [],
  head: number = to,
) => {
  const end = Math.min(to, head);
  const maxServed = blocks.reduce(
    (m, b) => Math.max(m, (b as any).header?.number ?? -1),
    -1,
  );
  const out =
    maxServed >= end ? blocks : [...blocks, { header: anchorHeader(end) }];
  res.writeHead(200, { 'content-type': 'application/x-ndjson' });
  res.end(`${out.map((b) => JSON.stringify(b)).join('\n')}\n`);
};

/**
 * Fixture: one Portal `/finalized-stream` NDJSON block carrying an Euler EVault
 * `Deposit` log AND its parent transaction (the `transaction` relation).
 *
 * Regression: portal.ts originally fetched logs+blocks only, so `event.transaction`
 * was undefined and Ponder's event profiler crashed reading `event.transaction.hash`
 * in multi-chain mode (see indexing-store/profile.ts). This fixture + test pin that
 * the matched log's transaction is fetched, transformed, and inserted.
 */
const TX_HASH =
  '0x62684e3dab102ad2e626d9121dba1d9915f238b2dd0316cdf8d4860751305071';
const BLOCK_HASH =
  '0xdce7daa5236cc31d94a3313648f2c0b2dbbb8a5fa26e10fb2edd26a4c45e7240';
const VAULT = '0x44b3c96db2caf61167a9eab82901139a404cdb6f';
const DEPOSIT_TOPIC0 =
  '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7';

const FIXTURE_BLOCK = {
  header: {
    number: 20558652,
    hash: BLOCK_HASH,
    parentHash:
      '0x1111111111111111111111111111111111111111111111111111111111111111',
    timestamp: 1724000000,
    logsBloom: '0x' + '00'.repeat(256),
    miner: '0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5',
    gasUsed: '0xabc',
    gasLimit: '0x1c9c380',
    stateRoot: '0x' + '22'.repeat(32),
    receiptsRoot: '0x' + '33'.repeat(32),
    transactionsRoot: '0x' + '44'.repeat(32),
    size: '0x500',
    difficulty: '0x0',
    extraData: '0x',
  },
  logs: [
    {
      address: VAULT,
      topics: [
        DEPOSIT_TOPIC0,
        '0x0000000000000000000000004b5ccdb3b7e44475d1f0a06499f12acbd4fc0032',
        '0x0000000000000000000000004b5ccdb3b7e44475d1f0a06499f12acbd4fc0032',
      ],
      data:
        '0x00000000000000000000000000000000000000000000000000000000000f4240' +
        '00000000000000000000000000000000000000000000000000000000000f4240',
      transactionHash: TX_HASH,
      transactionIndex: 1,
      logIndex: 4,
    },
  ],
  transactions: [
    {
      transactionIndex: 1,
      hash: TX_HASH,
      from: '0x4b5ccdb3b7e44475d1f0a06499f12acbd4fc0032',
      to: VAULT,
      input: '0x6e553f65',
      value: '0x0',
      nonce: 7,
      gas: '0x317fa',
      gasPrice: '0xc0db32e7d',
      maxFeePerGas: '0xc0db32e7d',
      maxPriorityFeePerGas: '0x0',
      type: 2,
      r: '0x' + 'ab'.repeat(32),
      s: '0x' + 'cd'.repeat(32),
      v: '0x1',
      yParity: '0x1',
    },
  ],
};

let server: http.Server;
let port: number;

beforeEach(async () => {
  process.env.PORTAL_CHUNK_FIXED = '1'; // skip head-based chunk scaling (no /finalized-head call)
  process.env.PORTAL_FINALIZED_HEAD = '2000000000'; // valid finality head — real usage always has one (C3: unknown head → RPC fallback)
  server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      // serve the fixture only for the request whose range covers the block
      if (
        req.url?.includes('finalized-stream') &&
        q.fromBlock <= 20558652 &&
        (q.toBlock ?? 1e12) >= 20558652
      ) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(JSON.stringify(FIXTURE_BLOCK) + '\n');
      } else {
        res.writeHead(204).end(); // above head / nothing more
      }
    });
  });
  port = await new Promise<number>((resolve) =>
    server.listen(0, () => resolve((server.address() as AddressInfo).port)),
  );
});

afterEach(() => {
  server.close();
  delete process.env.PORTAL_CHUNK_FIXED;
  delete process.env.PORTAL_CHUNK_BLOCKS;
  delete process.env.PORTAL_DISCOVERY_WINDOWS;
  delete process.env.PORTAL_FINALIZED_HEAD;
  delete process.env.PORTAL_WARMUP_BLOCKS;
  delete process.env.PORTAL_READAHEAD;
  delete process.env.PORTAL_CHECKS;
});

test("regression: matched log's transaction is fetched, transformed, and inserted (event.transaction defined)", async () => {
  const inserted = { logs: [] as any[], blocks: [] as any[], txs: [] as any[] };
  const syncStore: any = {
    insertLogs: (p: any) => inserted.logs.push(...p.logs),
    insertBlocks: (p: any) => inserted.blocks.push(...p.blocks),
    insertTransactions: (p: any) => inserted.txs.push(...p.transactions),
    insertTransactionReceipts: () => {},
    insertTraces: () => {},
  };

  const filter: any = {
    type: 'log',
    chainId: 1,
    sourceId: 'evault:deposit',
    address: VAULT,
    topic0: DEPOSIT_TOPIC0,
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: 20558652,
    toBlock: 20558652,
    hasTransactionReceipt: false,
    include: [],
  };
  const interval: [number, number] = [20558652, 20558652];

  const sync = createPortalHistoricalSync({
    common: {
      logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
    } as any,
    chain: {
      id: 1,
      name: 'mainnet',
      portal: `http://localhost:${port}`,
    } as any,
    childAddresses: new Map(),
    eventCallbacks: [{ filter }], // FULL per-chain filter set (C1: fetch-spec resolved from this)
  } as any);

  const logs = await sync.syncBlockRangeData({
    interval,
    requiredIntervals: [{ interval, filter }],
    requiredFactoryIntervals: [],
    syncStore,
  });
  await sync.syncBlockData({ interval, logs, syncStore } as any);

  // log was synced
  expect(inserted.logs).toHaveLength(1);
  expect(inserted.logs[0].transactionHash).toBe(TX_HASH);
  // block has a hash → event.block.hash is defined
  expect(inserted.blocks).toHaveLength(1);
  expect(inserted.blocks[0].hash).toBe(BLOCK_HASH);
  // THE REGRESSION: the parent transaction is fetched + inserted → event.transaction.hash is defined
  expect(inserted.txs).toHaveLength(1);
  expect(inserted.txs[0].hash).toBe(TX_HASH);
  expect(inserted.txs[0].blockHash).toBe(BLOCK_HASH);
  expect(inserted.txs[0].from).toBe(
    '0x4b5ccdb3b7e44475d1f0a06499f12acbd4fc0032',
  );
  expect(inserted.txs[0].transactionIndex).toBe('0x1');
});

test('regression (C1): a 2nd log filter sharing a chunk on a LATER call is still fetched — no silent gap', async () => {
  // Two independent log filters whose blocks both fall in chunk 0 ([0, 499_999]) but which Ponder
  // marks "required" on DIFFERENT syncBlockRangeData calls (each call carries only the filter still
  // missing its sub-interval). The bug keyed chunk 0 by idx alone, freezing it to the FIRST call's
  // single filter → the 2nd filter's log was never streamed yet its interval was marked complete.
  const T1 = '0x' + '11'.repeat(32);
  const T2 = '0x' + '22'.repeat(32);
  const A1 = '0x' + 'aa'.repeat(20);
  const A2 = '0x' + 'bb'.repeat(20);
  const TXA = '0x' + 'a1'.repeat(32);
  const TXB = '0x' + 'b2'.repeat(32);
  const BN1 = 10;
  const BN2 = 11; // both in chunk 0

  const mkBlock = (
    num: number,
    addr: string,
    topic0: string,
    txHash: string,
  ) => ({
    header: {
      number: num,
      hash: '0x' + num.toString(16).padStart(64, '0'),
      parentHash: '0x' + '00'.repeat(32),
      timestamp: 1_700_000_000 + num,
      logsBloom: '0x' + '00'.repeat(256),
      miner: '0x' + '99'.repeat(20),
      gasUsed: '0x1',
      gasLimit: '0x1c9c380',
      stateRoot: '0x' + '22'.repeat(32),
      receiptsRoot: '0x' + '33'.repeat(32),
      transactionsRoot: '0x' + '44'.repeat(32),
      size: '0x500',
      difficulty: '0x0',
      extraData: '0x',
    },
    logs: [
      {
        address: addr,
        topics: [topic0],
        data: '0x',
        transactionHash: txHash,
        transactionIndex: 0,
        logIndex: 0,
      },
    ],
    transactions: [
      {
        transactionIndex: 0,
        hash: txHash,
        from: A1,
        to: addr,
        input: '0x',
        value: '0x0',
        nonce: 0,
        gas: '0x1',
        gasPrice: '0x1',
        type: 0,
      },
    ],
  });
  // emit a block ONLY when the request's log spec matches it, so a chunk fetched for filter 1 alone
  // never contains filter 2's block (this is what makes the idx-only freeze observable)
  const matches = (specs: any[], topic0: string, addr: string) =>
    (specs ?? []).some(
      (s) =>
        (!s.topic0 ||
          s.topic0
            .map((x: string) => x.toLowerCase())
            .includes(topic0.toLowerCase())) &&
        (!s.address ||
          s.address
            .map((x: string) => x.toLowerCase())
            .includes(addr.toLowerCase())),
    );

  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 1_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 1e12;
      const out: any[] = [];
      if (from <= BN1 && to >= BN1 && matches(q.logs, T1, A1))
        out.push(mkBlock(BN1, A1, T1, TXA));
      if (from <= BN2 && to >= BN2 && matches(q.logs, T2, A2))
        out.push(mkBlock(BN2, A2, T2, TXB));
      // in-range window (head 1e9 ≫ chunk 0): terminate at the range-end anchor, never a mid-range 204
      anchorRes(res, to, out);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const f1: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's1',
      address: A1,
      topic0: T1,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: 499_999,
      hasTransactionReceipt: false,
      include: [],
    };
    const f2: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's2',
      address: A2,
      topic0: T2,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: 499_999,
      hasTransactionReceipt: false,
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter: f1 }, { filter: f2 }],
    } as any);
    const iA: [number, number] = [BN1, BN1];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter: f1 }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    const iB: [number, number] = [BN2, BN2];
    await sync.syncBlockRangeData({
      interval: iB,
      requiredIntervals: [{ interval: iB, filter: f2 }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    // BEFORE FIX: chunk 0 cached for filter 1 only → filter 2's block-11 log never streamed → 1 log.
    // AFTER FIX:  initSpec makes chunk 0 filter-complete → both logs present → 2 logs.
    const f2Log = inserted.logs.find(
      (l) => l.topics?.[0]?.toLowerCase() === T2.toLowerCase(),
    );
    expect(f2Log).toBeDefined();
    expect(inserted.logs).toHaveLength(2);
  } finally {
    srv.close();
  }
});

test('regression: a dataset-unsupported field (accessList) is dropped, not crashed on', async () => {
  // Per-dataset schema varies — e.g. Monad/plasma transactions have no accessList; the whole request
  // 400s ("column 'access_list_size' is not found in 'transactions'"). accessList is non-load-bearing
  // and NULLABLE, so the fork must drop it and retry — EVEN THOUGH Ponder's default `include` always
  // lists transaction.accessList (see `include` below). A static default include must never force a
  // crash on a droppable field; only NOT-NULL/bloom/core fields do.
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 2_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      if (
        req.url?.includes('finalized-stream') &&
        q.fields?.transaction?.accessList !== undefined
      ) {
        res.writeHead(400);
        res.end(
          "Bad request: couldn't parse request: column 'access_list_size' is not found in 'transactions'",
        );
        return;
      }
      if (
        req.url?.includes('finalized-stream') &&
        q.fromBlock <= 20558652 &&
        (q.toBlock ?? 1e12) >= 20558652
      ) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(JSON.stringify(FIXTURE_BLOCK) + '\n');
        return;
      }
      res.writeHead(204).end();
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    // include LISTS accessList (exactly as Ponder's static default does) — the fork must still drop it.
    const filter: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's',
      address: VAULT,
      topic0: DEPOSIT_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 20558652,
      toBlock: 20558652,
      hasTransactionReceipt: false,
      include: ['transaction.accessList', 'transaction.hash', 'log.address'],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [20558652, 20558652];
    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    await sync.syncBlockData({ interval, logs, syncStore } as any);
    expect(inserted.logs).toHaveLength(1); // degraded gracefully — dropped accessList despite it being in `include`
    expect(inserted.logs[0].transactionHash).toBe(TX_HASH);
  } finally {
    srv.close();
  }
});

test("regression: an 'unknown field' 400 (schema doesn't know accessList) is also dropped, not crashed on", async () => {
  // A second rejection shape: some datasets 400 with a query-PARSE error — "unknown field
  // `accessList`, expected one of `transactionIndex`, `hash`, ..." — instead of "column not found".
  // The fork must map it back to transaction.accessList (a droppable field) and retry, not crash.
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 2_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      if (
        req.url?.includes('finalized-stream') &&
        q.fields?.transaction?.accessList !== undefined
      ) {
        res.writeHead(400);
        res.end(
          "Bad request: couldn't parse request: Couldn't parse query: unknown field `accessList`, expected one of `transactionIndex`, `hash`, `nonce`, `from`",
        );
        return;
      }
      if (
        req.url?.includes('finalized-stream') &&
        q.fromBlock <= 20558652 &&
        (q.toBlock ?? 1e12) >= 20558652
      ) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(JSON.stringify(FIXTURE_BLOCK) + '\n');
        return;
      }
      res.writeHead(204).end();
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filter: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's',
      address: VAULT,
      topic0: DEPOSIT_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 20558652,
      toBlock: 20558652,
      hasTransactionReceipt: false,
      include: ['transaction.accessList'],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'plasma', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [20558652, 20558652];
    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    await sync.syncBlockData({ interval, logs, syncStore } as any);
    expect(inserted.logs).toHaveLength(1); // dropped accessList via the 'unknown field' path, fetched the block
  } finally {
    srv.close();
  }
});

// Server that 400s while `logsBloom` is requested, then serves `serveData ? the fixture : nothing`
// once it's dropped — models a dataset (e.g. Monad old chunks) that lacks the receipt logsBloom.
const missingLogsBloomServer = (serveData: boolean) =>
  http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 2_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      if (
        req.url?.includes('finalized-stream') &&
        q.fields?.transaction?.logsBloom !== undefined
      ) {
        res.writeHead(400);
        res.end(
          "Bad request: couldn't parse request: column 'logs_bloom' is not found in 'transactions'",
        );
        return;
      }
      if (
        serveData &&
        req.url?.includes('finalized-stream') &&
        q.fromBlock <= 20558652 &&
        (q.toBlock ?? 1e12) >= 20558652
      ) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(JSON.stringify(FIXTURE_BLOCK) + '\n');
        return;
      }
      // in-range window (head 2e9), logsBloom already dropped: terminate at the range-end anchor (the
      // event-less case) instead of a mid-range 204 that would now fail closed (issue #47).
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
const runMissingLogsBloom = async (serveData: boolean) => {
  const srv = missingLogsBloomServer(serveData);
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  const inserted = { logs: [] as any[] };
  const syncStore: any = {
    insertLogs: (x: any) => inserted.logs.push(...x.logs),
    insertBlocks: () => {},
    insertTransactions: () => {},
    insertTransactionReceipts: () => {},
    insertTraces: () => {},
  };
  const filter: any = {
    type: 'log',
    chainId: 1,
    sourceId: 's',
    address: VAULT,
    topic0: DEPOSIT_TOPIC0,
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: 20558652,
    toBlock: 20558652,
    hasTransactionReceipt: true,
    include: [],
  };
  const sync = createPortalHistoricalSync({
    common: {
      logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
    } as any,
    chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
    childAddresses: new Map(),
    eventCallbacks: [{ filter }],
  } as any);
  const interval: [number, number] = [20558652, 20558652];
  try {
    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    await sync.syncBlockData({ interval, logs, syncStore } as any);
    return inserted;
  } finally {
    srv.close();
  }
};

test('regression: a NEEDED missing field fails LOUDLY when the range has MATCHED data (no silent substitution)', async () => {
  // logsBloom is NOT-NULL + bloom-load-bearing; a chunk that lacks it AND has matched events must
  // CRASH, not default (a wrong bloom silently drops logs in realtime).
  await expect(runMissingLogsBloom(true)).rejects.toThrow(
    /logs_bloom.*matched data|matched data.*logs_bloom/,
  );
});

test('regression: a NEEDED missing field on an EVENT-LESS range is tolerated (irrelevant old chunks)', async () => {
  // Same missing logsBloom, but the range yields NO matched data (e.g. Monad's pre-schema chunks
  // before any relevant events) → harmless, must NOT crash. The indexer runs fine.
  const inserted = await runMissingLogsBloom(false);
  expect(inserted.logs).toHaveLength(0);
});

// ── FIX 3: the needed-field crash check is EXTEND-LOCAL — it counts only the rows THIS call adds ──────
// On a frontier EXTEND `cd` already carries the base chunk's matched data. The old check inspected the
// WHOLE accumulated `cd`, so an event-less EXTEND tail whose dataset lacks a needed column threw fatally
// (the base's matched data satisfied the "has matched data" test) → the chunk evicted → crash-loop on
// retry. The fix compares the matched-map size BEFORE/AFTER this runStreams call, so only the tail's own
// matched rows arm the crash. This server serves the base block (with logsBloom) at H1, then over the
// newly-finalized tail (after the head advances) 400s the logsBloom column; `tailBlock` controls whether
// the tail — once logsBloom is dropped — yields a MATCHED block or nothing.
const extendFieldDegradeServer = (opts: {
  headFn: () => number;
  baseBlock: number;
  tailBlock: number | undefined;
  tailFrom: number; // the extend tail starts here (coveredTo+1 = H1+1); only this region degrades
}) =>
  http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: opts.headFn() }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = Math.min(q.toBlock ?? 1e12, opts.headFn()); // Portal serves nothing above its head
      const wantsBloom = q.fields?.transaction?.logsBloom !== undefined;
      const isTail = from >= opts.tailFrom; // requests into the newly-finalized EXTEND tail
      // The EXTEND tail 400s while logsBloom is requested (the tail dataset lacks it); the BASE region
      // ([0, H1]) never degrades, so its continuation requests 204 cleanly and no needed-field fires there.
      if (isTail && wantsBloom) {
        res.writeHead(400);
        res.end(
          "Bad request: couldn't parse request: column 'logs_bloom' is not found in 'transactions'",
        );
        return;
      }
      // The BASE request serves the base block (matched data cached under the base fetch).
      if (!isTail && from <= opts.baseBlock && to >= opts.baseBlock) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(JSON.stringify(mkExtendBlock(opts.baseBlock)) + '\n');
        return;
      }
      // Tail with logsBloom already dropped: yield a MATCHED block only when `tailBlock` is set (fatal case).
      if (
        isTail &&
        opts.tailBlock !== undefined &&
        from <= opts.tailBlock &&
        to >= opts.tailBlock
      ) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(JSON.stringify(mkExtendBlock(opts.tailBlock)) + '\n');
        return;
      }
      // `to` is already head-clamped; an in-range window (from ≤ head) with no matched block terminates at
      // the range-end anchor. A request whose from is ABOVE the head (from > to) stays a bare 204 (#47).
      if (from > to) {
        res.writeHead(204).end();
        return;
      }

      anchorRes(res, to);
    });
  });
const mkExtendBlock = (n: number) => ({
  ...FIXTURE_BLOCK,
  header: {
    ...FIXTURE_BLOCK.header,
    number: n,
    hash: `0x${n.toString(16).padStart(64, '0')}`,
  },
  logs: [
    {
      ...FIXTURE_BLOCK.logs[0],
      transactionHash: `0x${(n + 3_000_000).toString(16).padStart(64, '0')}`,
    },
  ],
  transactions: [
    {
      ...FIXTURE_BLOCK.transactions[0],
      hash: `0x${(n + 3_000_000).toString(16).padStart(64, '0')}`,
    },
  ],
});

test('regression (FIX 3): an EVENT-LESS extend tail lacking a needed field over a data-bearing base is TOLERATED (not crash-looped)', async () => {
  // Base block 50 (with logsBloom, matched) is cached truncated at head H1=100. The head advances to
  // H2=200 and interval B extends chunk 0 over (100, 200]. The tail dataset lacks logsBloom (400s) and,
  // once dropped, yields NO matched block. BEFORE FIX: runStreams saw neededMissing={logsBloom} AND the
  // whole cd still held block 50's matched data → threw "missing … matched data" → evict → crash-loop.
  // AFTER FIX: the extend's matchedSize delta is 0 (the tail added nothing) → tolerated, no throw.
  delete process.env.PORTAL_FINALIZED_HEAD; // probe the mock so the head advances
  let head = 100;
  const srv = extendFieldDegradeServer({
    headFn: () => head,
    baseBlock: 50,
    tailBlock: undefined, // tail yields nothing matched → event-less
    tailFrom: 101, // the extend tail is (H1=100, H2]; only it degrades logsBloom
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filter: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's',
      address: VAULT,
      topic0: DEPOSIT_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: undefined, // unbounded → chunk end clamps to the head (frontier chunk)
      hasTransactionReceipt: true, // ← makes logsBloom a NEEDED field
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);

    const iA: [number, number] = [50, 50];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    expect(inserted.logs).toHaveLength(1); // base block 50 cached

    head = 200; // Portal advances → interval B extends chunk 0 over the event-less tail

    const iB: [number, number] = [150, 150];
    // MUST NOT throw — the tail added no matched rows, so the missing logsBloom is tolerated.
    await sync.syncBlockRangeData({
      interval: iB,
      requiredIntervals: [{ interval: iB, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    // no crash; the event-less tail simply contributed nothing new
    expect(inserted.logs).toHaveLength(1);
  } finally {
    srv.close();
  }
});

test('regression (FIX 3): an extend tail that DOES add matched rows while lacking a needed field STILL fails LOUD (the crash is not lost)', async () => {
  // The pair-semantics companion: same base + extend, but the tail — once logsBloom is dropped — yields a
  // MATCHED block (150). Now the extend's matchedSize delta is > 0 with a needed field absent, so the
  // indexer would process an incomplete event: it MUST crash. This pins that FIX 3 narrows the check to
  // the tail's own rows WITHOUT muting a genuinely-incomplete tail.
  delete process.env.PORTAL_FINALIZED_HEAD;
  let head = 100;
  const srv = extendFieldDegradeServer({
    headFn: () => head,
    baseBlock: 50,
    tailBlock: 150, // tail yields a MATCHED block lacking logsBloom → still fatal
    tailFrom: 101, // the extend tail is (H1=100, H2]; only it degrades logsBloom
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filter: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's',
      address: VAULT,
      topic0: DEPOSIT_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: undefined,
      hasTransactionReceipt: true,
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);

    const iA: [number, number] = [50, 50];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });

    head = 200;

    const iB: [number, number] = [150, 150];
    await expect(
      sync.syncBlockRangeData({
        interval: iB,
        requiredIntervals: [{ interval: iB, filter }],
        requiredFactoryIntervals: [],
        syncStore,
      }),
    ).rejects.toThrow(/logs_bloom.*matched data|matched data.*logs_bloom/);
  } finally {
    srv.close();
  }
});

// ── wave 4: the needed-field growth check counts logs POST-re-match, not raw cd.logs.size ─────────────
// The log re-match (INV-6 store parity) lets assembly DROP logs the Portal's merged server-side filter
// over-returns — a factory child's pre-creation logs, or a bounded filter's out-of-range logs. If those
// still counted toward "matched data this call added", a tail of ALL-dropped logs over a dataset missing a
// NEEDED field would arm the needed-field fatal → evict → crash-loop for data the indexer never keeps
// (the exact class of #20's trace/transfer residual, created by the new re-match boundary). Only logs
// surviving `logMatched` count. This server serves the base block (matched by filter A, WITH logsBloom),
// then over the newly-finalized tail 400s the logsBloom column and — once it's dropped — returns a log
// matching bounded filter B ABOVE B.toBlock (out of range → re-matched away by assembly).
const mkTopicBlock = (n: number, topic0: string) => ({
  ...FIXTURE_BLOCK,
  header: {
    ...FIXTURE_BLOCK.header,
    number: n,
    hash: `0x${n.toString(16).padStart(64, '0')}`,
  },
  logs: [
    {
      ...FIXTURE_BLOCK.logs[0],
      topics: [topic0, ...FIXTURE_BLOCK.logs[0].topics.slice(1)],
      transactionHash: `0x${(n + 3_000_000).toString(16).padStart(64, '0')}`,
    },
  ],
  transactions: [
    {
      ...FIXTURE_BLOCK.transactions[0],
      hash: `0x${(n + 3_000_000).toString(16).padStart(64, '0')}`,
    },
  ],
});
const extendReMatchDropServer = (opts: {
  headFn: () => number;
  baseBlock: number;
  baseTopic0: string;
  tailBlock: number;
  tailTopic0: string;
  tailFrom: number; // the extend tail starts here (coveredTo+1); only this region degrades logsBloom
}) =>
  http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: opts.headFn() }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = Math.min(q.toBlock ?? 1e12, opts.headFn());
      const wantsBloom = q.fields?.transaction?.logsBloom !== undefined;
      const isTail = from >= opts.tailFrom;
      // The EXTEND tail lacks logsBloom → 400 while it's requested (the BASE region never degrades).
      if (isTail && wantsBloom) {
        res.writeHead(400);
        res.end(
          "Bad request: couldn't parse request: column 'logs_bloom' is not found in 'transactions'",
        );
        return;
      }
      // BASE: a log matched by filter A (cached under the base fetch, WITH logsBloom).
      if (!isTail && from <= opts.baseBlock && to >= opts.baseBlock) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(
          JSON.stringify(mkTopicBlock(opts.baseBlock, opts.baseTopic0)) + '\n',
        );
        return;
      }
      // TAIL (logsBloom dropped): a log the merged request returns but assembly re-matches away.
      if (isTail && from <= opts.tailBlock && to >= opts.tailBlock) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(
          JSON.stringify(mkTopicBlock(opts.tailBlock, opts.tailTopic0)) + '\n',
        );
        return;
      }
      // `to` is head-clamped; an in-range no-match window terminates at the range-end anchor, and a
      // from-above-head request (from > to) stays a bare 204 (issue #47).
      if (from > to) {
        res.writeHead(204).end();
        return;
      }

      anchorRes(res, to);
    });
  });

test('regression (wave 4): an extend tail whose only new logs are RE-MATCH-DROPPED while lacking a needed field is TOLERATED (not crash-looped)', async () => {
  // Base block 50 (matched by unbounded filter A, WITH logsBloom) is cached truncated at head H1=100. The
  // head advances to H2=200 and interval B extends chunk 0 over (100, 200]. The tail lacks logsBloom (400s)
  // and — once it's dropped — returns ONLY a log matching bounded filter B (topic OTHER) at block 150,
  // ABOVE B.toBlock=50: assembly's `logMatched` re-matches it AWAY (out of B's range; not A's topic).
  // BEFORE the wave-4 seam fix: runStreams counted raw cd.logs.size, which grew by the tail block →
  // neededMissing={logsBloom} armed → threw "missing … matched data" → evict → crash-loop for a log the
  // indexer never keeps. AFTER: only logMatched-surviving logs count → the tail contributes 0 → tolerated.
  delete process.env.PORTAL_FINALIZED_HEAD; // probe the mock so the head advances
  const OTHER_TOPIC0 = `0x${'bb'.repeat(32)}`;
  let head = 100;
  const srv = extendReMatchDropServer({
    headFn: () => head,
    baseBlock: 50,
    baseTopic0: DEPOSIT_TOPIC0, // base log matches filter A → cached
    tailBlock: 150,
    tailTopic0: OTHER_TOPIC0, // tail log matches only bounded filter B, above its range → dropped
    tailFrom: 101, // the extend tail is (H1=100, H2]; only it degrades logsBloom
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filterA: any = {
      type: 'log',
      chainId: 1,
      sourceId: 'a',
      address: VAULT,
      topic0: DEPOSIT_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: undefined, // unbounded → the frontier chunk that extends as the head advances
      hasTransactionReceipt: true, // ← makes logsBloom a NEEDED field
      include: [],
    };
    const filterB: any = {
      type: 'log',
      chainId: 1,
      sourceId: 'b',
      address: VAULT,
      topic0: OTHER_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: 50, // bounded at 50 → its topic rides the merged request but a 150 log is out of range
      hasTransactionReceipt: true,
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter: filterA }, { filter: filterB }],
    } as any);

    const iA: [number, number] = [50, 50];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter: filterA }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    expect(inserted.logs).toHaveLength(1); // base block 50 (filter A) cached

    head = 200; // Portal advances → interval B extends chunk 0 over the re-match-dropped tail

    const iB: [number, number] = [150, 150];
    // MUST NOT throw — the tail's only new log is re-match-dropped, so the missing logsBloom is tolerated.
    await sync.syncBlockRangeData({
      interval: iB,
      requiredIntervals: [{ interval: iB, filter: filterA }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    // no crash; the out-of-range B log contributed nothing (assembly drops it too)
    expect(inserted.logs).toHaveLength(1);
  } finally {
    srv.close();
  }
});

test('regression: a dataset that starts after genesis (TAC starts at block 1) is clamped forward, not crashed on', async () => {
  // The Portal 400s "dataset starts from block N" when queried below its first block. The fork must
  // clamp the cursor to N and continue — NOT throw an unhandledRejection (which killed the whole
  // multichain app when TAC's dataset started at block 1 and startBlock was 0).
  const START = 1000;
  let clampedTo = -1;
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 2_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      if (req.url?.includes('finalized-stream')) {
        if ((q.fromBlock ?? 0) < START) {
          res.writeHead(400);
          res.end('Bad request: dataset starts from block ' + START);
          return;
        }
        clampedTo = q.fromBlock;
        // the clamped in-range query [START, to] (head 2e9) has no matched data → terminate at the
        // range-end anchor, not a mid-range 204 that would now fail closed (issue #47).
        anchorRes(res, q.toBlock ?? 1e12);
        return;
      }
      res.writeHead(204).end();
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const syncStore: any = {
      insertLogs: () => {},
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filter: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's',
      address: VAULT,
      topic0: DEPOSIT_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: 5000,
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'tac', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [0, 5000];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    }); // must not throw
    expect(clampedTo).toBe(START); // the retry was clamped to the dataset start, not re-issued from 0
  } finally {
    srv.close();
  }
});

test('regression (C6): deep matched trace is stored at its FULL-tree pre-order index (7), not filter-local (0)', async () => {
  // Ponder's RPC sync numbers trace_index as the pre-order DFS rank over each tx's FULL call tree,
  // THEN filters — so a lone deep match keeps its true position. Portal used to push the trace
  // filter server-side and rank over the matched SUBSET (→ 0). The fake server emulates Portal's
  // callTo filtering, so this fails before the fix (index 0) and passes after (index 7).
  process.env.PORTAL_FINALIZED_HEAD = String(22200011 + 1_000_000); // no finality fallback / no head call
  const BLOCK = 22200011;
  const TX_INDEX = 111;
  const TX_HASH = '0x' + '7e'.repeat(32);
  const B_HASH = '0x' + 'b1'.repeat(32);
  const TARGET = '0x000000000000000000000000000000000000dead';
  const OTHER = '0x000000000000000000000000000000000000beef';
  const mkTrace = (traceAddress: number[], to: string, subtraces: number) => ({
    transactionIndex: TX_INDEX,
    traceAddress,
    type: 'call',
    subtraces,
    error: null,
    revertReason: null,
    action: {
      from: OTHER,
      to,
      value: '0x0',
      gas: '0x1000',
      input: '0xabcdabcd',
      sighash: '0xabcdabcd',
      type: 'call',
      callType: 'call',
    },
    result: { gasUsed: '0x10', output: '0x' },
  });
  // pre-order: [] 0 · [0] 1 · [0,0] 2 · [0,1] 3 · [0,2] 4 · [1] 5 · [1,0] 6 · [1,0,0] 7(MATCH)
  const ALL = [
    mkTrace([], OTHER, 2),
    mkTrace([0], OTHER, 3),
    mkTrace([0, 0], OTHER, 0),
    mkTrace([0, 1], OTHER, 0),
    mkTrace([0, 2], OTHER, 0),
    mkTrace([1], OTHER, 1),
    mkTrace([1, 0], OTHER, 1),
    mkTrace([1, 0, 0], TARGET, 0),
  ];
  const STREAM = [
    ALL[7],
    ALL[0],
    ALL[5],
    ALL[2],
    ALL[6],
    ALL[1],
    ALL[4],
    ALL[3],
  ]; // shuffled → must sort
  const TX = {
    transactionIndex: TX_INDEX,
    hash: TX_HASH,
    from: OTHER,
    to: TARGET,
    input: '0xabcdabcd',
    value: '0x0',
    nonce: 1,
    gas: '0x100000',
    gasPrice: '0x1',
    type: 0,
    r: '0x' + '11'.repeat(32),
    s: '0x' + '22'.repeat(32),
    v: '0x1',
  };
  const HEADER = {
    number: BLOCK,
    hash: B_HASH,
    parentHash: '0x' + '00'.repeat(32),
    timestamp: 1700000000,
    logsBloom: '0x' + '00'.repeat(256),
    miner: OTHER,
    gasUsed: '0xabc',
    gasLimit: '0x1c9c380',
    stateRoot: '0x' + '22'.repeat(32),
    receiptsRoot: '0x' + '33'.repeat(32),
    transactionsRoot: '0x' + '44'.repeat(32),
    size: '0x500',
    difficulty: '0x0',
    extraData: '0x',
  };
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      const covers =
        (q.fromBlock ?? 0) <= BLOCK && (q.toBlock ?? 1e12) >= BLOCK;
      if (req.url?.includes('finalized-stream') && q.traces && covers) {
        const callTo = new Set(
          (q.traces as any[])
            .flatMap((r) => r.callTo ?? [])
            .map((s: string) => s.toLowerCase()),
        );
        let traces = STREAM;
        if (callTo.size)
          traces = traces.filter((t) => callTo.has(t.action.to.toLowerCase())); // emulate Portal server-side filter
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(
          JSON.stringify({ header: HEADER, traces, transactions: [TX] }) + '\n',
        );
      } else {
        res.writeHead(204).end();
      }
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted: any[] = [];
    const syncStore: any = {
      insertLogs: () => {},
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: (x: any) => inserted.push(...x.traces),
    };
    const filter: any = {
      type: 'trace',
      chainId: 1,
      sourceId: 'deep:trace',
      fromAddress: undefined,
      toAddress: TARGET,
      functionSelector: undefined,
      callType: undefined,
      includeReverted: false,
      fromBlock: BLOCK,
      toBlock: BLOCK,
      hasTransactionReceipt: false,
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [BLOCK, BLOCK];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    await sync.syncBlockData({ interval, logs: [], syncStore } as any);
    expect(inserted).toHaveLength(1); // count parity: only the matched trace is stored (like RPC)
    expect(inserted[0].trace.trace.index).toBe(7); // INDEX parity: full-tree position, not filter-local 0
    expect((inserted[0].trace.trace.to as string)?.toLowerCase()).toBe(TARGET);
  } finally {
    srv.close();
    delete process.env.PORTAL_FINALIZED_HEAD;
  }
});

test('regression (FIX 4): a trace filter with hasTransactionReceipt inserts a receipt for every matched trace tx (no "Missing transaction receipt")', async () => {
  // A trace/transfer source with hasTransactionReceipt needs a receipt row for each matched trace's
  // parent tx. Those txs ride ONLY on the trace query (no log/tx-filter branch sees them), so before
  // FIX 4 assembleRange emitted the trace + its transaction but NO receipt → downstream buildEvents
  // throws "Missing transaction receipt" on a legit trace-receipt config. The mock projects the receipt
  // fields onto the tx (needReceipts ⇒ RECEIPT_FIELDS) and this test asserts the receipt is inserted.
  process.env.PORTAL_FINALIZED_HEAD = String(22200011 + 1_000_000); // no finality fallback / no head call
  const BLOCK = 22200011;
  const TX_INDEX = 111;
  const TXH = '0x' + '7e'.repeat(32);
  const B_HASH = '0x' + 'b1'.repeat(32);
  const TARGET = '0x000000000000000000000000000000000000dead';
  const OTHER = '0x000000000000000000000000000000000000beef';
  const HEADER = {
    number: BLOCK,
    hash: B_HASH,
    parentHash: '0x' + '00'.repeat(32),
    timestamp: 1700000000,
    logsBloom: '0x' + '00'.repeat(256),
    miner: OTHER,
    gasUsed: '0xabc',
    gasLimit: '0x1c9c380',
    stateRoot: '0x' + '22'.repeat(32),
    receiptsRoot: '0x' + '33'.repeat(32),
    transactionsRoot: '0x' + '44'.repeat(32),
    size: '0x500',
    difficulty: '0x0',
    extraData: '0x',
  };
  // matched trace: a top-level call to TARGET; its parent tx carries the receipt fields.
  const TRACE = {
    transactionIndex: TX_INDEX,
    traceAddress: [],
    type: 'call',
    subtraces: 0,
    error: null,
    revertReason: null,
    action: {
      from: OTHER,
      to: TARGET,
      value: '0x0',
      gas: '0x1000',
      input: '0xabcdabcd',
      sighash: '0xabcdabcd',
      type: 'call',
      callType: 'call',
    },
    result: { gasUsed: '0x10', output: '0x' },
  };
  const TX = {
    transactionIndex: TX_INDEX,
    hash: TXH,
    from: OTHER,
    to: TARGET,
    input: '0xabcdabcd',
    value: '0x0',
    nonce: 1,
    gas: '0x100000',
    gasPrice: '0x1',
    type: 0,
    r: '0x' + '11'.repeat(32),
    s: '0x' + '22'.repeat(32),
    v: '0x1',
    // receipt fields (the trace query projects RECEIPT_FIELDS when needReceipts) — toSyncReceipt reads these
    status: '0x1',
    cumulativeGasUsed: '0x5208',
    gasUsed: '0x5208',
    effectiveGasPrice: '0x1',
    logsBloom: '0x' + '00'.repeat(256),
    contractAddress: null,
  };
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      const covers =
        (q.fromBlock ?? 0) <= BLOCK && (q.toBlock ?? 1e12) >= BLOCK;
      if (req.url?.includes('finalized-stream') && q.traces && covers) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(
          JSON.stringify({
            header: HEADER,
            traces: [TRACE],
            transactions: [TX],
          }) + '\n',
        );
      } else {
        res.writeHead(204).end();
      }
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { traces: [] as any[], receipts: [] as any[] };
    const syncStore: any = {
      insertLogs: () => {},
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: (x: any) =>
        inserted.receipts.push(...x.transactionReceipts),
      insertTraces: (x: any) => inserted.traces.push(...x.traces),
    };
    const filter: any = {
      type: 'trace',
      chainId: 1,
      sourceId: 'trace:receipt',
      fromAddress: undefined,
      toAddress: TARGET,
      functionSelector: undefined,
      callType: undefined,
      includeReverted: false,
      fromBlock: BLOCK,
      toBlock: BLOCK,
      hasTransactionReceipt: true, // ← the trace-receipt config the fix must honor
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [BLOCK, BLOCK];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    await sync.syncBlockData({ interval, logs: [], syncStore } as any);

    expect(inserted.traces).toHaveLength(1); // the matched trace is stored
    // THE REGRESSION: its parent tx's receipt is inserted (before FIX 4: zero receipts → downstream throws)
    expect(inserted.receipts).toHaveLength(1);
    expect(inserted.receipts[0].transactionHash).toBe(TXH);
    expect(inserted.receipts[0].blockHash).toBe(B_HASH);
  } finally {
    srv.close();
    delete process.env.PORTAL_FINALIZED_HEAD;
  }
});

test('regression (FIX 4): a TRANSFER filter with hasTransactionReceipt inserts a receipt for the matched transfer tx (pins the transferFilters half of needTraceReceipts)', async () => {
  // The FIX-4 sibling of the trace-receipt case: `needTraceReceipts` ORs the transfer half
  // (portal-assemble.ts: `|| spec.transferFilters.some((f) => f.hasTransactionReceipt)`). A `type:'transfer'`
  // source with hasTransactionReceipt rides the SAME unfiltered trace query, and its matched transfer's
  // parent tx is seen by no log/tx branch — so without the transfer half of the OR, assembleRange would
  // emit the transfer's transaction but NO receipt → buildEvents throws "Missing transaction receipt".
  // Unlike the trace filter, a transfer only matches a NON-ZERO value frame (isTransferFilterMatched),
  // so this mock carries `action.value: '0x1'`.
  process.env.PORTAL_FINALIZED_HEAD = String(22200011 + 1_000_000); // no finality fallback / no head call
  const BLOCK = 22200011;
  const TX_INDEX = 111;
  const TXH = '0x' + '9d'.repeat(32);
  const B_HASH = '0x' + 'c2'.repeat(32);
  const TARGET = '0x000000000000000000000000000000000000dead';
  const OTHER = '0x000000000000000000000000000000000000beef';
  const HEADER = {
    number: BLOCK,
    hash: B_HASH,
    parentHash: '0x' + '00'.repeat(32),
    timestamp: 1700000000,
    logsBloom: '0x' + '00'.repeat(256),
    miner: OTHER,
    gasUsed: '0xabc',
    gasLimit: '0x1c9c380',
    stateRoot: '0x' + '22'.repeat(32),
    receiptsRoot: '0x' + '33'.repeat(32),
    transactionsRoot: '0x' + '44'.repeat(32),
    size: '0x500',
    difficulty: '0x0',
    extraData: '0x',
  };
  // matched transfer: a top-level value-bearing call to TARGET; its parent tx carries the receipt fields.
  const TRACE = {
    transactionIndex: TX_INDEX,
    traceAddress: [],
    type: 'call',
    subtraces: 0,
    error: null,
    revertReason: null,
    action: {
      from: OTHER,
      to: TARGET,
      value: '0x1', // NON-ZERO ⇒ isTransferFilterMatched (a zero-value frame would not match)
      gas: '0x1000',
      input: '0x',
      sighash: '0x',
      type: 'call',
      callType: 'call',
    },
    result: { gasUsed: '0x10', output: '0x' },
  };
  const TX = {
    transactionIndex: TX_INDEX,
    hash: TXH,
    from: OTHER,
    to: TARGET,
    input: '0x',
    value: '0x1',
    nonce: 1,
    gas: '0x100000',
    gasPrice: '0x1',
    type: 0,
    r: '0x' + '11'.repeat(32),
    s: '0x' + '22'.repeat(32),
    v: '0x1',
    // receipt fields (the trace query projects RECEIPT_FIELDS when needReceipts) — toSyncReceipt reads these
    status: '0x1',
    cumulativeGasUsed: '0x5208',
    gasUsed: '0x5208',
    effectiveGasPrice: '0x1',
    logsBloom: '0x' + '00'.repeat(256),
    contractAddress: null,
  };
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      const covers =
        (q.fromBlock ?? 0) <= BLOCK && (q.toBlock ?? 1e12) >= BLOCK;
      if (req.url?.includes('finalized-stream') && q.traces && covers) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(
          JSON.stringify({
            header: HEADER,
            traces: [TRACE],
            transactions: [TX],
          }) + '\n',
        );
      } else {
        res.writeHead(204).end();
      }
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { traces: [] as any[], receipts: [] as any[] };
    const syncStore: any = {
      insertLogs: () => {},
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: (x: any) =>
        inserted.receipts.push(...x.transactionReceipts),
      insertTraces: (x: any) => inserted.traces.push(...x.traces),
    };
    const filter: any = {
      type: 'transfer',
      chainId: 1,
      sourceId: 'transfer:receipt',
      fromAddress: undefined,
      toAddress: TARGET,
      includeReverted: false,
      fromBlock: BLOCK,
      toBlock: BLOCK,
      hasTransactionReceipt: true, // ← the transfer-receipt config the fix must honor
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [BLOCK, BLOCK];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    await sync.syncBlockData({ interval, logs: [], syncStore } as any);

    expect(inserted.traces).toHaveLength(1); // the matched transfer is stored
    // THE REGRESSION: its parent tx's receipt is inserted (drops if the transfer half of the OR is removed)
    expect(inserted.receipts).toHaveLength(1);
    expect(inserted.receipts[0].transactionHash).toBe(TXH);
    expect(inserted.receipts[0].blockHash).toBe(B_HASH);
  } finally {
    srv.close();
    delete process.env.PORTAL_FINALIZED_HEAD;
  }
});

test('regression: an account source WITHOUT hasTransactionReceipt still inserts a receipt for every matched tx', async () => {
  // Pins the HARDENED contract end-to-end, not a production repro: upstream guarantees
  // hasTransactionReceipt: true on every account tx filter (literal type + build), so the `any`-forced
  // `false` below is unconstructible there today. The Portal path must not depend on that invariant —
  // ponder's buildEvents reads `transactionReceipt.status` on EVERY transaction event to apply
  // `includeReverted` (positional cursor over the stored receipts, no identity check) and the RPC path
  // adds every tx-filter-matched tx to requiredTransactionReceipts UNCONDITIONALLY (sync-historical).
  // Were tx-filter receipts still gated on hasTransactionReceipt and upstream relaxed it: ZERO receipts
  // → `undefined.status` TypeError in buildEvents; a SPARSE receipt set (receipt-bearing log source +
  // account source) → the cursor lands on a NEIGHBOR's receipt and events are silently dropped or
  // emitted for reverted txs, with the intervals marked synced (permanent).
  process.env.PORTAL_FINALIZED_HEAD = String(22200011 + 1_000_000); // no finality fallback / no head call
  const BLOCK = 22200011;
  const TX_INDEX = 111;
  const TXH = '0x' + 'ac'.repeat(32);
  const B_HASH = '0x' + 'd3'.repeat(32);
  const SENDER = '0x000000000000000000000000000000000000beef';
  const HEADER = {
    number: BLOCK,
    hash: B_HASH,
    parentHash: '0x' + '00'.repeat(32),
    timestamp: 1700000000,
    logsBloom: '0x' + '00'.repeat(256),
    miner: SENDER,
    gasUsed: '0xabc',
    gasLimit: '0x1c9c380',
    stateRoot: '0x' + '22'.repeat(32),
    receiptsRoot: '0x' + '33'.repeat(32),
    transactionsRoot: '0x' + '44'.repeat(32),
    size: '0x500',
    difficulty: '0x0',
    extraData: '0x',
  };
  const TX = {
    transactionIndex: TX_INDEX,
    hash: TXH,
    from: SENDER,
    to: '0x000000000000000000000000000000000000dead',
    input: '0x',
    value: '0x1',
    nonce: 1,
    gas: '0x100000',
    gasPrice: '0x1',
    type: 0,
    r: '0x' + '11'.repeat(32),
    s: '0x' + '22'.repeat(32),
    v: '0x1',
    // receipt fields (the tx query ALWAYS projects RECEIPT_FIELDS) — toSyncReceipt reads these
    status: '0x1',
    cumulativeGasUsed: '0x5208',
    gasUsed: '0x5208',
    effectiveGasPrice: '0x1',
    logsBloom: '0x' + '00'.repeat(256),
    contractAddress: null,
  };
  const txQueries: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      const covers =
        (q.fromBlock ?? 0) <= BLOCK && (q.toBlock ?? 1e12) >= BLOCK;
      if (req.url?.includes('finalized-stream') && q.transactions && covers) {
        txQueries.push(q);
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(JSON.stringify({ header: HEADER, transactions: [TX] }) + '\n');
      } else {
        res.writeHead(204).end();
      }
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { txs: [] as any[], receipts: [] as any[] };
    const syncStore: any = {
      insertLogs: () => {},
      insertBlocks: () => {},
      insertTransactions: (x: any) => inserted.txs.push(...x.transactions),
      insertTransactionReceipts: (x: any) =>
        inserted.receipts.push(...x.transactionReceipts),
      insertTraces: () => {},
    };
    const filter: any = {
      type: 'transaction',
      chainId: 1,
      sourceId: 'acct:no-receipt-flag',
      fromAddress: SENDER,
      toAddress: undefined,
      includeReverted: false,
      fromBlock: BLOCK,
      toBlock: BLOCK,
      hasTransactionReceipt: false, // ← NO source asks for receipts, yet buildEvents needs one per matched tx
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [BLOCK, BLOCK];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    await sync.syncBlockData({ interval, logs: [], syncStore } as any);

    expect(inserted.txs).toHaveLength(1); // the matched account tx is stored
    // THE HARDENED CONTRACT: its receipt is inserted despite hasTransactionReceipt:false everywhere
    expect(inserted.receipts).toHaveLength(1);
    expect(inserted.receipts[0].transactionHash).toBe(TXH);
    expect(inserted.receipts[0].blockHash).toBe(B_HASH);
    // and the tx query itself projected the receipt columns
    expect(txQueries[0].fields.transaction.status).toBe(true);
    expect(txQueries[0].fields.transaction.gasUsed).toBe(true);
  } finally {
    srv.close();
    delete process.env.PORTAL_FINALIZED_HEAD;
  }
});

test('merge: N same-address event filters collapse to ONE log request (unioned topic0) — keeps body small', async () => {
  // Ponder emits one filter per event; a 24-event EVault would otherwise repeat the child-address
  // list 24× in one body and blow past MAX_RAW_QUERY_SIZE. mergeLogRequests must fold them into one.
  const ADDR = '0x' + 'cc'.repeat(20);
  const topic0s = Array.from(
    { length: 6 },
    (_, i) => '0x' + (i + 1).toString(16).padStart(64, '0'),
  );
  const dataQueries: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      if (q.logs) dataQueries.push(q);
      // in-range window (pinned head 2e9): terminate at the range-end anchor so the merged single request
      // (asserted below) ends cleanly instead of a mid-range 204 that would now fail closed (issue #47).
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p = await new Promise<number>((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const filters = topic0s.map((t, i) => ({
      type: 'log',
      chainId: 1,
      sourceId: `evault:e${i}`,
      address: ADDR,
      topic0: t,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: 100,
      hasTransactionReceipt: false,
      include: [],
    }));
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      },
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` },
      childAddresses: new Map(),
      eventCallbacks: filters.map((f) => ({ filter: f })),
    } as any);
    await sync.syncBlockRangeData({
      interval: [0, 100],
      requiredIntervals: filters.map((f) => ({
        interval: [0, 100],
        filter: f,
      })),
      requiredFactoryIntervals: [],
      syncStore: {
        insertLogs() {},
        insertBlocks() {},
        insertTransactions() {},
        insertTransactionReceipts() {},
        insertTraces() {},
      },
    } as any);
    const merged = dataQueries.find(
      (q) => (q.logs?.[0]?.topic0?.length ?? 0) > 1,
    );
    expect(merged).toBeDefined();
    expect(merged.logs).toHaveLength(1); // ONE request, not 6
    expect(
      new Set(merged.logs[0].topic0.map((x: string) => x.toLowerCase())).size,
    ).toBe(6); // all selectors unioned
  } finally {
    srv.close();
  }
});

// ── discovered factory children must be PERSISTED (restart correctness) ──────────────────────────
// Ponder's core marks requiredFactoryIntervals cached (syncStore.insertIntervals) after EVERY
// interval regardless of the sync implementation, and on startup loads children ONLY from the store
// (runtime/index.ts getChildAddresses) — there is no re-derivation from stored logs. Stock sync
// therefore persists children inside syncBlockRangeData (sync-historical/index.ts). The portal sync
// only mutated the in-memory map: a single run worked (the map is shared), but any restart/resume
// loaded an EMPTY child set against already-cached factory intervals — discovery never re-ran and
// every factory-child event was silently skipped. These tests pin the persistence invariant.
const FACTORY_ADDR = '0x' + 'fa'.repeat(20);
const PROXY_CREATED_TOPIC0 = '0x' + '01'.repeat(32); // factory's creation-event selector
const CHILD_ADDR = '0x' + 'c1'.repeat(20);
const CHILD_CREATED_AT = 100; // child created here (ProxyCreated log, child in topic1)
const CHILD_EVENT_AT = 200; //  child emits its Deposit here
const FACTORY_RANGE_END = 300;

const mkFactory = (): any => ({
  id: 'factory_evault',
  type: 'log',
  chainId: 1,
  sourceId: 'evault',
  address: FACTORY_ADDR,
  eventSelector: PROXY_CREATED_TOPIC0,
  childAddressLocation: 'topic1',
  fromBlock: 0,
  toBlock: undefined,
});
// log filter whose address IS the factory (ponder's shape for factory sources)
const mkFactoryFilter = (factory: any): any => ({
  type: 'log',
  chainId: 1,
  sourceId: 'evault:deposit',
  address: factory,
  topic0: DEPOSIT_TOPIC0,
  topic1: null,
  topic2: null,
  topic3: null,
  fromBlock: 0,
  toBlock: FACTORY_RANGE_END,
  hasTransactionReceipt: false,
  include: [],
});

const mkFactoryHeader = (num: number) => ({
  number: num,
  hash: '0x' + num.toString(16).padStart(64, '0'),
  parentHash: '0x' + '00'.repeat(32),
  timestamp: 1_700_000_000 + num,
  logsBloom: '0x' + '00'.repeat(256),
  miner: '0x' + '99'.repeat(20),
  gasUsed: '0x1',
  gasLimit: '0x1c9c380',
  stateRoot: '0x' + '22'.repeat(32),
  receiptsRoot: '0x' + '33'.repeat(32),
  transactionsRoot: '0x' + '44'.repeat(32),
  size: '0x500',
  difficulty: '0x0',
  extraData: '0x',
});

/** Serves: the factory's ProxyCreated log for discovery requests, the child's Deposit log (+ parent
 * tx) for data requests that carry the DISCOVERED child address. Emulates the Portal's server-side
 * address filter — a data request without the child address gets nothing, exactly like production. */
const factoryPortalServer = () =>
  http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 1_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 1e12;
      const matches = (topic0: string, addr: string) =>
        ((q.logs ?? []) as any[]).some(
          (s) =>
            (!s.topic0 ||
              s.topic0
                .map((x: string) => x.toLowerCase())
                .includes(topic0.toLowerCase())) &&
            (!s.address ||
              s.address
                .map((x: string) => x.toLowerCase())
                .includes(addr.toLowerCase())),
        );
      const out: any[] = [];
      if (
        from <= CHILD_CREATED_AT &&
        to >= CHILD_CREATED_AT &&
        matches(PROXY_CREATED_TOPIC0, FACTORY_ADDR)
      ) {
        out.push({
          header: mkFactoryHeader(CHILD_CREATED_AT),
          logs: [
            {
              address: FACTORY_ADDR,
              topics: [
                PROXY_CREATED_TOPIC0,
                '0x' + '00'.repeat(12) + CHILD_ADDR.slice(2),
              ],
              data: '0x',
              transactionHash: '0x' + 'f0'.repeat(32),
              transactionIndex: 0,
              logIndex: 0,
            },
          ],
        });
      }
      if (
        from <= CHILD_EVENT_AT &&
        to >= CHILD_EVENT_AT &&
        matches(DEPOSIT_TOPIC0, CHILD_ADDR)
      ) {
        out.push({
          header: mkFactoryHeader(CHILD_EVENT_AT),
          logs: [
            {
              address: CHILD_ADDR,
              topics: [DEPOSIT_TOPIC0],
              data: '0x',
              transactionHash: '0x' + 'd1'.repeat(32),
              transactionIndex: 0,
              logIndex: 0,
            },
          ],
          transactions: [
            {
              transactionIndex: 0,
              hash: '0x' + 'd1'.repeat(32),
              from: '0x' + 'ee'.repeat(20),
              to: CHILD_ADDR,
              input: '0x',
              value: '0x0',
              nonce: 0,
              gas: '0x1',
              gasPrice: '0x1',
              type: 0,
            },
          ],
        });
      }
      // in-range window (head 1e9 ≫ chunk 0): terminate at the range-end anchor (issue #47), carrying any
      // matched discovery/data blocks — a mid-range 204 would now fail closed and hang the stream.
      anchorRes(res, to, out);
    });
  });

// The REAL sync store keys factory_addresses by STORE IDENTITY, not by factory.id: both
// insertChildAddresses and getChildAddresses strip { id, sourceId } and upsert/select the factories
// row on the remaining value (sync-store/index.ts, UNIQUE (factory)). So two factories that differ
// ONLY in id/sourceId (aliases) share ONE row-set. The mock MUST mirror this identity — keying the
// `persisted` map by factory.id (as it did before) would hide the alias-duplicate hole INV-17 closes,
// because two aliased factories would read into different mock buckets and each look empty.
const storeFactoryKey = (factory: any): string => {
  const rest: Record<string, unknown> = {};
  for (const key of Object.keys(factory).sort()) {
    if (key === 'id' || key === 'sourceId') continue;

    rest[key] = factory[key];
  }

  return JSON.stringify(rest);
};

// `persisted` models the store's factory_addresses rows keyed by STORE IDENTITY → child → block.
// getChildAddresses returns the upstream min-merged view; the default is an empty store (fresh
// backfill). Like the real tx-scoped store, a successful insertChildAddresses is visible to
// subsequent getChildAddresses reads (min-merged); a test simulating a transaction ROLLBACK clears
// the map it passed in.
const mkFactorySyncStore = (
  onInsertChildren: (p: any) => void,
  persisted?: Map<string, Map<string, number>>,
): any => {
  const store = persisted ?? new Map<string, Map<string, number>>();
  return {
    insertLogs: () => {},
    insertBlocks: () => {},
    insertTransactions: () => {},
    insertTransactionReceipts: () => {},
    insertTraces: () => {},
    // async: upstream's promiseAllSettledWithThrow calls .catch on the returned value directly
    insertChildAddresses: async (p: any) => {
      onInsertChildren(p);
      const key = storeFactoryKey(p.factory);
      let rows = store.get(key);
      if (rows === undefined) {
        rows = new Map();
        store.set(key, rows);
      }
      for (const [address, block] of p.childAddresses) {
        const prev = rows.get(address);
        if (prev === undefined || prev > block) rows.set(address, block);
      }
    },
    getChildAddresses: async ({ factory }: any) =>
      new Map(store.get(storeFactoryKey(factory)) ?? []),
  };
};

const mkFactorySync = (
  port: number,
  factory: any,
  filter: any,
  childAddresses: Map<string, Map<string, number>>,
) =>
  createPortalHistoricalSync({
    common: {
      logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
    } as any,
    chain: {
      id: 1,
      name: 'mainnet',
      portal: `http://localhost:${port}`,
    } as any,
    childAddresses: childAddresses as any,
    eventCallbacks: [{ filter }],
  } as any);

// ── issue #50: bounded time-to-first-durable-commit warmup ───────────────────────────────────────
const cx50PlainFilter = (toBlock = 100_000): any => ({
  type: 'log',
  chainId: 1,
  sourceId: 'cx50',
  address: VAULT,
  topic0: DEPOSIT_TOPIC0,
  topic1: null,
  topic2: null,
  topic3: null,
  fromBlock: 0,
  toBlock,
  hasTransactionReceipt: false,
  include: [],
});

const cx50FactoryTraceFilter = (factory: any, toBlock = 10_000_000): any => ({
  type: 'trace',
  chainId: 1,
  sourceId: 'cx50:factory-trace',
  fromAddress: undefined,
  toAddress: factory,
  functionSelector: undefined,
  callType: undefined,
  includeReverted: false,
  fromBlock: 0,
  toBlock,
  hasTransactionReceipt: false,
  include: [],
});

const cx50SyncStore = (insertedLogs?: any[]): any => ({
  insertLogs: (x: any) => {
    insertedLogs?.push(...x.logs);
  },
  insertBlocks: () => {},
  insertTransactions: () => {},
  insertTransactionReceipts: () => {},
  insertTraces: () => {},
  insertChildAddresses: async () => {},
  getChildAddresses: async () => new Map(),
});

const cx50Sync = (
  port: number,
  filter: any,
  childAddresses: Map<string, Map<string, number>> = new Map(),
) =>
  createPortalHistoricalSync({
    common: {
      logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
    } as any,
    chain: {
      id: 1,
      name: 'mainnet',
      portal: `http://localhost:${port}`,
    } as any,
    childAddresses: childAddresses as any,
    eventCallbacks: [{ filter }],
  } as any);

const cx50HasTopic = (q: any, topic0: string): boolean =>
  ((q.logs ?? []) as any[]).some((s) =>
    (s.topic0 ?? []).map((x: string) => x.toLowerCase()).includes(topic0),
  );

const cx50HasAddress = (q: any, address: string): boolean =>
  ((q.logs ?? []) as any[]).some((s) =>
    (s.address ?? [])
      .map((x: string) => x.toLowerCase())
      .includes(address.toLowerCase()),
  );

const cx50ChildEventBlock = (num: number, child: string) => ({
  header: mkFactoryHeader(num),
  logs: [
    {
      address: child,
      topics: [DEPOSIT_TOPIC0],
      data: '0x',
      transactionHash: '0x' + '50'.repeat(32),
      transactionIndex: 0,
      logIndex: 0,
    },
  ],
  transactions: [
    {
      transactionIndex: 0,
      hash: '0x' + '50'.repeat(32),
      from: '0x' + 'ee'.repeat(20),
      to: child,
      input: '0x',
      value: '0x0',
      nonce: 0,
      gas: '0x1',
      gasPrice: '0x1',
      type: 0,
    },
  ],
});

test('#50 T1: first data fetch is warmup-bounded', async () => {
  process.env.PORTAL_WARMUP_BLOCKS = '1000';
  process.env.PORTAL_READAHEAD = '0';
  const dataRequests: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 2_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      dataRequests.push(q);
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const filter = cx50PlainFilter();
    const sync = cx50Sync(p, filter);
    const interval: [number, number] = [0, 25];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore: cx50SyncStore(),
    });

    expect(dataRequests.length).toBeGreaterThan(0);
    expect(Math.max(...dataRequests.map((q) => q.toBlock))).toBeLessThanOrEqual(
      1_025,
    );
  } finally {
    srv.close();
  }
});

test('#50 T2: first factory discovery scan is warmup-bounded', async () => {
  process.env.PORTAL_WARMUP_BLOCKS = '1000';
  process.env.PORTAL_READAHEAD = '0';
  const discoveryRequests: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      if (cx50HasTopic(q, PROXY_CREATED_TOPIC0.toLowerCase())) {
        discoveryRequests.push(q);
      }
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = { ...mkFactoryFilter(factory), toBlock: 100_000 };
    const sync = cx50Sync(p, filter, new Map([[factory.id, new Map()]]));
    const interval: [number, number] = [0, 25];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: cx50SyncStore(),
    });

    expect(discoveryRequests.length).toBeGreaterThan(0);
    expect(
      Math.max(...discoveryRequests.map((q) => q.toBlock)),
    ).toBeLessThanOrEqual(1_000);
  } finally {
    srv.close();
  }
});

test('#50 T8: dense factory trace discovery is bounded by the discovery warmup quantum', async () => {
  process.env.PORTAL_READAHEAD = '0';
  process.env.PORTAL_DISCOVERY_WINDOWS = '1';
  const discoveryRequests: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      if (cx50HasTopic(q, PROXY_CREATED_TOPIC0.toLowerCase())) {
        discoveryRequests.push(q);
      }
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = cx50FactoryTraceFilter(factory);
    const sync = cx50Sync(p, filter, new Map([[factory.id, new Map()]]));
    const interval: [number, number] = [0, 25];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: cx50SyncStore(),
    });

    expect(discoveryRequests.length).toBeGreaterThan(0);
    expect(discoveryRequests[0]!.toBlock).toBeLessThanOrEqual(25_000);
  } finally {
    srv.close();
  }
});

test('#50 parity: PORTAL_WARMUP_BLOCKS=0 keeps a factory discovery scan full-range', async () => {
  process.env.PORTAL_WARMUP_BLOCKS = '0';
  process.env.PORTAL_READAHEAD = '0';
  process.env.PORTAL_DISCOVERY_WINDOWS = '1';
  const discoveryRequests: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      if (cx50HasTopic(q, PROXY_CREATED_TOPIC0.toLowerCase())) {
        discoveryRequests.push(q);
      }
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = { ...mkFactoryFilter(factory), toBlock: 10_000_000 };
    const sync = cx50Sync(p, filter, new Map([[factory.id, new Map()]]));
    const interval: [number, number] = [0, 25];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: cx50SyncStore(),
    });

    expect(discoveryRequests).toHaveLength(1);
    expect(discoveryRequests[0]!.fromBlock).toBe(0);
    expect(discoveryRequests[0]!.toBlock).toBe(10_000_000);
  } finally {
    srv.close();
  }
});

test('#50 T3: resumed factory discovery is seeded at the durable frontier', async () => {
  process.env.PORTAL_WARMUP_BLOCKS = '1000';
  process.env.PORTAL_READAHEAD = '0';
  const CHILD_PRELOADED = '0x' + '5c'.repeat(20);
  const EVENT_AT = 50_010;
  const discoveryRequests: any[] = [];
  const inserted: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 1e12;
      if (cx50HasTopic(q, PROXY_CREATED_TOPIC0.toLowerCase())) {
        discoveryRequests.push(q);
      }
      const out =
        from <= EVENT_AT &&
        to >= EVENT_AT &&
        cx50HasTopic(q, DEPOSIT_TOPIC0.toLowerCase()) &&
        cx50HasAddress(q, CHILD_PRELOADED)
          ? [cx50ChildEventBlock(EVENT_AT, CHILD_PRELOADED)]
          : [];
      anchorRes(res, to, out);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = { ...mkFactoryFilter(factory), toBlock: 100_000 };
    const childAddresses = new Map([
      [factory.id, new Map([[CHILD_PRELOADED, 100]])],
    ]);
    const sync = cx50Sync(p, filter, childAddresses as any);
    const interval: [number, number] = [50_001, 50_025];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: cx50SyncStore(inserted),
    });

    expect(discoveryRequests.length).toBeGreaterThan(0);
    expect(discoveryRequests.some((q) => (q.fromBlock ?? 0) <= 50_000)).toBe(
      false,
    );
    expect(
      inserted.some(
        (l) => (l.address as string).toLowerCase() === CHILD_PRELOADED,
      ),
    ).toBe(true);
  } finally {
    srv.close();
  }
});

test('#50 T4: resumed data fetch trims the low edge to the interval', async () => {
  process.env.PORTAL_WARMUP_BLOCKS = '1000';
  process.env.PORTAL_READAHEAD = '0';
  const CHILD_PRELOADED = '0x' + '6c'.repeat(20);
  const dataRequests: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      const isData = cx50HasTopic(q, DEPOSIT_TOPIC0.toLowerCase());
      if (isData) dataRequests.push(q);
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = { ...mkFactoryFilter(factory), toBlock: 100_000 };
    const childAddresses = new Map([
      [factory.id, new Map([[CHILD_PRELOADED, 100]])],
    ]);
    const sync = cx50Sync(p, filter, childAddresses as any);
    const interval: [number, number] = [50_001, 50_025];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: cx50SyncStore(),
    });

    expect(dataRequests.length).toBeGreaterThan(0);
    expect(
      Math.min(...dataRequests.map((q) => q.fromBlock)),
    ).toBeGreaterThanOrEqual(50_001);
  } finally {
    srv.close();
  }
});

test('#50 T5: an interval inside the warmed window is served as a need-based cache hit', async () => {
  process.env.PORTAL_WARMUP_BLOCKS = '1000';
  process.env.PORTAL_READAHEAD = '0';
  const dataRequests: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      dataRequests.push(q);
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const filter = cx50PlainFilter();
    const sync = cx50Sync(p, filter);
    const first: [number, number] = [0, 25];
    await sync.syncBlockRangeData({
      interval: first,
      requiredIntervals: [{ interval: first, filter }],
      requiredFactoryIntervals: [],
      syncStore: cx50SyncStore(),
    });
    expect(Math.max(...dataRequests.map((q) => q.toBlock))).toBeLessThanOrEqual(
      1_025,
    );
    const afterFirst = dataRequests.length;

    const second: [number, number] = [26, 50];
    await sync.syncBlockRangeData({
      interval: second,
      requiredIntervals: [{ interval: second, filter }],
      requiredFactoryIntervals: [],
      syncStore: cx50SyncStore(),
    });

    expect(dataRequests).toHaveLength(afterFirst);
  } finally {
    srv.close();
  }
});

test('#50 T6: INV-19 trips before serving below a trimmed cache low edge', async () => {
  process.env.PORTAL_WARMUP_BLOCKS = '1000';
  process.env.PORTAL_READAHEAD = '0';
  process.env.PORTAL_CHECKS = 'strict';
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const filter = cx50PlainFilter();
    const sync = cx50Sync(p, filter);
    const first: [number, number] = [1000, 1024];
    await sync.syncBlockRangeData({
      interval: first,
      requiredIntervals: [{ interval: first, filter }],
      requiredFactoryIntervals: [],
      syncStore: cx50SyncStore(),
    });

    const second: [number, number] = [0, 24];
    await expect(
      sync.syncBlockRangeData({
        interval: second,
        requiredIntervals: [{ interval: second, filter }],
        requiredFactoryIntervals: [],
        syncStore: cx50SyncStore(),
      }),
    ).rejects.toThrow(/INV-19/);
  } finally {
    srv.close();
  }
});

test('#50 convergence: fetch quantum doubles until the chunk is covered', async () => {
  process.env.PORTAL_CHUNK_BLOCKS = '8000';
  process.env.PORTAL_WARMUP_BLOCKS = '1000';
  process.env.PORTAL_READAHEAD = '0';
  const dataRequests: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      dataRequests.push(q);
      anchorRes(res, q.toBlock ?? 1e12);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const filter = cx50PlainFilter(7_999);
    const sync = cx50Sync(p, filter);
    const intervals: [number, number][] = [
      [0, 25],
      [1000, 1025],
      [3000, 3025],
      [7000, 7025],
    ];
    for (const interval of intervals) {
      await sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [],
        syncStore: cx50SyncStore(),
      });
    }

    expect(dataRequests.map((q) => q.toBlock - q.fromBlock + 1)).toEqual([
      1000, 2000, 4000, 1000,
    ]);
  } finally {
    srv.close();
  }
});

test('regression: discovered factory children are persisted via insertChildAddresses in the SAME syncBlockRangeData call', async () => {
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const calls: any[] = [];
    const syncStore = mkFactorySyncStore((x) => calls.push(x));
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    const interval: [number, number] = [0, FACTORY_RANGE_END];

    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore,
    });

    // THE REGRESSION: the child must be handed to the store in the same call — core commits this
    // transaction together with insertIntervals (which marks the factory interval cached), so
    // persisting later (or never) breaks every restart.
    expect(calls).toHaveLength(1);
    expect(calls[0].factory).toBe(factory);
    expect(calls[0].chainId).toBe(1);
    expect(calls[0].childAddresses.size).toBe(1);
    expect(calls[0].childAddresses.get(CHILD_ADDR)).toBe(CHILD_CREATED_AT); // creation block, for isAddressMatched
    // sanity: discovery→data worked — the child's own log was fetched via the discovered address
    expect(
      logs.some((l: any) => (l.address as string).toLowerCase() === CHILD_ADDR),
    ).toBe(true);

    // an already-persisted child is never re-inserted (no duplicate rows interval after interval)
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    expect(calls).toHaveLength(1);
  } finally {
    srv.close();
  }
});

test('regression: a failing insertChildAddresses fails LOUD and the children re-flush on the next call — never silently dropped', async () => {
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const calls: any[] = [];
    let fail = true;
    const syncStore = mkFactorySyncStore((x) => {
      if (fail) throw new Error('child-address insert failed');
      calls.push(x);
    });
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    const interval: [number, number] = [0, FACTORY_RANGE_END];

    // the interval must FAIL (core rolls back the transaction incl. insertIntervals) — a swallowed
    // error here would mark the factory interval cached with the children lost forever.
    await expect(
      sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [{ interval, factory }],
        syncStore,
      }),
    ).rejects.toThrow('child-address insert failed');

    // the retried interval re-flushes the SAME children (pending was restored, not cleared)
    fail = false;
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore,
    });
    expect(calls).toHaveLength(1);
    expect(calls[0].childAddresses.get(CHILD_ADDR)).toBe(CHILD_CREATED_AT);
  } finally {
    srv.close();
  }
});

test('restart: a new sync seeded ONLY with the persisted children still fetches child logs (cached factory intervals → no re-discovery)', async () => {
  // Models the restart: factory intervals are cached (requiredFactoryIntervals = []), so discovery
  // never runs, and childAddresses contains exactly what the store returns. BEFORE the fix the store
  // was empty → the data request carried no child address → the Portal (server-side filter) returned
  // nothing → the child's events were silently skipped while the interval was marked synced.
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);

    // run 1: fresh backfill discovers + persists
    const persisted: any[] = [];
    const sync1 = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    const interval: [number, number] = [0, FACTORY_RANGE_END];
    await sync1.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: mkFactorySyncStore((x) => persisted.push(x)),
    });
    expect(persisted).toHaveLength(1);

    // run 2 ("restart"): children seeded from what run 1 persisted; factory intervals cached
    const restored = new Map([
      [factory.id, new Map(persisted[0].childAddresses)],
    ]);
    const sync2 = mkFactorySync(p, factory, filter, restored as any);
    const resume: [number, number] = [150, FACTORY_RANGE_END]; // resume past the creation block
    const logs = await sync2.syncBlockRangeData({
      interval: resume,
      requiredIntervals: [{ interval: resume, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkFactorySyncStore(() => {}),
    });

    // with an empty store (the bug) this is [] — the silent post-restart event loss
    expect(
      logs.some((l: any) => (l.address as string).toLowerCase() === CHILD_ADDR),
    ).toBe(true);
  } finally {
    srv.close();
  }
});

// Two children created in DIFFERENT intervals — CHILD_LO@50 and CHILD_HI@150 — so the wide scan
// discovers both up-front but each belongs to a different interval's transaction.
const CHILD_LO = '0x' + '1c'.repeat(20);
const CHILD_HI = '0x' + '2c'.repeat(20);
const LO_AT = 50;
const HI_AT = 150;

/** Discovery-only server: returns ProxyCreated logs for whichever of CHILD_LO@50 / CHILD_HI@150 fall
 * in the requested range. Data requests (Deposit topic0) get 204 — these tests assert only which
 * children each interval hands to insertChildAddresses, not event extraction. */
const twoChildFactoryServer = () =>
  http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 1_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 1e12;
      const isDiscovery = ((q.logs ?? []) as any[]).some((s) =>
        (s.topic0 ?? [])
          .map((x: string) => x.toLowerCase())
          .includes(PROXY_CREATED_TOPIC0.toLowerCase()),
      );
      const out: any[] = [];
      if (isDiscovery) {
        for (const [child, at] of [
          [CHILD_LO, LO_AT],
          [CHILD_HI, HI_AT],
        ] as [string, number][]) {
          if (from <= at && to >= at) {
            out.push({
              header: mkFactoryHeader(at),
              logs: [
                {
                  address: FACTORY_ADDR,
                  topics: [
                    PROXY_CREATED_TOPIC0,
                    '0x' + '00'.repeat(12) + child.slice(2),
                  ],
                  data: '0x',
                  transactionHash: '0x' + 'e0'.repeat(32),
                  transactionIndex: 0,
                  logIndex: 0,
                },
              ],
            });
          }
        }
      }
      // in-range window (head 1e9 ≫ chunk 0): terminate at the range-end anchor, carrying any discovered
      // ProxyCreated blocks; data requests (no match) still end cleanly instead of a mid-range 204 (#47).
      anchorRes(res, to, out);
    });
  });

test("regression: each syncBlockRangeData persists ONLY its interval's children, not the whole cross-interval discovery queue", async () => {
  // The wide factory scan discovers children across the ENTIRE backfill up-front. If one call flushed
  // the whole shared queue it would commit children that belong to OTHER, concurrently-pipelined
  // intervals inside THIS interval's transaction. Under ponder's per-interval transactions a rollback
  // of that interval — while a lower interval already committed as cached — permanently loses the
  // lower interval's child (restart re-discovery is floored at the lowest UNCACHED interval and never
  // rescans it). Each interval must flush exactly the children created in its own range.
  const srv = twoChildFactoryServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const calls: any[] = [];
    const syncStore = mkFactorySyncStore((x) => calls.push(x));
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );

    // interval [0,100]: the wide scan finds BOTH children, but only CHILD_LO@50 belongs here
    const lo: [number, number] = [0, 100];
    await sync.syncBlockRangeData({
      interval: lo,
      requiredIntervals: [{ interval: lo, filter }],
      requiredFactoryIntervals: [{ interval: lo, factory }],
      syncStore,
    });
    expect(calls).toHaveLength(1);
    expect([...calls[0].childAddresses.keys()]).toEqual([CHILD_LO]);
    expect(calls[0].childAddresses.get(CHILD_LO)).toBe(LO_AT);
    expect(calls[0].childAddresses.has(CHILD_HI)).toBe(false); // NOT this interval's child

    // interval [101,200]: CHILD_HI@150 was left queued by the first call and is flushed HERE, in its
    // own transaction — so its persistence rides the same commit that marks [101,200] cached.
    const hi: [number, number] = [101, 200];
    await sync.syncBlockRangeData({
      interval: hi,
      requiredIntervals: [{ interval: hi, filter }],
      requiredFactoryIntervals: [{ interval: hi, factory }],
      syncStore,
    });
    expect(calls).toHaveLength(2);
    expect([...calls[1].childAddresses.keys()]).toEqual([CHILD_HI]);
    expect(calls[1].childAddresses.get(CHILD_HI)).toBe(HI_AT);
  } finally {
    srv.close();
  }
});

test('regression: a post-flush insertLogs failure fails the interval LOUD (core rolls back — never a cached interval with unpersisted children)', async () => {
  // The child flush happens mid-syncBlockRangeData; insertLogs (and, in core, syncBlockData +
  // insertIntervals) run AFTER it in the SAME transaction. A transient failure there MUST reject so
  // core rolls the whole interval back — otherwise the interval is marked cached with its children
  // absent from the store, i.e. the silent post-restart loss. Pins that the window after the flush is
  // not swallowed (the previous loud-failure test only made insertChildAddresses itself throw).
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const calls: any[] = [];
    const syncStore = {
      ...mkFactorySyncStore((x) => calls.push(x)),
      insertLogs: () => {
        throw new Error('insertLogs failed');
      },
    };
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    const interval: [number, number] = [0, FACTORY_RANGE_END];

    await expect(
      sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [{ interval, factory }],
        syncStore,
      }),
    ).rejects.toThrow('insertLogs failed');

    // the child WAS handed to the store before the failure — so it's part of the same transaction
    // core rolls back; the interval is NOT marked cached, so restart re-discovery re-persists it.
    expect(calls).toHaveLength(1);
    expect(calls[0].childAddresses.get(CHILD_ADDR)).toBe(CHILD_CREATED_AT);
  } finally {
    srv.close();
  }
});

test('regression (INV-15 tx-retry, PIPELINED): a post-flush rollback where a SIBLING interval enters before the same interval retries still re-flushes the children — the keyed slot is not evicted by the sibling', async () => {
  // Ponder's core runs the whole interval callback inside a RETRYING transaction: a transient failure
  // AFTER the child flush (insertLogs, syncBlockData, insertIntervals, COMMIT) rolls the inserted
  // children back and re-runs the callback. Core also PIPELINES intervals — the next interval is
  // dispatched INSIDE the failing interval's still-open transaction — so a sibling interval B can enter
  // syncBlockRangeData between A's flush and A's retry. A single remembered slot let B's entry evict A's,
  // so A's retry found nothing to restore, flushed EMPTY, and committed the factory interval cached
  // WITHOUT its children — a permanent silent loss on the next restart. The keyed map survives this:
  // B consumes only its own (absent) entry, A's entry stays put for A's retry. The earlier same-interval
  // (A→A) shape could not see the eviction; this drives the real A → B → A ordering.
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const calls: any[] = [];
    const persisted = new Map<string, Map<string, number>>();
    let failLogs = true;
    const syncStore = {
      ...mkFactorySyncStore((x) => calls.push(x), persisted),
      insertLogs: () => {
        if (failLogs) throw new Error('transient: connection reset');
      },
    };
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    const interval: [number, number] = [0, FACTORY_RANGE_END];
    const params = {
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore,
    };

    // attempt 1 (interval A): the flush succeeds (child@100 ∈ A → inserted, pending-slot recorded under
    // A's key), then insertLogs fails → the interval rejects loud
    await expect(sync.syncBlockRangeData(params)).rejects.toThrow(
      'transient: connection reset',
    );
    expect(calls).toHaveLength(1);

    // core rolls A's transaction back — the inserted children are GONE from the store
    persisted.clear();

    // a SIBLING interval B (a pipelined lane sharing chunk 0) enters BEFORE A retries. B holds none of
    // A's children (child@100 ∉ [0,50]) and records no slot of its own — its only job here is to prove
    // it does NOT evict A's still-uncommitted slot (the single-slot bug cleared it on any new interval).
    failLogs = false;
    const sibling: [number, number] = [0, 50];
    await sync.syncBlockRangeData({
      interval: sibling,
      requiredIntervals: [{ interval: sibling, filter }],
      requiredFactoryIntervals: [{ interval: sibling, factory }],
      syncStore,
    });
    expect(calls).toHaveLength(1); // the sibling inserted nothing

    // attempt 2 (core's retry re-runs interval A): the flush must re-insert the child — A's keyed slot
    // survived the sibling, so re-entry restores the rolled-back queue and re-flushes it
    const logs = await sync.syncBlockRangeData(params);
    expect(calls).toHaveLength(2);
    expect(calls[1].childAddresses.get(CHILD_ADDR)).toBe(CHILD_CREATED_AT);
    expect(persisted.size).toBeGreaterThan(0); // durably back in the store this time
    // and the retried interval still delivers its data
    expect(
      logs.some((l: any) => (l.address as string).toLowerCase() === CHILD_ADDR),
    ).toBe(true);
  } finally {
    srv.close();
  }
});

test("regression (INV-15 tx-retry, KEYED): a re-entering interval restores its OWN pending flush, not a sibling's — a non-matching interval never consumes/restores another interval's entry", async () => {
  // Companion to the pipelined tx-retry test, pinning the keyed map from the other side. Two children
  // live in two disjoint intervals: CHILD_LO@50 ∈ A=[0,100], CHILD_HI@150 ∈ B=[101,200]. Both intervals
  // flush then fail post-flush (rolled back), so BOTH keyed slots are live at once. When A retries it must
  // restore CHILD_LO (its OWN slot), never CHILD_HI (B's) and never nothing. A single slot would hold only
  // the last-recorded (B's) flush — A's key mismatch would restore nothing; an UNCONDITIONAL restore would
  // pull B's CHILD_HI (out of A's range → dropped). The INV-17 store dedupe absorbs an over-restore on the
  // app path, so this asserts the exact child A re-flushes rather than a mere count.
  const srv = twoChildFactoryServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const calls: any[] = [];
    const persisted = new Map<string, Map<string, number>>();
    let failLogs = true;
    const syncStore = {
      ...mkFactorySyncStore((x) => calls.push(x), persisted),
      insertLogs: () => {
        if (failLogs) throw new Error('transient: connection reset');
      },
    };
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    const mkParams = (interval: [number, number]) => ({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore,
    });
    const a: [number, number] = [0, 100];
    const b: [number, number] = [101, 200];

    // interval A flushes CHILD_LO@50, then fails post-flush → rolled back (slot "0-100" = {CHILD_LO})
    await expect(sync.syncBlockRangeData(mkParams(a))).rejects.toThrow(
      'transient: connection reset',
    );
    expect(calls.at(-1).childAddresses.get(CHILD_LO)).toBe(LO_AT);
    persisted.clear();

    // interval B flushes CHILD_HI@150, then fails post-flush → rolled back (slot "101-200" = {CHILD_HI}).
    // B did NOT consume A's slot — both are now live.
    await expect(sync.syncBlockRangeData(mkParams(b))).rejects.toThrow(
      'transient: connection reset',
    );
    expect(calls.at(-1).childAddresses.get(CHILD_HI)).toBe(HI_AT);
    persisted.clear();

    // A retries: it must restore CHILD_LO (its OWN slot), NOT B's CHILD_HI and not an empty queue
    failLogs = false;
    await sync.syncBlockRangeData(mkParams(a));
    expect(calls).toHaveLength(3);
    expect(calls[2].childAddresses.get(CHILD_LO)).toBe(LO_AT);
    expect(calls[2].childAddresses.has(CHILD_HI)).toBe(false);

    // A's re-entry consumed ONLY its own slot — B's slot is untouched, so B's own retry still re-flushes
    // CHILD_HI. A mutant that let a re-entering interval consume/delete a SIBLING's entry would strand it.
    await sync.syncBlockRangeData(mkParams(b));
    expect(calls).toHaveLength(4);
    expect(calls[3].childAddresses.get(CHILD_HI)).toBe(HI_AT);
  } finally {
    srv.close();
  }
});

test('regression (INV-15 × INV-9): children pre-discovered by the wide Portal scan are persisted when their interval is DELEGATED to RPC — the fallback dedupe-skips them, so the portal-side flush must run', async () => {
  // The wide discovery scan (endHint = dataEnd) records children far past the interval being served,
  // into the childAddresses record the RPC fallback SHARES. Upstream's syncAddressFactory persists only
  // children NOT already in that record — so when a straddling interval is delegated whole to RPC, the
  // pre-discovered child is persisted by NEITHER path while core still marks the factory interval
  // cached in the same transaction. Before the fix the delegation branch returned without flushing:
  // the child's events were permanently lost on the next restart.
  process.env.PORTAL_FINALIZED_HEAD = '150'; // pin the head BELOW the factory range end (300)
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const rpcCalls: string[] = [];
    const rpc: any = {
      request: async (req: any) => {
        rpcCalls.push(req.method);
        if (req.method === 'eth_getLogs') return [];
        throw new Error(`unexpected rpc ${req.method}`);
      },
    };
    const stub = () => ({
      debug() {},
      info() {},
      warn() {},
      error() {},
      trace() {},
      child: () => stub(), // the stock RPC sync forks a child logger
    });
    const sync = createPortalHistoricalSync({
      common: {
        logger: stub(),
        // read by the stock RPC sync's factory path (address-list vs unfiltered eth_getLogs)
        options: { factoryAddressCountThreshold: 10_000 },
      } as any,
      chain: {
        id: 1,
        name: 'mainnet',
        portal: `http://localhost:${p}`,
        finalityBlockCount: 10,
      } as any,
      rpc,
      childAddresses: new Map([[factory.id, new Map()]]),
      eventCallbacks: [{ filter }],
    } as any);
    const calls: any[] = [];
    const syncStore = mkFactorySyncStore((x) => calls.push(x));

    // interval A ≤ head: served by the Portal; its wide discovery scan runs through the head (150) and
    // queues the child created at 100 — OUTSIDE A, so A's interval-scoped flush leaves it pending
    const a: [number, number] = [0, 50];
    await sync.syncBlockRangeData({
      interval: a,
      requiredIntervals: [{ interval: a, filter }],
      requiredFactoryIntervals: [{ interval: a, factory }],
      syncStore,
    });
    expect(calls).toHaveLength(0); // nothing created in [0,50]

    // interval K straddles the head (150 < 300) → delegated WHOLE to RPC. The pending child@100 ∈ K
    // must be flushed by the portal side of the delegation — the fallback will dedupe-skip it.
    const k: [number, number] = [51, FACTORY_RANGE_END];
    await sync.syncBlockRangeData({
      interval: k,
      requiredIntervals: [{ interval: k, filter }],
      requiredFactoryIntervals: [{ interval: k, factory }],
      syncStore,
    });
    expect(rpcCalls).toContain('eth_getLogs'); // K really went to the stock RPC sync

    const withChild = calls.filter((c: any) =>
      c.childAddresses.has(CHILD_ADDR),
    );
    expect(withChild).toHaveLength(1); // persisted exactly once — by the portal-side flush
    expect(withChild[0].childAddresses.get(CHILD_ADDR)).toBe(CHILD_CREATED_AT);
    expect(withChild[0].chainId).toBe(1);
  } finally {
    srv.close();
  }
});

test('regression: a child already known from the store is NOT re-flushed when discovery re-runs live', async () => {
  // On a resumed run, childAddresses is seeded from the store yet discovery can still re-run over a
  // partially-cached factory interval. A child re-found at its known creation block must NOT be
  // re-queued (prevBn === bn) — else every resume re-inserts known children. The existing
  // "already-persisted" assertion skips discovery entirely (dataChunk cache hit); this exercises the
  // live de-dup guard.
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const calls: any[] = [];
    const syncStore = mkFactorySyncStore((x) => calls.push(x));
    // seed the child as if loaded from the store at its real creation block
    const seeded = new Map([
      [factory.id, new Map([[CHILD_ADDR, CHILD_CREATED_AT]])],
    ]);
    const sync = mkFactorySync(p, factory, filter, seeded as any);
    const interval: [number, number] = [0, FACTORY_RANGE_END];

    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }], // discovery RUNS and re-finds CHILD_ADDR@100
      syncStore,
    });

    // re-discovered at the same block it was seeded → not re-queued → nothing handed to the store
    expect(calls).toHaveLength(0);
  } finally {
    srv.close();
  }
});

test('regression (FIX 2, BUG-A): an EARLY spanning-chunk fetch with requiredFactoryIntervals=[] still runs discovery — the child log is fetched AND its address flushed (no silent factory gap)', async () => {
  // BUG-A: the discovery floor used to be pinned ONLY when ponder handed over requiredFactoryIntervals
  // (`discStartIdx` set inside `if (requiredFactoryIntervals.length > 0)`), and both INV-3 asserts had a
  // `discStartIdx === undefined` ESCAPE. So the very first syncBlockRangeData whose chunk spans the
  // factory range but which ponder issued WITHOUT requiredFactoryIntervals (a real pipeline ordering:
  // another filter's interval fetches the shared chunk first) ran with floor unset → discovery.ensure()
  // no-op'd (planDiscovery returns null when floor < 0) → NO scan → the child was never discovered → its
  // data request carried no child address → the Portal returned nothing AND pendingChildren stayed empty
  // → insertChildAddresses never fired. FIX 2 pins the floor from spec.factories at CONSTRUCTION (and
  // re-pins per call), so discovery runs on this early fetch regardless of requiredFactoryIntervals.
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const calls: any[] = [];
    const inserted = { logs: [] as any[] };
    const syncStore = {
      ...mkFactorySyncStore((x) => calls.push(x)),
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
    };
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    // interval spans the child creation (100) AND its event (200); ponder issues it with NO
    // requiredFactoryIntervals — the early-fetch race that the construction-time floor now covers.
    const interval: [number, number] = [0, FACTORY_RANGE_END];
    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [], // ← the crux: no factory intervals on this early spanning fetch
      syncStore,
    });

    // BEFORE FIX: floor unset → no discovery → child@100 unknown → its Deposit@200 never fetched.
    // AFTER FIX:  floor pinned from the spec at construction → discovery finds child@100 → log fetched.
    const childLog = logs.find(
      (l: any) => (l.address as string).toLowerCase() === CHILD_ADDR,
    );
    expect(childLog).toBeDefined();
    expect(
      inserted.logs.some(
        (l: any) => (l.address as string).toLowerCase() === CHILD_ADDR,
      ),
    ).toBe(true);
    // AND the discovered child is persisted (its creation block ∈ this interval) — before the fix the
    // pending queue was empty because discovery never ran, so insertChildAddresses was never called.
    expect(calls).toHaveLength(1);
    expect(calls[0].childAddresses.get(CHILD_ADDR)).toBe(CHILD_CREATED_AT);
  } finally {
    srv.close();
  }
});

test('regression (#53, write-side idempotence): a SECOND writer re-flushing an already-persisted child set does NOT re-insert it — the store keeps exactly one row at the min block', async () => {
  // #53: factory_addresses has no UNIQUE and insertChildAddresses is a plain INSERT, so a
  // resumed/concurrent writer that re-flushes an already-persisted child would durably DUPLICATE the
  // row (the campaign SIGKILL/resume run ended with all children ×2). The in-memory min-merge guard
  // (portal-discovery.ts prev===undefined||prev>bn) suppresses re-queueing WITHIN one process, but a
  // second live process has a FRESH discovery queue and would re-queue+re-flush from scratch. The
  // write-side dedupe reads the store (getChildAddresses) before inserting and drops any child already
  // persisted at an equal/lower block — the write-side analogue of read-side LEAST. Modelled here as
  // two independent syncs over one store: writer 1 discovers+persists; writer 2 (fresh queue) rediscovers
  // the SAME child and must insert NOTHING.
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const interval: [number, number] = [0, FACTORY_RANGE_END];

    // the shared store: getChildAddresses reads it, and each insert commits into it (min-merge, like
    // upstream), so a duplicate re-insert would show up as a second write on the same child.
    const store = new Map<string, Map<string, number>>();
    const writes: any[] = [];
    const commit = (x: any) => {
      writes.push(x);
      const key = storeFactoryKey(x.factory);
      let rows = store.get(key);
      if (rows === undefined) {
        rows = new Map();
        store.set(key, rows);
      }
      for (const [addr, block] of x.childAddresses as Map<string, number>) {
        const prev = rows.get(addr);
        if (prev === undefined || prev > block) rows.set(addr, block);
      }
    };

    // writer 1: fresh backfill discovers CHILD_ADDR@100 and persists it (one write).
    const w1 = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    await w1.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: mkFactorySyncStore(commit, store),
    });
    expect(writes).toHaveLength(1);
    expect([
      ...(store.get(storeFactoryKey(factory)) as Map<string, number>),
    ]).toEqual([[CHILD_ADDR, CHILD_CREATED_AT]]);

    // writer 2: an independent process with an EMPTY in-memory queue (childAddresses fresh) rediscovers
    // the SAME child over the SAME range. Its in-memory guard does not suppress it — only the store-side
    // dedupe can. THE REGRESSION: no second write, so the store still has exactly one row at the min block.
    const w2 = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    await w2.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: mkFactorySyncStore(commit, store),
    });
    expect(writes).toHaveLength(1); // the second flush was fully deduped — no insertChildAddresses call
    expect([
      ...(store.get(storeFactoryKey(factory)) as Map<string, number>),
    ]).toEqual([[CHILD_ADDR, CHILD_CREATED_AT]]);
  } finally {
    srv.close();
  }
});

test('regression (#53, write-side idempotence): a re-discovery at a STRICTLY LOWER creation block is re-inserted so read-side LEAST resolves to the min (equal/higher blocks stay deduped)', async () => {
  // The dedupe mirrors LEAST, not DO-NOTHING: a child re-found at a lower creation block than the one
  // already persisted MUST be inserted (its lower row wins the read-side min-merge). Dropping it (as a
  // blanket "already present → skip" would) could leave the store pinned at a too-high creation block,
  // which read-side getChildAddresses would then report — the exact loss DO-NOTHING risks (#53 §2).
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const interval: [number, number] = [0, FACTORY_RANGE_END];
    const writes: any[] = [];

    // store pre-seeded (by STORE IDENTITY) with the child at a LATER block (200) than its real creation
    // (100). Discovery rediscovers it at 100 < 200 → the write-side dedupe keeps it (strictly-lower) and
    // re-inserts.
    const store = new Map<string, Map<string, number>>([
      [storeFactoryKey(factory), new Map([[CHILD_ADDR, 200]])],
    ]);
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: mkFactorySyncStore((x) => writes.push(x), store),
    });

    expect(writes).toHaveLength(1);
    expect(writes[0].childAddresses.get(CHILD_ADDR)).toBe(CHILD_CREATED_AT); // the lower block, re-inserted
  } finally {
    srv.close();
  }
});

test('regression (#53, alias-hole): two sources whose factories differ ONLY in id/sourceId (store aliases) discovering the same child in one interval end with EXACTLY ONE store row — not two', async () => {
  // The store keys factory_addresses by STORE IDENTITY (factory minus id/sourceId; sync-store/index.ts
  // strips both and upserts the factories row by the remaining value under UNIQUE (factory)). So two
  // sources sharing one factory contract — identical fields, different id/sourceId — map to ONE row-set.
  // The INV-17 guard reads ALL pending factories, THEN inserts ALL. Keyed per factory.id (its state
  // before this fix), the two aliases would each getChildAddresses the SAME (empty) row-set, both read
  // absence, and BOTH insertChildAddresses the same child — a durable duplicate the guard exists to
  // prevent. Canonicalizing the flush by store identity collapses them to one read + one insert.
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    // A and B are the SAME factory to the store: every field equal except id/sourceId.
    const factoryA = mkFactory();
    const factoryB = {
      ...mkFactory(),
      id: 'factory_evault_2',
      sourceId: 'evault2',
    };
    expect(storeFactoryKey(factoryA)).toBe(storeFactoryKey(factoryB)); // same store row
    expect(factoryA.id).not.toBe(factoryB.id); // distinct sources
    const filterA = mkFactoryFilter(factoryA);
    const filterB = mkFactoryFilter(factoryB);
    const interval: [number, number] = [0, FACTORY_RANGE_END];

    // one store, min-merged like upstream, keyed by store identity.
    const store = new Map<string, Map<string, number>>();
    const writes: any[] = [];
    const commit = (x: any) => {
      writes.push(x);
      const key = storeFactoryKey(x.factory);
      let rows = store.get(key);
      if (rows === undefined) {
        rows = new Map();
        store.set(key, rows);
      }
      for (const [addr, block] of x.childAddresses as Map<string, number>) {
        const prev = rows.get(addr);
        if (prev === undefined || prev > block) rows.set(addr, block);
      }
    };

    // ONE sync process configured with BOTH aliased factory sources (a legal user config). Discovery
    // keys pendingChildren by the factory object, so both A and B produce a flush entry for the same
    // child in the same interval.
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      },
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` },
      childAddresses: new Map([
        [factoryA.id, new Map()],
        [factoryB.id, new Map()],
      ]),
      eventCallbacks: [{ filter: filterA }, { filter: filterB }],
    } as any);

    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [
        { interval, filter: filterA },
        { interval, filter: filterB },
      ],
      requiredFactoryIntervals: [
        { interval, factory: factoryA },
        { interval, factory: factoryB },
      ],
      syncStore: mkFactorySyncStore(commit, store),
    });

    // THE REGRESSION: the two aliases were canonicalized to one store row, so the child is inserted
    // once — EXACTLY ONE row. Before the fix, both aliases read the empty row-set and each inserted,
    // leaving the child ×2.
    const rows = store.get(storeFactoryKey(factoryA)) as Map<string, number>;
    expect([...rows]).toEqual([[CHILD_ADDR, CHILD_CREATED_AT]]);
    let total = 0;
    for (const x of writes)
      total += (x.childAddresses as Map<string, number>).size;
    expect(total).toBe(1); // one child inserted across all writes — not two
  } finally {
    srv.close();
  }
});

test('regression (#53, case-normalization): a CHECKSUMMED pre-existing store row is deduped against the lowercased discovered child — the guard normalizes case before comparing', async () => {
  // getChildAddresses returns stored address text VERBATIM and min-merges case-SENSITIVELY, while
  // portal discovery lowercases every child and upstream runtime matching lowercases its lookups. If
  // the guard compared verbatim, a checksummed pre-existing row would NOT match the lowercase discovered
  // child, so the child would be re-inserted at the SAME block — a durable duplicate under a different
  // case. Normalizing the persisted map to lowercase closes it.
  const srv = factoryPortalServer();
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const factory = mkFactory();
    const filter = mkFactoryFilter(factory);
    const interval: [number, number] = [0, FACTORY_RANGE_END];
    const writes: any[] = [];

    // pre-seed the store with the SAME child at the SAME creation block but CHECKSUMMED (mixed case).
    // Discovery re-finds it lowercase at CHILD_CREATED_AT; the guard must treat them as the same row.
    const checksummed = ('0x' + 'C1'.repeat(20)) as any;
    expect(checksummed.toLowerCase()).toBe(CHILD_ADDR); // same address, different case
    const store = new Map<string, Map<string, number>>([
      [storeFactoryKey(factory), new Map([[checksummed, CHILD_CREATED_AT]])],
    ]);
    const sync = mkFactorySync(
      p,
      factory,
      filter,
      new Map([[factory.id, new Map()]]),
    );
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [{ interval, factory }],
      syncStore: mkFactorySyncStore((x) => writes.push(x), store),
    });

    // THE REGRESSION: the checksummed persisted row is normalized and matched → equal block → no insert.
    expect(writes).toHaveLength(0);
  } finally {
    srv.close();
  }
});

test('regression (#21 §2): in a mixed config the discovery scan starts at the factory floor, not interval[0] (no sub-floor overscan)', async () => {
  // #21 §2: the discovery floor is the construction-time floor (min over spec.factories of fromBlock ?? 0)
  // and NOTHING lowers it per call. The removed per-call refinement took min(discFloorBlock, interval[0],
  // …requiredFactoryIntervals starts); its interval[0] term was dead for correctness but dragged the floor
  // to 0 in a MIXED config — a plain log filter from block 0 makes the first data interval start at 0, so
  // the old refinement pulled the scan origin to 0 and the first ensure() streamed ~15M blocks of factory-
  // query results the matcher then discarded (a one-time-per-process overscan). This pins that the FIRST
  // discovery request begins at the grid-snapped factory floor (15M here), never at interval[0]=0.
  //
  // Deterministic grid: PORTAL_CHUNK_FIXED disables density scaling and PORTAL_CHUNK_BLOCKS=1_000_000
  // fixes the width, so the floor snaps to idxOf(15_000_000, 1_000_000)*1_000_000 = 15_000_000 exactly.
  process.env.PORTAL_CHUNK_BLOCKS = '1000000';
  process.env.PORTAL_CHUNK_FIXED = '1';
  process.env.PORTAL_FINALIZED_HEAD = '20000000';
  const FACTORY_FROM = 15_000_000; // on the 1M grid → floor snaps to itself
  const factory = { ...mkFactory(), fromBlock: FACTORY_FROM };
  // a plain (non-factory) log filter from block 0 — its interval[0]=0 is what the old refinement latched.
  const plainFilter = {
    type: 'log',
    chainId: 1,
    sourceId: 'plain',
    address: '0x' + 'a1'.repeat(20),
    topic0: DEPOSIT_TOPIC0,
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: 0,
    toBlock: undefined,
    hasTransactionReceipt: false,
    include: [],
  };
  const factoryFilter = {
    ...mkFactoryFilter(factory),
    fromBlock: FACTORY_FROM,
  };

  // record the fromBlock of every DISCOVERY request (topic0 = ProxyCreated selector). Serves nothing.
  const discoveryFroms: number[] = [];
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 20_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const isDiscovery = ((q.logs ?? []) as any[]).some((s) =>
        (s.topic0 ?? [])
          .map((x: string) => x.toLowerCase())
          .includes(PROXY_CREATED_TOPIC0.toLowerCase()),
      );
      if (isDiscovery) discoveryFroms.push(q.fromBlock ?? 0);

      // in-range windows (head 20M) that match nothing terminate at the range-end anchor (min(to, head)),
      // never a mid-range 204 (issue #47) — this asserts on the request fromBlocks, unaffected by the anchor.
      anchorRes(res, q.toBlock ?? 1e12, [], 20_000_000);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      },
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` },
      childAddresses: new Map([[factory.id, new Map()]]),
      eventCallbacks: [{ filter: plainFilter }, { filter: factoryFilter }],
    } as any);
    // ponder issues the FIRST interval from block 0 (the plain filter's start) — the exact mixed-config
    // ordering the old interval[0] term latched. requiredFactoryIntervals is empty on this early fetch.
    const interval: [number, number] = [0, 1_000_000];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter: plainFilter }],
      requiredFactoryIntervals: [],
      syncStore: mkFactorySyncStore(() => {}),
    });

    expect(discoveryFroms.length).toBeGreaterThan(0); // discovery DID run (the floor is pinned)
    const firstWindowFrom = Math.min(...discoveryFroms);
    // THE REGRESSION: the scan starts at the grid-snapped factory floor, NOT at interval[0]=0.
    expect(firstWindowFrom).toBe(FACTORY_FROM);
    expect(firstWindowFrom).not.toBe(0);
    // and NO discovery request probes below the floor (the whole sub-floor [0, 15M) range is never scanned).
    expect(discoveryFroms.every((f) => f >= FACTORY_FROM)).toBe(true);
  } finally {
    srv.close();
    delete process.env.PORTAL_CHUNK_BLOCKS;
    delete process.env.PORTAL_CHUNK_FIXED;
    delete process.env.PORTAL_FINALIZED_HEAD;
  }
});

test('regression (#21 §1, INV-15 gate): a factory creation log BELOW factory.fromBlock is neither recorded in childAddresses nor queued/flushed (isLogFactoryMatched floor gate)', async () => {
  // INV-15's interval-scoped flush is lossless only because scanWindow delegates creation-log matching
  // to ponder's isLogFactoryMatched, which rejects any creation log below factory.fromBlock. A child
  // "created" below the floor must never enter childAddresses or the pending queue — otherwise the
  // min-merge guard would also suppress re-queueing at its in-range creation block, and the child would
  // sit in memory forever, never persisted while its factory interval is marked cached (a restart loss).
  // This pins the silent dependence: if the matcher seam is ever bypassed, the test fails loud.
  //
  // #21 §2 removed the per-call discovery-floor refinement, so the scan floor is now the CONSTRUCTION-time
  // floor (min over spec.factories of fromBlock ?? 0). To still deliver a sub-floor creation log to the
  // matcher WITHOUT relying on that removed refinement, this uses a MIXED factory config: a genesis
  // factory (gen, fromBlock 0) drags the construction floor to 0, so scanWindow legitimately issues a
  // window covering block 50; the GATED factory (fromBlock 100) is the one whose child is "created" at 50
  // (sub-floor) — and only isLogFactoryMatched (100 > 50) discards it. The gate, not the scan origin, is
  // what's under test.
  const gen = mkFactory(); // genesis factory (fromBlock 0) → construction floor = min(0,100) = 0
  const gated = {
    ...mkFactory(),
    id: 'factory_gated',
    sourceId: 'gated',
    address: '0x' + 'da'.repeat(20),
    fromBlock: 100, // floor gate at 100 — a child "created" at 50 is sub-floor for THIS factory
  };
  const genFilter = mkFactoryFilter(gen);
  const gatedFilter = { ...mkFactoryFilter(gated), fromBlock: 100 };
  const BELOW_FLOOR_CHILD = '0x' + 'bf'.repeat(20);

  // a Portal that emits the gated factory's ProxyCreated log at block 50 (below its floor) for discovery
  // requests that target the gated factory's address. The gen factory has no children (empty backfill).
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 1_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 1e12;
      const targetsGated = ((q.logs ?? []) as any[]).some(
        (s) =>
          (s.topic0 ?? [])
            .map((x: string) => x.toLowerCase())
            .includes(PROXY_CREATED_TOPIC0.toLowerCase()) &&
          (s.address ?? [])
            .map((x: string) => x.toLowerCase())
            .includes(gated.address.toLowerCase()),
      );
      const out: any[] = [];
      // emit the sub-floor creation log at 50 whenever the window covers it (the Portal has no floor
      // knowledge — the matcher, not the source, must reject it).
      if (targetsGated && from <= 50 && to >= 50) {
        out.push({
          header: mkFactoryHeader(50),
          logs: [
            {
              address: gated.address,
              topics: [
                PROXY_CREATED_TOPIC0,
                '0x' + '00'.repeat(12) + BELOW_FLOOR_CHILD.slice(2),
              ],
              data: '0x',
              transactionHash: '0x' + 'bb'.repeat(32),
              transactionIndex: 0,
              logIndex: 0,
            },
          ],
        });
      }
      // in-range window (head 1e9): terminate at the range-end anchor, carrying the sub-floor creation log
      // when covered; a mid-range 204 would now fail closed (issue #47).
      anchorRes(res, to, out);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const calls: any[] = [];
    const childAddresses = new Map([
      [gen.id, new Map<string, number>()],
      [gated.id, new Map<string, number>()],
    ]);
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      },
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` },
      childAddresses,
      eventCallbacks: [{ filter: genFilter }, { filter: gatedFilter }],
    } as any);
    // The genesis factory pins the construction floor at 0, so scanWindow reaches block 50 and DELIVERS
    // the gated factory's sub-floor creation log to the matcher. isLogFactoryMatched (gated.fromBlock=100
    // > 50) is the ONLY thing that discards it — pinning the gate under the post-#21-§2 floor semantics.
    const interval: [number, number] = [0, FACTORY_RANGE_END];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [
        { interval, filter: genFilter },
        { interval, filter: gatedFilter },
      ],
      requiredFactoryIntervals: [
        { interval, factory: gen },
        { interval, factory: gated },
      ],
      syncStore: mkFactorySyncStore((x) => calls.push(x)),
    });

    // the sub-floor child is NEITHER recorded in the in-memory map NOR flushed to the store.
    expect(childAddresses.get(gated.id)?.has(BELOW_FLOOR_CHILD)).toBe(false);
    expect(childAddresses.get(gated.id)?.size).toBe(0);
    expect(
      calls.some((c: any) => c.childAddresses.has(BELOW_FLOOR_CHILD)),
    ).toBe(false);
  } finally {
    srv.close();
  }
});

test('guard: an over-limit request body fails loud with the explicit size driver (never a silent Portal 400)', async () => {
  // 12k filter addresses → batched bodies sum > 256KB MAX_RAW_QUERY_SIZE. The proactive guard must
  // throw a clear, actionable error BEFORE the POST, not let the Portal reject it opaquely.
  const addrs = Array.from(
    { length: 12000 },
    (_, i) => '0x' + i.toString(16).padStart(40, '0'),
  );
  const srv = http.createServer((req, res) => {
    let b = '';
    req.on('data', (c) => {
      b += c;
    });
    req.on('end', () => res.writeHead(204).end());
  });
  const p = await new Promise<number>((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const filter = {
      type: 'log',
      chainId: 1,
      sourceId: 'big',
      address: addrs,
      topic0: '0x' + '11'.repeat(32),
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: 100,
      hasTransactionReceipt: false,
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      },
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` },
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    await expect(
      sync.syncBlockRangeData({
        interval: [0, 100],
        requiredIntervals: [{ interval: [0, 100], filter }],
        requiredFactoryIntervals: [],
        syncStore: {
          insertLogs() {},
          insertBlocks() {},
          insertTransactions() {},
          insertTransactionReceipts() {},
          insertTraces() {},
        },
      } as any),
    ).rejects.toThrow(/exceeds MAX_RAW_QUERY_SIZE/);
  } finally {
    srv.close();
  }
});

test('regression: frontier chunk truncated at a lagging Portal head is EXTENDED when the head advances (no silent gap)', async () => {
  // The FRONTIER chunk (grid end past Portal's finalized head) is fetched TRUNCATED at the head, then
  // cached by idx ALONE. If the head later advances and a LATER interval in the SAME chunk reaches into
  // the newly-finalized tail, a blind cache hit would serve the stale truncated chunk — and the interval
  // is marked synced with ZERO data over (oldHead, need]: a permanent silent gap. The fix records how far
  // each chunk was fetched (coveredTo) and EXTENDS it (streams only the new tail) before serving past it.
  // Reproduces the tail-of-backfill case where the Portal head catches up mid-run.
  delete process.env.PORTAL_FINALIZED_HEAD; // let refreshPortalHead PROBE the mock so the head can advance
  // PORTAL_CHUNK_FIXED stays "1" (beforeEach) → chunkBlocks = 500k, so blocks 50 & 150 share chunk 0,
  // whose grid end (499_999) is far past both heads → chunk 0 is the frontier chunk.

  const A_BLOCK = 50; // ≤ head H1 → interval A caches chunk 0 truncated at [0, H1]
  const H1 = 100;
  const B_BLOCK = 150; // ∈ (H1, H2] → interval B needs the newly-finalized tail of chunk 0
  const H2 = 200;
  let head = H1;

  const hashOf = (n: number) => `0x${n.toString(16).padStart(64, '0')}`;
  const txHashOf = (n: number) =>
    `0x${(n + 1_000_000).toString(16).padStart(64, '0')}`;
  const mkBlock = (n: number) => ({
    ...FIXTURE_BLOCK,
    header: { ...FIXTURE_BLOCK.header, number: n, hash: hashOf(n) },
    logs: [{ ...FIXTURE_BLOCK.logs[0], transactionHash: txHashOf(n) }],
    transactions: [{ ...FIXTURE_BLOCK.transactions[0], hash: txHashOf(n) }],
  });

  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: head }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 1e12;
      const out = [A_BLOCK, B_BLOCK]
        .filter((n) => from <= n && to >= n)
        .map(mkBlock);
      // a real Portal 204s ONLY when `from` is above its finalized head; an in-range window terminates at
      // the range-end anchor min(to, head) (issue #47). The frontier chunk is client-clamped to `head`, so
      // in-range requests carry any covered block PLUS the anchor rather than 204-ing the served-through tail.
      if (from > head) {
        res.writeHead(204).end();
        return;
      }

      anchorRes(res, to, out, head);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );

  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filter: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's',
      address: VAULT,
      topic0: DEPOSIT_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: undefined, // UNBOUNDED backfill → chunkRange clamps the chunk end to the Portal head
      hasTransactionReceipt: false,
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);

    // interval A ends at block 50 ≤ head H1 → chunk 0 fetched + cached truncated at [0, 100].
    const iA: [number, number] = [A_BLOCK, A_BLOCK];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });

    head = H2; // Portal advances mid-run (H1 → H2), finalizing block 150

    // interval B ends at block 150 ∈ (H1, H2] → same chunk 0, past its cached tail. Must EXTEND, not
    // serve the stale [0, 100] chunk (which never streamed block 150).
    const iB: [number, number] = [B_BLOCK, B_BLOCK];
    await sync.syncBlockRangeData({
      interval: iB,
      requiredIntervals: [{ interval: iB, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });

    // BEFORE FIX: dataChunk(0) is a blind idx cache hit → block-150 log never streamed → only 1 log.
    // AFTER FIX:  coveredTo(100) < desiredTo(200) → chunk 0 extended over (100, 200] → block 150 present.
    const b150 = inserted.logs.find((l) => l.blockHash === hashOf(B_BLOCK));
    expect(b150).toBeDefined();
    expect(inserted.logs).toHaveLength(2);
  } finally {
    srv.close();
  }
});

test('regression (FIX 1, BUG-B): a BOUNDED backfill whose toBlock is PAST the head is head-clamped — a later interval over the newly-finalized tail is fetched, not blind-cache-hit (silent gap)', async () => {
  // The distinct BUG-B shape vs the unbounded frontier-extend test above: EVERY source is BOUNDED
  // (toBlock defined) and its toBlock sits PAST the current Portal head. The old dataEnd() =
  // `spec.backfillEnd ?? portalHead` returned backfillEnd (the toBlock) and IGNORED the head, so
  // chunk 0's desiredTo/coveredTo extended to the toBlock (300) — past head H1 (100). The Portal serves
  // nothing above its head, so block 150 wasn't streamed on interval A, yet coveredTo was recorded as
  // 300 (phantom coverage). When the head later advanced to H2 (200), interval B at block 150 saw
  // desiredTo(300) ≤ coveredTo(300) → a BLIND cache hit → block 150's log never streamed, its interval
  // marked synced EMPTY: a permanent silent gap. FIX 1 clamps dataEnd() to min(backfillEnd, head), so
  // coveredTo tracks the head and the INV-13 extend re-arms as the head advances.
  delete process.env.PORTAL_FINALIZED_HEAD; // let refreshPortalHead PROBE the mock so the head can advance
  // PORTAL_CHUNK_FIXED stays "1" (beforeEach) → chunkBlocks = 500k, so blocks 50 & 150 share chunk 0.

  const A_BLOCK = 50; // ≤ head H1 → interval A caches chunk 0 truncated at [0, H1]
  const H1 = 100;
  const B_BLOCK = 150; // ∈ (H1, H2] → interval B needs the newly-finalized tail of chunk 0
  const H2 = 200;
  const TO_BLOCK = 300; // BOUNDED backfill end, PAST both heads — the crux of BUG-B
  let head = H1;

  const hashOf = (n: number) => `0x${n.toString(16).padStart(64, '0')}`;
  const txHashOf = (n: number) =>
    `0x${(n + 2_000_000).toString(16).padStart(64, '0')}`;
  const mkBlock = (n: number) => ({
    ...FIXTURE_BLOCK,
    header: { ...FIXTURE_BLOCK.header, number: n, hash: hashOf(n) },
    logs: [{ ...FIXTURE_BLOCK.logs[0], transactionHash: txHashOf(n) }],
    transactions: [{ ...FIXTURE_BLOCK.transactions[0], hash: txHashOf(n) }],
  });

  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: head }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      // a real Portal serves nothing ABOVE its finalized head — CLAMP the served range to `head`
      const to = Math.min(q.toBlock ?? 1e12, head);
      const out = [A_BLOCK, B_BLOCK]
        .filter((n) => from <= n && to >= n)
        .map(mkBlock);
      // `to` is head-clamped; a from-above-head request (from > to) stays a bare 204, an in-range window
      // terminates at the range-end anchor with any covered block (issue #47), never a served-through 204.
      if (from > to) {
        res.writeHead(204).end();
        return;
      }

      anchorRes(res, to, out);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );

  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filter: any = {
      type: 'log',
      chainId: 1,
      sourceId: 's',
      address: VAULT,
      topic0: DEPOSIT_TOPIC0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock: 0,
      toBlock: TO_BLOCK, // BOUNDED, past the head → old `backfillEnd ?? head` ignored the head
      hasTransactionReceipt: false,
      include: [],
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);

    // interval A ends at block 50 ≤ head H1 → chunk 0 fetched, clamped to the head at [0, 100].
    const iA: [number, number] = [A_BLOCK, A_BLOCK];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });

    head = H2; // Portal advances mid-run (H1 → H2), finalizing block 150

    // interval B ends at block 150 ∈ (H1, H2] → same chunk 0. Under the bug coveredTo was already 300
    // (backfillEnd) → blind cache hit → block 150 never streamed. Fixed: coveredTo tracked the head → EXTEND.
    const iB: [number, number] = [B_BLOCK, B_BLOCK];
    await sync.syncBlockRangeData({
      interval: iB,
      requiredIntervals: [{ interval: iB, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });

    // BEFORE FIX: coveredTo(300) ≥ desiredTo → blind hit → block-150 log never streamed → only 1 log.
    // AFTER FIX:  coveredTo tracked the head(100) → extend over (100, 200] → block 150 present.
    const b150 = inserted.logs.find((l) => l.blockHash === hashOf(B_BLOCK));
    expect(b150).toBeDefined();
    expect(inserted.logs).toHaveLength(2);
  } finally {
    srv.close();
  }
});

test('regression (FIX 1 discovery variant): a factory child CREATED in the newly-finalized tail is discovered + its log fetched once the head advances (bounded backfill, head-clamped discovery)', async () => {
  // FIX 1 flows the head clamp into discovery too (endHint via dataEnd()). BUG-B's silent gap also hides
  // factory children: with a BOUNDED backfill past the head, the old dataEnd() ran discovery to the
  // toBlock in ONE pass while the Portal served nothing above its head, so a child created in the tail
  // (> head at first pass) was never discovered — and once the head advanced, the blind cache hit meant
  // the tail was never re-scanned. Here CHILD is created at block 150 (in the tail finalized only at H2)
  // and emits its own Deposit at block 160. The child's event must be fetched after the head advances.
  delete process.env.PORTAL_FINALIZED_HEAD; // probe the mock so the head can advance
  const CREATE_LO = 40; // a first child created below H1 (drives interval A + initial discovery)
  const CHILD_LO_ADDR = '0x' + '3c'.repeat(20);
  const CHILD_TAIL_ADDR = '0x' + '4c'.repeat(20);
  const CREATE_TAIL = 150; // the tail child, created only once the head reaches H2
  const EVENT_TAIL = 160; // the tail child emits its Deposit here
  const H1 = 100;
  const H2 = 200;
  const TO_BLOCK = 300; // bounded backfill past both heads
  let head = H1;

  const FACTORY_ADDR2 = '0x' + 'fb'.repeat(20);
  const PROXY_TOPIC0 = '0x' + '01'.repeat(32);
  const factory2: any = {
    id: 'factory2',
    type: 'log',
    chainId: 1,
    sourceId: 'ev2',
    address: FACTORY_ADDR2,
    eventSelector: PROXY_TOPIC0,
    childAddressLocation: 'topic1',
    fromBlock: 0,
    toBlock: undefined,
  };
  const filter: any = {
    type: 'log',
    chainId: 1,
    sourceId: 'ev2:deposit',
    address: factory2,
    topic0: DEPOSIT_TOPIC0,
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: 0,
    toBlock: TO_BLOCK,
    hasTransactionReceipt: false,
    include: [],
  };

  const mkHeader = (num: number) => ({
    number: num,
    hash: '0x' + num.toString(16).padStart(64, '0'),
    parentHash: '0x' + '00'.repeat(32),
    timestamp: 1_700_000_000 + num,
    logsBloom: '0x' + '00'.repeat(256),
    miner: '0x' + '99'.repeat(20),
    gasUsed: '0x1',
    gasLimit: '0x1c9c380',
    stateRoot: '0x' + '22'.repeat(32),
    receiptsRoot: '0x' + '33'.repeat(32),
    transactionsRoot: '0x' + '44'.repeat(32),
    size: '0x500',
    difficulty: '0x0',
    extraData: '0x',
  });
  const proxyBlock = (at: number, child: string) => ({
    header: mkHeader(at),
    logs: [
      {
        address: FACTORY_ADDR2,
        topics: [PROXY_TOPIC0, '0x' + '00'.repeat(12) + child.slice(2)],
        data: '0x',
        transactionHash: '0x' + 'aa'.repeat(32),
        transactionIndex: 0,
        logIndex: 0,
      },
    ],
  });
  const childEventBlock = (at: number, child: string) => ({
    header: mkHeader(at),
    logs: [
      {
        address: child,
        topics: [DEPOSIT_TOPIC0],
        data: '0x',
        transactionHash: '0x' + 'bb'.repeat(32),
        transactionIndex: 0,
        logIndex: 0,
      },
    ],
    transactions: [
      {
        transactionIndex: 0,
        hash: '0x' + 'bb'.repeat(32),
        from: '0x' + 'ee'.repeat(20),
        to: child,
        input: '0x',
        value: '0x0',
        nonce: 0,
        gas: '0x1',
        gasPrice: '0x1',
        type: 0,
      },
    ],
  });

  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: head }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = Math.min(q.toBlock ?? 1e12, head); // Portal serves nothing above its head
      const wants = (topic0: string, addr: string) =>
        ((q.logs ?? []) as any[]).some(
          (s) =>
            (!s.topic0 ||
              s.topic0
                .map((x: string) => x.toLowerCase())
                .includes(topic0.toLowerCase())) &&
            (!s.address ||
              s.address
                .map((x: string) => x.toLowerCase())
                .includes(addr.toLowerCase())),
        );
      const out: any[] = [];
      // discovery requests (factory address + ProxyCreated selector)
      if (
        from <= CREATE_LO &&
        to >= CREATE_LO &&
        wants(PROXY_TOPIC0, FACTORY_ADDR2)
      )
        out.push(proxyBlock(CREATE_LO, CHILD_LO_ADDR));
      if (
        from <= CREATE_TAIL &&
        to >= CREATE_TAIL &&
        wants(PROXY_TOPIC0, FACTORY_ADDR2)
      )
        out.push(proxyBlock(CREATE_TAIL, CHILD_TAIL_ADDR));
      // data requests carrying a DISCOVERED child address
      if (
        from <= EVENT_TAIL &&
        to >= EVENT_TAIL &&
        wants(DEPOSIT_TOPIC0, CHILD_TAIL_ADDR)
      )
        out.push(childEventBlock(EVENT_TAIL, CHILD_TAIL_ADDR));
      // `to` is head-clamped; a from-above-head request (from > to) stays a bare 204, an in-range window
      // terminates at the range-end anchor with any covered discovery/data block (issue #47).
      if (from > to) {
        res.writeHead(204).end();
        return;
      }

      anchorRes(res, to, out);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );

  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
      insertChildAddresses: () => {},
      getChildAddresses: async () => new Map(),
    };
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` } as any,
      childAddresses: new Map([[factory2.id, new Map()]]) as any,
      eventCallbacks: [{ filter }],
    } as any);

    // interval A ends ≤ H1 → chunk 0 fetched clamped to the head at [0, 100]; discovery runs to the head.
    const iA: [number, number] = [CREATE_LO, CREATE_LO];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter }],
      requiredFactoryIntervals: [{ interval: iA, factory: factory2 }],
      syncStore,
    });

    head = H2; // Portal advances, finalizing the tail (CREATE_TAIL@150, EVENT_TAIL@160)

    // interval B reaches into the newly-finalized tail → chunk 0 must EXTEND: re-scan discovery over
    // (100, 200] (finds CHILD_TAIL@150) THEN stream its Deposit@160. Under the bug the blind cache hit
    // skipped both → the tail child's event was silently lost.
    const iB: [number, number] = [EVENT_TAIL, EVENT_TAIL];
    await sync.syncBlockRangeData({
      interval: iB,
      requiredIntervals: [{ interval: iB, filter }],
      requiredFactoryIntervals: [{ interval: iB, factory: factory2 }],
      syncStore,
    });

    // AFTER FIX: the tail child discovered post-advance → its Deposit@160 fetched via the discovered addr.
    const tailLog = inserted.logs.find(
      (l) => (l.address as string).toLowerCase() === CHILD_TAIL_ADDR,
    );
    expect(tailLog).toBeDefined();
  } finally {
    srv.close();
  }
});

test('regression: an undefined fromBlock is genesis — the [0, min) prefix of an unbounded source is NOT silently skipped (C10, ports #8 by @mo4islona)', async () => {
  // f1 has NO fromBlock (⇒ genesis); f2 starts at 15M. The bug took Math.min over only the DEFINED
  // fromBlocks → backfillStart=15M → chunkRange clamped chunk 0's fetch to start at 15M, so f1's block-100
  // log was never streamed and its [0,15M) history was silently marked synced. After the fix the floor is
  // 0 (any undefined ⇒ genesis, symmetric with backfillEnd). PORTAL_CHUNK_FIXED=1 + head=2e9 come from the
  // beforeEach: chunkBlocks stays 500k → block 100 ∈ chunk 0, and [100,100] is well under the head.
  const T1 = '0x' + '11'.repeat(32);
  const A1 = '0x' + 'aa'.repeat(20); // f1 — unbounded (genesis) source
  const A2 = '0x' + 'bb'.repeat(20); // f2 — fromBlock 15M
  const BN1 = 100;
  const TXA = '0x' + 'a1'.repeat(32);
  const mkBlock = () => ({
    ...FIXTURE_BLOCK,
    header: {
      ...FIXTURE_BLOCK.header,
      number: BN1,
      hash: '0x' + BN1.toString(16).padStart(64, '0'),
    },
    logs: [
      {
        address: A1,
        topics: [T1],
        data: '0x',
        transactionHash: TXA,
        transactionIndex: 0,
        logIndex: 0,
      },
    ],
    transactions: [
      {
        ...FIXTURE_BLOCK.transactions[0],
        transactionIndex: 0,
        hash: TXA,
        from: A1,
        to: A1,
      },
    ],
  });
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: 1_000_000_000 }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 1e12;
      const wantsA1 = (q.logs ?? []).some((s: any) =>
        (s.address ?? [])
          .map((x: string) => x.toLowerCase())
          .includes(A1.toLowerCase()),
      );
      if (from <= BN1 && to >= BN1 && wantsA1) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(JSON.stringify(mkBlock()) + '\n');
        return;
      }

      // in-range window (head 1e9): the continuation past the served block-100 (and any non-matching
      // window) terminates at the range-end anchor, not a mid-range 204 that would now fail closed (#47).
      anchorRes(res, to);
    });
  });
  const p: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = {
      insertLogs: (x: any) => inserted.logs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
      insertChildAddresses: () => {},
      getChildAddresses: async () => new Map(),
    };
    const mkFilter = (
      sourceId: string,
      address: string,
      topic0: string,
      fromBlock: number | undefined,
    ): any => ({
      type: 'log',
      chainId: 1,
      sourceId,
      address,
      topic0,
      topic1: null,
      topic2: null,
      topic3: null,
      fromBlock,
      toBlock: undefined,
      hasTransactionReceipt: false,
      include: [],
    });
    const f1 = mkFilter('s1', A1, T1, undefined); // no fromBlock ⇒ genesis
    const f2 = mkFilter('s2', A2, '0x' + '22'.repeat(32), 15_000_000);
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      },
      chain: { id: 1, name: 'mainnet', portal: `http://localhost:${p}` },
      childAddresses: new Map(),
      eventCallbacks: [{ filter: f1 }, { filter: f2 }],
    } as any);
    const iA: [number, number] = [BN1, BN1];
    const logs = await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter: f1 }],
      requiredFactoryIntervals: [],
      syncStore,
    } as any);
    // BEFORE FIX: floor=15M → chunk 0's fetch clamped to [15M, …] → block 100 never streamed → 0 logs.
    // AFTER FIX:  floor=0 → chunk 0's fetch [0, 499_999] reaches block 100 → f1's log present.
    expect(inserted.logs).toHaveLength(1);
    expect(inserted.logs[0].topics[0].toLowerCase()).toBe(T1.toLowerCase());
    expect(logs).toHaveLength(1);
  } finally {
    srv.close();
  }
});
