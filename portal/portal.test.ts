import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, beforeEach, expect, test } from "vitest";
import { createPortalHistoricalSync } from "./portal.js";

/**
 * Fixture: one Portal `/finalized-stream` NDJSON block carrying an Euler EVault
 * `Deposit` log AND its parent transaction (the `transaction` relation).
 *
 * Regression: portal.ts originally fetched logs+blocks only, so `event.transaction`
 * was undefined and Ponder's event profiler crashed reading `event.transaction.hash`
 * in multi-chain mode (see indexing-store/profile.ts). This fixture + test pin that
 * the matched log's transaction is fetched, transformed, and inserted.
 */
const TX_HASH = "0x62684e3dab102ad2e626d9121dba1d9915f238b2dd0316cdf8d4860751305071";
const BLOCK_HASH = "0xdce7daa5236cc31d94a3313648f2c0b2dbbb8a5fa26e10fb2edd26a4c45e7240";
const VAULT = "0x44b3c96db2caf61167a9eab82901139a404cdb6f";
const DEPOSIT_TOPIC0 = "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7";

const FIXTURE_BLOCK = {
  header: {
    number: 20558652, hash: BLOCK_HASH,
    parentHash: "0x1111111111111111111111111111111111111111111111111111111111111111",
    timestamp: 1724000000, logsBloom: "0x" + "00".repeat(256),
    miner: "0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5",
    gasUsed: "0xabc", gasLimit: "0x1c9c380", stateRoot: "0x" + "22".repeat(32),
    receiptsRoot: "0x" + "33".repeat(32), transactionsRoot: "0x" + "44".repeat(32),
    size: "0x500", difficulty: "0x0", extraData: "0x",
  },
  logs: [{
    address: VAULT,
    topics: [DEPOSIT_TOPIC0,
      "0x0000000000000000000000004b5ccdb3b7e44475d1f0a06499f12acbd4fc0032",
      "0x0000000000000000000000004b5ccdb3b7e44475d1f0a06499f12acbd4fc0032"],
    data: "0x00000000000000000000000000000000000000000000000000000000000f4240" +
          "00000000000000000000000000000000000000000000000000000000000f4240",
    transactionHash: TX_HASH, transactionIndex: 1, logIndex: 4,
  }],
  transactions: [{
    transactionIndex: 1, hash: TX_HASH,
    from: "0x4b5ccdb3b7e44475d1f0a06499f12acbd4fc0032", to: VAULT,
    input: "0x6e553f65", value: "0x0", nonce: 7, gas: "0x317fa", gasPrice: "0xc0db32e7d",
    maxFeePerGas: "0xc0db32e7d", maxPriorityFeePerGas: "0x0", type: 2,
    r: "0x" + "ab".repeat(32), s: "0x" + "cd".repeat(32), v: "0x1", yParity: "0x1",
  }],
};

let server: http.Server;
let port: number;

beforeEach(async () => {
  process.env.PORTAL_CHUNK_FIXED = "1"; // skip head-based chunk scaling (no /finalized-head call)
  process.env.PORTAL_FINALIZED_HEAD = "2000000000"; // valid finality head — real usage always has one (C3: unknown head → RPC fallback)
  server = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => {
      const q = body ? JSON.parse(body) : {};
      // serve the fixture only for the request whose range covers the block
      if (req.url?.includes("finalized-stream") && q.fromBlock <= 20558652 && (q.toBlock ?? 1e12) >= 20558652) {
        res.writeHead(200, { "content-type": "application/x-ndjson" });
        res.end(JSON.stringify(FIXTURE_BLOCK) + "\n");
      } else {
        res.writeHead(204).end(); // above head / nothing more
      }
    });
  });
  port = await new Promise<number>((resolve) => server.listen(0, () => resolve((server.address() as AddressInfo).port)));
});

afterEach(() => { server.close(); delete process.env.PORTAL_CHUNK_FIXED; delete process.env.PORTAL_FINALIZED_HEAD; });

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
    type: "log", chainId: 1, sourceId: "evault:deposit", address: VAULT,
    topic0: DEPOSIT_TOPIC0, topic1: null, topic2: null, topic3: null,
    fromBlock: 20558652, toBlock: 20558652, hasTransactionReceipt: false, include: [],
  };
  const interval: [number, number] = [20558652, 20558652];

  const sync = createPortalHistoricalSync({
    common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } } as any,
    chain: { id: 1, name: "mainnet", portal: `http://localhost:${port}` } as any,
    childAddresses: new Map(),
    eventCallbacks: [{ filter }], // FULL per-chain filter set (C1: fetch-spec resolved from this)
  } as any);

  const logs = await sync.syncBlockRangeData({ interval, requiredIntervals: [{ interval, filter }], requiredFactoryIntervals: [], syncStore });
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
  expect(inserted.txs[0].from).toBe("0x4b5ccdb3b7e44475d1f0a06499f12acbd4fc0032");
  expect(inserted.txs[0].transactionIndex).toBe("0x1");
});

test("regression (C1): a 2nd log filter sharing a chunk on a LATER call is still fetched — no silent gap", async () => {
  // Two independent log filters whose blocks both fall in chunk 0 ([0, 499_999]) but which Ponder
  // marks "required" on DIFFERENT syncBlockRangeData calls (each call carries only the filter still
  // missing its sub-interval). The bug keyed chunk 0 by idx alone, freezing it to the FIRST call's
  // single filter → the 2nd filter's log was never streamed yet its interval was marked complete.
  const T1 = "0x" + "11".repeat(32), T2 = "0x" + "22".repeat(32);
  const A1 = "0x" + "aa".repeat(20), A2 = "0x" + "bb".repeat(20);
  const TXA = "0x" + "a1".repeat(32), TXB = "0x" + "b2".repeat(32);
  const BN1 = 10, BN2 = 11; // both in chunk 0

  const mkBlock = (num: number, addr: string, topic0: string, txHash: string) => ({
    header: {
      number: num, hash: "0x" + num.toString(16).padStart(64, "0"), parentHash: "0x" + "00".repeat(32),
      timestamp: 1_700_000_000 + num, logsBloom: "0x" + "00".repeat(256), miner: "0x" + "99".repeat(20),
      gasUsed: "0x1", gasLimit: "0x1c9c380", stateRoot: "0x" + "22".repeat(32), receiptsRoot: "0x" + "33".repeat(32),
      transactionsRoot: "0x" + "44".repeat(32), size: "0x500", difficulty: "0x0", extraData: "0x",
    },
    logs: [{ address: addr, topics: [topic0], data: "0x", transactionHash: txHash, transactionIndex: 0, logIndex: 0 }],
    transactions: [{ transactionIndex: 0, hash: txHash, from: A1, to: addr, input: "0x", value: "0x0", nonce: 0, gas: "0x1", gasPrice: "0x1", type: 0 }],
  });
  // emit a block ONLY when the request's log spec matches it, so a chunk fetched for filter 1 alone
  // never contains filter 2's block (this is what makes the idx-only freeze observable)
  const matches = (specs: any[], topic0: string, addr: string) =>
    (specs ?? []).some((s) =>
      (!s.topic0 || s.topic0.map((x: string) => x.toLowerCase()).includes(topic0.toLowerCase())) &&
      (!s.address || s.address.map((x: string) => x.toLowerCase()).includes(addr.toLowerCase())));

  const srv = http.createServer((req, res) => {
    let body = ""; req.on("data", (c) => { body += c; });
    req.on("end", () => {
      if (req.url?.includes("finalized-head")) { res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ number: 1_000_000_000 })); return; }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0, to = q.toBlock ?? 1e12;
      const out: any[] = [];
      if (from <= BN1 && to >= BN1 && matches(q.logs, T1, A1)) out.push(mkBlock(BN1, A1, T1, TXA));
      if (from <= BN2 && to >= BN2 && matches(q.logs, T2, A2)) out.push(mkBlock(BN2, A2, T2, TXB));
      if (out.length === 0) { res.writeHead(204).end(); return; }
      res.writeHead(200, { "content-type": "application/x-ndjson" }); res.end(out.map((b) => JSON.stringify(b)).join("\n") + "\n");
    });
  });
  const p: number = await new Promise((r) => srv.listen(0, () => r((srv.address() as AddressInfo).port)));
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = { insertLogs: (x: any) => inserted.logs.push(...x.logs), insertBlocks: () => {}, insertTransactions: () => {}, insertTransactionReceipts: () => {}, insertTraces: () => {} };
    const f1: any = { type: "log", chainId: 1, sourceId: "s1", address: A1, topic0: T1, topic1: null, topic2: null, topic3: null, fromBlock: 0, toBlock: 499_999, hasTransactionReceipt: false, include: [] };
    const f2: any = { type: "log", chainId: 1, sourceId: "s2", address: A2, topic0: T2, topic1: null, topic2: null, topic3: null, fromBlock: 0, toBlock: 499_999, hasTransactionReceipt: false, include: [] };
    const sync = createPortalHistoricalSync({
      common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } } as any,
      chain: { id: 1, name: "mainnet", portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(), eventCallbacks: [{ filter: f1 }, { filter: f2 }],
    } as any);
    const iA: [number, number] = [BN1, BN1];
    await sync.syncBlockRangeData({ interval: iA, requiredIntervals: [{ interval: iA, filter: f1 }], requiredFactoryIntervals: [], syncStore });
    const iB: [number, number] = [BN2, BN2];
    await sync.syncBlockRangeData({ interval: iB, requiredIntervals: [{ interval: iB, filter: f2 }], requiredFactoryIntervals: [], syncStore });
    // BEFORE FIX: chunk 0 cached for filter 1 only → filter 2's block-11 log never streamed → 1 log.
    // AFTER FIX:  initSpec makes chunk 0 filter-complete → both logs present → 2 logs.
    const f2Log = inserted.logs.find((l) => l.topics?.[0]?.toLowerCase() === T2.toLowerCase());
    expect(f2Log).toBeDefined();
    expect(inserted.logs).toHaveLength(2);
  } finally { srv.close(); }
});

test("regression: a dataset-unsupported field (accessList) is dropped, not crashed on", async () => {
  // Per-dataset schema varies — e.g. Monad/plasma transactions have no accessList; the whole request
  // 400s ("column 'access_list_size' is not found in 'transactions'"). accessList is non-load-bearing
  // and NULLABLE, so the fork must drop it and retry — EVEN THOUGH Ponder's default `include` always
  // lists transaction.accessList (see `include` below). A static default include must never force a
  // crash on a droppable field; only NOT-NULL/bloom/core fields do.
  const srv = http.createServer((req, res) => {
    let body = ""; req.on("data", (c) => { body += c; });
    req.on("end", () => {
      if (req.url?.includes("finalized-head")) { res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ number: 2_000_000_000 })); return; }
      const q = body ? JSON.parse(body) : {};
      if (req.url?.includes("finalized-stream") && q.fields?.transaction?.accessList !== undefined) {
        res.writeHead(400); res.end("Bad request: couldn't parse request: column 'access_list_size' is not found in 'transactions'"); return;
      }
      if (req.url?.includes("finalized-stream") && q.fromBlock <= 20558652 && (q.toBlock ?? 1e12) >= 20558652) {
        res.writeHead(200, { "content-type": "application/x-ndjson" }); res.end(JSON.stringify(FIXTURE_BLOCK) + "\n"); return;
      }
      res.writeHead(204).end();
    });
  });
  const p: number = await new Promise((r) => srv.listen(0, () => r((srv.address() as AddressInfo).port)));
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = { insertLogs: (x: any) => inserted.logs.push(...x.logs), insertBlocks: () => {}, insertTransactions: () => {}, insertTransactionReceipts: () => {}, insertTraces: () => {} };
    // include LISTS accessList (exactly as Ponder's static default does) — the fork must still drop it.
    const filter: any = { type: "log", chainId: 1, sourceId: "s", address: VAULT, topic0: DEPOSIT_TOPIC0, topic1: null, topic2: null, topic3: null, fromBlock: 20558652, toBlock: 20558652, hasTransactionReceipt: false, include: ["transaction.accessList", "transaction.hash", "log.address"] };
    const sync = createPortalHistoricalSync({
      common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } } as any,
      chain: { id: 1, name: "mainnet", portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(), eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [20558652, 20558652];
    const logs = await sync.syncBlockRangeData({ interval, requiredIntervals: [{ interval, filter }], requiredFactoryIntervals: [], syncStore });
    await sync.syncBlockData({ interval, logs, syncStore } as any);
    expect(inserted.logs).toHaveLength(1); // degraded gracefully — dropped accessList despite it being in `include`
    expect(inserted.logs[0].transactionHash).toBe(TX_HASH);
  } finally { srv.close(); }
});

test("regression: an 'unknown field' 400 (schema doesn't know accessList) is also dropped, not crashed on", async () => {
  // A second rejection shape: some datasets 400 with a query-PARSE error — "unknown field
  // `accessList`, expected one of `transactionIndex`, `hash`, ..." — instead of "column not found".
  // The fork must map it back to transaction.accessList (a droppable field) and retry, not crash.
  const srv = http.createServer((req, res) => {
    let body = ""; req.on("data", (c) => { body += c; });
    req.on("end", () => {
      if (req.url?.includes("finalized-head")) { res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ number: 2_000_000_000 })); return; }
      const q = body ? JSON.parse(body) : {};
      if (req.url?.includes("finalized-stream") && q.fields?.transaction?.accessList !== undefined) {
        res.writeHead(400); res.end("Bad request: couldn't parse request: Couldn't parse query: unknown field `accessList`, expected one of `transactionIndex`, `hash`, `nonce`, `from`"); return;
      }
      if (req.url?.includes("finalized-stream") && q.fromBlock <= 20558652 && (q.toBlock ?? 1e12) >= 20558652) {
        res.writeHead(200, { "content-type": "application/x-ndjson" }); res.end(JSON.stringify(FIXTURE_BLOCK) + "\n"); return;
      }
      res.writeHead(204).end();
    });
  });
  const p: number = await new Promise((r) => srv.listen(0, () => r((srv.address() as AddressInfo).port)));
  try {
    const inserted = { logs: [] as any[] };
    const syncStore: any = { insertLogs: (x: any) => inserted.logs.push(...x.logs), insertBlocks: () => {}, insertTransactions: () => {}, insertTransactionReceipts: () => {}, insertTraces: () => {} };
    const filter: any = { type: "log", chainId: 1, sourceId: "s", address: VAULT, topic0: DEPOSIT_TOPIC0, topic1: null, topic2: null, topic3: null, fromBlock: 20558652, toBlock: 20558652, hasTransactionReceipt: false, include: ["transaction.accessList"] };
    const sync = createPortalHistoricalSync({
      common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } } as any,
      chain: { id: 1, name: "plasma", portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(), eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [20558652, 20558652];
    const logs = await sync.syncBlockRangeData({ interval, requiredIntervals: [{ interval, filter }], requiredFactoryIntervals: [], syncStore });
    await sync.syncBlockData({ interval, logs, syncStore } as any);
    expect(inserted.logs).toHaveLength(1); // dropped accessList via the 'unknown field' path, fetched the block
  } finally { srv.close(); }
});

// Server that 400s while `logsBloom` is requested, then serves `serveData ? the fixture : nothing`
// once it's dropped — models a dataset (e.g. Monad old chunks) that lacks the receipt logsBloom.
const missingLogsBloomServer = (serveData: boolean) => http.createServer((req, res) => {
  let body = ""; req.on("data", (c) => { body += c; });
  req.on("end", () => {
    if (req.url?.includes("finalized-head")) { res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ number: 2_000_000_000 })); return; }
    const q = body ? JSON.parse(body) : {};
    if (req.url?.includes("finalized-stream") && q.fields?.transaction?.logsBloom !== undefined) {
      res.writeHead(400); res.end("Bad request: couldn't parse request: column 'logs_bloom' is not found in 'transactions'"); return;
    }
    if (serveData && req.url?.includes("finalized-stream") && q.fromBlock <= 20558652 && (q.toBlock ?? 1e12) >= 20558652) {
      res.writeHead(200, { "content-type": "application/x-ndjson" }); res.end(JSON.stringify(FIXTURE_BLOCK) + "\n"); return;
    }
    res.writeHead(204).end();
  });
});
const runMissingLogsBloom = async (serveData: boolean) => {
  const srv = missingLogsBloomServer(serveData);
  const p: number = await new Promise((r) => srv.listen(0, () => r((srv.address() as AddressInfo).port)));
  const inserted = { logs: [] as any[] };
  const syncStore: any = { insertLogs: (x: any) => inserted.logs.push(...x.logs), insertBlocks: () => {}, insertTransactions: () => {}, insertTransactionReceipts: () => {}, insertTraces: () => {} };
  const filter: any = { type: "log", chainId: 1, sourceId: "s", address: VAULT, topic0: DEPOSIT_TOPIC0, topic1: null, topic2: null, topic3: null, fromBlock: 20558652, toBlock: 20558652, hasTransactionReceipt: true, include: [] };
  const sync = createPortalHistoricalSync({
    common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } } as any,
    chain: { id: 1, name: "mainnet", portal: `http://localhost:${p}` } as any,
    childAddresses: new Map(), eventCallbacks: [{ filter }],
  } as any);
  const interval: [number, number] = [20558652, 20558652];
  try { const logs = await sync.syncBlockRangeData({ interval, requiredIntervals: [{ interval, filter }], requiredFactoryIntervals: [], syncStore }); await sync.syncBlockData({ interval, logs, syncStore } as any); return inserted; }
  finally { srv.close(); }
};

test("regression: a NEEDED missing field fails LOUDLY when the range has MATCHED data (no silent substitution)", async () => {
  // logsBloom is NOT-NULL + bloom-load-bearing; a chunk that lacks it AND has matched events must
  // CRASH, not default (a wrong bloom silently drops logs in realtime).
  await expect(runMissingLogsBloom(true)).rejects.toThrow(/logs_bloom.*matched data|matched data.*logs_bloom/);
});

test("regression: a NEEDED missing field on an EVENT-LESS range is tolerated (irrelevant old chunks)", async () => {
  // Same missing logsBloom, but the range yields NO matched data (e.g. Monad's pre-schema chunks
  // before any relevant events) → harmless, must NOT crash. The indexer runs fine.
  const inserted = await runMissingLogsBloom(false);
  expect(inserted.logs).toHaveLength(0);
});

test("regression: a dataset that starts after genesis (TAC starts at block 1) is clamped forward, not crashed on", async () => {
  // The Portal 400s "dataset starts from block N" when queried below its first block. The fork must
  // clamp the cursor to N and continue — NOT throw an unhandledRejection (which killed the whole
  // multichain app when TAC's dataset started at block 1 and startBlock was 0).
  const START = 1000; let clampedTo = -1;
  const srv = http.createServer((req, res) => {
    let body = ""; req.on("data", (c) => { body += c; });
    req.on("end", () => {
      if (req.url?.includes("finalized-head")) { res.writeHead(200, { "content-type": "application/json" }); res.end(JSON.stringify({ number: 2_000_000_000 })); return; }
      const q = body ? JSON.parse(body) : {};
      if (req.url?.includes("finalized-stream")) {
        if ((q.fromBlock ?? 0) < START) { res.writeHead(400); res.end("Bad request: dataset starts from block " + START); return; }
        clampedTo = q.fromBlock; res.writeHead(204).end(); return; // clamped query → empty, no crash
      }
      res.writeHead(204).end();
    });
  });
  const p: number = await new Promise((r) => srv.listen(0, () => r((srv.address() as AddressInfo).port)));
  try {
    const syncStore: any = { insertLogs: () => {}, insertBlocks: () => {}, insertTransactions: () => {}, insertTransactionReceipts: () => {}, insertTraces: () => {} };
    const filter: any = { type: "log", chainId: 1, sourceId: "s", address: VAULT, topic0: DEPOSIT_TOPIC0, topic1: null, topic2: null, topic3: null, fromBlock: 0, toBlock: 5000, include: [] };
    const sync = createPortalHistoricalSync({
      common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } } as any,
      chain: { id: 1, name: "tac", portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(), eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [0, 5000];
    await sync.syncBlockRangeData({ interval, requiredIntervals: [{ interval, filter }], requiredFactoryIntervals: [], syncStore }); // must not throw
    expect(clampedTo).toBe(START); // the retry was clamped to the dataset start, not re-issued from 0
  } finally { srv.close(); }
});

test("regression (C6): deep matched trace is stored at its FULL-tree pre-order index (7), not filter-local (0)", async () => {
  // Ponder's RPC sync numbers trace_index as the pre-order DFS rank over each tx's FULL call tree,
  // THEN filters — so a lone deep match keeps its true position. Portal used to push the trace
  // filter server-side and rank over the matched SUBSET (→ 0). The fake server emulates Portal's
  // callTo filtering, so this fails before the fix (index 0) and passes after (index 7).
  process.env.PORTAL_FINALIZED_HEAD = String(22200011 + 1_000_000); // no finality fallback / no head call
  const BLOCK = 22200011, TX_INDEX = 111;
  const TX_HASH = "0x" + "7e".repeat(32), B_HASH = "0x" + "b1".repeat(32);
  const TARGET = "0x000000000000000000000000000000000000dead";
  const OTHER = "0x000000000000000000000000000000000000beef";
  const mkTrace = (traceAddress: number[], to: string, subtraces: number) => ({
    transactionIndex: TX_INDEX, traceAddress, type: "call", subtraces, error: null, revertReason: null,
    action: { from: OTHER, to, value: "0x0", gas: "0x1000", input: "0xabcdabcd", sighash: "0xabcdabcd", type: "call", callType: "call" },
    result: { gasUsed: "0x10", output: "0x" },
  });
  // pre-order: [] 0 · [0] 1 · [0,0] 2 · [0,1] 3 · [0,2] 4 · [1] 5 · [1,0] 6 · [1,0,0] 7(MATCH)
  const ALL = [mkTrace([], OTHER, 2), mkTrace([0], OTHER, 3), mkTrace([0, 0], OTHER, 0), mkTrace([0, 1], OTHER, 0), mkTrace([0, 2], OTHER, 0), mkTrace([1], OTHER, 1), mkTrace([1, 0], OTHER, 1), mkTrace([1, 0, 0], TARGET, 0)];
  const STREAM = [ALL[7], ALL[0], ALL[5], ALL[2], ALL[6], ALL[1], ALL[4], ALL[3]]; // shuffled → must sort
  const TX = { transactionIndex: TX_INDEX, hash: TX_HASH, from: OTHER, to: TARGET, input: "0xabcdabcd", value: "0x0", nonce: 1, gas: "0x100000", gasPrice: "0x1", type: 0, r: "0x" + "11".repeat(32), s: "0x" + "22".repeat(32), v: "0x1" };
  const HEADER = { number: BLOCK, hash: B_HASH, parentHash: "0x" + "00".repeat(32), timestamp: 1700000000, logsBloom: "0x" + "00".repeat(256), miner: OTHER, gasUsed: "0xabc", gasLimit: "0x1c9c380", stateRoot: "0x" + "22".repeat(32), receiptsRoot: "0x" + "33".repeat(32), transactionsRoot: "0x" + "44".repeat(32), size: "0x500", difficulty: "0x0", extraData: "0x" };
  const srv = http.createServer((req, res) => {
    let body = ""; req.on("data", (c) => { body += c; });
    req.on("end", () => {
      const q = body ? JSON.parse(body) : {};
      const covers = (q.fromBlock ?? 0) <= BLOCK && (q.toBlock ?? 1e12) >= BLOCK;
      if (req.url?.includes("finalized-stream") && q.traces && covers) {
        const callTo = new Set((q.traces as any[]).flatMap((r) => r.callTo ?? []).map((s: string) => s.toLowerCase()));
        let traces = STREAM;
        if (callTo.size) traces = traces.filter((t) => callTo.has(t.action.to.toLowerCase())); // emulate Portal server-side filter
        res.writeHead(200, { "content-type": "application/x-ndjson" }); res.end(JSON.stringify({ header: HEADER, traces, transactions: [TX] }) + "\n");
      } else { res.writeHead(204).end(); }
    });
  });
  const p: number = await new Promise((r) => srv.listen(0, () => r((srv.address() as AddressInfo).port)));
  try {
    const inserted: any[] = [];
    const syncStore: any = { insertLogs: () => {}, insertBlocks: () => {}, insertTransactions: () => {}, insertTransactionReceipts: () => {}, insertTraces: (x: any) => inserted.push(...x.traces) };
    const filter: any = { type: "trace", chainId: 1, sourceId: "deep:trace", fromAddress: undefined, toAddress: TARGET, functionSelector: undefined, callType: undefined, includeReverted: false, fromBlock: BLOCK, toBlock: BLOCK, hasTransactionReceipt: false, include: [] };
    const sync = createPortalHistoricalSync({
      common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } } as any,
      chain: { id: 1, name: "mainnet", portal: `http://localhost:${p}` } as any,
      childAddresses: new Map(), eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [BLOCK, BLOCK];
    await sync.syncBlockRangeData({ interval, requiredIntervals: [{ interval, filter }], requiredFactoryIntervals: [], syncStore });
    await sync.syncBlockData({ interval, logs: [], syncStore } as any);
    expect(inserted).toHaveLength(1); // count parity: only the matched trace is stored (like RPC)
    expect(inserted[0].trace.trace.index).toBe(7); // INDEX parity: full-tree position, not filter-local 0
    expect((inserted[0].trace.trace.to as string)?.toLowerCase()).toBe(TARGET);
  } finally { srv.close(); delete process.env.PORTAL_FINALIZED_HEAD; }
});

test("merge: N same-address event filters collapse to ONE log request (unioned topic0) — keeps body small", async () => {
  // Ponder emits one filter per event; a 24-event EVault would otherwise repeat the child-address
  // list 24× in one body and blow past MAX_RAW_QUERY_SIZE. mergeLogRequests must fold them into one.
  const ADDR = "0x" + "cc".repeat(20);
  const topic0s = Array.from({ length: 6 }, (_, i) => "0x" + (i + 1).toString(16).padStart(64, "0"));
  const dataQueries: any[] = [];
  const srv = http.createServer((req, res) => {
    let body = ""; req.on("data", (c) => { body += c; });
    req.on("end", () => { const q = body ? JSON.parse(body) : {}; if (q.logs) dataQueries.push(q); res.writeHead(204).end(); });
  });
  const p = await new Promise<number>((r) => srv.listen(0, () => r((srv.address() as AddressInfo).port)));
  try {
    const filters = topic0s.map((t, i) => ({
      type: "log", chainId: 1, sourceId: `evault:e${i}`, address: ADDR, topic0: t,
      topic1: null, topic2: null, topic3: null, fromBlock: 0, toBlock: 100, hasTransactionReceipt: false, include: [],
    }));
    const sync = createPortalHistoricalSync({
      common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } },
      chain: { id: 1, name: "mainnet", portal: `http://localhost:${p}` },
      childAddresses: new Map(), eventCallbacks: filters.map((f) => ({ filter: f })),
    } as any);
    await sync.syncBlockRangeData({
      interval: [0, 100], requiredIntervals: filters.map((f) => ({ interval: [0, 100], filter: f })),
      requiredFactoryIntervals: [], syncStore: { insertLogs() {}, insertBlocks() {}, insertTransactions() {}, insertTransactionReceipts() {}, insertTraces() {} },
    } as any);
    const merged = dataQueries.find((q) => (q.logs?.[0]?.topic0?.length ?? 0) > 1);
    expect(merged).toBeDefined();
    expect(merged.logs).toHaveLength(1); // ONE request, not 6
    expect(new Set(merged.logs[0].topic0.map((x: string) => x.toLowerCase())).size).toBe(6); // all selectors unioned
  } finally { srv.close(); }
});

test("guard: an over-limit request body fails loud with the explicit size driver (never a silent Portal 400)", async () => {
  // 12k filter addresses → batched bodies sum > 256KB MAX_RAW_QUERY_SIZE. The proactive guard must
  // throw a clear, actionable error BEFORE the POST, not let the Portal reject it opaquely.
  const addrs = Array.from({ length: 12000 }, (_, i) => "0x" + i.toString(16).padStart(40, "0"));
  const srv = http.createServer((req, res) => { let b = ""; req.on("data", (c) => { b += c; }); req.on("end", () => res.writeHead(204).end()); });
  const p = await new Promise<number>((r) => srv.listen(0, () => r((srv.address() as AddressInfo).port)));
  try {
    const filter = { type: "log", chainId: 1, sourceId: "big", address: addrs, topic0: "0x" + "11".repeat(32), topic1: null, topic2: null, topic3: null, fromBlock: 0, toBlock: 100, hasTransactionReceipt: false, include: [] };
    const sync = createPortalHistoricalSync({
      common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } },
      chain: { id: 1, name: "mainnet", portal: `http://localhost:${p}` },
      childAddresses: new Map(), eventCallbacks: [{ filter }],
    } as any);
    await expect(sync.syncBlockRangeData({
      interval: [0, 100], requiredIntervals: [{ interval: [0, 100], filter }], requiredFactoryIntervals: [],
      syncStore: { insertLogs() {}, insertBlocks() {}, insertTransactions() {}, insertTransactionReceipts() {}, insertTraces() {} },
    } as any)).rejects.toThrow(/exceeds MAX_RAW_QUERY_SIZE/);
  } finally { srv.close(); }
});
