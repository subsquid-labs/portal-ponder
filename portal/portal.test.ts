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

afterEach(() => { server.close(); delete process.env.PORTAL_CHUNK_FIXED; });

test("regression: matched log's transaction is fetched, transformed, and inserted (event.transaction defined)", async () => {
  const inserted = { logs: [] as any[], blocks: [] as any[], txs: [] as any[] };
  const syncStore: any = {
    insertLogs: (p: any) => inserted.logs.push(...p.logs),
    insertBlocks: (p: any) => inserted.blocks.push(...p.blocks),
    insertTransactions: (p: any) => inserted.txs.push(...p.transactions),
    insertTransactionReceipts: () => {},
    insertTraces: () => {},
  };

  const sync = createPortalHistoricalSync({
    common: { logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} } } as any,
    chain: { id: 1, name: "mainnet", portal: `http://localhost:${port}` } as any,
    childAddresses: new Map(),
  });

  const filter: any = {
    type: "log", chainId: 1, sourceId: "evault:deposit", address: VAULT,
    topic0: DEPOSIT_TOPIC0, topic1: null, topic2: null, topic3: null,
    fromBlock: 20558652, toBlock: 20558652, hasTransactionReceipt: false, include: [],
  };
  const interval: [number, number] = [20558652, 20558652];

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
