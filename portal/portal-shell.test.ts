import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, beforeEach, expect, test } from 'vitest';
import { createPortalHistoricalSync } from './portal.js';
import { loadPortalConfig } from './portal-config.js';
import { __resetSharedGate, sharedGate } from './portal-gate.js';
import { InvariantViolation } from './portal-invariant.js';

/**
 * Seam-level regression tests for the orchestration shell (portal.ts): the G1/G3 fixes, the
 * frontier-chunk extend (INV-13 — adapted from PR #5), the finality-delegation matrix (INV-9), the
 * stash lifecycle (INV-12), and the per-fetch row-accounting model (S1).
 */

const VAULT = '0x44b3c96db2caf61167a9eab82901139a404cdb6f';
const DEPOSIT_TOPIC0 =
  '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7';

const mkHeader = (num: number) => ({
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
const txHashOf = (n: number) =>
  `0x${(n + 1_000_000).toString(16).padStart(64, '0')}`;
const mkBlock = (num: number) => ({
  header: mkHeader(num),
  logs: [
    {
      address: VAULT,
      topics: [DEPOSIT_TOPIC0],
      data: '0x',
      transactionHash: txHashOf(num),
      transactionIndex: 0,
      logIndex: 0,
    },
  ],
  transactions: [
    {
      transactionIndex: 0,
      hash: txHashOf(num),
      from: `0x${'ee'.repeat(20)}`,
      to: VAULT,
      input: '0x',
      value: '0x0',
      nonce: 0,
      gas: '0x1',
      gasPrice: '0x1',
      type: 0,
    },
  ],
});

const stubLogger = () => ({
  debug() {},
  info() {},
  warn() {},
  error() {},
  trace() {},
  child() {
    return stubLogger();
  },
});

const mkSyncStore = (inserted?: { logs: unknown[] }): any => ({
  insertLogs: (x: any) => inserted?.logs.push(...x.logs),
  insertBlocks: () => {},
  insertTransactions: () => {},
  insertTransactionReceipts: () => {},
  insertTraces: () => {},
  insertChildAddresses: () => {},
});

const mkFilter = (over: Record<string, unknown> = {}): any => ({
  type: 'log',
  chainId: 1,
  sourceId: 's',
  address: VAULT,
  topic0: DEPOSIT_TOPIC0,
  topic1: null,
  topic2: null,
  topic3: null,
  fromBlock: 0,
  toBlock: 100,
  hasTransactionReceipt: false,
  include: [],
  ...over,
});

const mkSync = (
  port: number,
  filter: any,
  over: Record<string, unknown> = {},
) =>
  createPortalHistoricalSync({
    common: { logger: stubLogger() } as any,
    chain: {
      id: 1,
      name: 'mainnet',
      portal: `http://localhost:${port}`,
      finalityBlockCount: 10,
    } as any,
    childAddresses: new Map(),
    eventCallbacks: [{ filter }],
    ...over,
  } as any);

const listen = (srv: http.Server): Promise<number> =>
  new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );

beforeEach(() => {
  __resetSharedGate(); // gate state must not bleed across tests (this file asserts on it)
  process.env.PORTAL_CHUNK_FIXED = '1';
  process.env.PORTAL_FINALIZED_HEAD = '2000000000';
});
afterEach(() => {
  delete process.env.PORTAL_CHUNK_FIXED;
  delete process.env.PORTAL_FINALIZED_HEAD;
  delete process.env.PORTAL_REALTIME;
  delete process.env.PORTAL_CHECKS;
});

// ── G1 (INV-13): a rejected chunk promise is evicted — the next call REFETCHES ──────────────────────

test('G1: a failed chunk is evicted (rows freed) and a later call refetches instead of replaying the rejection', async () => {
  let dataPosts = 0;
  let failNext = true;
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      if (!req.url?.includes('finalized-stream')) {
        res.writeHead(204).end();
        return;
      }
      dataPosts++;
      // serve block 10 for the head of the chunk, then hard-fail ONCE mid-chunk (unrecognized 400 → no
      // retry) so the chunk promise rejects AFTER registering block 10's rows.
      if ((q.fromBlock ?? 0) <= 10) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(`${JSON.stringify(mkBlock(10))}\n`);
        return;
      }
      if (failNext) {
        failNext = false;
        res.writeHead(400);
        res.end('boom — not a recognized 400 variant');
        return;
      }
      res.writeHead(204).end();
    });
  });
  const port = await listen(srv);
  try {
    const filter = mkFilter();
    const sync = mkSync(port, filter);
    const gate = sharedGate(loadPortalConfig({}));
    const baseline = gate.snapshot().rows;
    const interval: [number, number] = [0, 100];

    // first call: the chunk rejects mid-stream (block 10 already registered rows)
    await expect(
      sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [],
        syncStore: mkSyncStore(),
      }),
    ).rejects.toThrow(/boom/);
    // G1: the rejected chunk's registered rows were freed exactly once
    expect(gate.snapshot().rows).toBe(baseline);

    // second call: the rejection was NOT cached — a fresh fetch happens and succeeds
    const postsBefore = dataPosts;
    const inserted = { logs: [] as unknown[] };
    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(inserted),
    });
    expect(dataPosts).toBeGreaterThan(postsBefore); // refetched, not served from a poisoned cache
    expect(logs).toHaveLength(1);
    expect(inserted.logs).toHaveLength(1);
  } finally {
    srv.close();
  }
});

// ── G3 (INV-7): rows are registered per ARRIVING batch, not on chunk completion ─────────────────────

test('G3: buffered rows are visible to the gate MID-CHUNK (registered per arriving batch)', async () => {
  let rowsSeenMidChunk = -1;
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      if (!req.url?.includes('finalized-stream')) {
        res.writeHead(204).end();
        return;
      }
      if ((q.fromBlock ?? 0) <= 10) {
        // batch 1 of the SAME chunk
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(`${JSON.stringify(mkBlock(10))}\n`);
        return;
      }
      // the chunk is still in flight (cursor advanced past batch 1) — observe the gate NOW
      rowsSeenMidChunk = sharedGate(loadPortalConfig({})).snapshot().rows;
      res.writeHead(204).end();
    });
  });
  const port = await listen(srv);
  try {
    const filter = mkFilter();
    const sync = mkSync(port, filter);
    const interval: [number, number] = [0, 100];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(),
    });
    // before G3, rows were registered only after the whole chunk assembled → 0 mid-chunk
    expect(rowsSeenMidChunk).toBeGreaterThan(0);
  } finally {
    srv.close();
  }
});

// ── INV-13 frontier extend: TAIL-ONLY angle (outcome parity itself is pinned by main's #5 test in
// portal.test.ts, which passes unchanged against this architecture) ─────────────────────────────────

test('frontier extend streams ONLY the newly-finalized tail (the extend request starts at coveredTo+1)', async () => {
  delete process.env.PORTAL_FINALIZED_HEAD; // let refreshPortalHead PROBE the mock so the head can advance
  const A_BLOCK = 50; //  ≤ head H1 → interval A caches chunk 0 truncated at [0, H1]
  const H1 = 100;
  const B_BLOCK = 150; // ∈ (H1, H2] → interval B needs the newly-finalized tail of chunk 0
  const H2 = 200;
  let head = H1;
  const tailFroms: number[] = [];

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
      if (from > H1) tailFroms.push(from); // requests into the extended tail
      const out = [A_BLOCK, B_BLOCK]
        .filter((n) => from <= n && to >= n)
        .map(mkBlock);
      if (out.length === 0) {
        res.writeHead(204).end();
        return;
      }
      res.writeHead(200, { 'content-type': 'application/x-ndjson' });
      res.end(`${out.map((b) => JSON.stringify(b)).join('\n')}\n`);
    });
  });
  const port = await listen(srv);
  try {
    const inserted = { logs: [] as any[] };
    const filter = mkFilter({ toBlock: undefined }); // UNBOUNDED backfill → chunk end clamps to the head
    const sync = mkSync(port, filter);

    // interval A ends ≤ H1 → chunk 0 fetched + cached truncated at [0, 100]
    const iA: [number, number] = [A_BLOCK, A_BLOCK];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(inserted),
    });

    head = H2; // the Portal advances mid-run, finalizing block 150

    // interval B ends ∈ (H1, H2] → same chunk 0, past its cached tail: must EXTEND, not serve stale
    const iB: [number, number] = [B_BLOCK, B_BLOCK];
    await sync.syncBlockRangeData({
      interval: iB,
      requiredIntervals: [{ interval: iB, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(inserted),
    });

    // BEFORE FIX: a blind idx cache hit → block-150's log never streamed → only 1 log.
    const b150 = inserted.logs.find(
      (l: any) => l.blockHash === mkHeader(B_BLOCK).hash,
    );
    expect(b150).toBeDefined();
    expect(inserted.logs).toHaveLength(2);
    // and it streamed ONLY the tail — the extend request starts past the old coveredTo (101)
    expect(tailFroms).toContain(H1 + 1);
  } finally {
    srv.close();
  }
});

// ── INV-9 delegation matrix ─────────────────────────────────────────────────────────────────────────

const headServer = (headFn: () => number | 'fail') =>
  http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        const h = headFn();
        if (h === 'fail') {
          res.writeHead(500).end();
          return;
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: h }));
        return;
      }
      res.writeHead(204).end();
    });
  });

test('INV-9: an interval past the Portal head delegates WHOLE to RPC; the delegated key is consumed exactly once', async () => {
  delete process.env.PORTAL_FINALIZED_HEAD;
  const srv = headServer(() => 100); // head stays at 100
  const port = await listen(srv);
  try {
    const rpcCalls: string[] = [];
    const rpc: any = {
      request: async (req: any) => {
        rpcCalls.push(req.method);
        if (req.method === 'eth_getLogs') return [];
        throw new Error(`unexpected rpc ${req.method}`);
      },
    };
    let childCalls = 0;
    const logger = {
      ...stubLogger(),
      child() {
        childCalls++;
        return stubLogger();
      },
    };
    const filter = mkFilter({ fromBlock: 150, toBlock: 200 });
    const sync = createPortalHistoricalSync({
      common: { logger } as any,
      chain: {
        id: 1,
        name: 'mainnet',
        portal: `http://localhost:${port}`,
        finalityBlockCount: 10,
      } as any,
      rpc,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [150, 200]; // past head 100

    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(),
    });
    expect(logs).toEqual([]);
    expect(rpcCalls).toContain('eth_getLogs'); // the WHOLE interval went to the stock RPC sync

    // syncBlockData consumes the delegated key exactly once (routes to the RPC sync's blockData)…
    const before = childCalls;
    await sync.syncBlockData({
      interval,
      requiredIntervals: [{ interval, filter }],
      logs: [],
      syncStore: mkSyncStore(),
    } as any);
    expect(childCalls).toBeGreaterThan(before); // upstream sync ran (it forks a child logger)

    // …and a SECOND syncBlockData for the same interval is a portal-side no-op (key consumed)
    const after = childCalls;
    const again = await sync.syncBlockData({
      interval,
      requiredIntervals: [{ interval, filter }],
      logs: [],
      syncStore: mkSyncStore(),
    } as any);
    expect(again).toBeUndefined();
    expect(childCalls).toBe(after); // upstream NOT invoked again
  } finally {
    srv.close();
  }
});

test('INV-9: an unknown head (probe persistently failing) delegates to RPC', async () => {
  delete process.env.PORTAL_FINALIZED_HEAD;
  const srv = headServer(() => 'fail');
  const port = await listen(srv);
  try {
    const rpcCalls: string[] = [];
    const rpc: any = {
      request: async (req: any) => {
        rpcCalls.push(req.method);
        if (req.method === 'eth_getLogs') return [];
        throw new Error(`unexpected rpc ${req.method}`);
      },
    };
    const filter = mkFilter();
    const sync = mkSync(port, filter, { rpc });
    const interval: [number, number] = [0, 100];
    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(),
    });
    expect(logs).toEqual([]);
    expect(rpcCalls).toContain('eth_getLogs'); // head unknown → conservative whole-interval delegation
  } finally {
    srv.close();
  }
}, 20_000); // the head probe retries with real backoff before giving up

test('INV-9: stream-realtime mode suppresses the RPC fallback (empty, NOT delegated) — known head past the interval', async () => {
  delete process.env.PORTAL_FINALIZED_HEAD;
  process.env.PORTAL_REALTIME = 'stream';
  const srv = headServer(() => 100);
  const port = await listen(srv);
  try {
    const debugs: string[] = [];
    const logger = {
      ...stubLogger(),
      debug(x: any) {
        debugs.push(x?.msg ?? '');
      },
    };
    let rpcTouched = false;
    const rpc: any = {
      request: async () => {
        rpcTouched = true;
        return [];
      },
    };
    const filter = mkFilter({ fromBlock: 150, toBlock: 200 });
    const sync = createPortalHistoricalSync({
      common: { logger } as any,
      chain: {
        id: 1,
        name: 'mainnet',
        portal: `http://localhost:${port}`,
        finalityBlockCount: 10,
      } as any,
      rpc,
      childAddresses: new Map(),
      eventCallbacks: [{ filter }],
    } as any);
    const interval: [number, number] = [150, 200]; // past head 100

    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(),
    });
    expect(logs).toEqual([]);
    // known head past the interval → the by-design case: log at debug, serve nothing (realtime covers it)
    expect(debugs.some((m) => m.includes('realtime /stream covers it'))).toBe(
      true,
    );
    expect(rpcTouched).toBe(false); // never delegated

    // NOT delegated: syncBlockData is a portal-side no-op, not an RPC-sync call
    const out = await sync.syncBlockData({
      interval,
      requiredIntervals: [{ interval, filter }],
      logs: [],
      syncStore: mkSyncStore(),
    } as any);
    expect(out).toBeUndefined();
    expect(rpcTouched).toBe(false);
  } finally {
    srv.close();
  }
});

test('INV-9: stream-realtime mode with an UNKNOWN head is FATAL — refuses to mark the range synced with no data (finding 6)', async () => {
  delete process.env.PORTAL_FINALIZED_HEAD;
  process.env.PORTAL_REALTIME = 'stream';
  const srv = headServer(() => 'fail'); // probe persistently fails → head unknown
  const port = await listen(srv);
  try {
    let rpcTouched = false;
    const rpc: any = {
      request: async () => {
        rpcTouched = true;
        return [];
      },
    };
    const filter = mkFilter({ fromBlock: 150, toBlock: 200 });
    const sync = mkSync(port, filter, { rpc });
    const interval: [number, number] = [150, 200];
    await expect(
      sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [],
        syncStore: mkSyncStore(),
      }),
    ).rejects.toThrow(/finalized-head probe failed/);
    expect(rpcTouched).toBe(false); // fatal, NOT delegated to RPC (suppressed in stream mode)
  } finally {
    srv.close();
  }
}, 20_000); // the head probe retries with real backoff before giving up

// ── INV-12 stash lifecycle ──────────────────────────────────────────────────────────────────────────

const oneBlockServer = () =>
  http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      if (
        req.url?.includes('finalized-stream') &&
        (q.fromBlock ?? 0) <= 10 &&
        (q.toBlock ?? 1e12) >= 10
      ) {
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(`${JSON.stringify(mkBlock(10))}\n`);
        return;
      }
      res.writeHead(204).end();
    });
  });

test('INV-12: the stash is consumed exactly once — a second syncBlockData returns undefined without re-inserting', async () => {
  const srv = oneBlockServer();
  const port = await listen(srv);
  try {
    const filter = mkFilter();
    const sync = mkSync(port, filter);
    const interval: [number, number] = [0, 100];
    let blockInserts = 0;
    const syncStore = {
      ...mkSyncStore(),
      insertBlocks: () => {
        blockInserts++;
      },
    };
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    const first = await sync.syncBlockData({
      interval,
      logs: [],
      syncStore,
    } as any);
    expect(first).toBeDefined(); // the stashed closest block
    expect(blockInserts).toBe(1);

    const second = await sync.syncBlockData({
      interval,
      logs: [],
      syncStore,
    } as any);
    expect(second).toBeUndefined(); // consumed — nothing re-inserted
    expect(blockInserts).toBe(1);
  } finally {
    srv.close();
  }
});

test("INV-12: 'on' mode keeps overwrite semantics for an upstream range retry; 'strict' makes it loud", async () => {
  // on (default): a re-issued range overwrites its stash entry silently (pre-refactor behavior)
  {
    const srv = oneBlockServer();
    const port = await listen(srv);
    try {
      const filter = mkFilter();
      const sync = mkSync(port, filter);
      const interval: [number, number] = [0, 100];
      const syncStore = mkSyncStore();
      await sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [],
        syncStore,
      });
      await expect(
        sync.syncBlockRangeData({
          interval,
          requiredIntervals: [{ interval, filter }],
          requiredFactoryIntervals: [],
          syncStore,
        }),
      ).resolves.toBeDefined(); // no throw — overwrite
    } finally {
      srv.close();
    }
  }
  // strict: the double-set is an InvariantViolation (tests/CI)
  {
    process.env.PORTAL_CHECKS = 'strict';
    const srv = oneBlockServer();
    const port = await listen(srv);
    try {
      const filter = mkFilter();
      const sync = mkSync(port, filter);
      const interval: [number, number] = [0, 100];
      const syncStore = mkSyncStore();
      await sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [],
        syncStore,
      });
      await expect(
        sync.syncBlockRangeData({
          interval,
          requiredIntervals: [{ interval, filter }],
          requiredFactoryIntervals: [],
          syncStore,
        }),
      ).rejects.toThrow(InvariantViolation);
    } finally {
      srv.close();
      delete process.env.PORTAL_CHECKS;
    }
  }
});

// ── S1: row accounting is keyed to the FETCH, not the idx ───────────────────────────────────────────

test('S1: rows return to baseline after chunks settle and are evicted (no double-free, no orphans)', async () => {
  // Serve every chunk with one block at its start; the read-ahead prefetches extra chunks whose rows
  // register asynchronously. Then jump far ahead (evicting everything behind) and let all in-flight
  // fetches settle: the gate's row count must equal exactly the LIVE cache's rows — here 0, because the
  // final far interval has no data and everything behind was evicted + freed via per-fetch tokens.
  process.env.PORTAL_READAHEAD = '2';
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      const q = body ? JSON.parse(body) : {};
      if (!req.url?.includes('finalized-stream')) {
        res.writeHead(204).end();
        return;
      }
      const from = q.fromBlock ?? 0;
      // chunks below 3M have one block each at a chunk boundary; beyond → empty
      if (from < 3_000_000) {
        const n = from + 10;
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(`${JSON.stringify(mkBlock(n))}\n`);
        return;
      }
      res.writeHead(204).end();
    });
  });
  const port = await listen(srv);
  try {
    const filter = mkFilter({ fromBlock: 0, toBlock: 10_000_000 });
    const sync = mkSync(port, filter);
    const gate = sharedGate(loadPortalConfig({}));
    const baseline = gate.snapshot().rows;
    const syncStore = mkSyncStore();

    const i1: [number, number] = [0, 100];
    await sync.syncBlockRangeData({
      interval: i1,
      requiredIntervals: [{ interval: i1, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    // far ahead: evicts chunk 0 AND the read-ahead chunks (some possibly still in flight)
    const i2: [number, number] = [9_500_000, 9_500_100];
    await sync.syncBlockRangeData({
      interval: i2,
      requiredIntervals: [{ interval: i2, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });
    // let any in-flight read-ahead fetches settle, then evict everything behind once more
    await new Promise((r) => setTimeout(r, 300));
    await sync.syncBlockRangeData({
      interval: i2,
      requiredIntervals: [{ interval: i2, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });

    // every token was freed exactly once; no stale fetch registered rows into an evicted slot
    expect(gate.snapshot().rows).toBe(baseline);
  } finally {
    srv.close();
    delete process.env.PORTAL_READAHEAD;
  }
});
