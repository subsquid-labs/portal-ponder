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

// Fixed chunk width for S1's chunk-boundary mock (matches the PORTAL_CHUNK_BLOCKS default).
const CHUNK_BLOCKS = 500_000;

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

// A header-only NDJSON record (NO logs/txs, so it registers no rows) used as the range-END cursor
// anchor. The real Portal terminates an in-range /finalized-stream by serving the range-end block header
// as the cursor anchor — it does NOT 204 an in-range window (issue #47 probe). A mid-range 204 now fails
// closed (PortalIncompleteRangeError), so these mocks must anchor the range end to end the stream cleanly
// instead of 204-ing the served-through tail — otherwise `stream` retries the 204 to the budget and hangs.
const anchor = (num: number) => ({ header: mkHeader(num) });

// Serve a Portal-accurate /finalized-stream response over [from, to]: the matching `blocks` (may be empty)
// PLUS the range-end header anchor at min(to, head) whenever the window is in range (from ≤ head); a bare
// 204 ONLY when `from` is above the served head. `head` defaults to `to` (the request's own ceiling) — the
// common case where the mock has no separate finalized-head notion. Blocks past the anchor are dropped
// (the replica can't serve them); their absence is exactly what a lagging tail looks like.
const streamRes = (
  res: http.ServerResponse,
  from: number,
  to: number,
  blocks: unknown[],
  head: number = to,
) => {
  if (from > head) {
    res.writeHead(204).end();
    return;
  }
  const end = Math.min(to, head);
  // Only append the header-only anchor if no served block already reaches `end` — the real Portal emits a
  // given block exactly once; a duplicate would double-insert / double-count.
  const maxServed = blocks.reduce(
    (m, b) => Math.max(m, (b as any).header?.number ?? -1),
    -1,
  );
  const out = maxServed >= end ? blocks : [...blocks, anchor(end)];
  const lines = out.map((b) => JSON.stringify(b));
  res.writeHead(200, { 'content-type': 'application/x-ndjson' });
  res.end(`${lines.join('\n')}\n`);
};

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
      // the refetch's tail [11,100]: anchor the range end so the stream terminates (not a mid-range 204)
      streamRes(res, q.fromBlock ?? 0, q.toBlock ?? 0, []);
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
      // anchor the tail so the stream terminates cleanly (a mid-range 204 would now fail closed)
      streamRes(res, q.fromBlock ?? 0, q.toBlock ?? 0, []);
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
      // anchor the range end at the current head so an in-range window ends cleanly (a 204 fires only past
      // the head); a served-through tail must NOT 204 mid-range (issue #47).
      streamRes(res, from, to, out, head);
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
      // an in-range stream POST (the client only POSTs data below the observed head) has no matching logs
      // here → 200 with just the range-end header anchor at its own toBlock; never a mid-range 204 (#47).
      const q = body ? JSON.parse(body) : {};
      streamRes(res, q.fromBlock ?? 0, q.toBlock ?? 0, []);
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

test('INV-9: stream-realtime mode with a KNOWN head below the interval is FATAL, not empty — a stale-LOW probe must not mark the range synced (wave 4)', async () => {
  // This used to debug + return [] as "realtime /stream covers it". But clampFinalizedToPortalHead bounds
  // every historical interval at the boundary head and realtime streams only ABOVE that boundary — so an
  // interval "past the head" here means OUR probe is stale-LOW (a lagging replica), and [] marked the
  // interval synced while NO path ever delivers its data: the exact G4/C11 silent gap. Fatal now, like
  // the unknown-head case; still never delegated to RPC.
  delete process.env.PORTAL_FINALIZED_HEAD;
  process.env.PORTAL_REALTIME = 'stream';
  const srv = headServer(() => 100);
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
    const interval: [number, number] = [150, 200]; // past head 100

    await expect(
      sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [],
        syncStore: mkSyncStore(),
      }),
    ).rejects.toThrow(/stale-LOW/);
    expect(rpcTouched).toBe(false); // fatal, NOT delegated to RPC (suppressed in stream mode)
  } finally {
    srv.close();
  }
});

test('INV-9: the cached Portal head is MONOTONIC — a stale-LOW later probe cannot unserve an interval at/below the highest observed head (wave 4)', async () => {
  // Load-balanced Portal replicas answer probes independently, so a later probe can return a LOWER head.
  // Adopting it regressed the cache; in stream mode an interval at/below the true head then read as
  // "past the head" — under the old code that returned [] (marked synced, silent gap), and under the
  // wave-4 contract it would fatal spuriously. With the max-keep, the interval is served normally.
  delete process.env.PORTAL_FINALIZED_HEAD;
  process.env.PORTAL_REALTIME = 'stream';
  let probes = 0;
  const srv = headServer(() => {
    probes += 1;
    return probes === 1 ? 200 : 150; // first replica answers 200; every later probe is stale-LOW
  });
  const port = await listen(srv);
  try {
    const filter = mkFilter({ fromBlock: 0, toBlock: 300 });
    const sync = mkSync(port, filter, {});

    // Call A seeds the head cache at 200 (first probe).
    const iA: [number, number] = [190, 200];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(),
    });
    expect(probes).toBeGreaterThan(0);

    // Call B targets past the cache → triggers re-probes, which now answer a stale-LOW 150. The head
    // must NOT regress; [201,210] is genuinely past the highest observed head → fatal (stream mode).
    const iB: [number, number] = [201, 210];
    await expect(
      sync.syncBlockRangeData({
        interval: iB,
        requiredIntervals: [{ interval: iB, filter }],
        requiredFactoryIntervals: [],
        syncStore: mkSyncStore(),
      }),
    ).rejects.toThrow(/stale-LOW|past the probed finalized head/);

    // Call C sits at/below the highest observed head (200). Under the regression (portalHead = 150 after
    // B's probes) this would read as past-the-head and fatal; with the monotonic cache it serves (204 →
    // no logs, no throw).
    const iC: [number, number] = [160, 200];
    const logs = await sync.syncBlockRangeData({
      interval: iC,
      requiredIntervals: [{ interval: iC, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(),
    });
    expect(logs).toEqual([]);
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

test('INV-9 (FIX 5): a PORTAL_FINALIZED_HEAD pin survives ensureChunkSize probing the live head — an interval above the pin (but below the live head) still DELEGATES to RPC', async () => {
  // The pin is authoritative for the finality/delegation decision. But chunk scaling (unless
  // PORTAL_CHUNK_FIXED) still probes the LIVE head via ensureChunkSize. The bug assigned `portalHead = h`
  // (the live head) unconditionally in ensureChunkSize, CLOBBERING the pin — so once a sub-pin interval
  // triggered scaling, a later interval above the pin but below the live head was SERVED by the Portal
  // instead of delegated to RPC (a finality-safety violation: the Portal only has finalized data ≤ pin).
  // FIX 5 adopts the probe as `portalHead` only when there is no pin (`cfg.finalizedHead === undefined`);
  // scaling still uses the live `h`. Here: pin=100, live head=1e9, chunk scaling ON.
  const PIN = 100;
  const LIVE = 1_000_000_000;
  process.env.PORTAL_FINALIZED_HEAD = String(PIN); // the pin
  delete process.env.PORTAL_CHUNK_FIXED; // chunk scaling ON → ensureChunkSize probes the LIVE head

  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: LIVE })); // live head far past the pin
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 0;
      // serve one block for the sub-pin interval A (block 10); ALWAYS anchor the range end at `to` so an
      // in-range window (incl. the served-through tail [11,50]) terminates cleanly — never a mid-range 204.
      const out = from <= 10 && to >= 10 ? [mkBlock(10)] : ([] as unknown[]);
      streamRes(res, from, to, out);
    });
  });
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
    // one bounded source spanning both intervals; its toBlock past the pin does NOT relax the pin.
    const filter = mkFilter({ fromBlock: 0, toBlock: 200 });
    const sync = createPortalHistoricalSync({
      common: { logger: stubLogger() } as any,
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

    // interval A [0,50] ≤ pin → served by the Portal; this call runs ensureChunkSize, which probes the
    // LIVE head. Under the bug that probe clobbers portalHead = LIVE.
    const iA: [number, number] = [0, 50];
    await sync.syncBlockRangeData({
      interval: iA,
      requiredIntervals: [{ interval: iA, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(),
    });
    expect(rpcCalls).not.toContain('eth_getLogs'); // A was served by the Portal, not delegated

    // interval B [150,200] > pin(100) but ≤ live(1e9). With the pin intact it delegates; under the bug
    // portalHead was clobbered to LIVE so isFinalityGap(200, LIVE) is false → the Portal SERVED it.
    const iB: [number, number] = [150, 200];
    const logs = await sync.syncBlockRangeData({
      interval: iB,
      requiredIntervals: [{ interval: iB, filter }],
      requiredFactoryIntervals: [],
      syncStore: mkSyncStore(),
    });
    expect(logs).toEqual([]);
    expect(rpcCalls).toContain('eth_getLogs'); // FIX 5: the pin governs delegation → B went to RPC
  } finally {
    srv.close();
  }
});

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
        // batch 1: block 10 only (below `to`), so the cursor advances into the tail as before
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        res.end(`${JSON.stringify(mkBlock(10))}\n`);
        return;
      }
      // the served-through tail [11, to]: anchor the range end so the stream terminates (not a 204)
      streamRes(res, q.fromBlock ?? 0, q.toBlock ?? 0, []);
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

test('S1: rows settle to exactly the LIVE cache after chunks are evicted (no double-free, no orphans)', async () => {
  // Serve every chunk with one block at its start; the read-ahead prefetches extra chunks whose rows
  // register asynchronously. Then jump far ahead (evicting everything behind) and let all in-flight fetches
  // settle: the gate's row count must equal EXACTLY the LIVE cache's rows — every evicted/stale fetch's
  // token freed once, no orphan rows. Post-#47 the live cache is NOT empty: an in-range window always
  // carries the range-end block HEADER as a cursor anchor (a header-only block → countRows counts 1 per
  // block, so 1 row), so the far interval's live chunk plus its one live read-ahead chunk legitimately hold
  // one anchor row each. The invariant is thus (i) rows == that live total and (ii) rows are STABLE across
  // the settle — a stale read-ahead fetch that lands AFTER its eviction must add nothing (the token.freed
  // guard). Pre-#47 the far window 204'd (no anchor) so this total was 0; the guarded orphan-freeing is
  // identical, only the legitimate live baseline shifted.
  process.env.PORTAL_READAHEAD = '2';
  // Pin the chunk width so the mock's chunk-boundary math below is exact (density scaling is already
  // off via PORTAL_CHUNK_FIXED in beforeEach; this fixes the grid to a known 500k regardless).
  process.env.PORTAL_CHUNK_BLOCKS = String(CHUNK_BLOCKS);
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
      const to = q.toBlock ?? 0;
      // EXACTLY one block per chunk, positioned at the chunk boundary; the block-only batch advances the
      // cursor into the chunk, and the CONTINUATION request [boundary+11, to] carries just the range-end
      // header anchor at `to` so the stream terminates by REACHING `to` — not by a mid-range 204, which now
      // fails closed (issue #47). Preserving the two-response shape keeps the read-ahead timing/row-accounting
      // race intact. The anchor is header-only (no logs/txs) but still counts as one block-row (countRows
      // counts 1 per block); a live chunk therefore retains that single anchor row (see the assertion below).
      const boundary = Math.ceil(from / CHUNK_BLOCKS) * CHUNK_BLOCKS;
      const servesBlock =
        boundary < 3_000_000 && boundary <= to && from <= boundary + 10;
      if (servesBlock) {
        const payload = `${JSON.stringify(mkBlock(boundary + 10))}\n`;
        // The read-ahead chunks (boundary ≥ CHUNK_BLOCKS — i.e. NOT chunk 0) respond SLOWLY so they are still
        // in flight when the far-ahead interval evicts and frees their tokens. That is the exact race the
        // per-fetch `token.freed` guard exists to close: a stale stream that outlives its eviction must not
        // register orphan rows into an already-freed budget. Chunk 0 (boundary 0) answers immediately so the
        // first interval's await returns promptly.
        res.writeHead(200, { 'content-type': 'application/x-ndjson' });
        if (boundary >= CHUNK_BLOCKS) {
          setTimeout(() => res.end(payload), 150);
          return;
        }
        res.end(payload);
        return;
      }
      // continuation / past-3M / empty in-range window → terminate at the range-end anchor (never a 204)
      streamRes(res, from, to, []);
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
    // let ALL in-flight fetches settle — the slow read-ahead chunks from BEHIND the jump (evicted, tokens
    // freed) AND the far interval's OWN read-ahead chunk (still live) — then re-issue the far interval so a
    // second eviction pass runs. A stale fetch that lands after its eviction must register NOTHING (the
    // token.freed guard); only the two LIVE chunks may hold rows.
    await new Promise((r) => setTimeout(r, 300));
    await sync.syncBlockRangeData({
      interval: i2,
      requiredIntervals: [{ interval: i2, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    });

    // The gate holds EXACTLY the live cache's rows: the far interval's chunk plus its ONE prefetched
    // read-ahead chunk — each an empty (no-log) window carrying a single range-end anchor row (post-#47 an
    // in-range window always anchors the range end; a header-only block counts as 1 row). Everything BEHIND
    // the jump was evicted and freed exactly once — a single leaked orphan row would make this 3+.
    expect(gate.snapshot().rows).toBe(2);
    expect(baseline).toBe(0); // sanity: the gate started empty (the residual is all live, not pre-existing)
  } finally {
    srv.close();
    delete process.env.PORTAL_READAHEAD;
    delete process.env.PORTAL_CHUNK_BLOCKS;
  }
});
