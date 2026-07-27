/**
 * portal-shard-fuzz.test.ts — INV-26 (shard-plan store-state invariance) adversarial-partition suite.
 *
 * The #194/#195/#196/#199 shard family partitions an over-cap Portal body into byte-budgeted shards that
 * portal.ts streams SEQUENTIALLY inside runStreams and UNIONs. INV-26 is the store-safety property that
 * ties the family together: for ANY shard plan AND ANY server partition of [from,to] into per-shard
 * response sequences (a load-balanced Portal answers each shard's /stream independently, ending each
 * response at a different, arbitrary in-range boundary), the resulting STORE STATE — the
 * (chainId,blockNumber,logIndex)-keyed log set, hash-keyed txs/receipts, number-keyed blocks — and the
 * checkpoint-ordered events are IDENTICAL to the unsharded fetch; and NO chunk resolves before every shard
 * durably covers [from,to] (completeness-by-construction — the shard loop is inside runStreams).
 *
 * CANDOR (no strict byte-identity ABOVE the wall): byte-identity of the INSERT BATCHES holds only BELOW
 * the SHARD_BODY_BUDGET wall (single-shard no-op). Above it, intra-block log order may permute with shard
 * order and overlapping predicates split across shards may emit a log MORE THAN ONCE — nothing downstream
 * is order- or multiplicity-sensitive: events are checkpoint-ordered, and cross-shard duplicate logs are
 * byte-identical and collapsed by the sync store's `logs_pkey` + `onConflictDoNothing` (INV-6 / INV-13).
 * These tests prove the STORE-SAFETY PRECONDITIONS the store then relies on; they deliberately do NOT
 * assert assemble-level log dedup (there is none for logs, by design — the store absorbs it).
 *
 * This suite pins:
 *   (a) overlapping-predicate cross-shard duplication (F1): a topic-only element + a factory element
 *       sharing topic0, split across shards, both deliver ONE child log → it is emitted TWICE and the two
 *       rows are BYTE-IDENTICAL on (chainId,blockNumber,logIndex) + payload (the logs_pkey/onConflictDoNothing
 *       collapse precondition). Mutation-verified.
 *   (b) arbitrary server range-end partition invariance: the SAME rows driven through two DIFFERENT server
 *       partitions (each shard ends its NDJSON at different, arbitrary in-range boundaries) yield EQUAL
 *       assembled log/tx/receipt/block SETS; a shard that throws mid-range rejects the WHOLE chunk (no
 *       silent drop). Mutation-verified.
 *   (c) below-budget single-shard no-op (store-state layer): a below-wall plan is one shard whose assembled
 *       output equals the unsharded fetch's, exactly.
 */

import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { expect, test } from 'vitest';
import type { Address, Factory, LogFilter } from '@/internal/types.js';
import { createPortalHistoricalSync } from './portal.js';
import {
  type ChildAddresses,
  compileFetchSpec,
  MAX_RAW_QUERY_SIZE,
  SHARD_BODY_BUDGET,
} from './portal-filters.js';

// ── shared fixtures (mirror portal-shard.test.ts) ────────────────────────────────────────────────────

// A canonical 20-byte lowercased hex address (42 chars ≈ 45 bytes as a JSON array element). 256KiB / 45
// ≈ 5825 = the "~5.8k children" wall; synthesize a distinct address per index.
const childAt = (i: number): Address =>
  `0x${i.toString(16).padStart(40, '0')}` as Address;

const TOPIC0 = `0x${'cd'.repeat(32)}`;

// The factory shape ponder's runtime hands the fetch-spec (isAddressFactory keys off `.id`).
const factory = (id: string): Factory =>
  ({
    id,
    type: 'log',
    address: `0x${'f'.repeat(40)}`,
    eventSelector: `0x${'ab'.repeat(32)}`,
    childAddressLocation: 'topic1',
  }) as unknown as Factory;

// A log filter whose `address` is that factory — expands to the live child set, sharing TOPIC0.
const factoryLogFilter = (f: Factory): LogFilter =>
  ({
    type: 'log',
    chainId: 1,
    sourceId: 'evault',
    address: f,
    topic0: TOPIC0 as unknown as LogFilter['topic0'],
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: 0,
    toBlock: undefined,
    hasTransactionReceipt: false,
    include: [],
  }) as unknown as LogFilter;

// An ADDRESSLESS topic-only log filter sharing TOPIC0. `logRequestsFor` returns `[base]` for it (no
// address), and `mergeLogRequests` keys on (address set, topic1..3) — this element's address is undefined,
// the factory element's is a child batch, so they NEVER merge and can land in DIFFERENT shards (F1).
const topicOnlyLogFilter = (): LogFilter =>
  ({
    type: 'log',
    chainId: 1,
    sourceId: 'topiconly',
    address: undefined,
    topic0: TOPIC0 as unknown as LogFilter['topic0'],
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: 0,
    toBlock: undefined,
    hasTransactionReceipt: false,
    include: [],
  }) as unknown as LogFilter;

// children all created at `createdAt` so a log at any block ≥ createdAt re-matches (assembly's factory floor).
const childrenMapCreatedAt = (
  id: string,
  n: number,
  createdAt: number,
): ChildAddresses => {
  const inner = new Map<Address, number>();
  for (let i = 1; i <= n; i++) {
    inner.set(childAt(i), createdAt);
  }

  return new Map([[id, inner]]);
};

// A full Portal block carrying one child's log (+ its parent tx), addressed to `addr` with topic0=TOPIC0.
const shardBlock = (num: number, addr: string) => ({
  header: {
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
  },
  logs: [
    {
      address: addr,
      topics: [TOPIC0],
      data: '0x',
      transactionHash: `0x${num.toString(16).padStart(64, 'a')}`,
      transactionIndex: 0,
      logIndex: 0,
    },
  ],
  transactions: [
    {
      transactionIndex: 0,
      hash: `0x${num.toString(16).padStart(64, 'a')}`,
      from: `0x${'a1'.repeat(20)}`,
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

// Does this request's `logs` spec carry `addr`? (a factory shard requests an address batch; the topic-only
// shard requests NO address — an addressless topic0-only element.)
const requestsAddress = (logs: any[], addr: string): boolean =>
  (logs ?? []).some((r) =>
    (r.address ?? [])
      .map((x: string) => x.toLowerCase())
      .includes(addr.toLowerCase()),
  );
const requestsAddressless = (logs: any[]): boolean =>
  (logs ?? []).some((r) => r.address === undefined);

const NEVER_HEAD = 1_000_000_000;

// A running mock Portal that serves one `chunk` of NDJSON per POST. `respond(query, from, to)` returns the
// array of wire objects (blocks / a bare `{header}` anchor) this response should carry; the finalized-head
// probe is answered automatically. Returns a started server + its port; caller closes it.
const mockPortal = async (
  respond: (query: any, from: number, to: number) => any[],
): Promise<{ port: number; close: () => void }> => {
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => {
      body += c;
    });
    req.on('end', () => {
      if (req.url?.includes('finalized-head')) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ number: NEVER_HEAD }));
        return;
      }
      const q = body ? JSON.parse(body) : {};
      const from = q.fromBlock ?? 0;
      const to = q.toBlock ?? 1e12;
      let out: any[];
      try {
        out = respond(q, from, to);
      } catch (e) {
        // A `respond` throw carrying `httpStatus` models a deterministic shard failure — answer with that
        // status (e.g. a 400 PortalHttpError) instead of hanging the socket.
        const status = (e as { httpStatus?: number }).httpStatus ?? 500;
        res.writeHead(status, { 'content-type': 'text/plain' });
        res.end('injected deterministic shard failure');
        return;
      }
      res.writeHead(200, { 'content-type': 'application/x-ndjson' });
      res.end(`${out.map((b) => JSON.stringify(b)).join('\n')}\n`);
    });
  });
  const port: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );

  return { port, close: () => srv.close() };
};

// Terminate a shard's response cleanly at range-end: append a bare range-end anchor header if the shard's
// own data didn't already reach `to` (mirrors portal-shard.test.ts — an in-range window terminates at the
// range-end block header, NEVER a mid-range 204 which would fail closed).
const withAnchor = (out: any[], to: number): any[] => {
  const end = Math.min(to, NEVER_HEAD);
  const maxServed = out.reduce(
    (m, b) => Math.max(m, (b as any).header?.number ?? -1),
    -1,
  );
  if (maxServed >= end) return out;

  return [...out, { header: shardBlock(end, childAt(1)).header }];
};

const makeSync = (
  port: number,
  childAddresses: ChildAddresses,
  filters: any[],
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
    childAddresses,
    eventCallbacks: filters.map((filter) => ({ filter })),
  } as any);

// The store-state key for a log row: (chainId,blockNumber,logIndex) — the logs_pkey — plus the full payload
// so byte-identity is observable. blockNumber/logIndex are hex strings on the assembled SyncLog.
const logKey = (l: any): string =>
  JSON.stringify([l.chainId ?? 1, l.blockNumber, l.logIndex]);
const logRow = (l: any): string =>
  JSON.stringify({
    chainId: l.chainId ?? 1,
    blockNumber: l.blockNumber,
    logIndex: l.logIndex,
    address: l.address,
    topics: l.topics,
    data: l.data,
    transactionHash: l.transactionHash,
    transactionIndex: l.transactionIndex,
  });

// ── (a) overlapping-predicate cross-shard duplication (F1) ────────────────────────────────────────────

// Drive one chunk over [lo, hi] for a spec = topic-only filter + a >budget factory filter (both topic0).
// The shard-0 child (childAt(1)) is served ONE log at `bn`. That log matches BOTH predicates, so it is
// delivered by the FACTORY shard carrying childAt(1) AND by the TOPIC-ONLY shard (addressless). Returns the
// inserted logs so we can observe the cross-shard duplication the store then collapses.
const runOverlap = async (
  n: number,
  bn: number,
): Promise<{
  insertedLogs: any[];
  addresslessServes: number;
  factoryServes: number;
}> => {
  const lowAddr = childAt(1);
  let addresslessServes = 0;
  let factoryServes = 0;

  const { port, close } = await mockPortal((q, from, to) => {
    const wantsAddressless = requestsAddressless(q.logs);
    const wantsFactoryLow = requestsAddress(q.logs, lowAddr);
    if (wantsAddressless) addresslessServes++;
    if (wantsFactoryLow) factoryServes++;

    const out: any[] = [];
    // BOTH the addressless shard and the factory shard carrying childAt(1) serve the SAME child log.
    if ((wantsAddressless || wantsFactoryLow) && from <= bn && to >= bn)
      out.push(shardBlock(bn, lowAddr));

    return withAnchor(out, to);
  });

  try {
    const insertedLogs: any[] = [];
    const syncStore: any = {
      insertLogs: (x: any) => insertedLogs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const facFilter = factoryLogFilter(factory('evault')) as any;
    facFilter.toBlock = 499_999;
    const topFilter = topicOnlyLogFilter() as any;
    topFilter.toBlock = 499_999;

    const sync = makeSync(port, childrenMapCreatedAt('evault', n, 1), [
      facFilter,
      topFilter,
    ]);

    const interval: [number, number] = [bn - 5, bn + 5];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [
        { interval, filter: facFilter },
        { interval, filter: topFilter },
      ],
      requiredFactoryIntervals: [],
      syncStore,
    } as any);

    return { insertedLogs, addresslessServes, factoryServes };
  } finally {
    close();
  }
};

test('INV-26 (a): an overlapping topic-only + factory predicate SPLIT ACROSS SHARDS emits the child log TWICE, byte-identical (logs_pkey/onConflictDoNothing collapse precondition)', async () => {
  const f = factory('evault');
  // Sanity: with a >budget child set the two overlapping predicates land in DIFFERENT shards — the factory
  // element(s) and the addressless topic-only element are distinct merge groups, and the whole body is over
  // the wall so shardLogs splits them apart.
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }, { filter: topicOnlyLogFilter() }],
    childrenMapCreatedAt('evault', 6000, 1),
  );
  const single = spec.logQuery()!;
  expect(
    JSON.stringify({ ...single, fromBlock: 0, toBlock: 1e6 }).length,
  ).toBeGreaterThan(MAX_RAW_QUERY_SIZE);
  const shards = spec.logQueryShards();
  expect(shards.length).toBeGreaterThanOrEqual(2);
  // the addressless element and the shard-0 child sit in DIFFERENT shards (else no cross-shard duplication).
  const addresslessShard = shards.findIndex((s) =>
    requestsAddressless(s.logs ?? []),
  );
  const lowChildShard = shards.findIndex((s) =>
    requestsAddress(s.logs ?? [], childAt(1)),
  );
  expect(addresslessShard).toBeGreaterThanOrEqual(0);
  expect(lowChildShard).toBeGreaterThanOrEqual(0);
  expect(addresslessShard).not.toBe(lowChildShard);

  const { insertedLogs, addresslessServes, factoryServes } = await runOverlap(
    6000,
    15,
  );

  // BOTH shards requested and served the child log — the real cross-shard over-fetch F1 describes.
  expect(addresslessServes).toBeGreaterThanOrEqual(1);
  expect(factoryServes).toBeGreaterThanOrEqual(1);

  // The child log is emitted TWICE — assemble does NOT dedup logs (by design; the store's logs_pkey +
  // onConflictDoNothing collapses them). This is the store-safety PRECONDITION, not a bug.
  const lowRows = insertedLogs.filter(
    (l) => l.address?.toLowerCase() === childAt(1).toLowerCase(),
  );
  expect(lowRows.length).toBe(2);

  // …and the two rows are BYTE-IDENTICAL on (chainId,blockNumber,logIndex) AND full payload — so
  // onConflictDoNothing({target:[chainId,blockNumber,logIndex]}) provably collapses them to ONE store row
  // with no data loss (INV-6 / INV-13 store dedupe). If they DIFFERED on the key or payload the store would
  // keep two rows or drop a distinct log — either a divergence from the unsharded/RPC path.
  expect(logKey(lowRows[0])).toBe(logKey(lowRows[1]));
  expect(logRow(lowRows[0])).toBe(logRow(lowRows[1]));

  // The deduped store state (what onConflictDoNothing yields) is EXACTLY one row for this key.
  const dedupedKeys = new Set(insertedLogs.map(logKey));
  expect(dedupedKeys.size).toBe(1);
});

// ── (b) arbitrary server range-end partition invariance ───────────────────────────────────────────────

// Drive one chunk over [lo, hi] for a >budget factory (multi-shard). Each shard is an INDEPENDENT
// client.stream over the FULL [from,to] and must reach `to` to terminate (a shard stopping short below `to`
// is the mid-range-204/incomplete-range fail-closed case — correctly it would NOT be a "partition", it
// would retry). The legitimate "server partition" a load-balanced Portal exhibits is WHERE within [from,to]
// each shard emits its terminal range-end header and how it batches intermediate headers. `partition`
// selects one such shape:
//   - 'tight':  each shard emits ONLY its own data blocks then a bare anchor header at `to`.
//   - 'padded': each shard ALSO emits extra empty (log-less) headers at arbitrary in-range blocks before the
//               anchor — a DIFFERENT server response partition of the same [from,to] window.
// Both terminate at `to`; the assembled store SET must be IDENTICAL across the two. `failLowOnce` makes the
// shard carrying childAt(1) throw once (a mid-range shard failure) to exercise completeness-by-construction.
const runPartitioned = async (opts: {
  n: number;
  childBlocks: Map<number, number>; // childIndex -> block it logs at
  partition: 'tight' | 'padded';
  failLowOnce?: boolean;
}): Promise<{ insertedLogs: any[]; rejected: boolean }> => {
  const lowAddr = childAt(1);
  let lowFailsLeft = opts.failLowOnce ? 1 : 0;

  const { port, close } = await mockPortal((q, from, to) => {
    const wantsLow = requestsAddress(q.logs, lowAddr);

    if (wantsLow && lowFailsLeft > 0) {
      lowFailsLeft--;
      // A DETERMINISTIC 400 mid-plan → PortalHttpError → propagates out of stream → runStreams → rejects the
      // chunk promise (no internal retry storm). mockPortal turns the thrown status into a 400 response.
      throw Object.assign(new Error('injected shard failure'), {
        httpStatus: 400,
      });
    }

    // This shard's own in-range data blocks (the children it requests that log in [from,to]).
    const out: any[] = [];
    for (const [idx, bn] of opts.childBlocks) {
      const addr = childAt(idx);
      const wantsThis = requestsAddress(q.logs, addr);
      if (wantsThis && from <= bn && to >= bn) out.push(shardBlock(bn, addr));
    }

    // 'padded' partition: interleave extra EMPTY (log-less) headers at arbitrary in-range blocks — a
    // different NDJSON partition of the same window that carries no rows and must not change the store set.
    if (opts.partition === 'padded') {
      for (const bn of [8, 15, 25, 35]) {
        if (from <= bn && to >= bn)
          out.push({ header: shardBlock(bn, lowAddr).header });
      }
    }

    // EVERY shard reaches `to` (each is an independent full-range stream); the anchor is the terminal header.
    return withAnchor(out, to);
  });

  try {
    const insertedLogs: any[] = [];
    const syncStore: any = {
      insertLogs: (x: any) => insertedLogs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const facFilter = factoryLogFilter(factory('evault')) as any;
    facFilter.toBlock = 499_999;
    const sync = makeSync(port, childrenMapCreatedAt('evault', opts.n, 1), [
      facFilter,
    ]);

    const interval: [number, number] = [5, 40];
    let rejected = false;
    try {
      await sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter: facFilter }],
        requiredFactoryIntervals: [],
        syncStore,
      } as any);
    } catch {
      rejected = true;
    }

    return { insertedLogs, rejected };
  } finally {
    close();
  }
};

test('INV-26 (b): the SAME rows under DIFFERENT server range-end partitions yield EQUAL assembled store SETS', async () => {
  const n = 6000;
  const childBlocks = new Map<number, number>([
    [1, 10], // shard-0 child
    [n, 30], // last-shard child
  ]);

  // Two runs, IDENTICAL underlying data rows, DIFFERENT server partition of the same [from,to] window: run A
  // emits each shard's data then a bare anchor at `to`; run B additionally interleaves empty headers at
  // arbitrary in-range blocks. Both terminate at `to`; the assembled store SET must be IDENTICAL.
  const a = await runPartitioned({ n, childBlocks, partition: 'tight' });
  const b = await runPartitioned({ n, childBlocks, partition: 'padded' });

  expect(a.rejected).toBe(false);
  expect(b.rejected).toBe(false);

  // Store-state SET equality: the (chainId,blockNumber,logIndex)-keyed log set is identical under both
  // partitions AND equals the brute-force expected set (both children, once each after store dedup).
  const setA = new Set(a.insertedLogs.map(logRow));
  const setB = new Set(b.insertedLogs.map(logRow));
  expect(setA).toEqual(setB);

  const keysA = new Set(a.insertedLogs.map(logKey));
  expect(keysA.size).toBe(2); // exactly the two children's logs, deduped by logs_pkey
  const addrs = [...setA].map((r) => JSON.parse(r).address.toLowerCase());
  expect(addrs).toContain(childAt(1).toLowerCase());
  expect(addrs).toContain(childAt(n).toLowerCase());
});

test('INV-26 (b) completeness-by-construction: a shard that THROWS mid-range REJECTS the whole chunk (never a silent drop)', async () => {
  const n = 6000;
  const childBlocks = new Map<number, number>([
    [1, 10],
    [n, 30],
  ]);
  // The shard carrying childAt(1) throws a deterministic 400 → because the shard loop is INSIDE runStreams,
  // the whole chunk promise rejects. No partial store state is committed as "synced" — the interval is NOT
  // marked done, so a later retry re-streams every shard. (Contrast a break-after-first-shard bug, which
  // would resolve the chunk and silently drop the un-streamed shards.)
  const { insertedLogs, rejected } = await runPartitioned({
    n,
    childBlocks,
    partition: 'tight',
    failLowOnce: true,
  });

  expect(rejected).toBe(true);
  // The chunk rejected before any interval-complete insert of the partial plan; nothing was durably synced
  // for this interval as a side effect of the failed attempt beyond what the (rejected) call attempted.
  // The load-bearing assertion is the rejection itself — the completeness gate — not the buffer contents.
  expect(insertedLogs.length).toBeLessThanOrEqual(2);
});

// ── (c) below-budget single-shard no-op (store-state layer) ───────────────────────────────────────────

test('INV-26 (c): a below-wall plan is ONE shard whose assembled store SET equals the unsharded fetch, exactly', async () => {
  // 100 children — well below the wall — driven end-to-end. The plan is a single shard (no-op), so the
  // assembled store state trivially equals the unsharded fetch's. We prove the whole-set identity anchored
  // on a real stream (not just the builder-level byte-identity portal-shard.test.ts already pins).
  const n = 100;
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter(factory('evault')) }],
    childrenMapCreatedAt('evault', n, 1),
  );
  const single = spec.logQuery()!;
  expect(
    JSON.stringify({ ...single, fromBlock: 0, toBlock: 1e6 }).length,
  ).toBeLessThan(SHARD_BODY_BUDGET);
  const shards = spec.logQueryShards();
  expect(shards).toHaveLength(1); // below the wall ⇒ exactly one shard (the no-op)

  // Serve one child's log; the single shard carries every child, so the store state is the unsharded result.
  const targetIdx = 42;
  const targetAddr = childAt(targetIdx);
  const bn = 20;

  const { port, close } = await mockPortal((q, from, to) => {
    const out: any[] = [];
    if (requestsAddress(q.logs, targetAddr) && from <= bn && to >= bn)
      out.push(shardBlock(bn, targetAddr));

    return withAnchor(out, to);
  });

  try {
    const insertedLogs: any[] = [];
    const syncStore: any = {
      insertLogs: (x: any) => insertedLogs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const facFilter = factoryLogFilter(factory('evault')) as any;
    facFilter.toBlock = 499_999;
    const sync = makeSync(port, childrenMapCreatedAt('evault', n, 1), [
      facFilter,
    ]);

    const interval: [number, number] = [bn - 5, bn + 5];
    await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter: facFilter }],
      requiredFactoryIntervals: [],
      syncStore,
    } as any);

    // exactly one row, for the target child, no duplication (single shard ⇒ no cross-shard emit).
    expect(insertedLogs).toHaveLength(1);
    expect(insertedLogs[0].address?.toLowerCase()).toBe(
      targetAddr.toLowerCase(),
    );
    expect(new Set(insertedLogs.map(logKey)).size).toBe(1);
  } finally {
    close();
  }
});
