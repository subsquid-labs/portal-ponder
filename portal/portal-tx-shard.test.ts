/**
 * portal-tx-shard.test.ts — issue #196 item-2 (factory → tx-filter query sharding) regression suite.
 *
 * THE WALL (tx variant). When a factory's discovered child set feeds a TRANSACTION filter (from/to)
 * rather than a log filter, `txRequestsFor` packs ALL child addresses into ONE `TxRequest.from` (or
 * `.to`) array with NO PORTAL_MAX_ADDRESSES batching (unlike `logRequestsFor`, whose elements are
 * pre-batched ≤1000 addresses each). So a large factory blows the WHOLE tx-filter body past Portal's
 * 256KiB raw-query cap (MAX_RAW_QUERY_SIZE) INSIDE one TxRequest's from[]/to[] array — the over-cap
 * does NOT live across the `transactions: TxRequest[]` array like it does for logs. Historically this
 * hit the pre-existing "cannot be safely split — narrow the filter" hard stop (portal-client.ts:509).
 *
 * THE FIX (#196): `spec.txQueryShards()` does CHUNK-THEN-BINPACK — NOT a literal shardLogs mirror. A
 * naive bin-pack over the TxRequest[] array would leave a single huge-from TxRequest overflowing one
 * shard. So step 1 CHUNKS each over-cap TxRequest by splitting its DOMINANT (larger) address side —
 * from XOR to — into disjoint PORTAL_MAX_ADDRESSES batches, keeping the OTHER side whole (restoring the
 * "single element always fits" invariant); step 2 bin-packs the resulting TxRequest[] into byte-budgeted
 * shards, each body strictly < SHARD_BODY_BUDGET < MAX_RAW_QUERY_SIZE. Below the wall it is a NO-OP:
 * exactly one shard, byte-identical to the un-sharded `txQuery()`.
 *
 * COMPLETE + NON-DUP BY CONSTRUCTION: a tx matches a TxRequest iff `from ∈ req.from AND to ∈ req.to`
 * (AND). Chunking the dominant side into disjoint batches Fi (other side whole) ⇒ a tx's from lands in
 * exactly ONE Fi ⇒ it matches exactly one chunk; union over chunks == the original request. The client
 * re-matches every returned tx (txFilterMatched) so over-fetch is harmless, and assemble's seenTx
 * hash-set (portal-assemble.ts:515) drops any tx seen across shards → a spanning tx is stored ONCE.
 *
 * This suite pins:
 *   - (a) below-the-wall NO-OP: EXACTLY ONE shard, byte-identical to txQuery();
 *   - (b) huge-from TxRequest ⇒ chunk-then-binpack yields >1 shard, EVERY body < budget, from-subsets
 *         DISJOINT with union == the original from set, `to` kept WHOLE in each;
 *   - (c) a tx whose from lands in the LAST shard is NOT dropped across a shard boundary (completeness);
 *   - (d) a tx delivered by TWO shards is stored ONCE (drives assemble's seenTx dedup end-to-end);
 *   - (e) both-sides-huge ⇒ the `invariant('#196', …)` fail-loud fires (attributable throw, not a 400).
 */

import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { expect, test } from 'vitest';
import type { Address, Factory, TransactionFilter } from '@/internal/types.js';
import { createPortalHistoricalSync } from './portal.js';
import {
  type ChildAddresses,
  compileFetchSpec,
  MAX_RAW_QUERY_SIZE,
  PORTAL_MAX_ADDRESSES,
  type PortalQuery,
  SHARD_BODY_BUDGET,
} from './portal-filters.js';

// A canonical 20-byte lowercased hex address (42 chars → ~45 bytes as a JSON array element). Synthesize
// a distinct address per index so every child is unique. 256*1024 / 45 ≈ 5825 = the "~5.8k" wall.
const childAt = (i: number): Address =>
  `0x${i.toString(16).padStart(40, '0')}` as Address;

// The factory shape ponder's runtime hands the fetch-spec (isAddressFactory keys off `.id`). We only
// touch the fields portal-filters reads: id / address / eventSelector.
const factory = (id: string): Factory =>
  ({
    id,
    type: 'log',
    address: `0x${'f'.repeat(40)}`,
    eventSelector: `0x${'ab'.repeat(32)}`,
    childAddressLocation: 'topic1',
  }) as unknown as Factory;

// A transaction filter whose `fromAddress` is that factory — the historical txQuery expands it to the
// live child set (into TxRequest.from). `toAddress` is a plain single address kept whole in each chunk.
const KEEP_TO = `0x${'e'.repeat(40)}`;
const factoryFromTxFilter = (
  f: Factory,
  over: Partial<TransactionFilter> = {},
): TransactionFilter =>
  ({
    type: 'transaction',
    chainId: 1,
    sourceId: 'evault',
    fromAddress: f,
    toAddress: KEEP_TO,
    includeReverted: false,
    fromBlock: 0,
    toBlock: undefined,
    hasTransactionReceipt: true,
    include: [],
    ...over,
  }) as unknown as TransactionFilter;

// The mirror: a transaction filter whose `toAddress` is the factory (the chunked DOMINANT side is `to`)
// and whose `fromAddress` is a single plain address kept whole in each chunk. Exercises the
// `chunkFrom = fromLen >= toLen` FALSE branch (chunk `to`, keep `from` whole) that the from-dominant
// tests never hit on the green path.
const KEEP_FROM = `0x${'d'.repeat(40)}`;
const factoryToTxFilter = (
  f: Factory,
  over: Partial<TransactionFilter> = {},
): TransactionFilter =>
  ({
    type: 'transaction',
    chainId: 1,
    sourceId: 'evault',
    fromAddress: KEEP_FROM,
    toAddress: f,
    includeReverted: false,
    fromBlock: 0,
    toBlock: undefined,
    hasTransactionReceipt: true,
    include: [],
    ...over,
  }) as unknown as TransactionFilter;

// Build a childAddresses map with `n` distinct children under one factory id. Each child's value is its
// creation block; `createdAt` (default 1) sets it so a tx served at any block ≥ createdAt re-matches
// (assembly's factory floor). The completeness tests serve txs at blocks 10/20, so children exist by 1.
const childrenMap = (id: string, n: number, createdAt = 1): ChildAddresses => {
  const inner = new Map<Address, number>();
  for (let i = 1; i <= n; i++) {
    inner.set(childAt(i), createdAt);
  }

  return new Map([[id, inner]]);
};

// The historical body EXACTLY as portal-client.stream() serializes it: the query plus the block bounds.
const historicalBody = (query: PortalQuery, from: number, to: number): string =>
  JSON.stringify({ ...query, fromBlock: from, toBlock: to });

// ── (a) below-the-wall NO-OP: EXACTLY ONE shard, byte-identical to txQuery() ──────────────────────────

test('#196 no-op: an 872-child tx filter yields EXACTLY ONE shard byte-identical to txQuery()', () => {
  const f = factory('evault');
  // 872 ("F-full") is the largest validated factory to date — well under the wall; its body fits ONE
  // request. The sharding fix MUST NOT shard it: txQueryShards() yields exactly ONE shard whose
  // serialized body is byte-identical to the un-sharded txQuery() — the dominant safety guarantee for
  // the whole validated corpus.
  const spec = compileFetchSpec(
    [{ filter: factoryFromTxFilter(f) }],
    childrenMap('evault', 872),
  );

  const single = spec.txQuery();
  expect(single).toBeDefined();
  const smallBody = historicalBody(single!, 0, 1_000_000);
  expect(smallBody.length).toBeLessThan(MAX_RAW_QUERY_SIZE);

  const shards = spec.txQueryShards();
  expect(shards).toHaveLength(1);
  // BYTE-IDENTITY: the single shard serializes exactly as the un-sharded query — same object shape,
  // same order, same field projection. No drift is possible below the wall.
  expect(JSON.stringify(shards[0])).toBe(JSON.stringify(single));
  expect(historicalBody(shards[0]!, 0, 1_000_000)).toBe(smallBody);
});

test('#196: a >budget tx filter yields ≥2 shards, EACH body strictly < MAX_RAW_QUERY_SIZE', () => {
  const f = factory('evault');
  const N = 12_000; // > ~5825 comfortably; big enough to force >1 shard after chunking
  const spec = compileFetchSpec(
    [{ filter: factoryFromTxFilter(f) }],
    childrenMap('evault', N),
  );

  // The un-sharded single body overflows the cap — THIS is the wall #196 cures.
  const single = spec.txQuery()!;
  expect(historicalBody(single, 0, 1_000_000).length).toBeGreaterThan(
    MAX_RAW_QUERY_SIZE,
  );

  const shards = spec.txQueryShards();
  expect(shards.length).toBeGreaterThanOrEqual(2);

  // EVERY shard body must fit under the cap, with clear headroom (SHARD_BODY_BUDGET < cap). This is the
  // whole point: no shard can 400 server-side. Measured with the SAME envelope client.stream serializes.
  for (const shard of shards) {
    const body = historicalBody(shard, 0, 1_000_000);
    expect(body.length).toBeLessThan(SHARD_BODY_BUDGET);
    expect(body.length).toBeLessThan(MAX_RAW_QUERY_SIZE);
  }
});

// ── (b) chunk-then-binpack: disjoint from-partition, union == original, `to` kept whole ───────────────

// Collect all TxRequest elements across a shard plan.
const allTxRequests = (shards: PortalQuery[]) =>
  shards.flatMap((q) => q.transactions ?? []);

test('#196: huge-from chunk-then-binpack partitions `from` (disjoint, union == original) keeping `to` whole', () => {
  const f = factory('evault');
  const N = 12_000;
  const spec = compileFetchSpec(
    [{ filter: factoryFromTxFilter(f) }],
    childrenMap('evault', N),
  );

  const shards = spec.txQueryShards();
  expect(shards.length).toBeGreaterThanOrEqual(2);

  const reqs = allTxRequests(shards);
  // Every request keeps the OTHER side (`to`) WHOLE — the chunk step splits only the dominant `from`.
  for (const r of reqs) {
    expect(r.to).toEqual([KEEP_TO.toLowerCase()]);
    // each chunk's from is at most one PORTAL_MAX_ADDRESSES batch wide (the chunk width).
    expect((r.from ?? []).length).toBeLessThanOrEqual(PORTAL_MAX_ADDRESSES);
  }

  // DISJOINT + UNION == original: the from-subsets across every request partition the child set exactly.
  const seen = new Set<string>();
  let dupes = 0;
  for (const r of reqs)
    for (const a of r.from ?? []) {
      if (seen.has(a)) dupes++;

      seen.add(a);
    }
  expect(dupes).toBe(0); // disjoint on the chunked side (a true partition, not a cover)

  const expected = new Set(
    Array.from({ length: N }, (_, i) => childAt(i + 1).toLowerCase()),
  );
  expect(seen).toEqual(expected); // union == the original from set (complete)
  expect(seen.size).toBe(N);
});

test('#196: huge-`to` chunk-then-binpack partitions `to` (disjoint, union == original) keeping `from` whole', () => {
  // Mirror of the from-dominant test with the sides swapped: a plain single `fromAddress` (kept whole)
  // and a factory `toAddress` whose child set overflows. Drives the `chunkFrom = fromLen >= toLen` FALSE
  // branch (chunk `to`, keep `from` whole) — the ONLY green-path exercise of it; every other positive
  // sharding test is from-dominant, so this branch was previously reached only by the throw test.
  const f = factory('evault');
  const N = 12_000;
  const spec = compileFetchSpec(
    [{ filter: factoryToTxFilter(f) }],
    childrenMap('evault', N),
  );

  // The un-sharded single body overflows the cap — the wall #196 cures, on the `to` side this time.
  const single = spec.txQuery()!;
  expect(historicalBody(single, 0, 1_000_000).length).toBeGreaterThan(
    MAX_RAW_QUERY_SIZE,
  );

  const shards = spec.txQueryShards();
  expect(shards.length).toBeGreaterThanOrEqual(2);

  const reqs = allTxRequests(shards);
  // EVERY shard body strictly < budget (no shard can 400 server-side).
  for (const shard of shards) {
    const body = historicalBody(shard, 0, 1_000_000);
    expect(body.length).toBeLessThan(SHARD_BODY_BUDGET);
    expect(body.length).toBeLessThan(MAX_RAW_QUERY_SIZE);
  }

  // Every request keeps the OTHER side (`from`) WHOLE — the chunk step splits only the dominant `to`.
  for (const r of reqs) {
    expect(r.from).toEqual([KEEP_FROM.toLowerCase()]);
    // each chunk's to is at most one PORTAL_MAX_ADDRESSES batch wide (the chunk width).
    expect((r.to ?? []).length).toBeLessThanOrEqual(PORTAL_MAX_ADDRESSES);
  }

  // DISJOINT + UNION == original: the to-subsets across every request partition the child set exactly.
  const seen = new Set<string>();
  let dupes = 0;
  for (const r of reqs)
    for (const a of r.to ?? []) {
      if (seen.has(a)) dupes++;

      seen.add(a);
    }
  expect(dupes).toBe(0); // disjoint on the chunked side (a true partition, not a cover)

  const expected = new Set(
    Array.from({ length: N }, (_, i) => childAt(i + 1).toLowerCase()),
  );
  expect(seen).toEqual(expected); // union == the original to set (complete)
  expect(seen.size).toBe(N);
});

// ── (e) both-sides-huge ⇒ the #196 fail-loud invariant fires ──────────────────────────────────────────

test('#196 fail-loud: from AND to each over budget ⇒ chunking one leaves the other over-cap ⇒ invariant throws', () => {
  const f = factory('evaultFrom');
  const g = factory('evaultTo');
  // Two factories: one feeds `fromAddress`, the other `toAddress`, EACH large enough that keeping the
  // other whole overflows one request. Chunking the dominant side into ≤1000-addr batches still leaves
  // the OTHER whole side ≥ budget → no single-side chunk fits → the plan-build invariant('#196') fires
  // (attributable throw at plan build, NOT a Portal 400 at stream time). Mirrors shardLogs:296 and
  // portal-client.ts:509.
  const N = 8000; // each side ≈ 8000*45 ≈ 360KB whole, > SHARD_BODY_BUDGET even at a 1000-addr chunk
  const children: ChildAddresses = new Map([
    ...childrenMap('evaultFrom', N).entries(),
    ...childrenMap('evaultTo', N).entries(),
  ]);
  const filter = {
    type: 'transaction',
    chainId: 1,
    sourceId: 'evault',
    fromAddress: f,
    toAddress: g,
    includeReverted: false,
    fromBlock: 0,
    toBlock: undefined,
    hasTransactionReceipt: true,
    include: [],
  } as unknown as TransactionFilter;

  const spec = compileFetchSpec([{ filter }], children);

  // Sanity: the un-sharded body is well over the cap (both sides huge).
  expect(historicalBody(spec.txQuery()!, 0, 1_000_000).length).toBeGreaterThan(
    MAX_RAW_QUERY_SIZE,
  );

  // The chunk-then-binpack cannot reduce a both-sides-huge request below budget (a grid partition would
  // blow up shard count) → an attributable plan-build throw referencing #196 + SHARD_BODY_BUDGET.
  expect(() => spec.txQueryShards()).toThrow(/#196/);
  expect(() => spec.txQueryShards()).toThrow(/SHARD_BODY_BUDGET/);
});

// ── (c)/(d) end-to-end completeness gate (portal.ts level) ────────────────────────────────────────────

// A full Portal block carrying one transaction from `fromAddr` to KEEP_TO (so txFilterMatched passes).
const txBlock = (num: number, fromAddr: string, hashSeed: string) => ({
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
  transactions: [
    {
      transactionIndex: 0,
      hash: hashSeed,
      from: fromAddr,
      to: KEEP_TO.toLowerCase(),
      input: '0x',
      value: '0x0',
      nonce: 0,
      gas: '0x1',
      gasPrice: '0x1',
      type: 0,
      status: '0x1',
      cumulativeGasUsed: '0x1',
      effectiveGasPrice: '0x1',
    },
  ],
});

// Does this request's `transactions` spec carry `addr` in its from-set? (disjoint chunks ⇒ exactly one
// shard requests each child — this is what makes "streamed all shards?" observable at the store).
const requestsFrom = (transactions: any[], addr: string): boolean =>
  (transactions ?? []).some((r) =>
    (r.from ?? [])
      .map((x: string) => x.toLowerCase())
      .includes(addr.toLowerCase()),
  );

const LOW_BN = 10;
const HIGH_BN = 20;

// Drive one chunk over [LOW_BN, HIGH_BN] for a >budget tx filter against a mock Portal. The shard-0
// child (childAt(1)) emits a tx at LOW_BN; the LAST-shard child (childAt(n)) emits a tx at HIGH_BN.
// When `dupHash` is set BOTH shards' txs share ONE hash so we can prove seenTx dedup stores it once.
const runShardedTxChunk = async (opts: {
  n: number;
  dupHash?: boolean;
}): Promise<{
  insertedTxs: any[];
  requestedLow: number;
  requestedHigh: number;
}> => {
  const n = opts.n;
  const lowAddr = childAt(1); // shard 0
  const highAddr = childAt(n); // last shard

  const lowHash = `0x${'a'.repeat(64)}`;
  const highHash = opts.dupHash ? lowHash : `0x${'b'.repeat(64)}`;

  let requestedLow = 0;
  let requestedHigh = 0;

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
      const wantsLow = requestsFrom(q.transactions, lowAddr);
      const wantsHigh = requestsFrom(q.transactions, highAddr);
      if (wantsLow) requestedLow++;
      if (wantsHigh) requestedHigh++;

      const out: any[] = [];
      // For dupHash: the SAME tx (one hash) is served by BOTH shards — the shard-0 tx lists lowAddr as
      // its from, the last shard lists highAddr; but the hash is identical, so assemble's seenTx must
      // keep exactly one. We serve each shard its own child's tx at its own block.
      if (wantsLow && from <= LOW_BN && to >= LOW_BN)
        out.push(txBlock(LOW_BN, lowAddr.toLowerCase(), lowHash));
      if (wantsHigh && from <= HIGH_BN && to >= HIGH_BN)
        out.push(txBlock(HIGH_BN, highAddr.toLowerCase(), highHash));

      const end = Math.min(to, 1_000_000_000);
      const maxServed = out.reduce(
        (m, b) => Math.max(m, (b as any).header?.number ?? -1),
        -1,
      );
      const final =
        maxServed >= end
          ? out
          : [
              ...out,
              { header: txBlock(end, lowAddr.toLowerCase(), lowHash).header },
            ];
      res.writeHead(200, { 'content-type': 'application/x-ndjson' });
      res.end(`${final.map((b) => JSON.stringify(b)).join('\n')}\n`);
    });
  });

  const port: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const insertedTxs: any[] = [];
    const syncStore: any = {
      insertLogs: () => {},
      insertBlocks: () => {},
      insertTransactions: (x: any) => insertedTxs.push(...x.transactions),
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filter = factoryFromTxFilter(factory('evault')) as any;
    filter.toBlock = 499_999;
    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: {
        id: 1,
        name: 'mainnet',
        portal: `http://localhost:${port}`,
      } as any,
      childAddresses: childrenMap('evault', n, 1),
      eventCallbacks: [{ filter }],
    } as any);

    const interval: [number, number] = [LOW_BN, HIGH_BN];
    // syncBlockRangeData streams every shard into cd.txBlocks and STASHES the assembled (deduped) txs;
    // syncBlockData drains the stash into insertTransactions. Both are needed to observe the tx insert
    // (unlike logs, which insert in syncBlockRangeData directly).
    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [{ interval, filter }],
      requiredFactoryIntervals: [],
      syncStore,
    } as any);
    await sync.syncBlockData({ interval, logs, syncStore } as any);

    return { insertedTxs, requestedLow, requestedHigh };
  } finally {
    srv.close();
  }
};

test('#196 completeness gate: a >budget tx filter streams ALL shards — the LAST shard tx lands (no cross-boundary drop)', async () => {
  const { insertedTxs, requestedLow, requestedHigh } = await runShardedTxChunk({
    n: 12_000,
  });

  // The plan is >1 shard, and BOTH the shard-0 child and the LAST-shard child were requested…
  expect(requestedLow).toBeGreaterThanOrEqual(1);
  expect(requestedHigh).toBeGreaterThanOrEqual(1);

  // …and BOTH txs landed. If the chunk resolved after the first shard (the bug this fix must not
  // reintroduce), the last shard is never streamed and the high child's tx is silently lost.
  const froms = insertedTxs.map((t) => t.from?.toLowerCase());
  expect(froms).toContain(childAt(1).toLowerCase());
  expect(froms).toContain(childAt(12_000).toLowerCase());
  expect(insertedTxs).toHaveLength(2);
});

test('#196 dedup: a tx delivered by TWO shards is stored ONCE (assemble seenTx across shards)', async () => {
  // BOTH shards deliver a tx carrying the SAME hash (a tx whose from could match multiple shards, or a
  // belt-and-suspenders overlap). assemble's seenTx hash-set (portal-assemble.ts:515) must keep exactly
  // one row — the disjoint-partition property makes true over-fetch rare, but the dedup is the safety net.
  const { insertedTxs } = await runShardedTxChunk({
    n: 12_000,
    dupHash: true,
  });

  const hashes = insertedTxs.map((t) => t.hash);
  const uniqueHashes = new Set(hashes);
  // one hash, served by two shards → exactly ONE stored row (seenTx dedup), not two.
  expect(uniqueHashes.size).toBe(1);
  expect(insertedTxs).toHaveLength(1);
});

// Drive [SHARED_BN, SHARED_BN] against a mock Portal for a spec with TWO tx filters, each on its OWN large
// factory (so each filter's from[] overflows and chunk-then-binpacks into MANY shards independently), whose
// child sets both contain ONE shared address (childAt(1)) → a tx `childAt(1) → KEEP_TO` matches BOTH filters.
// Because each factory is 12k children, filter A's shard carrying childAt(1) and filter B's shard carrying
// childAt(1) are DISTINCT bodies in DISTINCT stream calls (they cannot bin-pack together — each is ~full), so
// the SAME tx (one hash) is delivered on the wire TWICE across two filters. This is the REALISTIC cross-
// request over-fetch (a real tx has ONE `from`, so one filter's DISJOINT chunks never both serve it — but two
// filters legitimately can), so assemble's seenTx hash-set must dedup it to exactly ONE stored row.
const SHARED_CHILD = childAt(1);
const SHARED_BN = 30;
// factory `from` filter with an explicit sourceId (two distinct filters must differ so txRequestsFor keeps
// both requests rather than collapsing identical ones).
const factoryFromTxFilterId = (
  f: Factory,
  sourceId: string,
): TransactionFilter =>
  factoryFromTxFilter(f, { sourceId } as Partial<TransactionFilter>);
// childAddresses map with the SAME shared child (childAt(1)) present under TWO factory ids, plus n-1 filler
// children each so both requests overflow and shard.
const twoFactoriesSharingChild = (
  idA: string,
  idB: string,
  n: number,
): ChildAddresses => {
  const a = childrenMap(idA, n).get(idA)!;
  const b = childrenMap(idB, n).get(idB)!;

  return new Map([
    [idA, a],
    [idB, b],
  ]);
};
const runTwoFactoriesSharingChild = async (): Promise<{
  insertedTxs: any[];
  sharedServes: number;
}> => {
  const hash = `0x${'f'.repeat(64)}`;
  let sharedServes = 0;

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
      const wantsShared = requestsFrom(q.transactions, SHARED_CHILD);
      if (wantsShared) sharedServes++;

      const out: any[] = [];
      if (wantsShared && from <= SHARED_BN && to >= SHARED_BN)
        out.push(txBlock(SHARED_BN, SHARED_CHILD.toLowerCase(), hash));

      const end = Math.min(to, 1_000_000_000);
      const maxServed = out.reduce(
        (m, b) => Math.max(m, (b as any).header?.number ?? -1),
        -1,
      );
      const final =
        maxServed >= end
          ? out
          : [
              ...out,
              { header: txBlock(end, SHARED_CHILD.toLowerCase(), hash).header },
            ];
      res.writeHead(200, { 'content-type': 'application/x-ndjson' });
      res.end(`${final.map((b) => JSON.stringify(b)).join('\n')}\n`);
    });
  });

  const port: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const insertedTxs: any[] = [];
    const syncStore: any = {
      insertLogs: () => {},
      insertBlocks: () => {},
      insertTransactions: (x: any) => insertedTxs.push(...x.transactions),
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filterA = factoryFromTxFilterId(factory('evaultA'), 'srcA');
    const filterB = factoryFromTxFilterId(factory('evaultB'), 'srcB');
    (filterA as any).toBlock = 499_999;
    (filterB as any).toBlock = 499_999;

    const sync = createPortalHistoricalSync({
      common: {
        logger: { debug() {}, info() {}, warn() {}, error() {}, trace() {} },
      } as any,
      chain: {
        id: 1,
        name: 'mainnet',
        portal: `http://localhost:${port}`,
      } as any,
      childAddresses: twoFactoriesSharingChild('evaultA', 'evaultB', 12_000),
      eventCallbacks: [{ filter: filterA }, { filter: filterB }],
    } as any);

    const interval: [number, number] = [SHARED_BN, SHARED_BN];
    const logs = await sync.syncBlockRangeData({
      interval,
      requiredIntervals: [
        { interval, filter: filterA },
        { interval, filter: filterB },
      ],
      requiredFactoryIntervals: [],
      syncStore,
    } as any);
    await sync.syncBlockData({ interval, logs, syncStore } as any);

    return { insertedTxs, sharedServes };
  } finally {
    srv.close();
  }
};

test('#196 cross-request dedup: TWO factory filters sharing a child match the SAME tx ⇒ stored ONCE', async () => {
  const { insertedTxs, sharedServes } = await runTwoFactoriesSharingChild();

  // Sanity: the shared child was requested by MORE THAN ONE distinct shard body (filter A's shard AND filter
  // B's shard), so the SAME tx really is delivered on the wire TWICE — the REAL cross-request over-fetch the
  // dedup exists for, not the artificial one-filter-two-shards overlap (one filter's disjoint chunks can
  // never both serve one tx).
  expect(sharedServes).toBeGreaterThanOrEqual(2);

  // …yet the tx is stored exactly ONCE. Two INDEPENDENT dedup layers guard this (defense-in-depth, so
  // neutering EITHER alone is masked by the other — the mutation-check neuters BOTH): assemble's per-range
  // seenTx hash-set (portal-assemble.ts, tx-filter branch) drops a tx already emitted this assemble call, and
  // syncBlockData's final Map<hash, tx> (portal.ts) collapses any duplicate hash before insertTransactions.
  const hashes = insertedTxs.map((t) => t.hash);
  expect(new Set(hashes).size).toBe(1);
  expect(insertedTxs).toHaveLength(1);
});
