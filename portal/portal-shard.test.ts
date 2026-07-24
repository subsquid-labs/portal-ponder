/**
 * portal-shard.test.ts — issue #194 FAIL→PASS anchor + the sharding fix's regression suite.
 *
 * THE WALL. A factory whose discovered child set is large enough (~5.8k children) blows the WHOLE
 * child list past Portal's 256KiB raw-query cap (MAX_RAW_QUERY_SIZE) in ONE request body:
 * `logRequestsFor` / `logQuery()` batch the addresses into several `PortalLogRequest` entries
 * (PORTAL_MAX_ADDRESSES per entry) but ALL of them land in the SAME `logs: PortalLogRequest[]` array,
 * so batching+merge do NOT bound the TOTAL body — it grows linearly with the child count. Historically
 * this was a permanent hard stop (`portal-client.fetchBatch` size-guards `body.length`; no
 * block-bisection escape — the cap is on BYTES and the address list is range-independent).
 *
 * THE FIX (SHARD-DESIGN.md §3): `spec.logQueryShards()` partitions that address union into a
 * byte-budgeted set of full PortalQuery shards, each body strictly < SHARD_BODY_BUDGET <
 * MAX_RAW_QUERY_SIZE, each an IDENTICAL-fields/topics query over a DISJOINT address subset. portal.ts
 * streams them sequentially into ONE chunk and UNIONs the rows. Below the wall it is a NO-OP: exactly
 * one shard, byte-identical to the un-sharded `logQuery()`.
 *
 * This suite pins:
 *   - the wall on the pre-shard single-body builder (`logQuery()` still overflows for 6000 children),
 *   - the byte-identity no-op control (872 children ⇒ ONE shard === logQuery()),
 *   - shard count & per-shard budget (6000 children ⇒ ≥2 shards, each body < cap),
 *   - union completeness / partition property (INV-11 extension; mutation: drop a shard ⇒ fails),
 *   - the portal.ts completeness gate (INV-1/INV-3; ALL shards drain before the chunk resolves — a
 *     >budget factory's last-shard rows MUST land in the store, and a mid-plan shard throw rejects the
 *     chunk with no partial cache-set and re-streams every shard on retry).
 */

import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { expect, test } from 'vitest';
import type { Address, Factory, LogFilter } from '@/internal/types.js';
import { createPortalHistoricalSync } from './portal.js';
import { createPortalClient } from './portal-client.js';
import {
  type ChildAddresses,
  compileFetchSpec,
  MAX_RAW_QUERY_SIZE,
  PORTAL_MAX_ADDRESSES,
  type PortalQuery,
  SHARD_BODY_BUDGET,
} from './portal-filters.js';
import type { Gate } from './portal-gate.js';
import { createStats } from './portal-metrics.js';

// The full Gate surface (mirrors portal-client.test.ts's fakeGate) — the size guard fires long before
// any gate method is reached, but the client's ctor still needs a well-typed gate.
const fakeGate: Gate = {
  acquire: async () => {},
  release() {},
  onOk() {},
  onThrottle() {},
  addRows() {},
  freeRows() {},
  saturated: () => false,
  snapshot: () => ({ limit: 0, active: 0, rows: 0 }),
};

// A canonical 20-byte lowercased hex address, rendered `"0x"+40 hex` = 42 chars → ~45 bytes in a JSON
// array element (quotes + comma). 256*1024 / 45 ≈ 5825, i.e. the "~5.8k children" wall from the field
// report. We synthesize an address per child from its index so every child is distinct.
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

// A log filter whose `address` is that factory — the historical logQuery expands it to the live
// child set. topic0 present so it exercises the merge path too.
const factoryLogFilter = (f: Factory): LogFilter =>
  ({
    type: 'log',
    chainId: 1,
    sourceId: 'evault',
    address: f,
    topic0: `0x${'cd'.repeat(32)}` as unknown as LogFilter['topic0'],
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: 0,
    toBlock: undefined,
    hasTransactionReceipt: false,
    include: [],
  }) as unknown as LogFilter;

// Build a childAddresses map with `n` distinct children under one factory id. Each child's value is its
// creation block = its index (distinct, harmless for the pure builders — they read address KEYS only).
const childrenMap = (id: string, n: number): ChildAddresses => {
  const inner = new Map<Address, number>();
  for (let i = 1; i <= n; i++) {
    inner.set(childAt(i), i);
  }

  return new Map([[id, inner]]);
};

// Same shape, but ALL children created at `createdAt` — so a log served at any block ≥ createdAt
// re-matches (assembly's `isAddressMatched` enforces the child's creation floor). The portal.ts
// completeness test serves logs at blocks 10/20, so every child must exist by then.
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

// The historical body EXACTLY as portal-client.stream() serializes it: the query plus the block bounds.
const historicalBody = (query: PortalQuery, from: number, to: number): string =>
  JSON.stringify({ ...query, fromBlock: from, toBlock: to });

// The complete set of (address, topic0) match pairs a log-query plan carries — the thing the union
// over shards must preserve exactly. Order-independent (a Set of `address|topic0` keys).
const matchSet = (queries: PortalQuery[]): Set<string> => {
  const set = new Set<string>();
  for (const q of queries)
    for (const r of q.logs ?? [])
      for (const a of r.address ?? [''])
        for (const t of r.topic0 ?? [''])
          set.add(`${a.toLowerCase()}|${t.toLowerCase()}`);

  return set;
};

// ── the wall: total body of the (pre-shard) single-body builder grows linearly with children ────────

test('#194: a >5.8k-child factory blows the pre-shard logQuery body past MAX_RAW_QUERY_SIZE', () => {
  const f = factory('evault');
  const N = 6000; // > ~5825, comfortably over the cap
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }],
    childrenMap('evault', N),
  );
  const query = spec.logQuery();
  expect(query).toBeDefined();

  // ALL children land in ONE body — the batches are separate array entries in the SAME `logs`.
  const totalAddrs = (query!.logs ?? []).reduce(
    (s, r) => s + (r.address?.length ?? 0),
    0,
  );
  expect(totalAddrs).toBe(N);
  // batching splits into ceil(N / PORTAL_MAX_ADDRESSES) entries but keeps them in one array…
  expect(query!.logs!.length).toBe(Math.ceil(N / PORTAL_MAX_ADDRESSES));

  const body = historicalBody(query!, 0, 1_000_000);
  // …so the single un-sharded body still overflows the cap. THIS is the wall (#194) the shard fix cures.
  expect(body.length).toBeGreaterThan(MAX_RAW_QUERY_SIZE);
});

// ── byte-identity no-op control (CRITICAL, §3.5.1): the fix is a no-op below the wall ────────────────

test('#194 no-op: an 872-child factory yields EXACTLY ONE shard byte-identical to logQuery()', () => {
  const f = factory('evault');
  // The largest factory case validated to date is 872 ("F-full") — well under the wall; its body must
  // fit ONE request. The sharding fix MUST NOT shard it: logQueryShards() yields exactly ONE shard whose
  // serialized body is byte-identical to the un-sharded logQuery() body — the dominant safety guarantee
  // for the entire existing validated corpus.
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }],
    childrenMap('evault', 872),
  );

  const single = spec.logQuery();
  expect(single).toBeDefined();
  const smallBody = historicalBody(single!, 0, 1_000_000);
  expect(smallBody.length).toBeLessThan(MAX_RAW_QUERY_SIZE);

  const shards = spec.logQueryShards();
  expect(shards).toHaveLength(1);
  // BYTE-IDENTITY: the single shard serializes exactly as the un-sharded query — same object shape,
  // same order, same field projection. No drift is possible below the wall.
  expect(JSON.stringify(shards[0])).toBe(JSON.stringify(single));
  expect(historicalBody(shards[0]!, 0, 1_000_000)).toBe(smallBody);
});

// ── shard count & per-shard budget (§3.5.2) ─────────────────────────────────────────────────────────

test('#194: a >budget factory yields ≥2 shards, EACH body strictly < MAX_RAW_QUERY_SIZE', () => {
  const f = factory('evault');
  const N = 6000;
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }],
    childrenMap('evault', N),
  );

  const shards = spec.logQueryShards();
  expect(shards.length).toBeGreaterThanOrEqual(2);

  // EVERY shard body must fit under the cap — with clear headroom (SHARD_BODY_BUDGET < cap). This is the
  // whole point: no shard can 400 server-side. Measured with the SAME envelope client.stream serializes.
  for (const shard of shards) {
    const body = historicalBody(shard, 0, 1_000_000);
    expect(body.length).toBeLessThan(SHARD_BODY_BUDGET);
    expect(body.length).toBeLessThan(MAX_RAW_QUERY_SIZE);
  }

  // multiplicity is the BYTE budget, not the 1000-count batch multiplicity — a shard packs many batches.
  expect(shards.length).toBeLessThan(Math.ceil(N / PORTAL_MAX_ADDRESSES));

  // every child is transported exactly once across the plan (no dupes, none dropped).
  const total = shards.reduce(
    (s, q) =>
      s + (q.logs ?? []).reduce((a, r) => a + (r.address?.length ?? 0), 0),
    0,
  );
  expect(total).toBe(N);
});

// ── union completeness / partition property (§3.5.3, INV-11 extension) ───────────────────────────────

test('#194: the shard plan is a PARTITION — its union matches exactly the un-sharded merged set', () => {
  const f = factory('evault');
  // Exercise sizes straddling the wall AND several shard counts.
  for (const N of [1, 872, 5825, 6000, 12_345]) {
    const spec = compileFetchSpec(
      [{ filter: factoryLogFilter(f) }],
      childrenMap('evault', N),
    );

    const shards = spec.logQueryShards();
    const single = spec.logQuery();

    // union over the shard plan == the un-sharded merged match set (partition ⇒ same logs; INV-11).
    const union = matchSet(shards);
    const whole = matchSet(single ? [single] : []);
    expect(union).toEqual(whole);
    expect(union.size).toBe(N);

    // DISJOINT: the address subsets across shards never overlap (a true partition, not a cover).
    const seen = new Set<string>();
    let dupes = 0;
    for (const q of shards)
      for (const r of q.logs ?? [])
        for (const a of r.address ?? []) {
          if (seen.has(a)) dupes++;

          seen.add(a);
        }
    expect(dupes).toBe(0);
  }
});

test('#194 mutation-guard: DROPPING a shard breaks the partition (the completeness property has teeth)', () => {
  const f = factory('evault');
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }],
    childrenMap('evault', 6000),
  );

  const shards = spec.logQueryShards();
  expect(shards.length).toBeGreaterThanOrEqual(2);

  const whole = matchSet(spec.logQuery() ? [spec.logQuery()!] : []);
  const dropped = matchSet(shards.slice(0, -1)); // drop the LAST shard
  // The union of a strict subset of shards is strictly smaller than the whole — proving the partition
  // property above would FAIL if any shard's rows were silently discarded.
  expect(dropped).not.toEqual(whole);
  expect(dropped.size).toBeLessThan(whole.size);
});

// ── the pre-shard fail-loud still fires on a raw single-body query (the hard stop this fix removes) ───

test('#194: portal-client still fails loud on a single oversized body (the pre-shard hard stop)', async () => {
  const f = factory('evault');
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }],
    childrenMap('evault', 6000),
  );
  // The pre-shard single-body query (logQuery, NOT a shard) — still over the cap by construction.
  const query = spec.logQuery()!;

  // A fetchImpl that must NEVER be reached — the proactive size guard throws BEFORE any POST.
  let posted = false;
  const client = createPortalClient({
    portalUrl: 'http://portal.invalid',
    headers: {},
    gate: fakeGate,
    stats: createStats(),
    bufferSize: 10,
    chainName: 'ethereum',
    sleepImpl: async () => {},
    fetchImpl: (async () => {
      posted = true;
      throw new Error(
        'fetch must not be reached — the size guard should fire first',
      );
    }) as unknown as typeof fetch,
  });

  const drain = async (): Promise<void> => {
    for await (const _ of client.stream(query, 0, 1_000_000)) {
      // no batches expected — the guard throws before the first fetch
    }
  };

  await expect(drain()).rejects.toThrow(/MAX_RAW_QUERY_SIZE|too large/i);
  expect(posted).toBe(false);
});

// ── the completeness gate (§3.5.4, INV-1/INV-3 silent-loss guard) — portal.ts level ─────────────────

// A full Portal block carrying one child's log + its parent transaction, addressed to `addr`.
const shardBlock = (num: number, addr: string, topic0: string) => ({
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
      topics: [topic0],
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

// Does this request's `logs` spec carry `addr`? (per-shard address subset ⇒ only ONE shard requests
// each child — this is what makes "streamed all shards?" observable at the store).
const requestsAddress = (logs: any[], addr: string): boolean =>
  (logs ?? []).some((r) =>
    (r.address ?? [])
      .map((x: string) => x.toLowerCase())
      .includes(addr.toLowerCase()),
  );

const TOPIC0 = `0x${'cd'.repeat(32)}`;

// Two children on opposite ends of the child map: `lowChild` lands in shard 0, `highChild` (the very
// last child) lands in the LAST shard. If the chunk resolves after the FIRST shard, highChild's block
// is never requested → its log is lost.
const LOW_BN = 10;
const HIGH_BN = 20;

// Build the sync + mock Portal for a >budget factory and drive one chunk over [LOW_BN, HIGH_BN].
// Returns the inserted logs and, when `failShardOfHigh`, throws a 500 on the shard that requests the
// high child so we can assert the chunk rejects (no partial cache) and retries the full plan.
const runShardedChunk = async (opts: {
  n: number;
  failHighOnce?: boolean;
}): Promise<{
  insertedLogs: any[];
  requestedLow: number;
  requestedHigh: number;
}> => {
  const n = opts.n;
  const lowAddr = childAt(1); // shard 0
  const highAddr = childAt(n); // last shard

  let requestedLow = 0;
  let requestedHigh = 0;
  let highFailsLeft = opts.failHighOnce ? 1 : 0;

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
      const wantsLow = requestsAddress(q.logs, lowAddr);
      const wantsHigh = requestsAddress(q.logs, highAddr);
      if (wantsLow) requestedLow++;
      if (wantsHigh) requestedHigh++;

      if (wantsHigh && highFailsLeft > 0) {
        highFailsLeft--;
        // A DETERMINISTIC 400 (not a typed/degradable/transient variant) → PortalHttpError, which
        // propagates straight out of stream → runStreams → rejects the chunk promise (no internal
        // retry storm). This models "a shard failed mid-plan": the WHOLE chunk must reject.
        res.writeHead(400, { 'content-type': 'text/plain' });
        res.end('injected deterministic shard failure');
        return;
      }

      const out: any[] = [];
      if (wantsLow && from <= LOW_BN && to >= LOW_BN)
        out.push(shardBlock(LOW_BN, lowAddr, TOPIC0));
      if (wantsHigh && from <= HIGH_BN && to >= HIGH_BN)
        out.push(shardBlock(HIGH_BN, highAddr, TOPIC0));

      // in-range window (head 1e9 ≫ chunk): terminate at the range-end anchor, never a mid-range 204.
      const end = Math.min(to, 1_000_000_000);
      const maxServed = out.reduce(
        (m, b) => Math.max(m, (b as any).header?.number ?? -1),
        -1,
      );
      const final =
        maxServed >= end
          ? out
          : [...out, { header: shardBlock(end, lowAddr, TOPIC0).header }];
      res.writeHead(200, { 'content-type': 'application/x-ndjson' });
      res.end(`${final.map((b) => JSON.stringify(b)).join('\n')}\n`);
    });
  });

  const port: number = await new Promise((r) =>
    srv.listen(0, () => r((srv.address() as AddressInfo).port)),
  );
  try {
    const insertedLogs: any[] = [];
    const syncStore: any = {
      insertLogs: (x: any) => insertedLogs.push(...x.logs),
      insertBlocks: () => {},
      insertTransactions: () => {},
      insertTransactionReceipts: () => {},
      insertTraces: () => {},
    };
    const filter = factoryLogFilter(factory('evault')) as any;
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
      childAddresses: childrenMapCreatedAt('evault', n, 1),
      eventCallbacks: [{ filter }],
    } as any);

    const interval: [number, number] = [LOW_BN, HIGH_BN];
    const call = () =>
      sync.syncBlockRangeData({
        interval,
        requiredIntervals: [{ interval, filter }],
        requiredFactoryIntervals: [],
        syncStore,
      } as any);

    if (opts.failHighOnce) {
      // the shard carrying the high child 500s on the FIRST attempt → the WHOLE chunk must reject
      // (no partial cache-set). The retry re-streams EVERY shard fresh → both logs land.
      await expect(call()).rejects.toThrow();
      await call();
    } else {
      await call();
    }

    return { insertedLogs, requestedLow, requestedHigh };
  } finally {
    srv.close();
  }
};

test('#194 completeness gate: a >budget factory streams ALL shards — the LAST shard rows land in the store', async () => {
  const { insertedLogs, requestedLow, requestedHigh } = await runShardedChunk({
    n: 6000,
  });

  // The plan is >1 shard, and BOTH the shard-0 child and the LAST-shard child were requested…
  expect(requestedLow).toBeGreaterThanOrEqual(1);
  expect(requestedHigh).toBeGreaterThanOrEqual(1);

  // …and BOTH children's logs landed. If the chunk resolved after the first shard (the bug this fix
  // must not reintroduce), the last shard is never streamed and the high child's log is silently lost.
  const addrs = insertedLogs.map((l) => l.address?.toLowerCase());
  expect(addrs).toContain(childAt(1).toLowerCase());
  expect(addrs).toContain(childAt(6000).toLowerCase());
  expect(insertedLogs).toHaveLength(2);
});

test('#194 completeness gate: a shard throw mid-plan REJECTS the chunk (no partial cache) and a retry re-streams every shard', async () => {
  const { insertedLogs, requestedHigh } = await runShardedChunk({
    n: 6000,
    failHighOnce: true,
  });

  // The high-child shard was requested more than once: it 500'd first (rejecting the whole chunk), then
  // the retry re-streamed the ENTIRE plan — so the high child was re-requested and its log recovered.
  expect(requestedHigh).toBeGreaterThanOrEqual(2);

  const addrs = insertedLogs.map((l) => l.address?.toLowerCase());
  expect(addrs).toContain(childAt(1).toLowerCase());
  expect(addrs).toContain(childAt(6000).toLowerCase());
  // exactly the two children — no partial-commit duplicated the low child from a half-cached chunk.
  expect(insertedLogs).toHaveLength(2);
});
