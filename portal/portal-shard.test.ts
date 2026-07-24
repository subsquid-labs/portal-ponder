/**
 * portal-shard.test.ts — issue #194 FAIL→PASS anchor.
 *
 * A factory whose discovered child set is large enough to blow past Portal's 256KiB raw-query cap
 * (MAX_RAW_QUERY_SIZE) is the wall this suite pins. Today the WHOLE child list ships in ONE request
 * body: `logRequestsFor` / `compileFetchSpec().logQuery()` batch the addresses into several
 * `PortalLogRequest` entries (PORTAL_MAX_ADDRESSES per entry) but ALL of them go into the SAME
 * `logs: PortalLogRequest[]` array — so batching + merge do NOT reduce the TOTAL body size; it grows
 * linearly with the child count. `portal-client.fetchBatch` size-guards `body.length` and fails loud
 * once the body exceeds the cap → a permanent hard stop (there is no block-bisection escape: the cap
 * is on BYTES, and the address list is range-independent).
 *
 * This file is the pure/unit repro at the portal-filters ↔ portal-client seam (no network). It asserts
 * the CURRENT behaviour: the assembled body EXCEEDS the cap and the client's fail-loud FIRES. When the
 * sharding fix lands, the "body exceeds the cap" expectations flip (a sharded plan keeps every request
 * body under the cap) — see portal/SHARD-DESIGN.md. Until then these tests DOCUMENT the wall.
 */

import { expect, test } from 'vitest';
import type { Address, Factory, LogFilter } from '@/internal/types.js';
import { createPortalClient } from './portal-client.js';
import {
  type ChildAddresses,
  compileFetchSpec,
  MAX_RAW_QUERY_SIZE,
  PORTAL_MAX_ADDRESSES,
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
  (`0x${i.toString(16).padStart(40, '0')}`) as Address;

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

// Build a childAddresses map with `n` distinct children under one factory id.
const childrenMap = (id: string, n: number): ChildAddresses => {
  const inner = new Map<Address, number>();
  for (let i = 1; i <= n; i++) {
    inner.set(childAt(i), i);
  }

  return new Map([[id, inner]]);
};

// The historical body EXACTLY as portal-client.stream() serializes it: the query plus the block bounds.
const historicalBody = (
  query: ReturnType<ReturnType<typeof compileFetchSpec>['logQuery']>,
  from: number,
  to: number,
): string => JSON.stringify({ ...query, fromBlock: from, toBlock: to });

// ── the wall: total body grows linearly with children; batching does NOT bound it ──────────────────

test('#194: a >5.8k-child factory blows the historical logQuery body past MAX_RAW_QUERY_SIZE', () => {
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

  const body = historicalBody(query, 0, 1_000_000);
  // …so the TOTAL body still overflows the cap. THIS is the wall (#194).
  expect(body.length).toBeGreaterThan(MAX_RAW_QUERY_SIZE);
});

test('#194: the wall scales with the child count, not the batch count (a <5.8k factory fits)', () => {
  const f = factory('evault');
  // A factory well under the wall (the largest case validated to date is 872 — "F-full"); its body
  // must fit ONE request. The sharding fix must be a NO-OP here (see SHARD-DESIGN.md byte-identity
  // control) — pinned so a future change can't regress the fits-one-body case.
  const specSmall = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }],
    childrenMap('evault', 872),
  );
  const smallBody = historicalBody(specSmall.logQuery(), 0, 1_000_000);
  expect(smallBody.length).toBeLessThan(MAX_RAW_QUERY_SIZE);

  const specBig = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }],
    childrenMap('evault', 6000),
  );
  const bigBody = historicalBody(specBig.logQuery(), 0, 1_000_000);
  expect(bigBody.length).toBeGreaterThan(MAX_RAW_QUERY_SIZE);
});

// ── the fail-loud: the client's size guard fires on the oversized body (permanent hard stop) ────────

test('#194: portal-client fails loud on the oversized factory body (the current hard stop)', async () => {
  const f = factory('evault');
  const spec = compileFetchSpec(
    [{ filter: factoryLogFilter(f) }],
    childrenMap('evault', 6000),
  );
  const query = spec.logQuery()!;

  // A fetchImpl that must NEVER be reached — the proactive size guard throws BEFORE any POST. If the
  // guard ever stops firing (e.g. a fix lands) this fetch would run; we assert it does not.
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

  // Draining the stream must throw the loud size-guard error (permanent — a range bisection can't help).
  const drain = async (): Promise<void> => {
    for await (const _ of client.stream(query, 0, 1_000_000)) {
      // no batches expected — the guard throws before the first fetch
    }
  };

  await expect(drain()).rejects.toThrow(/MAX_RAW_QUERY_SIZE|too large/i);
  expect(posted).toBe(false);
});
