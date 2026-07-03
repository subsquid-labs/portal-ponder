import fc from 'fast-check';
import { expect, test } from 'vitest';
import type { Address, LogFilter } from '@/internal/types.js';
import {
  type ChildAddresses,
  compileFetchSpec,
  logRequestsFor,
  mergeLogRequests,
  PORTAL_MAX_ADDRESSES,
  type PortalLogRequest,
} from './portal-filters.js';

fc.configureGlobal({ seed: 1337 }); // deterministic CI

const logFilter = (over: Partial<LogFilter> = {}): LogFilter =>
  ({
    type: 'log',
    chainId: 1,
    sourceId: 's',
    address: undefined,
    topic0: '0x00' as any,
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: undefined,
    toBlock: undefined,
    hasTransactionReceipt: false,
    include: [],
    ...over,
  }) as LogFilter;

// ── INV-11: merge equivalence vs a model log-matcher ──────────────────────────────────────────────

// The model covers ALL of address + topic0..topic3 so a merge key that dropped any topic dimension
// (e.g. a mutant key omitting topic2/topic3) changes the match-set and fails the property.
type ModelLog = {
  address: string;
  topic0: string;
  topic1: string;
  topic2: string;
  topic3: string;
};
const matchesReq = (r: PortalLogRequest, l: ModelLog): boolean =>
  (!r.address || r.address.includes(l.address)) &&
  (!r.topic0 || r.topic0.includes(l.topic0)) &&
  (!r.topic1 || r.topic1.includes(l.topic1)) &&
  (!r.topic2 || r.topic2.includes(l.topic2)) &&
  (!r.topic3 || r.topic3.includes(l.topic3));
const matchesAny = (reqs: PortalLogRequest[], l: ModelLog): boolean =>
  reqs.some((r) => matchesReq(r, l));

const addrPool = ['0xa', '0xb', '0xc'];
const t0Pool = ['0x1', '0x2', '0x3'];
const t1Pool = ['0xp', '0xq'];
const t2Pool = ['0xs', '0xt'];
const t3Pool = ['0xu', '0xv'];

const arbReq: fc.Arbitrary<PortalLogRequest> = fc.record({
  address: fc.option(
    fc.subarray(addrPool, { minLength: 1 }).map((a) => [...a].sort()),
    { nil: undefined },
  ),
  topic0: fc.option(fc.subarray(t0Pool, { minLength: 1 }), { nil: undefined }),
  topic1: fc.option(
    fc.constantFrom(...t1Pool).map((t) => [t]),
    { nil: undefined },
  ),
  topic2: fc.option(
    fc.constantFrom(...t2Pool).map((t) => [t]),
    { nil: undefined },
  ),
  topic3: fc.option(
    fc.constantFrom(...t3Pool).map((t) => [t]),
    { nil: undefined },
  ),
});
const arbLog: fc.Arbitrary<ModelLog> = fc.record({
  address: fc.constantFrom(...addrPool),
  topic0: fc.constantFrom(...t0Pool),
  topic1: fc.constantFrom(...t1Pool),
  topic2: fc.constantFrom(...t2Pool),
  topic3: fc.constantFrom(...t3Pool),
});

test('INV-11: mergeLogRequests matches exactly the same logs as the originals', () => {
  fc.assert(
    fc.property(
      fc.array(arbReq, { maxLength: 8 }),
      fc.array(arbLog, { maxLength: 30 }),
      (reqs, logs) => {
        const merged = mergeLogRequests(reqs);
        for (const l of logs)
          expect(matchesAny(merged, l)).toBe(matchesAny(reqs, l));
      },
    ),
  );
});

test('INV-11: requests differing ONLY in topic2/topic3 are NOT merged (key includes every topic)', () => {
  // two requests sharing address+topic0 but with different topic2 — a merged (topic0-unioned) request
  // would wrongly match the cross product
  const merged23 = mergeLogRequests([
    { address: ['0xa'], topic0: ['0x1'], topic2: ['0xs'] },
    { address: ['0xa'], topic0: ['0x2'], topic2: ['0xt'] },
  ]);
  expect(merged23).toHaveLength(2);
  const merged3 = mergeLogRequests([
    { address: ['0xa'], topic0: ['0x1'], topic3: ['0xu'] },
    { address: ['0xa'], topic0: ['0x2'], topic3: ['0xv'] },
  ]);
  expect(merged3).toHaveLength(2);
});

test('INV-11: N same-address event filters collapse to ONE request, topic0 unioned', () => {
  const ADDR = '0x' + 'cc'.repeat(20);
  const reqs = Array.from({ length: 6 }, (_, i) => ({
    address: [ADDR],
    topic0: ['0x' + (i + 1).toString(16)],
  }));
  const merged = mergeLogRequests(reqs);
  expect(merged).toHaveLength(1);
  expect(new Set(merged[0]!.topic0)).toEqual(
    new Set(reqs.map((r) => r.topic0![0])),
  );
});

test('INV-11: an undefined topic0 absorbs all (kept broadest)', () => {
  const merged = mergeLogRequests([
    { address: ['0xa'], topic0: ['0x1'] },
    { address: ['0xa'] },
  ]);
  expect(merged).toHaveLength(1);
  expect(merged[0]!.topic0).toBeUndefined();
});

// ── batching + factory expansion ──────────────────────────────────────────────────────────────────

test('logRequestsFor: plain-address batches by PORTAL_MAX_ADDRESSES', () => {
  const addrs = Array.from(
    { length: PORTAL_MAX_ADDRESSES * 2 + 5 },
    (_, i) => ('0x' + i.toString(16).padStart(40, '0')) as Address,
  );
  const reqs = logRequestsFor(logFilter({ address: addrs }), new Map());
  expect(reqs).toHaveLength(3);
  expect(reqs[0]!.address).toHaveLength(PORTAL_MAX_ADDRESSES);
  expect(reqs[2]!.address).toHaveLength(5);
});

test('logRequestsFor: factory filter with zero children → [] (never match-all)', () => {
  const factory: any = {
    id: 'f',
    type: 'log',
    address: '0xF',
    eventSelector: '0xsel',
    childAddressLocation: 'topic1',
  };
  const reqs = logRequestsFor(logFilter({ address: factory }), new Map());
  expect(reqs).toEqual([]);
});

test('logRequestsFor: factory filter expands to known children', () => {
  const factory: any = {
    id: 'f',
    type: 'log',
    address: '0xF',
    eventSelector: '0xsel',
    childAddressLocation: 'topic1',
  };
  const childAddresses: ChildAddresses = new Map([
    [
      'f',
      new Map([
        ['0xchild1' as Address, 10],
        ['0xchild2' as Address, 20],
      ]),
    ],
  ]);
  const reqs = logRequestsFor(logFilter({ address: factory }), childAddresses);
  expect(reqs[0]!.address!.sort()).toEqual(['0xchild1', '0xchild2']);
});

// ── compile determinism + freeze (INV-1 support) ───────────────────────────────────────────────────

test('INV-1 support: compileFetchSpec is deterministic in structure and frozen', () => {
  const cbs = [{ filter: logFilter({ topic0: '0xdead' as any }) }];
  const a = compileFetchSpec(cbs, new Map());
  const b = compileFetchSpec(cbs, new Map());
  expect(Object.isFrozen(a)).toBe(true);
  expect(a.id).not.toBe(b.id); // each compile has a unique identity token
  expect(a.needReceipts).toBe(false);
  expect(a.needTraces).toBe(false);
  expect(a.backfillStart).toBe(0);
  // structurally identical log query
  expect(a.logQuery()).toEqual(b.logQuery());
});

test('INV-5 support: the trace request carries NO trace filter (fetch-all)', () => {
  const trace: any = {
    type: 'trace',
    chainId: 1,
    sourceId: 't',
    fromAddress: undefined,
    toAddress: '0xdead',
    functionSelector: undefined,
    callType: undefined,
    includeReverted: false,
    fromBlock: 0,
    toBlock: 100,
    hasTransactionReceipt: false,
    include: [],
  };
  const spec = compileFetchSpec([{ filter: trace }], new Map());
  expect(spec.needTraces).toBe(true);
  const tq = spec.traceQuery()!;
  expect(tq.traces).toEqual([{ transaction: true }]); // no callTo/callFrom/callSighash — INV-5
});

test('txQuery: account from/to filters pushed server-side; match-all skipped', () => {
  const txf: any = {
    type: 'transaction',
    chainId: 1,
    sourceId: 'x',
    fromAddress: '0xFROM',
    toAddress: undefined,
    includeReverted: false,
    fromBlock: 0,
    toBlock: 100,
    hasTransactionReceipt: true,
    include: [],
  };
  const spec = compileFetchSpec([{ filter: txf }], new Map());
  expect(spec.needTxFilter).toBe(true);
  expect(spec.needReceipts).toBe(true); // hasTransactionReceipt: true
  expect(spec.txQuery()!.transactions).toEqual([{ from: ['0xfrom'] }]);
});
