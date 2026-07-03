import { hexToNumber } from 'viem';
import { afterEach, expect, test } from 'vitest';
import type {
  Address,
  Factory,
  LightBlock,
  LogFilter,
} from '@/internal/types.js';
import type { PortalRealtimeEvent } from './portal-realtime.js';
import {
  assertStreamModeSupported,
  buildPortalLogRequests,
  clampFinalizedToPortalHead,
  discoverChildAddresses,
  getPortalRealtimeEventGenerator,
  isPortalRealtime,
  lightToLightBlock,
  toRealtimeSyncEvent,
  uniqueFactories,
} from './portal-realtime-wire.js';

// ── euler-real constants ──
const FACTORY_ADDR = '0x29a56a1b8214d9cf7c5561811750d5cbdb45cc8e';
const PROXY_CREATED =
  '0x04e664079117e113faa9684bc14aecb41651cbf098b14eda271248c6d0cda57c';
const DEPOSIT =
  '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7';

// euler GenericFactory → EVault (ProxyCreated(address indexed proxy,...), child = topic1)
const eulerFactory = (over: Partial<Factory> = {}): Factory =>
  ({
    id: 'euler-factory',
    type: 'log',
    chainId: 1,
    sourceId: 'EVault',
    address: FACTORY_ADDR,
    eventSelector: PROXY_CREATED as any,
    childAddressLocation: 'topic1',
    fromBlock: undefined,
    toBlock: undefined,
    ...over,
  }) as Factory;

const logFilter = (over: Partial<LogFilter> = {}): LogFilter =>
  ({
    type: 'log',
    chainId: 1,
    sourceId: 'EVault',
    address: eulerFactory() as any, // factory-address filter (isAddressFactory → true)
    topic0: DEPOSIT as any,
    topic1: null,
    topic2: null,
    topic3: null,
    fromBlock: undefined,
    toBlock: undefined,
    hasTransactionReceipt: false,
    include: [],
    ...over,
  }) as LogFilter;

// a padded topic word carrying a 20-byte address in its low bytes (indexed address encoding)
const topicAddr = (addr: string) =>
  `0x${'0'.repeat(24)}${addr.replace(/^0x/, '')}`;
const proxyLog = (proxy: string, over: Record<string, any> = {}): any => ({
  address: FACTORY_ADDR,
  topics: [PROXY_CREATED, topicAddr(proxy)],
  data: '0x',
  blockNumber: '0x64',
  logIndex: '0x0',
  transactionHash: '0xtx',
  transactionIndex: '0x0',
  removed: false,
  ...over,
});

const savedEnv = process.env.PORTAL_REALTIME;
afterEach(() => {
  if (savedEnv === undefined) delete process.env.PORTAL_REALTIME;
  else process.env.PORTAL_REALTIME = savedEnv;
});

// ─────────────────────────────── flag gating ───────────────────────────────

test("isPortalRealtime: only when a chain has a Portal source AND the flag is 'stream'", () => {
  process.env.PORTAL_REALTIME = 'stream';
  expect(isPortalRealtime({ portal: 'http://p' })).toBe(true);
  expect(isPortalRealtime({ portal: undefined })).toBe(false); // no portal source → A-path
  process.env.PORTAL_REALTIME = 'rpc';
  expect(isPortalRealtime({ portal: 'http://p' })).toBe(false); // flag off → A-path
  delete process.env.PORTAL_REALTIME;
  expect(isPortalRealtime({ portal: 'http://p' })).toBe(false); // unset → A-path
});

// ─────────────────────────────── stream-mode capability gate (finding 5) ───────────────────────────────

test('assertStreamModeSupported: log-only sources are accepted', () => {
  expect(() =>
    assertStreamModeSupported([logFilter()], 'mainnet'),
  ).not.toThrow();
});

test('assertStreamModeSupported: a non-log source is refused — it would be silently skipped while marked synced (finding 5)', () => {
  const trace = { type: 'trace' } as any;
  expect(() =>
    assertStreamModeSupported([logFilter(), trace], 'mainnet'),
  ).toThrow(/only log sources, but this chain has trace/);
});

test('assertStreamModeSupported: a log source that needs transaction receipts is refused (finding 5)', () => {
  expect(() =>
    assertStreamModeSupported(
      [logFilter({ hasTransactionReceipt: true })],
      'mainnet',
    ),
  ).toThrow(/transaction receipts/);
});

// ─────────────────────────────── light-block conversion ───────────────────────────────

test('lightToLightBlock: Portal decimal number/timestamp → ponder hex, hashes passthrough', () => {
  const lb = lightToLightBlock({
    number: 101,
    hash: '0xabc',
    parentHash: '0xdef',
    timestamp: 1712,
  });
  expect(lb).toEqual({
    number: '0x65',
    hash: '0xabc',
    parentHash: '0xdef',
    timestamp: '0x6b0',
  });
});

// ─────────────────────────────── factory child discovery ───────────────────────────────

test('discoverChildAddresses: euler ProxyCreated → child proxy (topic1), reusing ponder factory logic', () => {
  const factory = eulerFactory();
  const child = '0x1111111111111111111111111111111111111111';
  const out = discoverChildAddresses([proxyLog(child)] as any, [factory]);
  expect([...out.get(factory)!]).toEqual([child]);
});

test('discoverChildAddresses: ignores non-matching logs (wrong selector / wrong factory address)', () => {
  const factory = eulerFactory();
  const wrongSelector = proxyLog('0x2222222222222222222222222222222222222222', {
    topics: [DEPOSIT, topicAddr('0x2222222222222222222222222222222222222222')],
  });
  const wrongAddress = proxyLog('0x3333333333333333333333333333333333333333', {
    address: '0xdeadbeef00000000000000000000000000000000',
  });
  const out = discoverChildAddresses([wrongSelector, wrongAddress] as any, [
    factory,
  ]);
  expect(out.has(factory)).toBe(false); // nothing matched
});

test('discoverChildAddresses: multiple children in one block', () => {
  const factory = eulerFactory();
  const a = '0xaaaa000000000000000000000000000000000000';
  const b = '0xbbbb000000000000000000000000000000000000';
  const out = discoverChildAddresses([proxyLog(a), proxyLog(b)] as any, [
    factory,
  ]);
  expect([...out.get(factory)!].sort()).toEqual([a, b].sort());
});

// ─────────────────────────────── event conversion ───────────────────────────────

test('toRealtimeSyncEvent: block → BlockWithEventData with logs + their parent TRANSACTIONS (no receipts/traces, childAddresses, no blockCallback)', () => {
  const factory = eulerFactory();
  const childAddresses = new Map([
    [factory, new Set<Address>(['0xchild' as Address])],
  ]);
  const block: PortalRealtimeEvent = {
    type: 'block',
    block: {
      number: '0x65',
      hash: '0xh',
      parentHash: '0xp',
      timestamp: '0x1',
    } as any,
    logs: [{ address: '0xchild' } as any],
    // the matched log's parent tx rides the stream (TX_FIELDS via `transaction: true`) — it must reach
    // ponder's BlockWithEventData, so `event.transaction` works and the finalize insert stores it
    transactions: [{ hash: '0xt' } as any],
    hasMatchedFilter: true,
  };
  const ev = toRealtimeSyncEvent(block, childAddresses) as Extract<
    ReturnType<typeof toRealtimeSyncEvent>,
    { type: 'block' }
  >;
  expect(ev.type).toBe('block');
  expect(ev.hasMatchedFilter).toBe(true);
  expect(ev.transactions).toEqual([{ hash: '0xt' }]); // passthrough, not []
  expect(ev.transactionReceipts).toEqual([]);
  expect(ev.traces).toEqual([]);
  expect(ev.childAddresses).toBe(childAddresses);
  expect(ev.blockCallback).toBeUndefined();
  expect(ev.block.number).toBe('0x65');
});

test('toRealtimeSyncEvent: reorg / finalize pass through with hex LightBlocks', () => {
  const reorg = toRealtimeSyncEvent(
    {
      type: 'reorg',
      block: { number: 10, hash: 'a', parentHash: 'z', timestamp: 10 },
      reorgedBlocks: [
        { number: 11, hash: 'b', parentHash: 'a', timestamp: 11 },
      ],
    },
    new Map(),
  );
  expect(reorg).toEqual({
    type: 'reorg',
    block: { number: '0xa', hash: 'a', parentHash: 'z', timestamp: '0xa' },
    reorgedBlocks: [
      { number: '0xb', hash: 'b', parentHash: 'a', timestamp: '0xb' },
    ],
  });
  const finalize = toRealtimeSyncEvent(
    {
      type: 'finalize',
      block: { number: 12, hash: 'c', parentHash: 'b', timestamp: 12 },
    },
    new Map(),
  );
  expect(finalize).toEqual({
    type: 'finalize',
    block: { number: '0xc', hash: 'c', parentHash: 'b', timestamp: '0xc' },
  });
});

// ─────────────────────────────── log-request construction ───────────────────────────────

test('uniqueFactories: dedupes factories by id across event callbacks', () => {
  const f = uniqueFactories([{ filter: logFilter() }, { filter: logFilter() }]);
  expect(f.length).toBe(1);
  expect(f[0]!.id).toBe('euler-factory');
});

test('buildPortalLogRequests: factory-address log filter → known children, plus a ProxyCreated discovery request', () => {
  const childAddresses = new Map([
    [
      'euler-factory',
      new Map<Address, number>([
        ['0xvault1' as Address, 100],
        ['0xvault2' as Address, 101],
      ]),
    ],
  ]);
  const reqs = buildPortalLogRequests(
    [{ filter: logFilter() }],
    childAddresses,
  );
  // one request over the two known children for the Deposit topic0…
  const child = reqs.find((r) => r.topic0?.includes(DEPOSIT));
  expect(child?.address?.sort()).toEqual(['0xvault1', '0xvault2']);
  // …and a discovery request on the factory address for ProxyCreated
  const disc = reqs.find((r) => r.topic0?.includes(PROXY_CREATED));
  expect(disc?.address).toEqual([FACTORY_ADDR]);
});

test('buildPortalLogRequests: a factory filter with no known children yet still emits the discovery request', () => {
  const reqs = buildPortalLogRequests([{ filter: logFilter() }], new Map());
  expect(reqs.some((r) => r.topic0?.includes(DEPOSIT))).toBe(false); // no children → no child request
  expect(
    reqs.some(
      (r) =>
        r.topic0?.includes(PROXY_CREATED) && r.address?.[0] === FACTORY_ADDR,
    ),
  ).toBe(true);
});

// ─────────────────────────────── finality clamp (independence-critical) ───────────────────────────────

test('clampFinalizedToPortalHead: A-path passthrough — flag off never touches the finalized block or the network', async () => {
  delete process.env.PORTAL_REALTIME;
  let called = false;
  const fetchImpl = (async () => {
    called = true;
    return { json: async () => ({ number: 1 }) };
  }) as any;
  const finalized = {
    number: '0x3e8',
    hash: '0xh',
    parentHash: '0xp',
    timestamp: '0x1',
  } as LightBlock;
  const out = await clampFinalizedToPortalHead({
    chain: { portal: 'http://p', name: 'c' } as any,
    rpc: {} as any,
    finalizedBlock: finalized,
    fetchImpl,
  });
  expect(out).toBe(finalized); // unchanged
  expect(called).toBe(false); // no probe
});

test('clampFinalizedToPortalHead: Portal at/ahead of RPC finalized → no clamp (never RAISES the boundary)', async () => {
  process.env.PORTAL_REALTIME = 'stream';
  const fetchImpl = (async () => ({
    json: async () => ({ number: 5000 }),
  })) as any; // portal head ≥ finalized 1000
  const finalized = {
    number: '0x3e8',
    hash: '0xh',
    parentHash: '0xp',
    timestamp: '0x1',
  } as LightBlock;
  const out = await clampFinalizedToPortalHead({
    chain: { portal: 'http://p', name: 'c' } as any,
    rpc: {} as any,
    finalizedBlock: finalized,
    fetchImpl,
  });
  expect(out).toBe(finalized);
});

test('clampFinalizedToPortalHead: Portal head unknown in stream mode → FATAL (never silently passes the RPC finalized through) (finding 6)', async () => {
  process.env.PORTAL_REALTIME = 'stream';
  const fetchImpl = (async () => {
    throw new Error('down');
  }) as any;
  const finalized = {
    number: '0x3e8',
    hash: '0xh',
    parentHash: '0xp',
    timestamp: '0x1',
  } as LightBlock;
  // Old behavior passed `finalized` through — leaving historical targeting (portalHead, rpcFinalized] while
  // realtime starts above it: a permanent silent gap. In stream mode a head we can't probe is fatal.
  await expect(
    clampFinalizedToPortalHead({
      chain: { portal: 'http://p', name: 'c' } as any,
      rpc: {} as any,
      finalizedBlock: finalized,
      fetchImpl,
    }),
  ).rejects.toThrow(/finalized-head probe failed/);
});

test('clampFinalizedToPortalHead: Portal head BELOW RPC finalized → refetch the block at the Portal head', async () => {
  process.env.PORTAL_REALTIME = 'stream';
  const fetchImpl = (async () => ({
    json: async () => ({ number: 900 }),
  })) as any; // portal head 900 < finalized 1000
  let requested: any;
  const fullBlock = {
    number: '0x384',
    hash: '0xportalhead',
    parentHash: '0xpp',
    timestamp: '0x2',
    logsBloom: `0x${'0'.repeat(512)}`,
    sha3Uncles: '0x0',
    miner: '0x0',
    stateRoot: '0x0',
    transactionsRoot: '0x0',
    receiptsRoot: '0x0',
    gasUsed: '0x0',
    gasLimit: '0x0',
    extraData: '0x',
    nonce: '0x0',
    mixHash: '0x0',
    difficulty: '0x0',
    size: '0x0',
    transactions: [],
  };
  const rpc = {
    request: async (req: any) => {
      requested = req;
      return fullBlock;
    },
  } as any;
  const finalized = {
    number: '0x3e8',
    hash: '0xh',
    parentHash: '0xp',
    timestamp: '0x1',
  } as LightBlock;
  const out = await clampFinalizedToPortalHead({
    chain: { portal: 'http://p', name: 'c' } as any,
    rpc,
    finalizedBlock: finalized,
    fetchImpl,
  });
  expect(requested.method).toBe('eth_getBlockByNumber');
  expect(requested.params[0]).toBe('0x384'); // numberToHex(900)
  expect(hexToNumber(out.number)).toBe(900);
});

// ─────────────────────────────── end-to-end generator ───────────────────────────────

// mock the Portal /stream — one NDJSON connection per entry in `connections` (then 204) — and
// /finalized-head (JSON). `seenBodies` (optional) collects each /stream request body for filter asserts.
function mockPortalConns(
  connections: any[][],
  finalizedHead: number,
  seenBodies?: any[],
) {
  let conn = 0;
  return (async (url: string, init?: any) => {
    if (url.endsWith('/finalized-head'))
      return { json: async () => ({ number: finalizedHead }) };
    if (seenBodies && init?.body) seenBodies.push(JSON.parse(init.body));
    if (conn >= connections.length)
      return { status: 204, ok: false, body: null };
    const lines = connections[conn++]!.map(
      (b) => JSON.stringify(b) + '\n',
    ).join('');
    const body = new ReadableStream({
      start(c) {
        c.enqueue(new TextEncoder().encode(lines));
        c.close();
      },
    });
    return { status: 200, ok: true, body };
  }) as any;
}
const mockPortal = (batches: any[], finalizedHead: number) =>
  mockPortalConns([batches], finalizedHead);

test('getPortalRealtimeEventGenerator: a child discovered in block N gets N RE-DELIVERED complete — its SAME-BLOCK logs are not lost; terminates at endBlock', async () => {
  // The /stream filter is server-side and snapshotted at connection open, so a child created in block N
  // has its own block-N logs filtered out of the connection N arrived on. The old flow forwarded N's
  // incomplete event and resumed from N+1 — the child's same-block logs were permanently lost (interval
  // marked cached on finalize). Now the wire suppresses the incomplete event, widens the filter, and the
  // stream re-opens FROM N: ponder receives exactly ONE block event for N, carrying the child's log.
  process.env.PORTAL_REALTIME = 'stream';
  const factory = eulerFactory();
  const child = '0x1111111111111111111111111111111111111111';
  const childDeposit = {
    address: child,
    topics: [DEPOSIT],
    data: '0x',
    blockNumber: 101,
    logIndex: 1,
    transactionHash: '0xtx2',
    transactionIndex: 0,
    removed: false,
  };
  const conn1 = [
    {
      header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 1000 },
      logs: [],
    },
    {
      // the OLD filter matches only the factory's creation event — the child's own Deposit in the SAME
      // block was filtered out server-side and is absent here
      header: {
        number: 101,
        hash: 'h101',
        parentHash: 'h100',
        timestamp: 1012,
      },
      logs: [proxyLog(child, { blockNumber: 101 })],
    },
  ];
  const conn2 = [
    {
      // the reopened connection (widened filter) re-delivers block 101 COMPLETE
      header: {
        number: 101,
        hash: 'h101',
        parentHash: 'h100',
        timestamp: 1012,
      },
      logs: [proxyLog(child, { blockNumber: 101 }), childDeposit],
    },
  ];
  const childAddresses = new Map<string, Map<Address, number>>([
    ['euler-factory', new Map()],
  ]);
  const bodies: any[] = [];
  const events: any[] = [];
  for await (const { event } of getPortalRealtimeEventGenerator({
    common: { logger: { info() {}, debug() {}, warn() {}, trace() {} } } as any,
    chain: { id: 1, name: 'mainnet', portal: 'http://portal' } as any,
    rpc: {} as any,
    // share the same factory reference the assertion uses (getFilterFactories returns filter.address)
    eventCallbacks: [{ filter: logFilter({ address: factory as any }) } as any],
    syncProgress: {
      finalized: {
        number: '0x63',
        hash: 'h99',
        parentHash: 'h98',
        timestamp: '0x1',
      } as any,
      end: { number: '0x65' } as any,
    },
    childAddresses,
    fetchImpl: mockPortalConns([conn1, conn2], 0, bodies),
  })) {
    events.push(event);
  }

  const blocks = events.filter((e) => e.type === 'block');
  expect(blocks.length).toBe(2); // EXACTLY one event per block — no double-delivery of 101
  expect(blocks[0].block.number).toBe('0x64'); // 100
  expect(blocks[1].block.number).toBe('0x65'); // 101 (the redelivered, COMPLETE version)
  // THE REGRESSION: the child's own same-block Deposit is present on the (single) block-101 event
  expect(
    blocks[1].logs.some(
      (l: any) => l.address === child && l.topics[0] === DEPOSIT,
    ),
  ).toBe(true);
  // discovery still surfaced on the event AND folded into the running map
  expect([...blocks[1].childAddresses.get(factory)!]).toEqual([child]);
  expect(childAddresses.get('euler-factory')!.get(child as Address)).toBe(101);
  // the redelivery connection re-opened FROM block 101 with the WIDENED server-side filter
  const reopened = bodies[bodies.length - 1];
  expect(reopened.fromBlock).toBe(101);
  expect(JSON.stringify(reopened.logs)).toContain(child);
  expect(blocks[1].blockCallback).toBeUndefined();
});

test('getPortalRealtimeEventGenerator: a reorg PRUNES reorged-out children from the running map and narrows the filter', async () => {
  // Stock createRealtimeSync deletes reorged blocks' children (childAddressesPerBlock); without pruning,
  // a child whose creation block was reorged away keeps matching — every later log from that address is
  // indexed as a phantom child event until restart.
  process.env.PORTAL_REALTIME = 'stream';
  const factory = eulerFactory();
  const child = '0x2222222222222222222222222222222222222222';
  const conn1 = [
    {
      header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 1000 },
      logs: [],
    },
    {
      header: {
        number: 101,
        hash: 'h101a',
        parentHash: 'h100',
        timestamp: 1012,
      },
      logs: [proxyLog(child, { blockNumber: 101 })],
    },
  ];
  const conn2 = [
    {
      // redelivery of 101a (same-block handshake) — still carries the creation log
      header: {
        number: 101,
        hash: 'h101a',
        parentHash: 'h100',
        timestamp: 1012,
      },
      logs: [proxyLog(child, { blockNumber: 101 })],
    },
    {
      // the fork: 101b replaces 101a (parent = 100) — the creation event is GONE on the new fork
      header: {
        number: 101,
        hash: 'h101b',
        parentHash: 'h100',
        timestamp: 1013,
      },
      logs: [],
    },
  ];
  // the prune rebuilds the filter (revision bump) → the stream re-opens from the tip; the re-delivered
  // tip is a routine duplicate (skipped — no redelivery awaited), then the chain continues
  const conn3 = [
    {
      header: {
        number: 101,
        hash: 'h101b',
        parentHash: 'h100',
        timestamp: 1013,
      },
      logs: [],
    },
    {
      header: {
        number: 102,
        hash: 'h102b',
        parentHash: 'h101b',
        timestamp: 1024,
      },
      logs: [],
    },
  ];
  const childAddresses = new Map<string, Map<Address, number>>([
    ['euler-factory', new Map()],
  ]);
  const bodies: any[] = [];
  const events: any[] = [];
  for await (const { event } of getPortalRealtimeEventGenerator({
    common: { logger: { info() {}, debug() {}, warn() {}, trace() {} } } as any,
    chain: { id: 1, name: 'mainnet', portal: 'http://portal' } as any,
    rpc: {} as any,
    eventCallbacks: [{ filter: logFilter({ address: factory as any }) } as any],
    syncProgress: {
      finalized: {
        number: '0x63',
        hash: 'h99',
        parentHash: 'h98',
        timestamp: '0x1',
      } as any,
      end: { number: '0x66' } as any,
    },
    childAddresses,
    fetchImpl: mockPortalConns([conn1, conn2, conn3], 0, bodies),
  })) {
    events.push(event);
  }

  const reorg = events.find((e) => e.type === 'reorg');
  expect(reorg).toBeDefined();
  expect(reorg.block.hash).toBe('h100'); // common ancestor
  // THE REGRESSION: the reorged-out child is pruned from the running map…
  expect(childAddresses.get('euler-factory')!.has(child as Address)).toBe(
    false,
  );
  // …and the server-side filter was rebuilt WITHOUT it after the reorg
  const last = bodies[bodies.length - 1];
  expect(JSON.stringify(last.logs)).not.toContain(child);
});

test('getPortalRealtimeEventGenerator: emits a monotonic finalize (above the startup finalized) from the head poll', async () => {
  process.env.PORTAL_REALTIME = 'stream';
  const batches = [
    {
      header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 1000 },
      logs: [],
    },
    {
      header: {
        number: 101,
        hash: 'h101',
        parentHash: 'h100',
        timestamp: 1012,
      },
      logs: [],
    },
  ];
  const events: any[] = [];
  for await (const { event } of getPortalRealtimeEventGenerator({
    common: { logger: { info() {}, debug() {}, warn() {}, trace() {} } } as any,
    chain: { id: 1, name: 'mainnet', portal: 'http://portal' } as any,
    rpc: {} as any,
    eventCallbacks: [{ filter: logFilter() } as any],
    syncProgress: {
      finalized: {
        number: '0x63',
        hash: 'h99',
        parentHash: 'h98',
        timestamp: '0x1',
      } as any,
      end: { number: '0x65' } as any,
    },
    childAddresses: new Map([['euler-factory', new Map()]]),
    fetchImpl: mockPortal(batches, 100), // Portal head 100 > startup finalized 99 → finalize(100)
    finalizePollMs: 0,
  })) {
    events.push(event);
  }
  const finalize = events.find((e) => e.type === 'finalize');
  expect(finalize).toBeDefined();
  expect(hexToNumber(finalize.block.number)).toBe(100); // > startup finalized (99) → allowed
});
