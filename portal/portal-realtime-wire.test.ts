import { hexToNumber } from 'viem';
import { afterEach, expect, test } from 'vitest';
import type {
  Address,
  Factory,
  LightBlock,
  LogFilter,
} from '@/internal/types.js';
import { encodeCheckpoint, ZERO_CHECKPOINT } from '@/utils/checkpoint.js';
import type { PortalRealtimeEvent } from './portal-realtime.js';
import {
  assertStreamModeSupported,
  buildPortalLogRequests,
  checkpointBlockNumber,
  clampFinalizedToPortalHead,
  deriveFinalityFloor,
  discoverChildAddresses,
  getPortalRealtimeEventGenerator,
  isPortalRealtime,
  lightToLightBlock,
  portalFinalizedHead,
  resolveRedeliveryTimeoutMs,
  resolveStreamIdleMs,
  type SafeCrashRecoveryBlockLookup,
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
const savedPin = process.env.PORTAL_FINALIZED_HEAD;
afterEach(() => {
  if (savedEnv === undefined) delete process.env.PORTAL_REALTIME;
  else process.env.PORTAL_REALTIME = savedEnv;
  if (savedPin === undefined) delete process.env.PORTAL_FINALIZED_HEAD;
  else process.env.PORTAL_FINALIZED_HEAD = savedPin;
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

// ─────────────────────────────── finalized-head probe (bounded — wave 4) ───────────────────────────────

test('portalFinalizedHead: a HUNG probe is BOUNDED — resolves undefined instead of freezing the block loop (wave 4)', async () => {
  // This probe used to be a bare fetch().then(r => r.json()) with no timeout, abort signal, or body
  // bound — one black-holed connection froze finalize emission mid-run (portalRealtimeEvents awaits it
  // inline in the block loop) and startup (the clamp's 3-attempt retry never reached attempt 2, because
  // attempt 1 never settled). It now delegates to the client's shared bounded probe (probeFinalizedHead,
  // issue #14 / PR #16 hardening): the connect phase aborts after timeoutMs and every failure collapses
  // to undefined, so the caller stays conservative.
  const hung = ((_url: string, init?: { signal?: AbortSignal }) =>
    new Promise((_resolve, reject) => {
      init?.signal?.addEventListener('abort', () => {
        reject(new Error('aborted'));
      });
    })) as any;
  const out = await portalFinalizedHead('http://portal', {}, hung, 50);
  expect(out).toBeUndefined(); // resolved (bounded), not a forever-pending await
});

test('portalFinalizedHead: parses number + canonical hash (the hash arms the wrong-fork finalize guard)', async () => {
  const fetchImpl = (async () =>
    new Response(JSON.stringify({ number: 7, hash: '0xabc' }))) as any;
  const out = await portalFinalizedHead('http://portal/', {}, fetchImpl);
  expect(out).toEqual({ number: 7, hash: '0xabc' });
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

test('clampFinalizedToPortalHead: an explicit PORTAL_FINALIZED_HEAD pin below the live head is AUTHORITATIVE — the clamp honors it with NO /finalized-head probe (review B5a / fix 4)', async () => {
  // FIX 5 made the pin authoritative for the finality boundary in portal.ts (the historical seam). This
  // clamp MUST agree: if it re-probed the LIVE head while portal.ts honored the pin, intervals in
  // (pin, liveHead] would be marked synced EMPTY while realtime streamed from liveHead+1 — the exact
  // G4/C11 silent gap. So with the pin set, the clamp returns the block at the PIN and never hits the
  // network. (Zero-coverage before: making the clamp ignore the pin and probe the live head left the whole
  // suite green.)
  process.env.PORTAL_REALTIME = 'stream';
  process.env.PORTAL_FINALIZED_HEAD = '900'; // pin 900 < RPC finalized 1000
  let probed = false;
  const fetchImpl = (async () => {
    probed = true; // any /finalized-head probe flips this — the pin path must NOT
    return { json: async () => ({ number: 5000 }) }; // a live head that, if probed, would NOT clamp
  }) as any;
  let requested: any;
  const pinBlock = {
    number: '0x384', // block at the pin (900)
    hash: '0xpinhead',
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
      return pinBlock;
    },
  } as any;
  const finalized = {
    number: '0x3e8', // 1000
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
  expect(probed).toBe(false); // the pin is authoritative — NO live-head probe
  expect(requested.method).toBe('eth_getBlockByNumber');
  expect(requested.params[0]).toBe('0x384'); // clamped to the PIN (900), not the un-probed live head
  expect(hexToNumber(out.number)).toBe(900);
});

// ─────────────────────────────── persisted-finality floor ───────────────────────────────

// Shared scaffolding for the floor tests: RPC finalized at 1000, an rpc mock that records the block
// request, and a floor passed in by the caller (as the wiring does from ponder's persisted checkpoint).
const rpcFinalized1000 = {
  number: '0x3e8',
  hash: '0xh',
  parentHash: '0xp',
  timestamp: '0x1',
} as LightBlock;
const recordingRpc = (blockNumberHex: string) => {
  const calls: any[] = [];
  const rpc = {
    request: async (req: any) => {
      calls.push(req);
      return {
        number: blockNumberHex,
        hash: '0xfloorblock',
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
    },
  } as any;

  return { rpc, calls };
};

test('clampFinalizedToPortalHead: a probed head BELOW the persisted floor is clamped UP to the floor — a restart against a lagging replica must not re-stream already-finalized (unrevertable) blocks', async () => {
  process.env.PORTAL_REALTIME = 'stream';
  // Last run persisted finality at 950; this restart's probe hits a lagging replica reporting 900.
  // Without the floor, realtime would stream from 901 and re-index (900, 950] — rows crash recovery
  // cannot revert (their reorg-table rows were deleted at finalize).
  const fetchImpl = (async () => ({
    json: async () => ({ number: 900 }),
  })) as any;
  const { rpc, calls } = recordingRpc('0x3b6'); // 950
  const out = await clampFinalizedToPortalHead({
    chain: { portal: 'http://p', name: 'c' } as any,
    rpc,
    finalizedBlock: rpcFinalized1000,
    floor: 950,
    fetchImpl,
  });
  expect(calls[0]!.params[0]).toBe('0x3b6'); // the boundary block is fetched at the FLOOR (950), not 900
  expect(hexToNumber(out.number)).toBe(950);
});

test('clampFinalizedToPortalHead: a floor at/below the probed head is inert — the head is adopted exactly as before', async () => {
  process.env.PORTAL_REALTIME = 'stream';
  const fetchImpl = (async () => ({
    json: async () => ({ number: 900 }),
  })) as any;
  const { rpc, calls } = recordingRpc('0x384'); // 900
  const out = await clampFinalizedToPortalHead({
    chain: { portal: 'http://p', name: 'c' } as any,
    rpc,
    finalizedBlock: rpcFinalized1000,
    floor: 900, // == head → no effect
    fetchImpl,
  });
  expect(calls[0]!.params[0]).toBe('0x384'); // clamped to the head, same as the floorless behavior
  expect(hexToNumber(out.number)).toBe(900);
});

test('clampFinalizedToPortalHead: a floor at/above the RPC finalized block returns it unchanged — the floor never RAISES the boundary past ponder’s own finality', async () => {
  process.env.PORTAL_REALTIME = 'stream';
  const fetchImpl = (async () => ({
    json: async () => ({ number: 900 }),
  })) as any;
  const { rpc, calls } = recordingRpc('0x0');
  const out = await clampFinalizedToPortalHead({
    chain: { portal: 'http://p', name: 'c' } as any,
    rpc,
    finalizedBlock: rpcFinalized1000,
    floor: 1_000, // floor == RPC finalized → pass-through, no block refetch
    fetchImpl,
  });
  expect(out).toBe(rpcFinalized1000);
  expect(calls.length).toBe(0);
});

test('clampFinalizedToPortalHead: the floor overrides a PORTAL_FINALIZED_HEAD pin below it — a pin below persisted finality must not re-open the double-indexing hole', async () => {
  process.env.PORTAL_REALTIME = 'stream';
  process.env.PORTAL_FINALIZED_HEAD = '900'; // operator pin BELOW the persisted floor 950
  let probed = false;
  const fetchImpl = (async () => {
    probed = true;
    return { json: async () => ({ number: 5000 }) };
  }) as any;
  const { rpc, calls } = recordingRpc('0x3b6'); // 950
  const out = await clampFinalizedToPortalHead({
    chain: { portal: 'http://p', name: 'c' } as any,
    rpc,
    finalizedBlock: rpcFinalized1000,
    floor: 950,
    fetchImpl,
  });
  expect(probed).toBe(false); // the pin still suppresses the live probe
  expect(calls[0]!.params[0]).toBe('0x3b6'); // but the boundary is the FLOOR, not the pin
  expect(hexToNumber(out.number)).toBe(950);
});

test('checkpointBlockNumber: decodes the block number out of a persisted checkpoint string; undefined stays undefined', () => {
  const checkpoint = encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    blockTimestamp: 1_700_000_000n,
    chainId: 1n,
    blockNumber: 12_345n,
  });
  expect(checkpointBlockNumber(checkpoint, 1)).toBe(12_345);
  expect(checkpointBlockNumber(undefined, 1)).toBeUndefined();
});

test("checkpointBlockNumber: refuses a FOREIGN-chain checkpoint's block number (the same-chain fast path only) — its height is another chain's, meaningless locally", () => {
  // finalizeOmnichain updates PONDER_CHECKPOINT with no per-chain where clause: chain 1's row can carry
  // a checkpoint encoding chain 8453's block height. As a floor its block NUMBER is wrong in both
  // directions, so this helper (the same-chain fast path) refuses it; deriveFinalityFloor then maps the
  // FOREIGN checkpoint's TIMESTAMP to a local block instead (see the deriveFinalityFloor tests below).
  const foreign = encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    blockTimestamp: 1_700_000_000n,
    chainId: 8_453n,
    blockNumber: 99_999_999n, // a Base-scale height, nonsense on an Ethereum-scale chain
  });
  expect(checkpointBlockNumber(foreign, 1)).toBeUndefined();
  expect(checkpointBlockNumber(foreign, 8_453)).toBe(99_999_999); // same chain → its own block, valid
});

// ─────────────────────────────── deriveFinalityFloor ───────────────────────────────

// A getSafeCrashRecoveryBlock lookup that records its args and returns the block whose timestamp is the
// GREATEST that is STRICTLY BELOW the requested timestamp — the exact `timestamp < :ts` semantics of
// upstream's sync-store query (SELECT number,timestamp FROM blocks WHERE chainId=? AND timestamp<? ORDER
// BY number DESC LIMIT 1). `blocks` is an ascending-by-number list of {number, timestamp} for the chain.
const recordingLookup = (blocks: { number: bigint; timestamp: bigint }[]) => {
  const calls: { chainId: number; timestamp: number }[] = [];
  const lookup: SafeCrashRecoveryBlockLookup = async ({
    chainId,
    timestamp,
  }) => {
    calls.push({ chainId, timestamp });
    let match: { number: bigint; timestamp: bigint } | undefined;
    for (const block of blocks) {
      if (block.timestamp < BigInt(timestamp)) {
        match = block; // ascending list → last match is the highest block below the timestamp
      }
    }

    return match;
  };

  return { lookup, calls };
};

test('deriveFinalityFloor: SAME-CHAIN checkpoint uses its own block number directly (fast path) — never queries the store', async () => {
  const checkpoint = encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    blockTimestamp: 1_700_000_000n,
    chainId: 1n,
    blockNumber: 12_345n,
  });
  const { lookup, calls } = recordingLookup([
    { number: 999_999n, timestamp: 1_699_000_000n },
  ]);
  const floor = await deriveFinalityFloor({
    checkpoint,
    chainId: 1,
    getSafeCrashRecoveryBlock: lookup,
  });
  expect(floor).toBe(12_345); // the checkpoint's own block, verbatim
  expect(calls.length).toBe(0); // same-chain → no timestamp mapping, the store is never touched
});

test('deriveFinalityFloor: undefined checkpoint → undefined floor (no checkpoint persisted yet)', async () => {
  const { lookup, calls } = recordingLookup([]);
  const floor = await deriveFinalityFloor({
    checkpoint: undefined,
    chainId: 1,
    getSafeCrashRecoveryBlock: lookup,
  });
  expect(floor).toBeUndefined();
  expect(calls.length).toBe(0);
});

test('deriveFinalityFloor: a FOREIGN checkpoint is TIMESTAMP-MAPPED to the local chain’s highest block at/below that timestamp — the floor is a LOCAL block, not the foreign height (issue #57)', async () => {
  // finalizeOmnichain wrote chain 8453's checkpoint (timestamp 1_700_000_500, block 99_999_999) into
  // chain 1's row. The foreign block number is meaningless locally; the TIMESTAMP maps to chain 1's
  // highest block finalized at/below it. This mirrors upstream getSafeCrashRecoveryBlock verbatim.
  const foreign = encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    blockTimestamp: 1_700_000_500n,
    chainId: 8_453n,
    blockNumber: 99_999_999n,
  });
  // chain 1 local blocks: 900@…000, 950@…400 (below the checkpoint ts), 1000@…600 (ABOVE it, must NOT map)
  const { lookup, calls } = recordingLookup([
    { number: 900n, timestamp: 1_700_000_000n },
    { number: 950n, timestamp: 1_700_000_400n },
    { number: 1_000n, timestamp: 1_700_000_600n },
  ]);
  const floor = await deriveFinalityFloor({
    checkpoint: foreign,
    chainId: 1,
    getSafeCrashRecoveryBlock: lookup,
  });
  expect(floor).toBe(950); // highest LOCAL block with timestamp < 1_700_000_500 — NOT 99_999_999, NOT 1000
  expect(calls).toEqual([{ chainId: 1, timestamp: 1_700_000_500 }]); // queried with the LOCAL chainId + foreign ts
});

test('deriveFinalityFloor: FOREIGN checkpoint but NO local block at/below the timestamp → undefined floor (pre-#55 behavior — first-ever run / older-than-every-block checkpoint; strictly safe)', async () => {
  const foreign = encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    blockTimestamp: 1_700_000_500n,
    chainId: 8_453n,
    blockNumber: 99_999_999n,
  });
  // every local block is ABOVE the checkpoint timestamp (or the table is empty) → the store returns none
  const { lookup, calls } = recordingLookup([
    { number: 1_000n, timestamp: 1_700_000_600n },
  ]);
  const floor = await deriveFinalityFloor({
    checkpoint: foreign,
    chainId: 1,
    getSafeCrashRecoveryBlock: lookup,
  });
  expect(floor).toBeUndefined(); // no mappable local block → no floor → pre-#55 pass-through (never worse)
  expect(calls).toEqual([{ chainId: 1, timestamp: 1_700_000_500 }]);
});

test('deriveFinalityFloor: FOREIGN checkpoint with NO store lookup supplied → undefined floor (the cutover sites derive their floor from same-run state, not a checkpoint)', async () => {
  const foreign = encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    blockTimestamp: 1_700_000_500n,
    chainId: 8_453n,
    blockNumber: 99_999_999n,
  });
  const floor = await deriveFinalityFloor({ checkpoint: foreign, chainId: 1 });
  expect(floor).toBeUndefined(); // cannot map without a store → pre-#55 behavior (same as the old guard)
});

test('deriveFinalityFloor → clampFinalizedToPortalHead: the timestamp-mapped floor actually CLAMPS the stream-mode boundary UP — a lagging replica must not re-stream already-finalized blocks after an omnichain restart (issue #57)', async () => {
  // End-to-end: an omnichain restart where chain 1's row carries chain 8453's checkpoint. The foreign
  // checkpoint maps to chain 1's local block 950; a lagging replica reports head 900; the derived floor
  // must clamp the boundary UP to 950, exactly as #55's same-chain floor does. RPC finalized is 1000.
  process.env.PORTAL_REALTIME = 'stream';
  const foreign = encodeCheckpoint({
    ...ZERO_CHECKPOINT,
    blockTimestamp: 1_700_000_500n,
    chainId: 8_453n,
    blockNumber: 99_999_999n,
  });
  const { lookup } = recordingLookup([
    { number: 900n, timestamp: 1_700_000_000n },
    { number: 950n, timestamp: 1_700_000_400n },
  ]);
  const floor = await deriveFinalityFloor({
    checkpoint: foreign,
    chainId: 1,
    getSafeCrashRecoveryBlock: lookup,
  });
  expect(floor).toBe(950);

  const fetchImpl = (async () => ({
    json: async () => ({ number: 900 }), // lagging replica
  })) as any;
  const { rpc, calls } = recordingRpc('0x3b6'); // 950
  const out = await clampFinalizedToPortalHead({
    chain: { portal: 'http://p', name: 'c' } as any,
    rpc,
    finalizedBlock: rpcFinalized1000,
    floor,
    fetchImpl,
  });
  expect(calls[0]!.params[0]).toBe('0x3b6'); // boundary fetched at the MAPPED floor (950), not the lagging 900
  expect(hexToNumber(out.number)).toBe(950);
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
  // FIX 5 request side (review B5b): EVERY outgoing /stream body must project the parent transaction — the
  // `transaction: true` relation on each log request PLUS the TX_FIELDS `fields.transaction` map. Dropping
  // either would silently leave `event.transaction` undefined and store logs without tx rows (only mocks
  // that inject `transactions` directly would still pass). So assert the projection on the wire.
  for (const b of bodies) {
    expect(b.fields.transaction).toBeDefined();
    expect(b.fields.transaction.hash).toBe(true); // a TX_FIELDS column
    for (const r of b.logs) expect(r.transaction).toBe(true); // the parent-tx relation per log request
  }
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

test('getPortalRealtimeEventGenerator: a redelivery that never lands is bounded by a watchdog and fails loud (recommended)', async () => {
  // Block N discovers a child and is suppressed for its same-block redelivery; streamHotBlocks re-opens
  // FROM N. On a HALTED chain the reopened stream 204s forever, so no event reaches the wire loop to trip a
  // per-event check — the wait would stall SILENTLY. The watchdog bounds it: after redeliveryTimeoutMs it
  // aborts the stream and the generator rethrows a diagnosable fatal instead of hanging.
  process.env.PORTAL_REALTIME = 'stream';
  const factory = eulerFactory();
  const child = '0x5555555555555555555555555555555555555555';
  const conn1 = [
    {
      // block 100 creates a child → suppressed, redelivery awaited. No further connection redelivers it
      // (mockPortalConns 204s past the last entry), simulating a halted/non-re-serving stream.
      header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 1000 },
      logs: [proxyLog(child, { blockNumber: 100 })],
    },
  ];
  const run = (async () => {
    for await (const _ of getPortalRealtimeEventGenerator({
      common: {
        logger: { info() {}, debug() {}, warn() {}, trace() {} },
      } as any,
      chain: { id: 1, name: 'mainnet', portal: 'http://portal' } as any,
      rpc: {} as any,
      eventCallbacks: [
        { filter: logFilter({ address: factory as any }) } as any,
      ],
      syncProgress: {
        finalized: {
          number: '0x63',
          hash: 'h99',
          parentHash: 'h98',
          timestamp: '0x1',
        } as any,
        end: { number: '0x66' } as any,
      },
      childAddresses: new Map<string, Map<Address, number>>([
        ['euler-factory', new Map()],
      ]),
      fetchImpl: mockPortalConns([conn1], 0),
      redeliveryTimeoutMs: 50, // tiny watchdog: the redelivery never comes → fail loud fast
    })) {
      /* drain */
    }
  })();
  await expect(run).rejects.toThrow(/never re-delivered it within/i);
});

// like mockPortalConns but the /finalized-head carries the canonical HASH (arms the wrong-fork guard and
// lets a finalize land at the exact height) — used for the held-finalize-during-redelivery case.
function mockPortalConnsWithHead(
  connections: any[][],
  head: { number: number; hash: string },
  seenBodies?: any[],
) {
  let conn = 0;
  return (async (url: string, init?: any) => {
    if (url.endsWith('/finalized-head')) return { json: async () => head };
    if (seenBodies && init?.body) seenBodies.push(JSON.parse(init.body));
    if (conn >= connections.length)
      return { status: 204, ok: false, body: null };
    const lines = connections[conn++]!.map(
      (b: any) => JSON.stringify(b) + '\n',
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

test('getPortalRealtimeEventGenerator: a finalize covering a block held for redelivery is EMITTED after the redelivered block, not dropped (review B2)', async () => {
  // Block N discovers a child and is suppressed for redelivery. A finalize covering N arrives from the
  // head poll WHILE awaiting — portalRealtimeEvents has already consumed it (window cleared, anchor
  // advanced), so if the wire merely dropped it no later poll would re-emit it: at endBlock=N (here) or a
  // halted chain ponder would never finalize N. The wire must stash it and emit it right after forwarding
  // the redelivered N — ordering: block N, then finalize N.
  process.env.PORTAL_REALTIME = 'stream';
  const factory = eulerFactory();
  const child = '0x4444444444444444444444444444444444444444';
  const conn1 = [
    {
      // block 100 creates a child → its own same-block logs were filtered out; the wire suppresses it and
      // awaits the redelivery. The finalize poll right after (head 100 = h100) is HELD, not forwarded.
      header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 1000 },
      logs: [proxyLog(child, { blockNumber: 100 })],
    },
  ];
  const conn2 = [
    {
      // the reopened connection (widened filter) re-delivers block 100 complete
      header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 1000 },
      logs: [proxyLog(child, { blockNumber: 100 })],
    },
  ];
  const events: any[] = [];
  for await (const { event } of getPortalRealtimeEventGenerator({
    common: { logger: { info() {}, debug() {}, warn() {}, trace() {} } } as any,
    chain: { id: 1, name: 'mainnet', portal: 'http://portal' } as any,
    rpc: {} as any,
    eventCallbacks: [{ filter: logFilter({ address: factory as any }) } as any],
    syncProgress: {
      finalized: {
        number: '0x63', // 99 — startup finalized
        hash: 'h99',
        parentHash: 'h98',
        timestamp: '0x1',
      } as any,
      end: { number: '0x64' } as any, // endBlock 100: the finalize must still land before return
    },
    childAddresses: new Map<string, Map<Address, number>>([
      ['euler-factory', new Map()],
    ]),
    // head 100 with its canonical hash → finalize(100) fires at the exact height (no B1 defer)
    fetchImpl: mockPortalConnsWithHead([conn1, conn2], {
      number: 100,
      hash: 'h100',
    }),
    finalizePollMs: 0,
  })) {
    events.push(event);
  }
  // exactly one block-100 event (the redelivered, complete one) …
  const blocks = events.filter((e) => e.type === 'block');
  expect(blocks.length).toBe(1);
  expect(hexToNumber(blocks[0].block.number)).toBe(100);
  // … and the held finalize(100) IS emitted, AFTER the block (ordering: block N, then finalize N)
  const finalize = events.find((e) => e.type === 'finalize');
  expect(finalize).toBeDefined();
  expect(hexToNumber(finalize.block.number)).toBe(100);
  const blockIdx = events.findIndex((e) => e.type === 'block');
  const finalizeIdx = events.findIndex((e) => e.type === 'finalize');
  expect(finalizeIdx).toBeGreaterThan(blockIdx); // block N precedes finalize N
});

// ─────────────────────────────── redelivery watchdog env knob (delta review) ───────────────────────────────

test('resolveRedeliveryTimeoutMs: precedence — param wins, then env, then default; garbage env fails loud (delta review)', () => {
  // The 300_000ms (5 min) default is a deliberate availability/diagnosability trade; PORTAL_STREAM_REDELIVERY_
  // TIMEOUT_MS makes it a conscious production knob. Pure over its args so no process.env mutation is needed.

  // param (tests) wins over both env and default, even a valid env
  expect(resolveRedeliveryTimeoutMs(50, '120000', 300_000)).toBe(50);
  expect(resolveRedeliveryTimeoutMs(0, '120000', 300_000)).toBe(0); // explicit 0 is honored (test injection)

  // env used when no param — a valid positive integer
  expect(resolveRedeliveryTimeoutMs(undefined, '120000', 300_000)).toBe(
    120_000,
  );

  // unset env → the default
  expect(resolveRedeliveryTimeoutMs(undefined, undefined, 300_000)).toBe(
    300_000,
  );

  // garbage / non-positive env → LOUD, not silently ignored (a silently-dropped knob is an operator trap)
  for (const bad of ['abc', '12.5', '0', '-5', '', '  ', 'NaN', 'Infinity']) {
    expect(() => resolveRedeliveryTimeoutMs(undefined, bad, 300_000)).toThrow(
      /PORTAL_STREAM_REDELIVERY_TIMEOUT_MS must be a positive integer/i,
    );
  }
});

test('resolveStreamIdleMs: precedence — param wins, then env, then default; garbage env fails loud (RT-1 SC1)', () => {
  // The 120_000ms (2 min) default is the RT-G11 idle bound; PORTAL_STREAM_IDLE_MS makes it a production
  // knob. Validation is IDENTICAL to resolveRedeliveryTimeoutMs (positive-integer, else loud) — the SC1
  // spec requires mirroring it exactly. Pure over its args, so no process.env mutation is needed.

  // param (tests) wins over both env and default, even a valid env
  expect(resolveStreamIdleMs(50, '90000', 120_000)).toBe(50);
  expect(resolveStreamIdleMs(0, '90000', 120_000)).toBe(0); // explicit 0 is honored (test injection)

  // env used when no param — a valid positive integer
  expect(resolveStreamIdleMs(undefined, '90000', 120_000)).toBe(90_000);

  // unset env → the default
  expect(resolveStreamIdleMs(undefined, undefined, 120_000)).toBe(120_000);

  // garbage / non-positive env → LOUD, not silently ignored (a silently-dropped knob is an operator trap)
  for (const bad of ['abc', '12.5', '0', '-5', '', '  ', 'NaN', 'Infinity']) {
    expect(() => resolveStreamIdleMs(undefined, bad, 120_000)).toThrow(
      /PORTAL_STREAM_IDLE_MS must be a positive integer/i,
    );
  }
});

// ─────────────────────────────── 1-block orphan heal via 409 (issue #33) ───────────────────────────────

// mock the Portal /stream where a connection can be either a block run (200) or a 409 fork negotiation
// carrying { previousBlocks }. /finalized-head returns a bare number. Captures each /stream body.
function mockPortalConnsFork(
  connections: Array<
    | { status: 200; blocks: any[] }
    | { status: 409; previousBlocks: Array<{ number: number; hash: string }> }
  >,
  finalizedHead: number,
  seenBodies?: any[],
) {
  const enc = new TextEncoder();
  let conn = 0;
  return (async (url: string, init?: any) => {
    if (url.endsWith('/finalized-head'))
      return { json: async () => ({ number: finalizedHead }) };
    if (seenBodies && init?.body) seenBodies.push(JSON.parse(init.body));
    if (conn >= connections.length)
      return { status: 204, ok: false, body: null };
    const c = connections[conn++]!;
    if (c.status === 409) {
      const body = new ReadableStream({
        start(ctrl) {
          ctrl.enqueue(
            enc.encode(JSON.stringify({ previousBlocks: c.previousBlocks })),
          );
          ctrl.close();
        },
      });

      return { status: 409, ok: false, body };
    }
    const lines = c.blocks.map((b) => `${JSON.stringify(b)}\n`).join('');
    const body = new ReadableStream({
      start(ctrl) {
        ctrl.enqueue(enc.encode(lines));
        ctrl.close();
      },
    });

    return { status: 200, ok: true, body };
  }) as any;
}

test('getPortalRealtimeEventGenerator: a 1-block orphan at tip HEALS via 409 fork negotiation — the wire emits reorg→block→block with hex LightBlocks and the #26 child-prune fires on the reorg (issue #33 T5)', async () => {
  // End-to-end through the wire, combining the #26 same-block redelivery handshake with the #33 409 heal.
  // conn1 serves canonical 674 (no factory logs) then the ORPHAN 675 (a non-canonical sibling) which
  // CREATES factory child Y — so the wire suppresses 675o for its same-block redelivery and re-opens FROM
  // 675 (parentBlockHash = ring[674] = h674). conn2 re-delivers the complete 675o (child Y present) →
  // forwarded; the connection closes → resume at 676 carrying parentBlockHash = h675o. conn3 is the 409:
  // the Portal saw h675o orphaned and returns the canonical replacement chain [674, 675]. The wire rewinds
  // to 675 (just above the matched common ancestor 674) carrying parentBlockHash = h674; conn4 serves the
  // canonical 675c then 676c. reconcile surfaces 675c as a reorg off 674 (popping the orphan 675o) then
  // appends 675c, 676c. The #26 prune fires on that reorg: child Y (creation block 675, reorged away) is
  // deleted from the running childAddresses map.
  process.env.PORTAL_REALTIME = 'stream';
  const factory = eulerFactory();
  const childY = '0x2222222222222222222222222222222222222222';
  const conns = [
    {
      status: 200 as const,
      blocks: [
        {
          header: {
            number: 674,
            hash: 'h674',
            parentHash: 'h673',
            timestamp: 674,
          },
          logs: [],
        },
        {
          // the ORPHAN 675 — a non-canonical sibling of canonical 675; it creates child Y (reorged away)
          header: {
            number: 675,
            hash: 'h675o',
            parentHash: 'h674',
            timestamp: 675,
          },
          logs: [proxyLog(childY, { blockNumber: 675 })],
        },
      ],
    },
    {
      // the same-block redelivery of 675o (re-opened FROM 675, parentBlockHash = h674) — child Y present
      status: 200 as const,
      blocks: [
        {
          header: {
            number: 675,
            hash: 'h675o',
            parentHash: 'h674',
            timestamp: 675,
          },
          logs: [proxyLog(childY, { blockNumber: 675 })],
        },
      ],
    },
    {
      // the wire resumed at 676 with parentBlockHash = h675o → the Portal detects the orphan and 409s
      status: 409 as const,
      previousBlocks: [
        { number: 674, hash: 'h674' },
        { number: 675, hash: 'h675c' },
      ],
    },
    {
      // rewound to 675 with parentBlockHash = h674 → canonical 675c then 676c
      status: 200 as const,
      blocks: [
        {
          header: {
            number: 675,
            hash: 'h675c',
            parentHash: 'h674',
            timestamp: 675,
          },
          logs: [],
        },
      ],
    },
    {
      // The reorg prunes child Y and rebuilds the server filter (revision bump), so streamHotBlocks
      // re-opens from 675 (parentBlockHash = h674 again). This connection re-serves the canonical 675c (a
      // routine duplicate — skipped) then 676c, which appends and hits endBlock.
      status: 200 as const,
      blocks: [
        {
          header: {
            number: 675,
            hash: 'h675c',
            parentHash: 'h674',
            timestamp: 675,
          },
          logs: [],
        },
        {
          header: {
            number: 676,
            hash: 'h676c',
            parentHash: 'h675c',
            timestamp: 676,
          },
          logs: [],
        },
      ],
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
        number: '0x2a1', // 673 — startup finalized (the anchor that seeds the ring)
        hash: 'h673',
        parentHash: 'h672',
        timestamp: '0x1',
      } as any,
      end: { number: '0x2a4' } as any, // endBlock 676: stop after the healed tip is indexed
    },
    childAddresses,
    fetchImpl: mockPortalConnsFork(conns, 673, bodies), // finalized head stays at the anchor
    redeliveryTimeoutMs: 500, // bound the handshake await so a construction bug fails fast, not hangs
  })) {
    events.push(event);
  }

  // The healed event sequence reaching ponder includes: block 674, block 675(orphan), reorg→674, block
  // 675c, block 676c (a `finalize` may or may not interleave — the head sits at the anchor 673 here).
  const seq = events.map((e) =>
    e.type === 'block'
      ? `block ${hexToNumber(e.block.number)}:${e.block.hash}`
      : e.type === 'reorg'
        ? `reorg ${hexToNumber(e.block.number)}`
        : `finalize ${hexToNumber(e.block.number)}`,
  );
  expect(seq).toContain('block 674:h674');
  expect(seq).toContain('block 675:h675o'); // the orphan reached ponder (via the #26 redelivery)
  expect(seq).toContain('reorg 674'); // rollback to the common ancestor 674 (hex LightBlock)
  expect(seq).toContain('block 675:h675c'); // canonical 675 re-delivered post-409
  expect(seq).toContain('block 676:h676c');
  // ordering: the reorg precedes the canonical 675c which precedes 676c
  const iReorg = seq.indexOf('reorg 674');
  const i675c = seq.indexOf('block 675:h675c');
  const i676c = seq.indexOf('block 676:h676c');
  expect(iReorg).toBeGreaterThanOrEqual(0);
  expect(iReorg).toBeLessThan(i675c);
  expect(i675c).toBeLessThan(i676c);

  // the reorg carries a hex LightBlock for the common ancestor, and the orphan among the reorged blocks
  const reorgEv = events.find((e) => e.type === 'reorg');
  expect(reorgEv.block.number).toBe('0x2a2'); // 674 in hex
  expect(reorgEv.reorgedBlocks.some((b: any) => b.hash === 'h675o')).toBe(true);

  // #26 child-prune: child Y (created on the reorged-away orphan 675) is pruned from the running map
  expect(childAddresses.get('euler-factory')!.has(childY as Address)).toBe(
    false,
  );

  // wire-side proof the /stream carried the fork-negotiation parentBlockHash: the first request carried the
  // anchor hash; the resume that triggered the 409 sent the orphan's hash; the rewound reconnect sent the
  // confirmed common-ancestor hash
  const stream = bodies.filter((b) => b.fromBlock !== undefined);
  expect(stream[0].parentBlockHash).toBe('h673'); // first request carries the anchor hash
  const resume676 = stream.find((b) => b.fromBlock === 676);
  expect(resume676?.parentBlockHash).toBe('h675o');
  const rewind675 = stream.find(
    (b) => b.fromBlock === 675 && b.parentBlockHash === 'h674',
  );
  expect(rewind675).toBeDefined(); // the rewound reconnect after the 409 carried the ancestor hash
  // F3: in armed (production) mode EVERY /stream request must carry a parentBlockHash — a later connection
  // silently going number-only (dropping the key) would re-open the fork-negotiation hole this fix closes,
  // and a find()-based spot check would miss it. Assert the invariant over ALL captured stream bodies.
  expect(stream.length).toBeGreaterThan(0);
  for (const b of stream) {
    expect(typeof b.parentBlockHash).toBe('string');
    expect(b.parentBlockHash.length).toBeGreaterThan(0);
  }
});
