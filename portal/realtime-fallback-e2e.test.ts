import { hexToBytes, keccak256 } from 'viem';
import { expect, test, vi } from 'vitest';
import { zeroLogsBloom } from '@/sync-realtime/bloom.js';
import { createRealtimeSync } from '@/sync-realtime/index.js';

// End-to-end regression for issue #145 (the runtime half of the issue #23 fix). The helper-level
// test (`realtime-getlogs-fallback.test.ts`) proves the extracted units in isolation; this file
// proves the WIRING inside `createRealtimeSync`'s private `fetchBlockEventData` closure — reached
// only by driving the exposed `sync(headBlock, blockCallback)` async generator over one new head
// block. It pins two runtime behaviours that no helper-level assertion can reach:
//
//   (A) the `logsPromise.catch` actually fires on a `ResponseBodyTooLargeError` thrown by the
//       UNFILTERED full-block `eth_getLogs`, recovers via the FILTERED per-filter fallback, and the
//       block is ingested (a `block` event is yielded) with the assembled logs — no crash.
//   (B) the `usedFilteredLogsFallback === false || logs.length > 0` guard makes `validateLogsAndBlock`
//       SKIP happen ONLY on the empty-fallback path: a block with a non-empty `logsBloom` whose
//       filtered fallback returns zero logs is still ingested instead of throwing "logs array has
//       length 0" (the invariant that holds only for the unfiltered fetch).
//
// Both assertions FAIL against unfixed upstream ponder (no fallback, no guard) — see the M1/M2
// mutation-verification recorded on PR #145.

// viem's real `ResponseBodyTooLargeError` reproduced by shape (matched by `name`, not `instanceof`).
const responseBodyTooLargeError = (): Error => {
  const error = new Error('HTTP response body exceeded the size limit.');
  error.name = 'ResponseBodyTooLargeError';

  return error;
};

const TRANSFER =
  '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const ADDR_A = '0x000000000000000000000000000000000000000a';

const FINALIZED_HASH = '0xf1na112ed';
const HEAD_HASH = '0xhead';

// A raw `SyncLog` shaped just enough to pass `standardizeLogs` and the per-log consistency checks in
// `validateLogsAndBlock` (blockHash/blockNumber must agree with the head block; transactionHash must
// resolve to a tx carried by the block). `topics[0]` is the event signature the filter matches on.
const rawLog = (over: { logIndex: string }): any => ({
  address: ADDR_A,
  blockHash: HEAD_HASH,
  blockNumber: '0x2',
  data: '0x',
  logIndex: over.logIndex,
  topics: [TRANSFER],
  transactionHash: '0xtx',
  transactionIndex: '0x0',
  removed: false,
});

// One tx so a matched log's `transactionHash`/`transactionIndex` resolves inside the block during
// full validation (`validateLogsAndBlock` looks the tx up by index).
const HEAD_TX = {
  hash: '0xtx',
  transactionIndex: '0x0',
  blockHash: HEAD_HASH,
  blockNumber: '0x2',
  from: '0xfrom',
  to: '0xto',
  input: '0x',
  value: '0x0',
  nonce: '0x0',
  r: '0x0',
  s: '0x0',
  v: '0x0',
  gas: '0x0',
  type: '0x0',
};

// A full `SyncBlock` for head block #2 (child of finalized #1). Passing it with `transactions !==
// undefined` makes `fetchBlockEventData` treat it as already-fetched (`ethGetBlockMethod =
// "eth_getBlockByNumber"`), so ONLY the logs leg hits `args.rpc` — keeping the mock surface to
// `eth_getLogs` alone. `logsBloom` decides `shouldRequestLogs` and the empty-fallback hazard.
const headBlock = (over: { logsBloom: string }): any => ({
  hash: HEAD_HASH,
  parentHash: FINALIZED_HASH,
  number: '0x2',
  timestamp: '0x2',
  logsBloom: over.logsBloom,
  transactions: [HEAD_TX],
});

const finalizedBlock = {
  hash: FINALIZED_HASH,
  parentHash: '0xf1na112ed_parent',
  number: '0x1',
  timestamp: '0x1',
};

// A bloom that contains `topic0` — the exact inverse of `sync-realtime/bloom.ts:isInBloom`: set the
// three bits `keccak256(topic0)` selects. Case (B) needs a NON-EMPTY bloom that still makes
// `shouldRequestLogs` true (the filter's topic0 is in the bloom), so the empty-fallback SKIP is
// load-bearing — validateLogsAndBlock would otherwise throw on 0 logs vs this non-empty bloom.
const bloomWithTopic = (topic: `0x${string}`): string => {
  const bloom = new Uint8Array(256);
  const hash = hexToBytes(keccak256(topic));
  for (const i of [0, 2, 4]) {
    const bit = ((hash[i]! << 8) + hash[i + 1]!) & 0x7ff;
    const byteIndex = 256 - 1 - Math.floor(bit / 8);
    bloom[byteIndex]! |= 1 << (bit % 8);
  }

  let hex = '0x';
  for (const byte of bloom) {
    hex += byte.toString(16).padStart(2, '0');
  }

  return hex;
};

// A logger whose `child` returns itself and whose level methods are inert — enough for
// `createRealtimeSync`/`fetchBlockEventData` to log without a real logging backend.
const makeLogger = (): any => {
  const logger: any = {
    child: () => logger,
    trace: () => {},
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
    fatal: () => {},
  };

  return logger;
};

const makeCommon = (): any => ({
  logger: makeLogger(),
  shutdown: { isKilled: false },
});

const chain: any = {
  name: 'test',
  id: 1,
  rpc: 'http://localhost',
  ws: undefined,
  pollingInterval: 1_000,
  // High enough that ingesting head #2 never crosses the 2 * finalityBlockCount finality window, so
  // `sync` yields only the `block` event we assert on (no trailing `finalize`).
  finalityBlockCount: 1_000_000,
  disableCache: false,
  ethGetLogsBlockRange: undefined,
  viemChain: undefined,
};

const logFilter: any = {
  type: 'log',
  chainId: 1,
  sourceId: 's',
  address: undefined,
  topic0: TRANSFER,
  topic1: null,
  topic2: null,
  topic3: null,
  fromBlock: undefined,
  toBlock: undefined,
  hasTransactionReceipt: false,
  include: [],
};

const eventCallback: any = {
  filter: logFilter,
  name: 'test:Transfer',
  fn: () => {},
  chain,
  type: 'contract',
};

// Route `args.rpc.request` by method + whether the `eth_getLogs` call is the UNFILTERED full-block
// fetch (params[0] has NO `topics`) or a FILTERED fallback fetch (params[0] HAS `topics`). The
// unfiltered fetch always throws the oversized-body error; `filteredLogs` decides what the fallback
// returns. Any block fetch (never reached here — head block is passed pre-fetched) returns the head.
const makeRpc = (filteredLogs: any[]): any => {
  const request = vi.fn(async ({ method, params }: any) => {
    if (method === 'eth_getLogs') {
      const hasTopics = params[0].topics !== undefined;
      if (hasTopics === false) {
        throw responseBodyTooLargeError();
      }

      return filteredLogs;
    }

    if (method === 'eth_getBlockByHash' || method === 'eth_getBlockByNumber') {
      return headBlock({ logsBloom: zeroLogsBloom });
    }

    throw new Error(`unexpected rpc method in test: ${method}`);
  });

  return { request };
};

// Drive `sync` over the head block and collect every event it yields.
const driveSync = async (realtimeSync: any, block: any): Promise<any[]> => {
  const events: any[] = [];
  for await (const event of realtimeSync.sync(block, undefined)) {
    events.push(event);
  }

  return events;
};

const createSync = (rpc: any) =>
  createRealtimeSync({
    common: makeCommon(),
    chain,
    rpc,
    eventCallbacks: [eventCallback],
    syncProgress: { finalized: finalizedBlock as any },
    childAddresses: new Map([['s', new Map()]]) as any,
  } as any);

// ─────────────────────── (A) non-empty fallback recovers and ingests ───────────────────────

test('createRealtimeSync recovers from an oversized full-block eth_getLogs via the filtered fallback and ingests the block (#145)', async () => {
  // The unfiltered full-block fetch throws ResponseBodyTooLargeError; the filtered fallback returns
  // the block's matching Transfer log. `zeroLogsBloom` forces `shouldRequestLogs` on the happy path.
  const filteredLog = rawLog({ logIndex: '0x0' });
  const rpc = makeRpc([filteredLog]);
  const realtimeSync = createSync(rpc);

  const events = await driveSync(
    realtimeSync,
    headBlock({ logsBloom: zeroLogsBloom }),
  );

  const blockEvent = events.find((e) => e.type === 'block');

  // The block was ingested (fallback path taken, validateLogsAndBlock ran and passed): without the
  // `.catch` fallback the oversized error propagates, `sync` yields nothing, and this is undefined.
  expect(blockEvent).toBeDefined();
  expect(blockEvent.block.hash).toBe(HEAD_HASH);

  // The assembled log from the FILTERED fallback survived downstream filter-matching.
  expect(blockEvent.logs).toHaveLength(1);
  expect(blockEvent.logs[0].logIndex).toBe('0x0');

  // The unfiltered fetch (no topics) was attempted, then the filtered fallback (with topics) fired.
  const logsCalls = rpc.request.mock.calls.filter(
    (c: any) => c[0].method === 'eth_getLogs',
  );
  const unfiltered = logsCalls.filter(
    (c: any) => c[0].params[0].topics === undefined,
  );
  const filtered = logsCalls.filter(
    (c: any) => c[0].params[0].topics !== undefined,
  );
  expect(unfiltered).toHaveLength(1);
  expect(filtered.length).toBeGreaterThanOrEqual(1);
});

// ─────────────────────── (B) empty fallback skips validateLogsAndBlock ───────────────────────

test('createRealtimeSync skips validateLogsAndBlock on an EMPTY filtered fallback over a non-empty logsBloom block (#145)', async () => {
  // A "monster" block: non-empty logsBloom (containing the filter's topic0, so `shouldRequestLogs`
  // is true), but the filtered fallback legitimately matches ZERO of this app's filters/factories.
  const bloom = bloomWithTopic(TRANSFER);
  expect(bloom).not.toBe(zeroLogsBloom);

  const rpc = makeRpc([]);
  const realtimeSync = createSync(rpc);

  // Without the `usedFilteredLogsFallback === false || logs.length > 0` guard, validateLogsAndBlock
  // runs on 0 logs vs this non-empty bloom and throws "logs array has length 0" — `sync` then swallows
  // it via onError and yields no block event. With the guard, the block is ingested with no logs.
  const events = await driveSync(realtimeSync, headBlock({ logsBloom: bloom }));

  const blockEvent = events.find((e) => e.type === 'block');

  expect(blockEvent).toBeDefined();
  expect(blockEvent.block.hash).toBe(HEAD_HASH);
  expect(blockEvent.logs).toHaveLength(0);

  // The oversized unfiltered fetch was attempted and the empty filtered fallback fired.
  const logsCalls = rpc.request.mock.calls.filter(
    (c: any) => c[0].method === 'eth_getLogs',
  );
  const filtered = logsCalls.filter(
    (c: any) => c[0].params[0].topics !== undefined,
  );
  expect(filtered.length).toBeGreaterThanOrEqual(1);
});
