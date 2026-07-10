import { expect, test, vi } from 'vitest';
import {
  buildFilteredBlockLogRequests,
  eth_getFilteredBlockLogs,
  isResponseBodyTooLargeError,
  validateLogsAndBlock,
} from '@/rpc/actions.js';
import { shouldRetry } from '@/rpc/index.js';
import { zeroLogsBloom } from '@/sync-realtime/bloom.js';

// Regression for issue #23. In RPC-transport realtime, ponder's `fetchBlockEventData` fetches a
// block's logs with an UNFILTERED `eth_getLogs` pinned by `blockHash` (all logs in the block). On a
// "monster" block the response body exceeds viem's hardcoded ~10 MiB `readResponseBody` cap and viem
// throws `ResponseBodyTooLargeError`. Because the request is pinned by `blockHash`, every retry hits
// the same oversized body — the chain stalls then crash-loops at the tip (an `unhandledRejection`
// after N futile retries). The fork's wiring patch makes that error (1) non-retryable in
// `shouldRetry`, and (2) recoverable in the realtime block-data fetch via a bounded FILTERED
// per-filter/factory `eth_getLogs` fallback for the same block, assembled into the block's logs —
// identical in shape to what historical/backfill sync already fetches. No wrong data is ever stored.
//
// This file mutation-verifies the three exported units that make that work. Every assertion below
// FAILS against unfixed upstream ponder: `buildFilteredBlockLogRequests`, `eth_getFilteredBlockLogs`
// and `isResponseBodyTooLargeError` do not exist there, and upstream `shouldRetry` (a) is not
// exported and (b) returns `true` for a `ResponseBodyTooLargeError`.

// viem's real `ResponseBodyTooLargeError` (viem >=2.31) — reproduced by shape, since the class is
// absent from the older viem the fork test-builds against. The fix matches by `name`, not
// `instanceof`, precisely so it is robust across the floating `viem: ">=2"` core dependency.
const responseBodyTooLargeError = (): Error => {
  const error = new Error('HTTP response body exceeded the size limit.');
  error.name = 'ResponseBodyTooLargeError';

  return error;
};

// A raw `SyncLog` shaped just enough to pass `standardizeLogs` (address, blockHash, blockNumber,
// data, logIndex, topics all required). `topics[0]` is the event signature the filters/factories
// match on. Numbers stay small so no ceiling check fires.
const rawLog = (over: {
  logIndex: string;
  address: string;
  topic0: string;
  blockHash?: string;
}): any => ({
  address: over.address,
  blockHash: over.blockHash ?? '0xblock',
  blockNumber: '0x1',
  data: '0x',
  logIndex: over.logIndex,
  topics: [over.topic0],
  transactionHash: '0xtx',
  transactionIndex: '0x0',
  removed: false,
});

const TRANSFER =
  '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const PAIR_CREATED =
  '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9';
const SWAP =
  '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822';

const ADDR_A = '0x000000000000000000000000000000000000000a';
const ADDR_B = '0x000000000000000000000000000000000000000b';
const FACTORY_ADDR = '0x000000000000000000000000000000000000fac0';

const logFilter = (over: Record<string, unknown>): any => ({
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
  ...over,
});

const factory = (over: Record<string, unknown>): any => ({
  id: 'f',
  type: 'log',
  chainId: 1,
  sourceId: 's',
  address: FACTORY_ADDR,
  eventSelector: PAIR_CREATED,
  childAddressLocation: 'topic1',
  fromBlock: undefined,
  toBlock: undefined,
  ...over,
});

// ─────────────────────── isResponseBodyTooLargeError ───────────────────────

test('isResponseBodyTooLargeError matches the error by name and through its cause chain (#23)', () => {
  expect(isResponseBodyTooLargeError(responseBodyTooLargeError())).toBe(true);

  // viem may re-throw it directly, but a defensive walk of the `cause` chain also matches when a
  // wrapper (e.g. an HttpRequestError) carries it as `cause`.
  const wrapped = new Error('wrapped');
  wrapped.cause = responseBodyTooLargeError();
  expect(isResponseBodyTooLargeError(wrapped)).toBe(true);

  expect(isResponseBodyTooLargeError(new Error('something else'))).toBe(false);
  expect(isResponseBodyTooLargeError(undefined)).toBe(false);
});

// ─────────────────────── shouldRetry: oversized body is non-retryable ───────────────────────

// The mutation check for candidate fix #3: on unfixed upstream `shouldRetry` returns `true` for a
// `ResponseBodyTooLargeError`, so the request is retried RETRY_COUNT (9) times against the same
// oversized body — a guaranteed-futile stall. The fix returns `false` so it surfaces immediately.
test('shouldRetry(ResponseBodyTooLargeError) === false — never retry the same oversized body (#23)', () => {
  expect(shouldRetry(responseBodyTooLargeError())).toBe(false);

  // and it must not swallow ordinary retryable errors
  expect(shouldRetry(new Error('transient network blip'))).toBe(true);
});

// ─────────────────────── buildFilteredBlockLogRequests: the filtered coverage ───────────────────────

test('a static-address log filter → one {address, topic0} request (#23)', () => {
  const requests = buildFilteredBlockLogRequests({
    logFilters: [logFilter({ address: ADDR_A, topic0: TRANSFER })],
    factories: [],
  });

  expect(requests).toEqual([{ address: ADDR_A, topic0: TRANSFER }]);
});

test('a factory-ADDRESS log filter → wildcard address (child addresses are unknown pre-fetch) (#23)', () => {
  // filter.address is itself a Factory — its concrete child addresses are only discovered from this
  // block's own logs, so it can only be fetched wildcard-by-topic0 here and re-narrowed downstream.
  const requests = buildFilteredBlockLogRequests({
    logFilters: [logFilter({ address: factory({}), topic0: SWAP })],
    factories: [],
  });

  expect(requests).toEqual([{ address: undefined, topic0: SWAP }]);
});

test('registered factories contribute {factory.address, factory.eventSelector} (#23)', () => {
  const requests = buildFilteredBlockLogRequests({
    logFilters: [],
    factories: [factory({})],
  });

  expect(requests).toEqual([{ address: FACTORY_ADDR, topic0: PAIR_CREATED }]);
});

test('duplicate {address, topic0} across filters/factories are deduped (#23)', () => {
  const requests = buildFilteredBlockLogRequests({
    logFilters: [
      logFilter({ address: ADDR_A, topic0: TRANSFER }),
      logFilter({ address: ADDR_A, topic0: TRANSFER }),
    ],
    factories: [],
  });

  expect(requests).toHaveLength(1);
});

test('a full filter+factory set produces the union of distinct requests (#23)', () => {
  const requests = buildFilteredBlockLogRequests({
    logFilters: [
      logFilter({ address: ADDR_A, topic0: TRANSFER }),
      logFilter({ address: ADDR_B, topic0: SWAP }),
      logFilter({ address: factory({}), topic0: SWAP }), // wildcard
    ],
    factories: [factory({})],
  });

  expect(requests).toEqual(
    expect.arrayContaining([
      { address: ADDR_A, topic0: TRANSFER },
      { address: ADDR_B, topic0: SWAP },
      { address: undefined, topic0: SWAP },
      { address: FACTORY_ADDR, topic0: PAIR_CREATED },
    ]),
  );
  expect(requests).toHaveLength(4);
});

// ─────────────────────── eth_getFilteredBlockLogs: fetch, assemble, dedup, sort ───────────────────────

test('the fallback issues one filtered eth_getLogs per request and assembles/dedups/sorts (#23)', async () => {
  // Route the mock `rpc.request` by the request's `topics[0]`, returning that filter's logs for the
  // block. ADDR_A/TRANSFER at index 2, ADDR_B/SWAP at index 0 — the assembled result must be sorted
  // by logIndex regardless of fetch order.
  const rpc: any = {
    request: vi.fn(async ({ params }: any) => {
      const topic0 = params[0].topics[0];
      if (topic0 === TRANSFER) {
        return [rawLog({ logIndex: '0x2', address: ADDR_A, topic0: TRANSFER })];
      }
      if (topic0 === SWAP) {
        return [rawLog({ logIndex: '0x0', address: ADDR_B, topic0: SWAP })];
      }
      return [];
    }),
  };

  const logs = await eth_getFilteredBlockLogs(rpc, '0xblock', {
    logFilters: [
      logFilter({ address: ADDR_A, topic0: TRANSFER }),
      logFilter({ address: ADDR_B, topic0: SWAP }),
    ],
    factories: [],
  });

  // one call per distinct {address, topic0}, each pinned by blockHash + topics filter
  expect(rpc.request).toHaveBeenCalledTimes(2);
  for (const call of rpc.request.mock.calls) {
    expect(call[0].method).toBe('eth_getLogs');
    expect(call[0].params[0].blockHash).toBe('0xblock');
    expect(call[0].params[0].topics).toHaveLength(1);
  }

  // assembled, deduped, and sorted by logIndex
  expect(logs.map((l) => l.logIndex)).toEqual(['0x0', '0x2']);
  expect(logs.map((l) => l.address)).toEqual([ADDR_B, ADDR_A]);
});

test('the fallback dedups logs a filter and a factory both match on the same block (#23)', async () => {
  // The same physical log (blockHash+logIndex) can be returned by two overlapping requests. It must
  // appear exactly once in the assembled array.
  const shared = rawLog({ logIndex: '0x1', address: ADDR_A, topic0: TRANSFER });
  const rpc: any = {
    request: vi.fn(async () => [shared]),
  };

  const logs = await eth_getFilteredBlockLogs(rpc, '0xblock', {
    logFilters: [
      logFilter({ address: ADDR_A, topic0: TRANSFER }),
      logFilter({ address: ADDR_B, topic0: SWAP }),
    ],
    factories: [],
  });

  expect(rpc.request).toHaveBeenCalledTimes(2);
  expect(logs).toHaveLength(1);
  expect(logs[0]!.logIndex).toBe('0x1');
});

test('the fallback issues no request and returns [] when there are no filters/factories (#23)', async () => {
  const rpc: any = { request: vi.fn() };

  const logs = await eth_getFilteredBlockLogs(rpc, '0xblock', {
    logFilters: [],
    factories: [],
  });

  expect(rpc.request).not.toHaveBeenCalled();
  expect(logs).toEqual([]);
});

// ─────────────────────── the empty-fallback validation hazard the fix must guard ───────────────────────

// A "monster" block always has a non-empty `logsBloom`. The FILTERED fallback legitimately returns
// ZERO logs when no registered filter/factory matches such a block — but `validateLogsAndBlock`
// throws on `logs.length === 0` against a non-empty bloom (a valid invariant only for the UNFILTERED
// fetch). This test PINS that hazard: it is precisely why the realtime site skips this single check
// on the empty-fallback path (`usedFilteredLogsFallback === false || logs.length > 0`) — matching the
// `if (logs.length > 0)` guard historical filtered sync already uses. Without that guard the fix
// would merely trade the oversized-body crash for a validation crash. (issue #23)

const NON_EMPTY_BLOOM = `0x${'0'.repeat(511)}1`;

const block = (over: Record<string, unknown> = {}): any => ({
  hash: '0xblock',
  number: '0x1',
  logsBloom: NON_EMPTY_BLOOM,
  transactions: [],
  ...over,
});

const LOGS_REQUEST = {
  method: 'eth_getLogs',
  params: [{ blockHash: '0xblock' }],
} as any;
const BLOCK_REQUEST = {
  method: 'eth_getBlockByHash',
  params: ['0xblock', true],
} as any;

test('validateLogsAndBlock throws on 0 logs vs a non-empty logsBloom — the hazard the guard sidesteps (#23)', () => {
  expect(NON_EMPTY_BLOOM).not.toBe(zeroLogsBloom);
  expect(() =>
    validateLogsAndBlock([], block(), LOGS_REQUEST, BLOCK_REQUEST),
  ).toThrowError(/logs array has length 0/);
});

test('validateLogsAndBlock does NOT throw on 0 logs when the block bloom is empty (#23)', () => {
  expect(() =>
    validateLogsAndBlock(
      [],
      block({ logsBloom: zeroLogsBloom }),
      LOGS_REQUEST,
      BLOCK_REQUEST,
    ),
  ).not.toThrow();
});

test('validateLogsAndBlock still runs per-log consistency checks on non-empty fallback logs (#23)', () => {
  // A fallback that returns logs must still be validated: a log whose blockHash disagrees with the
  // block must throw. This is why the guard skips ONLY the length-0 case, never full validation.
  // A block carrying the log's transaction, so the consistent case passes the tx-index check too.
  const blockWithTx = block({
    transactions: [
      {
        hash: '0xtx',
        transactionIndex: '0x0',
        blockHash: '0xblock',
        blockNumber: '0x1',
      },
    ],
  });

  const goodLog = rawLog({
    logIndex: '0x0',
    address: ADDR_A,
    topic0: TRANSFER,
  });
  expect(() =>
    validateLogsAndBlock([goodLog], blockWithTx, LOGS_REQUEST, BLOCK_REQUEST),
  ).not.toThrow();

  const mismatchedLog = rawLog({
    logIndex: '0x0',
    address: ADDR_A,
    topic0: TRANSFER,
    blockHash: '0xdifferent',
  });
  expect(() =>
    validateLogsAndBlock(
      [mismatchedLog],
      blockWithTx,
      LOGS_REQUEST,
      BLOCK_REQUEST,
    ),
  ).toThrowError(/blockHash/);
});
