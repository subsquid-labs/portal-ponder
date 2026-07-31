import { afterEach, beforeEach, describe, expect, test } from 'vitest';

import {
  emitFreshnessHook,
  type FreshnessHook,
  makeDurableCommitAck,
  setFreshnessHookCollector,
} from './portal-freshness-hooks.js';
import { streamHotBlocks } from './portal-realtime.js';

// ─── helpers ───

/** Build a mock 200 Response whose body is an NDJSON stream of `lines`. */
function ndjsonResponse(lines: string[]): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const line of lines) {
        controller.enqueue(encoder.encode(line + '\n'));
      }
      controller.close();
    },
  });

  return new Response(body, {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

/** Build a single-block Portal /stream batch line with `logCount` dummy logs. */
function makeBatch(num: number, logCount = 0): string {
  const logs = Array.from({ length: logCount }, (_, i) => ({
    logIndex: i,
    blockNumber: num,
  }));

  return JSON.stringify({
    header: {
      number: num,
      hash: `0x${num.toString(16).padStart(64, '0')}`,
      parentHash: `0x${(num - 1).toString(16).padStart(64, '0')}`,
      timestamp: 1700000000 + num,
    },
    logs,
    transactions: [],
  });
}

// ─── tests ───

describe('portal-freshness-hooks', () => {
  const prevFlag = process.env.PORTAL_FRESHNESS_HOOKS;
  let collected: FreshnessHook[];

  beforeEach(() => {
    collected = [];
    setFreshnessHookCollector((hook: FreshnessHook) => {
      collected.push(hook);
    });
  });

  afterEach(() => {
    setFreshnessHookCollector(undefined);
    if (prevFlag === undefined) {
      delete process.env.PORTAL_FRESHNESS_HOOKS;
    } else {
      process.env.PORTAL_FRESHNESS_HOOKS = prevFlag;
    }
  });

  test('PORTAL_FRESHNESS_HOOKS unset ⇒ zero hook output and byte-identical behavior', async () => {
    delete process.env.PORTAL_FRESHNESS_HOOKS;

    const batchLine = makeBatch(100, 3);
    let callCount = 0;
    const fetchImpl = async (): Promise<Response> => {
      callCount += 1;
      if (callCount === 1) return ndjsonResponse([batchLine]);

      return new Response(null, { status: 204 });
    };

    const controller = new AbortController();
    const it = streamHotBlocks({
      portalUrl: 'https://portal.test',
      headers: {},
      fromBlock: 100,
      logs: [],
      chainId: 1,
      fetchImpl: fetchImpl as typeof fetch,
      signal: controller.signal,
      tickSleepMs: 1,
      idleMs: 1000,
    });

    // Consume the single delivered block.
    const first = await it.next();
    expect(first.done).toBe(false);
    expect((first.value as { kind: string }).kind).toBe('block');
    expect((first.value as { header: { number: number } }).header.number).toBe(
      100,
    );

    // Flag unset ⇒ emitFreshnessHook returns before touching the collector — zero side effects.
    expect(collected).toEqual([]);

    controller.abort();

    const rest = await it.next();
    expect(rest.done).toBe(true);
  });

  test('PORTAL_FRESHNESS_HOOKS=1 ⇒ exactly ONE batch-recv hook with correct field values, emitted before the yield', async () => {
    process.env.PORTAL_FRESHNESS_HOOKS = '1';

    const batchLine = makeBatch(100, 3);
    let callCount = 0;
    const fetchImpl = async (): Promise<Response> => {
      callCount += 1;
      if (callCount === 1) return ndjsonResponse([batchLine]);

      return new Response(null, { status: 204 });
    };

    const controller = new AbortController();
    const it = streamHotBlocks({
      portalUrl: 'https://portal.test',
      headers: {},
      fromBlock: 100,
      logs: [],
      chainId: 1,
      fetchImpl: fetchImpl as typeof fetch,
      signal: controller.signal,
      tickSleepMs: 1,
      idleMs: 1000,
    });

    // The hook is emitted synchronously BEFORE the yield, so when next() resolves the
    // collector already holds the hook — proving emission precedes the yield.
    const first = await it.next();

    expect(first.done).toBe(false);
    expect((first.value as { kind: string }).kind).toBe('block');

    // Exactly ONE batch-recv hook, already collected (emitted before the yield).
    expect(collected).toHaveLength(1);

    const hook = collected[0]!;
    expect(hook.evt).toBe('batch-recv');
    expect(hook.chainId).toBe(1);
    expect(hook.from).toBe(100);
    expect(hook.to).toBe(100);
    expect(hook.batchId).toBe(0);
    expect(hook.batchSize).toBe(3);
    expect(typeof hook.mono).toBe('number');
    expect(typeof hook.wall).toBe('string');

    controller.abort();

    const rest = await it.next();
    expect(rest.done).toBe(true);
  });

  test('emitFreshnessHook: flag unset ⇒ collector never invoked', () => {
    delete process.env.PORTAL_FRESHNESS_HOOKS;

    emitFreshnessHook({
      evt: 'batch-recv',
      chainId: 1,
      from: 1,
      to: 1,
      batchId: 0,
      batchSize: 0,
      mono: performance.now(),
      wall: new Date().toISOString(),
    });

    expect(collected).toEqual([]);
  });

  test('makeDurableCommitAck: emits exactly ONE durable-commit-ack on true; 2nd invocation no-op; false → no emit; env-off → no emit; mono ≥ pre-invocation floor', () => {
    process.env.PORTAL_FRESHNESS_HOOKS = '1';

    // true path: one emit, payload exact
    const ack1 = makeDurableCommitAck({
      chainId: 1,
      from: 100,
      to: 100,
      batchId: 0,
      batchSize: 3,
    });
    const floor = performance.now();
    ack1(true);
    expect(collected).toHaveLength(1);

    const hook = collected[0]!;
    expect(hook.evt).toBe('durable-commit-ack');
    expect(hook.chainId).toBe(1);
    expect(hook.from).toBe(100);
    expect(hook.to).toBe(100);
    expect(hook.batchId).toBe(0);
    expect(hook.batchSize).toBe(3);
    expect(typeof hook.mono).toBe('number');
    expect(hook.mono).toBeGreaterThanOrEqual(floor);
    expect(typeof hook.wall).toBe('string');

    // 2nd invocation → no-op (idempotence)
    ack1(true);
    expect(collected).toHaveLength(1);

    // false → no emit (and consumes the callback)
    const ack2 = makeDurableCommitAck({
      chainId: 1,
      from: 101,
      to: 101,
      batchId: 1,
      batchSize: 0,
    });
    ack2(false);
    expect(collected).toHaveLength(1);

    // a subsequent true on the same (false-consumed) callback is still a no-op
    ack2(true);
    expect(collected).toHaveLength(1);

    // env-off → no emit (emitFreshnessHook gates before the collector)
    delete process.env.PORTAL_FRESHNESS_HOOKS;
    const ack3 = makeDurableCommitAck({
      chainId: 1,
      from: 102,
      to: 102,
      batchId: 2,
      batchSize: 1,
    });
    ack3(true);
    expect(collected).toHaveLength(1);
  });
});
