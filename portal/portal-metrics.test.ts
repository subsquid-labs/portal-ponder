import { readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, test, vi } from 'vitest';
import type { Gate } from './portal-gate.js';
import {
  createCompletionSummary,
  createStats,
  startProgressLog,
  writeMetrics,
} from './portal-metrics.js';

afterEach(() => {
  vi.useRealTimers();
});

// GOLDEN: the metrics-file JSON shape is a FROZEN contract — the bench harness parses it field by
// field. Any rename/removal/addition must fail here first.
test('writeMetrics: the metrics file matches the documented shape field-for-field', () => {
  const stats = createStats();
  stats.dataChunks = 3;
  stats.extends = 1;
  stats.discChunks = 2;
  stats.http = 7;
  stats.bytes = 1234;
  stats.errors = 1;
  stats.retries = 1;
  stats.cacheHits = 5;
  stats.maxInflight = 4;
  stats.gateWaitMs = 10.4;
  stats.fetchMs = 20.6;
  stats.transformMs = 30.2;
  stats.logs = 100; // RAW streamed (a streaming/progress signal) — deliberately ≠ insertedLogs below
  stats.insertedLogs = 88; // #143: store-inserted (post-re-match) — this is what inserted.logs reports
  stats.blocks = 40;
  stats.txs = 40;
  stats.receipts = 6;
  stats.traces = 2;
  stats.rpcFallback = 1;
  const gate: Gate = {
    acquire: async () => {},
    release() {},
    onOk() {},
    onThrottle() {},
    addRows() {},
    freeRows() {},
    saturated: () => false,
    snapshot: () => ({ limit: 16, active: 3, rows: 42 }),
  };
  const file = join(tmpdir(), `portal-metrics-golden-${process.pid}`);
  try {
    writeMetrics({
      metricsFile: file,
      chain: { id: 1, name: 'mainnet' },
      stats,
      chunkBlocks: 500_000,
      portalHead: 21_000_000,
      gate,
      startTime: 0, // 0 → wallMs 0 (deterministic)
    });
    const parsed = JSON.parse(readFileSync(`${file}.1`, 'utf8'));
    expect(parsed).toEqual({
      chain: 'mainnet',
      chainId: 1,
      wallMs: 0,
      chunkBlocks: 500_000,
      portalFinalizedHead: 21_000_000,
      fetch: {
        dataChunks: 3,
        extends: 1,
        discChunks: 2,
        http: 7,
        bytes: 1234,
        errors: 1,
        retries: 1,
        cacheHits: 5,
        maxInflight: 4,
      },
      timing: { gateWaitMs: 10, fetchMs: 21, transformMs: 30 }, // Math.round of the cumulative ms
      portalGate: { limit: 16, active: 3, rows: 42 },
      inserted: { logs: 88, blocks: 40, txs: 40, receipts: 6, traces: 2 }, // #143: insertedLogs, not raw 100

      rpcFallbackIntervals: 1,
    });
  } finally {
    rmSync(`${file}.1`, { force: true });
  }
});

test('writeMetrics: unset metricsFile and an unknown head are handled (no file / null head)', () => {
  const gate: Gate = {
    acquire: async () => {},
    release() {},
    onOk() {},
    onThrottle() {},
    addRows() {},
    freeRows() {},
    saturated: () => false,
    snapshot: () => ({ limit: 16, active: 0, rows: 0 }),
  };
  // no metricsFile → no write, no throw
  writeMetrics({
    metricsFile: undefined,
    chain: { id: 1, name: 'm' },
    stats: createStats(),
    chunkBlocks: 1,
    portalHead: undefined,
    gate,
    startTime: 0,
  });

  const file = join(tmpdir(), `portal-metrics-nullhead-${process.pid}`);
  try {
    writeMetrics({
      metricsFile: file,
      chain: { id: 5, name: 'm' },
      stats: createStats(),
      chunkBlocks: 1,
      portalHead: undefined,
      gate,
      startTime: 0,
    });
    expect(
      JSON.parse(readFileSync(`${file}.5`, 'utf8')).portalFinalizedHead,
    ).toBeNull();
  } finally {
    rmSync(`${file}.5`, { force: true });
  }
});

test('progress ticker: emits on counter advance, then stays silent until the next advance', () => {
  vi.useFakeTimers();
  vi.setSystemTime(0);

  const stats = createStats();
  let through = -1;
  const logs: string[] = [];
  const stop = startProgressLog({
    chainName: 'mainnet',
    stats,
    intervalMs: 100,
    startTime: () => 0,
    discovery: () => ({ floor: 0, through }),
    logInfo: ({ msg }) => logs.push(msg),
  });

  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(0);

  stats.blocks = 10;
  stats.bytes = 1024 * 1024;
  stats.discChunks = 2;
  through = 99;
  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(1);
  expect(logs[0]).toContain('blocks_streamed=10');
  expect(logs[0]).toContain('mb_streamed=1.00');
  expect(logs[0]).toContain('blocks_per_s=50.0');
  expect(logs[0]).toContain('discChunks=2');
  expect(logs[0]).toContain('scanned=100');

  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(1);

  stats.bytes += 1024 * 1024;
  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(2);

  stop();
  stats.bytes += 1024 * 1024;
  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(2);
});

test('progress ticker: disabled interval and logger failures never throw', () => {
  vi.useFakeTimers();
  const disabledStats = createStats();
  const disabledLogs: string[] = [];
  const disabled = startProgressLog({
    chainName: 'mainnet',
    stats: disabledStats,
    intervalMs: 0,
    startTime: () => 0,
    discovery: () => ({ floor: -1, through: -1 }),
    logInfo: ({ msg }) => disabledLogs.push(msg),
  });
  disabledStats.bytes = 1;
  vi.advanceTimersByTime(1000);
  expect(disabledLogs).toHaveLength(0);
  expect(disabled()).toBeUndefined();

  const stats = createStats();
  const stop = startProgressLog({
    chainName: 'mainnet',
    stats,
    intervalMs: 100,
    startTime: () => 0,
    discovery: () => ({ floor: 0, through: 0 }),
    logInfo: () => {
      throw new Error('logger failed');
    },
  });
  stats.bytes = 1;
  expect(() => vi.advanceTimersByTime(100)).not.toThrow();
  stop();
});

test('completion summary: fires exactly once, reporting event counts and zero-fallback provenance', () => {
  const stats = createStats();
  stats.blocks = 40;
  stats.logs = 100; // RAW streamed — deliberately ≠ insertedLogs so the line's source is unambiguous
  stats.insertedLogs = 88; // #143: the completion line's `logs=` must report THIS (store-inserted)
  stats.txs = 55;
  stats.receipts = 6;
  stats.bytes = 2 * 1024 * 1024;
  stats.rpcFallback = 0;
  const logs: string[] = [];
  const complete = createCompletionSummary({
    chainName: 'mainnet',
    stats,
    startTime: () => 500,
    now: () => 2500,
    logInfo: ({ msg }) => logs.push(msg),
  });

  expect(complete()).toBe(true);
  expect(complete()).toBe(false);
  // The data-plane counts and the fallback counter are on the line. Each of these substrings is new
  // vs. the pre-change message (blocks=/logs=/txs=/receipts=/rpc_fallback=), so an old-format line
  // fails every one — the mutation red.
  expect(logs).toHaveLength(1);
  expect(logs[0]).toContain('blocks=40');
  // #143: `logs=` reports the store-inserted count (88), NOT the raw streamed 100 — a build that
  // still reads `stats.logs` here prints `logs=100` and fails this pair.
  expect(logs[0]).toContain('logs=88');
  expect(logs[0]).not.toContain('logs=100');
  expect(logs[0]).toContain('txs=55');
  expect(logs[0]).toContain('receipts=6');
  expect(logs[0]).toContain('mb_streamed=2.00');
  expect(logs[0]).toContain('elapsed=2.0s');
  expect(logs[0]).toContain('avg_blocks_per_s=20.0');
  expect(logs[0]).toContain('rpc_fallback=0');
  // rpcFallback === 0 ⟺ the whole history came from the Portal. The provenance clause must say so,
  // and MUST NOT claim any fallback happened.
  expect(logs[0]).toContain(
    'served entirely by the SQD Portal (0 JSON-RPC for history)',
  );
  expect(logs[0]).not.toContain('fell back');
});

test('completion summary: with rpcFallback > 0 the provenance flips to the fallback count, no zero-RPC claim', () => {
  const stats = createStats();
  stats.blocks = 40;
  stats.logs = 100;
  stats.rpcFallback = 3;
  const logs: string[] = [];
  const complete = createCompletionSummary({
    chainName: 'mainnet',
    stats,
    startTime: () => 500,
    now: () => 2500,
    logInfo: ({ msg }) => logs.push(msg),
  });

  expect(complete()).toBe(true);
  expect(logs).toHaveLength(1);
  expect(logs[0]).toContain('rpc_fallback=3');
  // The "0 JSON-RPC" provenance MUST NOT appear once any range fell back — the phrasing flips to the
  // actual count. A build that hard-codes the zero-RPC clause (or omits rpc_fallback) fails here.
  expect(logs[0]).toContain('3 block range(s) fell back to JSON-RPC');
  expect(logs[0]).not.toContain('0 JSON-RPC for history');
});

// The progress fingerprint MUST include discoveryScannedBlocks so the ticker advances during an empty
// window (a fresh dense source scans the range before the first block streams). This locks that
// behavior: with blocks/bytes/chunks all frozen at 0, advancing ONLY discovery.through must still fire
// the ticker. MUTATION: remove `discoveryScannedBlocks(discovery)` from `progressFingerprint` and this
// test fails (the fingerprint never changes, so `logs` stays empty).
test('progress ticker: fires on a scanned-only advance (blocks_streamed stays 0)', () => {
  vi.useFakeTimers();
  vi.setSystemTime(0);

  const stats = createStats();
  let through = -1;
  const logs: string[] = [];
  const stop = startProgressLog({
    chainName: 'mainnet',
    stats,
    intervalMs: 100,
    startTime: () => 0,
    discovery: () => ({ floor: 0, through }),
    logInfo: ({ msg }) => logs.push(msg),
  });

  // First tick: nothing has advanced yet.
  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(0);

  // Only the discovery scan advances — no streamed blocks, no bytes, no chunks. The ticker must fire
  // purely on discoveryScannedBlocks climbing.
  through = 4999;
  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(1);
  expect(logs[0]).toContain('blocks_streamed=0');
  expect(logs[0]).toContain('mb_streamed=0.00');
  expect(logs[0]).toContain('discChunks=0');
  expect(logs[0]).toContain('scanned=5000');

  // A further scanned-only advance fires again; a repeat with no advance stays silent.
  through = 9999;
  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(2);

  vi.advanceTimersByTime(100);
  expect(logs).toHaveLength(2);

  stop();
});
