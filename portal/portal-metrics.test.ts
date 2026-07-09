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
  stats.logs = 100;
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
      inserted: { logs: 100, blocks: 40, txs: 40, receipts: 6, traces: 2 },
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

test('completion summary: fires exactly once with final totals', () => {
  const stats = createStats();
  stats.blocks = 40;
  stats.bytes = 2 * 1024 * 1024;
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
  expect(logs).toEqual([
    'Portal mainnet complete: blocks=40 mb_streamed=2.00 elapsed=2.0s avg_blocks_per_s=20.0',
  ]);
});
