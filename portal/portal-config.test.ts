import { expect, test } from 'vitest';
import { loadPortalConfig, PortalConfigError } from './portal-config.js';

// INV-14: all PORTAL_* env is parsed ONCE into a frozen, validated config; garbage fails fast.

test('INV-14: empty env → documented defaults, frozen', () => {
  const c = loadPortalConfig({});
  expect(c.chunkBlocks).toBe(500_000);
  expect(c.readahead).toBe(6);
  expect(c.bufferSize).toBe(100);
  expect(c.discoveryWindows).toBe(8);
  expect(c.minConcurrency).toBe(8);
  expect(c.maxConcurrency).toBe(48);
  expect(c.startConcurrency).toBe(16);
  expect(c.maxRowsInMem).toBe(250_000);
  expect(c.traceChunkBlocks).toBe(2_000);
  expect(c.chunkFixed).toBe(false);
  expect(c.gateLog).toBe(false);
  expect(c.checks).toBe('on');
  expect(c.apiKey).toBeUndefined();
  expect(c.finalizedHead).toBeUndefined();
  expect(c.metricsFile).toBeUndefined();
  expect(Object.isFrozen(c)).toBe(true);
});

test('INV-14: valid overrides parse', () => {
  const c = loadPortalConfig({
    PORTAL_CHUNK_BLOCKS: '250000',
    PORTAL_READAHEAD: '3',
    PORTAL_API_KEY: 'k',
    PORTAL_FINALIZED_HEAD: '123',
    PORTAL_CHUNK_FIXED: '1',
    PORTAL_CHECKS: 'strict',
    PORTAL_METRICS_FILE: '/tmp/m',
    PORTAL_REALTIME: 'stream',
    PORTAL_GATE_LOG: '1',
  });
  expect(c.chunkBlocks).toBe(250_000);
  expect(c.readahead).toBe(3);
  expect(c.apiKey).toBe('k');
  expect(c.finalizedHead).toBe(123);
  expect(c.chunkFixed).toBe(true);
  expect(c.checks).toBe('strict');
  expect(c.metricsFile).toBe('/tmp/m');
  expect(c.realtime).toBe('stream');
  expect(c.gateLog).toBe(true);
});

test('INV-14: garbage numeric → loud PortalConfigError (not silent NaN)', () => {
  expect(() => loadPortalConfig({ PORTAL_CHUNK_BLOCKS: 'abc' })).toThrow(
    PortalConfigError,
  );
  expect(() => loadPortalConfig({ PORTAL_CHUNK_BLOCKS: 'abc' })).toThrow(
    /PORTAL_CHUNK_BLOCKS/,
  );
  expect(() => loadPortalConfig({ PORTAL_READAHEAD: '1.5' })).toThrow(
    /integer/,
  );
  expect(() => loadPortalConfig({ PORTAL_FINALIZED_HEAD: 'xyz' })).toThrow(
    PortalConfigError,
  );
});

test('INV-14: out-of-range → loud', () => {
  expect(() => loadPortalConfig({ PORTAL_CHUNK_BLOCKS: '0' })).toThrow(/range/);
  expect(() => loadPortalConfig({ PORTAL_MIN_CONCURRENCY: '0' })).toThrow(
    /range/,
  );
  // max must be ≥ min
  expect(() =>
    loadPortalConfig({
      PORTAL_MIN_CONCURRENCY: '20',
      PORTAL_MAX_CONCURRENCY: '10',
    }),
  ).toThrow(/range/);
});

test('INV-14: PORTAL_CHECKS must be off|on|strict', () => {
  expect(loadPortalConfig({ PORTAL_CHECKS: 'off' }).checks).toBe('off');
  expect(() => loadPortalConfig({ PORTAL_CHECKS: 'loud' })).toThrow(
    /off\|on\|strict/,
  );
});

test('INV-14: PORTAL_FINALIZED_HEAD=0 is honoured (truthiness edge), empty string → unset', () => {
  expect(loadPortalConfig({ PORTAL_FINALIZED_HEAD: '0' }).finalizedHead).toBe(
    0,
  );
  expect(
    loadPortalConfig({ PORTAL_FINALIZED_HEAD: '' }).finalizedHead,
  ).toBeUndefined();
});
