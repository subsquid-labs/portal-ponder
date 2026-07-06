import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parsePrometheus, summarizeMetrics } from './metrics-parse.mjs';

// A small but representative /metrics body: two chains, one histogram _count, an error counter, the
// completion gauges, and the start/end timestamps. Includes # comment lines and a labelless series.
const BODY = `
# HELP ponder_sync_is_complete Whether the sync is complete
# TYPE ponder_sync_is_complete gauge
ponder_sync_is_complete{chain="polygon"} 1
ponder_sync_is_complete{chain="ethereum"} 1
# TYPE ponder_historical_start_timestamp_seconds gauge
ponder_historical_start_timestamp_seconds{chain="polygon"} 1000
ponder_historical_start_timestamp_seconds{chain="ethereum"} 1010
ponder_historical_end_timestamp_seconds{chain="polygon"} 1300
ponder_historical_end_timestamp_seconds{chain="ethereum"} 1500
ponder_historical_completed_blocks{chain="polygon"} 500
ponder_historical_completed_blocks{chain="ethereum"} 800
ponder_historical_total_blocks{chain="polygon"} 500
ponder_historical_total_blocks{chain="ethereum"} 800
ponder_rpc_request_duration_count{chain="polygon",method="eth_getBlockByNumber"} 4
ponder_rpc_request_duration_count{chain="polygon",method="eth_chainId"} 1
ponder_rpc_request_duration_count{chain="ethereum",method="eth_getBlockByNumber"} 4
ponder_rpc_request_error_total{chain="polygon",method="eth_getBlockByNumber"} 0
process_cpu_seconds_total 12.5
`;

test('parsePrometheus: parses labelled + labelless series, ignores comments', () => {
  const samples = parsePrometheus(BODY);
  const complete = samples.filter((s) => s.name === 'ponder_sync_is_complete');
  assert.equal(complete.length, 2);
  assert.equal(complete[0].labels.chain, 'polygon');
  assert.equal(complete[0].value, 1);

  const labelless = samples.find((s) => s.name === 'process_cpu_seconds_total');
  assert.ok(labelless, 'a labelless series is parsed');
  assert.equal(labelless.value, 12.5);
  assert.deepEqual(labelless.labels, {});
});

test('parsePrometheus: multi-label series keep every label', () => {
  const samples = parsePrometheus(BODY);
  const s = samples.find(
    (x) =>
      x.name === 'ponder_rpc_request_duration_count' &&
      x.labels.chain === 'polygon' &&
      x.labels.method === 'eth_chainId',
  );
  assert.ok(s, 'the {chain,method} series is found by both labels');
  assert.equal(s.value, 1);
});

test('summarizeMetrics: a clean end-capped run — all complete, wall time, zero errors', () => {
  const samples = parsePrometheus(BODY);
  const sum = summarizeMetrics(samples, ['polygon', 'ethereum']);

  assert.equal(sum.allComplete, true);
  assert.equal(
    sum.allBlocksComplete,
    true,
    'both chains have completedBlocks === totalBlocks (both gauges present)',
  );
  assert.equal(sum.historicalStart, 1000, 'min start across chains');
  assert.equal(sum.historicalEnd, 1500, 'max end across chains');
  assert.equal(sum.wallSeconds, 500, 'wall = maxEnd − minStart');

  // rpc: 4+1+4 = 9 requests, 0 errors
  assert.equal(sum.rpc.requests, 9);
  assert.equal(sum.rpc.errors, 0);

  const poly = sum.perChain.find((c) => c.chain === 'polygon');
  assert.equal(poly.complete, true);
  assert.equal(poly.completedBlocks, 500);
  assert.equal(poly.totalBlocks, 500);
  assert.equal(poly.startTs, 1000);
  assert.equal(poly.endTs, 1300);
});

test('summarizeMetrics: an incomplete chain makes allComplete false', () => {
  const samples = parsePrometheus(BODY);
  // ethereum is present & complete, but we expect a third chain that never reported → not complete
  const sum = summarizeMetrics(samples, ['polygon', 'ethereum', 'arbitrum']);
  assert.equal(sum.allComplete, false);

  const arb = sum.perChain.find((c) => c.chain === 'arbitrum');
  assert.equal(
    arb.complete,
    false,
    'a chain with no is_complete sample is not complete',
  );
  assert.equal(arb.completedBlocks, 0);
});

test('summarizeMetrics: is_complete=1 but completedBlocks < totalBlocks → allBlocksComplete false', () => {
  // ethereum's is_complete gauge flipped to 1, but the block counters did NOT fully drain
  // (700 < 800). The is_complete gauge alone says done; the stricter block gate must catch it.
  const partial = `
ponder_sync_is_complete{chain="ethereum"} 1
ponder_historical_start_timestamp_seconds{chain="ethereum"} 1010
ponder_historical_end_timestamp_seconds{chain="ethereum"} 1500
ponder_historical_completed_blocks{chain="ethereum"} 700
ponder_historical_total_blocks{chain="ethereum"} 800
`;
  const sum = summarizeMetrics(parsePrometheus(partial), ['ethereum']);
  assert.equal(sum.allComplete, true, 'the is_complete gauge is still 1');
  assert.equal(
    sum.allBlocksComplete,
    false,
    'completedBlocks < totalBlocks → not block-complete (the clean gate must fail on this)',
  );
});

test('summarizeMetrics: a missing block gauge → allBlocksComplete false (both must be present)', () => {
  // is_complete=1 and completedBlocks reported, but total_blocks is ABSENT: cannot prove drained.
  const missingTotal = `
ponder_sync_is_complete{chain="ethereum"} 1
ponder_historical_completed_blocks{chain="ethereum"} 800
`;
  const sum = summarizeMetrics(parsePrometheus(missingTotal), ['ethereum']);
  assert.equal(sum.allComplete, true);
  assert.equal(
    sum.allBlocksComplete,
    false,
    'an absent total_blocks gauge cannot prove the backfill drained',
  );
});

test('summarizeMetrics: a non-zero rpc error total surfaces (a bench red flag)', () => {
  const withError = `${BODY}ponder_rpc_request_error_total{chain="ethereum",method="eth_getBlockByNumber"} 3\n`;
  const sum = summarizeMetrics(parsePrometheus(withError), [
    'polygon',
    'ethereum',
  ]);
  assert.equal(
    sum.rpc.errors,
    3,
    'errors are summed across chains — a clean run needs 0',
  );
});

test('summarizeMetrics: expectedChains defaults to the chains that reported is_complete', () => {
  const sum = summarizeMetrics(parsePrometheus(BODY), null);
  assert.equal(sum.perChain.length, 2);
  assert.equal(sum.allComplete, true);
});
