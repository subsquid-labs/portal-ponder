import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  advanceScenarioCursor,
  DEFAULT_SCENARIO,
  hashBlock,
  mergeScenario,
  normalizeScenario,
} from './mock-portal.mjs';

test('mergeScenario: deep-merges objects and replaces arrays', () => {
  const merged = mergeScenario(DEFAULT_SCENARIO, {
    genesis: { number: 500 },
    finalizedHeadSeq: [{ number: 500, hash: '0xabc' }],
    steps: [{ type: 'idle204', count: 2 }],
  });

  assert.equal(merged.genesis.number, 500);
  assert.equal(merged.genesis.timestamp, DEFAULT_SCENARIO.genesis.timestamp);
  assert.deepEqual(merged.finalizedHeadSeq, [{ number: 500, hash: '0xabc' }]);
  assert.deepEqual(merged.steps, [{ type: 'idle204', count: 2 }]);
});

test('hashBlock: deterministic 32-byte hex and branch-sensitive', () => {
  const a = hashBlock(101, 'main');
  const b = hashBlock(101, 'main');
  const fork = hashBlock(101, 'fork');

  assert.equal(a, b);
  assert.match(a, /^0x[0-9a-f]{64}$/);
  assert.notEqual(a, fork);
});

test('advanceScenarioCursor: advances only block-producing steps', () => {
  assert.equal(advanceScenarioCursor(100, { type: 'blocks', count: 3 }), 103);
  assert.equal(advanceScenarioCursor(100, { type: 'fork', count: 2 }), 102);
  assert.equal(advanceScenarioCursor(100, { type: 'idle204', count: 5 }), 100);
  assert.equal(
    advanceScenarioCursor(100, { type: 'childDiscovery', block: 160 }),
    160,
  );
});

test('normalizeScenario: fills deterministic genesis and finalized-head hashes', () => {
  const scenario = normalizeScenario({
    genesis: { number: 12 },
    finalizedHeadSeq: [{ number: 12 }],
    steps: [],
  });

  assert.equal(scenario.genesis.hash, hashBlock(12, 'main'));
  assert.equal(scenario.genesis.parentHash, hashBlock(11, 'main'));
  assert.equal(scenario.finalizedHeadSeq[0].hash, hashBlock(12, 'main'));
});
