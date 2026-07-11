import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  advanceScenarioCursor,
  cursorMatchesStep,
  DEFAULT_SCENARIO,
  encodeLog,
  eventTopic,
  gatePhaseForBlock,
  hashBlock,
  logsForBlock,
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

test('logsForBlock: filters scenario logs and strips fixture-only block fields', () => {
  const step = {
    logs: [
      { block: 101, address: '0x1', logIndex: 0 },
      { blockNumber: '0x66', address: '0x2', logIndex: 1 },
      { block: 103, address: '0x3', logIndex: 2 },
    ],
  };

  assert.deepEqual(logsForBlock(step, 102), [{ address: '0x2', logIndex: 1 }]);
});

test('gatePhaseForBlock: gates by killAt block and phase', () => {
  const step = { killAt: { block: 102, phase: 'K1-append' } };

  assert.equal(gatePhaseForBlock(step, 101), undefined);
  assert.equal(gatePhaseForBlock(step, 102), 'K1-append');
});

test('eventTopic and encodeLog: derive ABI-correct Euler topics', () => {
  assert.equal(
    eventTopic('ProxyCreated'),
    '0x04e664079117e113faa9684bc14aecb41651cbf098b14eda271248c6d0cda57c',
  );
  assert.equal(
    eventTopic('Deposit'),
    '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7',
  );

  const proxy = encodeLog(
    {
      event: 'ProxyCreated',
      proxy: '0x1111111111111111111111111111111111111111',
      transactionHash:
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    },
    101,
  );
  assert.equal(proxy.topics[0], eventTopic('ProxyCreated'));
  assert.equal(
    proxy.topics[1],
    '0x0000000000000000000000001111111111111111111111111111111111111111',
  );
  assert.match(proxy.data, /^0x[0-9a-f]+$/);

  const deposit = encodeLog(
    {
      event: 'Deposit',
      vault: '0x1111111111111111111111111111111111111111',
      sender: '0x2222222222222222222222222222222222222222',
      owner: '0x3333333333333333333333333333333333333333',
      assets: '123456789',
      shares: '987654321',
    },
    102,
  );
  assert.equal(deposit.topics[0], eventTopic('Deposit'));
  assert.equal(deposit.address, '0x1111111111111111111111111111111111111111');
  assert.equal(deposit.logIndex, 0);
});

test('cursorMatchesStep: routes redelivery and 409 steps by request cursor', () => {
  assert.equal(
    cursorMatchesStep(
      { type: 'awaitRedelivery', block: 101 },
      {
        fromBlock: 101,
        parentBlockHash: hashBlock(100, 'main'),
      },
    ),
    true,
  );
  assert.equal(
    cursorMatchesStep(
      { type: 'awaitRedelivery', block: 101 },
      {
        fromBlock: 102,
        parentBlockHash: hashBlock(101, 'main'),
      },
    ),
    false,
  );
  assert.equal(
    cursorMatchesStep(
      { type: 'status409', block: 105 },
      {
        fromBlock: 106,
        parentBlockHash: hashBlock(105, 'orphan'),
      },
    ),
    true,
  );
});
