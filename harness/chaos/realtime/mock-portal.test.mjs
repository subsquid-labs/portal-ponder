import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  advanceScenarioCursor,
  createRuntime,
  cursorMatchesStep,
  DEFAULT_SCENARIO,
  encodeLog,
  eventTopic,
  gatePhaseForBlock,
  hashBlock,
  logsForBlock,
  mergeScenario,
  normalizeScenario,
  retargetKillAtBlock,
} from './mock-portal.mjs';

// Minimal in-process response double: captures the ndjson stream and satisfies the surface
// handleRollbackApply touches (writeHead/write/end + the `once('close')` gate hook + the
// writableEnded/destroyed guards). Good enough to drive a gate-free rollbackApply to completion.
function fakeRes() {
  return {
    statusCode: undefined,
    chunks: [],
    writableEnded: false,
    destroyed: false,
    writeHead(code) {
      this.statusCode = code;
      return this;
    },
    write(chunk) {
      this.chunks.push(String(chunk));
      return true;
    },
    end(chunk) {
      if (chunk !== undefined) this.chunks.push(String(chunk));
      this.writableEnded = true;
    },
    once() {
      return this;
    },
  };
}

// Drive a single scenario step (no killAt gate) through the runtime's /stream handler and return
// the mock's reorgApplied counter after it completes.
async function reorgAppliedFor(step, body) {
  const runtime = createRuntime({
    chainId: 1,
    genesis: { number: 100 },
    finalizedHeadSeq: [{ number: 100 }],
    steps: [step],
  });
  const res = fakeRes();
  await runtime.stream(body, res);

  return runtime.stats().reorgApplied;
}

// Parse the block numbers a /stream connection emitted, in order, from the ndjson chunks a fakeRes
// captured. Each block-producing chunk is a JSON batch `{ header: { number, ... }, ... }`; the
// writeHead(200) call is not a chunk, so every captured chunk is one streamed block.
function streamedNumbers(res) {
  const numbers = [];
  for (const chunk of res.chunks) {
    const trimmed = chunk.trim();
    if (trimmed.length === 0) continue;

    const batch = JSON.parse(trimmed);
    numbers.push(batch.header.number);
  }

  return numbers;
}

// Assert a streamed sequence is strictly contiguous: [n, n+1, n+2, …] with no skip. This is the
// exact property the shared-cursor race violated (100→101→103, skipping 102). Reported with the
// full observed sequence so a failure shows the actual skip.
function assertContiguous(numbers, label) {
  for (let i = 1; i < numbers.length; i++) {
    assert.equal(
      numbers[i],
      numbers[i - 1] + 1,
      `${label}: streamed block ${numbers[i]} is not contiguous with ${
        numbers[i - 1]
      } (expected ${numbers[i - 1] + 1}); full sequence ${JSON.stringify(
        numbers,
      )}`,
    );
  }
}

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
  assert.equal(advanceScenarioCursor(100, { type: 'cutoverGate' }), 100);
  assert.equal(
    advanceScenarioCursor(100, { type: 'rollbackApply', count: 4 }),
    104,
  );
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

test('retargetKillAtBlock: retargets step-level and scenario-root gate blocks; ignores non-gate killAt and blank input', () => {
  // K2 shape: a step-level killAt on a blocks step is retargeted (varied mid-stream kill point).
  const stepScenario = retargetKillAtBlock(
    {
      steps: [
        {
          type: 'blocks',
          count: 12,
          killAt: { block: 107, phase: 'K2-midstream' },
        },
      ],
    },
    104,
  );
  assert.equal(stepScenario.steps[0].killAt.block, 104);

  // K6 shape: a scenario-root killAt with a phase is retargeted too.
  const rootScenario = retargetKillAtBlock(
    { killAt: { block: 105, phase: 'K6-cutover' }, steps: [] },
    109,
  );
  assert.equal(rootScenario.killAt.block, 109);

  // A killAt WITHOUT a phase (a plain step cursor field, e.g. childDiscovery) is left untouched.
  const noPhase = retargetKillAtBlock(
    { steps: [{ type: 'blocks', killAt: { block: 200 } }] },
    104,
  );
  assert.equal(noPhase.steps[0].killAt.block, 200);

  // Blank / undefined / NaN override is a no-op (empty env ⇒ scenario killAt unchanged).
  const untouched = retargetKillAtBlock(
    {
      steps: [
        { type: 'blocks', killAt: { block: 107, phase: 'K2-midstream' } },
      ],
    },
    '',
  );
  assert.equal(untouched.steps[0].killAt.block, 107);
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
  assert.equal(
    cursorMatchesStep(
      {
        type: 'cutoverGate',
        fromBlock: 106,
        parentBlockHash: hashBlock(105, 'main'),
      },
      {
        fromBlock: 106,
        parentBlockHash: hashBlock(105, 'main'),
      },
    ),
    true,
  );
  assert.equal(
    cursorMatchesStep(
      {
        type: 'cutoverGate',
        fromBlock: 106,
        parentBlockHash: hashBlock(105, 'main'),
      },
      {
        fromBlock: 108,
        parentBlockHash: hashBlock(107, 'main'),
      },
    ),
    false,
  );
});

test('cursorMatchesStep: rollbackApply fires only on the natural post-window resume cursor', () => {
  // K7: after streaming 101..106 on `main`, the client's natural next request is
  // fromBlock=107 with parentBlockHash = hashBlock(106, 'main'). The rollbackApply step's
  // `match` must fire there — and NOT on the earlier in-window cursors — so the reorg branch
  // is served on the resume request rather than mis-delivered mid-stream.
  const step = {
    type: 'rollbackApply',
    reorgBlock: 104,
    count: 3,
    branch: 'rollback',
    parentBranch: 'main',
    match: { fromBlock: 107, parentBlock: 106, parentBranch: 'main' },
  };

  assert.equal(
    cursorMatchesStep(step, {
      fromBlock: 107,
      parentBlockHash: hashBlock(106, 'main'),
    }),
    true,
  );
  assert.equal(
    cursorMatchesStep(step, {
      fromBlock: 106,
      parentBlockHash: hashBlock(105, 'main'),
    }),
    false,
  );
  assert.equal(
    cursorMatchesStep(step, {
      fromBlock: 107,
      parentBlockHash: hashBlock(106, 'rollback'),
    }),
    false,
  );
});

test('handleRollbackApply: reorgApplied counts ONLY a genuine below-tip cross-branch fork', async () => {
  // (a) The committed K7 shape: fork block 104 served below the tip the client requests
  // (fromBlock=107) and crossing branches (parent 103:main, self 104:rollback). This IS a reorg:
  // the mock rolls the tip back to 104, so the non-vacuity counter fires exactly once.
  const genuine = await reorgAppliedFor(
    {
      type: 'rollbackApply',
      reorgBlock: 104,
      count: 3,
      branch: 'rollback',
      parentBranch: 'main',
      match: { fromBlock: 107, parentBlock: 106, parentBranch: 'main' },
    },
    { fromBlock: 107, parentBlockHash: hashBlock(106, 'main') },
  );
  assert.equal(genuine, 1);
});

test('handleRollbackApply: an APPEND-shaped rollbackApply does NOT increment reorgApplied', async () => {
  // (b1) reorgBlock === fromBlock: the "fork" block is served AT the tip the client requests, so it
  // carries the chain forward (append) rather than rolling it back. Not a reorg ⇒ counter stays 0,
  // even though it still crosses branches (parent 106:main, self 107:rollback).
  const appendAtTip = await reorgAppliedFor(
    {
      type: 'rollbackApply',
      reorgBlock: 107,
      count: 3,
      branch: 'rollback',
      parentBranch: 'main',
      match: { fromBlock: 107, parentBlock: 106, parentBranch: 'main' },
    },
    { fromBlock: 107, parentBlockHash: hashBlock(106, 'main') },
  );
  assert.equal(appendAtTip, 0);

  // (b2) parentBranch === branch: a below-tip block whose parent is on its OWN branch is a plain
  // same-branch append with a different label, not a cross-branch fork ⇒ counter stays 0.
  const sameBranch = await reorgAppliedFor(
    {
      type: 'rollbackApply',
      reorgBlock: 104,
      count: 3,
      branch: 'rollback',
      parentBranch: 'rollback',
    },
    { fromBlock: 107, parentBlockHash: hashBlock(106, 'main') },
  );
  assert.equal(sameBranch, 0);
});

test('handleBlocks: two overlapping /stream connections each emit a contiguous sequence off their own fromBlock (K2 shared-cursor race guard)', async () => {
  // Regression for the K2 baseline (turnDelayMs:450) shared-cursor skip that PR #169 fixed. A slow
  // block step lets the client open an overlapping /stream (a legitimate reconnect on a
  // finalized-head advance). handleBlocks streams one block, then awaits streamTurnDelay — a real
  // await that yields the event loop, so two concurrent handleBlocks turns interleave through the
  // setTimeout queue. Under the OLD module-level `streamCursor`, the second connection wakes, reads
  // the first connection's advanced cursor, and skips a block (e.g. 101 → 103, dropping 102), which
  // the product rightly fatals on ("unknown parent"). The fix drives each turn off a per-connection
  // `localCursor` seeded from the request's fromBlock, so each connection is independent.
  //
  // Two connections from well-separated windows (101.. and 201..): under the shared cursor the
  // second stream's writes stomp the first's cursor across the delay, so at least one connection
  // becomes non-contiguous. Both windows must be strictly contiguous off their own fromBlock.
  const runtime = createRuntime({
    chainId: 1,
    genesis: { number: 100 },
    finalizedHeadSeq: [{ number: 100 }],
    // Two block steps so each concurrent /stream turn routes to handleBlocks. `blocks` is not a
    // cursor step, so both requests are served regardless of their fromBlock. turnDelayMs keeps the
    // per-block await long enough for the two turns to interleave on the shared timer queue.
    steps: [
      { type: 'blocks', count: 4, turnDelayMs: 20 },
      { type: 'blocks', count: 4, turnDelayMs: 20 },
    ],
  });

  const resA = fakeRes();
  const resB = fakeRes();

  // Kick both streams off concurrently and let their turns interleave through streamTurnDelay.
  await Promise.all([
    runtime.stream(
      { fromBlock: 101, parentBlockHash: hashBlock(100, 'main') },
      resA,
    ),
    runtime.stream(
      { fromBlock: 201, parentBlockHash: hashBlock(200, 'main') },
      resB,
    ),
  ]);

  const seqA = streamedNumbers(resA);
  const seqB = streamedNumbers(resB);

  // Sanity: both connections actually streamed their full window (guards against a vacuous pass
  // where a connection emitted nothing and "contiguity" held trivially).
  assert.deepEqual(seqA, [101, 102, 103, 104]);
  assert.deepEqual(seqB, [201, 202, 203, 204]);

  // The load-bearing property: each connection is strictly contiguous off its OWN fromBlock,
  // independent of how the two interleaved. This is what the shared cursor broke.
  assertContiguous(seqA, 'connection A (fromBlock=101)');
  assertContiguous(seqB, 'connection B (fromBlock=201)');
});

test('handleBlocks: a /stream reopened at a fresh fromBlock mid-flight starts at that fromBlock, not the shared cursor (K2 reconnect guard)', async () => {
  // The narrower K2 shape: a single in-flight stream is joined by a reconnect that requests a
  // DIFFERENT window while the first is still delivering. Under the shared cursor the reconnect
  // ignores its own fromBlock (it reads whatever the in-flight turn left in `streamCursor`), so its
  // first emitted block is wrong; the per-connection cursor makes it honor its fromBlock exactly.
  const runtime = createRuntime({
    chainId: 1,
    genesis: { number: 100 },
    finalizedHeadSeq: [{ number: 100 }],
    steps: [
      { type: 'blocks', count: 3, turnDelayMs: 20 },
      { type: 'blocks', count: 3, turnDelayMs: 20 },
    ],
  });

  const resInFlight = fakeRes();
  const inFlight = runtime.stream(
    { fromBlock: 110, parentBlockHash: hashBlock(109, 'main') },
    resInFlight,
  );

  // Let the in-flight stream write its first block and advance the shared cursor before the
  // reconnect arrives, so the shared-cursor bug (if present) would seed the reconnect off it.
  await new Promise((resolve) => setTimeout(resolve, 5));

  const resReconnect = fakeRes();
  const reconnect = runtime.stream(
    { fromBlock: 300, parentBlockHash: hashBlock(299, 'main') },
    resReconnect,
  );

  await Promise.all([inFlight, reconnect]);

  const reconnectSeq = streamedNumbers(resReconnect);

  // Sanity: the reconnect streamed its full window off its OWN fromBlock (guards against a vacuous
  // pass where it emitted nothing/too few and "contiguity" held trivially). Under the shared cursor
  // it would inherit the in-flight stream's position instead of honoring fromBlock 300.
  assert.deepEqual(reconnectSeq, [300, 301, 302]);

  // The load-bearing property: the reconnect begins at its own fromBlock and stays contiguous —
  // never inherits the in-flight stream's cursor position.
  assertContiguous(reconnectSeq, 'reconnect (fromBlock=300)');
});
