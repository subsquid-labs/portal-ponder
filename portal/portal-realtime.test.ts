import { expect, test } from 'vitest';
import {
  type Light,
  portalRealtimeEvents,
  reconcile,
  takeFinalized,
} from './portal-realtime.js';

const L = (number: number, hash: string, parentHash: string): Light => ({
  number,
  hash,
  parentHash,
  timestamp: number,
});

test('reconcile: append extends the tip (and the empty chain)', () => {
  expect(reconcile([], L(10, 'a', 'z'))).toEqual({ kind: 'append' });
  expect(
    reconcile([L(10, 'a', 'z'), L(11, 'b', 'a')], L(12, 'c', 'b')),
  ).toEqual({ kind: 'append' });
});

test('reconcile: duplicate tip is idempotent (re-delivery)', () => {
  expect(
    reconcile([L(10, 'a', 'z'), L(11, 'b', 'a')], L(11, 'b', 'a')),
  ).toEqual({ kind: 'duplicate' });
});

test('reconcile: reorg forks off an earlier common ancestor, reorged blocks after it', () => {
  const chain = [L(10, 'a', 'z'), L(11, 'b', 'a'), L(12, 'c', 'b')];
  const r = reconcile(chain, L(11, 'b2', 'a')); // 11' whose parent is block 10 (a)
  expect(r.kind).toBe('reorg');
  if (r.kind === 'reorg') {
    expect(r.commonAncestor.hash).toBe('a');
    expect(r.reorgedBlocks.map((b) => b.hash)).toEqual(['b', 'c']);
  }
});

test('reconcile: deep-fork reorg to the base', () => {
  const chain = [L(10, 'a', 'z'), L(11, 'b', 'a'), L(12, 'c', 'b')];
  const r = reconcile(chain, L(13, 'd2', 'a')); // parent jumps back to block 10
  expect(r.kind).toBe('reorg');
  if (r.kind === 'reorg')
    expect(r.reorgedBlocks.map((b) => b.hash)).toEqual(['b', 'c']);
});

test('reconcile: gap when the parent is unknown (beyond our window)', () => {
  expect(
    reconcile([L(10, 'a', 'z'), L(11, 'b', 'a')], L(20, 'x', 'unknown')),
  ).toEqual({ kind: 'gap' });
});

test('takeFinalized: splits the chain at the finalized number', () => {
  const chain = [L(10, 'a', 'z'), L(11, 'b', 'a'), L(12, 'c', 'b')];
  const { finalizedTip, remaining } = takeFinalized(chain, 11);
  expect(finalizedTip?.hash).toBe('b');
  expect(remaining.map((b) => b.number)).toEqual([12]);
  expect(takeFinalized(chain, 9).finalizedTip).toBeUndefined();
});

// mock /stream: a Response whose body streams the NDJSON batches once, then 204 (→ caller aborts)
function mockFetch(batches: any[], onExhausted?: () => void) {
  let served = false;
  return (async () => {
    if (served) {
      onExhausted?.();
      return { status: 204, ok: false, body: null };
    }
    served = true;
    const lines = batches.map((b) => JSON.stringify(b) + '\n').join('');
    const body = new ReadableStream({
      start(c) {
        c.enqueue(new TextEncoder().encode(lines));
        c.close();
      },
    });
    return { status: 200, ok: true, body };
  }) as any;
}

test('portalRealtimeEvents: streams block events (header + logs) and emits finalize from the head poll', async () => {
  const batches = [
    {
      header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 1000 },
      logs: [],
    },
    {
      header: {
        number: 101,
        hash: 'h101',
        parentHash: 'h100',
        timestamp: 1012,
      },
      logs: [
        {
          address: '0xVaULT',
          topics: ['0xabc'],
          data: '0x',
          logIndex: 0,
          transactionHash: '0xtx',
          transactionIndex: 0,
        },
      ],
    },
  ];
  const ac = new AbortController();
  const events: any[] = [];
  for await (const e of portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => 100,
    finalizePollMs: 0,
  })) {
    events.push(e);
  }
  const blocks = events.filter((e) => e.type === 'block');
  expect(blocks.length).toBe(2);
  expect(blocks[0].hasMatchedFilter).toBe(false); // block 100 has no logs
  expect(blocks[1].hasMatchedFilter).toBe(true); //  block 101 has a euler log
  expect(blocks[1].logs[0].address).toBe('0xvault'); // transform lowercases
  expect(blocks[1].block.number).toBe('0x65'); // 101 → hex
  expect(
    events.some((e) => e.type === 'finalize' && e.block.number === 100),
  ).toBe(true);
});

test('portalRealtimeEvents: a re-streamed fork emits a reorg to the common ancestor', async () => {
  const batches = [
    {
      header: { number: 10, hash: 'a', parentHash: 'z', timestamp: 10 },
      logs: [],
    },
    {
      header: { number: 11, hash: 'b', parentHash: 'a', timestamp: 11 },
      logs: [],
    },
    {
      header: { number: 11, hash: 'b2', parentHash: 'a', timestamp: 11 },
      logs: [],
    }, // fork off block 10
  ];
  const ac = new AbortController();
  const events: any[] = [];
  for await (const e of portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 10,
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => 0,
    finalizePollMs: 999999,
  })) {
    events.push(e);
  }
  const reorg = events.find((e) => e.type === 'reorg');
  expect(reorg).toBeDefined();
  expect(reorg.block.hash).toBe('a'); // common ancestor = block 10
  expect(reorg.reorgedBlocks.map((b: Light) => b.hash)).toEqual(['b']);
});
