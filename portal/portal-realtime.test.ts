import { expect, test } from 'vitest';
import {
  type Light,
  portalRealtimeEvents,
  reconcile,
  streamHotBlocks,
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

test('portalRealtimeEvents: an unknown-parent gap is FATAL, not silently skipped (finding 7)', async () => {
  const batches = [
    {
      header: { number: 10, hash: 'a', parentHash: 'z', timestamp: 10 },
      logs: [],
    },
    // block 12's parent is unknown to our window ([10]) — a reorg deeper than the window (e.g. one that
    // landed while disconnected, past the resume cursor). The old code silently cleared and continued.
    {
      header: { number: 12, hash: 'c', parentHash: 'unknown', timestamp: 12 },
      logs: [],
    },
  ];
  const ac = new AbortController();
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 10,
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => 0,
    finalizePollMs: 999999,
  });
  const seen: any[] = [];
  await expect(
    (async () => {
      for await (const e of iter) seen.push(e);
    })(),
  ).rejects.toThrow(/unknown parent/i);
  // block 10 was delivered before the gap; the gap block was NOT swallowed into a silent resync
  expect(seen.some((e) => e.type === 'block' && e.block.number === '0xa')).toBe(
    true,
  );
});

test('streamHotBlocks: re-opens the /stream with the widened filter the moment the logs revision advances (finding 4)', async () => {
  const enc = new TextEncoder();
  const streamOf = (block: any, close: boolean) =>
    new ReadableStream({
      start(c) {
        c.enqueue(enc.encode(`${JSON.stringify(block)}\n`));
        if (close) c.close(); // else leave open — only the revision change tears it down
      },
    });
  const bodies: any[] = [];
  let conn = 0;
  const fetchImpl = (async (_url: string, init: any) => {
    bodies.push(JSON.parse(init.body));
    conn += 1;
    if (conn === 1)
      // connection 1: deliver block 100, then stay OPEN (no more data) so nothing but a rev change reopens it
      return {
        status: 200,
        ok: true,
        body: streamOf(
          {
            header: {
              number: 100,
              hash: 'h100',
              parentHash: 'h99',
              timestamp: 1,
            },
            logs: [],
          },
          false,
        ),
      };
    if (conn === 2)
      // connection 2 (after the reopen): deliver block 101 and close
      return {
        status: 200,
        ok: true,
        body: streamOf(
          {
            header: {
              number: 101,
              hash: 'h101',
              parentHash: 'h100',
              timestamp: 2,
            },
            logs: [],
          },
          true,
        ),
      };
    return { status: 204, ok: false, body: null };
  }) as any;

  const logs: any[] = [{ address: ['0xfactory'], topic0: ['0xproxycreated'] }];
  let rev = 0;
  const ac = new AbortController();
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs,
    getLogsRevision: () => rev,
    fetchImpl,
    signal: ac.signal,
  });

  const first = await gen.next(); // block 100 from connection 1
  expect(first.value?.header.number).toBe(100);
  expect(bodies).toHaveLength(1);
  expect(bodies[0].fromBlock).toBe(100);

  // a newly-discovered factory child widens the filter and bumps the revision
  logs.length = 0;
  logs.push({
    address: ['0xfactory', '0xnewchild'],
    topic0: ['0xproxycreated'],
  });
  rev = 1;

  const second = await gen.next(); // rev advanced → connection 1 torn down, reopen resumes from cursor 101
  expect(second.value?.header.number).toBe(101);
  expect(bodies).toHaveLength(2);
  expect(bodies[1].fromBlock).toBe(101); // resumed PAST block 100 (no re-delivery / spurious reorg)
  expect(bodies[1].logs[0].address).toContain('0xnewchild'); // reopened with the widened filter

  await gen.return(undefined); // stop the generator
  ac.abort();
});
