import { getEventListeners } from 'node:events';
import { afterEach, expect, test, vi } from 'vitest';
import { BLOCK_FIELDS, TX_FIELDS } from './portal-filters.js';
import {
  diagDump,
  type HotBatch,
  type HotItem,
  type Light,
  portalRealtimeEvents,
  reconcile,
  type StreamDiag,
  sleep,
  streamHotBlocks,
  takeFinalized,
  windowDump,
} from './portal-realtime.js';

const L = (number: number, hash: string, parentHash: string): Light => ({
  number,
  hash,
  parentHash,
  timestamp: number,
});

const nextBlock = async (
  gen: AsyncGenerator<HotItem>,
): Promise<IteratorResult<HotBatch>> => {
  for (;;) {
    const r = await gen.next();
    if (r.done || r.value.kind !== 'tick') return r as IteratorResult<HotBatch>;
  }
};

// The reconcile anchor is REQUIRED since wave 4 (the optional blind-append legacy mode is gone);
// A9 is the finalized block below these windows' base.
const A9 = L(9, 'z', 'y');

test('reconcile: append extends the tip (and the anchored empty chain)', () => {
  expect(reconcile([], L(10, 'a', 'z'), A9)).toEqual({ kind: 'append' });
  expect(
    reconcile([L(10, 'a', 'z'), L(11, 'b', 'a')], L(12, 'c', 'b'), A9),
  ).toEqual({ kind: 'append' });
});

test('reconcile: duplicate tip is idempotent (re-delivery)', () => {
  expect(
    reconcile([L(10, 'a', 'z'), L(11, 'b', 'a')], L(11, 'b', 'a'), A9),
  ).toEqual({ kind: 'duplicate' });
});

test('reconcile: reorg forks off an earlier common ancestor, reorged blocks after it', () => {
  const chain = [L(10, 'a', 'z'), L(11, 'b', 'a'), L(12, 'c', 'b')];
  const r = reconcile(chain, L(11, 'b2', 'a'), A9); // 11' whose parent is block 10 (a)
  expect(r.kind).toBe('reorg');
  if (r.kind === 'reorg') {
    expect(r.commonAncestor.hash).toBe('a');
    expect(r.reorgedBlocks.map((b) => b.hash)).toEqual(['b', 'c']);
  }
});

test('reconcile: deep-fork reorg to the base', () => {
  const chain = [L(10, 'a', 'z'), L(11, 'b', 'a'), L(12, 'c', 'b')];
  const r = reconcile(chain, L(13, 'd2', 'a'), A9); // parent jumps back to block 10
  expect(r.kind).toBe('reorg');
  if (r.kind === 'reorg')
    expect(r.reorgedBlocks.map((b) => b.hash)).toEqual(['b', 'c']);
});

test('reconcile: gap when the parent is unknown (beyond our window)', () => {
  expect(
    reconcile([L(10, 'a', 'z'), L(11, 'b', 'a')], L(20, 'x', 'unknown'), A9),
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
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => ({ number: 100 }),
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

test('portalRealtimeEvents: parent transactions are deduped by hash before emit (review B4)', async () => {
  // Two matched logs sharing a parent tx (or overlapping log requests) can each carry the SAME tx in the
  // batch; ponder's finalize insert must store exactly one row per hash. The historical assembly dedupes
  // via `seenTx`; the realtime mapping must too.
  const dupTx = { transactionIndex: 0, hash: '0xdup', from: '0xa', to: '0xb' };
  const otherTx = { transactionIndex: 1, hash: '0xother', from: '0xc' };
  const batches = [
    {
      header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 1000 },
      logs: [
        { address: '0xv', topics: ['0xt'], data: '0x', logIndex: 0 },
        { address: '0xv', topics: ['0xt'], data: '0x', logIndex: 1 },
      ],
      // 0xdup appears twice (both logs share it); 0xother once
      transactions: [dupTx, dupTx, otherTx],
    },
  ];
  const ac = new AbortController();
  const events: any[] = [];
  for await (const e of portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => ({ number: 0 }),
    finalizePollMs: 999999,
  })) {
    events.push(e);
  }
  const block = events.find((e) => e.type === 'block');
  expect(block).toBeDefined();
  // exactly ONE row per hash — 0xdup collapsed from two occurrences to one, 0xother kept
  expect(block.transactions.map((t: any) => t.hash).sort()).toEqual([
    '0xdup',
    '0xother',
  ]);
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
    anchor: L(9, 'z', 'y'),
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => ({ number: 0 }),
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
    anchor: L(9, 'z', 'y'),
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => ({ number: 0 }),
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

  const first = await nextBlock(gen); // block 100 from connection 1
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

  const second = await nextBlock(gen); // rev advanced → connection 1 torn down, reopen re-delivers block 100
  expect(second.value?.header.number).toBe(101);
  expect(bodies).toHaveLength(2);
  // Re-opened FROM block 100, NOT 101: the child discovered in block 100 may have emitted its own logs
  // in that same block, and they were filtered out server-side by connection 1's narrower filter. The
  // re-delivered 100 reconciles as a duplicate that only an awaited redelivery re-emits (no spurious
  // reorg). Resuming from 101 permanently lost those same-block child logs.
  expect(bodies[1].fromBlock).toBe(100);
  expect(bodies[1].logs[0].address).toContain('0xnewchild'); // reopened with the widened filter

  await gen.return(undefined); // stop the generator
  ac.abort();
});

test('streamHotBlocks: caches the /stream body across same-cursor re-opens, rebuilds it only when the cursor advances (wave 5 perf)', async () => {
  // The /stream body is O(total children) — up to ~100k filter rows / multi-MB — and the loop re-enters
  // body construction on every re-poll/reopen. It is now cached and only rebuilt when (cursor,
  // parentBlockHash, logs-revision, dropped-field set) changes. We OBSERVE the rebuild directly: with
  // `txFields` set, the body path calls `args.logs.map(...)`, so an instrumented `map` counts serializations.
  const enc = new TextEncoder();
  const emptyStream = () =>
    new ReadableStream({
      start(c) {
        c.close(); // 200 with no data → reconnect at the SAME cursor (no block delivered)
      },
    });
  const streamOf = (block: any) =>
    new ReadableStream({
      start(c) {
        c.enqueue(enc.encode(`${JSON.stringify(block)}\n`));
        c.close();
      },
    });
  const hdr = (n: number, hash: string, parent: string) => ({
    header: { number: n, hash, parentHash: parent, timestamp: n },
    logs: [],
  });

  const logs: any[] = [{ address: ['0xfactory'], topic0: ['0xproxycreated'] }];
  const realMap = Array.prototype.map;
  let builds = 0;
  // count body serializations: the body maps `args.logs` exactly once per (re)build when txFields is set
  (logs as any).map = function (this: any[], ...a: any[]) {
    builds += 1;
    return (realMap as any).apply(this, a);
  };

  const bodies: string[] = [];
  let conn = 0;
  const fetchImpl = (async (_url: string, init: any) => {
    bodies.push(init.body);
    conn += 1;
    if (conn === 1) return { status: 200, ok: true, body: emptyStream() }; // cursor 100, no data
    if (conn === 2)
      return { status: 200, ok: true, body: streamOf(hdr(100, 'h100', 'h99')) }; // cursor 100 → deliver 100
    if (conn === 3)
      return {
        status: 200,
        ok: true,
        body: streamOf(hdr(101, 'h101', 'h100')),
      }; // cursor 101 → deliver 101
    return { status: 204, ok: false, body: null };
  }) as any;

  const ac = new AbortController();
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs,
    txFields: { hash: true },
    fetchImpl,
    signal: ac.signal,
  });

  const first = await nextBlock(gen); // conn1 (empty, cursor 100) then conn2 delivers block 100
  expect(first.value?.header.number).toBe(100);
  expect(bodies).toHaveLength(2);
  expect(builds).toBe(1); // ONE serialization for TWO same-cursor requests — the body was REUSED
  expect(bodies[0]).toBe(bodies[1]); // byte-identical
  expect(JSON.parse(bodies[0]!).fromBlock).toBe(100);

  const second = await nextBlock(gen); // conn3 at cursor 101
  expect(second.value?.header.number).toBe(101);
  expect(bodies).toHaveLength(3);
  expect(builds).toBe(2); // the cursor advanced → rebuilt exactly once more (never stale)
  expect(bodies[2]).not.toBe(bodies[0]);
  expect(JSON.parse(bodies[2]!).fromBlock).toBe(101);

  await gen.return(undefined);
  ac.abort();
});

test('streamHotBlocks: the body-cache key pins CURSOR independently — a cursor advance with an unchanged parentBlockHash still rebuilds (wave 5 follow-up)', async () => {
  // Finding 3 (PR #66 review): `cursor` must be an INDEPENDENT component of the body-cache key. The wave-5
  // cache test above can't catch a regression that drops `cursor` from the key, because there the cursor
  // advance (100→101) ALSO changes parentBlockHash (undefined→'h100'), so parentBlockHash alone still keys
  // the rebuild. Here we hold parentBlockHash CONSTANT across a cursor advance by delivering consecutive
  // blocks that share a hash string (unarmed mode → parentBlockHash = ring.get(cursor−1) = that shared
  // hash), leaving `cursor` as the SOLE differing key input. Dropping `cursor` from bodyKey makes the final
  // reopen a false cache HIT → the body would be re-POSTed with a STALE `fromBlock`; the count below then
  // reads `builds === 2` and fails. The existing test stays green under that same mutation.
  const enc = new TextEncoder();
  const SAME = 'hSAME'; // blocks 100/101/102 all carry this hash → parentBlockHash is constant across them
  const emptyStream = () =>
    new ReadableStream({
      start(c) {
        c.close(); // 200 with no data → reconnect at the SAME cursor (no block delivered)
      },
    });
  const streamOf = (block: any) =>
    new ReadableStream({
      start(c) {
        c.enqueue(enc.encode(`${JSON.stringify(block)}\n`));
        c.close();
      },
    });
  const hdr = (n: number, hash: string, parent: string) => ({
    header: { number: n, hash, parentHash: parent, timestamp: n },
    logs: [],
  });

  const logs: any[] = [{ address: ['0xfactory'], topic0: ['0xproxycreated'] }];
  const realMap = Array.prototype.map;
  let builds = 0;
  // count body serializations: the body maps `args.logs` exactly once per (re)build when txFields is set
  (logs as any).map = function (this: any[], ...a: any[]) {
    builds += 1;
    return (realMap as any).apply(this, a);
  };

  const bodies: string[] = [];
  let conn = 0;
  const fetchImpl = (async (_url: string, init: any) => {
    bodies.push(init.body);
    conn += 1;
    if (conn === 1) return { status: 200, ok: true, body: emptyStream() }; // cursor 100, pbh undefined
    if (conn === 2)
      return { status: 200, ok: true, body: streamOf(hdr(100, SAME, 'h99')) }; // cursor 100 → deliver 100
    if (conn === 3)
      return { status: 200, ok: true, body: streamOf(hdr(101, SAME, SAME)) }; // cursor 101, pbh=SAME → deliver 101
    if (conn === 4)
      return { status: 200, ok: true, body: streamOf(hdr(102, SAME, SAME)) }; // cursor 102, pbh=SAME (unchanged) → deliver 102
    return { status: 204, ok: false, body: null };
  }) as any;

  const ac = new AbortController();
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs,
    txFields: { hash: true },
    fetchImpl,
    signal: ac.signal,
  });

  const first = await nextBlock(gen); // conn1 (empty, cursor 100) then conn2 delivers 100
  expect(first.value?.header.number).toBe(100);
  expect(builds).toBe(1); // conn1 open built; conn2 reused (same cursor 100, same pbh)

  const second = await nextBlock(gen); // conn3: cursor 101, pbh=SAME → rebuilt (cursor advanced)
  expect(second.value?.header.number).toBe(101);
  expect(builds).toBe(2);
  expect(JSON.parse(bodies[2]!).fromBlock).toBe(101);
  expect(JSON.parse(bodies[2]!).parentBlockHash).toBe(SAME);

  const third = await nextBlock(gen); // conn4: cursor 102, pbh=SAME (UNCHANGED from conn3) → MUST rebuild on cursor alone
  expect(third.value?.header.number).toBe(102);
  // THE PIN: parentBlockHash is byte-identical between conn3 and conn4 (both 'hSAME'); only `cursor`
  // advanced 101→102. With `cursor` in the key this is a fresh build (fromBlock 102); drop `cursor` and it
  // is a false cache hit that re-POSTs the stale fromBlock-101 body → builds stays 2 and this fails.
  expect(builds).toBe(3);
  expect(JSON.parse(bodies[3]!).parentBlockHash).toBe(SAME); // pbh unchanged — the confound is removed
  expect(JSON.parse(bodies[3]!).fromBlock).toBe(102); // the body advanced with the cursor (never stale)

  await gen.return(undefined);
  ac.abort();
});

// ─────────────────────────────── /stream parentBlockHash + 409 fork negotiation (issue #33) ───────────────────────────────

// A scripted Portal /stream mock: one entry per connection, each either a 200 that streams blocks then
// closes, or a 409 that returns { previousBlocks }. Captures every request body so per-connection
// fromBlock / parentBlockHash assertions are hermetic. 204s past the last entry (→ the caller aborts).
type Conn =
  | { status: 200; blocks: any[] }
  | { status: 409; previousBlocks: Array<{ number: number; hash: string }> };
function mockForkFetch(
  conns: Conn[],
  bodies: any[],
  onExhausted?: () => void,
): typeof fetch {
  const enc = new TextEncoder();
  let i = 0;
  return (async (_url: string, init: any) => {
    bodies.push(JSON.parse(init.body));
    if (i >= conns.length) {
      onExhausted?.();

      return { status: 204, ok: false, body: null };
    }
    const conn = conns[i++]!;
    if (conn.status === 409) {
      const text = JSON.stringify({ previousBlocks: conn.previousBlocks });
      const body = new ReadableStream({
        start(c) {
          c.enqueue(enc.encode(text));
          c.close();
        },
      });

      return { status: 409, ok: false, body };
    }
    const lines = conn.blocks.map((b) => `${JSON.stringify(b)}\n`).join('');
    const body = new ReadableStream({
      start(c) {
        c.enqueue(enc.encode(lines));
        c.close();
      },
    });

    return { status: 200, ok: true, body };
  }) as any;
}

const hdr = (n: number, hash: string, parentHash: string) => ({
  header: { number: n, hash, parentHash, timestamp: n },
  logs: [],
});

test('portalRealtimeEvents: a 1-block orphan at tip heals via 409 fork negotiation — orphan N−1 then canonical N becomes reorg + appends, no gap fatal (issue #33 T1)', async () => {
  // The exact instance-4 shape. conn1 serves canonical 674 then the ORPHAN 675 (a non-canonical sibling)
  // and closes; conn2 resumes at 676 carrying parentBlockHash=675-orphan → the server sees it orphaned and
  // 409s with the canonical replacement chain [674, 675]; conn3 resumes at 675 (rewound to just above the
  // matched 674) carrying parentBlockHash=674 → serves canonical 675 then 676. reconcile surfaces the
  // canonical 675 (parent 674, forking off the orphan) as a reorg off 674, pops the orphan, and appends.
  const anchor = {
    number: 673,
    hash: 'h673',
    parentHash: 'h672',
    timestamp: 673,
  };
  const conns: Conn[] = [
    {
      status: 200,
      blocks: [hdr(674, 'h674', 'h673'), hdr(675, 'h675o', 'h674')],
    },
    {
      status: 409,
      previousBlocks: [
        { number: 674, hash: 'h674' },
        { number: 675, hash: 'h675c' },
      ],
    },
    {
      status: 200,
      blocks: [hdr(675, 'h675c', 'h674'), hdr(676, 'h676', 'h675c')],
    },
  ];
  const bodies: any[] = [];
  const ac = new AbortController();
  const events: any[] = [];
  for await (const e of portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 674,
    logs: [],
    anchor,
    fetchImpl: mockForkFetch(conns, bodies, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => ({ number: 673 }), // floor stays at the anchor; nothing new finalizes
    finalizePollMs: 999999,
  })) {
    events.push(e);
  }
  // exact event sequence: block 674, block 675o, reorg{674, [675o]}, block 675c, block 676
  const kinds = events.map((e) =>
    e.type === 'block'
      ? `block ${e.block.number}`
      : e.type === 'reorg'
        ? `reorg ${e.block.number}:[${e.reorgedBlocks.map((b: Light) => b.hash).join(',')}]`
        : `finalize ${e.block.number}`,
  );
  expect(kinds).toEqual([
    'block 0x2a2', // 674
    'block 0x2a3', // 675 (orphan)
    'reorg 674:[h675o]', // rollback to 674, reorging the orphan
    'block 0x2a3', // 675 (canonical, re-delivered)
    'block 0x2a4', // 676
  ]);
  // per-connection wire assertions: fromBlock + parentBlockHash
  const stream = bodies.filter((b) => b.fromBlock !== undefined);
  expect(stream[0].fromBlock).toBe(674);
  expect(stream[0].parentBlockHash).toBe('h673'); // first request carries the anchor hash
  expect(stream[1].fromBlock).toBe(676);
  expect(stream[1].parentBlockHash).toBe('h675o'); // resume past the orphan → its hash
  expect(stream[2].fromBlock).toBe(675); // rewound to just above the matched common ancestor 674
  expect(stream[2].parentBlockHash).toBe('h674'); // the canonical 674 hash the ring confirmed
});

test('streamHotBlocks: every /stream request carries parentBlockHash — first = anchor hash, post-reconnect = last delivered hash (issue #33 T2 conformance)', async () => {
  const bodies: any[] = [];
  const ac = new AbortController();
  const conns: Conn[] = [
    { status: 200, blocks: [hdr(100, 'h100', 'h99')] }, // conn1: one block, then close
    { status: 200, blocks: [hdr(101, 'h101', 'h100')] }, // conn2: resume carries h100
  ];
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [],
    seedRing: { number: 99, hash: 'h99' }, // the startup anchor seeds the ring
    fetchImpl: mockForkFetch(conns, bodies, () => ac.abort()),
    signal: ac.signal,
  });
  const first = await nextBlock(gen);
  expect(first.value?.header.number).toBe(100);
  expect(bodies[0].fromBlock).toBe(100);
  expect(bodies[0].parentBlockHash).toBe('h99'); // FIRST request carries the seeded anchor hash

  const second = await nextBlock(gen);
  expect(second.value?.header.number).toBe(101);
  expect(bodies[1].fromBlock).toBe(101);
  expect(bodies[1].parentBlockHash).toBe('h100'); // post-reconnect carries the LAST delivered hash
  // F3: EVERY captured /stream body must carry a parentBlockHash in armed mode — a connection dropping the
  // key (going number-only) would silently re-open the fork-negotiation hole. Assert across ALL bodies, not
  // just the two spot-checked above.
  const streamed = bodies.filter((b) => b.fromBlock !== undefined);
  expect(streamed.length).toBeGreaterThan(0);
  for (const b of streamed) {
    expect(typeof b.parentBlockHash).toBe('string');
    expect(b.parentBlockHash.length).toBeGreaterThan(0);
  }
  await gen.return(undefined);
  ac.abort();
});

test('streamHotBlocks: the finding-4 redelivery reopen (cursor = number) sends the AWAITED block’s parentHash, not its own hash (issue #33 T2 redelivery)', async () => {
  // The finding-4 same-block child reopen resumes FROM N (not N+1). The parentBlockHash must be the ring
  // entry at N−1 (the awaited block's parentHash), so the server re-serves N as a normal resume — NOT N's
  // own hash (which would ask for N+1 and skip the redelivery this handshake exists to force).
  const enc = new TextEncoder();
  const streamOf = (block: any, close: boolean) =>
    new ReadableStream({
      start(c) {
        c.enqueue(enc.encode(`${JSON.stringify(block)}\n`));
        if (close) c.close();
      },
    });
  const bodies: any[] = [];
  let conn = 0;
  const fetchImpl = (async (_url: string, init: any) => {
    bodies.push(JSON.parse(init.body));
    conn += 1;
    if (conn === 1)
      return {
        status: 200,
        ok: true,
        body: streamOf(hdr(100, 'h100', 'h99'), false), // deliver 100, stay open
      };
    if (conn === 2)
      return {
        status: 200,
        ok: true,
        body: streamOf(hdr(100, 'h100', 'h99'), true), // redelivered 100
      };
    return { status: 204, ok: false, body: null };
  }) as any;
  let rev = 0;
  const ac = new AbortController();
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [],
    seedRing: { number: 99, hash: 'h99' },
    getLogsRevision: () => rev,
    fetchImpl,
    signal: ac.signal,
  });
  const first = await nextBlock(gen);
  expect(first.value?.header.number).toBe(100);
  expect(bodies[0].parentBlockHash).toBe('h99');

  rev = 1; // a child was discovered in block 100 → force the reopen FROM 100
  const second = await nextBlock(gen);
  expect(second.value?.header.number).toBe(100); // redelivered
  expect(bodies[1].fromBlock).toBe(100); // reopened FROM 100, not 101
  expect(bodies[1].parentBlockHash).toBe('h99'); // the AWAITED block's parentHash (ring[99]), not h100
  await gen.return(undefined);
  ac.abort();
});

test('streamHotBlocks: a 409 whose previousBlocks match NOTHING steps the cursor down one block per retry and fatals at the finalized floor — nothing yielded past the fork (issue #33 T3 step-down)', async () => {
  const bodies: any[] = [];
  // conn1 delivers 101,102,103 so the ring holds every delivered height (100 seed + 101,102,103) — as in
  // production, where the ring mirrors the whole unfinalized window down to the floor. Then every 409 names
  // a fork point the ring can't confirm (hashes it never delivered), so no rewind matches and the cursor
  // steps DOWN one block per retry, re-sending the ring hash at each new cursor−1, until cursor−1 reaches
  // the floor (100) → fatal. Blocks 101–103 were delivered first; nothing is accepted PAST the fork.
  const conns: Conn[] = [
    {
      status: 200,
      blocks: [
        hdr(101, 'h101', 'h100'),
        hdr(102, 'h102', 'h101'),
        hdr(103, 'h103', 'h102'),
      ],
    },
    { status: 409, previousBlocks: [{ number: 103, hash: 'x103' }] },
    { status: 409, previousBlocks: [{ number: 102, hash: 'x102' }] },
    { status: 409, previousBlocks: [{ number: 101, hash: 'x101' }] },
    { status: 409, previousBlocks: [{ number: 100, hash: 'x100' }] },
  ];
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 101,
    logs: [],
    seedRing: { number: 100, hash: 'h100' },
    getFinalizedFloor: () => 100,
    fetchImpl: mockForkFetch(conns, bodies),
  });
  const yielded: any[] = [];
  await expect(
    (async () => {
      for await (const b of gen) {
        if (b.kind === 'block') yielded.push(b.header.number);
      }
    })(),
  ).rejects.toThrow(/fork point is at or below the finalized floor/i);
  expect(yielded).toEqual([101, 102, 103]); // the pre-fork deliveries; nothing accepted past the fork
  // resume at 104, then step DOWN 104→103→102→101 with the ring hash at each new cursor−1 until the floor
  const stream = bodies.filter((b) => b.fromBlock !== undefined);
  expect(stream.map((b) => b.fromBlock)).toEqual([101, 104, 103, 102, 101]);
  // the stepped-down requests each carried the ring hash at cursor−1 (real delivered hashes)
  expect(stream[1].parentBlockHash).toBe('h103'); // resume at 104 → ring[103]
  expect(stream[2].parentBlockHash).toBe('h102'); // stepped to 103 → ring[102]
  expect(stream[3].parentBlockHash).toBe('h101'); // stepped to 102 → ring[101]
  expect(stream[4].parentBlockHash).toBe('h100'); // stepped to 101 → ring[100] (the floor/anchor)
});

test('streamHotBlocks: an OSCILLATING 409 loop (server keeps re-409ing a rewind the ring confirms at the SAME height → no cursor progress) fatals at the 10 no-progress cap (issue #33 T3 cap)', async () => {
  const bodies: any[] = [];
  // conn1 delivers 101..105 (ring holds 100..105, cursor at the tip 106). Then EVERY connection 409s with a
  // previousBlocks the ring CONFIRMS at the SAME height ({105, ring[105]}) — so each 409 rewinds the cursor
  // to 106 (105+1), exactly where it already sits: NO cursor progress, an infinite rewind-to-the-same-spot
  // oscillation (the fork point never resolves and never descends). consecutive409 never resets (no 200
  // delivered after the first, and no round lowers the cursor), so the 10 no-progress cap must break it
  // rather than spin forever. (F2: the cap counts NO-PROGRESS rounds — this is the shape it exists to stop.)
  const conns: Conn[] = [
    {
      status: 200,
      blocks: [
        hdr(101, 'h101', 'h100'),
        hdr(102, 'h102', 'h101'),
        hdr(103, 'h103', 'h102'),
        hdr(104, 'h104', 'h103'),
        hdr(105, 'h105', 'h104'),
      ],
    },
    // 14 identical 409s: with the cap, only 11 no-progress negotiations fire before the fatal, well under
    // this bound; WITHOUT the cap the loop would exhaust these and hit the sentinel throw below (a clean,
    // distinct failure instead of an infinite hang → a cap-removal regression surfaces unambiguously).
    ...Array.from({ length: 14 }, () => ({
      status: 409 as const,
      previousBlocks: [{ number: 105, hash: 'h105' }], // a MATCH at the TIP → rewind to 106 (no descent)
    })),
  ];
  let onExhausted: (() => void) | undefined;
  const sentinel = new Promise<never>((_, reject) => {
    onExhausted = () =>
      reject(
        new Error(
          'NEVER-EXHAUSTED: the 409 loop was not cap-bounded — it consumed every scripted 409 (regression)',
        ),
      );
  });
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 101,
    logs: [],
    seedRing: { number: 100, hash: 'h100' },
    getFinalizedFloor: () => 0, // floor far below so the floor guard never fires — only the 10-cap can
    fetchImpl: mockForkFetch(conns, bodies, () => onExhausted?.()),
  });
  const yielded: any[] = [];
  await expect(
    Promise.race([
      (async () => {
        for await (const b of gen) {
          if (b.kind === 'block') yielded.push(b.header.number);
        }
      })(),
      sentinel,
    ]),
  ).rejects.toThrow(
    /consecutive .*409 fork-negotiations without cursor progress/i,
  );
  expect(yielded).toEqual([101, 102, 103, 104, 105]); // only the pre-loop deliveries
  // exactly 11 no-progress 409s reach the loop (the 11th trips consecutive409 > 10). EVERY one resumed at
  // the tip (106): the confirmed rewind lands at 106 = 105+1, the same height the cursor already held, so no
  // round makes progress and the cap counts from the very first — 11 identical requests before the fatal.
  const stream = bodies.filter((b) => b.fromBlock !== undefined);
  const negotiations = stream.slice(1); // drop the initial fromBlock=101 request
  expect(negotiations.length).toBe(11);
  for (const r of negotiations) {
    expect(r.fromBlock).toBe(106); // rewound to the SAME tip every time → no progress
    expect(r.parentBlockHash).toBe('h105'); // ring[105], the confirmed tip hash
  }
});

test('streamHotBlocks: a no-match step-down descending MORE than 10 heights reaches the FLOOR fatal, NOT the no-progress cap — a deep negotiation runs "until a match or the floor" (issue #33 T3 deep step-down / F2)', async () => {
  const bodies: any[] = [];
  // conn1 delivers 101..115 (ring holds 100..115, cursor at the tip 116). The floor is 100. EVERY 409 names
  // a fork point the ring CANNOT confirm (a hash it never delivered), so no rewind matches and the cursor
  // steps DOWN one height per 409. The descent is 116 → 115 → … → 101 (15 step-downs, FAR more than the
  // 10-cap). With a per-409 cap this would fatal at the 11th 409 (height ~106) — the bug. With the F2
  // no-PROGRESS cap, every step-down strictly lowers the cursor (progress → reset), so the streak never
  // accrues and the negotiation runs all the way to the floor fatal at height 101 (cursor−1 == floor 100).
  const blocks = Array.from({ length: 15 }, (_, i) =>
    hdr(101 + i, `h${101 + i}`, `h${100 + i}`),
  );
  // A no-match 409 for the request at fromBlock F names {F−1, x(F−1)} — a hash the ring never delivered — so
  // the cursor steps down to F−1. The descent runs from the tip (116) down to 101, where cursor−1 == floor
  // 100 fatals. That is one 409 per request at fromBlock 116,115,…,101 = 16 requests (the last one at 101
  // fatals). 16 > 10 proves the descent is NOT prematurely cap-fataled.
  const conns: Conn[] = [
    { status: 200, blocks },
    ...Array.from({ length: 16 }, (_, i) => ({
      status: 409 as const,
      previousBlocks: [{ number: 115 - i, hash: `x${115 - i}` }],
    })),
  ];
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 101,
    logs: [],
    seedRing: { number: 100, hash: 'h100' },
    getFinalizedFloor: () => 100,
    fetchImpl: mockForkFetch(conns, bodies),
  });
  const yielded: any[] = [];
  await expect(
    (async () => {
      for await (const b of gen) {
        if (b.kind === 'block') yielded.push(b.header.number);
      }
    })(),
  ).rejects.toThrow(/fork point is at or below the finalized floor/i);
  expect(yielded.length).toBe(15); // all pre-fork deliveries 101..115; nothing accepted past the fork
  // the cursor stepped down every single height from the tip to the floor — 15 descents + the floor-fatal
  // request at 101 = 16 negotiations, none cap-fataled (a per-409 cap would have fataled at the 11th)
  const stream = bodies.filter((b) => b.fromBlock !== undefined);
  const negotiations = stream.slice(1); // drop the initial fromBlock=101 request
  expect(negotiations.map((r) => r.fromBlock)).toEqual(
    Array.from({ length: 16 }, (_, i) => 116 - i), // 116,115,…,101 (the 101 request hits the floor fatal)
  );
});

test('streamHotBlocks: a 409 fork point BELOW the finalized floor is FATAL with no rewind (issue #33 T4 below-finality)', async () => {
  const bodies: any[] = [];
  // The server names a canonical replacement whose fork point (500) is BELOW the finalized floor (600).
  // Finalized data can't be rolled back, so there is no safe recovery — fatal immediately, no step-down.
  const conns: Conn[] = [
    { status: 409, previousBlocks: [{ number: 500, hash: 'h500' }] },
  ];
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 700,
    logs: [],
    seedRing: { number: 699, hash: 'h699' },
    getFinalizedFloor: () => 600,
    fetchImpl: mockForkFetch(conns, bodies),
  });
  await expect(gen.next()).rejects.toThrow(/at or below the finalized floor/i);
  // exactly ONE request went out — no step-down below finality
  const stream = bodies.filter((b) => b.fromBlock !== undefined);
  expect(stream).toHaveLength(1);
});

test('streamHotBlocks: a BODYLESS 409 (res.body === null) still DRIVES the fork negotiation — it does NOT silently re-poll forever (issue #33 F1)', async () => {
  // F1: the pre-existing `if (res.status === 204 || !res.body)` re-poll ran BEFORE the 409 branch, so a
  // bodyless 409 (a `new Response(null, {status:409})` shape — headers-only, no body) short-circuited to a
  // 500ms sleep + re-poll and NEVER reached the negotiation: a quiet permanent stall. The fix makes only a
  // 204 re-poll there; a bodyless 409 now flows into the 409 branch, where parsePreviousBlocks reads '' from
  // the null body (res.text() → '' → JSON.parse throws → caught → prev=[]), no rewind matches, and the
  // step-down hits the floor fatal. A bounded mock + sentinel proves it TERMINATES (fatal), not spins.
  const bodies: any[] = [];
  let served409 = false;
  let onExhausted: (() => void) | undefined;
  const sentinel = new Promise<never>((_, reject) => {
    onExhausted = () =>
      reject(
        new Error(
          'NEVER-NEGOTIATED: the bodyless 409 was re-polled instead of driving the fork negotiation (F1 regression)',
        ),
      );
  });
  // The floor sits at the resume parent (cursor−1 = 700 → floor 700), so the FIRST bodyless 409 that reaches
  // the negotiation immediately hits the floor fatal (cursor−1 <= floor). If the fix regressed, the bodyless
  // 409 would be re-polled: served409 flips true on the first call, so the SECOND call (a genuine re-poll,
  // not a negotiation step) exhausts the mock → the sentinel fires with a distinct, unambiguous failure.
  const fetchImpl = (async (_url: string, init: any) => {
    bodies.push(JSON.parse(init.body));
    if (!served409) {
      served409 = true;

      // headers-only 409: null body, and res.text() resolves to '' exactly like a real bodyless Response.
      return { status: 409, ok: false, body: null, text: async () => '' };
    }
    onExhausted?.();

    return { status: 204, ok: false, body: null };
  }) as any;
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 701,
    logs: [],
    seedRing: { number: 700, hash: 'h700' },
    getFinalizedFloor: () => 700, // floor at cursor−1 → the first negotiated 409 hits the floor fatal
    fetchImpl,
  });
  await expect(Promise.race([gen.next(), sentinel])).rejects.toThrow(
    /at or below the finalized floor/i,
  );
  // exactly ONE /stream request went out and it was the bodyless 409 — no silent re-poll past it
  const stream = bodies.filter((b) => b.fromBlock !== undefined);
  expect(stream).toHaveLength(1);
  expect(stream[0]!.fromBlock).toBe(701);
});

// ─────────────────────────────── diagnostic dumps (issue #33 F4) ───────────────────────────────

test('windowDump: renders the window size, first/tip, the entry at the parent height, and the anchor (issue #33 F4)', () => {
  // A direct unit test of the dump string so gutting windowDump() to '' fails LOUDLY — the fatal-message
  // integration tests below assert the SAME fields flow through, but this pins the exact format.
  const window: Light[] = [
    L(674, 'h674', 'h673'),
    L(675, 'h675', 'h674'),
    L(676, 'h676', 'h675'),
  ];
  const anchor = L(673, 'h673', 'h672');
  const dump = windowDump(window, anchor, 676); // parentHeight 676 is present in the window
  expect(dump).toMatch(/window: size=3/);
  expect(dump).toContain('first=674:h674');
  expect(dump).toContain('tip=676:h676');
  expect(dump).toContain('at(676)=present h676'); // the entry at the parent height, with its hash
  expect(dump).toContain('anchor=673:h673'); // the anchor number:hash

  // an ABSENT parent height renders `absent` (pins whether an orphaned sibling occupied the local N−1)
  expect(windowDump(window, anchor, 999)).toContain('at(999)=absent');
  // no anchor renders `anchor=none`
  expect(windowDump(window, undefined, 674)).toContain('anchor=none');
});

test('diagDump: renders the cursor, parentBlockHash sent, blocks delivered, the ring tail, and the last 409 (issue #33 F4)', () => {
  // Direct unit test of the 409/gap diagnostic string — gutting diagDump() to '' fails here.
  const diag: StreamDiag = {
    ring: [
      { number: 100, hash: 'h100' },
      { number: 101, hash: 'h101' },
    ],
    cursor: 102,
    parentBlockHashSent: 'h101',
    blocksDeliveredThisConn: 2,
    lastPreviousBlocks: [{ number: 101, hash: 'x101' }],
  };
  const dump = diagDump(diag);
  expect(dump).toMatch(/diag: cursor=102/);
  expect(dump).toContain('parentBlockHashSent=h101');
  expect(dump).toContain('blocksDeliveredThisConn=2');
  expect(dump).toContain('ring(last8)=[100:h100, 101:h101]'); // the ring fragment
  expect(dump).toContain('last409.previousBlocks=[101:x101]');

  // an absent lastPreviousBlocks renders `none`; a wholly-absent diag renders '' (no dump wired)
  const bare: StreamDiag = {
    ring: [],
    cursor: 5,
    parentBlockHashSent: undefined,
    blocksDeliveredThisConn: 0,
    lastPreviousBlocks: undefined,
  };
  expect(diagDump(bare)).toContain('last409.previousBlocks=none');
  expect(diagDump(bare)).toContain('parentBlockHashSent=none');
  expect(diagDump(undefined)).toBe('');
});

test('portalRealtimeEvents: the unknown-parent gap fatal MESSAGE carries the window + anchor diagnostic (issue #33 F4)', async () => {
  // The gap fatal must be self-identifying: its message embeds windowDump (size + anchor number:hash) and
  // diagDump (cursor). Gutting either dump to '' would leave the whole suite green without this assertion.
  const anchor: Light = L(673, 'h673', 'h672');
  const batches = [
    {
      header: { number: 674, hash: 'h674', parentHash: 'h673', timestamp: 674 },
      logs: [],
    },
    // block 676's parent is unknown to the window ([674]) — a gap fatal, with a wired anchor to dump
    {
      header: {
        number: 676,
        hash: 'h676',
        parentHash: 'unknown',
        timestamp: 676,
      },
      logs: [],
    },
  ];
  const ac = new AbortController();
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 674,
    logs: [],
    anchor,
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => ({ number: 673 }),
    finalizePollMs: 999999,
  });
  let msg = '';
  try {
    for await (const _ of iter) {
      /* drain until the gap throws */
    }
  } catch (e) {
    msg = (e as Error).message;
  }
  expect(msg).toMatch(/unknown parent/i);
  expect(msg).toMatch(/window: size=\d+/); // the window dump is present in the fatal
  expect(msg).toContain('anchor=673:h673'); // the anchor number:hash (armed via the wired anchor)
  expect(msg).toMatch(/diag: cursor=\d+/); // the shell's connection diagnostic is present too
});

test('streamHotBlocks: the 409-exhausted (oscillation-cap) fatal MESSAGE carries the diag cursor + ring fragment (issue #33 F4)', async () => {
  // The 409-exhausted fatal must dump the ring/cursor state. Gutting diagDump() to '' leaves the cap fatal
  // firing but strips its diagnostic — this assertion kills that mutant.
  const bodies: any[] = [];
  const conns: Conn[] = [
    {
      status: 200,
      blocks: [hdr(101, 'h101', 'h100'), hdr(102, 'h102', 'h101')],
    },
    // oscillate: rewind to the SAME tip (102) every time → no progress → the no-progress cap fatals at 10
    ...Array.from({ length: 14 }, () => ({
      status: 409 as const,
      previousBlocks: [{ number: 102, hash: 'h102' }],
    })),
  ];
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 101,
    logs: [],
    seedRing: { number: 100, hash: 'h100' },
    getFinalizedFloor: () => 0, // floor far below so only the cap can fire
    // wire a live diag mirror so the shell keeps it current and the fatal can dump it
    diag: {
      ring: [],
      cursor: 101,
      parentBlockHashSent: undefined,
      blocksDeliveredThisConn: 0,
      lastPreviousBlocks: undefined,
    },
    fetchImpl: mockForkFetch(conns, bodies),
  });
  let msg = '';
  try {
    for await (const _ of gen) {
      /* drain until the cap throws */
    }
  } catch (e) {
    msg = (e as Error).message;
  }
  expect(msg).toMatch(
    /consecutive .*409 fork-negotiations without cursor progress/i,
  );
  expect(msg).toMatch(/diag: cursor=\d+/); // the diag cursor is present
  expect(msg).toMatch(/ring\(last8\)=\[.*102:h102.*\]/); // the ring fragment carries the delivered tip
});

// ─────────────────────────────── reconcile anchor (finality boundary) ───────────────────────────────

test('reconcile: an EMPTY window with an anchor appends ONLY a child of the anchor (else duplicate/gap)', () => {
  const anchor = L(10, 'a', 'z');
  expect(reconcile([], L(11, 'b', 'a'), anchor)).toEqual({ kind: 'append' });
  // the anchor block itself re-delivered (reopen from the finalized cursor) is idempotent
  expect(reconcile([], L(10, 'a', 'z'), anchor)).toEqual({
    kind: 'duplicate',
  });
  // a block that does NOT extend the finalized anchor: skipped span or wrong fork → gap (fatal), the
  // pre-anchor blind append was undetectable
  expect(reconcile([], L(12, 'c', 'unknown'), anchor)).toEqual({
    kind: 'gap',
  });
});

test('reconcile: a depth-1 fork at the finality boundary reorgs off the ANCHOR instead of a fatal gap', () => {
  const anchor = L(10, 'a', 'z');
  const window = [L(11, 'b', 'a')];
  const r = reconcile(window, L(11, 'b2', 'a'), anchor); // 11' whose parent IS the finalized block
  expect(r.kind).toBe('reorg');
  if (r.kind === 'reorg') {
    expect(r.commonAncestor.hash).toBe('a'); // the anchor is the known-safe common ancestor
    expect(r.reorgedBlocks.map((b) => b.hash)).toEqual(['b']); // the whole window is reorged
  }
  // under an anchor that is NOT the fork parent this same shape stays a fatal gap
  expect(reconcile(window, L(11, 'b2', 'a'), L(8, 'w', 'v')).kind).toBe('gap');
});

// ─────────────────────────────── redelivery handshake + finalize guards ───────────────────────────────

test('portalRealtimeEvents: an AWAITED duplicate is re-emitted with the new logs (same-block child discovery); a routine duplicate stays skipped', async () => {
  const h = (n: number, hash: string, parent: string, ts: number) => ({
    number: n,
    hash,
    parentHash: parent,
    timestamp: ts,
  });
  const creation = {
    address: '0xfactory',
    topics: ['0xcreated'],
    data: '0x',
    logIndex: 0,
    transactionHash: '0xtx1',
    transactionIndex: 0,
  };
  const childLog = {
    address: '0xchild',
    topics: ['0xdeposit'],
    data: '0x',
    logIndex: 1,
    transactionHash: '0xtx1',
    transactionIndex: 0,
  };
  const tx = {
    transactionIndex: 0,
    hash: '0xtx1',
    from: '0xfrom',
    to: '0xto',
    input: '0x',
    value: '0x0',
    nonce: 0,
    gas: '0x1',
    type: 0,
  };
  const batches = [
    // the incomplete first delivery (old server filter): only the creation log
    { header: h(100, 'h100', 'h99', 1000), logs: [creation] },
    // the awaited redelivery (widened filter): creation + the child's own same-block log + parent tx
    {
      header: h(100, 'h100', 'h99', 1000),
      logs: [creation, childLog],
      transactions: [tx],
    },
    // a routine duplicate of the same block — NOT awaited → skipped (no triple delivery)
    { header: h(100, 'h100', 'h99', 1000), logs: [creation, childLog] },
    { header: h(101, 'h101', 'h100', 1012), logs: [] },
  ];
  let awaitHash: string | undefined = 'h100'; // the consumer awaits exactly one redelivery of h100
  const ac = new AbortController();
  const events: any[] = [];
  for await (const e of portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => ({ number: 0 }),
    finalizePollMs: 999999,
    shouldRedeliver: (hash) => {
      if (awaitHash !== hash) return false;
      awaitHash = undefined; // consumed — later duplicates are routine

      return true;
    },
  })) {
    events.push(e);
  }
  const blocks = events.filter((e) => e.type === 'block');
  expect(blocks.length).toBe(3); // 100 (incomplete), 100 (redelivered), 101 — the 3rd duplicate skipped
  expect(blocks[1].block.number).toBe('0x64'); // the redelivery is the SAME block…
  expect(blocks[1].logs.map((l: any) => l.address)).toEqual([
    '0xfactory',
    '0xchild',
  ]); // …with the widened filter's logs
  expect(blocks[1].transactions).toHaveLength(1); // parent txs ride the stream (toSyncTransaction shape)
  expect(blocks[1].transactions[0].hash).toBe('0xtx1');
  expect(events.some((e) => e.type === 'reorg')).toBe(false); // no spurious reorg from the redelivery
  expect(blocks[2].block.number).toBe('0x65'); // the chain continues cleanly past it
});

test('portalRealtimeEvents: a finalize whose canonical hash mismatches the local block is FATAL (wrong-fork finalize)', async () => {
  // takeFinalized splits by NUMBER; when /finalized-head carries the canonical hash and our local block
  // at that height differs, the window is on a fork that lost to a finalized competitor. Persisting it
  // as finalized would commit wrong-fork data with no rollback event — fail loud instead.
  const batches = [
    {
      header: { number: 10, hash: 'a', parentHash: 'z', timestamp: 10 },
      logs: [],
    },
  ];
  const ac = new AbortController();
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 10,
    anchor: L(9, 'z', 'y'),
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => ({ number: 10, hash: 'a-canonical' }), // ≠ local 'a'
    finalizePollMs: 0,
  });
  await expect(
    (async () => {
      for await (const _ of iter) {
        /* drain */
      }
    })(),
  ).rejects.toThrow(/losing fork/i);
});

test('portalRealtimeEvents: a hash-carrying finalize ABOVE the local tip is DEFERRED, not applied by number — it lands only once the window reaches a hash-verifiable boundary (review B1)', async () => {
  // The probe carries the canonical hash for block 12, but the window is still catching up: takeFinalized
  // splits by NUMBER, so a naive guard would finalize block 10/11 by number with NO way to check they
  // descend from canonical 12 — persisting a possibly-losing fork below finality. The fix DEFERS the
  // hash-unverifiable finalize until the window reaches height 12, where the local hash IS checkable.
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
      header: { number: 12, hash: 'c', parentHash: 'b', timestamp: 12 },
      logs: [],
    },
  ];
  const ac = new AbortController();
  const events: any[] = [];
  for await (const e of portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 10,
    anchor: L(9, 'z', 'y'),
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    // canonical head is block 12 (hash 'c'); it is stable across polls — the window catches up to it
    finalizedHead: async () => ({ number: 12, hash: 'c' }),
    finalizePollMs: 0, // poll on every block so the deferral is exercised at heights 10 and 11
  })) {
    events.push(e);
  }
  const finalizes = events.filter((e) => e.type === 'finalize');
  // EXACTLY ONE finalize, at block 12 — the polls at heights 10 and 11 deferred (no finalize(10)/(11)).
  expect(finalizes.map((f: any) => f.block.number)).toEqual([12]);
  expect(finalizes[0]!.block.hash).toBe('c');
});

test('portalRealtimeEvents: tick-driven finalize during a 204 stall (RT-1 SC2 T1)', async () => {
  const enc = new TextEncoder();
  const block100 = {
    header: {
      number: 100,
      hash: 'h100',
      parentHash: 'h99',
      timestamp: 100,
    },
    logs: [],
  };
  let request = 0;
  const fetchImpl = (async () => {
    request += 1;
    if (request === 1) {
      const body = new ReadableStream({
        start(c) {
          c.enqueue(enc.encode(`${JSON.stringify(block100)}\n`));
          c.close();
        },
      });

      return { status: 200, ok: true, body };
    }

    return { status: 204, ok: false, body: null };
  }) as any;
  let headCalls = 0;
  const ac = new AbortController();
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl,
    signal: ac.signal,
    finalizedHead: async () => {
      headCalls += 1;

      return headCalls === 1
        ? { number: 99, hash: 'h99' }
        : { number: 100, hash: 'h100' };
    },
    finalizePollMs: 50,
  });
  const events: any[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  const sentinel = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error('HEARTBEAT-STARVED: finalize never emitted')),
      2000,
    );
  });

  try {
    const finalize = await Promise.race([
      (async () => {
        for await (const e of iter) {
          events.push(e);
          if (e.type === 'finalize') return e;
        }

        throw new Error('HEARTBEAT-STARVED: generator ended before finalize');
      })(),
      sentinel,
    ]);
    const blocks = events.filter((e) => e.type === 'block');
    expect(blocks).toHaveLength(1);
    expect(finalize.block.number).toBe(100);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    ac.abort();
  }
});

test('portalRealtimeEvents: heartbeat ticks keep finalize polling on a 204-only wall-clock cadence (RT-1 SC2 T2)', async () => {
  const fetchImpl = (async () => ({
    status: 204,
    ok: false,
    body: null,
  })) as any;
  const pollTimes: number[] = [];
  const ac = new AbortController();
  const events: any[] = [];
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl,
    signal: ac.signal,
    finalizedHead: async () => {
      pollTimes.push(Date.now());

      return { number: 99, hash: 'h99' };
    },
    finalizePollMs: 100,
  });
  const drain = (async () => {
    for await (const e of iter) events.push(e);
  })();

  await sleep(1200);
  ac.abort();
  await drain;

  expect(pollTimes.length).toBeGreaterThanOrEqual(5);
  expect(events).toEqual([]);
});

test('portalRealtimeEvents: B1 defer fatal fires on wall-clock during no-delivery (RT-1 SC2 T3)', async () => {
  const enc = new TextEncoder();
  const block100 = {
    header: {
      number: 100,
      hash: 'h100',
      parentHash: 'h99',
      timestamp: 100,
    },
    logs: [],
  };
  let request = 0;
  const fetchImpl = (async () => {
    request += 1;
    if (request === 1) {
      const body = new ReadableStream({
        start(c) {
          c.enqueue(enc.encode(`${JSON.stringify(block100)}\n`));
          c.close();
        },
      });

      return { status: 200, ok: true, body };
    }

    return { status: 204, ok: false, body: null };
  }) as any;
  const ac = new AbortController();
  const events: any[] = [];
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl,
    signal: ac.signal,
    finalizedHead: async () => ({ number: 105, hash: '0xh105' }),
    finalizePollMs: 50,
    finalizeDeferMaxMs: 300,
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const sentinel = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(new Error('HEARTBEAT-STARVED: B1 defer watchdog never fired')),
      2000,
    );
  });

  try {
    await expect(
      Promise.race([
        (async () => {
          for await (const e of iter) events.push(e);
        })(),
        sentinel,
      ]),
    ).rejects.toThrow(/lagged the hash-carrying finalized head/);
    const blocks = events.filter((e) => e.type === 'block');
    expect(blocks).toHaveLength(1);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    ac.abort();
  }
});

// A /stream fetch that lazily yields an UNBOUNDED, strictly-increasing block chain (never 204s), so the
// window keeps advancing but never catches a finalized head that also keeps climbing above it. Used for the
// B1 starvation test — the block chain is infinite, so only the watchdog throw terminates the generator.
function unboundedFetch(fromBlock: number) {
  return (async (_url: string, init: any) => {
    const start = Math.max(
      JSON.parse(init.body).fromBlock as number,
      fromBlock,
    );
    let n = start;
    const enc = new TextEncoder();
    const body = new ReadableStream({
      pull(c) {
        const header = {
          number: n,
          hash: `h${n}`,
          parentHash: n === fromBlock ? 'z' : `h${n - 1}`,
          timestamp: n,
        };
        c.enqueue(enc.encode(`${JSON.stringify({ header, logs: [] })}\n`));
        n += 1;
      },
    });
    return { status: 200, ok: true, body };
  }) as any;
}

afterEach(() => {
  vi.restoreAllMocks();
});

test('portalRealtimeEvents: a MOVING hash-carrying finalized head that stays ABOVE the window forever is bounded — the B1 deferral fails loud instead of starving finality (delta review B1)', async () => {
  // Portal brownout: /stream delivery lags the chain, so the canonical finalized head keeps climbing ABOVE
  // the local window tip on EVERY poll. takeFinalized returns the window tip (always < fhNumber), so every
  // poll defers — the anchor never advances, `unfinalized` grows without bound, and ponder's finalized
  // checkpoint silently freezes. The streak watchdog must turn this into a LOUD fatal.
  //
  // Deterministic clock: each finalizedHead() call (exactly one per poll) advances Date.now by 40ms; with a
  // 100ms bound the streak arms on the first defer and trips a few polls later. Without a real wall clock the
  // streak-start delta would stay 0 and the loop would spin forever, so the clock drive IS the test.
  let clock = 1_000_000;
  vi.spyOn(Date, 'now').mockImplementation(() => clock);
  const ac = new AbortController();
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 10,
    anchor: L(9, 'z', 'y'),
    logs: [],
    fetchImpl: unboundedFetch(10),
    signal: ac.signal,
    // hash-carrying head, ALWAYS far above whatever height the window has crawled to → takeFinalized's tip
    // is forever < fhNumber and hash-unverifiable ⇒ perpetual defer. Advance the clock once per poll here.
    finalizedHead: async () => {
      clock += 40;
      return { number: 1_000_000, hash: 'canon' };
    },
    finalizePollMs: 0, // poll every block so the streak is exercised
    finalizeDeferMaxMs: 100, // tiny bound: a few deferrals exceed it
  });
  // Drain with a hard iteration cap so a REGRESSION (streak bound removed) surfaces as a clean, distinct
  // failure (the sentinel) instead of an infinite hang: with the fix the watchdog throws within a handful of
  // polls, far under the cap; without it the deferral loop would spin forever (the starvation bug itself).
  await expect(
    (async () => {
      let seen = 0;
      for await (const _ of iter) {
        seen += 1;
        if (seen > 5000) {
          throw new Error(
            'NEVER-THROWN: the deferral was not bounded — finality starved without a fatal (regression)',
          );
        }
      }
    })(),
  ).rejects.toThrow(/lagged the hash-carrying finalized head/i);
  ac.abort();
});

test('portalRealtimeEvents: a deferral that CATCHES UP clears the streak — later deferrals do NOT accumulate into a false watchdog throw even past the bound (delta review B1)', async () => {
  // Guards the reset side. Two defer-then-catch-up rounds:
  //   • head = 11: the window defers ONCE at block 10, then REACHES 11 → finalize(11) (streak clears).
  //   • head jumps to 13: defers ONCE at 12, then REACHES 13 → finalize(13).
  // The TOTAL wall clock across the whole run far exceeds the bound, but no single unbroken defer streak
  // does — so a correctly-RESETTING streak never throws. (A streak that never reset would fire at the
  // second round, since its start would still hold the first round's timestamp.)
  let clock = 2_000_000;
  vi.spyOn(Date, 'now').mockImplementation(() => {
    const t = clock;
    clock += 60; // 60ms/call: one defer gap < the 100ms bound, but the full run spans >100ms total
    return t;
  });
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
      header: { number: 12, hash: 'c', parentHash: 'b', timestamp: 12 },
      logs: [],
    },
    {
      header: { number: 13, hash: 'd', parentHash: 'c', timestamp: 13 },
      logs: [],
    },
  ];
  // Head advances 11 → 13 once the window has passed 11, so round 2 defers at 12 then finalizes at 13.
  let head: { number: number; hash: string } = { number: 11, hash: 'b' };
  const ac = new AbortController();
  const events: any[] = [];
  for await (const e of portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 10,
    anchor: L(9, 'z', 'y'),
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => head,
    finalizePollMs: 0,
    finalizeDeferMaxMs: 100, // single defer gap (60ms) < bound; cumulative run (>200ms) > bound
  })) {
    events.push(e);
    if (e.type === 'finalize' && e.block.number === 11)
      head = { number: 13, hash: 'd' };
  }
  const finalizes = events.filter((e) => e.type === 'finalize');
  // both hash-verifiable boundaries finalized, no false fatal — the streak reset at each catch-up
  expect(finalizes.map((f: any) => f.block.number)).toEqual([11, 13]);
});

test('sleep: does not leak an abort listener per call on a shared long-lived signal (issue #28)', async () => {
  // streamHotBlocks passes the same per-chain AbortSignal into sleep() on every poll for the life of the
  // process; `{ once: true }` only removes the listener when abort FIRES, so the normal timer path leaked
  // one listener per call (~3600/h → a MaxListenersExceededWarning storm + a real memory leak). After the
  // timers fire, the listener count on the signal must return to baseline, not grow with the call count.
  const ac = new AbortController();
  const before = getEventListeners(ac.signal, 'abort').length;
  await Promise.all(Array.from({ length: 200 }, () => sleep(1, ac.signal)));
  const after = getEventListeners(ac.signal, 'abort').length;
  // every call deregistered on its normal timer path → no accumulation (pre-fix: 200 dangling listeners)
  expect(after).toBe(before);
  ac.abort();
});

test('streamHotBlocks: a deterministic 4xx from /stream is FATAL, not an infinite silent retry loop', async () => {
  const fetchImpl = (async () => ({
    status: 400,
    ok: false,
    body: { cancel: async () => {} },
  })) as any;
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [],
    fetchImpl,
  });
  await expect(gen.next()).rejects.toThrow(/deterministic/);
});

test('streamHotBlocks: a DROPPABLE tx-field 400 (dataset lacks access_list) degrades like the historical path — drop the field and retry, not fatal (review B3)', async () => {
  // TX_FIELDS projects accessList, which is DROPPABLE (non-typed txs lack it) and which the historical
  // client degrades on a schema-field 400. Stream mode used to treat ANY non-429 4xx as fatal, so a dataset
  // historical handles fine (no access_list) refused stream mode entirely. Now the /stream degrades the same
  // droppable tx field and retries.
  const textBody = (s: string) =>
    new ReadableStream({
      start(c) {
        c.enqueue(new TextEncoder().encode(s));
        c.close();
      },
    });
  const bodies: any[] = [];
  let conn = 0;
  const fetchImpl = (async (_url: string, init: any) => {
    bodies.push(JSON.parse(init.body));
    conn += 1;
    if (conn === 1)
      // first request (with accessList) 400s exactly the way the Portal reports a missing parquet column
      return {
        status: 400,
        ok: false,
        body: textBody("column 'access_list' is not found in 'transactions'"),
      };
    if (conn === 2)
      // retry (accessList dropped) succeeds and streams a block
      return {
        status: 200,
        ok: true,
        body: textBody(
          `${JSON.stringify({
            header: {
              number: 100,
              hash: 'h100',
              parentHash: 'h99',
              timestamp: 1,
            },
            logs: [],
            transactions: [],
          })}\n`,
        ),
      };
    return { status: 204, ok: false, body: null };
  }) as any;

  const ac = new AbortController();
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [{ address: ['0xf'], topic0: ['0xt'] }],
    txFields: TX_FIELDS,
    fetchImpl,
    signal: ac.signal,
  });
  const first = await nextBlock(gen); // NOT a throw — the field is dropped and the retry delivers block 100
  expect(first.value?.header.number).toBe(100);
  // the first request carried accessList; the retry DROPPED it (kept the other tx fields)
  expect(bodies[0].fields.transaction.accessList).toBe(true);
  expect(bodies[1].fields.transaction.accessList).toBeUndefined();
  expect(bodies[1].fields.transaction.hash).toBe(true); // only the droppable field was removed
  await gen.return(undefined);
  ac.abort();
});

test('streamHotBlocks: a DROPPABLE BLOCK-field 400 (dataset lacks mix_hash) degrades too — B3 covered only transaction.*, leaving every droppable block field a fatal (wave 4)', async () => {
  // The wire always projects BLOCK_FIELDS, which includes five DROPPABLE nullable block columns
  // (mixHash, nonce, sha3Uncles, totalDifficulty, baseFeePerGas). The historical client degrades ANY of
  // them via stripFields, so a dataset without e.g. mix_hash backfills fine — but the B3 degradation
  // here required tableKey === 'transaction', so the SAME dataset fataled the moment stream realtime
  // started, on every restart. DROPPABLE_FIELDS itself is the whitelist; the table restriction is gone.
  const textBody = (s: string) =>
    new ReadableStream({
      start(c) {
        c.enqueue(new TextEncoder().encode(s));
        c.close();
      },
    });
  const bodies: any[] = [];
  let conn = 0;
  const fetchImpl = (async (_url: string, init: any) => {
    bodies.push(JSON.parse(init.body));
    conn += 1;
    if (conn === 1)
      // first request (with mixHash) 400s exactly the way the Portal reports a missing parquet column
      return {
        status: 400,
        ok: false,
        body: textBody("column 'mix_hash' is not found in 'blocks'"),
      };
    if (conn === 2)
      // retry (mixHash dropped) succeeds and streams a block
      return {
        status: 200,
        ok: true,
        body: textBody(
          `${JSON.stringify({
            header: {
              number: 100,
              hash: 'h100',
              parentHash: 'h99',
              timestamp: 1,
            },
            logs: [],
            transactions: [],
          })}\n`,
        ),
      };
    return { status: 204, ok: false, body: null };
  }) as any;

  const ac = new AbortController();
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [{ address: ['0xf'], topic0: ['0xt'] }],
    blockFields: BLOCK_FIELDS,
    fetchImpl,
    signal: ac.signal,
  });
  const first = await nextBlock(gen); // NOT a throw — the field is dropped and the retry delivers block 100
  expect(first.value?.header.number).toBe(100);
  // the first request carried mixHash; the retry DROPPED it (kept the required block fields)
  expect(bodies[0].fields.block.mixHash).toBe(true);
  expect(bodies[1].fields.block.mixHash).toBeUndefined();
  expect(bodies[1].fields.block.number).toBe(true); // the linkage-required fields stay
  expect(bodies[1].fields.block.parentHash).toBe(true);
  await gen.return(undefined);
  ac.abort();
});

// ─────────────────────────────── RT-1 SC1: idle-bounded read + tick-transparent line-wait ───────────────────────────────

// A /stream body that emits exactly ONE block line then stays OPEN and SILENT — never enqueues another
// line, never closes (no 204, no FIN/RST). This is the wedged-connection / silent-open state (R1): a plain
// `for await` over `ndjsonLines` suspends here FOREVER, so no ticks reach the consumer and finalize starves.
// `signal` (the test's AbortController) errors the body on teardown so a pending `reader.read()` SETTLES —
// otherwise, under the idle-bound NEUTER where the read never idle-expires, the never-settling read keeps
// vitest's event loop alive and the process hangs AFTER the assertion (a real hang, not just a slow test).
// A real fetch aborts its body on the request signal; the mock must model that to be drainable. (RT-1 SC1)
function silentOpenBlock(
  block: unknown,
  signal: AbortSignal,
): {
  status: number;
  ok: boolean;
  body: ReadableStream<Uint8Array>;
} {
  const enc = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      c.enqueue(enc.encode(`${JSON.stringify(block)}\n`));
      // deliberately NOT closed and NOTHING more enqueued — the read suspends open-but-silent, until abort
      const onAbort = (): void => {
        try {
          c.error(new Error('aborted'));
        } catch {
          /* already closed/errored */
        }
      };
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    },
  });

  return { status: 200, ok: true, body };
}

test('portalRealtimeEvents: tick-transparent line-wait keeps finalize polling on a SILENT-OPEN connection (RT-1 SC1)', async () => {
  const block100 = {
    header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 100 },
    logs: [],
  };
  let request = 0;
  const ac = new AbortController();
  // Request 1 delivers block 100 then holds the connection OPEN and SILENT (never a second line, never a
  // close, never a 204). On pre-SC1 code the `for await` suspends on this open body forever → the consumer
  // gets no ticks → finalize is never re-polled → the sentinel fires. With the tick-transparent line-wait,
  // heartbeat ticks drive finalize on wall-clock cadence WHILE the connection stays open. `idleMs` is set
  // large (30s) so the idle-reconnect path does NOT run within the test window — this isolates G2b (the
  // in-connection line-wait) from G2a (the idle reconnect). Any request ≥ 2 would only occur on a reconnect,
  // which must NOT happen here.
  const fetchImpl = (async () => {
    request += 1;
    if (request === 1) return silentOpenBlock(block100, ac.signal);

    return { status: 204, ok: false, body: null };
  }) as any;
  let headCalls = 0;
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl,
    signal: ac.signal,
    idleMs: 30_000, // large: no idle reconnect within the window — proves G2b alone
    finalizedHead: async () => {
      headCalls += 1;

      return headCalls === 1
        ? { number: 99, hash: 'h99' }
        : { number: 100, hash: 'h100' };
    },
    finalizePollMs: 50,
  });
  const events: any[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  const sentinel = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error('SILENT-OPEN-STARVED: finalize never emitted')),
      2000,
    );
  });

  try {
    const finalize = await Promise.race([
      (async () => {
        for await (const e of iter) {
          events.push(e);
          if (e.type === 'finalize') return e;
        }

        throw new Error('SILENT-OPEN-STARVED: generator ended before finalize');
      })(),
      sentinel,
    ]);
    const blocks = events.filter((e) => e.type === 'block');
    // Exactly ONE block was delivered; finalize was driven by the tick clock during the silent-open window,
    // NOT by a second delivery (there is none). This is the R1 proof.
    expect(blocks).toHaveLength(1);
    expect(finalize.block.number).toBe(100);
    // No reconnect happened — the large idleMs means the single open connection served the whole test.
    expect(request).toBe(1);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    ac.abort();
  }
});

test('streamHotBlocks: idle bound reconnects from cursor on a silent-open stream (RT-1 SC1)', async () => {
  const block100 = {
    header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 100 },
    logs: [],
  };
  const requestCursors: number[] = [];
  const ac = new AbortController();
  // Request 1 delivers block 100 then goes SILENT-OPEN. With a small idleMs the read hits its idle bound and
  // reconnects from `cursor` (now 101, past block 100). Request 2 records that advanced cursor and 204s. On
  // neutered code (idleMs not passed / removed) the read suspends forever → request 2 never happens → the
  // sentinel fires.
  const fetchImpl = (async (_url: string, init: any) => {
    const from = JSON.parse(init.body).fromBlock as number;
    requestCursors.push(from);
    if (requestCursors.length === 1)
      return silentOpenBlock(block100, ac.signal);

    return { status: 204, ok: false, body: null };
  }) as any;
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [],
    blockFields: BLOCK_FIELDS,
    fetchImpl,
    signal: ac.signal,
    idleMs: 150, // small: force the idle bound to fire well within the sentinel window
    tickSleepMs: 20,
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const sentinel = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error('IDLE-RECONNECT-STARVED: never reconnected')),
      2000,
    );
  });

  // `stop` lets the detached drive loop below terminate the instant we tear down — without it the loop keeps
  // pulling ticks every tickSleepMs FOREVER (under the neuter no reconnect ever ends it), keeping vitest's
  // event loop alive so the process hangs even after the sentinel fails. (cto-spec §4: never an open hang)
  let stop = false;
  try {
    // Drive the generator, pulling ticks/blocks, until the mock has served request 2 (the reconnect) at the
    // advanced cursor 101, or the sentinel fires.
    await Promise.race([
      (async () => {
        for (;;) {
          const r = await gen.next();
          if (r.done || stop) return;
          if (requestCursors.length >= 2) return;
        }
      })(),
      sentinel,
    ]);
    expect(requestCursors[0]).toBe(100); // first connection opened at fromBlock
    expect(requestCursors[1]).toBe(101); // reconnect resumed from cursor PAST block 100 — no re-fetch/skip
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    stop = true;
    ac.abort();
    // Fire-and-forget teardown (cto-spec §4: never an open hang). Under the idle-bound NEUTER the reader
    // never settles on the silent-open body, so `await gen.return()` would itself block and mask the
    // sentinel failure — the assertion above has already run, so we do not await the generator's unwind.
    void gen.return(undefined).catch(() => {});
  }
});

test('streamHotBlocks: idle bound fires onIdleReconnect and a slow-but-alive stream is NOT cut (RT-1 SC1)', async () => {
  const enc = new TextEncoder();
  let idleCalls = 0;
  let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  const requests: number[] = [];
  // A single connection that stays open. It delivers block 100 immediately, then — critically — enqueues a
  // heartbeat-frequency stream of blocks slowly enough to prove the connection is NEVER cut while alive
  // (each chunk re-arms the idle guard), but the SECOND connection (if a reconnect ever happens) 204s. We
  // deliver blocks 100..104 spaced ~40ms apart under a 150ms idle bound: 40ms < 150ms, so the guard re-arms
  // every chunk and the stream is never recycled — onIdleReconnect must stay at 0.
  const fetchImpl = (async (_url: string, init: any) => {
    requests.push(JSON.parse(init.body).fromBlock as number);
    if (requests.length > 1) return { status: 204, ok: false, body: null };

    const body = new ReadableStream<Uint8Array>({
      start(c) {
        controller = c;
        c.enqueue(
          enc.encode(
            `${JSON.stringify({ header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 100 }, logs: [] })}\n`,
          ),
        );
      },
    });

    return { status: 200, ok: true, body };
  }) as any;
  const ac = new AbortController();
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [],
    blockFields: BLOCK_FIELDS,
    fetchImpl,
    signal: ac.signal,
    idleMs: 150,
    tickSleepMs: 20,
    onIdleReconnect: () => {
      idleCalls += 1;
    },
  });
  // Feed a live-but-slow block every ~40ms (< idleMs 150) so the guard re-arms and never cuts.
  let n = 101;
  const feeder = setInterval(() => {
    if (controller === undefined || n > 104) return;

    controller.enqueue(
      enc.encode(
        `${JSON.stringify({ header: { number: n, hash: `h${n}`, parentHash: `h${n - 1}`, timestamp: n }, logs: [] })}\n`,
      ),
    );
    n += 1;
  }, 40);
  try {
    const blocks: number[] = [];
    let timer: ReturnType<typeof setTimeout> | undefined;
    const done = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, 400); // drive ~10 heartbeats + 5 slow blocks
    });
    const drive = (async () => {
      for (;;) {
        const r = await gen.next();
        if (r.done) return;
        if (r.value.kind === 'block') blocks.push(r.value.header.number);
        if (blocks.length >= 5) return;
      }
    })();
    await Promise.race([drive, done]);
    if (timer !== undefined) clearTimeout(timer);
    // The stream was alive (blocks kept arriving under the idle bound) → NEVER recycled, NEVER reconnected.
    expect(idleCalls).toBe(0);
    expect(requests.length).toBe(1);
    expect(blocks).toContain(100);
  } finally {
    clearInterval(feeder);
    ac.abort();
    await gen.return(undefined);
  }
});

// ─────────────────────────────── RT-1 SC1 B1: no unhandled rejection from an abandoned readerP ───────────────────────────────

// Capture unhandledRejection for the duration of `fn`. Vitest installs its OWN unhandledRejection listener
// that fails the whole run — so we SWAP the process listeners for our sentinel-only handler, run, wait one
// macrotask turn for a pending readerP's abort/idle rejection to surface, then RESTORE the originals. The
// return is the list of captured reasons; the fix asserts it is EMPTY. On pre-fix code an abandoned readerP
// (created at `it.next()`, its `reader.read()` rejected when the signal-aborted body errors) has no handler
// → it lands here non-empty. (RT-1 SC1 / B1)
async function captureUnhandledRejections(
  fn: () => Promise<void>,
): Promise<unknown[]> {
  const captured: unknown[] = [];
  const prior = process.listeners('unhandledRejection');
  for (const l of prior) {
    process.removeListener('unhandledRejection', l);
  }
  const onReject = (reason: unknown): void => {
    captured.push(reason);
  };
  process.on('unhandledRejection', onReject);
  try {
    await fn();
    // A rejection abandoned in a race surfaces on a LATER microtask/macrotask than the abort. Drain both:
    // a macrotask turn (setTimeout 0) flushes the microtask queue after it, so a `readerP` that rejects on
    // the aborted body (or a fired idle timer) has settled and dispatched to `unhandledRejection` by here.
    await new Promise<void>((resolve) => setTimeout(resolve, 50));
  } finally {
    process.removeListener('unhandledRejection', onReject);
    for (const l of prior) {
      process.on('unhandledRejection', l as (r: unknown) => void);
    }
  }

  return captured;
}

test('streamHotBlocks: B1-A — abort at the first loop-top guard (200 open body) does not unhandled-reject the initial readerP (RT-1 SC1)', async () => {
  const block100 = {
    header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 100 },
    logs: [],
  };
  // Entry A: the signal is ALREADY aborted when execution reaches the INNER loop-top `if
  // (args.signal?.aborted) break;` on the FIRST iteration — abort fired during/just after the /stream fetch
  // resolved 200 with an open body. Crucially the abort must fire AFTER the OUTER loop-top guard (which
  // would `return` before ever fetching on a pre-aborted signal) — so we abort from INSIDE fetchImpl, right
  // before handing back the 200 body: the outer guard already passed, the fetch resolves, the initial
  // `readerP = it.next()` is created (a pending reader.read() on a body wired to error on this signal), and
  // then the inner loop-top guard breaks BEFORE the first raceHeartbeat — abandoning that readerP with no
  // re-race handler. On pre-fix code its abort rejection is unhandled; the B1 `.catch` swallows it.
  const ac = new AbortController();
  const fetchImpl = (async () => {
    const r = silentOpenBlock(block100, ac.signal);
    // Abort now — the outer loop-top guard has already passed (fetch is running), so the generator does NOT
    // early-return; it opens the body, creates the initial readerP, then breaks at the inner guard. The
    // body's controller.error fires on this abort, rejecting the pending reader.read() in that readerP.
    ac.abort();

    return r;
  }) as any;
  const captured = await captureUnhandledRejections(async () => {
    const gen = streamHotBlocks({
      portalUrl: 'http://portal',
      headers: {},
      fromBlock: 100,
      logs: [],
      blockFields: BLOCK_FIELDS,
      fetchImpl,
      signal: ac.signal,
      idleMs: 30_000, // large: the abort, not the idle timer, is what settles the read
      tickSleepMs: 20,
    });
    let timer: ReturnType<typeof setTimeout> | undefined;
    const guard = new Promise<IteratorResult<HotItem>>((resolve) => {
      timer = setTimeout(() => resolve({ done: true, value: undefined }), 2000);
    });
    await Promise.race([gen.next(), guard]);
    if (timer !== undefined) clearTimeout(timer);
    await gen.return(undefined).catch(() => {});
  });

  // The abandoned initial readerP must NOT unhandled-reject. Pre-fix: captured carries the body abort error.
  expect(captured).toEqual([]);
});

test('streamHotBlocks: B1-B — .return() while paused at the block yield does not unhandled-reject the prefetched readerP (RT-1 SC1)', async () => {
  const block100 = {
    header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 100 },
    logs: [],
  };
  // Entry B: after yielding block 100 the driver has ALREADY prefetched `readerP = it.next()` (a pending
  // reader.read() on the still-open body). The consumer then ABANDONS the generator at that yield via
  // `.return()` — async-generator semantics: `.return()` behind a pending `.next()` does NOT observe that
  // pending `.next()`'s later rejection, so the prefetched readerP is left without a re-race handler. When
  // the body then errors on abort, that readerP rejects unobserved. On pre-fix code it lands in `captured`;
  // the B1 `.catch` on the prefetch swallows it.
  const ac = new AbortController();
  const fetchImpl = (async () => silentOpenBlock(block100, ac.signal)) as any;
  const captured = await captureUnhandledRejections(async () => {
    const gen = streamHotBlocks({
      portalUrl: 'http://portal',
      headers: {},
      fromBlock: 100,
      logs: [],
      blockFields: BLOCK_FIELDS,
      fetchImpl,
      signal: ac.signal,
      idleMs: 30_000, // large: the abort, not the idle timer, settles the prefetched read
      tickSleepMs: 20,
    });
    // Pull until block 100 arrives — at which point the driver is paused at the block yield with the NEXT
    // read already prefetched into readerP.
    const first = await nextBlock(gen);
    expect(first.done).toBe(false);
    expect(first.value.kind).toBe('block');
    // Abandon the generator AT the yield. `.return()` runs the driver's finally (it.return → body cancel),
    // but does not observe the prefetched readerP's pending rejection. Abort errors the body so that
    // prefetched reader.read() rejects.
    await gen.return(undefined).catch(() => {});
    ac.abort();
  });

  // The abandoned prefetched readerP must NOT unhandled-reject. Pre-fix: captured carries the abort error.
  expect(captured).toEqual([]);
});

// ─────────────────────────────── RT-1 SC3: delivery-progress watchdog (RT-G10 / INV-24) ───────────────────────────────

// A /stream fetch that delivers exactly ONE block on the first request, then 204s FOREVER after — the
// no-delivery-but-open-loop state the delivery watchdog must bound. NUMBER-ONLY finalized head (no hash) is
// used deliberately so the B1 hash-unverifiable defer watchdog (which requires a hash) can NEVER fire —
// isolating SC3 as the sole fatal path. `advanceHead` climbs the returned head per poll to model a chain
// whose finality keeps advancing while the stream starves us. (RT-1 SC3)
function oneBlockThen204(block: unknown, onExhausted?: () => void) {
  let served = false;
  const enc = new TextEncoder();
  return (async () => {
    if (served) {
      onExhausted?.();

      return { status: 204, ok: false, body: null };
    }
    served = true;
    const body = new ReadableStream({
      start(c) {
        c.enqueue(enc.encode(`${JSON.stringify(block)}\n`));
        c.close();
      },
    });

    return { status: 200, ok: true, body };
  }) as any;
}

test('portalRealtimeEvents: the finalized head advancing while ZERO blocks are delivered is FATAL — the delivery-progress watchdog fires (RT-1 SC3 T1)', async () => {
  // One block (100) is delivered and finalized number-only; the head then climbs far above forever while the
  // stream 204s. Zero further deliveries. With the head advancing ≥ threshold past the head-at-last-delivery
  // for the whole (tiny) bound, the delivery watchdog must throw its loud fatal — NOT stall silently.
  //
  // Deterministic clock: each finalizedHead() poll advances Date.now by 50ms (mirrors the B1 moving-head
  // test). With a 100ms bound the no-delivery clock crosses it after a couple of polls, at which point the
  // head has already climbed well past the 5-block threshold → fatal. Without a real wall clock the delta
  // stays 0 and the loop spins forever, so the clock drive IS the test.
  let clock = 3_000_000;
  vi.spyOn(Date, 'now').mockImplementation(() => clock);
  const block100 = {
    header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 100 },
    logs: [],
  };
  let headNumber = 100; // number-ONLY head: B1's hash-defer path is unreachable, isolating SC3
  const ac = new AbortController();
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl: oneBlockThen204(block100, () => {}),
    signal: ac.signal,
    finalizedHead: async () => {
      clock += 50; // advance the no-delivery clock once per poll
      headNumber += 5; // head climbs 5 blocks/poll — crosses the 5-block threshold quickly
      return { number: headNumber }; // NO hash → number-only finalize, never a B1 defer
    },
    finalizePollMs: 0, // poll every turn so the stall is exercised fast
    deliveryProgressMaxMs: 100, // tiny time bound
    deliveryProgressThreshold: 5, // small block threshold
    finalizeDeferMaxMs: 10_000, // large: prove the B1 path is NOT what fires
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const sentinel = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new Error('PROGRESS-WATCHDOG-STARVED: delivery watchdog never fired'),
        ),
      2000,
    );
  });
  const events: any[] = [];
  try {
    await expect(
      Promise.race([
        (async () => {
          let seen = 0;
          for await (const e of iter) {
            events.push(e);
            seen += 1;
            if (seen > 5000)
              throw new Error(
                'NEVER-THROWN: the stall was not bounded — delivery starved without a fatal (regression)',
              );
          }
        })(),
        sentinel,
      ]),
    ).rejects.toThrow(/delivered ZERO blocks for/i);
    // exactly the one block was delivered before the stall
    const blocks = events.filter((e) => e.type === 'block');
    expect(blocks).toHaveLength(1);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    ac.abort();
  }
});

test('portalRealtimeEvents: a QUIET chain (head STATIC) with zero delivery NEVER trips the watchdog — it is progress-conditioned, not a plain idle timeout (RT-1 SC3 T2)', async () => {
  // The head-static case is the whole point of PROGRESS-conditioning: an RPC-realtime-parity quiet chain
  // idles indefinitely without producing blocks, and that must NEVER be a fatal. The clock runs FAR past the
  // bound but the head never moves, so `fhNumber - lastDeliveryHead` stays 0 (< threshold) and the watchdog
  // holds its fire. A bounded poll count proves it TERMINATES cleanly (drained, no throw), not spins.
  let clock = 4_000_000;
  vi.spyOn(Date, 'now').mockImplementation(() => clock);
  const block100 = {
    header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 100 },
    logs: [],
  };
  let polls = 0;
  const ac = new AbortController();
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl: oneBlockThen204(block100, () => {}),
    signal: ac.signal,
    finalizedHead: async () => {
      clock += 1000; // clock races FAR past the 100ms bound…
      polls += 1;
      if (polls >= 40) ac.abort(); // …but a bounded run ends the test by aborting, not by a throw

      return { number: 100 }; // …while the head stays STATIC → never a threshold cross
    },
    finalizePollMs: 0,
    deliveryProgressMaxMs: 100, // tiny time bound — deliberately crossed many times over
    deliveryProgressThreshold: 5,
  });
  const events: any[] = [];
  // Must DRAIN to completion (abort) with NO fatal thrown — a static head is not a stall.
  for await (const e of iter) events.push(e);
  expect(events.filter((e) => e.type === 'block')).toHaveLength(1);
  expect(polls).toBeGreaterThanOrEqual(40); // the clock really did run past the bound many times
});

test('portalRealtimeEvents: a SINGLE-block finality lag (head advances by 1, below the threshold) with zero delivery does NOT trip the watchdog (RT-1 SC3 T3)', async () => {
  // A benign single-block finality lag: the head ticks forward by ONE while a block is momentarily in flight.
  // The time bound is crossed, but the head-advance (1) never reaches the threshold (5), so the watchdog must
  // hold its fire — proving BOTH conditions (time AND block advance) are required, not just the timer.
  let clock = 5_000_000;
  vi.spyOn(Date, 'now').mockImplementation(() => clock);
  const block100 = {
    header: { number: 100, hash: 'h100', parentHash: 'h99', timestamp: 100 },
    logs: [],
  };
  let polls = 0;
  const ac = new AbortController();
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    anchor: L(99, 'h99', 'h98'),
    logs: [],
    fetchImpl: oneBlockThen204(block100, () => {}),
    signal: ac.signal,
    finalizedHead: async () => {
      clock += 1000; // well past the 100ms bound
      polls += 1;
      if (polls >= 40) ac.abort();

      return { number: 101 }; // head is exactly ONE above the delivered block 100 → advance 1 < threshold 5
    },
    finalizePollMs: 0,
    deliveryProgressMaxMs: 100,
    deliveryProgressThreshold: 5,
  });
  const events: any[] = [];
  for await (const e of iter) events.push(e);
  expect(events.filter((e) => e.type === 'block')).toHaveLength(1);
  expect(polls).toBeGreaterThanOrEqual(40);
});

test('streamHotBlocks: a transient /stream fetch throw invokes onFetchError (the E1 non-delivery seam) and yields a tick, not a block (RT-1 SC3)', async () => {
  // The E1 site: `fetchImpl` THROWS (the read never opens). The producer must (a) surface it on the
  // onFetchError seam so the shell can rate-limit a warn, and (b) yield a `{kind:'tick'}` (non-delivery) —
  // never a block. The delivery watchdog counts this tick as non-delivery; this test pins the seam + shape.
  let calls = 0;
  const fetchImpl = (async () => {
    calls += 1;
    throw new Error('ECONNRESET');
  }) as any;
  let fetchErrors = 0;
  const ac = new AbortController();
  const gen = streamHotBlocks({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: 100,
    logs: [],
    blockFields: BLOCK_FIELDS,
    fetchImpl,
    signal: ac.signal,
    onFetchError: () => {
      fetchErrors += 1;
    },
    errorSleepMs: 1,
    tickSleepMs: 1,
  });
  const first = await gen.next();
  expect(first.done).toBe(false);
  expect(first.value.kind).toBe('tick'); // non-delivery: a tick, never a block
  expect(calls).toBe(1);
  expect(fetchErrors).toBe(1); // the E1 seam fired exactly once for the one throw
  await gen.return(undefined).catch(() => {});
  ac.abort();
});
