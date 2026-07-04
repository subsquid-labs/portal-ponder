import { expect, test } from 'vitest';
import { TX_FIELDS } from './portal-filters.js';
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
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => 0,
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

  const second = await gen.next(); // rev advanced → connection 1 torn down, reopen re-delivers block 100
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
  // without the anchor this same shape was a fatal gap
  expect(reconcile(window, L(11, 'b2', 'a')).kind).toBe('gap');
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
    logs: [],
    fetchImpl: mockFetch(batches, () => ac.abort()),
    signal: ac.signal,
    finalizedHead: async () => 0,
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
  const first = await gen.next(); // NOT a throw — the field is dropped and the retry delivers block 100
  expect(first.value?.header.number).toBe(100);
  // the first request carried accessList; the retry DROPPED it (kept the other tx fields)
  expect(bodies[0].fields.transaction.accessList).toBe(true);
  expect(bodies[1].fields.transaction.accessList).toBeUndefined();
  expect(bodies[1].fields.transaction.hash).toBe(true); // only the droppable field was removed
  await gen.return(undefined);
  ac.abort();
});
