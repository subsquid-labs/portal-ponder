import { expect, test } from 'vitest';
import {
  createPortalClient,
  ndjsonLines,
  type PortalClient,
  parseRetryAfterMs,
} from './portal-client.js';
import {
  isTransientError,
  PortalHttpError,
  PortalTruncatedBodyError,
} from './portal-errors.js';
import type { PortalQuery } from './portal-filters.js';
import type { Gate } from './portal-gate.js';
import { createStats } from './portal-metrics.js';

const fakeGate: Gate = {
  acquire: async () => {},
  release() {},
  onOk() {},
  onThrottle() {},
  addRows() {},
  freeRows() {},
  saturated: () => false,
  snapshot: () => ({ limit: 0, active: 0, rows: 0 }),
};
const QUERY: PortalQuery = { type: 'evm', fields: {} };

const streamOf = (chunks: string[]) =>
  new ReadableStream<Uint8Array>({
    start(c) {
      const enc = new TextEncoder();
      for (const ch of chunks) c.enqueue(enc.encode(ch));
      c.close();
    },
  });
const ndjsonRes = (blocks: unknown[]) => ({
  status: 200,
  ok: true,
  headers: { get: () => null },
  body: streamOf([blocks.map((b) => JSON.stringify(b)).join('\n') + '\n']),
});
const doneRes = () => ({
  status: 204,
  ok: true,
  headers: { get: () => null },
  body: { cancel: async () => {} },
});
const throttleRes = (status: number, retryAfter?: string) => ({
  status,
  ok: false,
  headers: {
    get: (k: string) =>
      k.toLowerCase() === 'retry-after' ? (retryAfter ?? null) : null,
  },
  body: { cancel: async () => {} },
});
const badRes = (text: string) => ({
  status: 400,
  ok: false,
  headers: { get: () => null },
  text: async () => text,
});

const mk = (
  over: Partial<Parameters<typeof createPortalClient>[0]>,
): PortalClient =>
  createPortalClient({
    portalUrl: 'http://p',
    headers: {},
    gate: fakeGate,
    stats: createStats(),
    bufferSize: 100,
    chainName: 'c',
    sleepImpl: async () => {},
    ...over,
  } as any);

const collect = async (gen: AsyncGenerator<unknown[]>): Promise<unknown[]> => {
  const out: unknown[] = [];
  for await (const b of gen) out.push(...b);
  return out;
};

// ── ndjsonLines ─────────────────────────────────────────────────────────────────────────────────

test('ndjsonLines: reassembles lines split across reads; flushes a newline-less tail; skips empties', async () => {
  const lines: string[] = [];
  for await (const l of ndjsonLines(
    streamOf(['{"a":1}\n{"b', '":2}\n\n{"c":3}']),
  ))
    lines.push(l);
  expect(lines).toEqual(['{"a":1}', '{"b":2}', '{"c":3}']);
});

// ── body-size guard ────────────────────────────────────────────────────────────────────────────

test('body-size guard: over-limit body fails loud with the explicit size driver (before any POST)', async () => {
  let posted = false;
  const client = mk({
    fetchImpl: (async () => {
      posted = true;
      return doneRes();
    }) as any,
  });
  const big: PortalQuery = {
    type: 'evm',
    fields: {},
    logs: [
      {
        address: Array.from(
          { length: 20000 },
          (_, i) => '0x' + i.toString(16).padStart(40, '0'),
        ),
      },
    ],
  };
  await expect(collect(client.stream(big, 0, 100))).rejects.toThrow(
    /exceeds MAX_RAW_QUERY_SIZE/,
  );
  expect(posted).toBe(false); // guarded before the POST
});

// ── error-mapping matrix ──────────────────────────────────────────────────────────────────────────

test('throttle 429 → retried, then succeeds (a header-less throttle backs off exponentially, not zero)', async () => {
  let n = 0;
  const sleeps: number[] = [];
  const stats = createStats();
  const client = mk({
    stats,
    sleepImpl: async (ms) => {
      sleeps.push(ms);
    },
    fetchImpl: (async () =>
      n++ === 0
        ? throttleRes(429)
        : ndjsonRes([{ header: { number: 10 } }])) as any,
  });
  const out = await collect(client.stream(QUERY, 0, 10));
  expect(out).toHaveLength(1);
  expect(stats.retries).toBe(1);
  expect(sleeps).toEqual([1_000]); // 500·2^1 exponential — no advice ⇒ NOT a 0ms prompt retry (issue #9)
});

test('5xx and 409 are treated as throttle (retried)', async () => {
  for (const status of [500, 502, 503, 409]) {
    let n = 0;
    const client = mk({
      fetchImpl: (async () =>
        n++ === 0 ? throttleRes(status) : doneRes()) as any,
    });
    await expect(collect(client.stream(QUERY, 0, 10))).resolves.toEqual([]); // eventually done
  }
});

test('network error → retried with exponential backoff (500·2^attempt)', async () => {
  let n = 0;
  const sleeps: number[] = [];
  const client = mk({
    sleepImpl: async (ms) => {
      sleeps.push(ms);
    },
    fetchImpl: (async () => {
      if (n++ === 0) throw new Error('fetch failed');
      return ndjsonRes([{ header: { number: 10 } }]);
    }) as any,
  });
  await collect(client.stream(QUERY, 0, 10));
  expect(sleeps).toEqual([1000]); // 500 * 2^1
});

test("field degradation: a droppable field (accessList) is dropped SILENTLY for this stream's retries", async () => {
  const bodies: any[] = [];
  const client = mk({
    fetchImpl: (async (_u: string, init: any) => {
      const q = JSON.parse(init.body);
      bodies.push(q);
      return q.fields?.transaction?.accessList !== undefined
        ? badRes("column 'access_list_size' is not found in 'transactions'")
        : ndjsonRes([{ header: { number: 10 } }]);
    }) as any,
  });
  const neededMissing = new Set<string>();
  const q: PortalQuery = {
    type: 'evm',
    fields: { transaction: { accessList: true, hash: true } },
  };
  await collect(client.stream(q, 0, 10, { neededMissing }));
  expect(neededMissing.size).toBe(0); // droppable → silent
  expect(
    bodies[bodies.length - 1].fields.transaction.accessList,
  ).toBeUndefined(); // dropped on the retry
  expect(bodies[bodies.length - 1].fields.transaction.hash).toBe(true); // other fields kept (locality)
});

test('field degradation: a NEEDED field (logsBloom) missing is recorded in neededMissing', async () => {
  const client = mk({
    fetchImpl: (async (_u: string, init: any) => {
      const q = JSON.parse(init.body);
      return q.fields?.transaction?.logsBloom !== undefined
        ? badRes("column 'logs_bloom' is not found in 'transactions'")
        : ndjsonRes([{ header: { number: 10 } }]);
    }) as any,
  });
  const neededMissing = new Set<string>();
  const q: PortalQuery = {
    type: 'evm',
    fields: { transaction: { logsBloom: true, hash: true } },
  };
  await collect(client.stream(q, 0, 10, { neededMissing }));
  expect([...neededMissing]).toEqual(['transaction.logsBloom (logs_bloom)']);
});

test('dataset-start 400 clamps the cursor forward, not a crash', async () => {
  let clampedFrom = -1;
  const client = mk({
    fetchImpl: (async (_u: string, init: any) => {
      const q = JSON.parse(init.body);
      if (q.fromBlock < 1000) return badRes('dataset starts from block 1000');
      clampedFrom = q.fromBlock;
      return doneRes();
    }) as any,
  });
  await collect(client.stream(QUERY, 0, 5000));
  expect(clampedFrom).toBe(1000);
});

test('dataset-start skip is LOUD: one warn naming the skipped range, then debug (no per-chunk storm)', async () => {
  // A dataset with partial history relative to the chain used to skip [fromBlock, startsAt) with NO
  // signal at any level — the interval assembled empty and was marked synced, indistinguishable from
  // full coverage. The skip stays (a chain genuinely starting at S must not crash-loop), but it must
  // be observable.
  const warns: string[] = [];
  const debugs: string[] = [];
  const client = mk({
    fetchImpl: (async (_u: string, init: any) => {
      const q = JSON.parse(init.body);
      if (q.fromBlock < 1000) return badRes('dataset starts from block 1000');

      return doneRes();
    }) as any,
    logWarn: (m: string) => warns.push(m),
    logDebug: (m: string) => debugs.push(m),
  });
  await collect(client.stream(QUERY, 0, 5000)); //   prefix skip: [0, 999]
  await collect(client.stream(QUERY, 100, 900)); //  whole chunk precedes the dataset
  expect(warns).toHaveLength(1); // loud exactly once per client
  expect(warns[0]).toMatch(/starts at block 1000/);
  expect(warns[0]).toMatch(/\[0, 999\]/); // the exact skipped range
  expect(warns[0]).toMatch(/partial/i); // actionable: names the dataset-gap possibility
  // the second skip is still visible, one level down
  expect(debugs.some((m) => /\[100, 900\]/.test(m))).toBe(true);
});

// ── truncated bodies (close-delimited response cut mid-line) ───────────────────────────────────────

test('a body cut mid-NDJSON-line retries from the same cursor (transient), not a fatal SyntaxError', async () => {
  // An intermediary that downgrades away the framing (HTTP/1.1 close-delimited, no Content-Length)
  // delivers a mid-line cut as CLEAN EOF: ndjsonLines flushes the partial line, JSON.parse throws, and
  // the SyntaxError matched nothing in isNetworkError → the whole sync died on a proxy hiccup. The
  // batch was never yielded and the cursor never advanced, so the retry is lossless.
  // NB the cut here ends OUTSIDE a JSON string (after the colon): V8 raises "Unexpected end of JSON
  // input" for it. A cut INSIDE a string raises "Unterminated string in JSON…", which the pre-fix
  // regex matched by ACCIDENT (the substring "terminated") — so only the typed error makes every cut
  // shape retryable, not just the lucky one.
  let call = 0;
  const froms: number[] = [];
  const client = mk({
    fetchImpl: (async (_u: string, init: any) => {
      call += 1;
      froms.push(JSON.parse(init.body).fromBlock);
      if (call === 1)
        return {
          status: 200,
          ok: true,
          headers: { get: () => null },
          body: streamOf(['{"header":{"number":7}}\n{"header":{"number":']), // cut mid-line, clean close
        };
      if (call === 2)
        return ndjsonRes([
          { header: { number: 7 } },
          { header: { number: 8 } },
        ]);

      return doneRes();
    }) as any,
  });
  const out = await collect(client.stream(QUERY, 0, 8));
  expect(out).toHaveLength(2); // both blocks delivered exactly once, from the retry
  expect((out[0] as any).header.number).toBe(7);
  // pin the SAME-cursor claim: the retry re-requests from the identical fromBlock — an
  // implementation that advanced the cursor while discarding the partial batch would fail here
  expect(froms[1]).toBe(froms[0]);
});

test('isTransientError: PortalTruncatedBodyError and gzip-truncation shapes are retryable', () => {
  expect(
    isTransientError(
      new PortalTruncatedBodyError(
        5,
        new SyntaxError('Unexpected end of JSON input'),
      ),
    ),
  ).toBe(true);
  // truncated gzip member (accept-encoding: gzip is sent globally) → zlib error, not a socket error
  const zlibErr = Object.assign(new Error('unexpected end of file'), {
    cause: { code: 'Z_BUF_ERROR' },
  });
  expect(isTransientError(zlibErr)).toBe(true);
  const premature = Object.assign(new Error('premature close'), {
    cause: { code: 'ERR_STREAM_PREMATURE_CLOSE' },
  });
  expect(isTransientError(premature)).toBe(true);
  // raw zlib shape: the code sits at the TOP level, no `cause` wrapper, and the message
  // ("unexpected end of file") matches nothing in the regex on its own
  const rawZlib = Object.assign(new Error('unexpected end of file'), {
    code: 'Z_BUF_ERROR',
  });
  expect(isTransientError(rawZlib)).toBe(true);
  // ...and the code-only premature-close shape (underscored) is still matched
  const prematureCode = Object.assign(new Error('stream ended'), {
    code: 'ERR_STREAM_PREMATURE_CLOSE',
  });
  expect(isTransientError(prematureCode)).toBe(true);
  // a fatal 400 whose body text merely CONTAINS "premature" must stay fatal
  expect(
    isTransientError(new Error('400: field "prematureBlocks" is unknown')),
  ).toBe(false);
});

test("'query is too large' 400 → actionable PORTAL_MAX_ADDRESSES error (bytes cap, not range)", async () => {
  const client = mk({
    fetchImpl: (async () => badRes('Query is too large')) as any,
  });
  await expect(collect(client.stream(QUERY, 0, 10))).rejects.toThrow(
    /lower PORTAL_MAX_ADDRESSES/,
  );
});

test('an unrecognised 400 surfaces as PortalHttpError', async () => {
  const client = mk({
    fetchImpl: (async () => badRes('some unexpected failure')) as any,
  });
  await expect(collect(client.stream(QUERY, 0, 10))).rejects.toThrow(
    PortalHttpError,
  );
});

// ── progress + termination (INV-13) ───────────────────────────────────────────────────────────────

test('INV-13: the cursor strictly advances and the stream terminates', async () => {
  const seen: number[] = [];
  const client = mk({
    fetchImpl: (async (_u: string, init: any) => {
      const q = JSON.parse(init.body);
      seen.push(q.fromBlock);
      return ndjsonRes([{ header: { number: q.fromBlock } }]);
    }) as any,
  });
  await collect(client.stream(QUERY, 0, 3));
  expect(seen).toEqual([0, 1, 2, 3]); // advances one past the last block each pass, then stops
});

// ── retry budget + back-off caps ──────────────────────────────────────────────────────────────────

test('retry budget: the 11th transient failure (initial try + 10 retries) throws', async () => {
  let fetches = 0;
  const client = mk({
    fetchImpl: (async () => {
      fetches++;
      throw new Error('fetch failed');
    }) as any,
  });
  await expect(collect(client.stream(QUERY, 0, 10))).rejects.toThrow(
    /fetch failed/,
  );
  expect(fetches).toBe(11); // attempt++ >= 10 stops after the 11th try
});

test('retry-after: a numeric header sleeps ra*1000 capped at 30s', async () => {
  const sleeps: number[] = [];
  let n = 0;
  const client = mk({
    sleepImpl: async (ms) => {
      sleeps.push(ms);
    },
    fetchImpl: (async () => {
      n++;
      if (n === 1) return throttleRes(429, '2'); //  2s advised
      if (n === 2) return throttleRes(429, '60'); // 60s advised → capped
      return doneRes();
    }) as any,
  });
  await collect(client.stream(QUERY, 0, 10));
  expect(sleeps).toEqual([2_000, 30_000]); // ra*1000, then min(ra*1000, 30_000)
});

// ── retry-after: HTTP-date form, zero-backoff, and garbage (issue #9) ──────────────────────────────

test('parseRetryAfterMs: positive advice only; absent / "0" / past-date / garbage ⇒ undefined', () => {
  const now = Date.parse('2025-01-01T00:00:00Z');
  expect(parseRetryAfterMs('2', now)).toBe(2_000); //             delta-seconds
  expect(parseRetryAfterMs(new Date(now + 5_000).toUTCString(), now)).toBe(
    5_000,
  ); //                                                            HTTP-date, 5s in the future
  // everything below carries no usable positive advice ⇒ undefined ⇒ caller uses exponential back-off
  expect(parseRetryAfterMs(null, now)).toBeUndefined(); //        absent (NOT Number(null)===0 → 0)
  expect(parseRetryAfterMs('0', now)).toBeUndefined(); //         explicit zero
  expect(parseRetryAfterMs('-5', now)).toBeUndefined(); //        negative
  expect(parseRetryAfterMs(new Date(now - 5_000).toUTCString(), now)).toBe(
    undefined,
  ); //                                                            already-past HTTP-date
  expect(parseRetryAfterMs('not-a-date', now)).toBeUndefined(); // garbage
  // strict RFC-7231 delta-seconds is `1*DIGIT`; the lax `Number()` used to honor these non-conforming
  // forms as advice. They must NOT be honored — each falls through to Date.parse and ends in exponential
  // back-off (undefined): "1e3"/"0x10" are not dates; V8 parses "1.5" as a PAST date ⇒ ≤0 delta. (PR #16 review)
  expect(parseRetryAfterMs('1e3', now)).toBeUndefined(); //       not delta-seconds, not a future date
  expect(parseRetryAfterMs('0x10', now)).toBeUndefined(); //      hex is not delta-seconds
  expect(parseRetryAfterMs('1.5', now)).toBeUndefined(); //       fractional ⇒ Date.parse past date ⇒ undefined
  // surrounding OWS is trimmed (HTTP field values routinely are), so a padded pure-digit run IS honored.
  expect(parseRetryAfterMs(' 120 ', now)).toBe(120_000); //       OWS-trimmed delta-seconds
});

test('retry-after: a header-less throttle storm backs off EXPONENTIALLY, never zero (issue #9)', async () => {
  const sleeps: number[] = [];
  let n = 0;
  const client = mk({
    sleepImpl: async (ms) => {
      sleeps.push(ms);
    },
    // three header-less 429s (the common LB 502/503 shape), then success
    fetchImpl: (async () => (n++ < 3 ? throttleRes(429) : doneRes())) as any,
  });
  await collect(client.stream(QUERY, 0, 10));
  // pre-fix: Number(null)===0 → retryAfterMs 0 → the wait takes the advised branch = 0ms every attempt
  // (an 11-deep 0ms hammer). now: no advice ⇒ 500·2^attempt.
  expect(sleeps).toEqual([1_000, 2_000, 4_000]);
});

test('retry-after: an HTTP-date header backs off instead of hard-throwing mid-run (issue #9)', async () => {
  const when = new Date(Date.now() + 5_000).toUTCString(); // ~5s in the future, second-granular
  const sleeps: number[] = [];
  let n = 0;
  const client = mk({
    sleepImpl: async (ms) => {
      sleeps.push(ms);
    },
    fetchImpl: (async () =>
      n++ === 0 ? throttleRes(429, when) : doneRes()) as any,
  });
  // pre-fix: Number(date)→NaN→undefined→non-retryable→this REJECTS. now: parsed → retried → resolves.
  await expect(collect(client.stream(QUERY, 0, 10))).resolves.toEqual([]);
  expect(sleeps).toHaveLength(1);
  expect(sleeps[0]).toBeGreaterThan(1_000); // backed off toward the advised instant
  expect(sleeps[0]).toBeLessThanOrEqual(30_000); // capped at the 30s ceiling
});

test('retry-after: a throttle with a garbage header still retries (exponential), never hard-throws (issue #9)', async () => {
  let n = 0;
  const client = mk({
    fetchImpl: (async () =>
      n++ === 0 ? throttleRes(429, 'garbage') : doneRes()) as any,
  });
  // a 429/5xx is always retryable; a malformed Retry-After must not turn it into a fatal error.
  await expect(collect(client.stream(QUERY, 0, 10))).resolves.toEqual([]);
});

// ── HTTP-path deadlines: a hung request/body must abort-or-cancel, retry, and release its gate slot ──
// (addendum) Without a deadline a hung request never runs fetchBatch's `finally`, so its gate slot leaks
// and every chain eventually parks in gate.acquire(). A counting gate proves the slot is returned.

const countingGate = () => {
  let active = 0;
  let peak = 0;
  return {
    acquire: async () => {
      active++;
      peak = Math.max(peak, active);
    },
    release() {
      active--;
    },
    onOk() {},
    onThrottle() {},
    addRows() {},
    freeRows() {},
    saturated: () => false,
    snapshot: () => ({ limit: 0, active, rows: 0 }),
    peak: () => peak,
  };
};

test('timeout: a fetch that never sends headers is aborted at the connect deadline, retried, gate released (addendum)', async () => {
  const gate = countingGate();
  let n = 0;
  const client = mk({
    gate: gate as any,
    requestTimeoutMs: 20,
    idleTimeoutMs: 20,
    fetchImpl: (async (_u: string, init: any) => {
      if (n++ === 0)
        // never resolves on its own; only the connect-phase abort (signal) rejects it
        return new Promise((_res, reject) => {
          init.signal.addEventListener('abort', () =>
            reject(Object.assign(new Error('aborted'), { name: 'AbortError' })),
          );
        });

      return doneRes();
    }) as any,
  });
  await expect(collect(client.stream(QUERY, 0, 10))).resolves.toEqual([]);
  expect(n).toBe(2); // first hung → aborted + retried; second completed
  expect(gate.snapshot().active).toBe(0); // slot released (finally ran)
  expect(gate.peak()).toBeGreaterThanOrEqual(1); // it was acquired
});

test('timeout: a body that stalls without closing hits the SOFT idle timeout, is cancelled (not aborted), retried, gate released (addendum)', async () => {
  const gate = countingGate();
  let n = 0;
  let cancelled = false;
  const client = mk({
    gate: gate as any,
    requestTimeoutMs: 1_000, // headers arrive fine; the stall is on the body
    idleTimeoutMs: 20,
    fetchImpl: (async () => {
      if (n++ === 0) {
        // headers OK, but the body never produces a chunk and never closes → idle timeout fires.
        // NB: no signal wiring — the soft timeout does NOT depend on abort reaching the stream.
        const body = new ReadableStream<Uint8Array>({
          start() {
            /* never enqueue, never close */
          },
          cancel() {
            cancelled = true; // graceful teardown = reader.cancel(), not abort()
          },
        });
        return { status: 200, ok: true, headers: { get: () => null }, body };
      }
      return doneRes();
    }) as any,
  });
  await expect(collect(client.stream(QUERY, 0, 10))).resolves.toEqual([]);
  expect(n).toBe(2); // stalled body → idle-timeout + retry; second completed
  expect(cancelled).toBe(true); // the stream was CANCELLED, never aborted
  expect(gate.snapshot().active).toBe(0); // slot released
});

test('timeout: a NON-OK (400) response whose body never yields still throws PortalHttpError within the idle deadline, gate released (PR #16 review)', async () => {
  const gate = countingGate();
  let cancelled = false;
  const client = mk({
    gate: gate as any,
    requestTimeoutMs: 1_000, // headers arrive fine; the stall is on the non-OK error body
    idleTimeoutMs: 20,
    // 400 headers arrive, but the error body never yields a chunk. Post-fix we OWN the reader and race
    // each read against the idle timer; on the stall we `reader.cancel()` (legal — we hold the lock) and
    // resolve with the partial text, so the typed error still throws PROMPTLY. A REAL `Response` wrapping
    // a real ReadableStream is required so the stream LOCKING is real: the pre-fix `res.text()` locks the
    // body, then `res.body.cancel()` REJECTS (spec: cancel on a locked stream) and the cancel() callback
    // never fires — the exact bug a fake `{ cancel }` mock hid (regression against 7b5791cd).
    fetchImpl: (async () =>
      new Response(
        new ReadableStream<Uint8Array>({
          start() {
            /* never enqueue, never close → the body-idle timer must win */
          },
          cancel() {
            cancelled = true; // graceful teardown = reader.cancel(), never abort()
          },
        }),
        { status: 400 },
      )) as any,
  });
  const err = await collect(client.stream(QUERY, 0, 10)).then(
    () => undefined,
    (e) => e,
  );
  expect(err).toBeInstanceOf(PortalHttpError);
  expect((err as PortalHttpError).status).toBe(400); // right status, surfaced promptly
  expect(cancelled).toBe(true); // the stalled error body's cancel() callback actually fired
  expect(gate.snapshot().active).toBe(0); // slot released (finally ran)
});

test('non-OK (400): a REAL streamed error body is read to completion (across chunks) and its schema message extracted', async () => {
  // The read loop must ACCUMULATE a multi-chunk body that closes normally — not just handle the stall. A
  // real Response whose 400 body streams the column-not-found message in two chunks proves readTextWithIdle
  // reassembles the full text (TextDecoder streaming) so the schema-field extraction still fires.
  const client = mk({
    idleTimeoutMs: 1_000, // the body closes promptly; no stall
    fetchImpl: (async (_u: string, init: any) => {
      const q = JSON.parse(init.body);
      return q.fields?.transaction?.accessList !== undefined
        ? new Response(
            streamOf([
              "column 'access_list_size' is not ",
              "found in 'transactions'",
            ]),
            { status: 400 },
          )
        : ndjsonRes([{ header: { number: 10 } }]);
    }) as any,
  });
  const neededMissing = new Set<string>();
  const q: PortalQuery = {
    type: 'evm',
    fields: { transaction: { accessList: true, hash: true } },
  };
  await collect(client.stream(q, 0, 10, { neededMissing }));
  expect(neededMissing.size).toBe(0); // droppable accessList → dropped silently on the retry (message read)
});

// ── G3 (INV-7): onRows fires per ARRIVING batch, not on stream completion ─────────────────────────

test('G3: onRows is called once per yielded batch, mid-stream', async () => {
  const client = mk({
    fetchImpl: (async (_u: string, init: any) => {
      const q = JSON.parse(init.body);
      return q.fromBlock <= 5
        ? ndjsonRes([{ header: { number: q.fromBlock }, logs: [{}, {}] }])
        : doneRes();
    }) as any,
  });
  const rowEvents: number[] = [];
  const gen = client.stream(QUERY, 0, 5, { onRows: (n) => rowEvents.push(n) });
  await gen.next(); // batch 1 yielded
  expect(rowEvents).toEqual([3]); // registered BEFORE the stream finished (1 header + 2 logs)
  await gen.next(); // batch 2
  expect(rowEvents).toEqual([3, 3]);
  for await (const _ of gen) {
    /* drain */
  }
  expect(rowEvents).toEqual([3, 3, 3, 3, 3, 3]); // one per batch (blocks 0..5)
});

// ── finalizedHead ─────────────────────────────────────────────────────────────────────────────────

test('finalizedHead: parses {number}; undefined on failure', async () => {
  expect(
    await mk({
      fetchImpl: (async () => ({ json: async () => ({ number: 123 }) })) as any,
    }).finalizedHead(),
  ).toBe(123);
  expect(
    await mk({
      fetchImpl: (async () => {
        throw new Error('down');
      }) as any,
    }).finalizedHead(),
  ).toBeUndefined();
});

test('finalizedHead: parses a REAL streamed JSON body (reader-owning read; json() ≡ text+parse)', async () => {
  // The happy path now flows through the reader-owning readTextWithIdle → JSON.parse. Prove a genuine
  // Response whose body streams in two chunks and CLOSES parses correctly (a body that closes must never
  // idle-stall). json() ≡ text+parse, so this stays green on both the fixed and the old r.json() form.
  expect(
    await mk({
      finalizedHeadTimeoutMs: 1_000,
      fetchImpl: (async () =>
        new Response(streamOf(['{"number":', '456}']))) as any,
    }).finalizedHead(),
  ).toBe(456);
});

test('finalizedHead: a fetch that never sends headers returns undefined at the connect deadline (PR #16 review)', async () => {
  // The head probe fetch never resolves on its own; only the connect-phase abort (signal) rejects it. If
  // disarm-on-headers or the connect-phase AbortController were removed this would hang forever.
  const client = mk({
    finalizedHeadTimeoutMs: 20,
    fetchImpl: (async (_u: string, init: any) =>
      new Promise((_res, reject) => {
        init.signal.addEventListener('abort', () =>
          reject(Object.assign(new Error('aborted'), { name: 'AbortError' })),
        );
      })) as any,
  });
  // Only the connect-phase abort can settle this; if it never fired the vitest timeout would trip instead.
  expect(await client.finalizedHead()).toBeUndefined();
});

test('finalizedHead: headers arrive but the JSON body never settles → undefined via the SOFT race, and NO abort() fires after headers (issue #14 / PR #16 review)', async () => {
  let abortedAfterHeaders = false;
  let headersDelivered = false;
  let cancelled = false;
  const client = mk({
    finalizedHeadTimeoutMs: 20,
    fetchImpl: (async (_u: string, init: any) => {
      // record any abort that lands AFTER we hand back the response (i.e. once headers "arrived")
      init.signal.addEventListener('abort', () => {
        if (headersDelivered) abortedAfterHeaders = true;
      });
      headersDelivered = true;

      // A REAL Response wrapping a ReadableStream that never yields a chunk → the reader-owning read
      // stalls on the idle timer, `reader.cancel()` fires (legal — we own the lock), the accumulated ''
      // fails JSON.parse, and we fall through to undefined. A REAL Response makes the stream LOCKING real:
      // the pre-fix `r.json()` locks the body, then `r.body.cancel()` REJECTS and the cancel() callback
      // never fires — the exact bug a fake `{ cancel }` mock hid.
      return new Response(
        new ReadableStream<Uint8Array>({
          start() {
            /* never enqueue, never close */
          },
          cancel() {
            cancelled = true;
          },
        }),
      );
    }) as any,
  });
  expect(await client.finalizedHead()).toBeUndefined();
  expect(cancelled).toBe(true); // the body's cancel() callback actually fired (graceful teardown)
  expect(abortedAfterHeaders).toBe(false); // never aborted after headers (the #14 kill shape)
});

test('finalizedHeadRetry: retries through the injectable sleep, then gives up undefined', async () => {
  let probes = 0;
  const sleeps: number[] = [];
  const failing = mk({
    sleepImpl: async (ms) => {
      sleeps.push(ms);
    },
    fetchImpl: (async () => {
      probes++;
      throw new Error('down');
    }) as any,
  });
  expect(await failing.finalizedHeadRetry(3)).toBeUndefined();
  expect(probes).toBe(3);
  expect(sleeps).toEqual([200, 400, 600]); // linear backoff, no real timers

  let n = 0;
  const flaky = mk({
    fetchImpl: (async () => {
      n++;
      if (n < 2) throw new Error('down');
      return { json: async () => ({ number: 7 }) };
    }) as any,
  });
  expect(await flaky.finalizedHeadRetry(3)).toBe(7); // succeeds on the 2nd probe
});
