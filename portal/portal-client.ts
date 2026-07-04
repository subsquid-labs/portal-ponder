/**
 * portal-client.ts — the Portal HTTP shell.
 *
 * Owns everything I/O between the pure core and the SQD Portal:
 *   • `finalizedHead()` — GET /finalized-head.
 *   • `stream(query, from, to)` — POST /finalized-stream and yield parsed NDJSON batches, advancing a
 *     cursor until `to`. Handles: error mapping to the typed taxonomy (429/5xx/409 throttle; the four
 *     400 variants), retry + back-off, per-stream field degradation (droppable → silent, needed →
 *     recorded in `neededMissing`), dataset-start clamp, body-size guard, no-progress guard, and gate
 *     acquire/release around every request.
 *   • incremental row registration (`onRows`, per arriving batch — INV-7/G3).
 *   • `ndjsonLines()` — the shared NDJSON line splitter (also used by the realtime stream).
 *
 * `fetchImpl` and `sleepImpl` are injectable so the retry/error-mapping matrix is unit-testable with no
 * real network or timers.
 */
import {
  isNetworkError,
  isTransientError,
  PortalDatasetStartError,
  PortalHttpError,
  PortalQueryTooLargeError,
  PortalSchemaFieldError,
  PortalThrottleError,
} from './portal-errors.js';
import {
  DROPPABLE_FIELDS,
  MAX_RAW_QUERY_SIZE,
  PORTAL_MAX_ADDRESSES,
  type PortalQuery,
} from './portal-filters.js';
import type { Gate } from './portal-gate.js';
import type { PortalStats } from './portal-metrics.js';
import type { RawBlock } from './portal-transform.js';

const realSleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

/**
 * Split a byte stream into NDJSON lines (skipping empties). Yields each complete line as it arrives,
 * buffering across reads so a line split over two chunks is reassembled. Flushes a trailing newline-less
 * line at end. `onBytes` observes raw byte counts (for metrics).
 *
 * `idleMs` (optional) is a SOFT stall guard: each `reader.read()` is raced against a timer and, if no chunk
 * arrives within `idleMs`, the read throws a `timeout` error (⇒ transient/retryable upstream). Teardown is
 * the `finally` below — `reader.cancel()`, the graceful stream close — NOT `AbortController.abort()`, which
 * was observed to silently kill Node when it landed on an in-flight gzip body. A progressing stream re-arms
 * on every chunk, so a slow-but-alive stream is never cut. Absent ⇒ no stall guard (realtime path).
 */
export async function* ndjsonLines(
  body: ReadableStream<Uint8Array>,
  onBytes?: (n: number) => void,
  idleMs?: number,
): AsyncGenerator<string> {
  const reader = body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  // A consumer may `break` early (the realtime stream re-opens the moment the logs filter revision bumps —
  // finding 4), or the idle guard may throw; cancel the reader in `finally` so the underlying response body
  // is closed rather than left locked+open. Fire-and-forget (NOT awaited): on a fully-drained stream it's a
  // no-op, and awaiting the socket teardown on every historical fetch would add needless latency.
  try {
    for (;;) {
      const { done, value } = await readWithIdle(reader, idleMs);
      if (done) break;

      if (value) {
        onBytes?.(value.byteLength);
        buf += dec.decode(value, { stream: true });
      }
      for (;;) {
        const nl = buf.indexOf('\n');
        if (nl < 0) break;

        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (line) yield line;
      }
    }
    buf += dec.decode();
    if (buf) yield buf;
  } finally {
    void reader.cancel().catch(() => {});
  }
}

/**
 * One `reader.read()`, optionally raced against an idle timer. On a stall the timer wins and this REJECTS
 * (message carries `timeout` ⇒ `isNetworkError` ⇒ retryable); the caller's `finally` cancels the reader,
 * which settles the abandoned read (swallowed). No `abort()` — see `ndjsonLines`. (addendum)
 */
async function readWithIdle<T>(
  reader: ReadableStreamDefaultReader<T>,
  idleMs?: number,
) {
  if (idleMs === undefined) return reader.read();

  const readP = reader.read();
  readP.catch(() => {}); // if the idle timer wins we abandon this read; cancel() settles it later

  let timer: ReturnType<typeof setTimeout> | undefined;
  const idle = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error('Portal stream idle timeout (no data)')),
      idleMs,
    );
    timer.unref?.();
  });
  try {
    return await Promise.race([readP, idle]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

/**
 * Read a non-OK response body to a string under a SOFT idle deadline — the error-body path's analogue of
 * `readWithIdle`. A gateway that sends non-OK HEADERS then stalls the BODY would hang the plain
 * `await res.text()` forever (fetchBatch's `finally` never runs, the gate slot never releases). So we OWN
 * the read: acquire `res.body.getReader()` and accumulate chunks under a per-chunk idle timer (the idle
 * deadline is the max GAP between chunks — `readWithIdle`'s semantics). On a stall we `reader.cancel()` —
 * LEGAL because we hold the lock (we own the reader), unlike `res.body.cancel()` on a body already locked
 * by `res.text()`, which the Web Streams spec REJECTS with a TypeError, leaking the socket — and resolve
 * with the text accumulated SO FAR (partial error text beats '' for the optional message extraction). We
 * never `abort()` (issue #14: abort on an in-flight gzip body can silently kill the Node process). A
 * mid-read rejection (network drop) resolves with what we have; the caller still throws the typed error
 * with the right status, so a truncated/empty body on timeout is acceptable — the win is that the error
 * surfaces PROMPTLY and the gate slot releases. If `res.body` is null/undefined (empty body, or a simple
 * test mock with only `text()`), there is nothing to lock or cancel: race `res.text()` against the timer.
 * (PR #16 review)
 */
export async function readTextWithIdle(
  res: { text(): Promise<string>; body?: ReadableStream<Uint8Array> | null },
  idleMs: number,
): Promise<string> {
  const stream = res.body;
  if (!stream) {
    // No stream to own (empty body / simple mock): race the whole text() read against one idle timer.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const textP = res.text();
    textP.catch(() => {}); // if the idle timer wins we abandon this read — no body to cancel

    const idle = new Promise<string>((resolve) => {
      timer = setTimeout(() => resolve(''), idleMs);
      timer.unref?.();
    });
    try {
      return await Promise.race([textP, idle]);
    } catch {
      return ''; // a rejecting body still yields the typed error promptly
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  const reader = stream.getReader();
  const dec = new TextDecoder();
  let text = '';
  try {
    for (;;) {
      const { done, value } = await readWithIdle(reader, idleMs);
      if (done) break;

      if (value) text += dec.decode(value, { stream: true });
    }
    text += dec.decode();
  } catch {
    // Idle-timer stall (readWithIdle rejected) OR a network drop mid-read. We own the lock, so cancel is
    // legal — settle the abandoned read and close the socket. Resolve with what we accumulated so far.
    void reader.cancel().catch(() => {});
  }

  return text;
}

// Portal reports a missing COLUMN in a plural TABLE; map back to the field key we requested.
const TABLE_TO_KEY: Record<string, string> = {
  transactions: 'transaction',
  blocks: 'block',
  logs: 'log',
  traces: 'trace',
};
const COL_SPECIAL: Record<string, string> = {
  access_list_size: 'accessList',
  access_list: 'accessList',
}; // Portal's derived column ≠ snake(field)
const colToFieldKey = (col: string, table: string): string => {
  const key = TABLE_TO_KEY[table] ?? table;
  const field =
    COL_SPECIAL[col] ??
    col.replace(/_([a-z0-9])/g, (_, c: string) => c.toUpperCase());
  return `${key}.${field}`;
};

/**
 * Parse a Portal 400 body for a "dataset can't serve this field" error and map it to a
 * `PortalSchemaFieldError` (the field key + table so the caller can drop it and retry). Two shapes:
 *   • "column '<col>' is not found in '<table>'" — the parquet column is absent (e.g. Monad has no
 *     accessList);
 *   • "unknown field `<field>`" — the schema doesn't know the field at all (a query PARSE error); the
 *     table is recovered from the request `body`'s fields block.
 * Returns undefined for any other 400 (not a droppable-field problem). SHARED by the historical fetch
 * loop and the realtime `/stream` so the two degrade identically. `body` is the JSON request string
 * (only read for the "unknown field" table lookup). (review B3)
 */
export function parseSchemaFieldError(
  status: number,
  text: string,
  body: string,
): PortalSchemaFieldError | undefined {
  if (status !== 400) return undefined;
  // a dataset that lacks a requested column (e.g. Monad has no accessList) → the whole request 400s.
  const m = text.match(/column '([a-z0-9_]+)' is not found in '([a-z_]+)'/i);
  if (m)
    return new PortalSchemaFieldError(
      colToFieldKey(m[1]!, m[2]!),
      TABLE_TO_KEY[m[2]!] ?? m[2]!,
      m[1]!,
    );
  // OTHER schema shape: a dataset whose schema doesn't know the field → query PARSE error.
  const u = text.match(/unknown field `([a-zA-Z0-9_]+)`/);
  if (u?.[1]) {
    const fn = u[1];
    let table = 'transaction';
    try {
      const q = JSON.parse(body);
      for (const t of ['transaction', 'block', 'log', 'trace'])
        if (q?.fields?.[t] && q.fields[t][fn] !== undefined) {
          table = t;
          break;
        }
    } catch {
      /* default transaction */
    }
    return new PortalSchemaFieldError(`${table}.${fn}`, table, fn);
  }

  return undefined;
}

const stripFields = (q: PortalQuery, dropped: Set<string>): PortalQuery => {
  if (dropped.size === 0 || !q.fields) return q;
  const fields: Record<string, Record<string, boolean>> = JSON.parse(
    JSON.stringify(q.fields),
  );
  for (const tf of dropped) {
    const i = tf.indexOf('.');
    const t = tf.slice(0, i);
    const f = tf.slice(i + 1);
    const tbl = fields[t];
    if (tbl) delete tbl[f];
  }
  return { ...q, fields };
};

// Row accounting is CONSERVATIVE (INV-7): each block counts its header too, so header-only batches
// (block-interval includeAllBlocks scans, whose retained headers are real buffered memory) register
// against the budget instead of registering ~0.
const countRows = (blocks: RawBlock[]): number => {
  let n = 0;
  for (const b of blocks)
    n +=
      1 +
      (b.logs?.length ?? 0) +
      (b.transactions?.length ?? 0) +
      (b.traces?.length ?? 0);
  return n;
};

export type StreamOpts = {
  /** Needed (non-droppable) fields the dataset lacked on this range are recorded here. */
  neededMissing?: Set<string>;
  /** Called with the row count of each arriving batch (incremental buffer accounting — G3). */
  onRows?: (n: number) => void;
};

export interface PortalClient {
  /** GET /finalized-head → the finalized block number (undefined on any failure). */
  finalizedHead(): Promise<number | undefined>;
  /** finalizedHead() with a bounded retry (backoff via the injectable sleep). */
  finalizedHeadRetry(attempts?: number): Promise<number | undefined>;
  /** POST /finalized-stream and yield parsed NDJSON batches over [from, to]. */
  stream(
    query: PortalQuery,
    from: number,
    to: number,
    opts?: StreamOpts,
  ): AsyncGenerator<RawBlock[]>;
}

export type PortalClientDeps = {
  portalUrl: string; // already trailing-slash-stripped
  headers: Record<string, string>;
  gate: Gate;
  stats: PortalStats;
  bufferSize: number;
  chainName: string;
  /** connect/headers deadline per stream POST (ms). Default 30_000; small values injected in tests. */
  requestTimeoutMs?: number;
  /** max gap between NDJSON chunks before a stalled body is CANCELLED (never abort(); issue #14) (ms). Default 60_000. */
  idleTimeoutMs?: number;
  /** connect + body deadline for the /finalized-head probe (ms). Default 10_000; small values injected in tests. */
  finalizedHeadTimeoutMs?: number;
  fetchImpl?: typeof fetch;
  sleepImpl?: (ms: number) => Promise<void>;
  logDebug?: (msg: string) => void;
};

/** Head probe deadline — a small GET; if it hangs, callers stay conservative rather than block. */
const FINALIZED_HEAD_TIMEOUT_MS = 10_000;

/**
 * Parse a `Retry-After` header into a POSITIVE back-off in ms, or `undefined` when the header carries no
 * usable positive advice. RFC-7231 allows two forms and a Portal gateway can send either: delta-seconds
 * (`"120"`) and an HTTP-date (`"Wed, 21 Oct 2025 07:28:00 GMT"` → the remaining delay via `Date.parse`).
 *
 * `undefined` (⇒ the caller uses exponential back-off) covers: an ABSENT header, `"0"` / a negative value,
 * an already-past HTTP-date, and any unparseable garbage. This is deliberate — two prior bugs lived here:
 *   • the HTTP-date form fell through `Number(date) → NaN` and hard-threw mid-run (issue #9);
 *   • an ABSENT header parsed as `Number(null) === 0`, so every attempt "waited" 0ms → an 11-deep
 *     zero-back-off retry storm hammered a struggling gateway before failing. Absent/0 now means NO
 *     advice, so the caller falls to `min(500·2^attempt, 30s)`.
 * Only a strictly-positive value is honored (the caller caps it at 30s). (issue #9)
 */
export function parseRetryAfterMs(
  header: string | null,
  now: number = Date.now(),
): number | undefined {
  if (header === null) return undefined;

  // delta-seconds is RFC-7231 `1*DIGIT` — a run of ASCII digits, nothing else. `Number()` was too lax: it
  // honored `"1e3"`, `"0x10"`, `"1.5"`, `"  120  "` (via NaN-free coercion) as advice, none of which a
  // conforming server means as delta-seconds. We trim surrounding OWS (routinely stripped from HTTP field
  // values) then require a pure-digit run; anything else falls through to the HTTP-date branch, where a
  // non-date (e.g. `"1.5"`, which V8's Date.parse reads as a PAST date) yields a ≤0 delta ⇒ undefined ⇒
  // the caller's exponential back-off. (PR #16 review)
  const trimmed = header.trim();
  if (/^[0-9]+$/.test(trimmed)) {
    const secs = Number(trimmed);

    return secs > 0 ? secs * 1000 : undefined; // "0" ⇒ no advice → exponential back-off
  }

  const at = Date.parse(header); // HTTP-date form
  if (Number.isNaN(at)) return undefined;

  const delta = at - now;

  return delta > 0 ? delta : undefined; // already-past ⇒ no advice → exponential back-off
}

export function createPortalClient(deps: PortalClientDeps): PortalClient {
  const { portalUrl, headers, gate, stats, bufferSize, chainName } = deps;
  const fetchImpl = deps.fetchImpl ?? fetch;
  const sleep = deps.sleepImpl ?? realSleep;
  const logDebug = deps.logDebug ?? (() => {});
  const requestTimeoutMs = deps.requestTimeoutMs ?? 30_000;
  const idleTimeoutMs = deps.idleTimeoutMs ?? 60_000;
  const finalizedHeadTimeoutMs =
    deps.finalizedHeadTimeoutMs ?? FINALIZED_HEAD_TIMEOUT_MS;

  // one POST+drain; returns blocks or "done" (204); throws a typed error (throttle carries retryAfterMs).
  async function fetchBatch(
    body: string,
    cursor: number,
  ): Promise<{ blocks: RawBlock[]; last: number } | 'done'> {
    // Proactive, uniform size guard — covers EVERY request type at the one POST choke point. A body over
    // MAX_RAW_QUERY_SIZE would 400; surface it explicitly with the real driver instead.
    if (body.length > MAX_RAW_QUERY_SIZE) {
      const q = (() => {
        try {
          return JSON.parse(body);
        } catch {
          return {};
        }
      })();
      const nLog = (q.logs ?? []).reduce(
        (s: number, r: { address?: unknown[] }) => s + (r.address?.length ?? 0),
        0,
      );
      const nTx = (q.transactions ?? []).reduce(
        (s: number, r: { from?: unknown[]; to?: unknown[] }) =>
          s + (r.from?.length ?? 0) + (r.to?.length ?? 0),
        0,
      );
      throw new Error(
        `Portal request body ${(body.length / 1024).toFixed(1)}KB exceeds MAX_RAW_QUERY_SIZE ${MAX_RAW_QUERY_SIZE / 1024}KB @ ${cursor}. ` +
          `Filter addresses in this request: ${nLog} log + ${nTx} tx(from/to). ` +
          `Log filters are already merged+batched (PORTAL_MAX_ADDRESSES=${PORTAL_MAX_ADDRESSES}); if this is a tx filter, its from/to set is too large to fit one request and cannot be safely split — narrow the filter.`,
      );
    }
    const tAcq = Date.now();
    await gate.acquire();
    stats.gateWaitMs += Date.now() - tAcq; // gate-wait = concurrency back-pressure
    const tFetch = Date.now();
    stats.inflight++;
    stats.maxInflight = Math.max(stats.maxInflight, stats.inflight);
    // Per-request deadline, in TWO phases with DIFFERENT teardown — because aborting an in-flight fetch whose
    // gzip body is mid-stream was observed to silently kill the Node process on a real slow endpoint
    // (undici/inflate/abort interaction). So:
    //   (a) CONNECT/headers phase — a genuine AbortController.abort(); aborting BEFORE the body streams is
    //       safe. Disarmed the instant headers arrive.
    //   (b) BODY-IDLE phase — a SOFT timeout inside `ndjsonLines`: each read is raced against idleTimeoutMs
    //       and on a stall the reader is CANCELLED (not aborted) — the graceful teardown PR #8 landed.
    // Without either, a gateway that accepts the POST but never completes hangs the chunk forever: the
    // `finally` never runs, its gate slot never releases, and every chain eventually parks in
    // `gate.acquire()`. The resulting error is a network/timeout error → transient → retried. (addendum)
    const controller = new AbortController();
    let connectTimer: ReturnType<typeof setTimeout> | undefined = setTimeout(
      () => controller.abort(),
      requestTimeoutMs,
    );
    connectTimer.unref?.();
    const disarmConnect = () => {
      if (connectTimer !== undefined) {
        clearTimeout(connectTimer);
        connectTimer = undefined;
      }
    };
    try {
      const res = await fetchImpl(
        `${portalUrl}/finalized-stream?buffer_size=${bufferSize}`,
        { method: 'POST', headers, body, signal: controller.signal },
      );
      disarmConnect(); // headers arrived — never abort() once the body is streaming (see above)
      stats.http++;
      if (res.status === 204) {
        gate.onOk();
        return 'done';
      }
      // Transient, retry with back-off: 429/529 explicit throttle; ALL 5xx gateway/proxy hiccups; 409 on
      // the FINALIZED stream = a gateway "conflict" (finalized data doesn't reorg). Back off on any.
      if (res.status >= 500 || res.status === 429 || res.status === 409) {
        // Fire-and-forget: cancel() settles promptly (it doesn't drain the body), so unlike an unbounded
        // `res.text()` it can't leak the gate slot — and it's the graceful teardown, never abort(). We do
        // NOT await it, so a pathological cancel() can't stall the throttle either. (PR #16 review)
        void res.body?.cancel().catch(() => {});
        // Retry-After may be delta-seconds OR an HTTP-date; parse both. The date form previously fell to
        // `NaN → undefined → non-retryable hard throw` mid-run — now it backs off. (issue #9)
        const retryAfterMs = parseRetryAfterMs(res.headers.get('retry-after'));
        gate.onThrottle();
        throw new PortalThrottleError(res.status, retryAfterMs);
      }
      if (!res.ok) {
        // A non-OK gateway can send headers then STALL the body — an unbounded `res.text()` would hang the
        // await forever, the `finally` (gate.release) never runs, and every chain eventually parks in
        // gate.acquire(). Bound it with the same SOFT idle pattern as the OK path — on a stall we resolve
        // with what we have (never abort, per issue #14) so the typed error still throws PROMPTLY. Only
        // the message-extraction below needs the body, so a truncated/empty body is acceptable. (PR #16 review)
        const text = (await readTextWithIdle(res, idleTimeoutMs)).slice(0, 300);
        // a dataset that can't serve a requested field (absent column / unknown field) → drop + retry.
        const schemaErr = parseSchemaFieldError(res.status, text, body);
        if (schemaErr) throw schemaErr;
        // a dataset that doesn't begin at genesis (e.g. TAC starts at block 1) 400s when queried below its first block.
        const s =
          res.status === 400 &&
          text.match(/dataset starts (?:from|at) block (\d+)/i);
        if (s) throw new PortalDatasetStartError(Number(s[1]));
        // a dense range can exceed the Portal's per-query size/work estimate → 400 "Query is too large".
        if (res.status === 400 && /query is too large/i.test(text))
          throw new PortalQueryTooLargeError(cursor);
        throw new PortalHttpError(res.status, cursor, text);
      }
      const blocks: RawBlock[] = [];
      let last = cursor;
      // idleTimeoutMs bounds the gap between chunks (soft, cancel-on-stall — see the two-phase note above).
      for await (const line of ndjsonLines(
        res.body!,
        (n) => {
          stats.bytes += n;
        },
        idleTimeoutMs,
      )) {
        const b = JSON.parse(line) as RawBlock;
        blocks.push(b);
        if (typeof b.header?.number === 'number' && b.header.number > last)
          last = b.header.number;
      }
      gate.onOk(); // clean full response → a generation of these ramps concurrency up
      return { blocks, last };
    } catch (err) {
      if (isNetworkError(err)) gate.onThrottle(); // dropped/timed-out connections under load = congestion
      throw err;
    } finally {
      disarmConnect();
      stats.fetchMs += Date.now() - tFetch;
      gate.release();
      stats.inflight--;
    }
  }

  async function* stream(
    query: PortalQuery,
    from: number,
    to: number,
    opts?: StreamOpts,
  ): AsyncGenerator<RawBlock[]> {
    const neededMissing = opts?.neededMissing;
    const onRows = opts?.onRows;
    let cursor = from;
    const dropped = new Set<string>();
    const triedCols = new Set<string>();
    while (cursor <= to) {
      let attempt = 0;
      let batch: Awaited<ReturnType<typeof fetchBatch>> | undefined;
      while (batch === undefined) {
        const body = JSON.stringify({
          ...stripFields(query, dropped),
          fromBlock: cursor,
          toBlock: to,
        });
        try {
          batch = await fetchBatch(body, cursor);
        } catch (err) {
          if (err instanceof PortalQueryTooLargeError) {
            // Portal caps request BYTES, not range — bisecting blocks can't help. If a merged+batched
            // body still overflows, the address batch itself is too big → fail loud with the lever.
            throw new Error(
              `Portal query body exceeds MAX_RAW_QUERY_SIZE even after merging event filters — lower PORTAL_MAX_ADDRESSES (currently ${PORTAL_MAX_ADDRESSES}) to shrink the address batch. @ ${cursor}`,
            );
          }
          if (err instanceof PortalDatasetStartError) {
            if (err.startsAt > to) return; // whole chunk precedes the dataset — nothing to fetch
            if (err.startsAt > cursor) {
              cursor = err.startsAt;
              continue;
            } // skip the missing prefix
            throw err; // start ≤ cursor yet still 400 ⇒ not a below-start issue; surface it
          }
          if (err instanceof PortalSchemaFieldError) {
            if (triedCols.has(err.tag)) throw err; // dropping its field didn't help → real error
            triedCols.add(err.tag);
            dropped.add(err.fieldKey); // drop for THIS chunk's retries only (chunks that have it keep it)
            if (!DROPPABLE_FIELDS.has(err.fieldKey))
              neededMissing?.add(`${err.fieldKey} (${err.tag})`);
            else
              logDebug(
                `Portal ${chainName} [${from},${to}]: dataset can't serve '${err.tag}' → skipping non-load-bearing field ${err.fieldKey}`,
              );
            continue;
          }
          if (!isTransientError(err) || attempt++ >= 10) throw err;
          stats.errors++;
          stats.retries++;
          // Honor a POSITIVE server-advised back-off (capped 30s); otherwise — including an absent or
          // unparseable Retry-After — use exponential back-off. `parseRetryAfterMs` already collapses
          // "no advice" to `undefined`, so this never degenerates into a zero-wait retry storm. (issue #9)
          const advised =
            err instanceof PortalThrottleError ? err.retryAfterMs : undefined;
          const wait =
            advised !== undefined && advised > 0
              ? Math.min(advised, 30_000)
              : Math.min(500 * 2 ** attempt, 30_000);
          await sleep(wait);
        }
      }
      if (batch === 'done') return;
      if (onRows) onRows(countRows(batch.blocks));
      yield batch.blocks;
      // Progress by construction (INV-13): fetchBatch initialises `last = cursor` and only ever raises
      // it, so `cursor = last + 1 ≥ cursor + 1` — the cursor strictly advances on every yielded batch,
      // and a 204 terminates. No runtime guard is needed (and none could ever fire).
      cursor = batch.last + 1;
    }
  }

  const finalizedHead = async (): Promise<number | undefined> => {
    // Bound the probe so a hung /finalized-head can't stall the finality-gap decision — in the SAME two
    // phases as fetchBatch, and for the SAME reason (issue #14): `AbortSignal.timeout` would stay live
    // while we read the body, and portal.ts sends `accept-encoding: gzip` globally, so a live abort landing
    // on an in-flight gzip body is the exact process-kill shape. So:
    //   (a) CONNECT/headers phase — a genuine AbortController.abort(), disarmed the instant headers arrive.
    //   (b) BODY phase — we OWN the read: acquire the body reader and accumulate the text under a SOFT idle
    //       deadline (`readTextWithIdle`), then `JSON.parse` (json() ≡ text+parse, so identical on the
    //       happy path). On a stall we `reader.cancel()` — LEGAL because we hold the lock, unlike a
    //       `body.cancel()` on a body already locked by `r.json()`, which the Web Streams spec REJECTS,
    //       leaking the socket — and fall through to `undefined`. Never abort() (graceful teardown).
    // Every failure collapses to `undefined`, so the caller stays conservative. (PR #16 review)
    const controller = new AbortController();
    let connectTimer: ReturnType<typeof setTimeout> | undefined = setTimeout(
      () => controller.abort(),
      finalizedHeadTimeoutMs,
    );
    connectTimer.unref?.();
    const disarm = () => {
      if (connectTimer !== undefined) {
        clearTimeout(connectTimer);
        connectTimer = undefined;
      }
    };
    try {
      const r = await fetchImpl(`${portalUrl}/finalized-head`, {
        headers,
        signal: controller.signal,
      });
      disarm(); // headers arrived — never abort() once the body is streaming (issue #14)

      // Own-read the body text under the soft deadline, then parse (json() ≡ text+parse). A simple mock or
      // an empty body (no `.body`) falls through readTextWithIdle to `r.text()`; if it exposes neither, use
      // `r.json()` directly. A stall yields '' → JSON.parse throws → caught → undefined.
      const h =
        r.body || typeof r.text === 'function'
          ? JSON.parse(await readTextWithIdle(r, finalizedHeadTimeoutMs))
          : await r.json();
      if (typeof h?.number === 'number') return h.number;
    } catch {
      /* head unknown (connect timed out, fetch failed, or bad JSON) → caller stays conservative */
    } finally {
      disarm();
    }

    return undefined;
  };

  // The head probe is cheap and load-bearing for the finality-gap decision, so callers retry it.
  // `sleepImpl` injection keeps the retry cadence testable (no real timers in unit tests).
  const finalizedHeadRetry = async (
    attempts = 3,
  ): Promise<number | undefined> => {
    for (let attempt = 0; attempt < attempts; attempt++) {
      const h = await finalizedHead();
      if (h !== undefined) return h;
      await sleep(200 * (attempt + 1));
    }
    return undefined;
  };

  return { finalizedHead, finalizedHeadRetry, stream };
}
