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
 */
export async function* ndjsonLines(
  body: ReadableStream<Uint8Array>,
  onBytes?: (n: number) => void,
): AsyncGenerator<string> {
  const reader = body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  for (;;) {
    const { done, value } = await reader.read();
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

const stripFields = (q: PortalQuery, dropped: Set<string>): PortalQuery => {
  if (dropped.size === 0 || !q.fields) return q;
  const fields: Record<string, Record<string, boolean>> = JSON.parse(
    JSON.stringify(q.fields),
  );
  for (const tf of dropped) {
    const i = tf.indexOf('.');
    const t = tf.slice(0, i),
      f = tf.slice(i + 1);
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
  fetchImpl?: typeof fetch;
  sleepImpl?: (ms: number) => Promise<void>;
  logDebug?: (msg: string) => void;
};

export function createPortalClient(deps: PortalClientDeps): PortalClient {
  const { portalUrl, headers, gate, stats, bufferSize, chainName } = deps;
  const fetchImpl = deps.fetchImpl ?? fetch;
  const sleep = deps.sleepImpl ?? realSleep;
  const logDebug = deps.logDebug ?? (() => {});

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
    try {
      const res = await fetchImpl(
        `${portalUrl}/finalized-stream?buffer_size=${bufferSize}`,
        { method: 'POST', headers, body },
      );
      stats.http++;
      if (res.status === 204) {
        gate.onOk();
        return 'done';
      }
      // Transient, retry with back-off: 429/529 explicit throttle; ALL 5xx gateway/proxy hiccups; 409 on
      // the FINALIZED stream = a gateway "conflict" (finalized data doesn't reorg). Back off on any.
      if (res.status >= 500 || res.status === 429 || res.status === 409) {
        await res.body?.cancel().catch(() => {});
        const ra = Number(res.headers.get('retry-after'));
        gate.onThrottle();
        throw new PortalThrottleError(
          res.status,
          Number.isFinite(ra) ? ra * 1000 : undefined,
        );
      }
      if (!res.ok) {
        const text = (await res.text()).slice(0, 300);
        // a dataset that lacks a requested column (e.g. Monad has no accessList) → the whole request 400s.
        const m =
          res.status === 400 &&
          text.match(/column '([a-z0-9_]+)' is not found in '([a-z_]+)'/i);
        if (m)
          throw new PortalSchemaFieldError(
            colToFieldKey(m[1]!, m[2]!),
            TABLE_TO_KEY[m[2]!] ?? m[2]!,
            m[1]!,
          );
        // OTHER schema shape: a dataset whose schema doesn't know the field → query PARSE error.
        const u =
          res.status === 400 && text.match(/unknown field `([a-zA-Z0-9_]+)`/);
        if (u && u[1]) {
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
          throw new PortalSchemaFieldError(`${table}.${fn}`, table, fn);
        }
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
      for await (const line of ndjsonLines(res.body!, (n) => {
        stats.bytes += n;
      })) {
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
    const dropped = new Set<string>(),
      triedCols = new Set<string>();
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
          const wait =
            err instanceof PortalThrottleError && err.retryAfterMs !== undefined
              ? Math.min(err.retryAfterMs, 30_000)
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
    try {
      const h = await fetchImpl(`${portalUrl}/finalized-head`, {
        headers,
      }).then((r) => r.json());
      if (typeof h?.number === 'number') return h.number;
    } catch {
      /* head unknown → caller stays conservative */
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
