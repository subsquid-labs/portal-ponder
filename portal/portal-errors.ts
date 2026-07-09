/**
 * portal-errors.ts — the Portal layer's typed error taxonomy.
 *
 * Contract: every failure the client can recognise gets a NAMED class carrying the
 * fields a caller needs to react (retry, drop-a-field, clamp-a-cursor, …) — replacing
 * the previous ad-hoc `(err as any).retryAfterMs` / `.unsupportedColumn` property
 * tagging. Two predicates classify raw/typed errors for the retry loop:
 *   - `isNetworkError`   — socket/timeout/connection-reset noise that is routine under
 *                          parallel load and always worth retrying.
 *   - `isTransientError` — the full "retry this" set: network errors PLUS throttle
 *                          responses that carry a concrete back-off (`retryAfterMs`).
 *
 * These types are pure (no I/O, no env) so they can be thrown from the functional core
 * and asserted on in unit tests.
 */

/** A non-transient Portal HTTP error (surfaced verbatim; not retried). */
export class PortalHttpError extends Error {
  readonly status: number;
  readonly cursor: number;
  readonly body: string;
  constructor(status: number, cursor: number, body: string) {
    super(`Portal ${status} @ ${cursor}: ${body}`);
    this.name = 'PortalHttpError';
    this.status = status;
    this.cursor = cursor;
    this.body = body;
  }
}

/**
 * A throttle / congestion response (HTTP 429/5xx/409 on the finalized stream, or a
 * timed-out/reset connection). Always retryable. `retryAfterMs` is the server-advised
 * back-off in ms (see `parseRetryAfterMs`): a strictly-positive value from a delta-seconds
 * OR an HTTP-date header when the server advised one, else `undefined` — meaning "no useful
 * advice", so the caller falls back to exponential back-off (NOT a hard throw, and NOT a
 * zero-wait retry). (issue #9)
 */
export class PortalThrottleError extends Error {
  readonly status: number | undefined;
  readonly retryAfterMs: number | undefined;
  constructor(status: number | undefined, retryAfterMs: number | undefined) {
    super(`Portal ${status ?? 'throttle'}`);
    this.name = 'PortalThrottleError';
    this.status = status;
    this.retryAfterMs = retryAfterMs;
  }
}

/**
 * A throttle that PERSISTED through the whole retry budget — the terminal shape. A single 429 is a
 * transient `PortalThrottleError` the stream retries with back-off; when the endpoint keeps throttling
 * for `attempts` retries the loop gives up and throws THIS instead of the bare `Portal 429`. It extends
 * `PortalThrottleError` (so any existing `instanceof` / `isTransientError` classification still holds —
 * the difference is purely the actionable MESSAGE), which the historical sync's fire-and-forget prefetch
 * fan-out would otherwise surface as an opaque `Error: Portal 429`. The message names the likely cause (a
 * shared/rate-limited endpoint, e.g. the free public Portal under a full-history backfill) and the two
 * levers a dev has: point at a DEDICATED Portal, or bound the range (`PONDER_END` / a per-source
 * `endBlock`) so the demo grinds through a smaller window. (issue #116)
 */
export class PortalThrottleExhaustedError extends PortalThrottleError {
  constructor(
    status: number | undefined,
    retryAfterMs: number | undefined,
    endpoint: string,
    chainName: string,
    attempts: number,
  ) {
    super(status, retryAfterMs);
    this.name = 'PortalThrottleExhaustedError';
    this.message =
      `Portal ${status ?? 'throttle'} for ${chainName}: the endpoint kept throttling across ${attempts} attempts (${endpoint}). ` +
      'This usually means a SHARED / rate-limited Portal (e.g. the free public Portal under a full-history backfill). ' +
      'Use a DEDICATED Portal endpoint, or bound the backfill to a smaller window (set PONDER_END, or a per-source endBlock) so a rate-limited endpoint can keep up.';
  }
}

/**
 * A dataset that cannot serve a requested field on this chunk — either the parquet
 * column is absent ("column not found") or the schema doesn't know the field at all
 * ("unknown field"). `fieldKey` is the `<table>.<field>` key we requested (so the
 * client can drop it and retry); `tableKey` is the fields-block table; `tag` is a
 * stable id (the raw column/field name) used to bound retries.
 */
export class PortalSchemaFieldError extends Error {
  readonly fieldKey: string;
  readonly tableKey: string;
  readonly tag: string;
  constructor(fieldKey: string, tableKey: string, tag: string) {
    super(`Portal 400: dataset cannot serve ${fieldKey} (${tag})`);
    this.name = 'PortalSchemaFieldError';
    this.fieldKey = fieldKey;
    this.tableKey = tableKey;
    this.tag = tag;
  }
}

/** A dataset that begins after the queried block ("dataset starts at block N"). */
export class PortalDatasetStartError extends Error {
  readonly startsAt: number;
  constructor(startsAt: number) {
    super(`Portal 400: dataset starts at block ${startsAt}`);
    this.name = 'PortalDatasetStartError';
    this.startsAt = startsAt;
  }
}

/** A request body that exceeded the Portal's raw query-size cap ("query is too large"). */
export class PortalQueryTooLargeError extends Error {
  readonly cursor: number;
  constructor(cursor: number, detail?: string) {
    super(detail ?? `Portal 400: query too large @ ${cursor}`);
    this.name = 'PortalQueryTooLargeError';
    this.cursor = cursor;
  }
}

/**
 * A response body that ended mid-NDJSON-line. A close-delimited response (an intermediary that
 * downgraded away the framing) cut mid-body surfaces as a CLEAN EOF — `read()` reports `done`, so the
 * truncation is detectable only when the flushed partial line fails to parse. The batch was never
 * yielded and the cursor never advanced, so a retry from the same cursor is lossless and duplicate-free
 * — exactly what the retry loop exists for. Always transient.
 */
export class PortalTruncatedBodyError extends Error {
  readonly cursor: number;
  constructor(cursor: number, cause: unknown) {
    super(
      `Portal response body truncated mid-line @ ${cursor} (connection cut without framing): ${
        cause instanceof Error ? cause.message : String(cause)
      }`,
    );
    this.name = 'PortalTruncatedBodyError';
    this.cursor = cursor;
  }
}

/**
 * A `/finalized-stream` 204 received while the cursor is still at or below the requested `toBlock`
 * (issue #47). A 204 means "fromBlock is above the SERVING replica's finalized head" — empirically
 * confirmed: an in-range window (below head) that matches zero filter rows returns 200 carrying the
 * range-end block header as a cursor anchor, never a 204. Load-balanced replicas answer independently,
 * so a replica whose finalized height lags the cached head serves `[cursor, itsHead]` then 204s the
 * tail `[itsHead+1, to]`. Treating that as clean completion recorded PHANTOM coverage — the tail blocks
 * were never delivered yet the caller cached the range as synced (a permanent silent gap in stream mode,
 * where nothing redelivers). So it is a TRANSIENT condition: retry lands on a fresher replica; the retry
 * budget bounds it so a range genuinely no replica can serve fails LOUD rather than holing. Always
 * transient (see `isTransientError`).
 */
export class PortalIncompleteRangeError extends Error {
  readonly cursor: number;
  readonly to: number;
  constructor(cursor: number, to: number) {
    super(
      `Portal 204 mid-range @ ${cursor} (requested toBlock ${to}): serving replica's finalized head is ` +
        `below the requested range end — retrying for a fresher replica rather than recording phantom coverage`,
    );
    this.name = 'PortalIncompleteRangeError';
    this.cursor = cursor;
    this.to = to;
  }
}

/**
 * A violated runtime invariant (see portal-invariant.ts + INVARIANTS.md). The repo's
 * philosophy: a loud crash beats silent corruption. Carries the invariant `id` and a
 * structured `context` so a failure points straight at the catalog row.
 */
export class InvariantViolation extends Error {
  readonly id: string;
  readonly context: Record<string, unknown> | undefined;
  constructor(id: string, msg: string, context?: Record<string, unknown>) {
    super(
      `Invariant ${id} violated: ${msg}${context ? ` — ${safeJson(context)}` : ''} ` +
        `(see portal/INVARIANTS.md ${id}; set PORTAL_CHECKS=off to bypass runtime checks)`,
    );
    this.name = 'InvariantViolation';
    this.id = id;
    this.context = context;
  }
}

const safeJson = (v: unknown): string => {
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
};

/**
 * transient = retry: HTTP 503/529/429 AND network/socket errors (parallel load makes
 * "other side closed" / ECONNRESET / fetch failed routine). Matches on the error and its
 * `cause` chain — Node wraps low-level errors in a `cause`.
 */
export function isNetworkError(err: unknown): boolean {
  const e = err as
    | {
        message?: string;
        name?: string;
        code?: string;
        cause?: { message?: string; code?: string };
      }
    | undefined;
  const m =
    `${e?.message ?? ''} ${e?.code ?? ''} ${e?.cause?.message ?? ''} ${e?.cause?.code ?? ''}`.toLowerCase();
  // z_buf_error / "premature close": a truncated gzip body (the client sends accept-encoding: gzip)
  // surfaces as a zlib error rather than a socket error — same dropped connection, same retry.
  // Raw zlib errors carry the code at the TOP level (no `cause` wrapper), hence `e?.code` above.
  // `premature[ _]close` (not bare `premature`) so a 400 whose body text merely contains the word
  // stays fatal, while both the message ("premature close") and code (ERR_STREAM_PREMATURE_CLOSE) match.
  return (
    /socket|closed|econnreset|fetch failed|terminated|timeout|network|epipe|und_err|z_buf_error|premature[ _]close/.test(
      m,
    ) || e?.name === 'AbortError'
  );
}

/**
 * The full retryable set: network noise plus EVERY throttle response. A 429/5xx/409 is always retryable —
 * the back-off (advised `retryAfterMs` vs exponential) is chosen at the retry site, but the decision to
 * retry never depends on the Retry-After header parsing (a missing/garbage header must not turn a throttle
 * into a hard throw). (issue #9) A truncated body is retryable by construction: the batch was never
 * yielded, so the same-cursor retry is lossless.
 */
export function isTransientError(err: unknown): boolean {
  if (err instanceof PortalThrottleError) return true;
  if (err instanceof PortalTruncatedBodyError) return true;
  if (err instanceof PortalIncompleteRangeError) return true;
  return isNetworkError(err);
}
