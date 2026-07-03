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
    this.name = "PortalHttpError";
    this.status = status;
    this.cursor = cursor;
    this.body = body;
  }
}

/**
 * A throttle / congestion response (HTTP 429/5xx/409 on the finalized stream, or a
 * timed-out/reset connection). `retryAfterMs` is the server-advised back-off in ms:
 * `0` when no numeric Retry-After header is present (retry promptly), a positive value
 * when advised, and `undefined` only when the header is a non-numeric HTTP-date — in
 * which case the response is NOT retried (mirrors the original behaviour exactly).
 */
export class PortalThrottleError extends Error {
  readonly status: number | undefined;
  readonly retryAfterMs: number | undefined;
  constructor(status: number | undefined, retryAfterMs: number | undefined) {
    super(`Portal ${status ?? "throttle"}`);
    this.name = "PortalThrottleError";
    this.status = status;
    this.retryAfterMs = retryAfterMs;
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
    this.name = "PortalSchemaFieldError";
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
    this.name = "PortalDatasetStartError";
    this.startsAt = startsAt;
  }
}

/** A request body that exceeded the Portal's raw query-size cap ("query is too large"). */
export class PortalQueryTooLargeError extends Error {
  readonly cursor: number;
  constructor(cursor: number, detail?: string) {
    super(detail ?? `Portal 400: query too large @ ${cursor}`);
    this.name = "PortalQueryTooLargeError";
    this.cursor = cursor;
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
      `Invariant ${id} violated: ${msg}${context ? ` — ${safeJson(context)}` : ""} ` +
        `(see portal/INVARIANTS.md ${id}; set PORTAL_CHECKS=off to bypass runtime checks)`,
    );
    this.name = "InvariantViolation";
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
  const e = err as { message?: string; name?: string; cause?: { message?: string; code?: string } } | undefined;
  const m = `${e?.message ?? ""} ${e?.cause?.message ?? ""} ${e?.cause?.code ?? ""}`.toLowerCase();
  return /socket|closed|econnreset|fetch failed|terminated|timeout|network|epipe|und_err/.test(m) || e?.name === "AbortError";
}

/** The full retryable set: network noise plus throttle responses carrying a concrete back-off. */
export function isTransientError(err: unknown): boolean {
  if (err instanceof PortalThrottleError) return err.retryAfterMs !== undefined;
  return isNetworkError(err);
}
