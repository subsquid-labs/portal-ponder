const fs = require('node:fs');

const traceFile =
  process.env.CHAOS_AIMD_TRACE_FILE || process.env.AIMD_TRACE_FILE || '';
let seq = 0;
let inFlight = 0;
let maxInFlight = 0;

function append(event) {
  if (!traceFile) return;
  const record = {
    ts: new Date().toISOString(),
    pid: process.pid,
    run: Number(process.env.CHAOS_RUN || 0),
    attempt: Number(process.env.CHAOS_ATTEMPT || 0),
    seq: ++seq,
    ...event,
  };
  try {
    fs.appendFileSync(traceFile, `${JSON.stringify(record)}\n`);
  } catch {
    // Evidence-only hook. Never perturb the app under test.
  }
}

function parseGateLine(args) {
  const line = args.map((a) => (typeof a === 'string' ? a : '')).join(' ');
  const m = line.match(
    /\[portalGate\]\s+concurrency_limit=(\d+)\s+active=(\d+)\s+buffered_rows=(\d+)/,
  );
  if (!m) return;
  append({
    event: 'aimd-gate-log',
    source: 'portalGate-log',
    concurrency: Number(m[1]),
    limit: Number(m[1]),
    active: Number(m[2]),
    rows: Number(m[3]),
  });
}

const originalLog = console.log;
console.log = function aimdConsoleLog(...args) {
  try {
    parseGateLine(args);
  } catch {
    // Ignore trace parse errors.
  }
  return originalLog.apply(this, args);
};

function isFinalizedStream(input) {
  const url =
    typeof input === 'string'
      ? input
      : input instanceof URL
        ? input.href
        : input?.url;
  return typeof url === 'string' && url.includes('/finalized-stream');
}

const originalFetch = globalThis.fetch;
if (typeof originalFetch !== 'function') {
  append({ event: 'aimd-env', error: 'global fetch is unavailable' });
} else {
  globalThis.fetch = async function aimdFetch(input, _init) {
    const traced = isFinalizedStream(input);
    if (!traced) {
      return originalFetch.apply(this, arguments);
    }

    inFlight += 1;
    maxInFlight = Math.max(maxInFlight, inFlight);
    append({
      event: 'aimd-fetch-start',
      source: 'fetch-derived',
      inFlight,
      concurrency: inFlight,
      maxInFlight,
    });

    try {
      const response = await originalFetch.apply(this, arguments);
      append({
        event: 'aimd-fetch-end',
        source: 'fetch-derived',
        status: response?.status ?? null,
        ok: response?.ok ?? null,
        inFlight,
        concurrency: inFlight,
        maxInFlight,
      });
      return response;
    } catch (error) {
      append({
        event: 'aimd-fetch-error',
        source: 'fetch-derived',
        error: error instanceof Error ? error.name : 'Error',
        message: error instanceof Error ? error.message : String(error),
        inFlight,
        concurrency: inFlight,
        maxInFlight,
      });
      throw error;
    } finally {
      inFlight = Math.max(0, inFlight - 1);
      append({
        event: 'aimd-fetch-release',
        source: 'fetch-derived',
        inFlight,
        concurrency: inFlight,
        maxInFlight,
      });
    }
  };

  append({
    event: 'aimd-env',
    source: 'hook',
    min: Number(process.env.PORTAL_MIN_CONCURRENCY || 1),
    max: Number(process.env.PORTAL_MAX_CONCURRENCY || 32),
    start: Number(process.env.PORTAL_START_CONCURRENCY || 4),
  });
}

process.on('exit', () => {
  append({
    event: 'aimd-exit',
    source: 'hook',
    inFlight,
    concurrency: inFlight,
    maxInFlight,
  });
});
