// rpc-meter.mjs — a tiny counting reverse-proxy in front of a JSON-RPC endpoint.
//
// Every cell points ponder's RPC URL at this meter (PONDER_RPC_URL_1=http://127.0.0.1:<port>); the
// meter forwards each request verbatim to the real (paid) RPC and tallies the JSON-RPC *calls* — a
// batch array counts as N calls, a single object as 1 — so run-cell.sh can record the exact request
// cost of every window and enforce the campaign budget. Method-level counters make it easy to see
// where the RPC spend goes (getLogs vs getBlockReceipts vs traces).
//
//   METER_TARGET=https://rpc… METER_PORT=8645 [METER_FILE=…] node rpc-meter.mjs
//
// Control endpoints (never forwarded):
//   GET  /__count   → { total, batches, byMethod, target, since }
//   POST /__reset   → zero the counters, returns the same shape
//
// No dependencies — node:http + global fetch only, so it runs anywhere the box does.

import { writeFileSync } from 'node:fs';
import { createServer } from 'node:http';

const TARGET = process.env.METER_TARGET;
const PORT = Number(process.env.METER_PORT ?? 8645);
const FILE = process.env.METER_FILE;

if (!TARGET) {
  console.error('rpc-meter: set METER_TARGET to the upstream JSON-RPC URL');
  process.exit(2);
}

const counters = {
  total: 0,
  batches: 0,
  byMethod: {},
  target: TARGET,
  since: Date.now(),
};
let flushTimer = null;

// Debounced persist (at most every 250ms) so a 1M-request run does not thrash the disk; the live
// in-memory counters are always exact and served by /__count regardless.
function scheduleFlush() {
  if (!FILE || flushTimer) {
    return;
  }

  flushTimer = setTimeout(() => {
    flushTimer = null;
    flushNow();
  }, 250);
}

function flushNow() {
  if (!FILE) {
    return;
  }

  try {
    writeFileSync(FILE, JSON.stringify(counters));
  } catch {
    // best-effort: a transient write failure must never break the proxied backfill
  }
}

function tally(body) {
  let parsed;
  try {
    parsed = JSON.parse(body);
  } catch {
    counters.total += 1; // unparseable body still cost a round-trip

    return;
  }

  const calls = Array.isArray(parsed) ? parsed : [parsed];
  if (Array.isArray(parsed)) {
    counters.batches += 1;
  }

  for (const call of calls) {
    counters.total += 1;
    const method =
      call && typeof call === 'object' ? (call.method ?? 'unknown') : 'unknown';
    counters.byMethod[method] = (counters.byMethod[method] ?? 0) + 1;
  }

  scheduleFlush();
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

const server = createServer(async (req, res) => {
  if (req.method === 'GET' && req.url === '/__count') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(counters));

    return;
  }

  if (req.method === 'POST' && req.url === '/__reset') {
    counters.total = 0;
    counters.batches = 0;
    counters.byMethod = {};
    counters.since = Date.now();
    flushNow();
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(counters));

    return;
  }

  let body;
  try {
    body = await readBody(req);
  } catch {
    res.writeHead(400).end();

    return;
  }

  tally(body.toString('utf8'));

  try {
    // fetch transparently decodes any upstream gzip and drops content-encoding, so forwarding the
    // decoded body with a plain JSON content-type is faithful for JSON-RPC.
    const upstream = await fetch(TARGET, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body,
    });
    const buf = Buffer.from(await upstream.arrayBuffer());
    res.writeHead(upstream.status, { 'content-type': 'application/json' });
    res.end(buf);
  } catch (e) {
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: `meter upstream failed: ${e.message}` }));
  }
});

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => {
    flushNow();
    server.close(() => process.exit(0));
  });
}

server.listen(PORT, '127.0.0.1', () => {
  console.log(
    `rpc-meter → ${TARGET} on http://127.0.0.1:${PORT}  (file=${FILE ?? 'none'})`,
  );
});
