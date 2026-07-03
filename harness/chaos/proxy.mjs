// proxy.mjs — a local fault-injecting reverse proxy in front of a SQD Portal, for the chaos/resume
// campaign. It forwards to PORTAL_UPSTREAM and, per a runtime-switchable scenario, injects the
// wire failures a real Portal client must survive: 429 bursts, 5xx storms, TCP reset mid-body,
// 90s stalls, truncated gzip, malformed NDJSON lines, spurious 204s, and /finalized-head
// freeze / regression / flap. The backfill under test must respond with either a loud typed failure
// or byte-identical completion — never silent corruption.
//
//   PORTAL_UPSTREAM=https://portal.sqd.dev/datasets/ethereum-mainnet \
//   CHAOS_PORT=8700 CHAOS_SCENARIO=scenario.json node harness/chaos/proxy.mjs
//
// Control plane (never forwarded):
//   GET  /__scenario → current scenario     POST /__scenario (JSON body) → hot-swap it
//   GET  /__stats    → per-fault counters    POST /__reset → zero the counters
//
// Scenario JSON:
//   { "head": { "mode": "passthrough|freeze|regression|flap", "value": N, "regressBy": 100000, "delta": N },
//     "faults": { "p429":0, "retryAfter":2, "p5xx":0, "pReset":0, "pStall":0, "stallMs":90000,
//                 "pTruncatedGzip":0, "pMalformedNdjson":0, "p204":0 } }

import { readFileSync } from 'node:fs';
import { createServer, request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';

const UPSTREAM = process.env.PORTAL_UPSTREAM;
const PORT = Number(process.env.CHAOS_PORT ?? 8700);

if (!UPSTREAM) {
  console.error('proxy: set PORTAL_UPSTREAM to the real Portal dataset URL');
  process.exit(2);
}

const DEFAULT_SCENARIO = {
  head: { mode: 'passthrough', value: 0, regressBy: 100_000, delta: 16 },
  faults: {
    p429: 0,
    retryAfter: 2,
    p5xx: 0,
    pReset: 0,
    pStall: 0,
    stallMs: 90_000,
    pTruncatedGzip: 0,
    pMalformedNdjson: 0,
    p204: 0,
  },
};

let scenario = DEFAULT_SCENARIO;
if (process.env.CHAOS_SCENARIO) {
  try {
    scenario = {
      ...DEFAULT_SCENARIO,
      ...JSON.parse(readFileSync(process.env.CHAOS_SCENARIO, 'utf8')),
    };
  } catch (e) {
    console.error(`proxy: bad CHAOS_SCENARIO (${e.message}) — using defaults`);
  }
}

const stats = {
  requests: 0,
  r429: 0,
  r5xx: 0,
  reset: 0,
  stall: 0,
  gzip: 0,
  ndjson: 0,
  r204: 0,
};
let flapTick = 0;

const upstreamUrl = new URL(UPSTREAM);
const drive = upstreamUrl.protocol === 'https:' ? httpsRequest : httpRequest;

// Buffer a small upstream GET (used for /finalized-head passthrough/regression).
function upstreamJson(path) {
  return new Promise((resolve, reject) => {
    const req = drive(
      `${upstreamUrl.origin}${upstreamUrl.pathname}${path}`,
      { method: 'GET' },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
          } catch (e) {
            reject(e);
          }
        });
      },
    );
    req.on('error', reject);
    req.end();
  });
}

async function handleHead(res) {
  const h = scenario.head ?? DEFAULT_SCENARIO.head;
  let number;
  if (h.mode === 'freeze') {
    number = h.value;
  } else if (h.mode === 'flap') {
    flapTick += 1;
    const base =
      h.value ||
      (await upstreamJson('/finalized-head')
        .then((j) => j.number)
        .catch(() => 0));
    number = base + (flapTick % 2 === 0 ? h.delta : -h.delta);
  } else {
    const live = await upstreamJson('/finalized-head').catch(() => ({
      number: h.value || 0,
    }));
    number =
      h.mode === 'regression'
        ? live.number - (h.regressBy ?? 100_000)
        : live.number;
  }

  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ number }));
}

// Pick at most one active fault for a /finalized-stream request.
function pickFault() {
  const f = scenario.faults ?? DEFAULT_SCENARIO.faults;
  const roll = Math.random();
  if (roll < f.p429) {
    return { kind: '429', retryAfter: f.retryAfter };
  }
  if (roll < f.p429 + f.p5xx) {
    return { kind: '5xx' };
  }
  if (roll < f.p429 + f.p5xx + f.p204) {
    return { kind: '204' };
  }
  if (roll < f.p429 + f.p5xx + f.p204 + f.pTruncatedGzip) {
    return { kind: 'gzip' };
  }
  if (roll < f.p429 + f.p5xx + f.p204 + f.pTruncatedGzip + f.pReset) {
    return { kind: 'reset' };
  }
  if (
    roll <
    f.p429 + f.p5xx + f.p204 + f.pTruncatedGzip + f.pReset + f.pStall
  ) {
    return { kind: 'stall', stallMs: f.stallMs };
  }
  if (
    roll <
    f.p429 +
      f.p5xx +
      f.p204 +
      f.pTruncatedGzip +
      f.pReset +
      f.pStall +
      f.pMalformedNdjson
  ) {
    return { kind: 'ndjson' };
  }

  return { kind: 'pass' };
}

function forwardStream(clientReq, res, body, fault) {
  const proxied = drive(
    `${upstreamUrl.origin}${upstreamUrl.pathname}/finalized-stream${clientReq.url.replace(/^\/finalized-stream/, '')}`,
    { method: 'POST', headers: { 'content-type': 'application/json' } },
    (up) => {
      res.writeHead(up.statusCode ?? 200, {
        'content-type': 'application/x-ndjson',
      });
      let sent = 0;
      const cutAt = 200 + Math.floor(Math.random() * 4000); // random mid-body offset for reset
      up.on('data', (chunk) => {
        if (fault.kind === 'reset' && sent + chunk.length >= cutAt) {
          res.write(chunk.subarray(0, Math.max(0, cutAt - sent)));
          stats.reset += 1;
          res.socket?.destroy(); // TCP reset mid-NDJSON

          return;
        }

        if (fault.kind === 'ndjson' && sent >= cutAt) {
          res.write('{ this is not valid json\n');
          stats.ndjson += 1;
          fault.kind = 'pass'; // inject once, then pass the rest through
        }

        sent += chunk.length;
        res.write(chunk);
      });
      up.on('end', () => res.end());
      up.on('error', () => res.destroy());
    },
  );
  proxied.on('error', () => res.writeHead(502).end());
  proxied.end(body);
}

const server = createServer(async (req, res) => {
  // ── control plane ──
  if (req.url === '/__stats') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(stats));

    return;
  }
  if (req.url === '/__reset' && req.method === 'POST') {
    for (const k of Object.keys(stats)) {
      stats[k] = 0;
    }
    res.writeHead(200).end('{}');

    return;
  }
  if (req.url === '/__scenario') {
    if (req.method === 'POST') {
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => {
        try {
          scenario = {
            ...DEFAULT_SCENARIO,
            ...JSON.parse(Buffer.concat(chunks).toString('utf8')),
          };
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify(scenario));
        } catch (e) {
          res.writeHead(400).end(String(e.message));
        }
      });

      return;
    }

    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(scenario));

    return;
  }

  // ── /finalized-head with override ──
  if (req.url.startsWith('/finalized-head')) {
    await handleHead(res).catch(() => res.writeHead(502).end());

    return;
  }

  // ── /finalized-stream with fault injection ──
  if (req.url.startsWith('/finalized-stream')) {
    stats.requests += 1;
    const fault = pickFault();

    if (fault.kind === '429') {
      stats.r429 += 1;
      res.writeHead(429, {
        'retry-after': String(fault.retryAfter),
        'content-type': 'application/json',
      });
      res.end(JSON.stringify({ error: 'chaos: rate limited' }));

      return;
    }
    if (fault.kind === '5xx') {
      stats.r5xx += 1;
      res
        .writeHead([500, 502, 503][Math.floor(Math.random() * 3)])
        .end('chaos: server error');

      return;
    }
    if (fault.kind === '204') {
      stats.r204 += 1;
      res.writeHead(204).end();

      return;
    }
    if (fault.kind === 'gzip') {
      stats.gzip += 1;
      res.writeHead(200, {
        'content-encoding': 'gzip',
        'content-type': 'application/x-ndjson',
      });
      res.write(Buffer.from([0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00])); // gzip header only
      res.write(Buffer.from([0x03, 0x00, 0xde, 0xad])); // then garbage → truncated/corrupt gzip
      res.end();

      return;
    }
    if (fault.kind === 'stall') {
      stats.stall += 1;
      res.writeHead(200, { 'content-type': 'application/x-ndjson' });
      setTimeout(() => res.destroy(), fault.stallMs); // hold the response open, then drop

      return;
    }

    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => forwardStream(req, res, Buffer.concat(chunks), fault));

    return;
  }

  res.writeHead(404).end();
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(
    `chaos proxy → ${UPSTREAM} on http://127.0.0.1:${PORT}  (scenario: ${process.env.CHAOS_SCENARIO ?? 'defaults'})`,
  );
});
