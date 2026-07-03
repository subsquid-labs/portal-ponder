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
//   GET  /__scenario → current scenario     POST /__scenario (JSON body) → hot-swap it (deep-merged)
//   GET  /__stats    → per-fault counters    POST /__reset → zero the counters
// /__stats includes missedReset / missedNdjson: a cut fault that could NOT fire (empty upstream body)
// is counted, not silently dropped, so chaos acceptance can require every configured fault fired.
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

// The PORTAL_UPSTREAM requirement + upstream URL/driver are RUNTIME concerns (only needed when the
// server actually runs). They are validated / initialized in main() below so that importing this
// module for its pure exports (mergeScenario, pickFault, resolveCut) needs no env and starts no
// server.

export const DEFAULT_SCENARIO = {
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

// Deep-merge an override scenario onto the defaults. A SHALLOW merge (`{...DEFAULT, ...override}`)
// replaced the whole `head`/`faults` object, so a partial override like `{"faults":{"pReset":1}}`
// dropped every other default probability (p429/p5xx/…) → they became `undefined` → the fault
// probability arithmetic went NaN and the intended fault never fired. Merge the nested `head` and
// `faults` objects field-by-field so a partial override only touches the fields it names.
export function mergeScenario(base, override) {
  const o = override ?? {};

  return {
    ...base,
    ...o,
    head: { ...base.head, ...(o.head ?? {}) },
    faults: { ...base.faults, ...(o.faults ?? {}) },
  };
}

let scenario = DEFAULT_SCENARIO;
if (process.env.CHAOS_SCENARIO) {
  try {
    scenario = mergeScenario(
      DEFAULT_SCENARIO,
      JSON.parse(readFileSync(process.env.CHAOS_SCENARIO, 'utf8')),
    );
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
  // #14 — a reset/ndjson cut that could not fire (empty upstream body) is counted here, NOT silently
  // dropped, so chaos acceptance can require missed==0 (every configured fault actually fired).
  missedReset: 0,
  missedNdjson: 0,
};
let flapTick = 0;

// initialized in main() from UPSTREAM (kept module-scope so the runtime handlers below can use them)
let upstreamUrl;
let drive;

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

// Pick at most one active fault for a /finalized-stream request from the faults probabilities and a
// [0,1) roll. Pure (roll injected) so the cumulative-probability banding is unit-testable.
export function pickFault(faults, roll) {
  const f = faults ?? DEFAULT_SCENARIO.faults;
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

// #14 — a reset / NDJSON-cut fault must actually FIRE. The old code picked a random cutAt in
// [200,4200); if the whole upstream body was shorter than cutAt the cut never triggered and the
// fault silently did nothing (its stat never incremented), so a chaos run could not prove the fault
// fired. resolveCut clamps the cut point INTO the observed body so the fault always injects when the
// body has ANY bytes; a truly empty body is the one case it cannot fire, which it reports as a MISSED
// injection the control-plane stats expose (chaos acceptance can then require missed==0).
//   reset : cut after `cutClamped` bytes (>=1 when the body is non-empty) — leaves a truncated body
//   ndjson: inject the malformed line once at/after `cutClamped` bytes
// Returns { cutAt, missed }. `desiredCut` is the random offset the caller rolled.
export function resolveCut(bodyLen, desiredCut) {
  if (bodyLen <= 0) {
    return { cutAt: 0, missed: true };
  }

  // clamp into [1, bodyLen] so the cut always lands inside the observed bytes and fires
  const cutAt = Math.max(1, Math.min(desiredCut, bodyLen));

  return { cutAt, missed: false };
}

function forwardStream(clientReq, res, body, fault) {
  const proxied = drive(
    `${upstreamUrl.origin}${upstreamUrl.pathname}/finalized-stream${clientReq.url.replace(/^\/finalized-stream/, '')}`,
    { method: 'POST', headers: { 'content-type': 'application/json' } },
    (up) => {
      // For the reset / ndjson-cut faults we must place the cut INSIDE the observed body (see
      // resolveCut, #14), so we buffer the upstream body first, then apply the fault against its real
      // length. Pass-through / non-cut faults just stream through. Bodies are bounded (one chunk of a
      // bounded backfill window), so buffering is safe for the chaos harness.
      const cuts = fault.kind === 'reset' || fault.kind === 'ndjson';
      res.writeHead(up.statusCode ?? 200, {
        'content-type': 'application/x-ndjson',
      });

      if (!cuts) {
        up.on('data', (chunk) => res.write(chunk));
        up.on('end', () => res.end());
        up.on('error', () => res.destroy());

        return;
      }

      const chunks = [];
      up.on('data', (c) => chunks.push(c));
      up.on('error', () => res.destroy());
      up.on('end', () => {
        const buf = Buffer.concat(chunks);
        const desired = 200 + Math.floor(Math.random() * 4000);
        const { cutAt, missed } = resolveCut(buf.length, desired);

        if (missed) {
          // the fault could not fire (empty upstream body) — record it so chaos acceptance can
          // require every configured fault actually fired (missed==0), never a silent no-op.
          stats[fault.kind === 'reset' ? 'missedReset' : 'missedNdjson'] += 1;
          res.end(buf);

          return;
        }

        if (fault.kind === 'reset') {
          res.write(buf.subarray(0, cutAt)); // truncated body …
          stats.reset += 1;
          res.socket?.destroy(); // … then TCP reset mid-NDJSON

          return;
        }

        // ndjson: emit up to the cut, inject one malformed line, then the rest verbatim
        res.write(buf.subarray(0, cutAt));
        res.write('{ this is not valid json\n');
        stats.ndjson += 1;
        res.write(buf.subarray(cutAt));
        res.end();
      });
    },
  );
  proxied.on('error', () => res.writeHead(502).end());
  proxied.end(body);
}

const handler = async (req, res) => {
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
          scenario = mergeScenario(
            DEFAULT_SCENARIO,
            JSON.parse(Buffer.concat(chunks).toString('utf8')),
          );
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
    const fault = pickFault(
      scenario.faults ?? DEFAULT_SCENARIO.faults,
      Math.random(),
    );

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
};

function main() {
  if (!UPSTREAM) {
    console.error('proxy: set PORTAL_UPSTREAM to the real Portal dataset URL');
    process.exit(2);
  }

  upstreamUrl = new URL(UPSTREAM);
  drive = upstreamUrl.protocol === 'https:' ? httpsRequest : httpRequest;

  const server = createServer(handler);
  server.listen(PORT, '127.0.0.1', () => {
    console.log(
      `chaos proxy → ${UPSTREAM} on http://127.0.0.1:${PORT}  (scenario: ${process.env.CHAOS_SCENARIO ?? 'defaults'})`,
    );
  });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
