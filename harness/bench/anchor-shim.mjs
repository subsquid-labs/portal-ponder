#!/usr/bin/env node
// anchor-shim.mjs — a local pinned-anchor JSON-RPC server for the deterministic 15-chain flagship
// benchmark. It serves the startup anchors of an end-capped [deploy, head] Portal backfill from a
// COMMITTED snapshot of REAL chain headers, so a bench run has ZERO external RPC dependence and is
// reproducible from the snapshot file alone.
//
// The mapping logic (request → pinned header, and fail-loud on anything else) lives in the pure module
// anchor-map.mjs and is unit-tested there; this file is the thin HTTP transport around it plus the
// snapshot loader.
//
//   node anchor-shim.mjs --anchors harness/bench/anchors-<date>.json [--port 8645] [--host 127.0.0.1]
//   node anchor-shim.mjs --selftest        # spawn on a random port, exercise the surface, exit 0/1
//
// One port, chain selected by path or ?chain= query:
//   POST http://127.0.0.1:8645/137        → chain 137
//   POST http://127.0.0.1:8645/?chain=137 → chain 137
// GET /health → 200 { ok, chains:[...] } (liveness probe for the run driver).
//
// EVERY request outside the pinned surface (unknown method, unknown chain, un-pinned block) is answered
// with a JSON-RPC error AND a loud stderr line prefixed "anchor-shim UNEXPECTED" — we want to LEARN
// about an unforeseen call, never silently serve junk that could corrupt a "correctness-proven" run.

import { readFileSync } from 'node:fs';
import http from 'node:http';
import { buildChainAnchors, serveRpc } from './anchor-map.mjs';

// Parse `--flag value` / `--flag=value` / `--flag` (boolean) argv into a plain object.
export function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) {
      continue;
    }

    const eq = a.indexOf('=');
    if (eq >= 0) {
      out[a.slice(2, eq)] = a.slice(eq + 1);

      continue;
    }

    const key = a.slice(2);
    const next = argv[i + 1];
    if (next !== undefined && !next.startsWith('--')) {
      out[key] = next;
      i++;
    } else {
      out[key] = true;
    }
  }

  return out;
}

// Extract the chain id from a request URL: /137, /rpc/137, or /?chain=137. Returns an integer or null.
export function chainIdFromUrl(rawUrl) {
  let url;
  try {
    url = new URL(rawUrl, 'http://shim.local');
  } catch {
    return null;
  }

  const q = url.searchParams.get('chain');
  if (q && /^[0-9]+$/.test(q)) {
    return Number(q);
  }

  // last non-empty path segment that is all digits
  const segs = url.pathname.split('/').filter(Boolean);
  const last = segs[segs.length - 1];
  if (last && /^[0-9]+$/.test(last)) {
    return Number(last);
  }

  return null;
}

// Build the per-chain anchors map from a parsed anchors-<date>.json snapshot. Returns Map<id, anchors>.
export function buildAnchorsByChain(snapshot) {
  if (!snapshot || !Array.isArray(snapshot.chains)) {
    throw new Error('anchor-shim: snapshot has no chains[] array');
  }

  const byChain = new Map();
  for (const chain of snapshot.chains) {
    const anchors = buildChainAnchors(chain);
    byChain.set(anchors.id, anchors);
  }

  if (byChain.size === 0) {
    throw new Error('anchor-shim: snapshot contains zero chains');
  }

  return byChain;
}

// Create (but do NOT start) the HTTP server around a built anchors map. `onUnexpected(reason, chainId)`
// is called for every out-of-surface request (default: a loud stderr line). Exposed for the selftest.
export function createShimServer(byChain, { onUnexpected } = {}) {
  const warn =
    onUnexpected ??
    ((reason, chainId) => {
      process.stderr.write(
        `anchor-shim UNEXPECTED chain=${chainId ?? '?'} ${reason}\n`,
      );
    });

  return http.createServer((request, response) => {
    if (request.method === 'GET') {
      if (request.url === '/health' || request.url === '/') {
        return sendJson(response, 200, {
          ok: true,
          chains: [...byChain.keys()],
        });
      }

      response.writeHead(404).end();

      return;
    }

    if (request.method !== 'POST') {
      response.writeHead(405).end();

      return;
    }

    const chainId = chainIdFromUrl(request.url ?? '/');
    if (chainId === null || !byChain.has(chainId)) {
      warn(`unknown chain in url ${request.url}`, chainId);

      return sendJson(response, 200, {
        jsonrpc: '2.0',
        id: null,
        error: {
          code: -32602,
          message: `anchor-shim: no pinned anchors for chain ${chainId ?? 'unknown'}`,
        },
      });
    }

    const anchors = byChain.get(chainId);
    const chunks = [];
    request.on('data', (c) => chunks.push(c));
    request.on('end', () => {
      let parsed;
      try {
        parsed = JSON.parse(Buffer.concat(chunks).toString('utf8') || 'null');
      } catch {
        warn(`malformed json body on chain ${chainId}`, chainId);

        return sendJson(response, 200, {
          jsonrpc: '2.0',
          id: null,
          error: { code: -32700, message: 'anchor-shim: parse error' },
        });
      }

      // JSON-RPC batch: an array of requests → an array of responses.
      if (Array.isArray(parsed)) {
        const bodies = parsed.map((req) => {
          const res = serveRpc(req, anchors);
          if (res.unexpected) {
            warn(res.unexpected, chainId);
          }

          return res.body;
        });

        return sendJson(response, 200, bodies);
      }

      const res = serveRpc(parsed, anchors);
      if (res.unexpected) {
        warn(res.unexpected, chainId);
      }

      return sendJson(response, res.status, res.body);
    });
  });
}

function sendJson(response, status, body) {
  const payload = JSON.stringify(body);
  response.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
  });
  response.end(payload);
}

async function runServer(args) {
  const anchorsPath = args.anchors;
  if (!anchorsPath || anchorsPath === true) {
    console.error(
      'anchor-shim: --anchors <anchors-<date>.json> is required (or --selftest)',
    );
    process.exit(2);
  }

  const snapshot = JSON.parse(readFileSync(anchorsPath, 'utf8'));
  const byChain = buildAnchorsByChain(snapshot);
  const server = createShimServer(byChain);
  const port = Number(args.port ?? process.env.ANCHOR_SHIM_PORT ?? 8645);
  const host = String(args.host ?? '127.0.0.1');

  server.listen(port, host, () => {
    console.log(
      `anchor-shim: ${byChain.size} chains on http://${host}:${port} (chains ${[...byChain.keys()].join(',')})`,
    );
  });
}

// ── selftest: spawn on a random port with a SYNTHETIC in-memory snapshot and exercise the whole
// surface (chainId, latest tag, finalized/safe tags, each pinned number, an un-pinned number, an
// unknown method, an unknown chain). Runnable in CI — touches no real service, no committed file. ──
async function selftest() {
  const snapshot = {
    chains: [
      {
        id: 137,
        headers: {
          latest: { number: '0x64', hash: '0xaa' },
          finalizedTarget: { number: '0x32', hash: '0xbb' },
          deploy: { number: '0x0a', hash: '0xcc' },
          deployParent: { number: '0x09', hash: '0xce' },
          head: { number: '0x1e', hash: '0xdd' },
        },
      },
    ],
  };

  const byChain = buildAnchorsByChain(snapshot);
  const unexpected = [];
  const server = createShimServer(byChain, {
    onUnexpected: (reason, chainId) => unexpected.push({ reason, chainId }),
  });

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  const base = `http://127.0.0.1:${port}`;

  const call = async (chainSuffix, method, params) => {
    const res = await fetch(`${base}${chainSuffix}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
    });

    return res.json();
  };

  const failures = [];
  const expect = (cond, label) => {
    if (!cond) {
      failures.push(label);
    }
  };

  try {
    const chainId = await call('/137', 'eth_chainId', []);
    expect(
      chainId.result === '0x89',
      `eth_chainId → 0x89 (got ${chainId.result})`,
    );

    const latest = await call('/137', 'eth_getBlockByNumber', [
      'latest',
      false,
    ]);
    expect(latest.result?.number === '0x64', 'latest tag → 0x64 header');

    const finalized = await call('/137', 'eth_getBlockByNumber', [
      'finalized',
      false,
    ]);
    expect(
      finalized.result?.number === '0x32',
      'finalized tag → finalized-target 0x32',
    );

    const safe = await call('/137', 'eth_getBlockByNumber', ['safe', false]);
    expect(safe.result?.number === '0x32', 'safe tag → finalized-target 0x32');

    // each pinned number, including a leading-zero / mixed-case form for the deploy block
    const deploy = await call('/137', 'eth_getBlockByNumber', ['0x00A', false]);
    expect(
      deploy.result?.number === '0x0a',
      'deploy 0x00A (normalized) → header',
    );

    // deploy-parent (deploy − 1): ponder fetches it at startup, so it MUST be on the surface
    const deployParent = await call('/137', 'eth_getBlockByNumber', [
      '0x9',
      false,
    ]);
    expect(
      deployParent.result?.number === '0x09',
      'deploy-parent 0x09 (deploy − 1) → header',
    );

    const head = await call('/137', 'eth_getBlockByNumber', ['0x1e', false]);
    expect(head.result?.number === '0x1e', 'head 0x1e (fullTx=false) → header');

    // fullTransactions=true is off the startup surface: even a pinned block is rejected fail-loud,
    // because a light header would be a wrong-shaped response for a fullTx request.
    const fullTx = await call('/137', 'eth_getBlockByNumber', ['0x1e', true]);
    expect(
      fullTx.error?.code === -32602,
      'fullTransactions=true → -32602 invalid params (off surface)',
    );

    // via ?chain= query too
    const viaQuery = await call('/?chain=137', 'eth_chainId', []);
    expect(viaQuery.result === '0x89', '?chain=137 selects the chain');

    // un-pinned block → error + flagged
    const unpinned = await call('/137', 'eth_getBlockByNumber', [
      '0x999',
      false,
    ]);
    expect(
      unpinned.error?.code === -32602,
      'un-pinned block → -32602 invalid params',
    );

    // unknown method → error + flagged
    const unknownMethod = await call('/137', 'eth_getLogs', [{}]);
    expect(
      unknownMethod.error?.code === -32601,
      'unknown method → -32601 method not found',
    );

    // unknown chain → error + flagged
    const unknownChain = await call('/999', 'eth_chainId', []);
    expect(
      unknownChain.error?.code === -32602,
      'unknown chain → -32602 invalid params',
    );

    // health probe
    const healthRes = await fetch(`${base}/health`);
    const health = await healthRes.json();
    expect(health.ok === true, '/health → { ok:true }');

    // every out-of-surface call must have been FLAGGED (fullTx=true, un-pinned block, unknown method,
    // unknown chain)
    expect(
      unexpected.length === 4,
      `4 unexpected calls flagged (got ${unexpected.length}: ${unexpected
        .map((u) => u.reason)
        .join('; ')})`,
    );
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }

  if (failures.length) {
    console.error(`anchor-shim selftest FAILED (${failures.length}):`);
    for (const f of failures) {
      console.error(`  ✗ ${f}`);
    }
    process.exit(1);
  }

  console.log('anchor-shim selftest PASSED (13 checks)');
  process.exit(0);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.selftest) {
    await selftest();

    return;
  }

  await runServer(args);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(`anchor-shim: ${e?.message ?? e}`);
    process.exit(1);
  });
}
