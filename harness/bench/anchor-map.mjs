// anchor-map.mjs — the PURE request→response mapping for the pinned-anchor RPC shim (anchor-shim.mjs).
//
// WHY a separate module: the shim's whole job is a deterministic map from a JSON-RPC request to a
// response drawn from a COMMITTED snapshot of real chain headers — no network, no clock, no I/O. That
// map is the thing whose correctness matters (does it serve exactly the anchors ponder asks for, and
// LOUDLY reject everything else?), so it lives here as a pure function with unit tests, and the HTTP
// server (anchor-shim.mjs) is a thin transport around it.
//
// The bench's guarantee is that a successful end-capped [deploy, head] backfill makes ZERO external RPC
// calls: the ONLY RPC traffic is per chain 1× eth_chainId + a handful of eth_getBlockByNumber for the
// startup anchors (latest, finalized-target = latest−finalityBlockCount, deploy=start, head=end).
// This module serves precisely those from the snapshot and turns ANYTHING else into a JSON-RPC error
// plus a caller-surfaced "unexpected" flag, so an unforeseen call is LEARNED about, never papered over
// with a plausible-looking junk block. (Ponder fetches the finalized target BY NUMBER — latest−finality
// — not by the "finalized" tag; we still serve the finalized/safe TAGS defensively, mapped to the same
// finalized-target header, so a viem/ponder path that does use the tag is covered rather than rejected.)
//
// Anchor requests observed in @subsquid/ponder 0.16.6 (runtime/index.ts, runtime/historical.ts):
//   eth_getBlockByNumber(["latest",   false])                                  → latest header
//   eth_getBlockByNumber([hex(latest.number − finalityBlockCount), false])     → finalized-target header
//   eth_getBlockByNumber([hex(start=deploy),   false])                         → deploy header
//   eth_getBlockByNumber([hex(cached=deploy−1), false])                        → deploy-parent header
//   eth_getBlockByNumber([hex(end=head),       false])                         → head header (end ≤ finalized)
// The deploy-parent (deploy−1) fetch is getLocalSyncProgress's "cached" diagnostic: getCachedBlock
// returns `firstMissingBlock − 1` (the last-completed block) even on a FRESH store, so on every run
// ponder fetches the block BEFORE the start of the backfill. It must be pinned or the run wedges.
// The `fullTransactions` boolean is ALWAYS false on ponder 0.16.6's startup surface. A `true` request
// would mean an unforeseen caller, and the snapshot only carries header fields — a light header is the
// WRONG-shaped response for fullTransactions=true — so we reject it fail-loud (error + unexpected flag)
// rather than silently serve a header that omits the requested transaction bodies.

// JSON-RPC error codes we return. -32601 method-not-found for unknown methods; -32602 invalid-params for
// a block the snapshot does not pin (the same code Polygon's public RPC returned for the wedging call,
// deliberately: a caller that mis-handles it here would mis-handle it against a real endpoint too).
export const ERR_METHOD_NOT_FOUND = -32601;
export const ERR_INVALID_PARAMS = -32602;

// Block tags the shim understands. "latest" → the snapshot's latest header; "finalized"/"safe" → the
// finalized-target header (see header note). "earliest"/"pending" are deliberately NOT served — the
// bench never asks for them and serving a guess would violate the fail-loud contract.
const LATEST_TAGS = new Set(['latest']);
const FINALIZED_TAGS = new Set(['finalized', 'safe']);

// Normalize a hex/decimal/number block key to a canonical lowercase 0x-hex string, or null if it is not
// a plain numeric block number (a tag, or garbage). We match snapshot entries by canonical hex so that
// "0x13d92e7", "0x013D92E7" and the decimal form all resolve to the one pinned header.
export function canonicalizeBlockNumber(raw) {
  if (typeof raw === 'number' && Number.isInteger(raw) && raw >= 0) {
    return `0x${raw.toString(16)}`;
  }

  if (typeof raw !== 'string') {
    return null;
  }

  const s = raw.trim().toLowerCase();
  if (/^0x[0-9a-f]+$/.test(s)) {
    // strip leading zeros (but keep a single 0) so 0x00ff and 0xff collide onto the same key.
    const stripped = s.slice(2).replace(/^0+(?=.)/, '');
    return `0x${stripped}`;
  }

  if (/^[0-9]+$/.test(s)) {
    return `0x${BigInt(s).toString(16)}`;
  }

  return null;
}

// Build the immutable lookup an anchor snapshot presents to the shim, for ONE chain. `snapshotChain`
// is a chains[] entry from an anchors-<date>.json file: { id, headers: { latest, finalizedTarget,
// deploy, deployParent, head } } where each header carries at least { number (0x hex), hash, ... }. We
// index every pinned header by its canonical hex number, and remember which header answers the
// latest/finalized tags. Throws if the snapshot is malformed — a shim that cannot build its map must
// fail at startup, never serve a partial surface.
export function buildChainAnchors(snapshotChain) {
  if (!snapshotChain || typeof snapshotChain !== 'object') {
    throw new Error('buildChainAnchors: snapshot chain entry is missing');
  }

  const { id, headers } = snapshotChain;
  if (!Number.isInteger(id)) {
    throw new Error('buildChainAnchors: chain entry has no integer id');
  }
  if (!headers || typeof headers !== 'object') {
    throw new Error(`buildChainAnchors: chain ${id} has no headers`);
  }

  const byNumber = new Map();
  for (const role of [
    'latest',
    'finalizedTarget',
    'deploy',
    'deployParent',
    'head',
  ]) {
    const header = headers[role];
    if (!header || typeof header.number !== 'string') {
      throw new Error(
        `buildChainAnchors: chain ${id} is missing the "${role}" header`,
      );
    }

    const key = canonicalizeBlockNumber(header.number);
    if (key === null) {
      throw new Error(
        `buildChainAnchors: chain ${id} "${role}" header has a non-numeric block number ${header.number}`,
      );
    }

    byNumber.set(key, header);
  }

  return {
    id,
    chainIdHex: `0x${id.toString(16)}`,
    byNumber,
    latest: headers.latest,
    finalizedTarget: headers.finalizedTarget,
  };
}

// Serve one JSON-RPC request against a chain's anchors. PURE: no I/O, no clock. Returns
//   { status: 200, body: <jsonrpc result envelope>, unexpected?: string }
// where `unexpected` is set (to a human-readable reason) whenever the request fell outside the pinned
// surface — the transport logs it to stderr so we LEARN about every unforeseen call. The body is always
// a well-formed JSON-RPC envelope (result on success, error otherwise) so a client sees a normal RPC.
//
// `req` is the parsed JSON-RPC object { id, method, params }. `anchors` is a buildChainAnchors() result.
export function serveRpc(req, anchors) {
  const id = req && req.id !== undefined ? req.id : null;
  const method = req ? req.method : undefined;

  if (method === 'eth_chainId') {
    return { status: 200, body: ok(id, anchors.chainIdHex) };
  }

  if (method === 'eth_getBlockByNumber') {
    return serveBlockByNumber(id, req.params, anchors);
  }

  // Any other method is outside the bench's expected surface — reject AND flag.
  return {
    status: 200,
    body: rpcError(
      id,
      ERR_METHOD_NOT_FOUND,
      `anchor-shim: unexpected method ${String(method)}`,
    ),
    unexpected: `method ${String(method)}`,
  };
}

function serveBlockByNumber(id, params, anchors) {
  const tag = Array.isArray(params) ? params[0] : undefined;
  const fullTx = Array.isArray(params) ? params[1] : undefined;

  // fullTransactions=true is off the startup surface (ponder 0.16.6 only ever sends false). The snapshot
  // carries header fields only, so a light header would be a wrong-shaped response — reject AND flag.
  if (fullTx === true) {
    return {
      status: 200,
      body: rpcError(
        id,
        ERR_INVALID_PARAMS,
        `anchor-shim: fullTransactions=true is off the pinned surface for chain ${anchors.id} (only light headers are pinned)`,
      ),
      unexpected: `fullTransactions=true (block ${String(tag)})`,
    };
  }

  // tag requests: latest / finalized / safe map to their pinned header.
  if (typeof tag === 'string') {
    const lower = tag.trim().toLowerCase();
    if (LATEST_TAGS.has(lower)) {
      return { status: 200, body: ok(id, anchors.latest) };
    }
    if (FINALIZED_TAGS.has(lower)) {
      return { status: 200, body: ok(id, anchors.finalizedTarget) };
    }
  }

  // numeric requests: serve the pinned header for that exact block, else fail loud.
  const key = canonicalizeBlockNumber(tag);
  if (key !== null) {
    const header = anchors.byNumber.get(key);
    if (header) {
      return { status: 200, body: ok(id, header) };
    }

    return {
      status: 200,
      body: rpcError(
        id,
        ERR_INVALID_PARAMS,
        `anchor-shim: block ${key} is not a pinned anchor for chain ${anchors.id}`,
      ),
      unexpected: `block ${key}`,
    };
  }

  // an unrecognized tag ("earliest"/"pending"/garbage) — reject and flag.
  return {
    status: 200,
    body: rpcError(
      id,
      ERR_INVALID_PARAMS,
      `anchor-shim: unsupported block tag ${String(tag)} for chain ${anchors.id}`,
    ),
    unexpected: `tag ${String(tag)}`,
  };
}

function ok(id, result) {
  return { jsonrpc: '2.0', id, result };
}

function rpcError(id, code, message) {
  return { jsonrpc: '2.0', id, error: { code, message } };
}
