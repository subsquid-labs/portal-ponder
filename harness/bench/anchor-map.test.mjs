import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  buildChainAnchors,
  canonicalizeBlockNumber,
  ERR_INVALID_PARAMS,
  ERR_METHOD_NOT_FOUND,
  serveRpc,
} from './anchor-map.mjs';

// A synthetic one-chain snapshot entry (polygon shape: id 137, five real-shaped headers). deployParent
// is deploy − 1 (0x0a → 0x09): ponder ALWAYS fetches the block before the backfill start at startup.
const SNAP = {
  id: 137,
  headers: {
    latest: { number: '0x5555', hash: '0xlatest' },
    finalizedTarget: { number: '0x548d', hash: '0xfinal' },
    deploy: { number: '0x0a', hash: '0xdeploy' },
    deployParent: { number: '0x09', hash: '0xdeployparent' },
    head: { number: '0x1e', hash: '0xhead' },
  },
};

// ── canonicalizeBlockNumber: hex/decimal/number all collapse onto one key; leading zeros & case fold ──

test('canonicalizeBlockNumber: hex, decimal and number forms collapse to one key', () => {
  assert.equal(canonicalizeBlockNumber('0x1e'), '0x1e');
  assert.equal(canonicalizeBlockNumber('0x1E'), '0x1e', 'uppercase folds');
  assert.equal(
    canonicalizeBlockNumber('0x001e'),
    '0x1e',
    'leading zeros stripped',
  );
  assert.equal(canonicalizeBlockNumber('30'), '0x1e', 'decimal string');
  assert.equal(canonicalizeBlockNumber(30), '0x1e', 'plain number');
  assert.equal(canonicalizeBlockNumber('0x0'), '0x0', 'zero survives as 0x0');
});

test('canonicalizeBlockNumber: tags and garbage are NOT block numbers (→ null)', () => {
  assert.equal(canonicalizeBlockNumber('latest'), null);
  assert.equal(canonicalizeBlockNumber('finalized'), null);
  assert.equal(canonicalizeBlockNumber('0xzz'), null);
  assert.equal(canonicalizeBlockNumber(''), null);
  assert.equal(canonicalizeBlockNumber(-1), null);
  assert.equal(canonicalizeBlockNumber(undefined), null);
});

// ── buildChainAnchors: constructs the lookup, fails closed on a malformed snapshot ──

test('buildChainAnchors: indexes every pinned header by canonical number + tag anchors', () => {
  const a = buildChainAnchors(SNAP);
  assert.equal(a.id, 137);
  assert.equal(a.chainIdHex, '0x89', 'chain id 137 → 0x89 hex');
  assert.equal(a.byNumber.get('0x1e').hash, '0xhead');
  assert.equal(a.byNumber.get('0xa').hash, '0xdeploy', 'deploy 0x0a → key 0xa');
  assert.equal(
    a.byNumber.get('0x9').hash,
    '0xdeployparent',
    'deploy-parent (deploy−1) is indexed too — ponder fetches it at startup',
  );
  assert.equal(a.latest.number, '0x5555');
  assert.equal(a.finalizedTarget.number, '0x548d');
});

test('serveRpc: the deploy-parent (deploy−1) block is a pinned anchor, served not rejected', () => {
  const a = buildChainAnchors(SNAP);
  const res = serveRpc(
    { id: 8, method: 'eth_getBlockByNumber', params: ['0x9', false] },
    a,
  );
  assert.equal(
    res.body.result.hash,
    '0xdeployparent',
    'ponder getCachedBlock fetches firstMissingBlock−1 even on a fresh store',
  );
  assert.equal(res.unexpected, undefined, 'deploy-parent is on the surface');
});

test('buildChainAnchors: a missing deploy-parent header fails closed (never a partial surface)', () => {
  const noParent = {
    id: 137,
    headers: {
      latest: { number: '0x5555' },
      finalizedTarget: { number: '0x548d' },
      deploy: { number: '0x0a' },
      head: { number: '0x1e' },
    },
  };
  assert.throws(
    () => buildChainAnchors(noParent),
    /missing the "deployParent"/,
  );
});

test('buildChainAnchors: a missing header role fails closed (never a partial surface)', () => {
  const missing = { id: 1, headers: { latest: { number: '0x1' } } };
  assert.throws(
    () => buildChainAnchors(missing),
    /missing the "finalizedTarget"/,
  );
});

test('buildChainAnchors: a non-integer chain id fails closed', () => {
  assert.throws(
    () => buildChainAnchors({ id: 'x', headers: {} }),
    /no integer id/,
  );
});

// ── serveRpc: the pinned surface is served, everything else fails loud AND is flagged ──

test('serveRpc: eth_chainId → the chain id as hex', () => {
  const a = buildChainAnchors(SNAP);
  const res = serveRpc({ id: 7, method: 'eth_chainId', params: [] }, a);
  assert.equal(res.body.result, '0x89');
  assert.equal(res.body.id, 7, 'echoes the request id');
  assert.equal(res.unexpected, undefined, 'a served call is not flagged');
});

test('serveRpc: the "latest" tag serves the pinned latest header', () => {
  const a = buildChainAnchors(SNAP);
  const res = serveRpc(
    { id: 1, method: 'eth_getBlockByNumber', params: ['latest', false] },
    a,
  );
  assert.equal(res.body.result.number, '0x5555');
  assert.equal(res.unexpected, undefined);
});

test('serveRpc: "finalized" and "safe" tags both serve the finalized-target header', () => {
  const a = buildChainAnchors(SNAP);
  for (const tag of ['finalized', 'safe']) {
    const res = serveRpc(
      { id: 1, method: 'eth_getBlockByNumber', params: [tag, false] },
      a,
    );
    assert.equal(res.body.result.number, '0x548d', `${tag} → finalized-target`);
    assert.equal(res.unexpected, undefined);
  }
});

test('serveRpc: a pinned block number is served regardless of hex form or fullTx flag', () => {
  const a = buildChainAnchors(SNAP);
  // deploy 0x0a requested with a padded/upcased form and fullTransactions=true
  const res = serveRpc(
    { id: 2, method: 'eth_getBlockByNumber', params: ['0x00A', true] },
    a,
  );
  assert.equal(res.body.result.hash, '0xdeploy');
  assert.equal(res.unexpected, undefined);
});

test('serveRpc: an UN-pinned block number → invalid-params error AND flagged unexpected', () => {
  const a = buildChainAnchors(SNAP);
  const res = serveRpc(
    { id: 3, method: 'eth_getBlockByNumber', params: ['0x99999', false] },
    a,
  );
  assert.equal(res.body.error.code, ERR_INVALID_PARAMS);
  assert.match(res.body.error.message, /not a pinned anchor/);
  assert.ok(res.unexpected, 'the out-of-surface call is flagged for stderr');
});

test('serveRpc: an unknown method → method-not-found error AND flagged', () => {
  const a = buildChainAnchors(SNAP);
  const res = serveRpc({ id: 4, method: 'eth_getLogs', params: [{}] }, a);
  assert.equal(res.body.error.code, ERR_METHOD_NOT_FOUND);
  assert.ok(res.unexpected);
  assert.match(res.unexpected, /method eth_getLogs/);
});

test('serveRpc: an unsupported block tag (earliest/pending) → error AND flagged', () => {
  const a = buildChainAnchors(SNAP);
  for (const tag of ['earliest', 'pending']) {
    const res = serveRpc(
      { id: 5, method: 'eth_getBlockByNumber', params: [tag, false] },
      a,
    );
    assert.equal(res.body.error.code, ERR_INVALID_PARAMS, `${tag} rejected`);
    assert.ok(res.unexpected, `${tag} flagged`);
  }
});
