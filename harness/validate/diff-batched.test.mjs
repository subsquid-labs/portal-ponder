import assert from 'node:assert/strict';
import { test } from 'node:test';
import { cmpKey, hashRows, mergeCompare, normRow } from './diff-batched.mjs';

// key by (block_number, log_index) as the real logs table would be ordered
const logKey = (r) => [BigInt(r.block_number), BigInt(r.log_index)];
const blockKey = (r) => [BigInt(r.number)];

const log = (bn, li, extra = {}) => ({
  block_number: bn,
  log_index: li,
  data: `d${bn}-${li}`,
  ...extra,
});

test('normRow: sorted keys, bigint→decimal, bytes→hex, dropped column removed', () => {
  const s = normRow(
    { z: 1n, a: new Uint8Array([0xde, 0xad]), total_difficulty: 5n, b: 'x' },
    new Set(['total_difficulty']),
  );
  assert.equal(s, JSON.stringify({ a: 'dead', b: 'x', z: '1' }));
});

test('cmpKey: lexicographic over composite keys', () => {
  assert.equal(cmpKey([1n, 2n], [1n, 3n]), -1);
  assert.equal(cmpKey([2n], [1n, 9n]), 1);
  assert.equal(cmpKey([5n, 5n], [5n, 5n]), 0);
});

test('strict mode: identical ordered streams pass', async () => {
  const rows = [log(100, 0), log(100, 1), log(101, 0)];
  const r = await mergeCompare(rows, rows.slice(), {
    keyFn: logKey,
    mode: 'strict',
  });
  assert.equal(r.fail, false);
  assert.equal(r.shared, 3);
  assert.equal(r.mismatch, 0);
});

test('strict mode: a portal-only log FAILS', async () => {
  const a = [log(100, 0), log(100, 1), log(101, 0)];
  const b = [log(100, 0), log(101, 0)]; // missing (100,1)
  const r = await mergeCompare(a, b, { keyFn: logKey, mode: 'strict' });
  assert.equal(r.fail, true);
  assert.equal(r.onlyA, 1);
  assert.equal(r.onlyB, 0);
});

test('strict mode: a shared-key field mismatch FAILS', async () => {
  const a = [log(100, 0, { data: 'X' })];
  const b = [log(100, 0, { data: 'Y' })];
  const r = await mergeCompare(a, b, { keyFn: logKey, mode: 'strict' });
  assert.equal(r.fail, true);
  assert.equal(r.mismatch, 1);
});

test('blocks mode: RPC-only inert block is reported, not failed', async () => {
  const portal = [
    { number: 100, hash: 'h100' },
    { number: 102, hash: 'h102' },
  ];
  const rpc = [
    { number: 100, hash: 'h100' },
    { number: 101, hash: 'h101' }, // inert event-less block only the RPC path traced
    { number: 102, hash: 'h102' },
  ];
  const r = await mergeCompare(portal, rpc, {
    keyFn: blockKey,
    mode: 'blocks',
  });
  assert.equal(r.fail, false, 'extra RPC-only block must not fail');
  assert.equal(r.onlyB, 1);
  assert.equal(r.shared, 2);
});

test('blocks mode: total_difficulty is excluded, real field mismatch still fails', async () => {
  const drop = new Set(['total_difficulty']);
  const portal = [{ number: 100, hash: 'h', total_difficulty: null }];
  const rpcOk = [{ number: 100, hash: 'h', total_difficulty: 999n }];
  const okRes = await mergeCompare(portal, rpcOk, {
    keyFn: blockKey,
    drop,
    mode: 'blocks',
  });
  assert.equal(okRes.fail, false, 'total_difficulty divergence is tolerated');

  const rpcBad = [{ number: 100, hash: 'DIFFERENT', total_difficulty: 999n }];
  const badRes = await mergeCompare(portal, rpcBad, {
    keyFn: blockKey,
    drop,
    mode: 'blocks',
  });
  assert.equal(badRes.fail, true, 'a real shared-block field mismatch fails');
  assert.equal(badRes.mismatch, 1);
});

test('hashRows: order-independent, field-sensitive, deterministic', () => {
  const rows = [log(101, 0), log(100, 1), log(100, 0)];
  const shuffled = [log(100, 0), log(101, 0), log(100, 1)];
  assert.equal(
    hashRows(rows, logKey),
    hashRows(shuffled, logKey),
    'canonical ordering makes the hash input-order-independent',
  );

  const changed = hashRows(
    [log(100, 0, { data: 'changed' }), log(100, 1), log(101, 0)],
    logKey,
  );
  assert.notEqual(
    hashRows(rows, logKey),
    changed,
    'a field change changes the hash',
  );
});
