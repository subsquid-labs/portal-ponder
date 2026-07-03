import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  killsSatisfied,
  MATCH_FIELDS,
  metaFromEnv,
  metaMatch,
  tarballHash,
} from './chaos-meta.mjs';

const meta = (over = {}) => ({
  app: 'euler-app',
  from: 20529207,
  to: 20579207,
  portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  tarball: 'subsquid-ponder-0.16.6-sqd.2.tgz',
  tarballHash: 'sha256:aaaa',
  chainId: 1,
  factory: '0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e',
  scenario: 'none',
  kills: 7,
  ...over,
});

test('metaMatch: identical app/range/portal/tarball/chain/factory → compatible', () => {
  const r = metaMatch(meta({ scenario: 'baseline', kills: 0 }), meta());
  assert.equal(
    r.ok,
    true,
    'scenario/kills differ by design; the identity fields all match',
  );
});

test('metaMatch: a different block range REFUSES the baseline (would be a false pass)', () => {
  const r = metaMatch(meta(), meta({ to: 99999999 }));
  assert.equal(r.ok, false);
  assert.equal(r.mismatches[0].field, 'to');
});

test('metaMatch: a different app / portal / tarball each refuse the baseline', () => {
  assert.equal(metaMatch(meta(), meta({ app: 'univ3-app' })).ok, false);
  assert.equal(
    metaMatch(
      meta(),
      meta({ portal: 'https://portal.sqd.dev/datasets/base-mainnet' }),
    ).ok,
    false,
  );
  assert.equal(
    metaMatch(meta(), meta({ tarball: 'subsquid-ponder-0.16.6-sqd.1.tgz' })).ok,
    false,
  );
});

test('metaMatch: scenario and kills are NOT match fields (a baseline is clean by design)', () => {
  assert.equal(MATCH_FIELDS.includes('scenario'), false);
  assert.equal(MATCH_FIELDS.includes('kills'), false);
  // a baseline (scenario=baseline, kills=0) vs a heavily-killed chaos run must still be compatible
  const r = metaMatch(
    meta({ scenario: 'baseline', kills: 0 }),
    meta({ scenario: 'reset-storm', kills: 231 }),
  );
  assert.equal(r.ok, true);
});

test('metaFromEnv: reduces the tarball to its basename (path is box-specific noise) + records its content hash', () => {
  const dir = mkdtempSync(join(tmpdir(), 'chaos-meta-'));
  const tgz = join(dir, 'subsquid-ponder-0.16.6-sqd.2.tgz');
  writeFileSync(tgz, 'FAKE-TARBALL-BYTES');
  const expectHash = `sha256:${createHash('sha256').update('FAKE-TARBALL-BYTES').digest('hex')}`;

  const m = metaFromEnv({
    CHAOS_META_APP: '/abs/path/to/euler-app',
    CHAOS_META_TARBALL: tgz,
    CHAOS_META_FROM: '100',
    CHAOS_META_TO: '200',
    CHAOS_META_NOW: 'fixed',
  });
  assert.equal(m.tarball, 'subsquid-ponder-0.16.6-sqd.2.tgz');
  assert.equal(m.tarballHash, expectHash, 'records the tarball content sha256');
  assert.equal(m.app, 'euler-app');
  assert.equal(m.from, 100);
  assert.equal(m.to, 200);

  // no tarball → 'published' (the fork's published @subsquid/ponder), a stable identity
  const pub = metaFromEnv({ CHAOS_META_APP: 'a', CHAOS_META_NOW: 'x' });
  assert.equal(pub.tarball, 'published');
  assert.equal(pub.tarballHash, 'published');
});

// #8(c) — the tarball IDENTITY is its content sha256, not just its basename: a re-packed fork tarball
// keeps the same version/filename but different bytes, and a baseline built from a DIFFERENT build
// must NOT reuse as "identical". tarballHash is a MATCH_FIELD, so a content-hash mismatch refuses the
// baseline even when the basename matches. MUTATION: drop 'tarballHash' from MATCH_FIELDS → the
// content-hash-mismatch assertion below passes (r.ok true) → this test fails.
test('tarballHash: content hash is a match field — same name, different bytes REFUSES the baseline', () => {
  const dirA = mkdtempSync(join(tmpdir(), 'chaos-tgz-a-'));
  const dirB = mkdtempSync(join(tmpdir(), 'chaos-tgz-b-'));
  const name = 'subsquid-ponder-0.16.6-sqd.2.tgz';
  const tgzA = join(dirA, name);
  const tgzB = join(dirB, name); // SAME basename, DIFFERENT content
  writeFileSync(tgzA, 'BUILD-ONE-BYTES');
  writeFileSync(tgzB, 'BUILD-TWO-BYTES-DIFFERENT');

  assert.notEqual(
    tarballHash(tgzA),
    tarballHash(tgzB),
    'different bytes → different content hash even with the same filename',
  );
  assert.match(tarballHash(tgzA), /^sha256:[0-9a-f]{64}$/);
  assert.equal(tarballHash(''), 'published');
  assert.equal(tarballHash(undefined), 'published');

  const baseline = metaFromEnv({
    CHAOS_META_TARBALL: tgzA,
    CHAOS_META_NOW: 'x',
  });
  const chaos = metaFromEnv({ CHAOS_META_TARBALL: tgzB, CHAOS_META_NOW: 'x' });
  // basenames match, so the OLD (basename-only) check would have passed
  assert.equal(baseline.tarball, chaos.tarball);
  const r = metaMatch(baseline, chaos);
  assert.equal(r.ok, false, 'a content-hash mismatch REFUSES the baseline');
  assert.equal(r.mismatches[0].field, 'tarballHash');

  assert.equal(
    MATCH_FIELDS.includes('tarballHash'),
    true,
    'tarballHash must be a match field',
  );
});

// #8(b) — the kill floor is enforced at VERIFY time from the chaos store's recorded kills, not only in
// kill-loop. A store that "completed" with too few kills proves nothing about resume. MUTATION: change
// `k < min` to `k < 0` in killsSatisfied → the below-floor assertion passes (ok true) → this test fails.
test('killsSatisfied: kills below MIN_KILLS is NOT satisfied; at/above the floor passes', () => {
  assert.equal(killsSatisfied(0, 1).ok, false, 'zero kills proves nothing');
  assert.equal(killsSatisfied(5, 10).ok, false, 'below the floor');
  assert.equal(killsSatisfied(10, 10).ok, true, 'exactly the floor passes');
  assert.equal(killsSatisfied(231, 25).ok, true, 'above the floor passes');
  // a missing / invalid kills count is unproven → not satisfied (fail closed)
  assert.equal(killsSatisfied(undefined, 1).ok, false);
  assert.equal(killsSatisfied(-3, 1).ok, false);
  assert.equal(killsSatisfied(2.5, 1).ok, false);
});
