import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MATCH_FIELDS, metaFromEnv, metaMatch } from './chaos-meta.mjs';

const meta = (over = {}) => ({
  app: 'euler-app',
  from: 20529207,
  to: 20579207,
  portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  tarball: 'subsquid-ponder-0.16.6-sqd.2.tgz',
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

test('metaFromEnv: reduces the tarball to its basename (path is box-specific noise)', () => {
  const m = metaFromEnv({
    CHAOS_META_APP: '/abs/path/to/euler-app',
    CHAOS_META_TARBALL: '/some/long/box/path/subsquid-ponder-0.16.6-sqd.2.tgz',
    CHAOS_META_FROM: '100',
    CHAOS_META_TO: '200',
    CHAOS_META_NOW: 'fixed',
  });
  assert.equal(m.tarball, 'subsquid-ponder-0.16.6-sqd.2.tgz');
  assert.equal(m.app, 'euler-app');
  assert.equal(m.from, 100);
  assert.equal(m.to, 200);

  // no tarball → 'published' (the fork's published @subsquid/ponder), a stable identity
  const pub = metaFromEnv({ CHAOS_META_APP: 'a', CHAOS_META_NOW: 'x' });
  assert.equal(pub.tarball, 'published');
});
