import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { test } from 'node:test';
import { combineDigests, DIGEST_TABLES, EXCLUDED_COLS } from './pg-digest.mjs';

// combineDigests is the pure fold that turns per-table {table,digest,rows} records into the single
// store digest. The DB-touching part (digestStore) needs a live Postgres and is exercised by the
// driver's built-in `selftest` (id-shift invariance + block-number mutation + run-2 duplication);
// here we lock down the pure fold's load-bearing properties.

const md5 = (s) => createHash('md5').update(s).digest('hex');

test('combineDigests: order-independent (per-table records sorted by table name)', () => {
  const a = combineDigests([
    { table: 'blocks', digest: 'aaa', rows: 2 },
    { table: 'logs', digest: 'bbb', rows: 5 },
  ]);
  const b = combineDigests([
    { table: 'logs', digest: 'bbb', rows: 5 },
    { table: 'blocks', digest: 'aaa', rows: 2 },
  ]);

  assert.equal(a.store, b.store);
});

test('combineDigests: matches the documented md5-of-sorted-"table=digest:rows"-lines algorithm', () => {
  const perTable = [
    { table: 'logs', digest: 'bbb', rows: 5 },
    { table: 'blocks', digest: 'aaa', rows: 2 },
  ];
  const { store, lines } = combineDigests(perTable);

  // lines are sorted; the store digest is md5 over their newline join.
  assert.deepEqual(lines, ['blocks=aaa:2', 'logs=bbb:5']);
  assert.equal(store, md5('blocks=aaa:2\nlogs=bbb:5'));
});

test('combineDigests: a changed per-table digest changes the store digest (content divergence)', () => {
  const base = combineDigests([
    { table: 'blocks', digest: 'aaa', rows: 2 },
  ]).store;
  const mutated = combineDigests([
    { table: 'blocks', digest: 'zzz', rows: 2 },
  ]).store;

  assert.notEqual(base, mutated);
});

test('combineDigests: row COUNT is bound into the digest — a duplicated row (same per-table digest, higher count) still diverges', () => {
  // The run-2 shape: identical logical content duplicated. Even if a table digest were unchanged, the
  // row count binds separately, so a duplication that changes count() cannot collide with the baseline.
  const clean = combineDigests([
    { table: 'factory_addresses', digest: 'same', rows: 8 },
  ]).store;
  const duplicated = combineDigests([
    { table: 'factory_addresses', digest: 'same', rows: 16 },
  ]).store;

  assert.notEqual(clean, duplicated);
});

test('combineDigests: the table NAME is bound — a table renamed (schema-shape change) diverges', () => {
  const asBlocks = combineDigests([
    { table: 'blocks', digest: 'd', rows: 1 },
  ]).store;
  const asLogs = combineDigests([
    { table: 'logs', digest: 'd', rows: 1 },
  ]).store;

  assert.notEqual(asBlocks, asLogs);
});

test('DIGEST_TABLES: the authoritative sync-state set, including intervals + factory tables', () => {
  // Guards against a silent drop of a table from the identity set (fail-closed schema shape).
  for (const t of [
    'blocks',
    'logs',
    'transactions',
    'transaction_receipts',
    'traces',
    'factories',
    'factory_addresses',
    'intervals',
  ]) {
    assert.ok(DIGEST_TABLES.includes(t), `DIGEST_TABLES must include ${t}`);
  }
});

test('EXCLUDED_COLS: surrogate serial ids are excluded ONLY for the two identity-column tables (run-20 fix)', () => {
  // The run-20 false-FAIL fix: factories.id / factory_addresses.id are sequence-backed, non-
  // transactional serials that shift across a killed+resumed flush, so they are excluded from the
  // logical content. NO other table excludes a column (blocks/logs/… key on natural columns).
  assert.deepEqual(EXCLUDED_COLS.factories, ['id']);
  assert.deepEqual(EXCLUDED_COLS.factory_addresses, ['id']);

  const excludedTables = Object.keys(EXCLUDED_COLS).sort();
  assert.deepEqual(excludedTables, ['factories', 'factory_addresses']);
});
