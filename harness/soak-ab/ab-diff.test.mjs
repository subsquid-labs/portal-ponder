import assert from 'node:assert/strict';
import {
  chmodSync,
  existsSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { mergeCompare } from '../validate/diff-batched.mjs';
import {
  AB_STAGNATION_DEFAULT_MAX_SKEW_S,
  aggregateKnownBadRows,
  aggregateToleratedIssue27,
  aggregateToleratedIssue36,
  buildBucketExclusionFilter,
  buildTxSql,
  CHECKPOINT_BLOCK_LEN,
  CHECKPOINT_BLOCK_OFFSET_0,
  COMPARE_CHAIN_DEPS,
  chainCounters,
  chainWindowedFail,
  checkpointDecision,
  checkpointMonotonic,
  chunk,
  classifyBucketMismatches,
  classifyFailTaxonomy,
  classifyOnlyBDiff,
  classifyOnlyBRow,
  classifyOnlyBTx,
  classifyOnlyBTxDiff,
  classifySharedTx,
  classifyTxDiff,
  collectOnlyB,
  collectReferenced,
  compareBucketHashes,
  compareChain,
  composeAlerts,
  crossCheckOnlyBCollector,
  extractCheckpointBlock,
  formatKnownBadRowsLine,
  formatToleratedIssue27Line,
  formatToleratedIssue36Line,
  isLegAStagnationFail,
  knownBadRows,
  ONLYB_ROW_CAP,
  psqlArgs,
  psqlExitVerdict,
  psqlRows,
  readStagnationThreshold,
  restartStats,
  sampleToleratedOnlyB,
  sanitizeSchemaIdent,
  stagnationAlerts,
  stagnationDecision,
  stripTxOnlyBRows,
  TOLERATED_CLASSES,
  TOLERATED_ONLYB_CLASSES,
  TOLERATED_SAMPLE_SIZE,
  TX_COL,
  TX_SELECT_COLUMNS,
  writeJsonAtomic,
} from './ab-diff.mjs';

// Build a real ponder@0.16.6-encoded checkpoint the way encodeCheckpoint does (verified against
// package/src/utils/checkpoint.ts): timestamp(10) chainId(16) blockNumber(16) txIndex(16)
// eventType(1) eventIndex(16) = 75 chars. Used to prove extractCheckpointBlock reads the right field.
const encodeCheckpoint = ({
  ts = 0n,
  chainId = 0n,
  blockNumber = 0n,
  txIndex = 0n,
  eventType = 0,
  eventIndex = 0n,
}) =>
  `${String(ts).padStart(10, '0')}${String(chainId).padStart(16, '0')}` +
  `${String(blockNumber).padStart(16, '0')}${String(txIndex).padStart(16, '0')}` +
  `${eventType}${String(eventIndex).padStart(16, '0')}`;

test('classifyTxDiff: B missing A parent txs, all log-referenced → expected class (PASS)', () => {
  const onlyA = ['0xaa', '0xbb']; // A has these, B (realtime stream) does not
  const onlyB = [];
  const referenced = new Set(['0xaa', '0xbb']); // both are parent txs of A-side logs
  const r = classifyTxDiff(onlyA, onlyB, referenced);
  assert.equal(r.fail, false);
  assert.equal(r.class, 'realtime-parent-tx-gap');
  assert.equal(r.expectedMissing, 2);
});

test('classifyTxDiff: any B-extra tx → FAIL', () => {
  const r = classifyTxDiff(['0xaa'], ['0xff'], new Set(['0xaa']));
  assert.equal(r.fail, true);
  assert.deepEqual(r.unexpectedB, ['0xff']);
});

test('classifyTxDiff: an A-only tx no log references → FAIL', () => {
  const r = classifyTxDiff(['0xaa', '0xorphan'], [], new Set(['0xaa']));
  assert.equal(r.fail, true);
  assert.deepEqual(r.unreferencedA, ['0xorphan']);
});

test('classifyTxDiff: perfect identity (no diff at all) → PASS', () => {
  const r = classifyTxDiff([], [], new Set());
  assert.equal(r.fail, false);
  assert.equal(r.expectedMissing, 0);
  assert.equal(r.sharedMismatch, 0);
});

test('classifyTxDiff: a SHARED tx whose full-row fields diverge → FAIL', () => {
  // both stores have the tx (no onlyA/onlyB), but its normalized full-row hash differs on one field
  // — the same finalized transaction is NOT byte-identical, which must FAIL. The old code selected
  // only "hash", so a shared tx with a divergent field silently passed.
  const r = classifyTxDiff([], [], new Set(), 1);
  assert.equal(r.fail, true);
  assert.equal(r.sharedMismatch, 1);
  assert.equal(r.class, 'UNEXPECTED');

  // zero shared mismatches with an otherwise-clean diff still passes (the expected realtime gap)
  const ok = classifyTxDiff(['0xaa'], [], new Set(['0xaa']), 0);
  assert.equal(ok.fail, false);
});

// #21 — the referenced-parent-tx lookup must verify EVERY onlyA hash, no fixed skip-threshold. The
// old `onlyA.length <= 200_000` gate left referenced=[] on a large soak so classifyTxDiff mass-FAILed
// healthy parent-tx gaps. collectReferenced chunks the IN-list and unions across ALL chunks.

test('chunk: splits into fixed-size batches covering every item, last batch is the remainder', () => {
  assert.deepEqual(chunk([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  assert.deepEqual(chunk([], 2), []);
  assert.deepEqual(chunk([1, 2], 5), [[1, 2]]);
  // an onlyA larger than the default chunk size is split, not skipped
  const big = Array.from({ length: 12_003 }, (_, i) => i);
  const batches = chunk(big);
  assert.equal(batches.length, 3, '12003 / 5000 → 3 chunks');
  assert.equal(batches.flat().length, big.length, 'every item is covered');
});

test('collectReferenced: unions referenced hashes across EVERY chunk (any onlyA size verified)', async () => {
  // 3 chunks of 2 — the referenced hashes live in the FIRST, MIDDLE, and LAST chunk. A mutation that
  // stops after the first chunk (or applies the old <=200_000 skip) would miss the middle/last ones.
  const hashes = ['a', 'b', 'c', 'd', 'e', 'f'];
  const seen = new Set(['a', 'd', 'f']); // referenced parent txs, one per chunk
  const calls = [];
  const lookup = async (batch) => {
    calls.push([...batch]);

    return batch.filter((h) => seen.has(h));
  };
  const referenced = await collectReferenced(hashes, lookup, 2);
  assert.deepEqual(calls, [
    ['a', 'b'],
    ['c', 'd'],
    ['e', 'f'],
  ]);
  assert.deepEqual(
    referenced.sort(),
    ['a', 'd', 'f'],
    'referenced hashes from every chunk are unioned',
  );

  // and this is what makes the difference downstream: with the full referenced set, an onlyA of all
  // three referenced hashes is the EXPECTED realtime gap (PASS); if the lookup missed later chunks,
  // classifyTxDiff would flag the un-found ones as unreferenced → false FAIL.
  const cls = classifyTxDiff(['a', 'd', 'f'], [], new Set(referenced));
  assert.equal(
    cls.fail,
    false,
    'a fully-referenced large onlyA is the expected gap, not a FAIL',
  );
});

test('checkpointMonotonic: non-decreasing passes, a regression fails at the point', () => {
  assert.deepEqual(checkpointMonotonic([1n, 1n, 5n, 9n]), { ok: true });
  assert.deepEqual(checkpointMonotonic(['10', '20', '20', '30']), { ok: true });
  const bad = checkpointMonotonic([100n, 250n, 240n]);
  assert.equal(bad.ok, false);
  assert.equal(bad.at, 2);
});

// #9 — the checkpoint ledger (the monotonicity source) must be persisted atomically (temp + rename)
// so a torn write never corrupts it. MUTATION: replace `renameSync(tmp, file)` with a plain
// `writeFileSync(file, ...)` that leaves the temp file behind (or drop the rename) → the "no temp
// file remains" assertion fails.
test('writeJsonAtomic: writes the target with correct content and leaves NO temp file (rename ran)', () => {
  const dir = mkdtempSync(join(tmpdir(), 'ab-atomic-'));
  const target = join(dir, 'soak-ab-checkpoints.json');
  const data = { 1: [100, 200, 300], 8453: [7, 8] };

  writeJsonAtomic(target, data);

  assert.equal(existsSync(target), true, 'the target file exists after write');
  assert.deepEqual(
    JSON.parse(readFileSync(target, 'utf8')),
    data,
    'target has the full serialized content',
  );
  // the atomic path is temp-file + rename: after a successful rename NO `.tmp.*` sibling remains. A
  // mutation that writes directly (no rename) would leave the temp file, failing this assertion.
  const leftovers = readdirSync(dir).filter((f) => f.includes('.tmp.'));
  assert.deepEqual(
    leftovers,
    [],
    'no temp file remains — the rename completed',
  );

  // a second write overwrites atomically (rename over an existing target)
  const data2 = { 1: [100, 200, 300, 400] };
  writeJsonAtomic(target, data2);
  assert.deepEqual(JSON.parse(readFileSync(target, 'utf8')), data2);
  assert.deepEqual(
    readdirSync(dir).filter((f) => f.includes('.tmp.')),
    [],
  );
});

test('psqlExitVerdict: a clean exit passes; ANY non-clean exit fails (no silent zero-rows)', () => {
  // the ONLY passing case: exit 0, no signal, no spawn error
  assert.deepEqual(
    psqlExitVerdict({ code: 0, signal: null, spawnError: null }),
    {
      ok: true,
    },
  );

  // a non-zero exit (bad SQL / connection refused / auth failure) must FAIL, never read as zero rows
  const nonzero = psqlExitVerdict({ code: 2, signal: null, spawnError: null });
  assert.equal(nonzero.ok, false);
  assert.match(nonzero.reason, /exited 2/);

  // killed by signal → fail
  const killed = psqlExitVerdict({
    code: null,
    signal: 'SIGKILL',
    spawnError: null,
  });
  assert.equal(killed.ok, false);
  assert.match(killed.reason, /SIGKILL/);

  // spawn failure (psql not on PATH) → fail, takes precedence
  const noBin = psqlExitVerdict({
    code: null,
    signal: null,
    spawnError: 'spawn psql ENOENT',
  });
  assert.equal(noBin.ok, false);
  assert.match(noBin.reason, /ENOENT/);
});

// The chain-1 E2BIG regression: the query text must NEVER ride in argv. psqlArgs uses `-f -` (read the
// statement from stdin) with NO `-c <sql>` entry, so argv is a small constant no matter how large the
// query is. A 5000-hash IN-list (~340 KiB) blew Linux's per-arg MAX_ARG_STRLEN (128 KiB) on the
// ethereum chain under the old `-c` path; reverting to `'-c', sql` fails this assertion. Pure — no spawn.
test('psqlArgs: SQL rides via stdin (-f -), never in argv — E2BIG-proof', () => {
  const args = psqlArgs('postgres://user@host/db');

  // the argv is the fixed flag set; it must NOT contain `-c` (which would inline the SQL into one arg)
  assert.equal(
    args.includes('-c'),
    false,
    'no -c: the query must not be inlined into argv',
  );

  // it MUST read the statement from stdin: `-f -`
  const fIdx = args.indexOf('-f');
  assert.ok(fIdx >= 0, '-f is present');
  assert.equal(args[fIdx + 1], '-', '-f - reads the query from stdin');

  // and the essential flags/URL survive the refactor unchanged
  assert.equal(args[0], 'postgres://user@host/db');
  for (const flag of ['-X', '-q', '-A', '-t', '-F']) {
    assert.ok(args.includes(flag), `${flag} preserved`);
  }
  const vIdx = args.indexOf('-v');
  assert.equal(args[vIdx + 1], 'ON_ERROR_STOP=1', 'ON_ERROR_STOP=1 preserved');
});

// End-to-end proof through the REAL spawn/stream path: run a query whose text is far larger than
// MAX_ARG_STRLEN (128 KiB) against a stub `psql` on PATH. Under the old `-c <sql>` argv, node's spawn
// would throw E2BIG before the child even ran; via stdin it streams fine. The stub echoes back the
// stdin byte count so we prove the WHOLE oversized query was delivered (not truncated), and asserts it
// was NOT handed a `-c` arg — belt-and-braces against a partial revert.
test('psqlRows: a query larger than MAX_ARG_STRLEN streams via stdin without E2BIG', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'ab-e2big-'));
  const fakePsql = join(dir, 'psql');

  // A stub psql: fail loud if handed `-c` (the E2BIG path); otherwise count stdin bytes and emit the
  // tally as a single SEP-free row so psqlRows yields it and exits 0 (clean).
  writeFileSync(
    fakePsql,
    [
      '#!/usr/bin/env bash',
      'set -euo pipefail',
      'for a in "$@"; do',
      '  if [ "$a" = "-c" ]; then echo "STUB: got -c (argv), E2BIG path" >&2; exit 3; fi',
      'done',
      'n=$(wc -c)', // reads all of stdin
      'printf "%s\\n" "$n"',
    ].join('\n'),
    'utf8',
  );
  chmodSync(fakePsql, 0o755);

  // a query whose text alone exceeds MAX_ARG_STRLEN (131072) — as a single -c arg this is a hard E2BIG
  const bigLen = 300_000;
  const sql = `select 1 -- ${'x'.repeat(bigLen)}`;
  assert.ok(
    sql.length > 131072,
    'the query exceeds the per-arg MAX_ARG_STRLEN cap',
  );

  const prevPath = process.env.PATH;
  process.env.PATH = `${dir}:${prevPath}`;
  try {
    const rows = [];
    for await (const row of psqlRows('postgres://ignored', sql)) {
      rows.push(row);
    }

    assert.equal(
      rows.length,
      1,
      'the stub emitted exactly one row (clean exit, streamed OK)',
    );
    assert.equal(
      Number(rows[0][0]),
      sql.length,
      'the FULL oversized query reached the child via stdin (byte count matches, not truncated)',
    );
  } finally {
    process.env.PATH = prevPath;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('compareBucketHashes: shared buckets must match; one-sided buckets are reported not failed', () => {
  const a = new Map([
    ['0', 'h0'],
    ['1', 'h1'],
    ['2', 'h2'],
  ]);
  const good = new Map([
    ['0', 'h0'],
    ['1', 'h1'],
    ['3', 'hEdge'], // B has an extra edge bucket A hasn't reached
  ]);
  const okRes = compareBucketHashes(a, good);
  assert.equal(okRes.ok, true, 'edge-only buckets do not fail');
  assert.equal(okRes.onlyA, 1); // bucket 2
  assert.equal(okRes.onlyB, 1); // bucket 3

  const drift = new Map([
    ['0', 'h0'],
    ['1', 'DIFFERENT'],
  ]);
  const badRes = compareBucketHashes(a, drift);
  assert.equal(badRes.ok, false);
  assert.equal(badRes.mismatches.length, 1);
  assert.equal(badRes.mismatches[0].bucket, '1');
});

test('restartStats: counts restarts, reports last, flags crash-loop only above 3/hour', () => {
  const now = Date.parse('2026-07-03T12:00:00Z');
  // 2 restarts within the hour, 1 old → not a crash-loop
  const calm = restartStats(
    [
      '2026-07-03T09:00:00Z restart',
      '2026-07-03T11:30:00Z restart',
      '2026-07-03T11:55:00Z restart',
      '', // trailing blank line from the log split
    ],
    now,
  );
  assert.equal(calm.restartCount, 3);
  assert.equal(calm.lastRestartAt, '2026-07-03T11:55:00.000Z');
  assert.equal(calm.restartsLastHour, 2);
  assert.equal(calm.crashLoop, false);

  // 4 restarts within the hour → crash-loop
  const loop = restartStats(
    [
      '2026-07-03T11:10:00Z restart',
      '2026-07-03T11:25:00Z restart',
      '2026-07-03T11:40:00Z restart',
      '2026-07-03T11:59:00Z restart',
    ],
    now,
  );
  assert.equal(loop.restartsLastHour, 4);
  assert.equal(loop.crashLoop, true);

  // empty / never-started
  const none = restartStats([], now);
  assert.deepEqual(none, {
    restartCount: 0,
    lastRestartAt: null,
    restartsLastHour: 0,
    crashLoop: false,
  });
});

// The checkpoint monotonicity guard extracts the BLOCK-NUMBER field of ponder's encoded
// `latest_checkpoint`, NOT Number(whole-75-digit-checkpoint) (which overflows Number's precision so
// same-second rewinds are invisible). Offsets verified against ponder@0.16.6 checkpoint.ts:
// blockNumber occupies 0-based [26,42). MUTATION: change CHECKPOINT_BLOCK_OFFSET_0 (e.g. 26→10, the
// chainId field) or the length → the extracted value no longer equals the blockNumber and this fails.
test('extractCheckpointBlock: reads the blockNumber field of a real 0.16.6 checkpoint', () => {
  // sanity-lock the verified layout constants themselves
  assert.equal(
    CHECKPOINT_BLOCK_OFFSET_0,
    26,
    '0-based blockNumber offset (10+16)',
  );
  assert.equal(CHECKPOINT_BLOCK_LEN, 16, 'BLOCK_NUMBER_DIGITS');

  const cp = encodeCheckpoint({
    ts: 1_700_000_000n,
    chainId: 8453n,
    blockNumber: 12_345_678n,
    txIndex: 42n,
    eventType: 5,
    eventIndex: 3n,
  });
  assert.equal(cp.length, 75, 'a full checkpoint is 75 chars');
  assert.equal(
    extractCheckpointBlock(cp),
    '12345678',
    'the blockNumber field is extracted, not the timestamp/chainId prefix',
  );

  // the extraction must NOT be confused by a large chainId (16 digits) that could look like a big
  // number if the offset were wrong — block 1 with a max-ish chainId still extracts as "1".
  const cp2 = encodeCheckpoint({
    ts: 9_999_999_999n,
    chainId: 9_999_999_999_999n,
    blockNumber: 1n,
  });
  assert.equal(extractCheckpointBlock(cp2), '1');

  // block 0 (genesis / no progress) extracts as "0", never null
  assert.equal(extractCheckpointBlock(encodeCheckpoint({})), '0');

  // malformed / short / non-string → null (caller falls back to the sync-store block max)
  assert.equal(extractCheckpointBlock('too-short'), null);
  assert.equal(extractCheckpointBlock(null), null);
  assert.equal(extractCheckpointBlock(undefined), null);
});

// The checkpoint query returns a ROW COUNT alongside the max block so checkpointDecision can tell a
// chain with NO checkpoint row (rows===0 → fall back) apart from a VALID checkpoint whose blockNumber
// field is exactly 0 (rows>0 → REPORT 0). The rows-present-zero-max case is the strongest form of the
// rewind the guard exists to catch: a resume that rewound the committed checkpoint to block 0 while
// the sync store still holds a high block max. Reporting 0 lets monotonicity FAIL it against prior
// real progress; a fallback to the block max would silently PASS the rewind.
// MUTATION: gate the value on `maxBlock > 0` (the old `n > 0` behavior) instead of on row presence
// → the rows-present-zero-max case reads not-usable and the guard falls back, silently passing the
// zero-checkpoint rewind. The `usable === true, value === 0` assertion below then fails.
test('checkpointDecision: rows present with a ZERO max block is usable, value 0 (a real committed rewind, not a fallback)', () => {
  // (iii) — the bug: a VALID checkpoint row whose blockNumber is exactly 0. Rows are present, so this
  // is REPORTABLE progress of 0, NOT a reason to fall back. Monotonicity will fail 0 against any prior
  // real progress — which is the whole point of the guard.
  const zeroWithRows = checkpointDecision(1, '0');
  assert.equal(
    zeroWithRows.usable,
    true,
    'a committed checkpoint row is usable even at block 0',
  );
  assert.equal(
    zeroWithRows.value,
    0,
    'the reportable progress value is 0, not a fallback',
  );

  // rows present with a real height → usable, that height
  const real = checkpointDecision(3, '12345678');
  assert.equal(real.usable, true);
  assert.equal(real.value, 12_345_678);

  // (ii) — NO rows for the chain (max NULL → coalesce 0 in SQL, count 0 here) → NOT usable; the caller
  // falls back to the sync-store block max. This is what stops the zero-max fallback conflation.
  const noRows = checkpointDecision(0, '0');
  assert.equal(
    noRows.usable,
    false,
    'zero rows → fall back to the sync-store block max',
  );
  assert.equal(noRows.value, null);

  // a NaN / unparseable row count is treated as not-usable (fall back), never as a phantom progress
  assert.equal(checkpointDecision(Number.NaN, '5').usable, false);

  // a non-numeric max with rows present cannot yield a coherent progress value → not usable
  assert.equal(checkpointDecision(1, 'not-a-number').usable, false);
});

// AB_SCHEMA_B is interpolated verbatim into `"<schema>"._ponder_checkpoint`, so it MUST be a bare SQL
// identifier — anything else is rejected (fail loud) rather than reaching the query. Empty ⇒ null so
// the caller emits the unqualified table (legacy default-search_path behavior).
test('sanitizeSchemaIdent: accepts bare identifiers, rejects injection payloads, empty → null', () => {
  assert.equal(sanitizeSchemaIdent('soak_b'), 'soak_b');
  assert.equal(sanitizeSchemaIdent('_ponder'), '_ponder');
  assert.equal(sanitizeSchemaIdent('Euler_RT_b2'), 'Euler_RT_b2');

  // empty / unset ⇒ null (unqualified table, default search_path)
  assert.equal(sanitizeSchemaIdent(''), null);
  assert.equal(sanitizeSchemaIdent(undefined), null);
  assert.equal(sanitizeSchemaIdent(null), null);

  // anything not ^[A-Za-z_][A-Za-z0-9_]*$ throws — a quote/space/paren/dot/hyphen cannot reach SQL
  for (const bad of [
    'a"; drop table x; --',
    'a b',
    'a-b',
    'a.b',
    "a'b",
    '1abc',
    'a)b',
  ]) {
    assert.throws(
      () => sanitizeSchemaIdent(bad),
      /not a valid SQL identifier/,
      `must reject ${JSON.stringify(bad)}`,
    );
  }
});

// ── issue #27: tolerated access_list-null shared-tx class (classifySharedTx) ──────────────────────
//
// classifySharedTx classifies ONE shared tx whose full-row md5 diverged. It returns 'tolerated' ONLY
// for the exact, fully-root-caused issue #27 shape (A-side access_list NULL, B-side non-null, ALL other
// columns byte-identical, at/above the per-chain realtime floor, within the open window). ANY other
// combination is a hard 'mismatch' → FAIL, exactly as before the class existed. Each adversarial case
// below is its own test; the mutation table in the PR body records which clause each guards.

// Per-chain measured realtime-span floors (min block_number of the class). The tolerated cases sit at
// or above the chain-1 floor; sub-floor cases sit below it.
const FLOOR_1 = 25445239; // TOLERATED_CLASSES.issue27AccessListNull.perChainFloor[1]

// A shared-tx shape whose full-row md5 diverged; helper defaults are the tolerated shape (A null, B
// non-null, ex-access_list md5 EQUAL, at the floor). Override any field to build an adversarial case.
const sharedTx = (o = {}) => ({
  blockNumber: FLOOR_1,
  exAlMd5A: 'exAL', // md5 over `to_jsonb(t) - 'access_list'` — EQUAL ⇒ only access_list differs
  exAlMd5B: 'exAL',
  aAccessListNull: true, // A (RPC realtime) nulled the key
  bAccessListNull: false, // B (Portal /stream) stored the chain-true value
  ...o,
});

test('classifySharedTx: tolerated happy path — A-null, B-non-null, only access_list differs, at floor → tolerated', () => {
  assert.equal(
    TOLERATED_CLASSES.issue27AccessListNull.perChainFloor[1],
    FLOOR_1,
  );
  assert.equal(classifySharedTx(sharedTx(), 1), 'tolerated');
  // and comfortably above the floor is tolerated too
  assert.equal(
    classifySharedTx(sharedTx({ blockNumber: FLOOR_1 + 100_000 }), 1),
    'tolerated',
  );
});

test('classifySharedTx: A non-null but DIFFERENT from B (a real access_list divergence) → FAIL', () => {
  // A holds a concrete value that differs from B's — NOT the null-loss shape. Guards the A-null clause:
  // dropping `aAccessListNull === true` would wrongly tolerate a genuine access_list disagreement.
  assert.equal(
    classifySharedTx(sharedTx({ aAccessListNull: false }), 1),
    'mismatch',
  );
});

test('classifySharedTx: a SECOND column also diverges (ex-access_list md5 differs) → FAIL', () => {
  // The rows differ on some OTHER column too, so md5 over `to_jsonb(t) - 'access_list'` is unequal.
  // Guards the ex-AL-md5-equality clause: dropping it would let a two-column divergence hide behind an
  // access_list gap.
  assert.equal(
    classifySharedTx(sharedTx({ exAlMd5B: 'DIFFERENT' }), 1),
    'mismatch',
  );
});

test('classifySharedTx: INVERTED asymmetry — B null, A non-null → FAIL', () => {
  // The loss is on side B, not the RPC-realtime A leg — not this class. Guards the B-non-null clause.
  assert.equal(
    classifySharedTx(
      sharedTx({ aAccessListNull: false, bAccessListNull: true }),
      1,
    ),
    'mismatch',
  );
});

test('classifySharedTx: block BELOW the per-chain floor → FAIL', () => {
  // Below leg A's measured realtime span — outside the class's evidence. Guards the floor clause.
  assert.equal(
    classifySharedTx(sharedTx({ blockNumber: FLOOR_1 - 1 }), 1),
    'mismatch',
  );
});

test('classifySharedTx: a chain with NO floor entry → FAIL (missing floor is hard-fail, never default-tolerate)', () => {
  // An unknown chain has no measured floor, so it CANNOT be tolerated. Guards the missing-floor
  // hard-fail: a mutation that defaulted a missing floor to −Infinity (tolerate) would flip this.
  assert.equal(classifySharedTx(sharedTx(), 999_999), 'mismatch');
});

test('classifySharedTx: toBlock set + block ABOVE it → FAIL (closed window)', () => {
  // Once the fix deploys and the window closes, toBlock is set; a row past it is no longer tolerated.
  const closed = {
    issue27AccessListNull: {
      ...TOLERATED_CLASSES.issue27AccessListNull,
      toBlock: FLOOR_1 + 1000,
    },
  };
  assert.equal(
    classifySharedTx(sharedTx({ blockNumber: FLOOR_1 + 999 }), 1, closed),
    'tolerated',
    'a row inside the closed window is still tolerated',
  );
  assert.equal(
    classifySharedTx(sharedTx({ blockNumber: FLOOR_1 + 1001 }), 1, closed),
    'mismatch',
    'a row past toBlock is NOT tolerated',
  );
  // exactly AT toBlock is still inside the window (inclusive)
  assert.equal(
    classifySharedTx(sharedTx({ blockNumber: FLOOR_1 + 1000 }), 1, closed),
    'tolerated',
  );
});

test('classifySharedTx: TOLERATED_CLASSES entry deleted → everything hard-fails again (full strictness restored)', () => {
  // Deleting the entry (the removal instruction) must restore full strictness with no other code
  // change — the otherwise-tolerated happy path becomes a hard mismatch. Both an empty classes map and
  // an explicitly-deleted entry key model the removal.
  assert.equal(classifySharedTx(sharedTx(), 1, {}), 'mismatch');
  assert.equal(
    classifySharedTx(sharedTx(), 1, { issue27AccessListNull: undefined }),
    'mismatch',
  );
});

test('classifyTxDiff: threads the tolerated tally through without failing the verdict', () => {
  // A clean diff whose only shared divergence is the tolerated class → NOT a fail. classifyTxDiff's
  // `fail` is driven only by unexpectedB / unreferencedA / (hard) sharedMismatch — tolerated is carried
  // for reporting only, so the run verdict stays PASS-compatible.
  const tol = { count: 3, perChain: { 1: 3 } };
  const r = classifyTxDiff([], [], new Set(), 0, tol);
  assert.equal(r.fail, false);
  assert.deepEqual(r.toleratedIssue27, tol);

  // a HARD sharedMismatch alongside tolerated rows still FAILs
  const bad = classifyTxDiff([], [], new Set(), 1, tol);
  assert.equal(bad.fail, true);
  assert.equal(bad.sharedMismatch, 1);
  assert.deepEqual(bad.toleratedIssue27, tol);

  // default when no tolerated rows: an empty {count:0, perChain:{}}
  const none = classifyTxDiff([], [], new Set());
  assert.deepEqual(none.toleratedIssue27, { count: 0, perChain: {} });
});

test('aggregateToleratedIssue27: sums per-chain tolerated tallies across chain results', () => {
  const results = [
    {
      chain: 1,
      classes: {
        transactions: { toleratedIssue27: { count: 2, perChain: { 1: 2 } } },
      },
    },
    {
      chain: 8453,
      classes: {
        transactions: { toleratedIssue27: { count: 5, perChain: { 8453: 5 } } },
      },
    },
    // a chain with no tolerated rows contributes nothing
    {
      chain: 42161,
      classes: {
        transactions: { toleratedIssue27: { count: 0, perChain: {} } },
      },
    },
    // a PENDING/ERROR chain with no transactions class is skipped, never throws
    { chain: 10, classes: { note: 'no finalized overlap yet' } },
  ];
  assert.deepEqual(aggregateToleratedIssue27(results), {
    count: 7,
    perChain: { 1: 2, 8453: 5 },
  });
});

test('formatToleratedIssue27Line: loud REMOVE line when count>0, empty string otherwise', () => {
  assert.equal(formatToleratedIssue27Line({ count: 0, perChain: {} }), '');
  assert.equal(formatToleratedIssue27Line(null), '');
  assert.equal(formatToleratedIssue27Line(undefined), '');

  const line = formatToleratedIssue27Line({
    count: 7,
    perChain: { 1: 2, 8453: 5 },
  });
  assert.match(line, /^TOLERATED \(known issue #27 — REMOVE/);
  assert.match(line, /7 access_list-null rows/);
  assert.match(line, /1:2/);
  assert.match(line, /8453:5/);
});

// ── issue #32: knownBadRows exact-hash pin (classifySharedTx → 'knownBadRow') ──────────────────────
//
// A SEPARATE, even narrower tolerance than issue #27: an exact tx hash pinned in `knownBadRows` is
// classified 'knownBadRow' IFF its hash is listed for THIS chain AND only access_list differs
// (exAlMd5A === exAlMd5B). UNLIKE issue #27 the row has BOTH sides NON-NULL (A='[]', B chain-true), so
// the predicate does NOT require aAccessListNull. If a second column diverges (ex-AL md5s differ) the
// pin stops protecting the row → 'mismatch' → hard FAIL. Each adversarial case is its own test; the
// mutation table in the PR body records which clause each guards.

// The exact issue #32 row: chain 42161, block 469300066, the fabricated-empty access_list tx. Both
// sides non-null (A='[]', B=63-entry list); every other column byte-identical (ex-AL md5s equal).
const ISSUE_32_HASH =
  '0x0af5f9831bff6430dca4197962554f7f4779da2bb4f533844b4224953e7ab5fe';

// A shared-tx shape matching the issue #32 pin; helper defaults are the knownBadRow shape (exact hash,
// chain 42161, ex-AL md5s EQUAL, both sides NON-NULL). Override any field to build an adversarial case.
const knownBadTx = (o = {}) => ({
  hash: ISSUE_32_HASH,
  blockNumber: 469300066,
  exAlMd5A: 'exAL', // md5 over `to_jsonb(t) - 'access_list'` — EQUAL ⇒ only access_list differs
  exAlMd5B: 'exAL',
  aAccessListNull: false, // A stored '[]' — a CONCRETE value, not NULL (unlike issue #27)
  bAccessListNull: false, // B stored the chain-true 63-entry list
  ...o,
});

test('classifySharedTx: the exact issue #32 row (hash + chain 42161, only access_list differs, both non-null) → knownBadRow', () => {
  // the pin is present in the shipped list with the issue #32 metadata
  const entry = knownBadRows.find((r) => r.hash === ISSUE_32_HASH);
  assert.ok(entry, 'the issue #32 hash is pinned in knownBadRows');
  assert.equal(entry.chain, 42161);
  assert.equal(
    entry.issue,
    'https://github.com/subsquid-labs/portal-ponder/issues/32',
  );

  assert.equal(classifySharedTx(knownBadTx(), 42161), 'knownBadRow');
});

// ── issue #32, Round-2 FINDING 1 (High): the pin must enforce the EVIDENCED both-non-null shape ──────
//
// The evidenced divergence is A='[]' (concrete) vs B=63-entry list (concrete) — BOTH sides non-null.
// Without the `aAccessListNull === false && bAccessListNull === false` guard, the pin would tolerate
// ANY access_list divergence on the exact pinned hash+chain, including an A-NULL / B-non-null drift
// (issue #27's leaked-key shape masquerading as the pin) or a B-side rot to NULL — precisely the
// silent-mask the pin must never become. MUTATION: drop the both-non-null clause from the pin predicate
// in classifySharedTx → the A-NULL pinned-hash row below flips 'mismatch' → 'knownBadRow' and this fails.

test('classifySharedTx: FINDING 1 — the pinned hash with A access_list NULL is NOT a knownBadRow → FAIL (both sides must be concrete)', () => {
  // The exact issue #32 hash+chain, ex-AL md5s equal — but side A is NULL, not the evidenced '[]'. This
  // is NOT the measured '[]'-vs-concrete-list shape; it is an access_list divergence the pin must refuse.
  // (It is also not the issue #27 tolerated shape here: block 469300066 is below chain 42161's issue #27
  // floor 479635494, so there is no fall-through tolerance — it is a hard mismatch.)
  assert.equal(
    classifySharedTx(knownBadTx({ aAccessListNull: true }), 42161),
    'mismatch',
    'A-NULL on the pinned hash is not the evidenced both-non-null shape',
  );

  // and the inverted rot — B side goes NULL — is equally refused on the pinned hash.
  assert.equal(
    classifySharedTx(knownBadTx({ bAccessListNull: true }), 42161),
    'mismatch',
    'B-NULL on the pinned hash is not the evidenced both-non-null shape',
  );

  // both NULL (a wholly different divergence) is also not the pin's shape.
  assert.equal(
    classifySharedTx(
      knownBadTx({ aAccessListNull: true, bAccessListNull: true }),
      42161,
    ),
    'mismatch',
  );
});

test('classifySharedTx: the issue #32 hash on the WRONG chain → FAIL (chain must match)', () => {
  // Same pinned hash, but the shared tx is on a different chain than the entry pins. Guards the chain
  // clause: dropping `r.chain === chain` would tolerate the hash on ANY chain.
  assert.equal(classifySharedTx(knownBadTx(), 1), 'mismatch');
  assert.equal(classifySharedTx(knownBadTx(), 8453), 'mismatch');
});

test('classifySharedTx: the issue #32 hash but a SECOND column rotted (ex-access_list md5s differ) → FAIL', () => {
  // The pinned row diverged on some OTHER column too, so md5 over `to_jsonb(t) - 'access_list'` is now
  // unequal. Guards the exAl-equality clause: dropping it would keep tolerating a row that rots further
  // than the single access_list divergence it was pinned for.
  assert.equal(
    classifySharedTx(knownBadTx({ exAlMd5B: 'ROTTED' }), 42161),
    'mismatch',
  );
});

test('classifySharedTx: a hash NOT in knownBadRows with the same shape → FAIL (only pinned hashes get the pin)', () => {
  // An UNPINNED hash on chain 42161 with the exact knownBadRow shape (both non-null, only access_list
  // differs) is NOT protected by the issue #32 pin. It also fails the issue #27 path (aAccessListNull is
  // false here), so it is a hard mismatch — the pin protects ONLY the exact listed hashes.
  assert.equal(
    classifySharedTx(knownBadTx({ hash: '0xdeadbeef' }), 42161),
    'mismatch',
  );
});

test('classifySharedTx: knownBadRows entry deleted → the pinned row hard-fails again (removal restores strictness)', () => {
  // Deleting the entry (the removal instruction) restores full strictness with no other code change:
  // the otherwise-knownBadRow shape becomes a hard mismatch. Both an empty list and a list missing the
  // hash model the removal. (The row is both-non-null so it does NOT fall through to the issue #27 path.)
  assert.equal(
    classifySharedTx(knownBadTx(), 42161, TOLERATED_CLASSES, []),
    'mismatch',
  );
  assert.equal(
    classifySharedTx(knownBadTx(), 42161, TOLERATED_CLASSES, [
      { hash: '0xother', chain: 42161 },
    ]),
    'mismatch',
  );
});

test('classifySharedTx: a pinned knownBadRow does NOT disturb the issue #27 tolerated path', () => {
  // The knownBadRow check is additive: an issue #27-shaped row (A-null, B-non-null, at floor) whose hash
  // is NOT pinned still classifies 'tolerated' via the issue #27 conjunction, unchanged by this delta.
  const issue27Shaped = {
    hash: '0xnotpinned',
    blockNumber: TOLERATED_CLASSES.issue27AccessListNull.perChainFloor[1],
    exAlMd5A: 'exAL',
    exAlMd5B: 'exAL',
    aAccessListNull: true,
    bAccessListNull: false,
  };
  assert.equal(classifySharedTx(issue27Shaped, 1), 'tolerated');
});

test('classifyTxDiff: threads the knownBadRows tally through, SEPARATELY from toleratedIssue27, without failing', () => {
  // A clean diff whose only shared divergence is a knownBadRow → NOT a fail; the tally is carried for
  // reporting, kept distinct from toleratedIssue27.
  const kbr = { count: 1, perChain: { 42161: 1 } };
  const tol = { count: 3, perChain: { 1: 3 } };
  const r = classifyTxDiff([], [], new Set(), 0, tol, kbr);
  assert.equal(r.fail, false);
  assert.deepEqual(r.knownBadRows, kbr);
  assert.deepEqual(r.toleratedIssue27, tol, 'the two counters stay separate');

  // a HARD sharedMismatch alongside knownBadRows still FAILs
  const bad = classifyTxDiff([], [], new Set(), 1, tol, kbr);
  assert.equal(bad.fail, true);
  assert.deepEqual(bad.knownBadRows, kbr);

  // default when no knownBadRows tally is passed: an empty {count:0, perChain:{}, perHash:{}}
  const none = classifyTxDiff([], [], new Set());
  assert.deepEqual(none.knownBadRows, { count: 0, perChain: {}, perHash: {} });
});

test('aggregateKnownBadRows: sums per-chain + per-hash knownBadRows tallies across chain results', () => {
  const results = [
    {
      chain: 42161,
      classes: {
        transactions: {
          knownBadRows: {
            count: 1,
            perChain: { 42161: 1 },
            perHash: { [ISSUE_32_HASH]: 1 },
          },
        },
      },
    },
    // a chain with no knownBadRows contributes nothing
    {
      chain: 1,
      classes: {
        transactions: { knownBadRows: { count: 0, perChain: {}, perHash: {} } },
      },
    },
    // a PENDING/ERROR chain with no transactions class is skipped, never throws
    { chain: 10, classes: { note: 'no finalized overlap yet' } },
  ];
  // pass an explicit single pin on chain 42161 that DID match → nothing unmatched
  const pins = [{ hash: ISSUE_32_HASH, chain: 42161, issue: 'x' }];
  assert.deepEqual(aggregateKnownBadRows(results, pins), {
    count: 1,
    perChain: { 42161: 1 },
    perHash: { [ISSUE_32_HASH]: 1 },
    unmatched: [],
  });
});

// ── issue #32 (spec §4): an unmatched pin is NOT a failure but stays VISIBLE ───────────────────────
//
// A configured pin that fires zero times this run — its row was repaired, or its chain wasn't diffed —
// must never fail the verdict, must never crash, and must surface in the output so a stale pin cannot
// rot silently. aggregateKnownBadRows reports it under `unmatched`; formatKnownBadRowsLine prints it
// EVEN WHEN the matched count is 0.
test('aggregateKnownBadRows: a configured pin that matched nothing is reported as UNMATCHED (not a failure, still visible)', () => {
  // chain 42161's row was repaired this run → zero knownBadRow matches anywhere.
  const results = [
    {
      chain: 42161,
      classes: {
        transactions: { knownBadRows: { count: 0, perChain: {} } },
      },
    },
    {
      chain: 1,
      classes: { transactions: { knownBadRows: { count: 0, perChain: {} } } },
    },
  ];
  const pins = [{ hash: ISSUE_32_HASH, chain: 42161, issue: 'x' }];
  const agg = aggregateKnownBadRows(results, pins);
  assert.equal(agg.count, 0);
  assert.deepEqual(agg.perChain, {});
  assert.deepEqual(
    agg.unmatched,
    pins,
    'the repaired pin is surfaced as unmatched',
  );
});

test('aggregateKnownBadRows: a pin that DID match is NOT flagged unmatched (per-hash fire count)', () => {
  // The pin's OWN hash fired at least once (perHash carries it), so it is matched — never mis-reported
  // as unmatched. Guards the per-hash matched-detection: a mutation reverting to a chain-keyed tally
  // would still pass THIS single-pin case, which is why FINDING 2's two-pins-one-chain test below is
  // the real tripwire.
  const results = [
    {
      chain: 42161,
      classes: {
        transactions: {
          knownBadRows: {
            count: 1,
            perChain: { 42161: 1 },
            perHash: { [ISSUE_32_HASH]: 1 },
          },
        },
      },
    },
  ];
  const pins = [{ hash: ISSUE_32_HASH, chain: 42161, issue: 'x' }];
  assert.deepEqual(aggregateKnownBadRows(results, pins).unmatched, []);
});

// ── issue #32, Round-2 FINDING 2 (Medium): unmatched detection is per-PIN (hash, chain), not per-CHAIN ─
//
// Two pins on the SAME chain: pin P1's hash fires, pin P2's hash fires ZERO times (its row was repaired).
// A chain-keyed unmatched detector sees "chain 42161 fired" and marks BOTH matched — hiding the stale
// P2. Threading perHash makes the fate exact: only P2 is unmatched. MUTATION: revert aggregateKnownBadRows
// to key matched-detection by chain (perPin/perChain) → P2 is wrongly treated as matched and this fails.

test('aggregateKnownBadRows: FINDING 2 — two pins on one chain, only the non-firing pin is UNMATCHED (per (hash, chain), not per chain)', () => {
  const P1_HASH = ISSUE_32_HASH; // fires this run
  const P2_HASH =
    '0x1111111111111111111111111111111111111111111111111111111111111111'; // repaired → 0 fires
  const results = [
    {
      chain: 42161,
      classes: {
        transactions: {
          knownBadRows: {
            count: 1,
            perChain: { 42161: 1 },
            perHash: { [P1_HASH]: 1 }, // ONLY P1's hash fired
          },
        },
      },
    },
  ];
  const pins = [
    { hash: P1_HASH, chain: 42161, issue: 'x' },
    { hash: P2_HASH, chain: 42161, issue: 'y' },
  ];
  const agg = aggregateKnownBadRows(results, pins);

  // per-hash fire counts are aggregated and exposed for exact pin fate.
  assert.deepEqual(agg.perHash, { [P1_HASH]: 1 });

  // ONLY P2 (the pin whose OWN hash fired zero times) is unmatched — P1 fired so it is not. A chain-keyed
  // detector would return [] here (both "matched" via the chain), hiding the stale P2.
  assert.deepEqual(
    agg.unmatched,
    [{ hash: P2_HASH, chain: 42161, issue: 'y' }],
    'the non-firing pin is surfaced even though a sibling pin on the same chain fired',
  );
});

test('aggregateKnownBadRows: string/number hash keys — a pin fired via perHash is matched regardless of result key type', () => {
  // perHash keys are the raw hash strings from the tx stream; pin.hash is the same string literal. This
  // pins the key type contract so a matched pin is never mis-flagged on a coercion mismatch.
  const results = [
    {
      chain: 42161,
      classes: {
        transactions: {
          knownBadRows: {
            count: 2,
            perChain: { 42161: 2 },
            perHash: { [ISSUE_32_HASH]: 2 },
          },
        },
      },
    },
  ];
  const pins = [{ hash: ISSUE_32_HASH, chain: 42161, issue: 'x' }];
  const agg = aggregateKnownBadRows(results, pins);
  assert.deepEqual(agg.unmatched, []);
  assert.equal(agg.perHash[ISSUE_32_HASH], 2);
});

test('formatKnownBadRowsLine: an UNMATCHED pin prints (visible) even when the matched count is 0', () => {
  // count 0 but one unmatched pin → the line must still be non-empty and name the stale pin so it is
  // never silent. (Contrast the count>0 case and the truly-empty case, both asserted above.)
  const line = formatKnownBadRowsLine({
    count: 0,
    perChain: {},
    unmatched: [{ hash: ISSUE_32_HASH, chain: 42161, issue: 'x' }],
  });
  assert.match(line, /^KNOWN-BAD ROWS \(issue #32/);
  assert.match(line, /1 UNMATCHED pin\(s\) fired 0 times/);
  assert.match(line, new RegExp(`42161:${ISSUE_32_HASH}`));

  // no matched rows AND no unmatched pins ⇒ still empty string
  assert.equal(
    formatKnownBadRowsLine({ count: 0, perChain: {}, unmatched: [] }),
    '',
  );
});

test('formatKnownBadRowsLine: loud REMOVE line naming issue #32 when count>0, empty string otherwise', () => {
  assert.equal(formatKnownBadRowsLine({ count: 0, perChain: {} }), '');
  assert.equal(formatKnownBadRowsLine(null), '');
  assert.equal(formatKnownBadRowsLine(undefined), '');

  const line = formatKnownBadRowsLine({ count: 1, perChain: { 42161: 1 } });
  assert.match(line, /^KNOWN-BAD ROWS \(issue #32 — REMOVE/);
  assert.match(line, /1 pinned access_list-only rows/);
  assert.match(line, /42161:1/);
});

// ── Round-2 FINDING 3 (Low): a tripwire pinning the tx SQL SELECT column ORDER to the destructuring ──
//
// classifySharedTx reads positional row[] indices (hash=0, block_number=2, ex-AL md5=3, null flag=4).
// If buildTxSql's SELECT list is reordered without reordering the destructuring — or vice versa — the
// classifier silently reads the wrong column (e.g. the null flag as an md5), corrupting the tolerance
// classification with NO error. This tripwire asserts the SELECT list, in order, matches TX_SELECT_COLUMNS
// AND that TX_COL (the indices diffTx destructures from) equals each column's position — without psql.
// MUTATION: permute buildTxSql's projection (swap block_number and the ex-AL md5), or change any TX_COL
// index, → the ordered-SELECT assertion or the position-contract assertion below fails.

test('FINDING 3 — buildTxSql SELECT column order matches the classifySharedTx destructuring contract', () => {
  const sql = buildTxSql(42161, 100, 200);

  // 1) the projection is exactly TX_SELECT_COLUMNS, in order, right after `select ` and before ` from`.
  const m = sql.match(/^select (.+?) from ponder_sync\.transactions t /);
  assert.ok(m, 'buildTxSql projects from ponder_sync.transactions');
  const projected = m[1].split(', ');
  assert.deepEqual(
    projected,
    TX_SELECT_COLUMNS.map((c) => c.sql),
    'the SELECT list, in order, is exactly TX_SELECT_COLUMNS',
  );

  // 2) the positional contract: each destructured index (TX_COL) equals the field's position in the
  // ordered SELECT. If either the SQL order or an index moves without the other, this diverges.
  const posByName = Object.fromEntries(
    TX_SELECT_COLUMNS.map((c, i) => [c.name, i]),
  );
  assert.equal(TX_COL.hash, posByName.hash, 'hash is SELECT index 0');
  assert.equal(
    TX_COL.fullRowMd5,
    posByName.fullRowMd5,
    'full-row md5 is index 1',
  );
  assert.equal(
    TX_COL.blockNumber,
    posByName.blockNumber,
    'block_number is index 2',
  );
  assert.equal(
    TX_COL.exAccessListMd5,
    posByName.exAccessListMd5,
    'ex-access_list md5 is index 3',
  );
  assert.equal(
    TX_COL.accessListNull,
    posByName.accessListNull,
    'access_list-null flag is index 4',
  );

  // 3) lock the exact indices the review contract names, so a silent renumber of BOTH the array and
  // TX_COL together (keeping them consistent but wrong) is still caught against the spec.
  assert.deepEqual(TX_COL, {
    hash: 0,
    fullRowMd5: 1,
    blockNumber: 2,
    exAccessListMd5: 3,
    accessListNull: 4,
  });

  // 4) the specific SQL fragments the classifier depends on are the ones at their contract positions.
  assert.equal(TX_SELECT_COLUMNS[TX_COL.hash].sql, '"hash"');
  assert.equal(TX_SELECT_COLUMNS[TX_COL.blockNumber].sql, 'block_number');
  assert.equal(
    TX_SELECT_COLUMNS[TX_COL.exAccessListMd5].sql,
    "md5((to_jsonb(t)-'access_list')::text)",
  );
  assert.equal(
    TX_SELECT_COLUMNS[TX_COL.accessListNull].sql,
    '(access_list is null)',
  );

  // and the query is still ordered by hash (the merge-join precondition) and bounds by the args.
  assert.match(sql, /order by "hash"$/);
  assert.match(sql, /chain_id=42161 and block_number between 100 and 200/);
});

// ── issue #36: tolerated onlyB row-loss class for the LOGS and BLOCKS tables ──────────────────────
//
// Leg A (RPC realtime) silently lost on-chain log rows + block rows leg B (Portal stream, chain-true)
// holds — onlyB rows inside the finalized overlap. classifyOnlyBRow tolerates ONE such row ONLY at/above
// the per-chain realtime-era floor, within the open window; classifyOnlyBDiff tolerates a whole table
// diff ONLY when EVERY onlyB row is tolerated AND there is no onlyA and no shared mismatch;
// classifyBucketMismatches only excuses a bucket md5 mismatch that is EXACTLY the tolerated onlyB rows.
// Each adversarial case below is its own test; the PR body records which clause each mutation guards.

// The shipped realtime-era floors (issue #36). Tolerated cases sit at/above the chain's floor; sub-floor
// below is a HARD FAIL. Sourced from the config so a floor change here breaks the binding tests.
const ISSUE_36_FLOOR_1 = 25445239; // TOLERATED_ONLYB_CLASSES.issue36OnlyBRowLoss.perChainFloor[1]
// Base chain 8453 — its measured realtime-era cutover; an INDEPENDENT literal that coincides with issue
// #27's 8453 floor today by design (the two classes are deliberately kept as separate literals).
const ISSUE_36_FLOOR_8453 =
  TOLERATED_ONLYB_CLASSES.issue36OnlyBRowLoss.perChainFloor[8453];

test('classifyOnlyBRow: config ships chain 1 AND Base 8453 at their measured realtime-era floors', () => {
  assert.equal(
    TOLERATED_ONLYB_CLASSES.issue36OnlyBRowLoss.perChainFloor[1],
    ISSUE_36_FLOOR_1,
  );
  // 8453 was confirmed lossy (leg-A silent gap, leg-B chain-true) by direct store inspection 2026-07-22
  // → configured at its measured realtime-era cutover, the value issue #27 also measured for 8453.
  assert.equal(ISSUE_36_FLOOR_8453, 48092254);
  // ship chain 1 + 8453 ONLY — 42161 is currently clean on this class and must NOT be pre-tolerated.
  assert.deepEqual(
    Object.keys(TOLERATED_ONLYB_CLASSES.issue36OnlyBRowLoss.perChainFloor),
    ['1', '8453'],
  );
  // open-ended window by design (the class grows while leg A stays lossy).
  assert.equal(TOLERATED_ONLYB_CLASSES.issue36OnlyBRowLoss.toBlock, null);
});

test('classifyOnlyBRow: an onlyB row at/above the chain-1 floor → tolerated', () => {
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_1 }, 1),
    'tolerated',
  );
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_1 + 100_000 }, 1),
    'tolerated',
  );
});

test('classifyOnlyBRow: an onlyB row BELOW the chain-1 floor → mismatch (HARD FAIL)', () => {
  // Below the floor leg A's store came from the complete-by-construction historical backfill, so an
  // A-missing row there is a real, hard gap. MUTATION (drop the `block >= floor` clause) → this fails.
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_1 - 1 }, 1),
    'mismatch',
  );
});

test('classifyOnlyBRow: an onlyB row at/above the Base 8453 floor → tolerated', () => {
  // 8453 is now a configured issue-#36 chain (leg-A silent gap DB-confirmed 2026-07-22). At/above its
  // measured realtime-era floor an onlyB row is the tolerated shape. MUTATION (revert the 8453 config
  // line) → this fails (no floor for 8453 → 'mismatch').
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_8453 }, 8453),
    'tolerated',
  );
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_8453 + 100_000 }, 8453),
    'tolerated',
  );
});

test('classifyOnlyBRow: an onlyB row BELOW the Base 8453 floor → mismatch (HARD FAIL — no below-cutover gap is ever masked)', () => {
  // Below the 8453 cutover leg A's store came from the complete-by-construction historical backfill, so
  // an A-missing row there is a real, hard gap. MUTATION (drop the `block >= floor` clause) → this fails.
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_8453 - 1 }, 8453),
    'mismatch',
  );
});

test('classifyOnlyBRow: a chain with NO configured floor → mismatch (HARD FAIL, the #30 missing-floor semantic)', () => {
  // 42161 is NOT in the shipped config, nor is a synthetic unknown chain → an onlyB row there is a hard
  // fail, never a default-tolerate. MUTATION (default-tolerate an unknown chain) → this fails.
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_1 }, 42161),
    'mismatch',
  );
  assert.equal(classifyOnlyBRow({ blockNumber: 999_999 }, 999_999), 'mismatch');
});

test('classifyOnlyBRow: a deleted/absent config entry → mismatch for all (full strictness restored)', () => {
  // Removing the entry (the removal step when issue #36 is resolved) must restore strictness with no
  // other change. MUTATION (ignore a missing entry) → this fails.
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_1 }, 1, {}),
    'mismatch',
  );
});

test('classifyOnlyBRow: a CLOSED window (toBlock set) rejects rows past it', () => {
  const closed = {
    issue36OnlyBRowLoss: {
      perChainFloor: { 1: ISSUE_36_FLOOR_1 },
      toBlock: ISSUE_36_FLOOR_1 + 10,
    },
  };
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_1 + 10 }, 1, closed),
    'tolerated',
  );
  // one past the closed window is NOT tolerated. MUTATION (drop the withinWindow clause) → this fails.
  assert.equal(
    classifyOnlyBRow({ blockNumber: ISSUE_36_FLOOR_1 + 11 }, 1, closed),
    'mismatch',
  );
});

// classifyOnlyBDiff: the whole-table verdict. A logs/blocks diff is tolerated ONLY when every onlyB row
// is tolerated AND there is no onlyA and no shared mismatch.

const onlyBRow = (blockNumber, logIndex) => ({ blockNumber, logIndex });

test('classifyOnlyBDiff: pure onlyB loss, all rows at/above floor → PASS-compatible (fail=false), counted', () => {
  const diff = { onlyA: 0, onlyB: 3, mismatch: 0 };
  const rows = [
    onlyBRow(ISSUE_36_FLOOR_1, 5),
    onlyBRow(ISSUE_36_FLOOR_1 + 1, 0),
    onlyBRow(ISSUE_36_FLOOR_1 + 700, 9),
  ];
  const r = classifyOnlyBDiff(diff, rows, 1);
  assert.equal(r.fail, false);
  assert.equal(r.toleratedOnlyB.count, 3);
  assert.deepEqual(r.toleratedOnlyB.perChain, { 1: 3 });
  assert.equal(r.hardOnlyB, 0);
});

test('classifyOnlyBDiff: any onlyA row → HARD FAIL (the class NEVER tolerates onlyA — refuses the inverted asymmetry)', () => {
  // leg B has a row leg A lacks is the tolerated shape; leg A has a row leg B lacks (onlyA) is the
  // OPPOSITE and a real divergence. MUTATION (drop the `onlyA > 0` clause) → this fails.
  const diff = { onlyA: 1, onlyB: 1, mismatch: 0 };
  const r = classifyOnlyBDiff(diff, [onlyBRow(ISSUE_36_FLOOR_1, 0)], 1);
  assert.equal(r.fail, true);
});

test('classifyOnlyBDiff: any shared mismatch → HARD FAIL (a shared-row field divergence is a different class)', () => {
  // MUTATION (drop the `mismatch > 0` clause) → this fails.
  const diff = { onlyA: 0, onlyB: 1, mismatch: 1 };
  const r = classifyOnlyBDiff(diff, [onlyBRow(ISSUE_36_FLOOR_1, 0)], 1);
  assert.equal(r.fail, true);
});

test('classifyOnlyBDiff: 88 tolerated + 1 below-floor onlyB → HARD FAIL (the below-floor row is not masked by its siblings)', () => {
  const rows = [onlyBRow(ISSUE_36_FLOOR_1 - 1, 0)]; // one below floor
  for (let i = 0; i < 88; i++) {
    rows.push(onlyBRow(ISSUE_36_FLOOR_1 + i, i));
  }
  const diff = { onlyA: 0, onlyB: rows.length, mismatch: 0 };
  const r = classifyOnlyBDiff(diff, rows, 1);
  assert.equal(r.fail, true, 'a single below-floor row FAILs the whole table');
  assert.equal(r.hardOnlyB, 1);
  assert.equal(r.toleratedOnlyB.count, 88, 'the 88 siblings are still counted');
});

test('classifyOnlyBDiff: onlyB rows on an unconfigured chain → HARD FAIL (missing floor)', () => {
  // 42161 is NOT in the shipped config → every onlyB row on it is a hard fail (missing-floor semantic),
  // no default-tolerate. (8453 was moved out of this role — it is now a configured issue-#36 chain.)
  const diff = { onlyA: 0, onlyB: 2, mismatch: 0 };
  const rows = [
    onlyBRow(ISSUE_36_FLOOR_1, 0),
    onlyBRow(ISSUE_36_FLOOR_1 + 1, 1),
  ];
  const r = classifyOnlyBDiff(diff, rows, 42161);
  assert.equal(r.fail, true);
  assert.equal(r.hardOnlyB, 2);
  assert.equal(r.toleratedOnlyB.count, 0);
});

test('classifyOnlyBDiff: no divergence at all → PASS (fail=false, zero tolerated)', () => {
  const r = classifyOnlyBDiff({ onlyA: 0, onlyB: 0, mismatch: 0 }, [], 1);
  assert.equal(r.fail, false);
  assert.equal(r.toleratedOnlyB.count, 0);
});

// ── classifyOnlyBTx — the issue #36 TRANSACTION facet (wholly-A-absent-block predicate) ──────────────
//
// The 12:22Z hourly cross-validation proved onlyB txs are the SAME leg-A loss the logs/blocks class
// tolerates, one table deeper: 30 B-only chain-1 txs, ALL in blocks WHOLLY ABSENT from leg A. The tx
// predicate is STRICTER than classifyOnlyBRow by ONE extra conjunct — the tx's block must be in the
// wholly-A-absent set (the blocks-onlyB set) — so a tx-level-only loss (leg A HAS the block, misses just
// the tx) is a genuinely NEW divergence class and stays a HARD FAIL.

test('classifyOnlyBTx: a tolerated case — onlyB tx whose block is WHOLLY absent from A, at/above floor → tolerated', () => {
  const absent = new Set([ISSUE_36_FLOOR_1, ISSUE_36_FLOOR_1 + 5]);
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_1 }, 1, absent),
    'tolerated',
  );
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_1 + 5 }, 1, absent),
    'tolerated',
  );
});

test('classifyOnlyBTx: the STRICTNESS case — onlyB tx whose block EXISTS in A (not in the absent set) → mismatch (HARD FAIL)', () => {
  // The extra conjunct over classifyOnlyBRow: the block is at/above the floor AND would be tolerated as a
  // bare row, but it is NOT wholly A-absent (absent set does not contain it) → a tx-level-only loss → the
  // NEW divergence class that MUST keep failing loudly. MUTATION (loosen the predicate to tolerate ALL
  // onlyB txs / drop the absent-set gate) → this assertion fails.
  const absent = new Set([ISSUE_36_FLOOR_1 + 999]); // some OTHER block is A-absent, not this one
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_1 }, 1, absent),
    'mismatch',
  );
  // and with an empty absent set (no block wholly absent) NOTHING is tolerated, even at/above floor.
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_1 + 100 }, 1, new Set()),
    'mismatch',
  );
});

test('classifyOnlyBTx: a wholly-A-absent block BELOW the floor → mismatch (the floor conjunct still bites)', () => {
  // Below the floor leg A came from the complete-by-construction historical backfill, so even a
  // wholly-absent block there is a real gap, never this class. Reuses classifyOnlyBRow's floor logic.
  const absent = new Set([ISSUE_36_FLOOR_1 - 1]);
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_1 - 1 }, 1, absent),
    'mismatch',
  );
});

test('classifyOnlyBTx: Base 8453 — a wholly-A-absent block at/above the floor → tolerated', () => {
  // 8453 is now a configured issue-#36 chain. A tx whose block is WHOLLY absent from leg A, at/above the
  // 8453 realtime-era floor, is the tolerated shape. MUTATION (revert the 8453 config line) → this fails
  // (no floor for 8453 → 'mismatch').
  const absent = new Set([ISSUE_36_FLOOR_8453, ISSUE_36_FLOOR_8453 + 5]);
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_8453 }, 8453, absent),
    'tolerated',
  );
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_8453 + 5 }, 8453, absent),
    'tolerated',
  );
});

test('classifyOnlyBTx: Base 8453 — a wholly-A-absent block BELOW the floor → mismatch (the floor conjunct still bites)', () => {
  // Below the 8453 cutover leg A came from the historical backfill, so even a wholly-absent block there
  // is a real gap, never this class. MUTATION (drop the `block >= floor` clause) → this fails.
  const absent = new Set([ISSUE_36_FLOOR_8453 - 1]);
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_8453 - 1 }, 8453, absent),
    'mismatch',
  );
});

test('classifyOnlyBTx: an unconfigured chain (no floor) → mismatch even for a wholly-absent block (the #30 missing-floor semantic)', () => {
  // 42161 is NOT in the shipped config → not default-tolerated even for a wholly-A-absent block.
  const absent = new Set([ISSUE_36_FLOOR_1]);
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_1 }, 42161, absent),
    'mismatch',
  );
});

test('classifyOnlyBTx: a deleted/absent config entry → mismatch for all (full strictness restored)', () => {
  const absent = new Set([ISSUE_36_FLOOR_1]);
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_1 }, 1, absent, {}),
    'mismatch',
  );
});

test('classifyOnlyBTx: accepts an array absentBlocks too (coerced to a Set)', () => {
  assert.equal(
    classifyOnlyBTx({ blockNumber: ISSUE_36_FLOOR_1 }, 1, [ISSUE_36_FLOOR_1]),
    'tolerated',
  );
});

// ── classifyOnlyBTxDiff — folding the tolerance into a classifyTxDiff result ─────────────────────────

test('classifyOnlyBTxDiff: a tolerated onlyB tx (wholly-A-absent block) is removed from unexpectedB and folded into toleratedIssue36 → no FAIL', () => {
  // classifyTxDiff over one onlyB tx (bare hash) reads UNEXPECTED/fail. Once we know its block is wholly
  // A-absent (in the absent set), the fold tolerates it: unexpectedB empties, fail flips false, and the
  // tolerated count surfaces. MUTATION (drop the tolerance entirely) → this fails.
  const base = classifyTxDiff([], ['0xtol'], new Set());
  assert.equal(base.fail, true, 'pre-tolerance: an onlyB tx fails');
  const rows = [{ hash: '0xtol', blockNumber: ISSUE_36_FLOOR_1 }];
  const absent = new Set([ISSUE_36_FLOOR_1]);
  const r = classifyOnlyBTxDiff(base, rows, 1, absent);
  assert.equal(
    r.fail,
    false,
    'a wholly-A-absent-block onlyB tx no longer fails',
  );
  assert.equal(r.class, 'realtime-parent-tx-gap');
  assert.deepEqual(r.unexpectedB, []);
  assert.equal(r.toleratedIssue36.count, 1);
  assert.deepEqual(r.toleratedIssue36.perChain, { 1: 1 });
});

test('classifyOnlyBTxDiff: the STRICTNESS case — an onlyB tx whose block EXISTS in A stays in unexpectedB → still UNEXPECTED FAIL', () => {
  // The tx-level-only-loss shape: block present in A (not in the absent set), tx onlyB. This is a NEW
  // divergence class and MUST keep failing. MUTATION (tolerate ALL onlyB txs regardless of block
  // absence) → this assertion fails: unexpectedB would empty and fail would flip false.
  const base = classifyTxDiff([], ['0xnew'], new Set());
  const rows = [{ hash: '0xnew', blockNumber: ISSUE_36_FLOOR_1 }];
  const absent = new Set(); // A HAS the block — it is not wholly absent
  const r = classifyOnlyBTxDiff(base, rows, 1, absent);
  assert.equal(r.fail, true, 'a B-only tx in an A-present block still FAILs');
  assert.equal(r.class, 'UNEXPECTED');
  assert.deepEqual(r.unexpectedB, ['0xnew']);
  assert.equal(r.toleratedIssue36.count, 0);
});

test('classifyOnlyBTxDiff: a MIX — one tolerated (A-absent block) + one hard (A-present block) → still FAIL, only the hard one in unexpectedB', () => {
  const base = classifyTxDiff([], ['0xtol', '0xhard'], new Set());
  const rows = [
    { hash: '0xtol', blockNumber: ISSUE_36_FLOOR_1 }, // wholly A-absent
    { hash: '0xhard', blockNumber: ISSUE_36_FLOOR_1 + 1 }, // A HAS this block
  ];
  const absent = new Set([ISSUE_36_FLOOR_1]); // only the first block is A-absent
  const r = classifyOnlyBTxDiff(base, rows, 1, absent);
  assert.equal(
    r.fail,
    true,
    'the hard onlyB tx is not masked by its tolerated sibling',
  );
  assert.deepEqual(r.unexpectedB, ['0xhard']);
  assert.equal(r.toleratedIssue36.count, 1);
});

test('classifyOnlyBTxDiff: does NOT touch onlyA (unreferencedA) or sharedMismatch — a run failing on those keeps failing', () => {
  // An A-only tx no log references → unreferencedA; the fold must leave that FAIL cause intact even when
  // every onlyB tx is tolerated.
  const base = classifyTxDiff(['0xorphan'], ['0xtol'], new Set());
  const rows = [{ hash: '0xtol', blockNumber: ISSUE_36_FLOOR_1 }];
  const r = classifyOnlyBTxDiff(base, rows, 1, new Set([ISSUE_36_FLOOR_1]));
  assert.equal(r.fail, true, 'the unreferenced onlyA tx still fails the run');
  assert.deepEqual(r.unreferencedA, ['0xorphan']);
  // and a sharedMismatch is likewise untouched
  const base2 = classifyTxDiff([], ['0xtol'], new Set(), 1);
  const r2 = classifyOnlyBTxDiff(base2, rows, 1, new Set([ISSUE_36_FLOOR_1]));
  assert.equal(r2.fail, true, 'the shared-row mismatch still fails the run');
  assert.equal(r2.sharedMismatch, 1);
});

test('classifyOnlyBTxDiff: empty / missing onlyBTxRows → the class is returned UNCHANGED', () => {
  const base = classifyTxDiff([], [], new Set());
  assert.equal(classifyOnlyBTxDiff(base, [], 1, new Set()), base);
  assert.equal(classifyOnlyBTxDiff(base, undefined, 1, new Set()), base);
});

test('stripTxOnlyBRows: drops the internal onlyBTxRows carrier, keeps every other field', () => {
  const withRows = {
    fail: false,
    class: 'realtime-parent-tx-gap',
    unexpectedB: [],
    onlyBTxRows: [{ hash: '0xa', blockNumber: 1 }],
  };
  const stripped = stripTxOnlyBRows(withRows);
  assert.equal('onlyBTxRows' in stripped, false);
  assert.equal(stripped.fail, false);
  assert.equal(stripped.class, 'realtime-parent-tx-gap');
  // a class with no onlyBTxRows is returned as-is
  const plain = { fail: false };
  assert.equal(stripTxOnlyBRows(plain), plain);
});

// classifyBucketMismatches: the checkpointBuckets knock-on. A bucket md5 mismatch is EXPLAINED only when
// removing the tolerated onlyB rows from leg B's bucket makes it byte-identical to leg A's bucket — EXACT
// attribution, never a count heuristic. A bucket with any OTHER cause stays a hard FAIL.

test('classifyBucketMismatches: a mismatch fully explained by tolerated onlyB rows → ok (not failed)', () => {
  // bucket '42' diverged (A=hAlpha, B=hBeta). Recomputing B WITHOUT the tolerated onlyB rows yields
  // hAlpha === A's md5 → the tolerated rows are the ENTIRE difference → explained.
  const compare = { mismatches: [{ bucket: '42', a: 'hAlpha', b: 'hBeta' }] };
  const bExcl = new Map([['42', 'hAlpha']]);
  const r = classifyBucketMismatches(compare, bExcl);
  assert.equal(r.ok, true);
  assert.deepEqual(r.explained, ['42']);
  assert.deepEqual(r.unexplained, []);
});

test('classifyBucketMismatches: a mismatch NOT fully explained (a second cause) → FAIL (the compensating-pair hole is closed)', () => {
  // bucket '42' has a tolerated onlyB row AND some OTHER divergence (a shared-row md5 drift). Removing
  // only the tolerated row leaves the bucket STILL different from A (hOther !== hAlpha) → unexplained →
  // FAIL. This is the exact hole a count-delta heuristic (one onlyB + one shared mismatch nets the same
  // row count) would let through. MUTATION (accept a bucket merely PRESENT in exB regardless of md5
  // equality) → this fails.
  const compare = { mismatches: [{ bucket: '42', a: 'hAlpha', b: 'hBeta' }] };
  const bExcl = new Map([['42', 'hOther']]); // B-minus-tolerated STILL differs from A
  const r = classifyBucketMismatches(compare, bExcl);
  assert.equal(r.ok, false);
  assert.deepEqual(r.explained, []);
  assert.equal(r.unexplained.length, 1);
  assert.equal(r.unexplained[0].bucket, '42');
});

test('classifyBucketMismatches: a mismatched bucket with NO tolerated onlyB row (absent from exB) → FAIL', () => {
  // bucket '7' diverged but had NO tolerated onlyB row removed (it is absent from the recomputed map),
  // so nothing about the tolerated class explains it → unexplained → FAIL. MUTATION (treat an absent
  // bucket as explained) → this fails.
  const compare = { mismatches: [{ bucket: '7', a: 'hA', b: 'hB' }] };
  const bExcl = new Map(); // no bucket '7' — no tolerated row fell in it
  const r = classifyBucketMismatches(compare, bExcl);
  assert.equal(r.ok, false);
  assert.deepEqual(r.unexplained, [{ bucket: '7', a: 'hA', b: 'hB' }]);
});

test('classifyBucketMismatches: mixed — one explained, one not → FAIL (the unexplained one survives)', () => {
  const compare = {
    mismatches: [
      { bucket: '10', a: 'hX', b: 'hXb' }, // explained (exB '10' === 'hX')
      { bucket: '20', a: 'hY', b: 'hYb' }, // NOT explained (exB '20' !== 'hY')
    ],
  };
  const bExcl = new Map([
    ['10', 'hX'],
    ['20', 'hStillDifferent'],
  ]);
  const r = classifyBucketMismatches(compare, bExcl);
  assert.equal(r.ok, false);
  assert.deepEqual(r.explained, ['10']);
  assert.equal(r.unexplained.length, 1);
  assert.equal(r.unexplained[0].bucket, '20');
});

test('classifyBucketMismatches: no mismatches at all → ok', () => {
  const r = classifyBucketMismatches({ mismatches: [] }, new Map());
  assert.equal(r.ok, true);
  assert.deepEqual(r.explained, []);
  assert.deepEqual(r.unexplained, []);
});

// buildBucketExclusionFilter: the SQL predicate that removes EXACTLY the tolerated onlyB log rows when
// recomputing leg B's bucket md5. Over- or under-exclusion would mis-attribute a bucket.

test('buildBucketExclusionFilter: excludes exactly the given (block_number, log_index) pairs', () => {
  const filter = buildBucketExclusionFilter([
    onlyBRow(25455946, 989),
    onlyBRow(25455946, 990),
    onlyBRow(25455045, 3),
  ]);
  // NUMERIC-EXACT anti-join: two PARALLEL integer array Consts (int8[] blocks, int4[] indices) zipped
  // row-wise by multi-arg unnest, then an anti-join on int=int equality. The k-th block pairs with the
  // k-th index, so this excludes EXACTLY these three pairs.
  assert.equal(
    filter,
    " and not exists (select 1 from unnest('{25455946,25455946,25455045}'::int8[], '{989,990,3}'::int4[]) as e(b, i) where e.b = block_number and e.i = log_index)",
  );
});

test('buildBucketExclusionFilter: an empty set → empty string (no exclusion)', () => {
  assert.equal(buildBucketExclusionFilter([]), '');
  assert.equal(buildBucketExclusionFilter(undefined), '');
});

test('buildBucketExclusionFilter: rows without a finite (block, index) are dropped, never injected', () => {
  // a block-table onlyB row has no logIndex; a NaN/garbage value must never leak into the SQL. The two
  // arrays must stay POSITIONALLY ALIGNED after drops — the surviving pair keeps its block WITH its index.
  const filter = buildBucketExclusionFilter([
    { blockNumber: 25455946 }, // no logIndex → dropped
    { blockNumber: 'x', logIndex: 5 }, // non-finite block → dropped
    onlyBRow(25455946, 989), // the only real pair
  ]);
  assert.equal(
    filter,
    " and not exists (select 1 from unnest('{25455946}'::int8[], '{989}'::int4[]) as e(b, i) where e.b = block_number and e.i = log_index)",
  );
});

test('buildBucketExclusionFilter: a LARGE excluded set stays a flat two-Const anti-join (no RowExpr IN-list)', () => {
  // The CONFIRMED chain-1 stack-depth thrower: the old `(block_number, log_index) in ((b0,i0),…)` was a
  // row-value IN-list Postgres expands into a right-recursive OR/AND tree — O(N) deep — which overran
  // the 2 MB max_stack_depth at parse time (reproduced throwing at ~7_000 pairs on an ephemeral PG16).
  // Normally the tolerated-onlyB set is tiny, but on chain 1 it can exceed that. Assert the flat
  // two-Const anti-join form even for a large set.
  const rows = Array.from({ length: 5_000 }, (_, i) =>
    onlyBRow(1_000_000 + i, i),
  );
  const filter = buildBucketExclusionFilter(rows);

  // (a) the numeric-exact anti-join over two array-literal Consts, zipped by multi-arg unnest
  assert.match(
    filter,
    /^ and not exists \(select 1 from unnest\('\{.*\}'::int8\[\], '\{.*\}'::int4\[\]\) as e\(b, i\) where e\.b = block_number and e\.i = log_index\)$/s,
    'uses `not exists (… unnest(int8[], int4[]) …)` — two Const parse nodes',
  );
  // exactly ONE int8[] block array and ONE int4[] index array (each a single Const), never per-pair
  assert.equal(
    (filter.match(/::int8\[\]/g) ?? []).length,
    1,
    'exactly ONE int8[] block array for the whole excluded set',
  );
  assert.equal(
    (filter.match(/::int4\[\]/g) ?? []).length,
    1,
    'exactly ONE int4[] index array for the whole excluded set',
  );

  // (b) NOT the old RowExpr IN-list — no `(block_number, log_index) in (`, no list at all
  assert.ok(
    !/\(block_number,\s*log_index\)\s+in\s*\(/.test(filter),
    'not the row-value-constructor IN-list (N RowExpr parse nodes)',
  );
  assert.ok(!/\bin\s*\(/.test(filter), 'no `in (…)` list at all');

  // (c) the two arrays carry every block and index in order (positionally aligned), byte-for-byte.
  const expectedBlocks = rows.map((r) => r.blockNumber).join(',');
  const expectedIndexes = rows.map((r) => r.logIndex).join(',');
  assert.ok(
    filter.includes(`'{${expectedBlocks}}'::int8[]`),
    'every excluded block is present in the int8[] array, in order, byte-for-byte',
  );
  assert.ok(
    filter.includes(`'{${expectedIndexes}}'::int4[]`),
    'every excluded index is present in the int4[] array, in order, byte-for-byte',
  );
});

test('buildBucketExclusionFilter: pairing is positional/injective — distinct pairs never collide, no cross-product', () => {
  // The anti-join reconstructs each pair POSITIONALLY from the two int arrays (unnest zips element k of
  // blocks with element k of indices), so (1,23), (12,3) and (123,0) stay THREE DISTINCT pairs. A naive
  // separator-less string concat would merge (1,23) and (12,3) into "123" and under-exclude; a
  // cross-product over the arrays would wrongly also exclude (1,0), (12,23), (123,3) etc. Assert the
  // exact zipped shape: block k lines up with index k, and nothing cross-pairs.
  const filter = buildBucketExclusionFilter([
    onlyBRow(1, 23),
    onlyBRow(12, 3),
    onlyBRow(123, 0),
  ]);
  assert.equal(
    filter,
    " and not exists (select 1 from unnest('{1,12,123}'::int8[], '{23,3,0}'::int4[]) as e(b, i) where e.b = block_number and e.i = log_index)",
    'blocks {1,12,123} zip position-for-position with indices {23,3,0} — (1,23),(12,3),(123,0), never a cross-product',
  );
});

// aggregateToleratedIssue36 + formatToleratedIssue36Line: the LOUD, VISIBLE reporting contract.

test('aggregateToleratedIssue36: sums logs + blocks per table and per chain into a grand total', () => {
  const results = [
    {
      chain: 1,
      classes: {
        logs: { toleratedIssue36: { count: 89, perChain: { 1: 89 } } },
        blocks: { toleratedIssue36: { count: 18, perChain: { 1: 18 } } },
      },
    },
    {
      chain: 8453,
      classes: {
        logs: { toleratedIssue36: { count: 0, perChain: {} } },
        blocks: { toleratedIssue36: { count: 0, perChain: {} } },
      },
    },
  ];
  const agg = aggregateToleratedIssue36(results);
  assert.equal(agg.count, 107, 'grand total = 89 logs + 18 blocks');
  assert.equal(agg.logs.count, 89);
  assert.equal(agg.blocks.count, 18);
  assert.deepEqual(agg.perChain, { 1: 107 });
});

test('aggregateToleratedIssue36: the REPORTING case — tolerated tx counts surface in a transactions sub-object and the grand total', () => {
  // The 12:22Z instance: 30 tolerated onlyB txs on chain 1, alongside the logs/blocks facets. The tx
  // count rolls into its own transactions sub-object, the per-chain total, and the grand total.
  const results = [
    {
      chain: 1,
      classes: {
        logs: { toleratedIssue36: { count: 238, perChain: { 1: 238 } } },
        blocks: { toleratedIssue36: { count: 27, perChain: { 1: 27 } } },
        transactions: { toleratedIssue36: { count: 30, perChain: { 1: 30 } } },
      },
    },
  ];
  const agg = aggregateToleratedIssue36(results);
  assert.equal(
    agg.transactions.count,
    30,
    'the tx facet surfaces its own count',
  );
  assert.deepEqual(agg.transactions.perChain, { 1: 30 });
  assert.equal(agg.count, 295, 'grand total = 238 logs + 27 blocks + 30 txs');
  assert.deepEqual(agg.perChain, { 1: 295 });
});

test('aggregateToleratedIssue36: a run with no issue #36 rows → zeros (transactions facet included)', () => {
  const agg = aggregateToleratedIssue36([
    { chain: 1, classes: { logs: {}, blocks: {}, transactions: {} } },
  ]);
  assert.equal(agg.count, 0);
  assert.equal(agg.transactions.count, 0);
  assert.deepEqual(agg.perChain, {});
});

test('formatToleratedIssue36Line: loud REMOVE line naming issue #36 + removal condition when count>0, including the transactions facet', () => {
  const line = formatToleratedIssue36Line({
    count: 295,
    logs: { count: 238 },
    blocks: { count: 27 },
    transactions: { count: 30 },
    perChain: { 1: 295 },
  });
  assert.match(line, /^TOLERATED \(known issue #36 — REMOVE/);
  assert.match(
    line,
    /REMOVE when issue #36 is resolved \(A repaired or leg retired\)/,
  );
  assert.match(line, /295 onlyB rows leg A lost/);
  assert.match(line, /logs:238 blocks:27 transactions:30/);
  assert.match(line, /1:295/);
});

test('formatToleratedIssue36Line: empty string when nothing tolerated (never a noisy zero line)', () => {
  // MUTATION (the LOUD counter always prints / prints on 0) → these fail.
  assert.equal(formatToleratedIssue36Line({ count: 0, perChain: {} }), '');
  assert.equal(formatToleratedIssue36Line(null), '');
  assert.equal(formatToleratedIssue36Line(undefined), '');
});

// ── D2: collectOnlyB — the injectable-cap capped path, and the REAL streaming-integration hook test ──

test('collectOnlyB: collects each onlyB row (block + logIndex) under the default cap, not capped', () => {
  const c = collectOnlyB();
  c.onOnlyB({ key: [100n, 5n] });
  c.onOnlyB({ key: [101n] }); // block-table shape: no log_index
  assert.equal(c.capped(), false);
  assert.deepEqual(c.onlyBRows, [
    { blockNumber: 100, logIndex: 5 },
    { blockNumber: 101, logIndex: undefined },
  ]);
});

test('collectOnlyB: an injectable cap flips capped() and stops collecting past the cap (default is ONLYB_ROW_CAP)', () => {
  // Parameterize the cap rather than lowering the production constant. With cap=2, the 3rd onlyB row
  // trips the cap: capped() flips true and the row is NOT collected. MUTATION (ignore the injected cap /
  // never set capped) → capped() stays false and this fails.
  const c = collectOnlyB(2);
  c.onOnlyB({ key: [1n, 0n] });
  c.onOnlyB({ key: [2n, 0n] });
  assert.equal(c.capped(), false, 'at the cap, not yet over');
  c.onOnlyB({ key: [3n, 0n] }); // over the cap
  assert.equal(c.capped(), true, 'the 3rd row trips the injected cap');
  assert.equal(c.onlyBRows.length, 2, 'the over-cap row is NOT collected');
  // the default cap is the production constant, unchanged
  assert.equal(ONLYB_ROW_CAP, 100_000);
});

// D2 HOOK INTEGRATION: drive the REAL mergeCompare/streamingDiff (from diff-batched.mjs) with a
// collectOnlyB collector as onOnlyB, over a fixture with B-only rows (a) in the MIDDLE of the merge, (b)
// in the TAIL DRAIN (after A's iterator is exhausted), and (c) at least one BELOW-floor row. Assert the
// collector received EVERY B-only row (count + identities) AND classifyOnlyBDiff FAILs on the below-floor
// row. This is the test that catches verifier mutation h1 ("streamingDiff onOnlyB skips every 2nd B-only
// row"), which the pure classifyOnlyBDiff unit tests alone do NOT — they feed a hand-built onlyBRows
// array and never exercise the streamingDiff hook wiring.
const HASH_ROW = (block, logIndex, h) => ({
  key: [BigInt(block), BigInt(logIndex)],
  hash: h,
});
const KEY_OF = (r) => r.key;

test('D2 hook integration: the REAL streamingDiff onOnlyB streams EVERY B-only row (middle + tail-drain), collector + classify catch the below-floor one', async () => {
  // Leg A holds a shared prefix; leg B additionally holds B-only rows scattered in the MIDDLE and, after
  // A is exhausted, a run of them in the TAIL DRAIN — plus one BELOW the chain-1 floor.
  const belowFloor = ISSUE_36_FLOOR_1 - 10; // (c) below-floor B-only row
  const a = [
    HASH_ROW(ISSUE_36_FLOOR_1, 0, 'shared0'),
    HASH_ROW(ISSUE_36_FLOOR_1 + 5, 0, 'shared1'),
    HASH_ROW(ISSUE_36_FLOOR_1 + 9, 0, 'shared2'),
  ];
  const b = [
    HASH_ROW(belowFloor, 0, 'bBelow'), // (c) B-only, below floor, sorts FIRST
    HASH_ROW(ISSUE_36_FLOOR_1, 0, 'shared0'), // shared
    HASH_ROW(ISSUE_36_FLOOR_1 + 2, 0, 'bMid1'), // (a) B-only in the middle
    HASH_ROW(ISSUE_36_FLOOR_1 + 5, 0, 'shared1'), // shared
    HASH_ROW(ISSUE_36_FLOOR_1 + 7, 0, 'bMid2'), // (a) B-only in the middle
    HASH_ROW(ISSUE_36_FLOOR_1 + 9, 0, 'shared2'), // shared (last A row)
    HASH_ROW(ISSUE_36_FLOOR_1 + 20, 0, 'bTail1'), // (b) B-only in the tail drain
    HASH_ROW(ISSUE_36_FLOOR_1 + 21, 1, 'bTail2'), // (b) B-only in the tail drain
  ];

  const collector = collectOnlyB();
  const diff = await mergeCompare(a, b, {
    keyFn: KEY_OF,
    mode: 'strict',
    onOnlyB: collector.onOnlyB,
  });

  // The diff's own onlyB count and the collected array must AGREE.
  assert.equal(
    diff.onlyB,
    5,
    'five B-only rows: below-floor + 2 middle + 2 tail',
  );
  assert.equal(
    collector.onlyBRows.length,
    5,
    'the collector received EVERY B-only row, including both tail-drain rows',
  );
  assert.equal(collector.capped(), false);

  // Exact identities (block numbers) of every collected B-only row, in key order.
  assert.deepEqual(
    collector.onlyBRows.map((r) => r.blockNumber),
    [
      belowFloor,
      ISSUE_36_FLOOR_1 + 2,
      ISSUE_36_FLOOR_1 + 7,
      ISSUE_36_FLOOR_1 + 20,
      ISSUE_36_FLOOR_1 + 21,
    ],
  );

  // classifyOnlyBDiff over the collected rows FAILs on the below-floor row (the 4 at/above floor are
  // tolerated; the 1 below floor is a hard onlyB). This is the wiring the pure unit tests never touch.
  const cls = classifyOnlyBDiff(diff, collector.onlyBRows, 1);
  assert.equal(cls.fail, true, 'the below-floor B-only row FAILs the table');
  assert.equal(cls.hardOnlyB, 1);
  assert.equal(cls.toleratedOnlyB.count, 4);
});

// ── D3: sampleToleratedOnlyB — the BOUNDED, spot-auditable sample for the status JSON ──
// Within the tolerated span cross-validation alone cannot distinguish leg-A loss from a leg-B
// fabrication of the same shape; the control is a third-party spot audit. This surfaces a bounded
// sample of tolerated block numbers so that audit is possible without psql access to the raw diff.

test('sampleToleratedOnlyB: bounded at TOLERATED_SAMPLE_SIZE, with min/max over the WHOLE set', () => {
  // 12 tolerated rows (block numbers out of order) → the sample is the FIRST 5, but min/max bracket ALL
  // 12. MUTATION (unbound the sample → returns all 12; or break min/max → wrong bracket) → this fails.
  const rows = [
    { blockNumber: 500 },
    { blockNumber: 100 },
    { blockNumber: 900 },
    { blockNumber: 300 },
    { blockNumber: 700 },
    { blockNumber: 200 },
    { blockNumber: 800 },
    { blockNumber: 400 },
    { blockNumber: 600 },
    { blockNumber: 1000 },
    { blockNumber: 50 },
    { blockNumber: 1100 },
  ];
  const s = sampleToleratedOnlyB(rows);
  assert.equal(TOLERATED_SAMPLE_SIZE, 5);
  assert.equal(
    s.sample.length,
    5,
    'the sample is bounded at TOLERATED_SAMPLE_SIZE, never the whole set',
  );
  assert.deepEqual(s.sample, [500, 100, 900, 300, 700], 'first 5, in order');
  assert.equal(s.min, 50, 'min over the WHOLE set, not just the sample');
  assert.equal(s.max, 1100, 'max over the WHOLE set, not just the sample');
  assert.equal(s.count, 12, 'the full tolerated count is reported alongside');
});

test('sampleToleratedOnlyB: fewer than the cap → sample is the whole (small) set; empty → null', () => {
  const s = sampleToleratedOnlyB([{ blockNumber: 42 }, { blockNumber: 7 }]);
  assert.deepEqual(s.sample, [42, 7]);
  assert.equal(s.min, 7);
  assert.equal(s.max, 42);
  assert.equal(s.count, 2);

  // nothing tolerated ⇒ null (no noisy empty sample in the status JSON)
  assert.equal(sampleToleratedOnlyB([]), null);
  assert.equal(sampleToleratedOnlyB(undefined), null);
});

test('sampleToleratedOnlyB: non-finite block numbers are dropped (never leak into the audit sample)', () => {
  const s = sampleToleratedOnlyB([
    { blockNumber: 10 },
    { blockNumber: 'x' }, // dropped
    { blockNumber: 20 },
  ]);
  assert.deepEqual(s.sample, [10, 20]);
  assert.equal(s.min, 10);
  assert.equal(s.max, 20);
  assert.equal(s.count, 2);
});

// ── D1: crossCheckOnlyBCollector — the BACKSTOP that refuses to trust an incomplete onlyB collector ──
// classifyOnlyBDiff decides tolerance from the rows the streamingDiff onOnlyB hook COLLECTED. That
// verdict is only sound if the collected array is EVERY onlyB row the diff counted. The backstop
// cross-checks the collected COUNT against the diff's own onlyB count: a silent hook-wiring drop (a
// skipped call, a lost row) leaves classifyOnlyBDiff deciding on an INCOMPLETE set → HARD FAIL that
// names itself (collectorMismatch). Only fires on the UN-capped path (a capped gap is expected).

test('crossCheckOnlyBCollector: collected count === diff.onlyB (not capped) → ok', () => {
  assert.deepEqual(crossCheckOnlyBCollector(89, 89, false), { ok: true });
});

test('crossCheckOnlyBCollector: collected count !== diff.onlyB (not capped) → HARD FAIL that names itself', () => {
  // The diff counted 89 onlyB rows but the collector only holds 44 — some were silently dropped before
  // classification. MUTATION (compare against collectedCount itself, or drop the count-mismatch clause)
  // → this fails: the backstop no longer catches the silent drop.
  const r = crossCheckOnlyBCollector(89, 44, false);
  assert.equal(r.ok, false);
  assert.deepEqual(r.collectorMismatch, { expected: 89, collected: 44 });
});

test('crossCheckOnlyBCollector: a count mismatch while CAPPED is expected → ok (the capped→FAIL path covers it)', () => {
  // When capped, collected < onlyB is BY DESIGN (the cap stopped collecting) — the separate capped→FAIL
  // path already fails the table, so the backstop must NOT double-fire here. MUTATION (fire the
  // cross-check even when capped) → this asserts ok:true and fails.
  const r = crossCheckOnlyBCollector(1_000_000, ONLYB_ROW_CAP, true);
  assert.deepEqual(r, { ok: true });
});

test('crossCheckOnlyBCollector: a nullish diff.onlyB with zero collected → ok (0 === 0)', () => {
  assert.deepEqual(crossCheckOnlyBCollector(undefined, 0, false), { ok: true });
});

// D1 HOOK INTEGRATION, mutation h1 in situ: a collector that SKIPS EVERY 2ND B-only row (the exact shape
// of the streamingDiff onOnlyB-skip mutation) yields collector.onlyBRows.length < diff.onlyB — which the
// D1 backstop catches as a HARD FAIL. This proves the backstop, not just the pure classifier, is what
// closes the "hook wiring has no backstop" gap: without D1 this drop would be silent.
test('D1 hook integration: a collector that skips every 2nd B-only row is caught by the backstop (HARD FAIL)', async () => {
  const hashRow = (block, logIndex, h) => ({
    key: [BigInt(block), BigInt(logIndex)],
    hash: h,
  });
  const keyOf = (r) => r.key;
  const a = [hashRow(ISSUE_36_FLOOR_1, 0, 'shared0')];
  const b = [
    hashRow(ISSUE_36_FLOOR_1, 0, 'shared0'),
    hashRow(ISSUE_36_FLOOR_1 + 1, 0, 'b1'),
    hashRow(ISSUE_36_FLOOR_1 + 2, 0, 'b2'),
    hashRow(ISSUE_36_FLOOR_1 + 3, 0, 'b3'),
    hashRow(ISSUE_36_FLOOR_1 + 4, 0, 'b4'),
  ];

  // A DELIBERATELY LOSSY collector standing in for mutation h1: it forwards only every 2nd B-only row.
  const collected = [];
  let seen = 0;
  const lossyOnOnlyB = (row) => {
    seen += 1;
    if (seen % 2 === 0) {
      return; // skip every 2nd row — the h1 shape
    }

    const key = row.key ?? [];
    collected.push({ blockNumber: Number(key[0]) });
  };

  const diff = await mergeCompare(a, b, {
    keyFn: keyOf,
    mode: 'strict',
    onOnlyB: lossyOnOnlyB,
  });

  assert.equal(
    diff.onlyB,
    4,
    'the diff independently counts all 4 B-only rows',
  );
  assert.equal(
    collected.length,
    2,
    'the lossy collector dropped every 2nd row',
  );
  // The backstop catches the drop: collected (2) !== diff.onlyB (4) → HARD FAIL that names itself.
  const back = crossCheckOnlyBCollector(diff.onlyB, collected.length, false);
  assert.equal(
    back.ok,
    false,
    'the backstop FAILs on the incomplete collection',
  );
  assert.deepEqual(back.collectorMismatch, { expected: 4, collected: 2 });
});

// ── persist-stagnation guard (issue #38) ─────────────────────────────────────────────────────────

// AB_STAGNATION_MAX_SKEW_S is interpolated into a numeric threshold the guard fails against, so a
// present-but-garbage or non-positive value MUST fail loud (a config guard never silently disables
// itself) — the same idiom as AB_SCHEMA_B / sanitizeSchemaIdent. Unset ⇒ the documented default.
// MUTATION: change the guard `n > 0` to `n >= 0` → the `readStagnationThreshold('0')` throws-assertion
// below fails (0 no longer rejected, silently disabling the guard since no skew can exceed 0... wait,
// a 0 threshold makes EVERY positive skew fail — a different, equally-wrong footgun; either way the
// value must be rejected). MUTATION: drop the `Number.isFinite(n)` conjunct → 'abc'→NaN passes the
// `n > 0`? no (NaN > 0 is false) — but 'Infinity'→Infinity passes `n > 0`, so the Infinity-rejection
// assertion fails.
test('readStagnationThreshold: default when unset, fail-loud on garbage / non-positive naming the var', () => {
  // unset / empty ⇒ the documented default (2h), never a silent 0
  assert.equal(
    readStagnationThreshold(undefined),
    AB_STAGNATION_DEFAULT_MAX_SKEW_S,
  );
  assert.equal(readStagnationThreshold(null), AB_STAGNATION_DEFAULT_MAX_SKEW_S);
  assert.equal(readStagnationThreshold(''), AB_STAGNATION_DEFAULT_MAX_SKEW_S);
  assert.equal(AB_STAGNATION_DEFAULT_MAX_SKEW_S, 7200);

  // a valid positive value is taken verbatim (string or number)
  assert.equal(readStagnationThreshold('3600'), 3600);
  assert.equal(readStagnationThreshold(1800), 1800);
  // a large threshold is accepted as-is (intentional widen), never silently capped
  assert.equal(readStagnationThreshold('999999999'), 999_999_999);

  // 0 (disables the guard), a negative, NaN, Infinity, and non-numeric text ALL fail loud, and the
  // error names the env var so an operator knows exactly what to fix.
  for (const bad of ['0', '-1', '-0.5', 'NaN', 'abc', '1h', 'Infinity', '  ']) {
    assert.throws(
      () => readStagnationThreshold(bad),
      /AB_STAGNATION_MAX_SKEW_S must be a positive number of seconds/,
      `garbage/non-positive ${JSON.stringify(bad)} must fail loud`,
    );
  }
});

// ── stagnationDecision: the DIRECTION-AWARE, cross-run decision (issue #38, review D2) ──────────────
//
// The decision now takes the prior run's per-chain state (`prev`) and a wall clock (`nowMs`) and
// returns { fail, reason, staleSide, skew, tsA, tsB, maxA, maxB, nextState }. A one-shot skew reading
// cannot tell a transient lag from a wedge; two observations tell them apart by DIRECTION. Each clause
// below is mutation-verified; the PR body records which mutation each guards.

// Fixed "now" so the empty-leg wall-clock timing is deterministic.
const NOW = 1_000_000_000_000; // ms

test('stagnationDecision: both legs populated, skew ≤ threshold → PASS, clears state', () => {
  const t = 7200;
  const r = stagnationDecision({
    maxA: '100',
    tsA: 1_700_000_000,
    maxB: '99',
    tsB: 1_700_000_000 - 3600, // 1h behind, under 2h
    threshold: t,
    prev: { tsA: 1_699_000_000, tsB: 1_699_000_000 - 999_999, skew: 999_999 }, // stale prior state
    nowMs: NOW,
  });
  assert.equal(r.fail, false, '1h skew is under the 2h threshold');
  assert.equal(r.reason, 'both-populated-in-skew');
  assert.equal(
    r.staleSide,
    null,
    'a below-threshold lag names no stalled side',
  );
  // FINDING 3: the real skew IS surfaced below threshold (telemetry), not nulled.
  assert.equal(
    r.skew,
    3600,
    'the below-threshold skew is surfaced for telemetry',
  );
  // D1/D2: a healthy (skew ≤ threshold) run clears the wedge and the emptiness arm, but carries the
  // last-known ts + skew forward as evidence — the unified record, not null.
  assert.deepEqual(r.nextState, {
    tsA: 1_700_000_000,
    tsB: 1_700_000_000 - 3600,
    skew: 3600,
    emptySinceA: null,
    emptySinceB: null,
    wedgeFailedSince: null,
    wedgeStaleSide: null,
    wedgeReason: null,
  });
  assert.equal(
    r.nextState.wedgeFailedSince,
    null,
    'skew ≤ threshold is a recovery (D2): wedge cleared',
  );
  // maxA/maxB carried through for the counters JSON
  assert.equal(r.maxA, 100);
  assert.equal(r.maxB, 99);
});

// NO-PREV ARMING: the FIRST run to see an over-threshold skew does NOT fail — it ARMS and defers the
// verdict one run (candor: one-run detection delay). MUTATION: make the no-prev branch FAIL immediately
// (drop the arming, `fail: false` → `fail: true`) → the `fail === false` assertion here fails.
test('stagnationDecision: both populated, skew > threshold, NO prev → ARM (non-fail), reason skew-above-threshold-arming', () => {
  const t = 7200;
  const r = stagnationDecision({
    maxA: '100',
    tsA: 1_700_000_000,
    maxB: '90',
    tsB: 1_700_000_000 - 10_800, // 3h behind → over threshold, but no prior observation
    threshold: t,
    prev: null,
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    false,
    'the FIRST over-threshold observation arms, does not fail',
  );
  assert.equal(r.reason, 'skew-above-threshold-arming');
  assert.equal(r.staleSide, null, 'no side named on the deferred (armed) run');
  assert.equal(
    r.skew,
    10_800,
    'the skew is surfaced (loud info), just not yet a fail',
  );
  // nextState carries BOTH legs' ts + skew (finding 2: NOT keyed on the stale side) for the next run's
  // direction check, in the unified evidence shape (D1). No prior arm, no wedge → both null.
  assert.deepEqual(r.nextState, {
    tsA: 1_700_000_000,
    tsB: 1_700_000_000 - 10_800,
    skew: 10_800,
    emptySinceA: null,
    emptySinceB: null,
    wedgeFailedSince: null,
    wedgeStaleSide: null,
    wedgeReason: null,
  });
});

// DIRECTION — FROZEN: prev carries BOTH legs' ts; this run leg B did NOT advance while leg A did → a
// one-sided wedge → FAIL 'frozen'. MUTATION: invert the frozen clause (`(!bAdvanced && aAdvanced)` →
// `(bAdvanced && aAdvanced)`) → a frozen leg reads as advancing and this `fail === true` fails.
test('stagnationDecision: both populated, over threshold, prev armed, stale leg FROZEN → FAIL frozen', () => {
  const t = 7200;
  const prev = { tsA: 1_699_995_000, tsB: 1_699_989_200, skew: 5800 };
  const r = stagnationDecision({
    maxA: '100',
    tsA: 1_700_000_000, // leg A advanced since prev.tsA
    maxB: '90',
    tsB: 1_699_989_200, // leg B UNCHANGED since prev.tsB → frozen
    threshold: t,
    prev,
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    true,
    'a leg that did not advance while the other did is a frozen wedge',
  );
  assert.equal(r.reason, 'frozen');
  assert.equal(r.staleSide, 'B', 'the OLDER-ts leg is the stalled one');
  assert.equal(r.skew, 10_800);
});

// DIRECTION — SKEW GROWING: BOTH legs advanced (so it is not a frozen wedge), but the skew INCREASED
// vs prev (the stale leg trickles forward while the healthy leg races ahead = falling further behind)
// → FAIL 'skew-growing'. This is the clause that stops a slow wedge fail-OPENing. MUTATION: drop the
// `skewIncreased` disjunct (fail = oneLegFrozenWhileOtherAdvanced only) → a trickling leg reads
// non-fail and this fails.
test('stagnationDecision: both populated, prev armed, stale leg advanced BUT skew GROWING → FAIL skew-growing', () => {
  const t = 7200;
  const prev = { tsA: 1_699_997_000, tsB: 1_699_988_000, skew: 9000 };
  const r = stagnationDecision({
    maxA: '100',
    tsA: 1_700_000_000, // leg A advanced +3000s
    maxB: '95',
    tsB: 1_699_988_500, // leg B advanced +500s — both moved, but B fell further behind
    threshold: t,
    prev,
    nowMs: NOW,
  });
  // skew now = 1_700_000_000 - 1_699_988_500 = 11_500 > prev.skew 9000 → growing
  assert.equal(r.skew, 11_500, 'skew grew from 9000 to 11500');
  assert.equal(r.fail, true, 'a trickling leg still falling behind is a wedge');
  assert.equal(r.reason, 'skew-growing');
  assert.equal(r.staleSide, 'B');
});

// DIRECTION — CATCHING UP: BOTH legs advanced AND the skew SHRANK → non-fail 'catching-up' (a leg
// mid-recovery, e.g. a Soak restart re-syncing, closing the gap). MUTATION: drop the catching-up
// branch (always fail when over threshold) → a legitimately-recovering leg false-FAILs and this
// `fail === false` fails.
test('stagnationDecision: both populated, prev armed, stale leg advanced AND skew SHRANK → non-fail catching-up', () => {
  const t = 7200;
  const prev = { tsA: 1_699_995_000, tsB: 1_699_980_000, skew: 15_000 };
  const r = stagnationDecision({
    maxA: '100',
    tsA: 1_700_000_000, // leg A advanced +5000s
    maxB: '98',
    tsB: 1_699_990_000, // leg B advanced +10000s; skew now 10_000 < prev 15_000 → shrinking, gap closing
    threshold: t,
    prev,
    nowMs: NOW,
  });
  assert.equal(r.skew, 10_000, 'skew shrank from 15000 to 10000');
  assert.equal(
    r.fail,
    false,
    'both legs advancing AND closing the gap is catching up, not a wedge',
  );
  assert.equal(r.reason, 'catching-up');
  assert.equal(r.staleSide, null, 'a catching-up leg is not named as stalled');
});

// The stale leg is named by the OLDER timestamp, symmetric in both directions. MUTATION: name the
// stale side by the LARGER ts (swap the a<b / b<a arms) → the leg-A-stale case below reads 'B'.
test('stagnationDecision: direction — the OLDER-timestamp leg is named, symmetric A/B', () => {
  const t = 7200;
  // leg A frozen (unchanged) while leg B advanced → one-sided wedge, A stalled
  const prevA = { tsA: 1_699_989_200, tsB: 1_699_995_000, skew: 5800 };
  const aStale = stagnationDecision({
    maxA: '90',
    tsA: 1_699_989_200, // leg A older AND frozen since prev.tsA
    maxB: '100',
    tsB: 1_700_000_000, // leg B advanced
    threshold: t,
    prev: prevA,
    nowMs: NOW,
  });
  assert.equal(aStale.fail, true);
  assert.equal(
    aStale.staleSide,
    'A',
    'leg A (older newest row) is the stalled side',
  );

  // mirror: leg B frozen while leg A advanced → B stalled
  const prevB = { tsA: 1_699_995_000, tsB: 1_699_989_200, skew: 5800 };
  const bStale = stagnationDecision({
    maxA: '100',
    tsA: 1_700_000_000, // leg A advanced
    maxB: '90',
    tsB: 1_699_989_200, // leg B older AND frozen since prev.tsB
    threshold: t,
    prev: prevB,
    nowMs: NOW,
  });
  assert.equal(bStale.fail, true);
  assert.equal(
    bStale.staleSide,
    'B',
    'leg B (older newest row) is the stalled side',
  );
});

// STRICT threshold boundary: a skew EXACTLY at the threshold is within tolerance → PASS (in-skew), no
// arming. MUTATION: `skew <= threshold` → `skew < threshold` → the exactly-at case falls through to the
// over-threshold arm and this `fail === false, reason 'both-populated-in-skew'` assertion fails.
test('stagnationDecision: skew exactly at the threshold is in tolerance (PASS, cleared)', () => {
  const t = 7200;
  const r = stagnationDecision({
    maxA: '10',
    tsA: 1_700_000_000,
    maxB: '9',
    tsB: 1_700_000_000 - t, // skew exactly == threshold
    threshold: t,
    prev: { tsA: 1, tsB: 1 - 999, skew: 999 },
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    false,
    'skew == threshold is within tolerance, not a stall',
  );
  assert.equal(r.reason, 'both-populated-in-skew');
  // in-tolerance clears the wedge + emptiness arm (D2 recovery) but carries the ts/skew evidence (D1)
  assert.equal(
    r.nextState.wedgeFailedSince,
    null,
    'in-tolerance clears any prior wedge',
  );
  assert.equal(
    r.nextState.emptySinceA,
    null,
    'in-tolerance leaves no leg-A emptiness arm',
  );
  assert.equal(
    r.nextState.emptySinceB,
    null,
    'in-tolerance leaves no leg-B emptiness arm',
  );
  assert.equal(
    r.nextState.skew,
    t,
    'the exactly-at-threshold skew is carried as evidence',
  );
});

// EQUAL timestamps → skew 0 → in tolerance → PASS. (skew 0 can never exceed a positive threshold.)
test('stagnationDecision: equal timestamps → PASS, no stale side, state cleared', () => {
  const eq = stagnationDecision({
    maxA: '500',
    tsA: 1_700_000_000,
    maxB: '500',
    tsB: 1_700_000_000,
    threshold: 7200,
    prev: null,
    nowMs: NOW,
  });
  assert.equal(eq.fail, false);
  assert.equal(eq.reason, 'both-populated-in-skew');
  assert.equal(eq.staleSide, null);
});

// ── FINDING 2 (High): a stale-side FLIP must not discard evidence and re-arm ─────────────────────────
//
// The pre-fix state was keyed on the stale side, so an ALTERNATING sequence — one leg always frozen but
// the OLDER side flipping run to run — re-armed every run and never FAILed. The fix persists BOTH legs'
// ts + skew each run and evaluates each leg's frozen-ness regardless of which is currently stale, so the
// wedge is caught the first run after arming. This encodes the exact adversarial sequence from the
// review: it MUST FAIL at run 2 (leg B frozen while leg A advanced, skew grew 19000 → 30000).
//
// MUTATION: revert to keying the prior-observation lookup on the stale side
// (`prev && prev.staleSide === olderSide` for `priorBoth`, carrying only the stale leg's ts) → run 2's
// stale side (B) differs from run 1's (A), so `priorBoth` is null, run 2 RE-ARMS instead of failing,
// and the `run2.fail === true` assertion below fails. This is the finding-2 mutation.
test('FINDING 2: the exact 4-run alternating adversarial sequence FAILs at run 2 (one leg always frozen)', () => {
  const t = 7200;
  // Feed the runs through the state machine exactly as the differ would: prev = the previous run's
  // nextState. threshold 7200s throughout.
  const run = (tsA, tsB, prev) =>
    stagnationDecision({
      maxA: String(tsA),
      tsA,
      maxB: String(tsB),
      tsB,
      threshold: t,
      prev,
      nowMs: NOW,
    });

  // run1: A=1000, B=20000 → A older, skew 19000 > threshold, NO prev → ARM (non-fail)
  const r1 = run(1000, 20_000, null);
  assert.equal(
    r1.fail,
    false,
    'run1 is the first over-threshold observation → arms',
  );
  assert.equal(r1.reason, 'skew-above-threshold-arming');
  assert.deepEqual(r1.nextState, {
    tsA: 1000,
    tsB: 20_000,
    skew: 19_000,
    emptySinceA: null,
    emptySinceB: null,
    wedgeFailedSince: null,
    wedgeStaleSide: null,
    wedgeReason: null,
  });

  // run2: A=50000, B=20000 → B FROZEN (unchanged 20000) while A advanced; skew grew 19000 → 30000.
  // The stale side FLIPPED A→B, but the fix does NOT re-arm: it FAILs (B frozen, A advanced). This is
  // the FIRST fail of the wedge episode: wedgeStaleSide='B', wedgeReason='frozen' are locked in.
  const r2 = run(50_000, 20_000, r1.nextState);
  assert.equal(
    r2.fail,
    true,
    'run2: leg B frozen while leg A advanced is a one-sided wedge → FAIL',
  );
  assert.equal(r2.reason, 'frozen');
  assert.equal(
    r2.staleSide,
    'B',
    'leg B (the frozen, older leg) is named stalled',
  );
  assert.equal(r2.skew, 30_000, 'skew grew from 19000 to 30000');

  // run3 & run4: the wedge is now STICKY (review delta 4). Neither run genuinely recovers (the skew keeps
  // growing), so they stay FAIL 'wedge-unrecovered' attributed to the CARRIED wedge stale leg (B) — never
  // laundered by the alternation. (Old non-sticky code re-derived staleSide per run; the sticky gate now
  // owns attribution, so the wedge B is never dropped.)
  const r3 = run(50_000, 90_000, r2.nextState); // B advanced but skew grew 30000→40000: no recovery
  assert.equal(r3.fail, true, 'run3: the sticky wedge is not recovered → FAIL');
  assert.equal(r3.reason, 'wedge-unrecovered');
  assert.equal(
    r3.staleSide,
    'B',
    'the carried wedge stale leg (B) stays named through the sticky fail',
  );
  const r4 = run(140_000, 90_000, r3.nextState); // A advanced, skew grew again: still no recovery
  assert.equal(r4.fail, true, 'run4: still the same unrecovered wedge → FAIL');
  assert.equal(r4.reason, 'wedge-unrecovered');
  assert.equal(r4.staleSide, 'B');
});

// FINDING 2 — the CONVERSE: a stale-side flip on a HEALTHY (below-threshold, or both-advancing)
// sequence must NOT false-FAIL. Flips alone are not a signal — only a frozen leg while the other
// advances (or a growing skew) is. Here the older side flips A→B across runs but both legs keep
// advancing and the skew stays under / shrinks — never a fail.
test('FINDING 2: a stale-side flip while both legs stay HEALTHY does not false-FAIL', () => {
  const t = 7200;
  const run = (tsA, tsB, prev) =>
    stagnationDecision({
      maxA: String(tsA),
      tsA,
      maxB: String(tsB),
      tsB,
      threshold: t,
      prev,
      nowMs: NOW,
    });

  // run1: A older by 100 (under threshold) — PASS, no wedge/emptiness armed (D1: evidence still carried)
  const r1 = run(1_700_000_000, 1_700_000_100, null);
  assert.equal(r1.fail, false);
  assert.equal(r1.reason, 'both-populated-in-skew');
  assert.equal(
    r1.nextState.wedgeFailedSince,
    null,
    'a below-threshold run arms no wedge',
  );
  assert.equal(
    r1.nextState.emptySinceA,
    null,
    'a below-threshold run arms no leg-A emptiness',
  );
  assert.equal(
    r1.nextState.emptySinceB,
    null,
    'a below-threshold run arms no leg-B emptiness',
  );

  // run2: the older side FLIPS to B, still under threshold, both advanced — PASS, no fail
  const r2 = run(1_700_000_300, 1_700_000_150, r1.nextState);
  assert.equal(r2.fail, false, 'a below-threshold flip is not a wedge');
  assert.equal(r2.reason, 'both-populated-in-skew');
  assert.equal(r2.staleSide, null);

  // Now an OVER-threshold flip where BOTH legs advance and the skew SHRINKS across the flip: run1 arms
  // (A older by 29000), run2 flips (B now older) but BOTH legs moved and the gap narrowed to 10000 →
  // catching-up, NOT a fail — a flip that is genuinely closing (both legs live) is not a wedge.
  const a1 = run(1_000, 30_000, null); // A older, skew 29000 → arm
  assert.equal(a1.fail, false);
  assert.equal(a1.reason, 'skew-above-threshold-arming');
  // A advanced 1000→45000, B advanced 30000→35000 (both live); B now older by 10000 (flip), skew shrank
  const a2 = run(45_000, 35_000, a1.nextState);
  assert.equal(
    a2.fail,
    false,
    'both legs advanced and the gap narrowed — catching up, not a wedge',
  );
  assert.equal(a2.reason, 'catching-up');
  assert.equal(a2.staleSide, null);
});

// ── FINDING 1 (semantics + candor): a steady over-threshold lag with BOTH legs advancing and the skew
// HELD FLAT is a distinct, honest NON-FAIL reason ('lagging-constant'), not mislabelled 'catching-up' ──
//
// My ruling (see PR body): this stays NON-FAIL — the stale leg IS advancing, so the finalized-overlap
// window `hi` advances with it and the WINDOWED diff owns coverage of that regime; this guard exists
// only for FROZEN windows. But labelling a constant-skew lag as 'catching-up' (which implies the gap is
// closing) is dishonest — a flat skew is NOT closing. MUTATION: fold the flat-skew case back into
// 'catching-up' (drop the `skewDecreased` split, use `reason = 'catching-up'` for any non-fail) → the
// `reason === 'lagging-constant'` assertion below fails.
test('FINDING 1: both legs advance, skew HELD FLAT over threshold → non-fail, reason lagging-constant (not catching-up)', () => {
  const t = 7200;
  // prev: A older by 10000; this run BOTH advance by the SAME +5000 so the skew is unchanged at 10000
  const prev = { tsA: 1_699_990_000, tsB: 1_700_000_000, skew: 10_000 };
  const r = stagnationDecision({
    maxA: '100',
    tsA: 1_699_995_000, // leg A advanced +5000
    maxB: '110',
    tsB: 1_700_005_000, // leg B advanced +5000 → skew still 10000 (flat), still > threshold
    threshold: t,
    prev,
    nowMs: NOW,
  });
  assert.equal(r.skew, 10_000, 'the skew held flat at 10000 across the run');
  assert.equal(
    r.fail,
    false,
    'a steady lag with the stale leg advancing is NOT a frozen-window wedge',
  );
  assert.equal(
    r.reason,
    'lagging-constant',
    'a flat skew is honestly labelled lagging-constant, not catching-up (the gap is NOT closing)',
  );
  assert.equal(r.staleSide, null, 'a non-fail lag names no stalled side');
});

// ── FINDING 3 (Low): the real skew is surfaced BELOW threshold, in chainCounters, in the pass state ───
//
// pass() returned skew:null for a both-populated below-threshold state, so chainCounters emitted
// stagnationSkewSeconds:null — real telemetry lost. The fix surfaces the computed skew whenever both
// legs have timestamps. MUTATION: restore `pass('both-populated-in-skew')` (skew:null) for the
// below-threshold branch → chainCounters.stagnationSkewSeconds reads null and this assertion fails.
test('FINDING 3: below-threshold both-populated → real skew surfaced in chainCounters (not null)', () => {
  // drive the decision below threshold, then flow it through the exact persistStagnation shape the
  // caller builds, then chainCounters — the full telemetry path.
  const d = stagnationDecision({
    maxA: '100',
    tsA: 1_700_000_000,
    maxB: '99',
    tsB: 1_700_000_000 - 3600, // 1h skew, under the 2h threshold
    threshold: 7200,
    prev: null,
    nowMs: NOW,
  });
  assert.equal(d.fail, false);
  assert.equal(
    d.skew,
    3600,
    'the decision surfaces the real below-threshold skew',
  );

  const r = {
    chain: 1,
    lo: 100,
    hi: 200,
    verdict: 'PASS',
    classes: {
      persistStagnation: {
        fail: d.fail,
        reason: d.reason,
        staleSide: d.staleSide,
        skewSeconds: d.skew,
        thresholdSeconds: 7200,
        maxA: d.maxA,
        maxB: d.maxB,
        tsA: d.tsA,
        tsB: d.tsB,
      },
    },
  };
  const c = chainCounters(r);
  assert.equal(
    c.stagnationSkewSeconds,
    3600,
    'the below-threshold skew is real telemetry in the counters, never null',
  );
  assert.equal(c.stagnationReason, 'both-populated-in-skew');
});

// ── one-leg-empty: wall-clock arming + mutual-freeze exemption ──────────────────────────────────────

// EMPTY ARMING: on the FIRST observation of a one-sided emptiness, arm firstEmptyAtMs = now and do NOT
// fail (no prior populated-leg motion to compare). MUTATION: fail immediately on emptiness (drop the
// arming / the `emptyLongEnough && populatedAdvanced` gate) → this `fail === false` at cold start fails.
test('stagnationDecision: one leg empty, FIRST observation → arm firstEmptyAtMs, non-fail (empty-arming)', () => {
  const r = stagnationDecision({
    maxA: '12345',
    tsA: 1_700_000_000,
    maxB: null,
    tsB: null, // leg B empty
    threshold: 7200,
    prev: null,
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    false,
    'the first one-sided-empty observation arms, never fails at cold start',
  );
  assert.equal(r.reason, 'empty-arming');
  assert.equal(r.skew, null, 'skew is unmeasurable with one leg empty');
  assert.equal(r.staleSide, null);
  // D1 unified record: the empty leg (B) arms its per-leg emptySinceB {atMs}; leg A is not empty so
  // emptySinceA stays null; the populated leg's ts is carried, the empty leg's is null (never observed);
  // no wedge.
  assert.deepEqual(r.nextState, {
    tsA: 1_700_000_000,
    tsB: null,
    skew: null,
    emptySinceA: null,
    emptySinceB: { atMs: NOW },
    wedgeFailedSince: null,
    wedgeStaleSide: null,
    wedgeReason: null,
  });
  // the populated leg's max is still surfaced for the counters JSON; the empty leg's is null
  assert.equal(r.maxA, 12_345);
  assert.equal(r.maxB, null);
});

// EMPTY WEDGE FIRES: empty long enough (now − firstEmptyAtMs > threshold*1000) AND the populated leg
// ADVANCED since prev → a genuinely one-sided wedge → FAIL, staleSide = the empty leg. MUTATION: drop
// the `emptyLongEnough` clause → it would fire the instant the populated leg moves (no grace); drop the
// `populatedAdvanced` clause → it would fire even on a mutual freeze (below). Either flips an assertion.
test('stagnationDecision: one leg empty PAST the grace window while the other advances → FAIL one-sided-empty', () => {
  const t = 7200; // → grace = 7_200_000 ms
  // Unified prev (D1): B empty since armAt (> threshold ago), A last-known 1_700_000_000, no wedge yet.
  const armAt = NOW - (t * 1000 + 60_000);
  const prev = {
    tsA: 1_700_000_000,
    tsB: null,
    skew: null,
    emptySinceA: null,
    emptySinceB: { atMs: armAt },
    wedgeFailedSince: null,
  };
  const r = stagnationDecision({
    maxA: '12500',
    tsA: 1_700_050_000, // populated leg ADVANCED since prev.tsA
    maxB: null,
    tsB: null, // leg B still empty
    threshold: t,
    prev,
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    true,
    'empty past the grace window while the other leg advances is a wedge',
  );
  assert.equal(r.reason, 'one-sided-empty');
  assert.equal(r.staleSide, 'B', 'the empty leg is the stalled side');
  // emptySinceB.atMs is preserved from prev (not re-armed); the live leg's advanced ts is carried; the
  // fresh fail arms the sticky wedge (D2) with the empty leg (B) as the carried wedge stale side.
  assert.equal(r.nextState.emptySinceB.atMs, armAt);
  assert.equal(
    r.nextState.emptySinceA,
    null,
    'the populated leg arms no emptiness',
  );
  assert.equal(r.nextState.tsA, 1_700_050_000);
  assert.equal(r.nextState.tsB, null, 'the empty leg has no prior ts to carry');
  assert.equal(
    r.nextState.wedgeFailedSince,
    NOW,
    'a fresh one-sided-empty wedge sets wedgeFailedSince',
  );
  assert.equal(
    r.nextState.wedgeStaleSide,
    'B',
    'the wedge stale side is the empty leg',
  );
  assert.equal(
    r.nextState.wedgeReason,
    'one-sided-empty',
    'the true first-fail reason is locked in',
  );
});

// MUTUAL-FREEZE EXEMPTION: empty long enough, but the populated leg is ALSO frozen (did NOT advance
// since prev) → NOT a one-sided wedge → non-fail. MUTATION: drop the `populatedAdvanced` conjunct so
// emptiness alone fails → this mutual-freeze case false-FAILs and the `fail === false` assertion fails.
test('stagnationDecision: one leg empty past grace but the OTHER leg is also frozen → non-fail (mutual freeze)', () => {
  const t = 7200;
  const prev = {
    tsA: 1_700_000_000,
    tsB: null,
    skew: null,
    emptySinceA: null,
    emptySinceB: { atMs: NOW - (t * 1000 + 60_000) },
    wedgeFailedSince: null,
  };
  const r = stagnationDecision({
    maxA: '12345',
    tsA: 1_700_000_000, // populated leg UNCHANGED since prev → also frozen
    maxB: null,
    tsB: null,
    threshold: t,
    prev,
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    false,
    'a mutual freeze (both quiet) is benign, never a one-sided wedge',
  );
  assert.equal(r.reason, 'empty-arming');
  assert.equal(r.staleSide, null);
  assert.equal(
    r.nextState.wedgeFailedSince,
    null,
    'a mutual freeze arms no wedge',
  );
});

// EMPTY CLEARS ON FIRST ROW: the moment the empty leg gains rows, both legs are populated → the
// emptiness arm is dropped and (skew ≤ threshold) any prior wedge recovers. Here the once-empty leg now
// has a ts within skew → PASS, emptySince/wedgeFailedSince cleared. MUTATION: carry emptySince forward
// once populated → the assertion emptySince === null fails.
test('stagnationDecision: the empty leg gains rows → clears emptiness state (both-populated branch)', () => {
  const prev = {
    tsA: 1_700_000_000,
    tsB: null,
    skew: null,
    emptySinceA: null,
    emptySinceB: { atMs: NOW - 9_000_000 },
    wedgeFailedSince: null,
  };
  const r = stagnationDecision({
    maxA: '12500',
    tsA: 1_700_000_100,
    maxB: '9',
    tsB: 1_700_000_000, // leg B now HAS a row, within skew
    threshold: 7200,
    prev,
    nowMs: NOW,
  });
  assert.equal(r.fail, false);
  assert.equal(r.reason, 'both-populated-in-skew');
  assert.equal(
    r.nextState.emptySinceB,
    null,
    'gaining rows clears leg B armed emptiness',
  );
  assert.equal(r.nextState.emptySinceA, null, 'leg A was never empty');
  assert.equal(
    r.nextState.wedgeFailedSince,
    null,
    'within-skew is a recovery, no wedge carried',
  );
});

// BOTH EMPTY → benign mutual/not-started → PASS, no side, state cleared. MUTATION: coalesce null→0
// would make skew 0 read via the populated branch; the explicit reason 'both-empty' pins the branch.
test('stagnationDecision: both legs empty → PASS, no side, state cleared (mutual / not started)', () => {
  const both = stagnationDecision({
    maxA: null,
    tsA: null,
    maxB: null,
    tsB: null,
    threshold: 7200,
    prev: { emptySide: 'B', firstEmptyAtMs: NOW - 9_000_000, populatedTs: 1 },
    nowMs: NOW,
  });
  assert.equal(both.fail, false);
  assert.equal(both.reason, 'both-empty');
  assert.equal(both.skew, null);
  assert.equal(both.staleSide, null);
  assert.equal(both.nextState, null);
  assert.equal(both.maxA, null);
  assert.equal(both.maxB, null);
});

// A NULL/NaN/empty-string timestamp is "no rows" for that leg — never a phantom 0 (which would be
// ~1.7e9 seconds behind any real block and false-alarm). Numeric-string timestamps (psql bigint text)
// parse. MUTATION: drop the `Number.isFinite` guard in tsToSeconds → an unparseable string coerces to
// NaN and flows into the populated branch as a phantom, breaking the "no rows" assertion below.
test('stagnationDecision: NULL / unparseable timestamps are "no rows"; numeric strings parse', () => {
  // both parse (psql bigint-as-text), within skew → PASS
  const parsed = stagnationDecision({
    maxA: '100',
    tsA: '1700000000',
    maxB: '100',
    tsB: '1699999000',
    threshold: 7200,
    prev: null,
    nowMs: NOW,
  });
  assert.equal(parsed.fail, false);
  assert.equal(parsed.reason, 'both-populated-in-skew');

  // an unparseable ts on one leg → that leg reads as no-rows (one-sided empty, armed), never a phantom 0
  const garbageTs = stagnationDecision({
    maxA: '100',
    tsA: 'not-a-number',
    maxB: '100',
    tsB: '1700000000',
    threshold: 7200,
    prev: null,
    nowMs: NOW,
  });
  assert.equal(
    garbageTs.fail,
    false,
    'an unparseable ts never becomes a phantom 0 skew',
  );
  assert.equal(garbageTs.reason, 'empty-arming');
  assert.equal(garbageTs.tsA, null, 'the unparseable-ts leg reads as no rows');
});

// EMPTY GRACE WINDOW: armed and the populated leg ADVANCED, but the emptiness has NOT yet lasted longer
// than threshold*1000 ms → NOT a fail (the grace window absorbs a brief one-sided quiet before the
// empty leg's first row). MUTATION: drop the `emptyLongEnough` conjunct (fire the instant the populated
// leg advances, no grace) → this within-grace case false-FAILs and the `fail === false` assertion fails.
test('stagnationDecision: one leg empty, populated leg advanced but WITHIN the grace window → non-fail', () => {
  const t = 7200; // grace = 7_200_000 ms
  const armAt = NOW - 60_000; // armed only 60s ago — well within the 2h grace
  const prev = {
    tsA: 1_700_000_000,
    tsB: null,
    skew: null,
    emptySinceA: null,
    emptySinceB: { atMs: armAt },
    wedgeFailedSince: null,
  };
  const r = stagnationDecision({
    maxA: '12500',
    tsA: 1_700_050_000, // populated leg ADVANCED since prev …
    maxB: null,
    tsB: null, // … but leg B is still empty and the grace window has NOT elapsed
    threshold: t,
    prev,
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    false,
    'within the grace window an advancing other leg is not yet a wedge',
  );
  assert.equal(r.reason, 'empty-arming');
  assert.equal(
    r.nextState.emptySinceB.atMs,
    armAt,
    'the per-leg B arm time is preserved (not re-armed)',
  );
});

// ── DELTA-3 (ruling D1–D3): the unified evidence record kills the SHAPE-flip evidence-laundering bug ──
//
// Round-3 adversarial review found the two-shape state machine still laundered wedge evidence, now
// across STATE SHAPES (empty↔populated flips) and via advancement-blind labels. These tests pin the
// exact holes with the review's sequences; each is mutation-verified (the mutation named inline).

// A tiny driver: thread the previous run's nextState into the next run, exactly as the differ persists.
const runSeq =
  (t) =>
  (tsA, tsB, prev, nowMs = NOW) =>
    stagnationDecision({
      maxA: tsA === null ? null : String(tsA),
      tsA,
      maxB: tsB === null ? null : String(tsB),
      tsB,
      threshold: t,
      prev,
      nowMs,
    });

// D4(a) — HOLE 1: the full 5-run empty↔populated oscillation MUST FAIL at r3, r4, r5. The stuck leg B
// oscillates empty ↔ one-stale-row (tsB frozen at 1_700_000_000) while leg A advances every run. The
// pre-fix machine FAILed once (r2) then re-armed on every shape flip and read non-fail forever. Under
// D1+D2 the wedge is sticky and B never advances past its last-known ts, so r3/r4/r5 stay FAIL.
// MUTATION 1 (revert D2 sticky-wedge — drop the `prevWedgeFailedSince !== null && !genuinelyRecovered`
// short-circuit in BOTH populated and empty branches): r3 re-derives regime from the fresh both-
// populated observation, finds prev without a usable baseline for B's freeze vs a stale row, and reads
// non-fail → the `r3.fail === true` assertion fails.
// MUTATION 2 (revert D1 carry-forward — set `carriedB = b` even when b is null): r4's empty run loses
// B's last-known ts, the recovery/advancement check has no B baseline, and the machine mislabels →
// the r4 fail/reason assertions fail.
test('DELTA-3 D4(a): empty↔populated oscillation — one leg stuck — FAILs at r3, r4, r5 (sticky wedge)', () => {
  const t = 7200; // grace/threshold
  const run = runSeq(t);

  // r1: A=1_700_000_000, B empty → first one-sided emptiness → arm (non-fail)
  const r1 = run(1_700_000_000, null, null);
  assert.equal(
    r1.fail,
    false,
    'r1 arms the one-sided emptiness, no fail at first sight',
  );
  assert.equal(r1.reason, 'empty-arming');
  assert.deepEqual(r1.nextState.emptySinceB, { atMs: NOW });
  assert.equal(r1.nextState.emptySinceA, null);

  // r2: A advanced to 1_700_050_000, B STILL empty and now past the grace window → one-sided-empty FAIL.
  // Force the grace to have elapsed by back-dating the arm through prev (emptySinceB.atMs > threshold ago).
  const r2prev = {
    ...r1.nextState,
    emptySinceB: { atMs: NOW - (t * 1000 + 60_000) },
  };
  const r2 = run(1_700_050_000, null, r2prev);
  assert.equal(
    r2.fail,
    true,
    'r2: B empty past grace while A advances is a one-sided wedge → FAIL',
  );
  assert.equal(r2.reason, 'one-sided-empty');
  assert.equal(r2.staleSide, 'B');
  assert.ok(
    Number.isFinite(r2.nextState.wedgeFailedSince),
    'r2 arms the sticky wedge',
  );

  // r3: B writes ONE STALE row tsB=1_700_000_000 (frozen), A=1_700_100_000 → both populated, skew huge.
  // Pre-fix: the both-populated branch saw prev in "the empty shape" and re-armed → non-fail. Now: the
  // sticky gate runs FIRST; B (the carried wedge stale leg) did NOT advance past its last-known ts → no
  // recovery → wedge-unrecovered FAIL. N2: originalReason is the TRUE first-fail reason (one-sided-empty
  // from r2), not a reason re-derived from r3's both-populated shape.
  const r3 = run(1_700_100_000, 1_700_000_000, r2.nextState);
  assert.equal(
    r3.fail,
    true,
    'r3: a single stale B row does not recover the wedge → FAIL',
  );
  assert.equal(r3.reason, 'wedge-unrecovered');
  assert.equal(
    r3.originalReason,
    'one-sided-empty',
    'the true first-fail reason (r2 one-sided-empty) is carried',
  );
  assert.equal(
    r3.staleSide,
    'B',
    'B (the carried wedge stale leg) is still the stalled side',
  );
  assert.ok(
    Number.isFinite(r3.nextState.wedgeFailedSince),
    'the wedge stays armed through r3',
  );

  // r4: B empty again, A=1_700_150_000 → one leg empty. Pre-fix: the one-empty branch saw prev without
  // emptySide and re-armed firstEmptyAtMs → non-fail. Now: sticky gate FAILs an empty run (no finite
  // both-populated recovery evidence) → wedge-unrecovered.
  const r4 = run(1_700_150_000, null, r3.nextState);
  assert.equal(
    r4.fail,
    true,
    'r4: B empty again does not recover the wedge → FAIL',
  );
  assert.equal(r4.reason, 'wedge-unrecovered');
  assert.equal(
    r4.originalReason,
    'one-sided-empty',
    'the carried first-fail reason stays one-sided-empty',
  );
  assert.equal(r4.staleSide, 'B');

  // r5: repeats r3 (B one stale row, A advances) → still FAIL. The oscillation NEVER launders the wedge.
  const r5 = run(1_700_200_000, 1_700_000_000, r4.nextState);
  assert.equal(
    r5.fail,
    true,
    'r5: the oscillation still FAILs — the wedge is never laundered',
  );
  assert.equal(r5.reason, 'wedge-unrecovered');
});

// D4(b) — HOLE 2: BOTH-FROZEN over threshold must NOT read 'lagging-constant' (whose comment claims
// "both advanced"). r2 is IDENTICAL to r1 (neither leg advanced) with an over-threshold skew → this is
// a mutual freeze, honestly 'mutually-quiescent', non-fail. MUTATION (route the neither-advanced case
// to 'lagging-constant' — drop the `neitherAdvanced` branch): the `reason === 'mutually-quiescent'`
// assertion fails, exposing the dishonest label.
test('DELTA-3 D4(b): both legs FROZEN over threshold → mutually-quiescent (non-fail), NOT lagging-constant', () => {
  const t = 7200;
  const run = runSeq(t);
  // r1 arms: A older by 20000, over threshold, no prev → arm
  const r1 = run(1_700_000_000, 1_700_020_000, null);
  assert.equal(r1.fail, false);
  assert.equal(r1.reason, 'skew-above-threshold-arming');
  // r2 IDENTICAL to r1 — neither leg advanced, skew unchanged at 20000 → mutual freeze, honest non-fail
  const r2 = run(1_700_000_000, 1_700_020_000, r1.nextState);
  assert.equal(
    r2.fail,
    false,
    'a mutual freeze is out of scope for an A-vs-B divergence guard',
  );
  assert.equal(
    r2.reason,
    'mutually-quiescent',
    'neither leg advanced → mutually-quiescent, never lagging-constant (which implies both advanced)',
  );
  assert.equal(r2.staleSide, null);
});

// D4(c) — LEADER-REGRESSION + FROZEN-STALE: the leader leg (A) REGRESSES newest-ts (a one-sided realtime
// reorg-prune) while the stale leg (B) stays frozen — the skew SHRINKS but NOBODY advanced. This must be
// 'mutually-quiescent' (non-fail), NOT 'catching-up' (which would launder a frozen stale leg on a leader
// artefact). THEN the next run, the leader re-advances while the stale leg is STILL frozen → the honest
// 'frozen' FAIL surfaces. MUTATION (the naive "catching-up on any skew shrink" — replace the
// `neitherAdvanced → mutually-quiescent` + `staleAdvanced && skewDecreased → catching-up` pair with a
// single leading `else if (skewDecreased) reason = 'catching-up'`): the leader reorg-prune shrinks the
// skew with nobody advancing, so r2 mislabels 'catching-up' and the `reason === 'mutually-quiescent'`
// assertion fails. Two barriers protect this — the `neitherAdvanced` branch order AND the
// `staleAdvanced` conjunct — so the discriminating mutation drops BOTH (the anti-pattern D3 forbids).
test('DELTA-3 D4(c): leader-regression + frozen stale → mutually-quiescent, then re-advance → frozen FAIL', () => {
  const t = 7200;
  const run = runSeq(t);
  // r1 arms: A leader at 1_700_100_000, B stale/frozen at 1_700_000_000; skew 100000 > threshold
  const r1 = run(1_700_100_000, 1_700_000_000, null);
  assert.equal(r1.fail, false);
  assert.equal(r1.reason, 'skew-above-threshold-arming');
  // r2: A REGRESSES to 1_700_050_000 (reorg-prune of newest), B unchanged at 1_700_000_000. Skew shrank
  // 100000 → 50000, but NEITHER leg advanced (A regressed, B frozen) → mutually-quiescent, non-fail.
  const r2 = run(1_700_050_000, 1_700_000_000, r1.nextState);
  assert.equal(
    r2.fail,
    false,
    'a leader ts regression that shrinks the skew is not recovery',
  );
  assert.equal(
    r2.reason,
    'mutually-quiescent',
    'nobody advanced — a leader reorg-prune is NOT catching-up',
  );
  // r3: leader A RE-ADVANCES to 1_700_120_000, stale B STILL frozen at 1_700_000_000 → one-sided wedge,
  // the honest 'frozen' FAIL now surfaces (the one-run delay discriminated).
  const r3 = run(1_700_120_000, 1_700_000_000, r2.nextState);
  assert.equal(
    r3.fail,
    true,
    'once the leader re-advances the frozen stale leg is a wedge → FAIL',
  );
  assert.equal(r3.reason, 'frozen');
  assert.equal(r3.staleSide, 'B');
});

// D4(d) — STICKY-FAIL RECOVERY (D2): from a FAILed wedge, verify each recovery predicate.
// MUTATION (drop the recovery clearing — never reset wedgeFailedSince to null): the genuine-recovery
// assertions (fail === false) fail, since the wedge would stay stuck even after real recovery.
test('DELTA-3 D4(d): sticky-fail recovery — advance+shrink clears, under-threshold clears, static does NOT', () => {
  const t = 7200;
  const run = runSeq(t);
  // Reach a FAILed frozen wedge: r1 arms (B stale), r2 A advances while B frozen → FAIL frozen.
  const r1 = run(1_700_100_000, 1_700_000_000, null);
  const r2 = run(1_700_150_000, 1_700_000_000, r1.nextState);
  assert.equal(r2.fail, true, 'precondition: a frozen wedge is live');
  assert.ok(Number.isFinite(r2.nextState.wedgeFailedSince));

  // RECOVERY 1 — the stale leg (B) STRICTLY ADVANCES and the skew SHRINKS (skew must not grow): clears.
  // B 1_700_000_000 → 1_700_120_000 (advanced), A 1_700_150_000 → 1_700_160_000; skew 150000 → 40000.
  const rec1 = run(1_700_160_000, 1_700_120_000, r2.nextState);
  assert.equal(
    rec1.fail,
    false,
    'the stale leg advancing with a shrinking skew is genuine recovery',
  );
  assert.equal(rec1.reason, 'catching-up');
  assert.equal(
    rec1.nextState.wedgeFailedSince,
    null,
    'genuine recovery clears the sticky wedge',
  );

  // RECOVERY 2 — from a fresh FAILed wedge, the skew drops UNDER threshold → clears regardless of motion.
  const w2 = run(1_700_150_000, 1_700_000_000, r1.nextState); // FAIL again (frozen)
  assert.equal(w2.fail, true);
  const rec2 = run(1_700_150_000, 1_700_149_000, w2.nextState); // skew 1000 < threshold
  assert.equal(
    rec2.fail,
    false,
    'skew back under threshold is a genuine recovery',
  );
  assert.equal(rec2.reason, 'both-populated-in-skew');
  assert.equal(
    rec2.nextState.wedgeFailedSince,
    null,
    'under-threshold clears the wedge',
  );

  // NON-RECOVERY — a STATIC reappearance (the stale leg reappears with the SAME frozen ts, over
  // threshold): NOT advancing → the wedge stays FAIL. This is the oscillation's core: static ≠ recovery.
  const w3 = run(1_700_150_000, 1_700_000_000, r1.nextState); // FAIL (frozen)
  assert.equal(w3.fail, true);
  const stat = run(1_700_200_000, 1_700_000_000, w3.nextState); // B unchanged, A advanced
  assert.equal(
    stat.fail,
    true,
    'a static (unchanged) stale leg is NOT recovery — the wedge holds FAIL',
  );
  assert.equal(stat.reason, 'wedge-unrecovered');
});

// D4(e) — SHAPE TOGGLES PRESERVE EVIDENCE (D1 invariant): a run where one leg goes empty must carry the
// OTHER leg's last-known ts, the emptiness arm, and any live wedge THROUGH the empty run — no field is
// dropped on a shape transition. MUTATION (drop carry-forward — `carriedA = a`, `carriedB = b` with no
// prev fallback): the empty run's nextState loses the empty leg's carried ts and the deepEqual fails.
test('DELTA-3 D4(e): a shape toggle (populated → one-empty) carries every evidence field through', () => {
  const t = 7200;
  const run = runSeq(t);
  // r1: both populated, over threshold, A older → arm. nextState carries tsA/tsB/skew.
  const r1 = run(1_700_000_000, 1_700_020_000, null);
  assert.equal(r1.reason, 'skew-above-threshold-arming');
  assert.equal(r1.nextState.tsA, 1_700_000_000);
  assert.equal(r1.nextState.tsB, 1_700_020_000);

  // r2: leg A goes EMPTY, leg B still populated (advanced). D1 carry-forward: A's last-known ts
  // (1_700_000_000) is preserved in nextState even though A is empty this run; B's live ts is recorded;
  // emptySince arms for side A.
  const r2 = run(null, 1_700_030_000, r1.nextState);
  assert.equal(
    r2.nextState.tsA,
    1_700_000_000,
    'the now-empty leg A keeps its last-known ts (carry-forward)',
  );
  assert.equal(
    r2.nextState.tsB,
    1_700_030_000,
    'the live leg B records its current ts',
  );
  assert.deepEqual(
    r2.nextState.emptySinceA,
    { atMs: NOW },
    'per-leg emptiness arms for the empty leg A',
  );
  assert.equal(
    r2.nextState.emptySinceB,
    null,
    'the populated leg B arms no emptiness',
  );

  // r3: A REAPPEARS still frozen at its carried ts while B advanced past grace — because A's evidence
  // survived the empty run, the machine can still judge A frozen. Here A reappears at 1_700_000_000
  // (unchanged from its carried ts) with B advancing → the frozen wedge is detectable, not laundered.
  const r3prev = {
    ...r2.nextState,
    emptySinceA: { atMs: NOW - (t * 1000 + 60_000) },
  };
  // A empty one more run past grace while B advances → one-sided-empty FAIL (A carried, never advanced).
  const r3 = run(null, 1_700_060_000, r3prev);
  assert.equal(
    r3.fail,
    true,
    'A empty past grace while B advances is a wedge — evidence survived the toggle',
  );
  assert.equal(r3.reason, 'one-sided-empty');
  assert.equal(r3.staleSide, 'A');
  assert.equal(
    r3.nextState.tsA,
    1_700_000_000,
    'A last-known ts still carried through the second empty run',
  );
});

// ── DELTA-4 (adversarial review round 4): the sticky-wedge gate is the FIRST branch — no shape branch ──
// can bypass stickiness by construction. Each test encodes the review's EXACT reproduction sequence and
// names its discriminating mutation inline.

// D5(a) — F1 (High BLOCKER): a sticky wedge must NOT clear on a one-empty run when the WIPED leg was
// last-known AHEAD. The frozen leg is the NEWER one (A); B is older but advancing. When A is wiped, the
// OLD code recomputed olderSide=B, checked B's advancement (true), skew was null so skew-grew was false,
// declared genuine recovery, and the empty branch fresh-armed — reopening a grace window on a proven
// wedge. The restructure reads the CARRIED wedge stale side (A) and never clears on an empty run.
// MUTATION (revert to recomputed-olderSide recovery in the sticky gate — replace `prevWedgeStaleSide`
// with a per-run olderSide from carried ts, and drop the `bothPopulated` recovery precondition): r3's
// empty run recomputes olderSide=B, sees B advanced, clears the wedge → the `r3.fail === true` assertion
// fails (it reads empty-arming, non-fail).
test('DELTA-4 D5(a) F1: sticky wedge does NOT clear on a one-empty run where the wiped leg was ahead', () => {
  const t = 7200;
  const run = runSeq(t);

  // r1: A=100000, B=80000 → A leader, skew 20000 > threshold, no prev → arm.
  const r1 = run(100_000, 80_000, null);
  assert.equal(r1.fail, false);
  assert.equal(r1.reason, 'skew-above-threshold-arming');

  // r2: A=100000 (FROZEN — the newer leg stopped), B=85000 (advanced). One-sided wedge → FAIL frozen,
  // staleSide = A (the frozen leg, N1). The carried wedge stale side is A even though A is the NEWER leg.
  const r2 = run(100_000, 85_000, r1.nextState);
  assert.equal(
    r2.fail,
    true,
    'r2: A frozen while B advances is a one-sided wedge',
  );
  assert.equal(r2.reason, 'frozen');
  assert.equal(
    r2.staleSide,
    'A',
    'the FROZEN (newer) leg A is named stalled, not the older leg B (N1)',
  );
  assert.equal(
    r2.nextState.wedgeStaleSide,
    'A',
    'the wedge stale side A is locked in',
  );

  // r3: A=null (WIPED while its carried ts 100000 is still AHEAD of B), B=90000. The sticky gate runs
  // first; an empty run presents no both-populated recovery evidence → wedge-unrecovered FAIL. The old
  // recomputed-olderSide path would have flipped the recovery check to B and fresh-armed. This is F1.
  const r3 = run(null, 90_000, r2.nextState);
  assert.equal(
    r3.fail,
    true,
    'r3: a one-empty run cannot clear the sticky wedge — F1 closed',
  );
  assert.equal(r3.reason, 'wedge-unrecovered');
  assert.equal(
    r3.staleSide,
    'A',
    'the carried wedge stale leg A stays named through the empty run',
  );
  assert.equal(
    r3.nextState.wedgeFailedSince,
    r2.nextState.wedgeFailedSince,
    'the wedge episode is preserved',
  );

  // r4: A still empty, B advances further → still no recovery evidence → FAIL (never a fresh grace window).
  const r4 = run(null, 95_000, r3.nextState);
  assert.equal(
    r4.fail,
    true,
    'r4: the wedge stays FAIL — no fail-open grace window reopened',
  );
  assert.equal(r4.reason, 'wedge-unrecovered');
});

// D5(b) — F2 (Med): a both-empty TRANSIENT must NOT clear an active sticky wedge for even one run. The
// mutual-freeze / both-down exemption applies ONLY when NOT already wedged. MUTATION (move the both-empty
// branch ABOVE the sticky gate, or add a both-empty escape inside the gate that clears the wedge): r3's
// both-empty run clears the wedge → `r3.fail === true` fails.
test('DELTA-4 D5(b) F2: a both-empty transient does NOT clear an active sticky wedge', () => {
  const t = 7200;
  const run = runSeq(t);

  // r1: A=100000, B=80000 → arm.
  const r1 = run(100_000, 80_000, null);
  assert.equal(r1.reason, 'skew-above-threshold-arming');

  // r2: A=130000 (advanced), B=80000 (FROZEN) → one-sided wedge FAIL frozen, staleSide B.
  const r2 = run(130_000, 80_000, r1.nextState);
  assert.equal(r2.fail, true, 'r2: B frozen while A advances → FAIL frozen');
  assert.equal(r2.reason, 'frozen');
  assert.equal(r2.staleSide, 'B');

  // r3: A=null, B=null → both empty WITH a live wedge. The sticky gate runs first; an empty run has no
  // both-populated recovery evidence → wedge-unrecovered FAIL. The both-empty exemption is out of reach.
  const r3 = run(null, null, r2.nextState);
  assert.equal(
    r3.fail,
    true,
    'r3: a both-empty transient cannot clear an active wedge — F2 closed',
  );
  assert.equal(r3.reason, 'wedge-unrecovered');
  assert.ok(
    Number.isFinite(r3.nextState.wedgeFailedSince),
    'the wedge stays armed through the both-empty run',
  );

  // r4: A=160000, B=80000 → B still frozen, no recovery → still FAIL (the wedge never laundered).
  const r4 = run(160_000, 80_000, r3.nextState);
  assert.equal(r4.fail, true, 'r4: B still frozen → the wedge holds FAIL');
  assert.equal(r4.reason, 'wedge-unrecovered');
});

// D5(c) — F3 (Med): a null prev skew after an empty-run gap must NOT coerce to 0 and manufacture a false
// 'skew-growing' FAIL. Both legs advance and the last-known gap SHRINKS across the gap; the run must not
// hard-fail skew-growing. MUTATION (replace the null-safe `skewGrew`/`skewGrewFrom` at the fresh-decision
// site with `const skewIncreased = skew > prevSkew`): the null prevSkew coerces to 0, any positive
// over-threshold skew reads skew-growing, and the `r3.reason !== 'skew-growing'` assertion fails.
test('DELTA-4 D5(c) F3: null prev skew after an empty gap does NOT false-FAIL skew-growing', () => {
  const t = 7200;
  const run = runSeq(t);

  // r1: A=100000, B=80000 → arm, skew 20000. (No wedge; an arming run does not fail.)
  const r1 = run(100_000, 80_000, null);
  assert.equal(r1.reason, 'skew-above-threshold-arming');
  assert.equal(r1.nextState.skew, 20_000);

  // r2: A=101000, B=null → one leg empty, prev has no wedge → empty-arming. nextState carries tsA=101000,
  // tsB=80000 (carried), but skew=null (unmeasurable) — the source of the null prev skew for r3.
  const r2 = run(101_000, null, r1.nextState);
  assert.equal(r2.reason, 'empty-arming');
  assert.equal(
    r2.nextState.skew,
    null,
    'skew is unmeasurable across the empty run',
  );

  // r3: A=102000, B=82000 → both advanced, the last-known gap shrank 21000 → 20000. prevSkew is null;
  // the null-safe helper treats that as UNKNOWN (not 0), so this is NOT skew-growing. Both advanced with
  // a flat/shrinking skew → a benign lag, never a false FAIL.
  const r3 = run(102_000, 82_000, r2.nextState);
  assert.equal(
    r3.fail,
    false,
    'r3: a null prev skew is unknown, not zero — no false skew-growing FAIL (F3)',
  );
  assert.notEqual(
    r3.reason,
    'skew-growing',
    'the null prev skew never coerces to a skew-growing FAIL',
  );
});

// D5(d) — F5 (Low): alternating-empty-SIDES must NOT re-arm the emptiness timer forever. Per-leg
// emptySince timers mean a flip on the OTHER leg no longer resets THIS leg's timer. Here leg A's timer,
// once armed, survives leg B's empty runs. (Accepted residual: strict alternation where each leg
// advances whenever observed does not fail — both legs demonstrably advance; the windowed diff owns
// divergence. This test pins the TIMER-PRESERVATION mechanism, not a fail.) MUTATION (revert to a single
// shared `emptySince` reset on any side flip): leg A's timer is reset on r2's B-empty run, so r3's
// emptySinceA.atMs would be re-armed to NOW instead of preserved from r1.
test('DELTA-4 D5(d) F5: per-leg emptiness timers survive a flip on the other leg (no forever re-arm)', () => {
  const t = 7200;
  const run = runSeq(t);
  const armA = NOW - 5_000_000; // leg A armed empty in the past

  // Start with leg A empty since armA (past arm), leg B populated. prev has A's per-leg timer set.
  const prev = {
    tsA: null,
    tsB: 100_000,
    skew: null,
    emptySinceA: { atMs: armA },
    emptySinceB: null,
    wedgeFailedSince: null,
  };

  // r-flip: leg B goes empty, leg A populated. B arms its OWN timer at NOW; A's timer clears (A populated).
  const rFlip = run(90_000, null, prev);
  assert.equal(
    rFlip.nextState.emptySinceA,
    null,
    'A is populated this run → its timer clears',
  );
  assert.deepEqual(
    rFlip.nextState.emptySinceB,
    { atMs: NOW },
    'B arms its OWN per-leg timer, not a shared one',
  );

  // Now A goes empty again while B populated: A re-arms fresh (it WAS populated last run — a genuine new
  // empty episode), and B's timer clears. The point: a flip does not corrupt the OTHER leg's live timer.
  const rBack = run(null, 110_000, rFlip.nextState);
  assert.deepEqual(
    rBack.nextState.emptySinceA,
    { atMs: NOW },
    'A re-arms on a genuinely new empty episode',
  );
  assert.equal(
    rBack.nextState.emptySinceB,
    null,
    'B is populated this run → its timer clears',
  );

  // The preservation case: leg A stays empty across a run — its timer is PRESERVED, never re-armed.
  const rStay = run(null, 120_000, {
    ...rBack.nextState,
    emptySinceA: { atMs: armA },
  });
  assert.equal(
    rStay.nextState.emptySinceA.atMs,
    armA,
    'a leg that STAYS empty preserves its arm time (F5)',
  );
});

// D5(e) — N1 attribution on a FRESH frozen FAIL: staleSide must name the leg that FAILED TO ADVANCE, not
// the older leg. A newer+frozen leg (A) vs an older+advancing leg (B) → FAIL frozen with staleSide 'A'.
// MUTATION (attribute staleSide from olderSide instead of per-leg advancement): staleSide reads 'B' (the
// older leg) and the `staleSide === 'A'` assertion fails.
test('DELTA-4 D5(e) N1: a fresh frozen FAIL names the FROZEN leg, not the older leg', () => {
  const t = 7200;
  const run = runSeq(t);
  // r1 arms: A newer (100000) but will freeze; B older (80000) but will advance.
  const r1 = run(100_000, 80_000, null);
  assert.equal(r1.reason, 'skew-above-threshold-arming');
  // r2: A FROZEN at 100000 (newer leg stopped), B advanced 80000 → 85000 (older leg moving). The FROZEN
  // leg is A even though A is newer. N1: staleSide must be A.
  const r2 = run(100_000, 85_000, r1.nextState);
  assert.equal(r2.fail, true);
  assert.equal(r2.reason, 'frozen');
  assert.equal(
    r2.staleSide,
    'A',
    'the FROZEN leg (A) is named, not the older leg (B) — N1',
  );
  assert.equal(r2.nextState.wedgeStaleSide, 'A');
});

// D5(f) — N2: the sticky originalReason is the TRUE first-fail reason, not a per-branch hardcode. Two of
// Codex's reproduced mismatches: (1) first a skew-growing fail, then it goes empty → the sticky return
// must still say skew-growing (old code hardcoded 'one-sided-empty' on the empty branch). (2) first a
// one-sided-empty fail, then both populated → must still say one-sided-empty (old code hardcoded
// 'frozen'). MUTATION (revert to hardcoded per-branch originalReason): each carried-reason assertion fails.
test('DELTA-4 D5(f) N2: sticky originalReason is the true first-fail reason across shape flips', () => {
  const t = 7200;
  const run = runSeq(t);

  // Case 1: FIRST fail is skew-growing (both populated, both advance, gap widens), THEN one leg empties.
  const c1r1 = run(100_000, 80_000, null); // arm, skew 20000
  const c1r2 = run(140_000, 85_000, c1r1.nextState); // both advanced, skew 20000→55000 → skew-growing FAIL
  assert.equal(c1r2.fail, true);
  assert.equal(c1r2.reason, 'skew-growing');
  assert.equal(
    c1r2.nextState.wedgeReason,
    'skew-growing',
    'the first-fail reason is locked as skew-growing',
  );
  const c1r3 = run(150_000, null, c1r2.nextState); // now B empties → sticky wedge, still unrecovered
  assert.equal(c1r3.fail, true);
  assert.equal(c1r3.reason, 'wedge-unrecovered');
  assert.equal(
    c1r3.originalReason,
    'skew-growing',
    'the empty run still reports the skew-growing origin (N2)',
  );

  // Case 2: FIRST fail is one-sided-empty, THEN both legs populated (stale leg not advancing).
  const c2prev = {
    tsA: 100_000,
    tsB: null,
    skew: null,
    emptySinceA: null,
    emptySinceB: { atMs: NOW - (t * 1000 + 60_000) },
    wedgeFailedSince: null,
  };
  const c2r1 = run(150_000, null, c2prev); // B empty past grace, A advanced → one-sided-empty FAIL
  assert.equal(c2r1.fail, true);
  assert.equal(c2r1.reason, 'one-sided-empty');
  assert.equal(
    c2r1.nextState.wedgeReason,
    'one-sided-empty',
    'the first-fail reason is locked as one-sided-empty',
  );
  const c2r2 = run(200_000, 80_000, c2r1.nextState); // B reappears frozen, A advances → still unrecovered
  assert.equal(c2r2.fail, true);
  assert.equal(c2r2.reason, 'wedge-unrecovered');
  assert.equal(
    c2r2.originalReason,
    'one-sided-empty',
    'the both-populated run still reports the empty origin (N2)',
  );
});

// ── review delta 5 (final round) — the three adversarial-review sequences ──────────────────────────────

// DELTA-5 finding 1 (High, the only code-semantics fix): skew-growth was judged from `prev.skew`, which
// is null after ANY empty-run gap — so a genuinely GROWN last-known gap was INVISIBLE (fail-open) in BOTH
// the sticky-recovery gate (`skewGrew`) and the fresh-decision (`skewIncreased`). Fix: derive growth from
// the CARRIED timestamps (which persist through empty gaps) via null-safe gap()/gapGrewFrom().
//
// SEQUENCE A (sticky gate, verbatim from the adversarial review): a live wedge must NOT clear when the
// carried gap grew across an empty-run gap that nulled prev.skew, even though the stale leg ticked +1.
// MUTATION (revert the comparison basis to prev.skew — `const gapGrew = skewGrewFrom(prevSkew, skew)`):
// prevSkew is null → gapGrew false → wedgeStaleAdvanced && !gapGrew clears the wedge → r3 reads
// `PASS lagging-constant` and the `r3.fail === true` / `r3.reason === 'wedge-unrecovered'` asserts fail.
test('DELTA-5 finding 1A: sticky wedge does NOT clear when the carried gap grew across an empty-run gap (null prev.skew)', () => {
  const t = 7200;
  const run = runSeq(t);

  // r1: A=100000, B=80000, prev=null → arm, skew 20000 (> threshold).
  const r1 = run(100_000, 80_000, null);
  assert.equal(r1.fail, false);
  assert.equal(r1.reason, 'skew-above-threshold-arming');
  assert.equal(r1.nextState.skew, 20_000);

  // r2: A=200000 (advanced), B=null with emptySinceB BACKDATED past the grace window → FAIL
  // one-sided-empty. The prior gap at the end of r2 = |carriedA 200000 − carriedB 80000| = 120000.
  const r2prev = {
    ...r1.nextState,
    emptySinceB: { atMs: NOW - (t * 1000 + 60_000) },
  };
  const r2 = run(200_000, null, r2prev);
  assert.equal(
    r2.fail,
    true,
    'r2: B empty past grace, A advanced → one-sided-empty FAIL',
  );
  assert.equal(r2.reason, 'one-sided-empty');
  assert.equal(r2.nextState.wedgeStaleSide, 'B');
  assert.equal(r2.nextState.tsA, 200_000, 'A carried');
  assert.equal(
    r2.nextState.tsB,
    80_000,
    'B last-known carried through the empty run',
  );
  assert.equal(
    r2.nextState.skew,
    null,
    'skew unmeasurable across the empty run → prev.skew null for r3',
  );

  // r3: A=300000, B=80001 → B (the carried wedge stale leg) advanced by ONLY 1, but the carried gap GREW
  // from 120000 to |300000 − 80001| = 219999. prev.skew is null (from r2), so the OLD basis saw "not
  // growing" and cleared the wedge — the fail-open. The carried-gap basis sees the gap grew → NO recovery.
  const r3 = run(300_000, 80_001, r2.nextState);
  assert.equal(
    r3.fail,
    true,
    'r3: carried gap grew (120000→219999) despite B ticking +1 → wedge NOT recovered (finding 1 fail-open closed)',
  );
  assert.equal(
    r3.reason,
    'wedge-unrecovered',
    'the live sticky wedge stays FAIL — it did not clear without genuine recovery',
  );
  assert.equal(r3.staleSide, 'B', 'the carried wedge stale leg B stays named');
});

// SEQUENCE B (fresh decision, verbatim from the adversarial review): after an empty-run gap nulls
// prev.skew, a fresh over-threshold run whose carried gap GREW must FAIL skew-growing — the old
// `prev.skew`-based `skewIncreased` was null-blind and read non-fail.
// MUTATION (same revert — `const gapGrew = skewGrewFrom(prevSkew, skew)`): r3 reads `PASS
// lagging-constant` and the `r3.fail === true` / `r3.reason === 'skew-growing'` asserts fail.
test('DELTA-5 finding 1B: fresh run FAILs skew-growing when the carried gap grew across an empty-run gap (null prev.skew)', () => {
  const t = 7200;
  const run = runSeq(t);

  // r1: A=100000, B=80000 → arm, skew 20000.
  const r1 = run(100_000, 80_000, null);
  assert.equal(r1.reason, 'skew-above-threshold-arming');

  // r2: A=110000, B=null → empty-arming (no wedge). Carried last-known gap = |110000 − 80000| = 30000;
  // skew=null (unmeasurable), the source of the null prev.skew for r3.
  const r2 = run(110_000, null, r1.nextState);
  assert.equal(r2.reason, 'empty-arming');
  assert.equal(
    r2.nextState.skew,
    null,
    'skew unmeasurable across the empty run',
  );
  assert.equal(r2.nextState.tsA, 110_000);
  assert.equal(r2.nextState.tsB, 80_000, 'B last-known carried');

  // r3: A=200000, B=81000 → both advanced, but the carried gap GREW from 30000 to |200000 − 81000| =
  // 119000. prev.skew is null; the OLD basis coerced "no growth" and read PASS. The carried-gap basis
  // sees the gap grew → FAIL skew-growing (the trickling wedge losing ground).
  const r3 = run(200_000, 81_000, r2.nextState);
  assert.equal(
    r3.fail,
    true,
    'r3: carried gap grew (30000→119000) across the empty gap → FAIL (finding 1 fail-open closed)',
  );
  assert.equal(
    r3.reason,
    'skew-growing',
    'a widening carried gap is a skew-growing wedge, not a benign lagging-constant',
  );
});

// DELTA-5 finding 1C (regression guard): the delta-4 F3 case — a null prev.skew after an empty gap where
// the carried gap genuinely SHRINKS — must still NOT false-FAIL skew-growing under the new carried-gap
// basis. This pins that finding 1's fix does not reopen F3. (Mirror of D5(c) above, re-asserted against
// the carried-gap comparison.)
test('DELTA-5 finding 1C: null prev.skew with a SHRINKING carried gap does NOT false-FAIL (F3 preserved)', () => {
  const t = 7200;
  const run = runSeq(t);

  const r1 = run(100_000, 80_000, null); // arm, gap 20000
  assert.equal(r1.reason, 'skew-above-threshold-arming');
  const r2 = run(101_000, null, r1.nextState); // B empty → carried gap 21000, skew null
  assert.equal(r2.reason, 'empty-arming');
  const r3 = run(102_000, 82_000, r2.nextState); // both advance, carried gap 21000→20000 (SHRANK)
  assert.equal(
    r3.fail,
    false,
    'r3: a shrinking carried gap after a null prev.skew is NOT a fail (F3 preserved)',
  );
  assert.notEqual(
    r3.reason,
    'skew-growing',
    'a null prev.skew with a shrinking carried gap is never skew-growing',
  );
});

// DELTA-5 finding 2 (High, SUSTAINED as fail-closed — PINNING test): a genuine catch-up run recovers a
// sticky wedge in the gate, then the fresh predicate IMMEDIATELY names the (now-frozen) former LEADER as
// a new `frozen` wedge. This is CONSCIOUSLY CORRECT at the hourly cadence: a leg whose newest-row ts did
// not advance across a full inter-run interval has genuinely stopped persisting, so N1 names the truly-
// frozen leg and this is an honest NEW episode (fresh wedgeFailedSince), not laundering. This test PINS
// the exact behavior so any future change to it is a deliberate decision (there is no mutation — the
// disposition is "keep as-is"; the assert IS the pin).
test('DELTA-5 finding 2: leader frozen while the stale leg catches up FAILs frozen naming the leader (fail-closed, pinned)', () => {
  const t = 7200;
  const run = runSeq(t);

  // r1: A=1700100000, B=1700000000, prev=null → arm (skew 100000 > threshold), A leader.
  const r1 = run(1_700_100_000, 1_700_000_000, null);
  assert.equal(r1.reason, 'skew-above-threshold-arming');

  // r2 at NOW (episode 1 opens): A=1700150000 (advanced), B=1700000000 (FROZEN) → one-sided wedge FAIL
  // frozen, staleSide B, skew 150000. The sticky wedge episode opens with B as the carried stale leg.
  const r2 = run(1_700_150_000, 1_700_000_000, r1.nextState, NOW);
  assert.equal(r2.fail, true, 'r2: B frozen while A advances → FAIL frozen');
  assert.equal(r2.reason, 'frozen');
  assert.equal(r2.staleSide, 'B');
  assert.equal(r2.skew, 150_000);
  assert.equal(r2.nextState.wedgeFailedSince, NOW, 'episode 1 opens at NOW');

  // r3 ONE HOUR LATER (the deployment's cadence): A=1700150000 (now the LEADER is FROZEN),
  // B=1700120000 (the stale leg CAUGHT UP; skew shrank 150000→30000). The sticky gate sees B advanced
  // and the carried gap shrank → wedge recovers → falls through. The fresh predicate then sees A frozen
  // while B advanced → FAIL frozen, staleSide A, a FRESH wedgeFailedSince (episode 2). SUSTAINED ruling.
  const laterMs = NOW + 3_600_000;
  const r3 = run(1_700_150_000, 1_700_120_000, r2.nextState, laterMs);
  assert.equal(
    r3.fail,
    true,
    'r3: the former leader A is now frozen across a full interval → FAIL frozen (fail-closed, honest new episode)',
  );
  assert.equal(r3.reason, 'frozen');
  assert.equal(
    r3.staleSide,
    'A',
    'N1: the leg that FAILED TO ADVANCE (A, the former leader) is named — not the older leg',
  );
  assert.equal(
    r3.nextState.wedgeFailedSince,
    laterMs,
    'the fresh wedge arms a NEW wedgeFailedSince at THIS run (episode 2 restarted, not carried from episode 1)',
  );
  assert.notEqual(
    r3.nextState.wedgeFailedSince,
    r2.nextState.wedgeFailedSince,
    'the recovery-then-fresh-wedge is a NEW episode, not the same carried wedgeFailedSince',
  );
});

// DELTA-5 finding 3 (Med, SUSTAINED semantics — PINNING test): a both-empty run arms BOTH per-leg timers,
// so a mutual outage consumes the one-sided-empty grace window. This is the INTENDED F5 semantic and is
// fail-closed (the timer factually measures THAT leg's emptiness duration; a leg still empty past the
// window while the other advances SHOULD alarm). This test PINS the sustained behavior. (The wording of
// the alert was corrected separately to claim only what is known.) No mutation — the disposition is
// "keep"; the assert IS the pin.
test('DELTA-5 finding 3: both-empty arms both timers → the next one-sided-empty run FAILs (fail-closed, pinned)', () => {
  const t = 7200;
  const run = runSeq(t);

  // r0 at NOW: A=100000, B=90000 → arm (skew 10000 > threshold). No emptiness armed.
  const r0 = run(100_000, 90_000, null, NOW);
  assert.equal(r0.reason, 'skew-above-threshold-arming');
  assert.equal(r0.nextState.emptySinceA, null);
  assert.equal(r0.nextState.emptySinceB, null);

  // r1 at NOW: A=null, B=null → both-empty (benign mutual outage, non-fail). But BOTH per-leg timers arm
  // at NOW — the sustained F5 semantic: each timer measures its own leg's emptiness from here.
  const r1 = run(null, null, r0.nextState, NOW);
  assert.equal(r1.fail, false);
  assert.equal(r1.reason, 'both-empty');
  assert.deepEqual(
    r1.nextState.emptySinceA,
    { atMs: NOW },
    'the both-empty run arms A per-leg timer (sustained F5 semantic)',
  );
  assert.deepEqual(
    r1.nextState.emptySinceB,
    { atMs: NOW },
    'the both-empty run arms B per-leg timer (sustained F5 semantic)',
  );

  // r2 at NOW + grace + 1: A=150000 (recovered, advancing), B=null (still empty past the window). B's
  // timer, armed during the both-empty outage, is now past grace → FAIL one-sided-empty, staleSide B.
  // SUSTAINED: the emptiness B factually exhibits is real; a downstream consumer of B sees no rows.
  const later = NOW + t * 1000 + 1;
  const r2 = run(150_000, null, r1.nextState, later);
  assert.equal(
    r2.fail,
    true,
    'r2: B empty past the (both-empty-consumed) grace window while A advances → FAIL one-sided-empty (fail-closed, pinned)',
  );
  assert.equal(r2.reason, 'one-sided-empty');
  assert.equal(r2.staleSide, 'B', 'the empty leg B is named stalled');
});

// ── stagnationAlerts: one loud, self-describing line per stalled chain ───────────────────────────────
//
// The alert layer turns a fired guard into one loud line per stalled chain (naming what was PROVEN: a
// frozen skew, a growing skew, or an empty-leg wedge) and stays SILENT for a passing/absent guard.
test('stagnationAlerts: one line per stalled chain naming the older leg + reason, silent otherwise', () => {
  const results = [
    // chain 1: frozen wedge, leg A stalled
    {
      chain: 1,
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'frozen',
          staleSide: 'A',
          skewSeconds: 10_800,
          thresholdSeconds: 7200,
          maxA: 90,
          maxB: 100,
          tsA: 989_200,
          tsB: 1_000_000,
        },
      },
    },
    // chain 8453: below threshold → PASS, no alert
    {
      chain: 8453,
      classes: {
        persistStagnation: {
          fail: false,
          reason: 'both-populated-in-skew',
          staleSide: null,
          skewSeconds: null,
          thresholdSeconds: 7200,
          maxA: 5,
          maxB: 5,
          tsA: 1,
          tsB: 1,
        },
      },
    },
    // an ERROR result with no persistStagnation contributes nothing
    { chain: 42_161, classes: { error: 'boom' } },
  ];

  const lines = stagnationAlerts(results);
  // MUTATION: drop the `!s?.fail` continue (fire on PASS too) → length becomes 2 and this fails.
  assert.equal(lines.length, 1, 'only the stalled chain emits an alert');
  assert.match(lines[0], /persist-stagnation: chain 1/);
  // MUTATION: name the wrong leg (map 'A' → 'leg B') → this "leg A" substring assertion fails.
  assert.match(lines[0], /leg A has stopped persisting rows/);
  assert.match(lines[0], /FROZEN; skew 10800s > 7200s/);
  assert.match(lines[0], /maxA=90 maxB=100 tsA=989200 tsB=1000000/);

  // a leg-B stall names leg B
  const bLine = stagnationAlerts([
    {
      chain: 5,
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'frozen',
          staleSide: 'B',
          skewSeconds: 9000,
          thresholdSeconds: 7200,
          maxA: 100,
          maxB: 90,
          tsA: 1_000_000,
          tsB: 991_000,
        },
      },
    },
  ]);
  assert.equal(bLine.length, 1);
  assert.match(bLine[0], /leg B has stopped persisting rows/);

  // no results / all-passing → no alerts
  assert.deepEqual(stagnationAlerts([]), []);
});

test('stagnationAlerts: a skew-growing wedge and a one-sided-empty wedge each get a self-describing line', () => {
  const growing = stagnationAlerts([
    {
      chain: 1,
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'skew-growing',
          staleSide: 'B',
          skewSeconds: 12_000,
          thresholdSeconds: 7200,
          maxA: 100,
          maxB: 95,
          tsA: 1_700_000_000,
          tsB: 1_699_988_000,
        },
      },
    },
  ]);
  assert.match(growing[0], /leg B is falling further behind/);
  assert.match(growing[0], /skew GROWING to 12000s > 7200s/);

  const empty = stagnationAlerts([
    {
      chain: 8453,
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'one-sided-empty',
          staleSide: 'B',
          skewSeconds: null,
          thresholdSeconds: 7200,
          maxA: 12_500,
          maxB: null,
          tsA: 1_700_050_000,
          tsB: null,
        },
      },
    },
  ]);
  assert.match(
    empty[0],
    /leg B has persisted NO rows for over 7200s \(empty leg\) and the other leg is advancing/,
  );
  assert.match(empty[0], /maxA=12500 maxB=null tsA=1700050000 tsB=null/);
});

// DELTA-3 (D2): a sticky 'wedge-unrecovered' fail resolves through its carried originalReason so the
// alert reads as the wedge it STILL is (frozen / one-sided-empty) plus an explicit "still unrecovered"
// note — the human sees the ongoing wedge, not an opaque tag. MUTATION (drop the effectiveReason
// resolution — use s.reason directly): 'wedge-unrecovered' falls to the generic FROZEN clause with the
// WRONG wording for an empty-origin wedge, and the empty-origin match below fails.
test('DELTA-3 stagnationAlerts: a sticky wedge-unrecovered fail renders via its original wedge reason', () => {
  const frozenOrigin = stagnationAlerts([
    {
      chain: 1,
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'wedge-unrecovered',
          originalReason: 'frozen',
          staleSide: 'B',
          skewSeconds: 30_000,
          thresholdSeconds: 7200,
          maxA: 100,
          maxB: 90,
          tsA: 1_700_000_000,
          tsB: 1_699_970_000,
        },
      },
    },
  ]);
  assert.match(frozenOrigin[0], /leg B has stopped persisting rows/);
  assert.match(
    frozenOrigin[0],
    /\(still unrecovered\)/,
    'a sticky wedge is flagged as still unrecovered',
  );

  const emptyOrigin = stagnationAlerts([
    {
      chain: 8453,
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'wedge-unrecovered',
          originalReason: 'one-sided-empty',
          staleSide: 'B',
          skewSeconds: null,
          thresholdSeconds: 7200,
          maxA: 12_500,
          maxB: null,
          tsA: 1_700_050_000,
          tsB: null,
        },
      },
    },
  ]);
  assert.match(
    emptyOrigin[0],
    /leg B has persisted NO rows for over 7200s \(empty leg\) and the other leg is advancing/,
    'an empty-origin sticky wedge renders with the empty-leg wording, not the generic frozen clause',
  );
  assert.match(emptyOrigin[0], /\(still unrecovered\)/);
});

// DELTA-4 F6: stagnationAlerts must never render misleading text when originalReason is missing or the
// skew is null. (1) A wedge-unrecovered fail with NO originalReason falls back to the reason itself, not
// an undefined-driven generic clause. (2) A null skew OMITS the skew clause — never "skew nulls > Ns".
// MUTATION-1 (drop the `?? s.reason` fallback for effectiveReason): originalReason is undefined, the
// clause chain falls to the frozen branch as before, but this is the robustness path the review flagged —
// the fallback makes it deterministic. MUTATION-2 (render the skew clause unconditionally — remove the
// `hasSkew` guard, hardcode `skew ${s.skewSeconds}s`): a null skew renders "skew nulls > 7200s" and the
// negative `doesNotMatch(/skew null/)` assertion fails.
test('DELTA-4 F6: stagnationAlerts renders no misleading text for missing originalReason / null skew', () => {
  // A wedge-unrecovered fail carrying a NULL skew and NO originalReason (the review's exact shape).
  const line = stagnationAlerts([
    {
      chain: 1,
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'wedge-unrecovered',
          // originalReason intentionally absent
          staleSide: 'A',
          skewSeconds: null,
          thresholdSeconds: 7200,
          maxA: 100,
          maxB: null,
          tsA: 1_700_000_000,
          tsB: null,
        },
      },
    },
  ]);
  assert.equal(line.length, 1);
  // The pre-fix bug rendered "skew nulls > 7200s" / "skew null > 7200s" from a null skewSeconds.
  assert.doesNotMatch(
    line[0],
    /skew null/,
    'a null skew never renders as "skew null(s) > Ns"',
  );
  assert.doesNotMatch(
    line[0],
    /undefined/,
    'no undefined leaks into the rendered line',
  );
  assert.match(
    line[0],
    /leg A has stopped persisting rows/,
    'a missing originalReason falls back cleanly',
  );
  assert.match(
    line[0],
    /\(still unrecovered\)/,
    'a sticky wedge is still flagged unrecovered',
  );
  assert.doesNotMatch(
    line[0],
    /skew \S+ > 7200s/,
    'the skew clause is omitted when skew is null',
  );
});

// ── D1: OR-composition of the verdict — the guard fires on a frozen-window PASS, windowed classes stay ─
//
// The WHOLE POINT of the guard: it FAILs a chain whose WINDOWED diff would read PASS (the issue #36
// shape — leg A stopped, the window `hi` froze at leg A's max, so the in-window diff is clean while
// hours of divergence sit above hi). D1 requires the FULL windowed diff to still run and report, with a
// fired guard forcing FAIL by OR-composition. We drive the REAL compareChain with STUBBED deps.

// A deps stub factory: a healthy windowed diff (no divergence at all) so the ONLY failure source is the
// stagnation guard — proving the guard's FAIL is not leaking from a windowed cause.
const cleanDiff = { onlyA: 0, onlyB: 0, mismatch: 0, shared: 10 };
const cleanTableRes = { diff: cleanDiff, onlyBRows: [], capped: false };
const cleanTx = {
  fail: false,
  class: 'realtime-parent-tx-gap',
  expectedMissing: 0,
  unexpectedB: [],
  unreferencedA: [],
  sharedMismatch: 0,
  toleratedIssue27: { count: 0, perChain: {} },
  knownBadRows: { count: 0, perChain: {}, perHash: {} },
};

// Stub deps that yield the issue #36 frozen-window shape: leg A frozen 17h behind leg B, but the
// WINDOWED diff (over [lo, hi] where hi is pinned to leg A's frozen max) is perfectly clean.
const frozenWindowDeps = (overrides = {}) => ({
  // both legs reach the SAME bound (window is well-formed, hi >= lo)
  overlapBound: async () => 200_000,
  checkpointProgress: async () => 200_000,
  legNewestRow: async (_url, _chain, ...rest) => {
    // deps.legNewestRow(url, chain); the test distinguishes the two legs by call order via a closure
    return rest; // unused; overridden per-leg below
  },
  diffLogs: async () => cleanTableRes,
  diffBlocks: async () => cleanTableRes,
  diffTx: async () => cleanTx,
  bucketHashes: async () => new Map(),
  bucketHashesExcluding: async () => new Map(),
  ...overrides,
});

test('D1: compareChain — fired guard forces FAIL even when the windowed diff is CLEAN, windowed classes still present', async () => {
  // leg A newest row is 17h older than leg B; prev armed the same stale side so the direction check
  // fires (frozen). The windowed diff is clean (cleanDiff) → the ONLY failure source is stagnation.
  const tsB = 1_700_000_000;
  const tsA = tsB - 17 * 3600; // 17h behind
  let legCall = 0;
  const deps = frozenWindowDeps({
    legNewestRow: async () => {
      legCall += 1;
      // call order in compareChain's Promise.all: legNewestRow(urlA) then legNewestRow(urlB)
      return legCall === 1
        ? { maxBlock: '100000', ts: String(tsA) } // leg A (frozen, pins hi)
        : { maxBlock: '132825', ts: String(tsB) }; // leg B (advanced)
    },
  });
  // armed last run: leg A frozen (same tsA), leg B advanced since prev.tsB → one-sided wedge
  const prev = { tsA, tsB: tsB - 3600, skew: 17 * 3600 - 3600 };

  const out = await compareChain(
    'urlA',
    'urlB',
    8453,
    0, // cutover (lo)
    64, // margin
    1000, // bucket
    '', // schemaB
    7200, // threshold
    prev,
    Date.now(),
    deps,
  );

  // MUTATION (remove the verdict override — drop `|| stagnation.fail` from the OR-composition): the
  // clean windowed diff makes verdict PASS and this assertion fails. This is wiring mutation W1.
  assert.equal(
    out.verdict,
    'FAIL',
    'a fired guard forces FAIL despite a clean window',
  );
  assert.equal(out.classes.persistStagnation.fail, true);
  assert.equal(out.classes.persistStagnation.reason, 'frozen');
  assert.equal(out.classes.persistStagnation.staleSide, 'A');

  // D1: the FULL windowed diff STILL ran and reported — logs/blocks/transactions/checkpointBuckets are
  // all present (not short-circuited by an early return), each reading clean.
  assert.ok(
    out.classes.logs,
    'the windowed logs class is present (no early return)',
  );
  assert.ok(out.classes.blocks, 'the windowed blocks class is present');
  assert.ok(
    out.classes.transactions,
    'the windowed transactions class is present',
  );
  assert.ok(
    out.classes.checkpointBuckets,
    'the windowed checkpointBuckets class is present',
  );
  assert.equal(out.classes.logs.fail, false, 'the windowed logs diff is clean');
  assert.equal(out.classes.transactions.fail, false);

  // and the state to carry forward is the unified evidence record — BOTH legs' ts + skew (finding 2),
  // plus the fresh sticky wedge (D2) and no emptiness arm (both populated).
  assert.equal(out.stagnationState.tsA, tsA);
  assert.equal(out.stagnationState.tsB, tsB);
  assert.equal(out.stagnationState.skew, 17 * 3600);
  assert.equal(out.stagnationState.emptySinceA, null);
  assert.equal(out.stagnationState.emptySinceB, null);
  assert.equal(
    out.stagnationState.wedgeStaleSide,
    'A',
    'the fired wedge locks in the frozen leg A',
  );
  assert.equal(
    out.stagnationState.wedgeReason,
    'frozen',
    'the true first-fail reason is carried',
  );
  assert.ok(
    Number.isFinite(out.stagnationState.wedgeFailedSince),
    'a fired frozen wedge sets wedgeFailedSince (sticky, D2)',
  );
});

test('D1: compareChain — a fired guard OR-composes to FAIL even on the NO-OVERLAP (hi<lo) PENDING path', async () => {
  // A one-sided wedge is precisely a state that can hold hi below lo (the window never opens). The
  // guard must still FAIL there, not read PENDING. We force hi<lo by a huge margin, and arm a frozen
  // stale leg so the guard fires. classes.persistStagnation is attached; verdict is FAIL not PENDING.
  const tsB = 1_700_000_000;
  const tsA = tsB - 17 * 3600;
  let legCall = 0;
  const deps = frozenWindowDeps({
    // both bounds small; the margin below drives hi < lo
    overlapBound: async () => 10,
    checkpointProgress: async () => 10,
    legNewestRow: async () => {
      legCall += 1;

      return legCall === 1
        ? { maxBlock: '100000', ts: String(tsA) }
        : { maxBlock: '132825', ts: String(tsB) };
    },
  });
  const prev = { tsA, tsB: tsB - 3600, skew: 17 * 3600 - 3600 };
  const out = await compareChain(
    'a',
    'b',
    8453,
    100, // cutover (lo=100) …
    64, // margin → hi = min(10,10)-64 = -54 < lo=100 → no overlap
    1000,
    '',
    7200,
    prev,
    Date.now(),
    deps,
  );
  // MUTATION (PENDING path drops the stagnation OR — `stagnation.fail ? 'FAIL' : 'PENDING'` → always
  // 'PENDING'): this reads PENDING and the assertion fails. This is the no-overlap arm of W1.
  assert.equal(
    out.verdict,
    'FAIL',
    'a fired guard FAILs even with no finalized overlap',
  );
  assert.equal(out.classes.persistStagnation.fail, true);
  assert.match(out.classes.note, /no finalized overlap yet/);
  // and the windowed classes are NOT present (there is no window to diff) — the guard is the sole cause
  assert.equal(out.classes.logs, undefined, 'no windowed diff runs when hi<lo');
});

test('D3: compareChain — window path attaches classes.persistStagnation with the surfaced fields (below-threshold, PASS)', async () => {
  // a healthy chain (both legs current, skew 0) → PASS, but persistStagnation is STILL attached with
  // maxA/maxB/tsA/tsB so a frozen window is legible even below threshold. MUTATION: gate attaching
  // persistStagnation on `stagnation.fail` → this PASS chain would omit it and the assertions fail.
  const ts = 1_700_000_000;
  let legCall = 0;
  const deps = frozenWindowDeps({
    legNewestRow: async () => {
      legCall += 1;

      return { maxBlock: legCall === 1 ? '200000' : '200000', ts: String(ts) };
    },
  });
  const out = await compareChain(
    'urlA',
    'urlB',
    1,
    0,
    64,
    1000,
    '',
    7200,
    null,
    Date.now(),
    deps,
  );
  assert.equal(out.verdict, 'PASS', 'a healthy chain passes');
  const s = out.classes.persistStagnation;
  assert.ok(s, 'persistStagnation is attached on the window (PASS) path');
  assert.equal(s.fail, false);
  assert.equal(s.maxA, 200_000);
  assert.equal(s.maxB, 200_000);
  assert.equal(s.tsA, ts);
  assert.equal(s.tsB, ts);
  assert.equal(s.thresholdSeconds, 7200);
});

test('D3: compareChain — the threshold parameter actually reaches the decision (two thresholds, different outcomes)', async () => {
  // Same frozen shape (leg A ~2.5h behind leg B), prev armed frozen. With a 2h threshold the guard
  // FAILs; with a 3h threshold the same skew is within tolerance → PASS. Proves the threshold arg is
  // threaded to stagnationDecision. MUTATION: stub the decision to a constant / ignore the threshold arg
  // (wiring mutation W2) → both thresholds give the same verdict and one of these assertions fails.
  const tsB = 1_700_000_000;
  const skew = Math.round(2.5 * 3600); // 9000s
  const tsA = tsB - skew;
  const mkDeps = () => {
    let legCall = 0;

    return frozenWindowDeps({
      legNewestRow: async () => {
        legCall += 1;

        return legCall === 1
          ? { maxBlock: '100000', ts: String(tsA) }
          : { maxBlock: '132825', ts: String(tsB) };
      },
    });
  };
  // prev: leg A frozen at tsA, leg B advanced since prev.tsB → one-sided wedge over the 2h threshold
  const prev = { tsA, tsB: tsB - 1800, skew: skew - 1800 };

  const failing = await compareChain(
    'a',
    'b',
    8453,
    0,
    64,
    1000,
    '',
    7200,
    prev,
    Date.now(),
    mkDeps(),
  );
  assert.equal(
    failing.verdict,
    'FAIL',
    '2h threshold: a 2.5h frozen skew fails',
  );

  const passing = await compareChain(
    'a',
    'b',
    8453,
    0,
    64,
    1000,
    '',
    10_800,
    prev,
    Date.now(),
    mkDeps(),
  );
  assert.equal(
    passing.verdict,
    'PASS',
    '3h threshold: the same 2.5h skew is in tolerance',
  );
  assert.equal(passing.classes.persistStagnation.fail, false);
});

test('D1: compareChain — a WINDOWED hard-fail AND a fired guard compose to one FAIL, both classes reported', async () => {
  // The stagnation guard fires AND the windowed logs diff has a hard onlyA (a real divergence). D1's
  // OR-composition must FAIL and BOTH the stagnation class and the hard logs class must be present —
  // neither suppresses the other's reporting.
  const tsB = 1_700_000_000;
  const tsA = tsB - 17 * 3600;
  let legCall = 0;
  const hardLogs = {
    diff: { onlyA: 3, onlyB: 0, mismatch: 0, shared: 5 }, // onlyA → hard fail
    onlyBRows: [],
    capped: false,
  };
  const deps = frozenWindowDeps({
    legNewestRow: async () => {
      legCall += 1;

      return legCall === 1
        ? { maxBlock: '100000', ts: String(tsA) }
        : { maxBlock: '132825', ts: String(tsB) };
    },
    diffLogs: async () => hardLogs,
  });
  // leg A frozen at tsA, leg B advanced since prev.tsB → one-sided wedge fires alongside the hard logs
  const prev = { tsA, tsB: tsB - 3600, skew: 17 * 3600 - 3600 };
  const out = await compareChain(
    'a',
    'b',
    8453,
    0,
    64,
    1000,
    '',
    7200,
    prev,
    Date.now(),
    deps,
  );
  assert.equal(out.verdict, 'FAIL');
  assert.equal(
    out.classes.persistStagnation.fail,
    true,
    'the stagnation class is reported',
  );
  assert.equal(
    out.classes.logs.fail,
    true,
    'the windowed hard-fail is ALSO reported',
  );
  assert.equal(out.classes.logs.onlyA, 3);
});

// ── E2E: the issue #36 TRANSACTION tolerance through the REAL compareChain fold (blocks-onlyB → tx) ───
//
// These drive the REAL compareChain wiring: diffBlocks yields the wholly-A-absent block set, diffTx
// yields the onlyB txs (with block numbers), and compareChain folds classifyOnlyBTxDiff over the two.
// A non-stagnating (skew 0) window isolates the tx signal as the ONLY verdict driver.

// A NON-stagnating deps stub (both legs same ts → skew 0 → PASS on the stagnation axis) with injectable
// block/tx diffs, so the tx tolerance is the ONLY thing that can move the verdict. Chain 1 (has a floor).
const txToleranceDeps = (overrides = {}) => {
  const ts = 1_700_000_000;

  return {
    overlapBound: async () => 200_000,
    checkpointProgress: async () => 200_000,
    legNewestRow: async () => ({ maxBlock: '200000', ts: String(ts) }),
    diffLogs: async () => cleanTableRes,
    diffBlocks: async () => cleanTableRes,
    diffTx: async () => cleanTx,
    bucketHashes: async () => new Map(),
    bucketHashesExcluding: async () => new Map(),
    ...overrides,
  };
};
const ISSUE_36_FLOOR_1_E2E = 25445239;
const runTxToleranceChain = (deps) =>
  compareChain('a', 'b', 1, 0, 64, 1000, '', 7200, null, Date.now(), deps);

test('E2E compareChain: the TOLERATED case — an onlyB tx in a wholly-A-absent block ⇒ transactions do NOT fail, verdict PASS', async () => {
  // diffBlocks reports the block wholly absent from A (onlyB block row) → the block is in the absent set;
  // diffTx reports one onlyB tx in that same block. The fold tolerates it: transactions.fail=false,
  // toleratedIssue36 surfaces, and with no other divergence the verdict is PASS.
  const block = ISSUE_36_FLOOR_1_E2E + 10;
  const deps = txToleranceDeps({
    diffBlocks: async () => ({
      diff: { onlyA: 0, onlyB: 1, mismatch: 0, shared: 5 },
      onlyBRows: [{ blockNumber: block }],
      capped: false,
    }),
    diffTx: async () => ({
      ...cleanTx,
      onlyBTxRows: [{ hash: '0xdeadbeef', blockNumber: block }],
    }),
  });
  const out = await runTxToleranceChain(deps);
  assert.equal(
    out.verdict,
    'PASS',
    'a tolerated tx facet does not fail the run',
  );
  assert.equal(out.classes.transactions.fail, false);
  assert.deepEqual(out.classes.transactions.unexpectedB, []);
  assert.equal(out.classes.transactions.toleratedIssue36.count, 1);
  assert.deepEqual(out.classes.transactions.toleratedIssue36.perChain, {
    1: 1,
  });
  // the internal carrier is stripped from the status class
  assert.equal('onlyBTxRows' in out.classes.transactions, false);
  // the block facet is itself tolerated (its onlyB block row is at/above the floor)
  assert.equal(out.classes.blocks.fail, false);
});

test('E2E compareChain: the STRICTNESS case — an onlyB tx whose block EXISTS in A ⇒ transactions still UNEXPECTED, verdict FAIL', async () => {
  // diffBlocks reports NO onlyB block (leg A has every block) → the absent set is empty; diffTx reports
  // one onlyB tx. That is a tx-level-only loss → a NEW divergence class → it MUST keep failing loudly.
  const block = ISSUE_36_FLOOR_1_E2E + 20;
  const deps = txToleranceDeps({
    diffBlocks: async () => cleanTableRes, // no onlyB block → A HAS the block
    diffTx: async () => ({
      ...cleanTx,
      onlyBTxRows: [{ hash: '0xnewclass', blockNumber: block }],
    }),
  });
  const out = await runTxToleranceChain(deps);
  assert.equal(
    out.verdict,
    'FAIL',
    'a B-only tx in an A-present block still FAILs loudly',
  );
  assert.equal(out.classes.transactions.fail, true);
  assert.equal(out.classes.transactions.class, 'UNEXPECTED');
  assert.deepEqual(out.classes.transactions.unexpectedB, ['0xnewclass']);
  assert.equal(out.classes.transactions.toleratedIssue36.count, 0);
});

test('E2E compareChain: FAIL-CLOSED — an onlyB tx in an A-absent block is NOT tolerated when the blocks onlyB stream was CAPPED', async () => {
  // The absent set is only authoritative when the blocks onlyB collector saw every row. If blocks CAPPED,
  // compareChain passes an EMPTY absent set → the tx is NOT tolerated even though its block genuinely
  // appears in the (truncated) onlyB set. An untrustworthy evidence source must never widen tolerance.
  // MUTATION (build the absent set regardless of blocksRes.capped) → this assertion fails.
  const block = ISSUE_36_FLOOR_1_E2E + 30;
  const deps = txToleranceDeps({
    diffBlocks: async () => ({
      diff: { onlyA: 0, onlyB: 1, mismatch: 0, shared: 5 },
      onlyBRows: [{ blockNumber: block }],
      capped: true, // the blocks onlyB stream was truncated → absent set not authoritative
    }),
    diffTx: async () => ({
      ...cleanTx,
      onlyBTxRows: [{ hash: '0xcapped', blockNumber: block }],
    }),
  });
  const out = await runTxToleranceChain(deps);
  assert.equal(out.verdict, 'FAIL', 'blocks capped ⇒ tx tolerance is disabled');
  assert.equal(out.classes.transactions.fail, true);
  assert.deepEqual(out.classes.transactions.unexpectedB, ['0xcapped']);
  assert.equal(out.classes.transactions.toleratedIssue36.count, 0);
});

test('E2E compareChain: FAIL-CLOSED — an onlyB tx in an A-absent block is NOT tolerated when the blocks onlyB collector cross-check fails (!blocksCollector.ok)', async () => {
  // The absent set is only authoritative when the blocks onlyB collector received EVERY row the diff
  // counted. If the diff reports onlyB=2 but the collector only has 1 row (not capped — a silent wiring
  // drop), crossCheckOnlyBCollector returns {ok:false}: the set is incomplete and MUST NOT widen
  // tolerance. compareChain passes an EMPTY absent set → the otherwise-tolerable onlyB tx stays in
  // unexpectedB and the run FAILs.
  // MUTATION (drop `!blocksCollector.ok` from the absentBlocks guard) → this assertion fails: the
  // tx would be tolerated via the partial set even though the collector is incomplete.
  const block = ISSUE_36_FLOOR_1_E2E + 40;
  const deps = txToleranceDeps({
    diffBlocks: async () => ({
      // diff reports 2 onlyB rows — but the collector only received 1 (silent wiring drop, not capped)
      diff: { onlyA: 0, onlyB: 2, mismatch: 0, shared: 5 },
      onlyBRows: [{ blockNumber: block }], // only 1 row collected → cross-check fails
      capped: false, // NOT capped: the gap is a real collector wiring bug, not an expected cap stop
    }),
    diffTx: async () => ({
      ...cleanTx,
      onlyBTxRows: [{ hash: '0xcollectorfail', blockNumber: block }],
    }),
  });
  const out = await runTxToleranceChain(deps);
  assert.equal(
    out.verdict,
    'FAIL',
    'blocks collector cross-check fails ⇒ tx tolerance is disabled',
  );
  assert.equal(out.classes.transactions.fail, true);
  assert.deepEqual(out.classes.transactions.unexpectedB, ['0xcollectorfail']);
  assert.equal(out.classes.transactions.toleratedIssue36.count, 0);
});

// ── D3: default deps wiring — the production call path uses the REAL module functions ─────────────────
// COMPARE_CHAIN_DEPS is the default; a test override swaps only the seams it needs, and the default
// binds every real adapter. MUTATION: drop a key from COMPARE_CHAIN_DEPS (e.g. remove legNewestRow) →
// compareChain's default-deps call throws "deps.legNewestRow is not a function" — caught here.
test('D3: COMPARE_CHAIN_DEPS binds every adapter compareChain calls (default-deps wiring)', () => {
  for (const key of [
    'overlapBound',
    'checkpointProgress',
    'legNewestRow',
    'diffLogs',
    'diffBlocks',
    'diffTx',
    'bucketHashes',
    'bucketHashesExcluding',
  ]) {
    assert.equal(
      typeof COMPARE_CHAIN_DEPS[key],
      'function',
      `COMPARE_CHAIN_DEPS.${key} is wired to a real function`,
    );
  }
});

// ── D4 / finding 3: composeAlerts — the generic finalized-diff line fires on a WINDOWED fail only ─────

test('chainWindowedFail: true only for a FAIL with a hard windowed class (logs/blocks/tx/buckets) or an ERROR', () => {
  // a stagnation-ONLY FAIL (no hard windowed class) is NOT a windowed fail
  assert.equal(
    chainWindowedFail({
      chain: 1,
      verdict: 'FAIL',
      classes: {
        persistStagnation: { fail: true },
        logs: { fail: false },
        blocks: { fail: false },
        transactions: { fail: false },
        checkpointBuckets: { ok: true },
      },
    }),
    false,
    'a stagnation-only FAIL is not a windowed fail',
  );
  // a hard logs fail IS a windowed fail
  assert.equal(
    chainWindowedFail({
      chain: 1,
      verdict: 'FAIL',
      classes: { logs: { fail: true } },
    }),
    true,
  );
  // a hard checkpointBuckets (ok:false) IS a windowed fail
  assert.equal(
    chainWindowedFail({
      chain: 1,
      verdict: 'FAIL',
      classes: { checkpointBuckets: { ok: false } },
    }),
    true,
  );
  // an ERROR (diff could not complete) counts as a windowed hard-fail
  assert.equal(
    chainWindowedFail({ chain: 1, verdict: 'ERROR', classes: {} }),
    true,
  );
  // a PASS / PENDING is never a windowed fail
  assert.equal(
    chainWindowedFail({ chain: 1, verdict: 'PASS', classes: {} }),
    false,
  );
  assert.equal(
    chainWindowedFail({ chain: 1, verdict: 'PENDING', classes: {} }),
    false,
  );
});

test('composeAlerts: FINDING 3 — chain X stagnation-only + chain Y windowed FAIL → BOTH the stagnation line AND the generic line', () => {
  const results = [
    // chain X: stagnation-only FAIL (frozen window, clean windowed classes)
    {
      chain: 1,
      verdict: 'FAIL',
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'frozen',
          staleSide: 'A',
          skewSeconds: 10_800,
          thresholdSeconds: 7200,
          maxA: 90,
          maxB: 100,
          tsA: 989_200,
          tsB: 1_000_000,
        },
        logs: { fail: false },
        blocks: { fail: false },
        transactions: { fail: false },
        checkpointBuckets: { ok: true },
      },
    },
    // chain Y: a genuine WINDOWED hard-fail (hard onlyA in logs), no stagnation
    {
      chain: 8453,
      verdict: 'FAIL',
      classes: {
        persistStagnation: { fail: false, reason: 'both-populated-in-skew' },
        logs: { fail: true, onlyA: 2 },
      },
    },
  ];
  const alerts = composeAlerts(results, [], { crashLoop: false });

  // the stagnation line for chain X is present
  assert.ok(
    alerts.some((a) => /persist-stagnation: chain 1/.test(a)),
    'chain X stagnation line present',
  );
  // MUTATION (drop the generic-line push / gate it on `stagnations.length === 0`): with chain X
  // carrying a stagnation line, the OLD suppression logic would swallow the generic line even though
  // chain Y has a real windowed fail. The generic line MUST still appear for chain Y.
  assert.ok(
    alerts.some((a) =>
      /finalized-diff: an unexpected finalized-overlap divergence/.test(a),
    ),
    'the generic finalized-diff line fires because chain Y has a WINDOWED fail',
  );
});

test('composeAlerts: a stagnation-ONLY run (no windowed fail anywhere) does NOT emit the generic line', () => {
  const results = [
    {
      chain: 1,
      verdict: 'FAIL',
      classes: {
        persistStagnation: {
          fail: true,
          reason: 'frozen',
          staleSide: 'A',
          skewSeconds: 10_800,
          thresholdSeconds: 7200,
          maxA: 90,
          maxB: 100,
          tsA: 1,
          tsB: 2,
        },
        logs: { fail: false },
        blocks: { fail: false },
        transactions: { fail: false },
        checkpointBuckets: { ok: true },
      },
    },
  ];
  const alerts = composeAlerts(results, [], { crashLoop: false });
  assert.ok(alerts.some((a) => /persist-stagnation: chain 1/.test(a)));
  assert.equal(
    alerts.some((a) => /finalized-diff/.test(a)),
    false,
    'a stagnation-only FAIL does not emit the vague generic line',
  );
});

test('composeAlerts: crash-loop and checkpoint-regression lines are emitted; a checkpoint regression alone does NOT emit the generic line', () => {
  const results = [
    {
      chain: 1,
      verdict: 'FAIL', // FAILed by the checkpoint regression, but no windowed class fail
      classes: {
        persistStagnation: { fail: false, reason: 'both-populated-in-skew' },
        checkpointRegression: { ok: false, prev: '500', cur: '400' },
      },
    },
  ];
  const regressions = [{ chain: 1, prev: '500', cur: '400' }];
  const alerts = composeAlerts(results, regressions, {
    crashLoop: true,
    restartsLastHour: 5,
  });
  assert.ok(alerts.some((a) => /crash-loop: 5 restarts/.test(a)));
  assert.ok(
    alerts.some((a) =>
      /checkpoint-regression: chain 1 rewound 500 → 400/.test(a),
    ),
  );
  // a checkpoint regression is not a WINDOWED diff cause → no generic line
  assert.equal(
    alerts.some((a) => /finalized-diff/.test(a)),
    false,
  );
});

// ── D5(a): chainCounters surfaces the stagnation summary in the per-chain counters entry ─────────────

test('chainCounters: carries the persist-stagnation summary (maxA/maxB/tsA/tsB/skew/reason) next to lo/hi/verdict', () => {
  const r = {
    chain: 1,
    lo: 100,
    hi: 200,
    verdict: 'FAIL',
    classes: {
      persistStagnation: {
        fail: true,
        reason: 'frozen',
        staleSide: 'A',
        skewSeconds: 10_800,
        thresholdSeconds: 7200,
        maxA: 90,
        maxB: 100,
        tsA: 989_200,
        tsB: 1_000_000,
      },
    },
  };
  const c = chainCounters(r);
  assert.equal(c.lo, 100);
  assert.equal(c.hi, 200);
  assert.equal(c.verdict, 'FAIL');
  // MUTATION (drop the stagnation-summary spread): these fields go missing and the assertions fail.
  assert.equal(c.maxA, 90);
  assert.equal(c.maxB, 100);
  assert.equal(c.tsA, 989_200);
  assert.equal(c.tsB, 1_000_000);
  assert.equal(c.stagnationSkewSeconds, 10_800);
  assert.equal(c.stagnationReason, 'frozen');

  // a result with no persistStagnation (ERROR before the guard) → bare {lo, hi, verdict}
  const bare = chainCounters({ chain: 9, lo: 1, hi: 2, verdict: 'ERROR' });
  assert.deepEqual(bare, { lo: 1, hi: 2, verdict: 'ERROR' });
});

// ── D2: cross-run state round-trips through the CHECKPOINT_FILE (backward-compatible `_stagnation`) ────
//
// The differ persists per-chain stagnation state under a `_stagnation` top-level key in the SAME file
// as the checkpoint monotonicity series. An ABSENT key ⇒ no prior state (backward compatible). The key
// is chain-disjoint (numeric chains), so the monotonicity loop never treats it as a chain.
test('CHECKPOINT_FILE: _stagnation state round-trips and is disjoint from the numeric-chain series', () => {
  const dir = mkdtempSync(join(tmpdir(), 'ab-stag-'));
  const file = join(dir, 'soak-ab-checkpoints.json');

  // a legacy file with ONLY the checkpoint series (no _stagnation key) is valid → absent = no state
  writeJsonAtomic(file, { 1: [100, 200], 8453: [7, 8] });
  const legacy = JSON.parse(readFileSync(file, 'utf8'));
  assert.equal(
    legacy._stagnation,
    undefined,
    'a legacy file has no _stagnation key',
  );

  // writing a file WITH the state section: the numeric series and _stagnation coexist untouched
  const withState = {
    1: [100, 200, 300],
    8453: [7, 8, 9],
    _stagnation: {
      // both-populated shape: BOTH legs' ts + skew (finding 2) — no longer keyed on the stale side
      1: { tsA: 1_699_995_000, tsB: 1_699_989_200, skew: 10_800 },
      8453: {
        emptySide: 'B',
        firstEmptyAtMs: 1_000_000_000_000,
        populatedTs: 1_700_000_000,
      },
    },
  };
  writeJsonAtomic(file, withState);
  const back = JSON.parse(readFileSync(file, 'utf8'));
  assert.deepEqual(
    back[1],
    [100, 200, 300],
    'the numeric-chain series is intact',
  );
  assert.deepEqual(
    back._stagnation[1],
    { tsA: 1_699_995_000, tsB: 1_699_989_200, skew: 10_800 },
    'the per-chain both-populated stagnation state round-trips',
  );
  assert.deepEqual(back._stagnation[8453].emptySide, 'B');
});

// ── W4: the env fail-loud wiring — garbage AB_STAGNATION_MAX_SKEW_S exits nonzero BEFORE any DB access ─
//
// main() must parse the env (readStagnationThreshold) BEFORE the per-chain loop touches a DB, so a
// garbage threshold fails loud immediately. This spawns the REAL script with a garbage threshold and
// dummy DB URLs and asserts a nonzero exit + the naming error on stderr. MUTATION (bypass env parse /
// move it after the DB loop — wiring mutation W4) → the process would instead hang/err on the fake DB
// with a DIFFERENT message, so the "must be a positive number of seconds" match fails.
test('W4: node ab-diff.mjs with garbage AB_STAGNATION_MAX_SKEW_S exits nonzero naming the var, before any DB access', async () => {
  const { spawnSync } = await import('node:child_process');
  const script = join(dirname(fileURLToPath(import.meta.url)), 'ab-diff.mjs');
  // Hygiene: point the status/checkpoint files at a throwaway temp dir (never the cwd) and run the child
  // there, so this spawn cannot litter soak-ab-status.json / soak-ab-checkpoints.json into the repo.
  const dir = mkdtempSync(join(tmpdir(), 'ab-w4-'));
  try {
    const res = spawnSync(process.execPath, [script], {
      cwd: dir,
      env: {
        ...process.env,
        AB_STAGNATION_MAX_SKEW_S: 'garbage',
        // dummy URLs that pass the presence check; if env-parse were AFTER the DB loop, the script would
        // instead fail trying to reach these — a DIFFERENT error, which the assertion below would miss.
        DATABASE_URL_A: 'postgresql://nobody@127.0.0.1:1/none',
        DATABASE_URL_B: 'postgresql://nobody@127.0.0.1:1/none',
        CHAINS: '1',
        STATUS_FILE: join(dir, 'soak-ab-status.json'),
        CHECKPOINT_FILE: join(dir, 'soak-ab-checkpoints.json'),
      },
      encoding: 'utf8',
      timeout: 20_000,
    });
    assert.notEqual(res.status, 0, 'a garbage threshold exits nonzero');
    assert.match(
      res.stderr,
      /AB_STAGNATION_MAX_SKEW_S must be a positive number of seconds/,
      'the naming error is on stderr, raised before any DB access',
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ── sticky-wedge recovery-precondition isolation (independent mutation survivor-hunt) ────────────────
//
// Two survivors an independent mutation hunt found the 242-test suite did NOT kill in ISOLATION. The
// shipped code is CORRECT for both — these tests close the coverage gap so a future regression is caught.
//
// S7 — the sticky-gate `bothPopulated = a !== null && b !== null` recovery precondition. Every existing
// sticky-recovery test that lands in the FAIL exit ALSO has a growing carried gap (or an out-of-threshold
// skew), so the `bothPopulated` conjunct was never the SOLE thing keeping the wedge FAILed: mutating it to
// `||` survived. This case isolates it — the ONLY reason recovery does not fire is that a leg is EMPTY.
// The carried wedge-stale leg (A, behind B) ADVANCES this run (toward B, so the carried gap SHRINKS and
// the skew-growth guard does NOT catch it) while B reads EMPTY. Under the shipped `&&` this is
// wedge-unrecovered (an empty observation lacks the finite both-populated evidence recovery requires).
// MUTATION S7 (`a !== null && b !== null` → `||`): bothPopulated reads true off the single populated leg,
// the carried stale leg advanced with a non-growing gap → the wedge CLEARS → this run reads non-fail
// ('empty-arming'), so `fail === true` / `reason === 'wedge-unrecovered'` here fail.
test('stagnationDecision: S7 — an EMPTY leg can NEVER recover a wedge even when the carried stale leg advances (bothPopulated is load-bearing)', () => {
  const t = 7200;
  // prev is a LIVE wedge: A is the carried stale leg (behind B), reason 'frozen'. This run A advances
  // 100000 → 140000 (toward B's carried 150000, so the carried gap SHRINKS 50000 → 10000), B is EMPTY.
  const prev = {
    tsA: 100_000,
    tsB: 150_000,
    skew: 50_000,
    emptySinceA: null,
    emptySinceB: null,
    wedgeFailedSince: NOW - 3_600_000,
    wedgeStaleSide: 'A',
    wedgeReason: 'frozen',
  };
  const r = stagnationDecision({
    maxA: '140',
    tsA: 140_000, // A (the carried stale leg) advanced toward B — gap shrinks, no skew growth
    maxB: null,
    tsB: null, // B reads EMPTY → no finite both-populated recovery evidence
    threshold: t,
    prev,
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    true,
    'an empty observation can NEVER clear a live wedge — recovery requires BOTH legs populated',
  );
  assert.equal(r.reason, 'wedge-unrecovered');
  assert.equal(
    r.staleSide,
    'A',
    'the CARRIED wedge stale leg (A) stays named through the sticky fail',
  );
  assert.equal(
    r.originalReason,
    'frozen',
    'the true first-fail reason of the wedge episode is carried',
  );
  // the wedge is carried forward UNCHANGED — the advancing-but-empty run laundered nothing.
  assert.equal(
    r.nextState.wedgeFailedSince,
    prev.wedgeFailedSince,
    'the wedge episode start is preserved (still the same unrecovered wedge)',
  );
  assert.equal(
    r.nextState.wedgeStaleSide,
    'A',
    'the carried stale side is threaded forward unchanged',
  );
  assert.equal(
    r.nextState.wedgeReason,
    'frozen',
    'the carried first-fail reason is threaded forward unchanged',
  );
});

// S5 — a CORRUPT/hand-edited live wedge: wedgeFailedSince is set but wedgeStaleSide AND wedgeReason are
// MISSING (a legacy/corrupt checkpoint record). The sticky gate reads the carried stale side defensively
// (prevWedgeStaleSide is null when it is neither 'A' nor 'B'), so `wedgeStaleAdvanced` MUST fall to its
// final ternary arm = false: an UNKNOWN carried stale side cannot count as "the stale leg advanced".
// Recovery from a corrupt wedge record is then possible ONLY via skew ≤ threshold. Here both legs are
// populated with the skew still over threshold (one leg advanced), so the shipped code stays
// wedge-unrecovered and falls its originalReason back to 'frozen'. MUTATION S5 (the wedgeStaleAdvanced
// final arm false → true — an unknown/null carried side is treated as advanced): with a non-growing
// carried gap the corrupt wedge wrongly CLEARS and this run re-derives a FRESH regime ('frozen'), so the
// `reason === 'wedge-unrecovered'` and `originalReason === 'frozen'` assertions here fail.
test('stagnationDecision: S5 — a corrupt wedge (wedgeStaleSide/wedgeReason missing) does NOT fail-open; unknown stale side is NOT "advanced"', () => {
  const t = 7200;
  // Corrupt prev: a live wedge (wedgeFailedSince set) whose stale-side + reason attribution was lost.
  const prev = {
    tsA: 100_000,
    tsB: 80_000,
    skew: 20_000,
    emptySinceA: null,
    emptySinceB: null,
    wedgeFailedSince: NOW - 3_600_000,
    // wedgeStaleSide MISSING, wedgeReason MISSING (hand-edited / corrupt state)
  };
  const r = stagnationDecision({
    maxA: '100',
    tsA: 100_000, // A holds
    maxB: '81',
    tsB: 81_000, // the stale-ish leg B advances 80000 → 81000; skew 19000 still > threshold
    threshold: t,
    prev,
    nowMs: NOW,
  });
  assert.equal(
    r.fail,
    true,
    'a corrupt wedge with an unknown stale side recovers ONLY via skew ≤ threshold, not via a phantom advance',
  );
  assert.equal(r.reason, 'wedge-unrecovered');
  assert.equal(
    r.staleSide,
    null,
    'the corrupt record carried no stale side, so none is named',
  );
  assert.equal(
    r.originalReason,
    'frozen',
    'a missing carried wedgeReason falls back to frozen',
  );
});

// ── skew-arming: the verdict-reason taxonomy (leg-A-stagnation FAIL vs windowed divergence) ───────────
//
// The taxonomy is ADDITIVE telemetry: it tags WHY an OVERALL FAIL happened so a downstream reader can
// tell the KNOWN §5.13 leg-A-stagnation story (Portal clean, the RPC baseline starved) from a genuine
// windowed Portal-vs-baseline divergence. It NEVER changes the verdict string, the fold, or the exit
// code, and it NEVER excuses a real failure. These tests lock that contract in isolation.

// Fixtures matching the exact per-chain result shape compareChain emits (verdict + classes).
const legAStagnationChain = (chain, reason = 'wedge-unrecovered') => ({
  chain,
  verdict: 'FAIL',
  classes: {
    persistStagnation: {
      fail: true,
      reason,
      originalReason: reason === 'wedge-unrecovered' ? 'frozen' : undefined,
      staleSide: 'A',
      skewSeconds: reason === 'one-sided-empty' ? null : 10_800,
      thresholdSeconds: 7200,
      maxA: 90,
      maxB: 100,
      tsA: 989_200,
      tsB: 1_000_000,
    },
    logs: { fail: false },
    blocks: { fail: false },
    transactions: { fail: false },
    checkpointBuckets: { ok: true },
  },
});

// A chain that FAILs for a real WINDOWED reason (a hard tx divergence) — models the current chain-1
// residual: transactions.fail with 8 unexpectedB hashes under separate investigation (issue-tracked),
// which MUST stay visibly FAIL and classified a windowed divergence, never a benign leg-A tag.
const windowedFailChain = (chain) => ({
  chain,
  verdict: 'FAIL',
  classes: {
    persistStagnation: { fail: false, reason: 'both-populated-in-skew' },
    logs: { fail: false },
    blocks: { fail: false },
    transactions: { fail: true, unexpectedB: 8 },
    checkpointBuckets: { ok: true },
  },
});

const passChain = (chain) => ({
  chain,
  verdict: 'PASS',
  classes: {
    persistStagnation: { fail: false, reason: 'both-populated-in-skew' },
    logs: { fail: false },
    blocks: { fail: false },
    transactions: { fail: false },
    checkpointBuckets: { ok: true },
  },
});

test('isLegAStagnationFail: an ELIGIBLE sustained staleSide-A wedge (wedge-unrecovered / one-sided-empty) with no windowed fail → true', () => {
  assert.equal(isLegAStagnationFail(legAStagnationChain(42161)), true);
  assert.equal(
    isLegAStagnationFail(legAStagnationChain(42161, 'one-sided-empty')),
    true,
  );
});

test('isLegAStagnationFail: a staleSide-B wedge is NEVER eligible (leg B stale is not the leg-A story)', () => {
  const bWedge = legAStagnationChain(42161);
  bWedge.classes.persistStagnation.staleSide = 'B';
  // MUTATION (`s.staleSide === 'A'` → drop / `!== 'B'`): a leg-B wedge would wrongly read eligible.
  assert.equal(
    isLegAStagnationFail(bWedge),
    false,
    'a leg-B wedge must never be tagged leg-A-catchup',
  );
});

test('isLegAStagnationFail: a FRESH frozen / skew-growing staleSide-A wedge is NOT eligible (only sustained reasons qualify — SAFE direction)', () => {
  const frozen = legAStagnationChain(42161);
  frozen.classes.persistStagnation.reason = 'frozen';
  frozen.classes.persistStagnation.originalReason = undefined;
  // MUTATION (widen LEG_A_STAGNATION_REASONS to include 'frozen'): a fresh frozen wedge would wrongly
  // qualify for the benign tag. The eligible set is narrow BY DESIGN.
  assert.equal(
    isLegAStagnationFail(frozen),
    false,
    'a fresh frozen wedge is a real/other divergence, not the sustained leg-A story',
  );
  const growing = legAStagnationChain(42161);
  growing.classes.persistStagnation.reason = 'skew-growing';
  assert.equal(isLegAStagnationFail(growing), false);
});

test('isLegAStagnationFail: a staleSide-A wedge that ALSO has a windowed hard-fail is NOT eligible (the windowed fail wins)', () => {
  const both = legAStagnationChain(42161);
  both.classes.transactions = { fail: true, unexpectedB: 3 };
  // MUTATION (drop the `chainWindowedFail(r)` guard): a chain with a real windowed fail would be
  // mislabelled leg-A-stagnation and its divergence excused. The windowed fail MUST win.
  assert.equal(
    isLegAStagnationFail(both),
    false,
    'a co-occurring windowed hard-fail disqualifies the benign tag',
  );
});

test('isLegAStagnationFail: a PASS / PENDING chain is never eligible', () => {
  assert.equal(isLegAStagnationFail(passChain(1)), false);
  assert.equal(
    isLegAStagnationFail({ chain: 1, verdict: 'PENDING', classes: {} }),
    false,
  );
});

test('classifyFailTaxonomy: a non-FAIL verdict (PASS / PENDING) → null (no failTaxonomy field at all)', () => {
  // MUTATION (drop the `verdict !== 'FAIL'` early return): a PASS run would sprout a failTaxonomy.
  assert.equal(classifyFailTaxonomy([passChain(1)], 'PASS'), null);
  assert.equal(classifyFailTaxonomy([passChain(1)], 'PENDING'), null);
});

test('classifyFailTaxonomy: PURE leg-A stagnation (only 42161 wedges, all rows clean) → tag skew-arming:likely-A-catchup, legAStagnationOnly true', () => {
  const results = [passChain(1), passChain(8453), legAStagnationChain(42161)];
  const tax = classifyFailTaxonomy(results, 'FAIL');
  // MUTATION (map an eligible leg-A wedge to 'windowed-divergence' instead of 'leg-A-stagnation'):
  // the tag would flip to 'windowed-divergence' and legAStagnationOnly to false.
  assert.equal(tax.tag, 'skew-arming:likely-A-catchup');
  assert.equal(tax.legAStagnationOnly, true);
  assert.equal(tax.windowedDivergence, false);
  assert.equal(tax.perChain[42161], 'leg-A-stagnation');
  assert.equal(tax.perChain[1], 'clean');
  assert.equal(tax.perChain[8453], 'clean');
});

test('classifyFailTaxonomy: a WINDOWED hard-fail present (chain 1 tx unexpectedB) → windowedDivergence true, that chain windowed-divergence, NOT leg-A-stagnation', () => {
  const results = [windowedFailChain(1), passChain(8453), passChain(42161)];
  const tax = classifyFailTaxonomy(results, 'FAIL');
  // MUTATION (route a non-eligible FAIL to 'clean' instead of 'windowed-divergence'): windowedDivergence
  // would read false and the real chain-1 tx divergence would silently vanish from the taxonomy.
  assert.equal(tax.tag, 'windowed-divergence');
  assert.equal(tax.windowedDivergence, true);
  assert.equal(tax.legAStagnationOnly, false);
  assert.equal(
    tax.perChain[1],
    'windowed-divergence',
    'the chain-1 tx residual stays a windowed divergence, never excused as leg-A-catchup',
  );
});

test('classifyFailTaxonomy: MIXED — a leg-A stagnation FAIL AND a windowed divergence both present → tag mixed', () => {
  const results = [
    windowedFailChain(1),
    passChain(8453),
    legAStagnationChain(42161),
  ];
  const tax = classifyFailTaxonomy(results, 'FAIL');
  // MUTATION (drop the mixed branch / fall through to skew-arming): a real windowed divergence would be
  // hidden behind the benign tag even though chain 1 is genuinely diverging.
  assert.equal(tax.tag, 'mixed');
  assert.equal(tax.legAStagnationOnly, false);
  assert.equal(tax.windowedDivergence, true);
  assert.equal(tax.perChain[42161], 'leg-A-stagnation');
  assert.equal(tax.perChain[1], 'windowed-divergence');
});

test('classifyFailTaxonomy: a staleSide-B wedge FAIL (leg B stale) is a windowed-divergence, NOT leg-A-catchup', () => {
  const bWedge = legAStagnationChain(42161);
  bWedge.classes.persistStagnation.staleSide = 'B';
  const tax = classifyFailTaxonomy([passChain(1), bWedge], 'FAIL');
  // A leg-B stall must NOT be mislabelled the leg-A story; it folds to a real divergence (SAFE direction).
  assert.equal(tax.tag, 'windowed-divergence');
  assert.equal(tax.legAStagnationOnly, false);
  assert.equal(tax.perChain[42161], 'windowed-divergence');
});

test('classifyFailTaxonomy: an ERROR chain (diff could not complete) counts as a windowed divergence', () => {
  const results = [
    { chain: 1, verdict: 'ERROR', classes: {} },
    passChain(8453),
  ];
  const tax = classifyFailTaxonomy(results, 'FAIL');
  assert.equal(tax.windowedDivergence, true);
  assert.equal(tax.tag, 'windowed-divergence');
  assert.equal(tax.perChain[1], 'windowed-divergence');
});

test('composeAlerts: a pure leg-A-stagnation FAIL surfaces the skew-arming human line (additive; existing lines untouched)', () => {
  const results = [passChain(1), passChain(8453), legAStagnationChain(42161)];
  const tax = classifyFailTaxonomy(results, 'FAIL');
  const alerts = composeAlerts(results, [], { crashLoop: false }, tax);
  // the existing per-chain stagnation line is still present (semantics unchanged)
  assert.ok(
    alerts.some((a) => /persist-stagnation: chain 42161/.test(a)),
    'the existing stagnation line is untouched',
  );
  // the generic finalized-diff line does NOT fire (no windowed fail anywhere)
  assert.equal(
    alerts.some((a) => /finalized-diff/.test(a)),
    false,
  );
  // MUTATION (drop the additive skew-arming push): the human-readable tag line goes missing.
  assert.ok(
    alerts.some((a) =>
      /skew-arming: overall FAIL is leg-A stagnation ONLY/.test(a),
    ),
    'the additive skew-arming line names the likely-A-catchup case',
  );
});

test('composeAlerts: a windowed-divergence FAIL does NOT get the benign skew-arming line; a mixed FAIL gets the MIXED line', () => {
  // windowed-only: no skew-arming line
  const windowed = [windowedFailChain(1)];
  const wtax = classifyFailTaxonomy(windowed, 'FAIL');
  const walerts = composeAlerts(windowed, [], { crashLoop: false }, wtax);
  assert.equal(
    walerts.some((a) => /skew-arming:/.test(a)),
    false,
    'a real windowed divergence never gets a skew-arming annotation',
  );
  assert.ok(
    walerts.some((a) => /finalized-diff/.test(a)),
    'the generic windowed line still fires',
  );

  // mixed: the MIXED line, not the likely-A-catchup line
  const mixed = [windowedFailChain(1), legAStagnationChain(42161)];
  const mtax = classifyFailTaxonomy(mixed, 'FAIL');
  const malerts = composeAlerts(mixed, [], { crashLoop: false }, mtax);
  // MUTATION (fold mixed into the likely-A-catchup branch): a mixed FAIL would falsely read benign.
  assert.ok(
    malerts.some((a) => /skew-arming: overall FAIL is MIXED/.test(a)),
    'a mixed FAIL is annotated MIXED, not likely-A-catchup',
  );
  assert.equal(
    malerts.some((a) => /likely-A-catchup/.test(a)),
    false,
    'a mixed FAIL is NOT annotated likely-A-catchup',
  );
});

test('composeAlerts: called WITHOUT a failTaxonomy arg (legacy 3-arg call) behaves exactly as before', () => {
  const results = [passChain(1), legAStagnationChain(42161)];
  // MUTATION (make the 4th param required / crash on undefined): the legacy 3-arg call would throw.
  const alerts = composeAlerts(results, [], { crashLoop: false });
  assert.ok(alerts.some((a) => /persist-stagnation: chain 42161/.test(a)));
  assert.equal(
    alerts.some((a) => /skew-arming:/.test(a)),
    false,
    'no skew-arming line without the taxonomy arg — existing callers are unaffected',
  );
});
