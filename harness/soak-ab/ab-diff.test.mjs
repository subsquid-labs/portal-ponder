import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  aggregateKnownBadRows,
  aggregateToleratedIssue27,
  buildTxSql,
  CHECKPOINT_BLOCK_LEN,
  CHECKPOINT_BLOCK_OFFSET_0,
  checkpointDecision,
  checkpointMonotonic,
  chunk,
  classifySharedTx,
  classifyTxDiff,
  collectReferenced,
  compareBucketHashes,
  extractCheckpointBlock,
  formatKnownBadRowsLine,
  formatToleratedIssue27Line,
  knownBadRows,
  psqlExitVerdict,
  restartStats,
  sanitizeSchemaIdent,
  TOLERATED_CLASSES,
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
