import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  CHECKPOINT_BLOCK_LEN,
  CHECKPOINT_BLOCK_OFFSET_0,
  checkpointDecision,
  checkpointMonotonic,
  chunk,
  classifyTxDiff,
  collectReferenced,
  compareBucketHashes,
  extractCheckpointBlock,
  psqlExitVerdict,
  restartStats,
  sanitizeSchemaIdent,
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
