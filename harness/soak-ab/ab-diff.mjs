// ab-diff.mjs — hourly finalized-overlap differ for Soak A (RPC realtime) vs Soak B (Portal-native
// realtime, PORTAL_REALTIME=stream). Over [cutover, min(finalizedA, finalizedB) - margin] per chain:
//   • logs         : strict row-set + field identity (PRIMARY — must be 0)
//   • blocks       : STRICT row-set + field identity (total_difficulty excluded) — in the finalized
//     overlap a one-sided block is a real gap, not a tolerated inert block, so it FAILS
//   • transactions (shared): full-row identity — a tx in BOTH stores must be byte-identical
//   • transactions : asserted to be EXACTLY the expected class — B may be MISSING parent txs for
//     realtime-ingested spans (the verified stream wire gap), and every such tx must be referenced
//     by an A-side log. Any B-extra tx, or an A-only tx no log references, is a FAIL.
//   • per-1000-block ordered md5 checkpoint hashes both sides (persisted for drift tracking)
//   • _ponder_checkpoint monotonicity across runs
// Writes soak-ab-status.json {ts, chains, verdict, diffClasses, lagA, lagB, counters} for the monitor.
//
// Two "PG connections" = two `psql` processes (no npm driver → runs on the box with node + psql).
//   DATABASE_URL_A=… DATABASE_URL_B=… CHAINS=1,8453,42161 CUTOVER=<block> \
//   STATUS_FILE=soak-ab-status.json node harness/soak-ab/ab-diff.mjs
//
// AB_SCHEMA_B (REQUIRED in the real deployment): Soak B runs `ponder start --schema <app-schema>`
// (e.g. soak_b), so its `_ponder_checkpoint` table lives in that schema — NOT on psql's default
// search_path. The checkpoint monotonicity guard's query MUST qualify the table with that schema
// or it errors every run and silently falls back to the sync-store block max (dead guard). Set
// AB_SCHEMA_B to Soak B's DATABASE_SCHEMA value. Empty ⇒ unqualified `_ponder_checkpoint` (the
// legacy behavior, for a store on the default search_path). The identifier is sanitized before it
// reaches SQL.

import { spawn } from 'node:child_process';
import { readFileSync, renameSync, writeFileSync } from 'node:fs';
import { streamingDiff } from '../validate/diff-batched.mjs';

// ── pure, unit-tested core ─────────────────────────────────────────────────────────────────────

// KNOWN, FULLY-TOLERATED shared-tx divergence classes. Each entry is a NARROW, reportable, removable
// exception to the otherwise-strict shared-tx byte-identity check — never a general mask. Deleting an
// entry restores full strictness with no other code change (classifySharedTx returns 'mismatch' for
// anything not matched by a live entry).
//
// issue27AccessListNull — issue #27, root-caused in its authoritative comment: a non-compliant RPC
// provider omits the `accessList` key on typed realtime txs; upstream ponder tolerates the missing key
// and persists NULL (`encode.ts`: `accessList ? JSON.stringify(...) : null`) permanently. Side A
// (RPC-transport realtime) therefore stores access_list NULL for a span of realtime-ingested typed txs
// while side B (Portal /stream) stored the chain-true value. Every OTHER column of those rows is
// byte-identical (proven: md5 over `to_jsonb(t) - 'access_list'` equal on all three chains). The
// tolerance is scoped to EXACTLY that shape (see classifySharedTx) — A-side NULL, B-side non-null, all
// other columns identical, at/above the measured realtime-span floor per chain, within the (currently
// open) window. `perChainFloor` values are the measured min block_number of the class per chain;
// `toBlock` is null until the fork-side fix deploys and the realtime window closes — set it then, and
// once the whole span is finalized-and-past, DELETE this entry to restore full strictness.
export const TOLERATED_CLASSES = {
  issue27AccessListNull: {
    issue: 'https://github.com/subsquid-labs/portal-ponder/issues/27',
    perChainFloor: { 1: 25445239, 8453: 48092254, 42161: 479635494 },
    toBlock: null,
  },
};

// The expected-class assertion for the transactions table. `onlyA` = txs A has but B lacks; `onlyB`
// = txs B has but A lacks; `referenced` = the subset of onlyA that an A-side log points at;
// `sharedMismatch` = txs present on BOTH sides whose full-row hash diverges (default 0). A tx that
// EXISTS in both A and B must be byte-identical — the same finalized transaction indexed two ways.
export function classifyTxDiff(
  onlyA,
  onlyB,
  referenced,
  sharedMismatch = 0,
  toleratedIssue27 = { count: 0, perChain: {} },
) {
  const ref = referenced instanceof Set ? referenced : new Set(referenced);
  const unexpectedB = [...onlyB];
  const unreferencedA = onlyA.filter((h) => !ref.has(h));
  const fail =
    unexpectedB.length > 0 || unreferencedA.length > 0 || sharedMismatch > 0;

  return {
    fail,
    class: fail ? 'UNEXPECTED' : 'realtime-parent-tx-gap',
    expectedMissing: onlyA.length,
    unexpectedB,
    unreferencedA,
    sharedMismatch,
    // Reported, never fails: shared txs whose ONLY divergence is issue #27 (access_list NULL on A,
    // chain-true on B). Counted here so the run verdict is PASS-compatible while still visible.
    toleratedIssue27,
  };
}

// Classify ONE shared tx whose FULL-row md5 diverges between side A and side B. Returns 'tolerated'
// ONLY for the exact, fully-root-caused issue #27 shape; ANY other divergence returns 'mismatch' → a
// hard sharedMismatch → FAIL, exactly as before this class existed. Pure + exported so every
// adversarial case is unit-tested and mutation-verified in isolation.
//
// The predicate is deliberately a conjunction of five clauses, each refusing to mask a distinct thing:
//   1. exAlMd5A === exAlMd5B   — md5 over `to_jsonb(t) - 'access_list'`. If ANY second column also
//      differs, these differ → 'mismatch'. This is what stops the tolerance widening to "any diff on a
//      row that also happens to have an access_list gap".
//   2. aAccessListNull === true — the loss is on side A (the RPC-realtime leg that nulled the key). If
//      A is non-null-but-different from B, this is false → 'mismatch' (a real access_list divergence).
//   3. bAccessListNull === false — side B holds a concrete value (the chain-true one). An inverted
//      asymmetry (B null, A non-null) is NOT this class → 'mismatch'.
//   4. blockNumber >= floor      — at/above the measured realtime-span floor for THIS chain. A missing
//      floor entry (unknown chain) is an explicit HARD FAIL, never a default-tolerate.
//   5. toBlock === null || blockNumber <= toBlock — within the (open) window; once toBlock is set, a
//      row past it is NOT tolerated → 'mismatch'.
// A missing/deleted TOLERATED_CLASSES.issue27AccessListNull entry → no floors → 'mismatch' for all.
export function classifySharedTx(
  { blockNumber, exAlMd5A, exAlMd5B, aAccessListNull, bAccessListNull },
  chain,
  classes = TOLERATED_CLASSES,
) {
  const entry = classes?.issue27AccessListNull;
  if (!entry) {
    return 'mismatch';
  }

  // Missing floor for this chain ⇒ HARD FAIL — never default-tolerate an unknown chain.
  const floor = entry.perChainFloor?.[chain];
  if (!Number.isFinite(floor)) {
    return 'mismatch';
  }

  const block = Number(blockNumber);
  const withinWindow =
    entry.toBlock === null ||
    entry.toBlock === undefined ||
    block <= Number(entry.toBlock);

  const tolerated =
    exAlMd5A === exAlMd5B &&
    aAccessListNull === true &&
    bAccessListNull === false &&
    Number.isFinite(block) &&
    block >= floor &&
    withinWindow;

  return tolerated ? 'tolerated' : 'mismatch';
}

// A psql child is only a trustworthy data source if it exited cleanly. A non-zero exit (bad SQL,
// connection refused, auth failure) or a spawn error must NEVER be read as "zero rows" — that is the
// false-PASS class: an empty/partial stream silently compared as a completed diff. Pure so it can be
// asserted directly.
export function psqlExitVerdict({ code, signal, spawnError }) {
  if (spawnError) {
    return { ok: false, reason: `psql spawn failed: ${spawnError}` };
  }
  if (signal) {
    return { ok: false, reason: `psql killed by signal ${signal}` };
  }
  if (code !== 0) {
    return { ok: false, reason: `psql exited ${code}` };
  }

  return { ok: true };
}

// Split an array of hashes into fixed-size chunks (default 5000) for a chunked IN-list lookup. Pure +
// exported: the referenced-parent-tx lookup must verify ANY onlyA size, so it splits into bounded IN
// lists rather than skipping the lookup above a fixed threshold (the old `onlyA.length <= 200_000`
// gate classified every parent-tx gap in a large healthy soak as unreferenced → false FAIL).
export function chunk(items, size = 5000) {
  const out = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }

  return out;
}

// Collect the referenced subset of `hashes` by running `lookup(batch)` over EVERY chunk and unioning
// the results. `lookup` is an async fn batch → referenced-hashes-in-batch (the psql IN-list query in
// diffTx). Pure over its callback + exported so a mutation that stops after the first chunk (leaving
// later chunks' referenced hashes unfound) is caught by a test rather than surfacing as a false FAIL.
export async function collectReferenced(hashes, lookup, size = 5000) {
  const referenced = [];
  for (const batch of chunk(hashes, size)) {
    const found = await lookup(batch);
    for (const h of found) {
      referenced.push(h);
    }
  }

  return referenced;
}

// _ponder_checkpoint (or any progress value) must never go backwards across runs.
export function checkpointMonotonic(values) {
  for (let i = 1; i < values.length; i++) {
    if (BigInt(values[i]) < BigInt(values[i - 1])) {
      return {
        ok: false,
        at: i,
        prev: String(values[i - 1]),
        cur: String(values[i]),
      };
    }
  }

  return { ok: true };
}

// ── ponder checkpoint encoding (verified against ponder@0.16.6 source) ──────────────────────────
// packages/core src/utils/checkpoint.ts (published: package/dist/esm/utils/checkpoint.js). The 75-char
// `_ponder_checkpoint.latest_checkpoint` (VARCHAR(75), created in database/index.js) is:
//   blockTimestamp(10) chainId(16) blockNumber(16) transactionIndex(16) eventType(1) eventIndex(16)
// so blockNumber occupies 0-based offsets [26,42) — 1-based SQL substring position 27, length 16.
// We extract the BLOCK-NUMBER field (a plain integer height, Number-safe and on the SAME scale as the
// overlapBound sync-store block max), NOT Number(whole-encoded-checkpoint): the full ~75-digit value
// overflows Number's ~16 significant digits, so same-second block rewinds are invisible AND, after a
// later fallback to the small-scale block max, a checkpoint-scale prior value would wrongly read as a
// rewind (false FAIL). The block field keeps the monotonicity series coherent across fallback flapping.
export const CHECKPOINT_BLOCK_OFFSET_0 = 26; // 0-based char offset of blockNumber (10 + 16)
export const CHECKPOINT_BLOCK_LEN = 16; // BLOCK_NUMBER_DIGITS
// 1-based position for SQL substring(str from <pos> for <len>).
export const CHECKPOINT_BLOCK_SQL_POS = CHECKPOINT_BLOCK_OFFSET_0 + 1; // 27

// Extract the block-number field from a raw 75-char encoded checkpoint string. Returns the height as a
// decimal string (no Number() — the field is <=16 digits but the caller wants an exact, BigInt-safe
// value), or null for a malformed/short/absent checkpoint. Pure + exported: mutate the offset/len and
// the extraction test reads the wrong slice and fails.
export function extractCheckpointBlock(raw) {
  if (typeof raw !== 'string') {
    return null;
  }

  const seg = raw.slice(
    CHECKPOINT_BLOCK_OFFSET_0,
    CHECKPOINT_BLOCK_OFFSET_0 + CHECKPOINT_BLOCK_LEN,
  );
  if (seg.length !== CHECKPOINT_BLOCK_LEN || !/^\d+$/.test(seg)) {
    return null;
  }

  return String(BigInt(seg));
}

// Decide whether the `_ponder_checkpoint` query yielded a REPORTABLE progress value, given the row
// COUNT for the chain and the extracted max block. The count is what distinguishes the three states a
// bare `coalesce(max(...),0)` conflates:
//   • rowCount === 0 → NO checkpoint row for this chain → not usable; the caller falls back to the
//     sync-store block max (an older store, or the chain never committed a checkpoint).
//   • rowCount > 0 → a REAL committed checkpoint exists → usable, and value = maxBlock EVEN IF 0. A
//     valid checkpoint whose blockNumber field is exactly 0 is a real, reportable progress value —
//     the strongest form of the rewind this guard exists to catch (a resume that rewound the
//     committed checkpoint to block 0 while the sync store still holds a high block max). Reporting 0
//     lets monotonicity FAIL it against prior real progress; falling back to the (still-high) block
//     max would silently PASS that rewind. So a zero max with rows present is usable, NOT a fallback.
// Pure + exported so the rows-present-with-zero-max case is mutation-verified directly.
export function checkpointDecision(rowCount, maxBlock) {
  if (!(Number.isFinite(rowCount) && rowCount > 0)) {
    return { usable: false, value: null };
  }

  const value = Number(maxBlock);

  return {
    usable: Number.isFinite(value),
    value: Number.isFinite(value) ? value : null,
  };
}

// A schema name goes verbatim into `"<schema>"._ponder_checkpoint` — reject anything that is not a
// bare SQL identifier so it can never carry an injection payload. Empty ⇒ null (caller emits the
// unqualified table name, the legacy default-search_path behavior). Pure + exported.
export function sanitizeSchemaIdent(schema) {
  if (schema === undefined || schema === null || schema === '') {
    return null;
  }
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(schema)) {
    throw new Error(
      `AB_SCHEMA_B is not a valid SQL identifier: ${JSON.stringify(schema)}`,
    );
  }

  return schema;
}

// Compare per-bucket checkpoint hashes: shared buckets must match (determinism); buckets on only one
// side are reported (overlap edges), not failed.
export function compareBucketHashes(aBuckets, bBuckets) {
  const a =
    aBuckets instanceof Map ? aBuckets : new Map(Object.entries(aBuckets));
  const b =
    bBuckets instanceof Map ? bBuckets : new Map(Object.entries(bBuckets));
  const mismatches = [];
  for (const [bucket, ha] of a) {
    if (b.has(bucket) && b.get(bucket) !== ha) {
      mismatches.push({ bucket, a: ha, b: b.get(bucket) });
    }
  }
  const onlyA = [...a.keys()].filter((k) => !b.has(k)).length;
  const onlyB = [...b.keys()].filter((k) => !a.has(k)).length;

  return { ok: mismatches.length === 0, mismatches, onlyA, onlyB };
}

// Restart accounting from the systemd ExecStartPre log (one UTC-timestamp line per (re)start). In
// stream mode a fatal unknown-head / reorg-gap exits 75 and systemd restarts — designed recovery, so
// only the RATE matters: >3 restarts in the trailing hour is a crash-loop alert.
export function restartStats(lines, nowMs = Date.now()) {
  const stamps = [];
  for (const line of lines) {
    const m = line.trim().match(/^(\d{4}-\d\d-\d\dT[\d:.]+Z)/);
    if (m) {
      stamps.push(Date.parse(m[1]));
    }
  }
  stamps.sort((x, y) => x - y);

  const hourAgo = nowMs - 3_600_000;
  const restartsLastHour = stamps.filter((t) => t >= hourAgo).length;
  const lastRestartAt =
    stamps.length > 0
      ? new Date(stamps[stamps.length - 1]).toISOString()
      : null;

  return {
    restartCount: stamps.length,
    lastRestartAt,
    restartsLastHour,
    crashLoop: restartsLastHour > 3,
  };
}

// ── psql adapters ──────────────────────────────────────────────────────────────────────────────

const SEP = '\x1f'; // unit separator — never appears in the hex/numeric fields we select

// Stream rows of a query as string[] (fields split on SEP), constant memory. `-v ON_ERROR_STOP=1`
// makes a mid-query server error a non-zero exit rather than a partial stream; we additionally await
// the child's terminal state and throw on any non-clean exit (spawn error / signal / non-zero code)
// so a failed query can NEVER be silently read as "zero rows" (the false-PASS class).
async function* psqlRows(url, sql) {
  const proc = spawn(
    'psql',
    [
      url,
      '-X',
      '-q',
      '-A',
      '-t',
      '-v',
      'ON_ERROR_STOP=1',
      '-F',
      SEP,
      '-c',
      sql,
    ],
    {
      stdio: ['ignore', 'pipe', 'inherit'],
    },
  );

  let spawnError = null;
  proc.on('error', (e) => {
    spawnError = e.message;
  });

  const exited = new Promise((resolve) => {
    proc.on('close', (code, signal) => {
      resolve({ code, signal });
    });
  });

  let buf = '';
  for await (const chunk of proc.stdout) {
    buf += chunk.toString('utf8');
    let nl = buf.indexOf('\n');
    while (nl >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (line.length > 0) {
        yield line.split(SEP);
      }

      nl = buf.indexOf('\n');
    }
  }

  if (buf.trim().length > 0) {
    yield buf.split(SEP);
  }

  const { code, signal } = await exited;
  const verdict = psqlExitVerdict({ code, signal, spawnError });
  if (!verdict.ok) {
    throw new Error(verdict.reason);
  }
}

async function psqlScalar(url, sql) {
  for await (const row of psqlRows(url, sql)) {
    return row[0];
  }

  return null;
}

async function psqlList(url, sql) {
  const out = [];
  for await (const row of psqlRows(url, sql)) {
    out.push(row[0]);
  }

  return out;
}

// Turn a keyed hash stream ({key:[...], hash}) into rows streamingDiff can compare: the hash is the
// only field, so a field mismatch on a shared key surfaces as a row mismatch.
function hashRowsIter(url, sql, keyIdx) {
  return (async function* () {
    for await (const row of psqlRows(url, sql)) {
      yield {
        key: keyIdx.map((i) => BigInt(row[i])),
        hash: row[row.length - 1],
      };
    }
  })();
}

const keyFn = (r) => r.key;

// ── per-chain comparison ─────────────────────────────────────────────────────────────────────

async function overlapBound(url, chain) {
  const v = await psqlScalar(
    url,
    `select coalesce(max(number),0) from ponder_sync.blocks where chain_id=${chain}`,
  );

  return Number(v ?? 0);
}

// Real indexing progress for the monotonicity guard: ponder's own `_ponder_checkpoint` row (in the
// APP schema, not ponder_sync) is the source of truth for how far indexing has committed. The encoded
// `latest_checkpoint` is a fixed-width string whose blockNumber field sits at offsets [26,42) (see the
// verified layout above) — we extract THAT field as a plain block height. max(ponder_sync.blocks.number)
// is only how far the SYNC store reached, which can move independently of committed indexing progress;
// a resume that rewound the COMMITTED checkpoint but kept sync-store rows would slip past a block-max
// guard. Falls back to the sync-store block max only if `_ponder_checkpoint` is absent/unreadable
// (older store, or wrong schema) so the guard is never silently disabled.
async function checkpointProgress(url, chain, schema) {
  // `_ponder_checkpoint` lives in Soak B's APP schema (ponder start --schema <schema>), which is NOT
  // on psql's default search_path — so we QUALIFY the table with the (sanitized) schema. Empty schema
  // ⇒ unqualified (legacy default-search_path stores). One query returns TWO values: the ROW COUNT for
  // this chain and the blockNumber SUBSTRING of the encoded checkpoint (position CHECKPOINT_BLOCK_SQL_POS,
  // length CHECKPOINT_BLOCK_LEN) cast to numeric — a Number-safe height on the SAME scale as the
  // overlapBound fallback, so the monotonicity series stays coherent even when a run flaps between the
  // two sources.
  //
  // Why BOTH values: a bare `coalesce(max(...),0)` cannot tell a chain with NO checkpoint row (max is
  // NULL → 0) apart from a VALID checkpoint whose blockNumber field is exactly 0 (a resume that rewound
  // the committed checkpoint to block 0 while the sync store still holds a high block max). The zero-max
  // rewind is the STRONGEST form of the rewind this guard exists to catch — the row count distinguishes
  // it (checkpointDecision): rows present ⇒ report the value even if 0, so monotonicity FAILs it; zero
  // rows ⇒ fall back.
  const table = schema
    ? `"${schema}"._ponder_checkpoint`
    : '_ponder_checkpoint';
  try {
    let row = null;
    for await (const r of psqlRows(
      url,
      `select count(*)::text, ` +
        `coalesce(max(substring(latest_checkpoint from ${CHECKPOINT_BLOCK_SQL_POS} for ${CHECKPOINT_BLOCK_LEN})::numeric),0)::text ` +
        `from ${table} where chain_id=${chain}`,
    )) {
      row = r;
      break;
    }

    const rowCount = Number(row?.[0] ?? 0);
    const maxBlock = row?.[1] ?? '0';
    const decision = checkpointDecision(rowCount, maxBlock);
    if (decision.usable) {
      return decision.value;
    }
    // rows === 0 for this chain: NO committed checkpoint (older store, or the chain never committed).
    // We fall back to the sync-store block max below rather than disabling the guard. CANDID RESIDUAL:
    // an APP-schema FULL RESET that leaves ZERO checkpoint rows (e.g. the table truncated/re-created)
    // is INDISTINGUISHABLE here from a store that never had a checkpoint — both read rows===0, so we
    // fall back to the (still-high) sync-store block max and monotonicity can PASS silently, masking
    // that rewind. This is the guard's remaining blind spot; detecting a zero-ROW reset needs
    // cross-run source tracking (was there a checkpoint row last run that is gone now?) — out of scope.
  } catch {
    // _ponder_checkpoint not resolvable (older store / different schema) — fall through to the
    // sync-store block max below rather than disabling the guard.
  }

  return overlapBound(url, chain);
}

async function diffLogs(urlA, urlB, chain, lo, hi) {
  const sql =
    `select block_number, log_index, md5(to_jsonb(t)::text) from ponder_sync.logs t ` +
    `where chain_id=${chain} and block_number between ${lo} and ${hi} order by block_number, log_index`;

  return streamingDiff(
    hashRowsIter(urlA, sql, [0, 1]),
    hashRowsIter(urlB, sql, [0, 1]),
    {
      keyFn,
      mode: 'strict',
    },
  );
}

async function diffBlocks(urlA, urlB, chain, lo, hi) {
  // total_difficulty is excluded in SQL (meaningless post-Merge, RPC-dependent). In the FINALIZED
  // overlap [cutover, min(finalizedA,finalizedB)-margin] both realtime paths must have every block:
  // a one-sided block here is a real gap (a block one store missed), so this is STRICT, not the
  // 'blocks' tolerance harness/diff/diff.mjs uses for the stock RPC path's inert event-less blocks
  // — those never appear in the realtime A/B stores, which only persist event-bearing blocks.
  const sql = () =>
    `select number, md5((to_jsonb(t)-'total_difficulty')::text) from ponder_sync.blocks t ` +
    `where chain_id=${chain} and number between ${lo} and ${hi} order by number`;

  return streamingDiff(
    hashRowsIter(urlA, sql(), [0]),
    hashRowsIter(urlB, sql(), [0]),
    {
      keyFn,
      mode: 'strict',
    },
  );
}

// tx class: merge the two tx streams (each row = [hash, md5(full row)]) to onlyA/onlyB AND to a
// SHARED set whose row hashes must be byte-identical, then ask A which onlyA hashes a log references.
// A tx present on both sides is the SAME finalized transaction indexed two ways — every field must
// match. The full-row md5 mirrors the strict transactions identity diff-batched.mjs already asserts:
// no column is excluded (unlike blocks' total_difficulty, no transactions column legitimately differs
// between the RPC-sourced A store and the Portal-sourced B store — both write the same ponder_sync
// schema from the same finalized chain data).
async function diffTx(urlA, urlB, chain, lo, hi) {
  // Per side per tx: hash, full-row md5, block_number, ex-access_list md5 (the jsonb-minus idiom used
  // for total_difficulty in diffBlocks), and (access_list IS NULL) as psql's t/f flag. The extra
  // columns are what classifySharedTx needs to tell the tolerated issue #27 shape apart from any real
  // divergence WITHOUT loosening the strict full-row identity for every other column.
  const txSql =
    `select "hash", md5(to_jsonb(t)::text), block_number, ` +
    `md5((to_jsonb(t)-'access_list')::text), (access_list is null) ` +
    `from ponder_sync.transactions t ` +
    `where chain_id=${chain} and block_number between ${lo} and ${hi} order by "hash"`;
  const onlyA = [];
  const onlyB = [];
  let sharedMismatch = 0;
  const toleratedIssue27 = { count: 0, perChain: {} };
  const ia = psqlRows(urlA, txSql)[Symbol.asyncIterator]();
  const ib = psqlRows(urlB, txSql)[Symbol.asyncIterator]();
  let a = await ia.next();
  let b = await ib.next();
  while (!a.done || !b.done) {
    const ha = a.done ? null : a.value[0];
    const hb = b.done ? null : b.value[0];
    if (hb === null || (ha !== null && ha < hb)) {
      onlyA.push(ha);
      a = await ia.next();
    } else if (ha === null || hb < ha) {
      onlyB.push(hb);
      b = await ib.next();
    } else {
      // shared tx (same hash on both sides) — the full-row md5 must be identical, EXCEPT for the one
      // fully-root-caused, reported tolerated class (issue #27); anything else is a hard mismatch.
      if (a.value[1] !== b.value[1]) {
        const verdict = classifySharedTx(
          {
            blockNumber: a.value[2],
            exAlMd5A: a.value[3],
            exAlMd5B: b.value[3],
            aAccessListNull: a.value[4] === 't',
            bAccessListNull: b.value[4] === 't',
          },
          chain,
        );
        if (verdict === 'tolerated') {
          toleratedIssue27.count += 1;
          toleratedIssue27.perChain[chain] =
            (toleratedIssue27.perChain[chain] ?? 0) + 1;
        } else {
          sharedMismatch += 1;
        }
      }

      a = await ia.next();
      b = await ib.next();
    }
  }

  // Which onlyA hashes are referenced by an A-side log (a legit realtime parent-tx gap)? Verify EVERY
  // onlyA hash regardless of count by chunking the IN list into bounded queries (no fixed threshold):
  // the old `onlyA.length <= 200_000` skip left `referenced=[]` on a large healthy soak, so
  // classifyTxDiff saw every gap as unreferenced and mass-FAILed a healthy run. collectReferenced
  // accumulates across every chunk so any onlyA size is fully verified.
  const referenced = await collectReferenced(onlyA, (batch) => {
    const inList = batch.map((h) => `'${h.replace(/'/g, '')}'`).join(',');

    return psqlList(
      urlA,
      `select distinct transaction_hash from ponder_sync.logs where chain_id=${chain} and transaction_hash in (${inList})`,
    );
  });

  return classifyTxDiff(
    onlyA,
    onlyB,
    referenced,
    sharedMismatch,
    toleratedIssue27,
  );
}

async function bucketHashes(url, chain, lo, hi, bucket) {
  const sql =
    `select (block_number/${bucket})::text, md5(string_agg(md5(to_jsonb(t)::text), ',' order by block_number, log_index)) ` +
    `from ponder_sync.logs t where chain_id=${chain} and block_number between ${lo} and ${hi} group by 1 order by 1`;
  const map = new Map();
  for await (const row of psqlRows(url, sql)) {
    map.set(row[0], row[1]);
  }

  return map;
}

async function compareChain(
  urlA,
  urlB,
  chain,
  cutover,
  margin,
  bucket,
  schemaB,
) {
  const [boundA, boundB, progressB] = await Promise.all([
    overlapBound(urlA, chain),
    overlapBound(urlB, chain),
    // Soak B's COMMITTED indexing progress from `_ponder_checkpoint` — the real value the
    // monotonicity guard asserts, not merely how far the sync store reached (see checkpointProgress).
    checkpointProgress(urlB, chain, schemaB),
  ]);
  const hi = Math.min(boundA, boundB) - margin;
  const lo = cutover;
  const out = {
    chain,
    lo,
    hi,
    lagA: 0,
    lagB: 0,
    // Soak B's committed indexing checkpoint — the per-run progress value monotonicity is asserted
    // against (never rewinds across hourly runs).
    progressB,
    verdict: 'PASS',
    classes: {},
  };
  out.lagA = boundA - Math.min(boundA, boundB);
  out.lagB = boundB - Math.min(boundA, boundB);

  if (hi < lo) {
    out.verdict = 'PENDING';
    out.classes.note = `no finalized overlap yet (lo=${lo} hi=${hi})`;

    return out;
  }

  const [logs, blocks, tx] = await Promise.all([
    diffLogs(urlA, urlB, chain, lo, hi),
    diffBlocks(urlA, urlB, chain, lo, hi),
    diffTx(urlA, urlB, chain, lo, hi),
  ]);
  const [ba, bb] = await Promise.all([
    bucketHashes(urlA, chain, lo, hi, bucket),
    bucketHashes(urlB, chain, lo, hi, bucket),
  ]);
  const buckets = compareBucketHashes(ba, bb);

  out.classes = {
    logs: {
      fail: logs.fail,
      onlyA: logs.onlyA,
      onlyB: logs.onlyB,
      mismatch: logs.mismatch,
      shared: logs.shared,
    },
    blocks: {
      fail: blocks.fail,
      onlyA: blocks.onlyA,
      onlyB: blocks.onlyB,
      mismatch: blocks.mismatch,
    },
    transactions: tx,
    checkpointBuckets: {
      ok: buckets.ok,
      mismatches: buckets.mismatches.length,
    },
  };
  if (logs.fail || blocks.fail || tx.fail || !buckets.ok) {
    out.verdict = 'FAIL';
  }

  return out;
}

async function main() {
  const urlA = process.env.DATABASE_URL_A;
  const urlB = process.env.DATABASE_URL_B;
  if (!urlA || !urlB) {
    console.error('ab-diff: set DATABASE_URL_A and DATABASE_URL_B');
    process.exit(2);
  }

  const chains = (process.env.CHAINS ?? '1,8453,42161')
    .split(',')
    .map((s) => Number(s.trim()));
  const cutover = Number(process.env.CUTOVER ?? 0);
  const margin = Number(process.env.FINALITY_MARGIN ?? 64);
  const bucket = Number(process.env.BUCKET ?? 1000);
  // Schema Soak B's `_ponder_checkpoint` lives in (its DATABASE_SCHEMA / `ponder start --schema`
  // value). Unset ⇒ unqualified (default search_path). Sanitized — it goes into SQL. A bad identifier
  // fails loud here (before any per-chain query) rather than silently disabling the checkpoint guard.
  const schemaB = sanitizeSchemaIdent(process.env.AB_SCHEMA_B ?? '');
  const statusFile = process.env.STATUS_FILE ?? 'soak-ab-status.json';
  const checkpointFile =
    process.env.CHECKPOINT_FILE ?? 'soak-ab-checkpoints.json';

  const results = [];
  for (const chain of chains) {
    try {
      results.push(
        await compareChain(urlA, urlB, chain, cutover, margin, bucket, schemaB),
      );
    } catch (e) {
      results.push({ chain, verdict: 'ERROR', classes: { error: e.message } });
    }
  }

  // Checkpoint monotonicity across runs: Soak B's per-chain progress must never rewind between
  // hourly runs. A regression (a resume/restart that lost ground) is a hard FAIL, wired into both
  // the verdict/exit code and the alerts — not merely logged.
  const prior = loadPriorCheckpoints(checkpointFile);
  const nextCheckpoints = { ...prior };
  const regressions = [];
  for (const r of results) {
    if (r.progressB === undefined) {
      continue;
    }

    const history = Array.isArray(prior[r.chain]) ? prior[r.chain] : [];
    const series = [...history, r.progressB];
    const mono = checkpointMonotonic(series);
    if (!mono.ok) {
      regressions.push({ chain: r.chain, prev: mono.prev, cur: mono.cur });
      r.verdict = 'FAIL';
      r.classes = { ...r.classes, checkpointRegression: mono };
    }

    // keep a bounded tail so the file does not grow unbounded across a long soak
    nextCheckpoints[r.chain] = series.slice(-64);
  }

  const verdict = results.some(
    (r) => r.verdict === 'FAIL' || r.verdict === 'ERROR',
  )
    ? 'FAIL'
    : results.every((r) => r.verdict === 'PASS')
      ? 'PASS'
      : 'PENDING';

  // Soak B restart signal (from the systemd ExecStartPre log).
  const restartLog =
    process.env.RESTART_LOG ?? `${process.env.HOME ?? '.'}/soak-b-restarts.log`;
  let restarts = {
    restartCount: 0,
    lastRestartAt: null,
    restartsLastHour: 0,
    crashLoop: false,
  };
  try {
    restarts = restartStats(readFileSync(restartLog, 'utf8').split('\n'));
  } catch {
    // no restart log yet — Soak B not started, or first boot
  }

  const alerts = [];
  if (restarts.crashLoop) {
    alerts.push(
      `crash-loop: ${restarts.restartsLastHour} restarts in the last hour (>3)`,
    );
  }
  for (const reg of regressions) {
    alerts.push(
      `checkpoint-regression: chain ${reg.chain} rewound ${reg.prev} → ${reg.cur} between runs`,
    );
  }
  if (verdict === 'FAIL' && regressions.length === 0) {
    alerts.push(
      'finalized-diff: an unexpected finalized-overlap divergence (see diffClasses)',
    );
  }

  const toleratedIssue27 = aggregateToleratedIssue27(results);

  const status = {
    ts: new Date().toISOString(),
    chains: results.map((r) => r.chain),
    verdict,
    restartCount: restarts.restartCount,
    lastRestartAt: restarts.lastRestartAt,
    restartsLastHour: restarts.restartsLastHour,
    alerts,
    diffClasses: Object.fromEntries(results.map((r) => [r.chain, r.classes])),
    lagA: Object.fromEntries(results.map((r) => [r.chain, r.lagA ?? null])),
    lagB: Object.fromEntries(results.map((r) => [r.chain, r.lagB ?? null])),
    checkpointRegressions: regressions,
    // Aggregate of the reported-but-tolerated issue #27 shared-tx class across all chains. A run whose
    // only divergence is this class is PASS (never fails the verdict); the count keeps it VISIBLE.
    toleratedIssue27,
    counters: Object.fromEntries(
      results.map((r) => [r.chain, { lo: r.lo, hi: r.hi, verdict: r.verdict }]),
    ),
  };

  writeFileSync(statusFile, `${JSON.stringify(status, null, 2)}\n`);
  // The checkpoint file is the monotonicity ledger — a torn write (process killed mid-write) would
  // corrupt it and, on the next run, either fail-closed on the parse error or lose the prior series
  // (silently disabling the rewind guard). Write it atomically: full temp file + rename, so a reader
  // ever sees only the old complete file or the new complete file, never a partial one.
  writeJsonAtomic(checkpointFile, nextCheckpoints);
  console.log(JSON.stringify(status, null, 2));
  const toleratedLine = formatToleratedIssue27Line(toleratedIssue27);
  if (toleratedLine) {
    console.log(toleratedLine);
  }

  process.exit(verdict === 'FAIL' ? 1 : 0);
}

// Sum the per-chain issue #27 tolerated tallies from every chain result into one {count, perChain}.
// Pure + exported: the status JSON's top-level counter and the human line both read this, so a
// mutation that miscounts is caught directly rather than only through a full run.
export function aggregateToleratedIssue27(results) {
  const perChain = {};
  let count = 0;
  for (const r of results) {
    const tol = r?.classes?.transactions?.toleratedIssue27;
    if (!tol) {
      continue;
    }

    count += tol.count ?? 0;
    for (const [chain, n] of Object.entries(tol.perChain ?? {})) {
      perChain[chain] = (perChain[chain] ?? 0) + n;
    }
  }

  return { count, perChain };
}

// One human-readable line for a run that carried tolerated issue #27 rows (empty string ⇒ print
// nothing). The wording is deliberately loud about REMOVAL so the class cannot quietly become
// permanent. Pure + exported so the message contract is asserted.
export function formatToleratedIssue27Line(tolerated) {
  if (!tolerated || (tolerated.count ?? 0) <= 0) {
    return '';
  }

  const breakdown = Object.entries(tolerated.perChain ?? {})
    .map(([chain, n]) => `${chain}:${n}`)
    .join(', ');

  return (
    `TOLERATED (known issue #27 — REMOVE when the fix deploys and the window closes): ` +
    `${tolerated.count} access_list-null rows (${breakdown})`
  );
}

// Atomic JSON write: serialize to a sibling temp file, then rename over the target (rename is atomic
// within a filesystem). Exported so a mutation that skips the rename is caught by a test.
export function writeJsonAtomic(file, data) {
  const tmp = `${file}.tmp.${process.pid}`;
  writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`);
  renameSync(tmp, file);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(e);
    process.exit(2);
  });
}

// re-export for a caller that wants to persist prior checkpoints and assert monotonicity across runs
export function loadPriorCheckpoints(file) {
  try {
    return JSON.parse(readFileSync(file, 'utf8'));
  } catch {
    return {};
  }
}
