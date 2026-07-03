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

import { spawn } from 'node:child_process';
import { readFileSync, renameSync, writeFileSync } from 'node:fs';
import { streamingDiff } from '../validate/diff-batched.mjs';

// ── pure, unit-tested core ─────────────────────────────────────────────────────────────────────

// The expected-class assertion for the transactions table. `onlyA` = txs A has but B lacks; `onlyB`
// = txs B has but A lacks; `referenced` = the subset of onlyA that an A-side log points at;
// `sharedMismatch` = txs present on BOTH sides whose full-row hash diverges (default 0). A tx that
// EXISTS in both A and B must be byte-identical — the same finalized transaction indexed two ways.
export function classifyTxDiff(onlyA, onlyB, referenced, sharedMismatch = 0) {
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
  };
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
// APP schema, not ponder_sync) is the source of truth for how far indexing has committed — the
// encoded checkpoint embeds the block height in its leading digits (a fixed-width zero-padded,
// lexicographically-ordered string). max(ponder_sync.blocks.number) is only how far the SYNC store
// reached, which can move independently of committed indexing progress; a resume that rewound the
// COMMITTED checkpoint but kept sync-store rows would slip past a block-max guard. We extract the
// block-height integer from the latest checkpoint across all `_ponder_checkpoint` rows for this
// chain. Falls back to the sync-store block max only if `_ponder_checkpoint` is absent (older stores)
// so the guard is never silently disabled.
async function checkpointProgress(url, chain) {
  // Probe `_ponder_checkpoint` directly and tolerate its absence. The encoded `latest_checkpoint`
  // embeds the committed block height in its leading segment; strip non-digits so we get a comparable
  // numeric height regardless of the exact checkpoint encoding width across ponder versions. On ANY
  // error (missing table/column — an older store, or the table not on the search_path) we fall back
  // to the sync-store block max so the monotonicity guard is never silently disabled.
  try {
    const v = await psqlScalar(
      url,
      `select coalesce(max(nullif(regexp_replace(split_part(latest_checkpoint,'_',1),'\\D','','g'),'')::numeric),0)::text ` +
        `from _ponder_checkpoint where chain_id=${chain}`,
    );
    const n = Number(v ?? 0);
    if (Number.isFinite(n) && n > 0) {
      return n;
    }
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
  const txSql =
    `select "hash", md5(to_jsonb(t)::text) from ponder_sync.transactions t ` +
    `where chain_id=${chain} and block_number between ${lo} and ${hi} order by "hash"`;
  const onlyA = [];
  const onlyB = [];
  let sharedMismatch = 0;
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
      // shared tx (same hash on both sides) — the full-row md5 must be identical.
      if (a.value[1] !== b.value[1]) {
        sharedMismatch += 1;
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

  return classifyTxDiff(onlyA, onlyB, referenced, sharedMismatch);
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

async function compareChain(urlA, urlB, chain, cutover, margin, bucket) {
  const [boundA, boundB, progressB] = await Promise.all([
    overlapBound(urlA, chain),
    overlapBound(urlB, chain),
    // Soak B's COMMITTED indexing progress from `_ponder_checkpoint` — the real value the
    // monotonicity guard asserts, not merely how far the sync store reached (see checkpointProgress).
    checkpointProgress(urlB, chain),
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
  const statusFile = process.env.STATUS_FILE ?? 'soak-ab-status.json';
  const checkpointFile =
    process.env.CHECKPOINT_FILE ?? 'soak-ab-checkpoints.json';

  const results = [];
  for (const chain of chains) {
    try {
      results.push(
        await compareChain(urlA, urlB, chain, cutover, margin, bucket),
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
  process.exit(verdict === 'FAIL' ? 1 : 0);
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
