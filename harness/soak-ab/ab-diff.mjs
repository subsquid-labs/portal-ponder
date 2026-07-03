// ab-diff.mjs — hourly finalized-overlap differ for Soak A (RPC realtime) vs Soak B (Portal-native
// realtime, PORTAL_REALTIME=stream). Over [cutover, min(finalizedA, finalizedB) - margin] per chain:
//   • logs         : strict row-set + field identity (PRIMARY — must be 0)
//   • blocks       : field identity, total_difficulty excluded
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
import { readFileSync, writeFileSync } from 'node:fs';
import { streamingDiff } from '../validate/diff-batched.mjs';

// ── pure, unit-tested core ─────────────────────────────────────────────────────────────────────

// The expected-class assertion for the transactions table. `onlyA` = txs A has but B lacks; `onlyB`
// = txs B has but A lacks; `referenced` = the subset of onlyA that an A-side log points at.
export function classifyTxDiff(onlyA, onlyB, referenced) {
  const ref = referenced instanceof Set ? referenced : new Set(referenced);
  const unexpectedB = [...onlyB];
  const unreferencedA = onlyA.filter((h) => !ref.has(h));
  const fail = unexpectedB.length > 0 || unreferencedA.length > 0;

  return {
    fail,
    class: fail ? 'UNEXPECTED' : 'realtime-parent-tx-gap',
    expectedMissing: onlyA.length,
    unexpectedB,
    unreferencedA,
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
  const sql = () =>
    `select number, md5((to_jsonb(t)-'total_difficulty')::text) from ponder_sync.blocks t ` +
    `where chain_id=${chain} and number between ${lo} and ${hi} order by number`;

  return streamingDiff(
    hashRowsIter(urlA, sql(), [0]),
    hashRowsIter(urlB, sql(), [0]),
    {
      keyFn,
      mode: 'blocks',
    },
  );
}

// tx class: merge tx-hash streams to onlyA/onlyB, then ask A which onlyA hashes a log references.
async function diffTx(urlA, urlB, chain, lo, hi) {
  const txSql = `select "hash" from ponder_sync.transactions where chain_id=${chain} and block_number between ${lo} and ${hi} order by "hash"`;
  const onlyA = [];
  const onlyB = [];
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
      a = await ia.next();
      b = await ib.next();
    }
  }

  let referenced = [];
  if (onlyA.length > 0 && onlyA.length <= 200_000) {
    const inList = onlyA.map((h) => `'${h.replace(/'/g, '')}'`).join(',');
    referenced = await psqlList(
      urlA,
      `select distinct transaction_hash from ponder_sync.logs where chain_id=${chain} and transaction_hash in (${inList})`,
    );
  }

  return classifyTxDiff(onlyA, onlyB, referenced);
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
  const [boundA, boundB] = await Promise.all([
    overlapBound(urlA, chain),
    overlapBound(urlB, chain),
  ]);
  const hi = Math.min(boundA, boundB) - margin;
  const lo = cutover;
  const out = {
    chain,
    lo,
    hi,
    lagA: 0,
    lagB: 0,
    // Soak B's max synced block — the per-run progress value monotonicity is asserted against.
    progressB: boundB,
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
  writeFileSync(
    checkpointFile,
    `${JSON.stringify(nextCheckpoints, null, 2)}\n`,
  );
  console.log(JSON.stringify(status, null, 2));
  process.exit(verdict === 'FAIL' ? 1 : 0);
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
