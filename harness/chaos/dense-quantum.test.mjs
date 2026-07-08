import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

const CONFIGURED_TRACE_DIRS = [
  process.env.CHAOS_DENSE_TRACE_DIR,
  process.env.CHAOS_ART,
].filter(Boolean);
const DEFAULT_TRACE_DIRS =
  CONFIGURED_TRACE_DIRS.length > 0
    ? CONFIGURED_TRACE_DIRS
    : [
        '/tmp/chaos-92-dense/artifacts',
        '/tmp/chaos-92-dense-smoke/artifacts',
        path.resolve('harness/chaos/.chaos-pg/artifacts'),
      ];

function uniqueFiles(files) {
  return [...new Set(files)].sort();
}

function findTraceFiles() {
  const files = [];
  if (process.env.CHAOS_DENSE_TRACE_FILE) {
    files.push(process.env.CHAOS_DENSE_TRACE_FILE);
  }

  for (const dir of DEFAULT_TRACE_DIRS) {
    if (!fs.existsSync(dir)) continue;
    for (const name of fs.readdirSync(dir)) {
      if (name.endsWith('.dense-trace.jsonl')) {
        files.push(path.join(dir, name));
      }
    }
  }

  return uniqueFiles(files).filter((file) => fs.existsSync(file));
}

function parseDenseLine(line, source) {
  if (line.trim() === '') return undefined;

  try {
    const record = JSON.parse(line);
    if (record.event === 'portal-dense-discovery-scan') {
      return {
        type: 'scan',
        source,
        run: Number(record.run ?? 0),
        attempt: Number(record.attempt ?? 0),
        pid: Number(record.pid ?? 0),
        seq: Number(record.seq ?? 0),
        fromBlock: Number(record.fromBlock),
        toBlock: Number(record.toBlock),
        span: Number(record.span),
        windows: Number(record.windows ?? 0),
      };
    }
    if (record.event === 'portal-dense-env') {
      return {
        type: 'env',
        source,
        run: Number(record.run ?? 0),
        attempt: Number(record.attempt ?? 0),
        warmupBlocks: Number(record.warmupBlocks),
        chunkBlocks: Number(record.chunkBlocks),
      };
    }
  } catch {
    // Fall through to the text format parser.
  }

  const scan = line.match(
    /\[portalDenseTrace\]\s+kind=discovery_scan\s+fromBlock=(\d+)\s+toBlock=(\d+)\s+span=(\d+)\s+windows=(\d+)/,
  );
  if (scan) {
    return {
      type: 'scan',
      source,
      run: 0,
      attempt: 0,
      pid: 0,
      seq: 0,
      fromBlock: Number(scan[1]),
      toBlock: Number(scan[2]),
      span: Number(scan[3]),
      windows: Number(scan[4]),
    };
  }

  return undefined;
}

function loadDenseTrace() {
  const files = findTraceFiles();
  const scans = [];
  const env = [];

  for (const file of files) {
    const text = fs.readFileSync(file, 'utf8');
    for (const line of text.split('\n')) {
      const parsed = parseDenseLine(line, file);
      if (parsed?.type === 'scan') scans.push(parsed);
      if (parsed?.type === 'env') env.push(parsed);
    }
  }

  scans.sort(
    (a, b) =>
      a.source.localeCompare(b.source) ||
      a.run - b.run ||
      a.attempt - b.attempt ||
      a.pid - b.pid ||
      a.seq - b.seq ||
      a.fromBlock - b.fromBlock,
  );

  return { files, scans, env };
}

function groupBy(scans, keyFn) {
  const groups = new Map();
  for (const scan of scans) {
    const key = keyFn(scan);
    const group = groups.get(key);
    if (group) group.push(scan);
    else groups.set(key, [scan]);
  }

  return groups;
}

function collapseAdjacentDuplicates(scans) {
  const out = [];
  for (const scan of scans) {
    const prev = out.at(-1);
    if (
      prev &&
      prev.fromBlock === scan.fromBlock &&
      prev.toBlock === scan.toBlock &&
      prev.span === scan.span
    ) {
      continue;
    }
    out.push(scan);
  }

  return out;
}

function findDoublingEvidence(scans, warmupBlocks) {
  let best = [];
  const groups = groupBy(
    scans,
    (scan) => `${scan.source}:${scan.run}:${scan.attempt}:${scan.pid}`,
  );

  for (const group of groups.values()) {
    const ordered = collapseAdjacentDuplicates(group).filter(
      (scan) => scan.span >= warmupBlocks,
    );
    for (let i = 0; i < ordered.length; i++) {
      const chain = [ordered[i]];
      let next = ordered[i].span * 2;
      for (let j = i + 1; j < ordered.length; j++) {
        if (ordered[j].span !== next) continue;
        chain.push(ordered[j]);
        next *= 2;
        if (chain.length >= 4) return chain;
      }
      if (chain.length > best.length) best = [...chain];
    }
  }

  return best;
}

function warmupFrom(env) {
  for (const record of env) {
    if (Number.isFinite(record.warmupBlocks) && record.warmupBlocks > 0) {
      return record.warmupBlocks;
    }
  }

  return Number(process.env.PORTAL_WARMUP_BLOCKS || 2000);
}

const trace = loadDenseTrace();
const noTraceSkip =
  trace.files.length === 0
    ? 'set CHAOS_DENSE_TRACE_DIR or run the dense chaos smoke first'
    : false;

test('dense-source shape engages at least 3 successive geometric discovery-scan doublings', {
  skip: noTraceSkip,
}, () => {
  assert.ok(
    trace.scans.length > 0,
    `dense trace files were present but no discovery scans were parsed: ${trace.files.join(', ')}`,
  );

  const evidence = findDoublingEvidence(trace.scans, warmupFrom(trace.env));
  assert.ok(
    evidence.length >= 4,
    `expected at least 3 successive geometric discovery-scan doublings; best observed sequence: ${
      evidence.map((scan) => scan.span).join(' -> ') || '<none>'
    }`,
  );
});

test('resume re-seeds discovery from the durable frontier instead of reusing a stale grown quantum', {
  skip: noTraceSkip,
}, () => {
  assert.ok(
    trace.scans.length > 0,
    `dense trace files were present but no discovery scans were parsed: ${trace.files.join(', ')}`,
  );

  const warmupBlocks = warmupFrom(trace.env);
  const attempts = [
    ...groupBy(
      trace.scans,
      (scan) => `${scan.source}:${scan.run}:${scan.attempt}`,
    ).values(),
  ]
    .map((group) => collapseAdjacentDuplicates(group))
    .filter((group) => group.length > 0)
    .sort(
      (a, b) =>
        a[0].source.localeCompare(b[0].source) ||
        a[0].run - b[0].run ||
        a[0].attempt - b[0].attempt,
    );

  const failures = [];
  let checked = 0;
  for (let i = 0; i < attempts.length - 1; i++) {
    const before = attempts[i];
    const after = attempts[i + 1];
    if (before[0].source !== after[0].source || before[0].run !== after[0].run)
      continue;
    if (Math.max(...before.map((scan) => scan.span)) < warmupBlocks * 4)
      continue;

    checked++;
    const first = after[0];
    if (first.span > warmupBlocks) {
      failures.push(
        `run ${first.run} attempt ${first.attempt} first span ${first.span} after grown prior attempt, warmup ${warmupBlocks}`,
      );
    }
  }

  assert.equal(
    failures.length,
    0,
    `post-restart discovery reused a grown quantum: ${failures.join('; ')}`,
  );
  assert.ok(
    checked > 0,
    `expected at least one restart after a grown discovery quantum; observed attempts: ${attempts
      .map(
        (group) =>
          `run ${group[0].run} attempt ${group[0].attempt} spans ${group
            .map((scan) => scan.span)
            .join(' -> ')}`,
      )
      .join(' | ')}`,
  );
});

test('PORTAL_WARMUP_BLOCKS is forwarded into the tier-1 app env (load-bearing driver passthrough)', {
  skip: noTraceSkip,
}, () => {
  const configuredWarmup = Number(process.env.PORTAL_WARMUP_BLOCKS || 2000);

  const forwarded = trace.env.filter(
    (record) =>
      Number.isFinite(record.warmupBlocks) &&
      record.warmupBlocks === configuredWarmup,
  );

  assert.ok(
    forwarded.length > 0,
    `no portal-dense-env record carried warmupBlocks === ${configuredWarmup} (configured PORTAL_WARMUP_BLOCKS); the driver forwarded a different value or omitted the knob, so the geometric warmup shape would be driven by the app default rather than the chaos knob (env records: ${JSON.stringify(
      trace.env,
    )})`,
  );
});

// Mutation proof expected by #92:
// Test 1 (doublings): changing portal/portal-discovery.ts from `discoveryQuantum *= 2` to
// `discoveryQuantum = initialDiscoveryQuantum` flattens the spans and fails the first assertion.
// Test 2 (resume re-seed): a trace whose *later* kill→resume reuses a grown quantum
// (first post-restart span > warmup after a grown prior attempt) fails the failures.length === 0
// assertion — verified against a synthetic regression trace (a prior early-`return` here made the
// assertion vacuous, letting a later-pair regression pass; see PR #96 committee review).
