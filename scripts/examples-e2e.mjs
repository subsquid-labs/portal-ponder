#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { cp, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const EXAMPLES_DIR = join(ROOT, 'examples');
const ALL_EXAMPLES = ['euler-subgraph', 'euler-multichain', 'uniswap-portal'];
const DEFAULT_READY_TIMEOUT_MS = 360_000;
const SHUTDOWN_TIMEOUT_MS = 30_000;
const FETCH_TIMEOUT_MS = 5_000;
const POLL_MS = 1_000;
const MAX_TAIL_LINES = 24;
const CRASH_PATTERN = /unhandledRejection|UnhandledPromiseRejection|uncaught/i;
const GRAPHQL_URL = 'http://localhost:42069/graphql';
const READY_BASE_URL = 'http://localhost:42069';

const activeChildren = new Set();

class HarnessFailure extends Error {
  constructor(message) {
    super(message);
    this.name = 'HarnessFailure';
  }
}

class LineRing {
  constructor(limit = 200) {
    this.limit = limit;
    this.lines = [];
    this.pending = '';
  }

  push(chunk) {
    const text = this.pending + chunk;
    const parts = text.split(/\r?\n/);
    this.pending = parts.pop() ?? '';

    for (const line of parts) {
      this.lines.push(line);
      if (this.lines.length > this.limit) {
        this.lines.shift();
      }
    }
  }

  tail(count, redactor) {
    const lines = this.snapshot().slice(-count);

    return redactor(lines.join('\n'));
  }

  text() {
    return this.snapshot().join('\n');
  }

  snapshot() {
    const lines = [...this.lines];
    if (this.pending.length > 0) {
      lines.push(this.pending);
    }

    return lines;
  }
}

function usage() {
  return [
    'usage: node scripts/examples-e2e.mjs <example-dir-name> [flags]',
    '       node scripts/examples-e2e.mjs --all [flags]',
    '',
    'flags:',
    '  --ponder-version <version>  force-install @subsquid/ponder@<version>',
    '  --check-pins               require example pin to match npm latest',
    '  --verify-docs              README graphql fences must match manifest queries',
  ].join('\n');
}

function parseArgs(argv) {
  const options = {
    all: false,
    checkPins: false,
    examples: [],
    ponderVersion: undefined,
    verifyDocs: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--all') {
      options.all = true;
      continue;
    }

    if (arg === '--check-pins') {
      options.checkPins = true;
      continue;
    }

    if (arg === '--verify-docs') {
      options.verifyDocs = true;
      continue;
    }

    if (arg === '--ponder-version') {
      const version = argv[index + 1];
      if (!version || version.startsWith('-')) {
        throw new HarnessFailure(
          `missing value for --ponder-version\n${usage()}`,
        );
      }

      options.ponderVersion = version;
      index += 1;
      continue;
    }

    if (arg.startsWith('-')) {
      throw new HarnessFailure(`unknown flag: ${arg}\n${usage()}`);
    }

    options.examples.push(arg);
  }

  if (options.all && options.examples.length > 0) {
    throw new HarnessFailure(
      `use either --all or one example name\n${usage()}`,
    );
  }

  if (!options.all && options.examples.length !== 1) {
    throw new HarnessFailure(
      `exactly one example is required unless --all is set\n${usage()}`,
    );
  }

  return options;
}

async function readJson(path) {
  const text = await readFile(path, 'utf8');

  return JSON.parse(text);
}

async function loadExample(example) {
  if (!ALL_EXAMPLES.includes(example)) {
    throw new HarnessFailure(
      `unknown example "${example}" (expected one of ${ALL_EXAMPLES.join(', ')})`,
    );
  }

  const dir = join(EXAMPLES_DIR, example);
  const manifestPath = join(dir, 'e2e.json');
  const packagePath = join(dir, 'package.json');
  const manifest = await readJson(manifestPath);
  const pkg = await readJson(packagePath);

  validateManifest(example, manifest);
  assertCommittedPin(example, manifest, pkg);
  await assertNoTrackedArtifacts(example);

  return { dir, manifest, pkg };
}

function validateManifest(example, manifest) {
  if (!manifest || typeof manifest !== 'object') {
    throw new HarnessFailure(`${example}: e2e.json must be an object`);
  }

  if (typeof manifest.pin !== 'string' || manifest.pin.length === 0) {
    throw new HarnessFailure(`${example}: e2e.json pin must be a string`);
  }

  if (!Array.isArray(manifest.chains) || manifest.chains.length === 0) {
    throw new HarnessFailure(
      `${example}: e2e.json chains must be a non-empty array`,
    );
  }

  for (const chain of manifest.chains) {
    if (!Number.isInteger(chain.chainId)) {
      throw new HarnessFailure(
        `${example}: every chain must have an integer chainId`,
      );
    }
  }

  if (!Array.isArray(manifest.graphql) || manifest.graphql.length === 0) {
    throw new HarnessFailure(
      `${example}: e2e.json graphql must be a non-empty array`,
    );
  }

  for (const assertion of manifest.graphql) {
    if (typeof assertion.query !== 'string' || assertion.query.length === 0) {
      throw new HarnessFailure(
        `${example}: every graphql assertion needs a query`,
      );
    }

    if (typeof assertion.path !== 'string' || assertion.path.length === 0) {
      throw new HarnessFailure(
        `${example}: every graphql assertion needs a path`,
      );
    }

    if (typeof assertion.min !== 'number') {
      throw new HarnessFailure(
        `${example}: every graphql assertion needs a numeric min`,
      );
    }
  }
}

function assertCommittedPin(example, manifest, pkg) {
  const dependency = ponderDependency(pkg);
  if (!dependency) {
    throw new HarnessFailure(
      `${example}: package.json must depend on @subsquid/ponder`,
    );
  }

  if (dependency !== manifest.pin) {
    throw new HarnessFailure(
      `${example}: package.json @subsquid/ponder (${dependency}) does not match e2e.json pin (${manifest.pin})`,
    );
  }
}

function ponderDependency(pkg) {
  if (pkg.dependencies?.['@subsquid/ponder']) {
    return pkg.dependencies['@subsquid/ponder'];
  }

  if (pkg.devDependencies?.['@subsquid/ponder']) {
    return pkg.devDependencies['@subsquid/ponder'];
  }

  return undefined;
}

async function assertNoTrackedArtifacts(example) {
  const result = await runCommand(
    'git',
    ['ls-files', '-z', `examples/${example}`],
    {
      cwd: ROOT,
      env: process.env,
      timeoutMs: 30_000,
    },
  );
  if (result.code !== 0) {
    throw new HarnessFailure(
      `${example}: git ls-files failed: ${result.stderr.tail(MAX_TAIL_LINES, redactUrls)}`,
    );
  }

  const prefix = `examples/${example}/`;
  const tracked = result.stdout
    .text()
    .split('\0')
    .filter((path) => path.length > 0);
  const offending = tracked.filter((path) =>
    isRegenerableArtifact(path.slice(prefix.length)),
  );
  if (offending.length > 0) {
    throw new HarnessFailure(
      `${example}: git tracks regenerable artifact(s) that must never be committed: ${offending.join(', ')}; run \`git rm --cached ${offending.join(' ')}\` (they are gitignored regenerables)`,
    );
  }
}

function isRegenerableArtifact(relative) {
  const segments = relative.split('/');
  const first = segments[0];
  if (first === '.ponder') return true;

  if (first === 'generated') return true;

  if (relative === 'ponder-env.d.ts') return true;

  if (relative === 'package-lock.json') return true;

  return false;
}

function checkLatestPin(example, manifest, pkg, latest) {
  const dependency = ponderDependency(pkg);
  if (dependency !== latest) {
    throw new HarnessFailure(
      `${example}: package.json @subsquid/ponder (${dependency}) does not match npm latest (${latest})`,
    );
  }

  if (manifest.pin !== latest) {
    throw new HarnessFailure(
      `${example}: e2e.json pin (${manifest.pin}) does not match npm latest (${latest})`,
    );
  }
}

async function verifyDocs(example, dir, manifest) {
  const readme = await readFile(join(dir, 'README.md'), 'utf8');
  const queries = new Set(manifest.graphql.map((assertion) => assertion.query));
  const blocks = graphqlBlocks(readme);

  if (blocks.length === 0) {
    throw new HarnessFailure(
      `${example}: README.md has no graphql fenced block`,
    );
  }

  for (const block of blocks) {
    if (!queries.has(block)) {
      throw new HarnessFailure(
        `${example}: README.md graphql block does not byte-match any e2e.json query`,
      );
    }
  }
}

function graphqlBlocks(markdown) {
  const blocks = [];
  const pattern = /```graphql\n([\s\S]*?)\n```/g;

  for (;;) {
    const match = pattern.exec(markdown);
    if (!match) break;

    blocks.push(match[1]);
  }

  return blocks;
}

async function npmLatest() {
  const result = await runCommand(
    'npm',
    ['view', '@subsquid/ponder', 'dist-tags.latest'],
    {
      cwd: ROOT,
      env: process.env,
      timeoutMs: 60_000,
    },
  );

  if (result.code !== 0) {
    throw new HarnessFailure(
      `npm view @subsquid/ponder dist-tags.latest failed: ${result.stderr.tail(MAX_TAIL_LINES, redactUrls)}`,
    );
  }

  const latest = result.stdout.text().trim();
  if (!latest) {
    throw new HarnessFailure(
      'npm latest dist-tag for @subsquid/ponder was empty',
    );
  }

  return latest;
}

async function runExample(example, options, latestPin) {
  const result = {
    example,
    exit: undefined,
    failures: [],
    graphql: [],
    metrics: [],
    status: 'FAIL',
  };
  let tempRoot;
  let dev;
  let redactor = redactUrls;

  try {
    const loaded = await loadExample(example);
    if (options.checkPins) {
      await checkLatestPin(example, loaded.manifest, loaded.pkg, latestPin);
    }

    if (options.verifyDocs) {
      await verifyDocs(example, loaded.dir, loaded.manifest);
    }

    tempRoot = await mkdtemp(join(tmpdir(), `portal-ponder-e2e-${example}-`));
    const workDir = join(tempRoot, example);
    await copyExample(loaded.dir, workDir);

    const installVersion = options.ponderVersion ?? loaded.manifest.pin;
    await forcePonderVersion(workDir, installVersion);
    const install = await runCommand(
      'npm',
      ['install', '--no-audit', '--no-fund'],
      {
        cwd: workDir,
        env: process.env,
        timeoutMs: 600_000,
      },
    );
    if (install.code !== 0) {
      throw new HarnessFailure(
        `${example}: npm install failed: ${install.stderr.tail(MAX_TAIL_LINES, redactUrls)}`,
      );
    }

    const metricsBase = join(tempRoot, 'portal-metrics');
    const childEnv = buildChildEnv(loaded.manifest, metricsBase);
    redactor = createRedactor(childEnv);
    dev = startDev(workDir, childEnv);

    await pollReady(
      dev,
      loaded.manifest.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS,
      redactor,
    );
    result.metrics = await readPortalMetrics(metricsBase, loaded.manifest);
    result.graphql = await runGraphqlAssertions(loaded.manifest);
  } catch (error) {
    result.failures.push(messageFromError(error));
  } finally {
    if (dev) {
      result.exit = await stopDev(dev);
      const crash = firstCrashLine(dev.stderr.text(), redactor);
      if (crash) {
        result.failures.push(`stderr contains crash marker: ${crash}`);
      }

      if (!cleanSigintExit(result.exit)) {
        result.failures.push(formatExitFailure(result.exit));
      }
    }

    if (tempRoot) {
      await rm(tempRoot, { force: true, recursive: true });
    }
  }

  const metricFailures = metricFailuresFor(result.metrics);
  for (const failure of metricFailures) {
    result.failures.push(failure);
  }

  const graphqlFailures = graphqlFailuresFor(result.graphql);
  for (const failure of graphqlFailures) {
    result.failures.push(failure);
  }

  if (result.failures.length === 0) {
    result.status = 'PASS';
  }

  return result;
}

async function copyExample(source, destination) {
  await cp(source, destination, {
    filter: (path) => {
      const name = basename(path);
      if (name === 'node_modules') return false;
      if (name === '.ponder') return false;
      if (name === 'generated') return false;
      if (name === 'package-lock.json') return false;
      if (name === 'ponder-env.d.ts') return false;
      if (name === 'dist') return false;

      return true;
    },
    recursive: true,
  });
}

async function forcePonderVersion(workDir, version) {
  const packagePath = join(workDir, 'package.json');
  const pkg = await readJson(packagePath);
  if (pkg.dependencies?.['@subsquid/ponder']) {
    pkg.dependencies['@subsquid/ponder'] = version;
  } else {
    pkg.dependencies ??= {};
    pkg.dependencies['@subsquid/ponder'] = version;
  }

  await writeFile(packagePath, `${JSON.stringify(pkg, null, 2)}\n`);
}

function buildChildEnv(manifest, metricsBase) {
  const env = { ...process.env };
  env.CI = '1';
  env.NO_COLOR = '1';
  env.PORTAL_METRICS_FILE = metricsBase;

  for (const chain of manifest.chains) {
    const ponderKey = `PONDER_RPC_URL_${chain.chainId}`;
    const e2eKey = `E2E_RPC_URL_${chain.chainId}`;
    delete env[ponderKey];

    if (process.env[e2eKey]) {
      env[ponderKey] = process.env[e2eKey];
    }
  }

  return env;
}

function startDev(cwd, env) {
  const child = spawn('npm', ['run', 'dev'], {
    cwd,
    detached: true,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const stdout = new LineRing();
  const stderr = new LineRing();
  const state = {
    child,
    exit: undefined,
    exitPromise: undefined,
    stderr,
    stdout,
  };
  state.exitPromise = new Promise((resolveExit) => {
    child.once('exit', (code, signal) => {
      state.exit = { code, signal, timedOut: false };
      activeChildren.delete(child);
      resolveExit(state.exit);
    });
  });

  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk) => stdout.push(chunk));
  child.stderr.on('data', (chunk) => stderr.push(chunk));
  activeChildren.add(child);

  return state;
}

async function pollReady(dev, timeoutMs, redactor) {
  const deadline = Date.now() + timeoutMs;
  let endpoint = '/ready';
  let lastStatus = 'not probed';

  while (Date.now() < deadline) {
    if (dev.exit) {
      throw new HarnessFailure(
        `npm run dev exited before ready: ${formatExit(dev.exit)}; last status: ${lastStatus}; stdout tail:\n${dev.stdout.tail(MAX_TAIL_LINES, redactor)}\nstderr tail:\n${dev.stderr.tail(MAX_TAIL_LINES, redactor)}`,
      );
    }

    const status = await httpStatus(`${READY_BASE_URL}${endpoint}`);
    lastStatus = `${endpoint} ${status ?? 'fetch-failed'}`;
    if (status === 200) {
      return endpoint;
    }

    if (endpoint === '/ready' && status === 404) {
      endpoint = '/status';
    }

    await sleep(POLL_MS);
  }

  throw new HarnessFailure(
    `ready timeout after ${timeoutMs}ms; last status: ${lastStatus}; stdout tail:\n${dev.stdout.tail(MAX_TAIL_LINES, redactor)}; stderr tail:\n${dev.stderr.tail(MAX_TAIL_LINES, redactor)}`,
  );
}

async function httpStatus(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(url, { signal: controller.signal });

    return response.status;
  } catch {
    return undefined;
  } finally {
    clearTimeout(timer);
  }
}

async function readPortalMetrics(metricsBase, manifest) {
  const metrics = [];

  for (const chain of manifest.chains) {
    const path = `${metricsBase}.${chain.chainId}`;
    await waitForFile(path, 5_000);
    const parsed = await readJson(path);
    const dataChunks = numberAt(parsed, ['fetch', 'dataChunks']);
    const insertedBlocks = numberAt(parsed, ['inserted', 'blocks']);
    const bytes = numberAt(parsed, ['fetch', 'bytes']);
    const rpcFallbackIntervals = numberAt(parsed, ['rpcFallbackIntervals']);
    const wallMs = numberAt(parsed, ['wallMs']);
    const blocksPerSec = wallMs > 0 ? insertedBlocks / (wallMs / 1000) : 0;

    metrics.push({
      blocksPerSec,
      bytes,
      chainId: chain.chainId,
      checks: {
        dataChunks: dataChunks >= 1,
        fetchBytes: bytes > 0,
        insertedBlocks: insertedBlocks > 0,
        noRpcFallback: rpcFallbackIntervals === 0,
      },
      dataChunks,
      insertedBlocks,
      rpcFallbackIntervals,
      wallMs,
    });
  }

  return metrics;
}

async function waitForFile(path, timeoutMs) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      const info = await stat(path);
      if (info.isFile()) return;
    } catch {
      // Keep polling until the metrics writer has flushed the file.
    }

    await sleep(250);
  }

  throw new HarnessFailure(`metrics file not found: ${path}`);
}

function numberAt(value, path) {
  let current = value;
  for (const segment of path) {
    current = current?.[segment];
  }

  return typeof current === 'number' ? current : Number.NaN;
}

async function runGraphqlAssertions(manifest) {
  const responses = new Map();
  const assertions = [];

  for (const item of manifest.graphql) {
    let data = responses.get(item.query);
    if (!data) {
      data = await graphql(item.query);
      responses.set(item.query, data);
    }

    const raw = valueAtPath(data, item.path);
    const actual = Number(raw);
    const ok = Number.isFinite(actual) && actual >= item.min;
    assertions.push({
      actual,
      min: item.min,
      ok,
      path: item.path,
    });
  }

  return assertions;
}

async function graphql(query) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(GRAPHQL_URL, {
      body: JSON.stringify({ query }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      throw new HarnessFailure(
        `GraphQL HTTP ${response.status}: ${text.slice(0, 500)}`,
      );
    }

    const body = JSON.parse(text);
    if (Array.isArray(body.errors) && body.errors.length > 0) {
      throw new HarnessFailure(
        `GraphQL errors: ${JSON.stringify(body.errors)}`,
      );
    }

    return body.data;
  } finally {
    clearTimeout(timer);
  }
}

function valueAtPath(value, path) {
  let current = value;
  const segments = path.split('.');

  for (const segment of segments) {
    current = current?.[segment];
  }

  return current;
}

async function stopDev(dev) {
  if (!dev.child.pid) {
    return { code: null, signal: 'missing-pid', timedOut: false };
  }

  if (dev.exit) {
    return dev.exit;
  }

  await signalDevProcess(dev.child.pid);

  const exit = await Promise.race([
    dev.exitPromise,
    sleep(SHUTDOWN_TIMEOUT_MS).then(() => undefined),
  ]);
  if (exit) {
    return exit;
  }

  try {
    process.kill(-dev.child.pid, 'SIGKILL');
  } catch {
    // Best-effort cleanup; the process group may already be gone.
  }

  await dev.exitPromise;

  return { ...dev.exit, timedOut: true };
}

async function signalDevProcess(rootPid) {
  const targets = await nonShellDescendantPids(rootPid);
  if (targets.length === 0) {
    try {
      process.kill(rootPid, 'SIGINT');
    } catch {
      // The process may have exited between the last poll and shutdown.
    }

    return;
  }

  for (const pid of targets) {
    try {
      process.kill(pid, 'SIGINT');
    } catch {
      // Best-effort: a descendant may exit while signals are being sent.
    }
  }
}

async function nonShellDescendantPids(rootPid) {
  const result = await runCommand('ps', ['-eo', 'pid=,ppid=,comm='], {
    cwd: ROOT,
    env: process.env,
    timeoutMs: 5_000,
  });
  if (result.code !== 0) return [];

  const children = new Map();
  const commands = new Map();
  const lines = result.stdout.text().split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const parts = trimmed.split(/\s+/, 3);
    const pid = Number(parts[0]);
    const ppid = Number(parts[1]);
    const command = parts[2] ?? '';
    if (!Number.isInteger(pid) || !Number.isInteger(ppid)) continue;

    commands.set(pid, command);
    const siblings = children.get(ppid) ?? [];
    siblings.push(pid);
    children.set(ppid, siblings);
  }

  const descendants = [];
  const stack = [...(children.get(rootPid) ?? [])];
  while (stack.length > 0) {
    const pid = stack.pop();
    if (!pid) continue;

    descendants.push(pid);
    for (const child of children.get(pid) ?? []) {
      stack.push(child);
    }
  }

  const shellNames = new Set(['bash', 'dash', 'sh', 'zsh']);

  return descendants.filter((pid) => !shellNames.has(commands.get(pid) ?? ''));
}

function cleanSigintExit(exit) {
  return exit?.code === 0 && !exit.signal && exit.timedOut === false;
}

function formatExitFailure(exit) {
  if (!exit) {
    return 'SIGINT shutdown did not produce an exit event';
  }

  if (exit.timedOut) {
    return `SIGINT shutdown timed out after ${SHUTDOWN_TIMEOUT_MS}ms`;
  }

  return `SIGINT shutdown was not clean: ${formatExit(exit)}`;
}

function firstCrashLine(stderr, redactor) {
  const lines = stderr.split(/\r?\n/);

  for (const line of lines) {
    if (CRASH_PATTERN.test(line)) {
      return redactor(line);
    }
  }

  return undefined;
}

function metricFailuresFor(metrics) {
  const failures = [];

  for (const metric of metrics) {
    if (!metric.checks.dataChunks) {
      failures.push(
        `chain ${metric.chainId}: fetch.dataChunks=${metric.dataChunks} < 1`,
      );
    }

    if (!metric.checks.insertedBlocks) {
      failures.push(
        `chain ${metric.chainId}: inserted.blocks=${metric.insertedBlocks} <= 0`,
      );
    }

    if (!metric.checks.fetchBytes) {
      failures.push(
        `chain ${metric.chainId}: fetch.bytes=${metric.bytes} <= 0`,
      );
    }

    if (!metric.checks.noRpcFallback) {
      failures.push(
        `chain ${metric.chainId}: rpcFallbackIntervals=${metric.rpcFallbackIntervals} != 0`,
      );
    }
  }

  return failures;
}

function graphqlFailuresFor(assertions) {
  const failures = [];

  for (const assertion of assertions) {
    if (!assertion.ok) {
      failures.push(
        `GraphQL ${assertion.path}=${formatMaybeNumber(assertion.actual)} < ${assertion.min}`,
      );
    }
  }

  return failures;
}

function formatExit(exit) {
  const code = exit?.code ?? 'null';
  const signal = exit?.signal ?? 'null';

  return `code=${code} signal=${signal}`;
}

function formatMaybeNumber(value) {
  return Number.isFinite(value) ? String(value) : 'NaN';
}

function formatMb(bytes) {
  if (!Number.isFinite(bytes)) return 'NaN';

  return (bytes / (1024 * 1024)).toFixed(2);
}

function formatRate(value) {
  if (!Number.isFinite(value)) return 'NaN';

  return value.toFixed(1);
}

function formatCheck(value) {
  return value ? 'true' : 'false';
}

function formatGraphql(assertions) {
  if (assertions.length === 0) return '-';

  return assertions
    .map((assertion) => {
      const actual = formatMaybeNumber(assertion.actual);
      const ok = assertion.ok ? 'true' : 'false';

      return `${assertion.path}=${actual}>=${assertion.min}:${ok}`;
    })
    .join('; ');
}

function formatResultRows(results) {
  const rows = [];

  for (const result of results) {
    const metrics = result.metrics.length > 0 ? result.metrics : [undefined];
    for (const metric of metrics) {
      rows.push({
        blocks: metric ? String(metric.insertedBlocks) : '-',
        blocksPerSec: metric ? formatRate(metric.blocksPerSec) : '-',
        bytes: metric ? formatMb(metric.bytes) : '-',
        chain: metric ? String(metric.chainId) : '-',
        dataChunks: metric ? formatCheck(metric.checks.dataChunks) : '-',
        example: result.example,
        exit: result.exit ? formatExit(result.exit) : '-',
        fetchBytes: metric ? formatCheck(metric.checks.fetchBytes) : '-',
        graphql: formatGraphql(result.graphql),
        insertedBlocks: metric
          ? formatCheck(metric.checks.insertedBlocks)
          : '-',
        reason: result.failures.join('; '),
        rpcFallback: metric ? formatCheck(metric.checks.noRpcFallback) : '-',
        status: result.status,
      });
    }
  }

  return rows;
}

function printTable(results) {
  const rows = formatResultRows(results);
  const headers = [
    'example',
    'status',
    'chain',
    'blocks',
    'MB',
    'blocks/s',
    'dataChunks>=1',
    'inserted.blocks>0',
    'fetch.bytes>0',
    'rpcFallback=0',
    'graphql',
    'exit',
    'reason',
  ];
  const data = rows.map((row) => [
    row.example,
    row.status,
    row.chain,
    row.blocks,
    row.bytes,
    row.blocksPerSec,
    row.dataChunks,
    row.insertedBlocks,
    row.fetchBytes,
    row.rpcFallback,
    row.graphql,
    row.exit,
    row.reason || '-',
  ]);

  console.log(markdownTable(headers, data));
}

function markdownTable(headers, rows) {
  const escapedHeaders = headers.map(escapeCell);
  const escapedRows = rows.map((row) => row.map(escapeCell));
  const widths = escapedHeaders.map((header, index) => {
    let width = header.length;
    for (const row of escapedRows) {
      width = Math.max(width, row[index].length);
    }

    return width;
  });
  const headerLine = formatTableLine(escapedHeaders, widths);
  const dividerLine = formatTableLine(
    widths.map((width) => '-'.repeat(width)),
    widths,
  );
  const rowLines = escapedRows.map((row) => formatTableLine(row, widths));

  return [headerLine, dividerLine, ...rowLines].join('\n');
}

function formatTableLine(cells, widths) {
  const padded = cells.map((cell, index) => cell.padEnd(widths[index], ' '));

  return `| ${padded.join(' | ')} |`;
}

function escapeCell(value) {
  return String(value).replaceAll('|', '\\|').replaceAll('\n', ' ');
}

async function runCommand(command, args, options) {
  const stdout = new LineRing();
  const stderr = new LineRing();
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: options.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk) => stdout.push(chunk));
  child.stderr.on('data', (chunk) => stderr.push(chunk));

  let timer;
  const exitPromise = new Promise((resolveExit) => {
    child.once('exit', (code, signal) => {
      if (timer) {
        clearTimeout(timer);
      }

      resolveExit({ code, signal, timedOut: false });
    });
  });

  const timeoutPromise = new Promise((resolveExit) => {
    timer = setTimeout(() => {
      child.kill('SIGKILL');
      resolveExit({ code: null, signal: 'SIGKILL', timedOut: true });
    }, options.timeoutMs);
  });
  const exit = await Promise.race([exitPromise, timeoutPromise]);

  return { ...exit, stderr, stdout };
}

function messageFromError(error) {
  if (error instanceof Error) {
    return redactUrls(error.message);
  }

  return redactUrls(String(error));
}

function createRedactor(env) {
  const secrets = [];

  for (const [key, value] of Object.entries(env)) {
    if (!value) continue;

    if (key.startsWith('E2E_RPC_URL_') || key.startsWith('PONDER_RPC_URL_')) {
      secrets.push(value);
    }
  }

  return (text) => {
    let redacted = redactUrls(text);
    for (const secret of secrets) {
      redacted = redacted.split(secret).join('<redacted-rpc-url>');
    }

    return redacted;
  };
}

function redactUrls(text) {
  return String(text).replace(/https?:\/\/[^\s'"`)]+/g, '<redacted-url>');
}

function cleanupActiveSync(signal) {
  for (const child of activeChildren) {
    if (!child.pid) continue;

    try {
      process.kill(-child.pid, signal);
    } catch {
      // Best-effort process-group cleanup during harness shutdown.
    }
  }
}

function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const examples = options.all ? ALL_EXAMPLES : options.examples;
  const latestPin = options.checkPins ? await npmLatest() : undefined;
  const results = [];

  for (const example of examples) {
    const result = await runExample(example, options, latestPin);
    results.push(result);
  }

  printTable(results);

  const passCount = results.filter((result) => result.status === 'PASS').length;
  const failCount = results.length - passCount;
  console.log(`Summary: ${passCount} PASS, ${failCount} FAIL`);

  if (failCount > 0) {
    process.exitCode = 1;
  }
}

process.once('SIGINT', () => {
  cleanupActiveSync('SIGINT');
  process.exit(130);
});
process.once('SIGTERM', () => {
  cleanupActiveSync('SIGTERM');
  process.exit(143);
});
process.once('exit', () => {
  cleanupActiveSync('SIGTERM');
});

try {
  await main();
} catch (error) {
  const result = {
    example: '-',
    exit: undefined,
    failures: [messageFromError(error)],
    graphql: [],
    metrics: [],
    status: 'FAIL',
  };
  printTable([result]);
  console.log('Summary: 0 PASS, 1 FAIL');
  process.exitCode = 1;
}
