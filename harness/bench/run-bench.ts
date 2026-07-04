/**
 * Single-indexer backfill bench. Spawns `ponder start` for one project over a bounded
 * range, and measures what matters for our priorities — STABILITY (errors/retries/OOM,
 * clean completion) and BACKFILL SPEED (wall-clock, events/sec) — plus the Portal-side
 * efficiency it pulls from PORTAL_METRICS_FILE (http, bytes, chunks, fallback, inserts).
 */
import { spawn } from 'node:child_process';
import { existsSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';

export type BenchSpec = {
  name: string; // display name
  dir: string; // ponder project dir (has ponder.config.ts + node_modules/.bin/ponder)
  schema: string; // --schema
  port: number;
  start: number;
  end: number;
  chainIds: number[]; // for reading PORTAL_METRICS_FILE.<chainId>
  env?: Record<string, string>; // PORTAL_URL / PONDER_RPC_URL_* / PORTAL_* overrides
  maxOldSpaceMB?: number; // default 4096
  timeoutMin?: number; // default 30
};

export type BenchResult = {
  name: string;
  ok: boolean;
  wallSec: number;
  events: number;
  eventsPerSec: number;
  peakRssMB: number;
  error?: string;
  portal: {
    http: number;
    mb: number;
    dataChunks: number;
    errors: number;
    retries: number;
    cacheHits: number;
    maxInflight: number;
    rpcFallback: number;
    chunkBlocks: number;
    inserted: Record<string, number>;
  } | null;
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
// biome-ignore lint/suspicious/noControlCharactersInRegex: intentional ANSI escape strip
const strip = (s: string) => s.replace(/\u001B\[[0-9;]*m/g, '');

/** sum RSS (MB) of a pid and all its descendants (ponder forks indexing workers). */
async function rssTreeMB(pid: number): Promise<number> {
  return new Promise((resolve) => {
    const ps = spawn('bash', ['-c', 'ps -Ao pid=,ppid=,rss=']);
    let out = '';
    ps.stdout.on('data', (d) => (out += d));
    ps.on('close', () => {
      const rss = new Map<number, number>(),
        kids = new Map<number, number[]>();
      for (const l of out.trim().split('\n')) {
        const [p, pp, r] = l.trim().split(/\s+/).map(Number);
        if (!p) continue;
        rss.set(p, r);
        if (!kids.has(pp)) kids.set(pp, []);
        kids.get(pp)!.push(p);
      }
      let total = 0;
      const stack = [pid];
      while (stack.length) {
        const p = stack.pop()!;
        total += rss.get(p) ?? 0;
        for (const c of kids.get(p) ?? []) stack.push(c);
      }
      resolve(total / 1024);
    });
    ps.on('error', () => resolve(0));
  });
}

export async function runBench(spec: BenchSpec): Promise<BenchResult> {
  const metricsBase = `/tmp/portal-metrics-${spec.schema}`;
  for (const cid of spec.chainIds)
    try {
      rmSync(`${metricsBase}.${cid}`);
    } catch {}
  try {
    rmSync(join(spec.dir, '.ponder'), { recursive: true, force: true });
  } catch {}

  const env: Record<string, string> = {
    ...(process.env as Record<string, string>),
    PONDER_START: String(spec.start),
    PONDER_END: String(spec.end),
    PONDER_LOG_LEVEL: 'info',
    CI: 'true',
    PORTAL_METRICS_FILE: metricsBase,
    NODE_OPTIONS: `--max-old-space-size=${spec.maxOldSpaceMB ?? 4096}`,
    // size the chunk to the bench range so we measure the PURE backfill of that range,
    // not chunk over-fetch (the big chunk amortizes only over a full multi-interval backfill).
    // dense sources (traces/blocks) still auto-cap below this.
    PORTAL_CHUNK_FIXED: '1',
    PORTAL_CHUNK_BLOCKS: String(Math.max(1000, spec.end - spec.start)),
    // read-ahead prefetches chunks BEYOND the bounded endBlock (pure waste in a bounded bench;
    // amortizes only in an open-ended backfill) — keep it shallow here.
    PORTAL_READAHEAD: '1',
    ...(spec.env ?? {}),
  };
  const bin = join(spec.dir, 'node_modules/.bin/ponder');
  const proc = spawn(
    bin,
    ['start', '--schema', spec.schema, '--port', String(spec.port)],
    { cwd: spec.dir, env },
  );

  const t0 = Date.now();
  let events = 0,
    peakRssMB = 0,
    done = false,
    failed = '';
  let buf = '';
  const onLine = (line: string) => {
    const m = line.match(/event_count=(\d+)/);
    if (m) events += Number(m[1]);
    if (/Completed indexing across all chains/.test(line)) done = true;
    if (
      /heap out of memory|FATAL|Error while processing|BuildError|Cannot find/i.test(
        line,
      )
    )
      failed ||= strip(line).slice(0, 180);
  };
  const onData = (d: Buffer) => {
    buf += d.toString();
    let nl = buf.indexOf('\n');
    while (nl >= 0) {
      onLine(buf.slice(0, nl));
      buf = buf.slice(nl + 1);
      nl = buf.indexOf('\n');
    }
  };
  proc.stdout.on('data', onData);
  proc.stderr.on('data', onData);
  proc.on('error', (e) => (failed ||= String(e)));

  const timeoutMs = (spec.timeoutMin ?? 10) * 60_000;
  while (
    !done &&
    !failed &&
    Date.now() - t0 < timeoutMs &&
    proc.exitCode === null
  ) {
    if (proc.pid)
      peakRssMB = Math.max(peakRssMB, await rssTreeMB(proc.pid).catch(() => 0));
    await sleep(1000);
  }
  const wallSec = (Date.now() - t0) / 1000;
  if (!done && !failed)
    failed =
      proc.exitCode !== null
        ? `process exited (code ${proc.exitCode})`
        : 'timeout';
  try {
    proc.kill('SIGTERM');
  } catch {}
  await sleep(400);
  try {
    proc.kill('SIGKILL');
  } catch {}

  let portal: BenchResult['portal'] = null;
  for (const cid of spec.chainIds) {
    const file = `${metricsBase}.${cid}`;
    if (!existsSync(file)) continue;
    let m: any;
    try {
      m = JSON.parse(readFileSync(file, 'utf8'));
    } catch {
      continue;
    }
    portal ??= {
      http: 0,
      mb: 0,
      dataChunks: 0,
      errors: 0,
      retries: 0,
      cacheHits: 0,
      maxInflight: 0,
      rpcFallback: 0,
      chunkBlocks: 0,
      inserted: {},
    };
    portal.http += m.fetch.http;
    portal.mb += m.fetch.bytes / 1e6;
    portal.dataChunks += m.fetch.dataChunks;
    portal.errors += m.fetch.errors;
    portal.retries += m.fetch.retries;
    portal.cacheHits += m.fetch.cacheHits;
    portal.maxInflight = Math.max(portal.maxInflight, m.fetch.maxInflight);
    portal.rpcFallback += m.rpcFallbackIntervals;
    portal.chunkBlocks = m.chunkBlocks;
    for (const k of Object.keys(m.inserted))
      portal.inserted[k] = (portal.inserted[k] ?? 0) + m.inserted[k];
  }

  const ok = done && !failed;
  return {
    name: spec.name,
    ok,
    wallSec: +wallSec.toFixed(1),
    events,
    eventsPerSec: wallSec > 0 ? Math.round(events / wallSec) : 0,
    peakRssMB: Math.round(peakRssMB),
    error: failed || undefined,
    portal,
  };
}
