#!/usr/bin/env node
// emit-manifest.mjs — write bench.manifest.json: the full REPRODUCIBILITY record for a flagship bench
// run. It captures WHAT ran (tarball + its sha256, repo main SHA, chains.json + anchors-file sha256),
// the RUN KNOBS (Portal/concurrency env by NAME — secret values are recorded as "<from-env>", never
// the value), the HOST envelope (cgroup memory/cpu limits as the environment reports them), a read-only
// snapshot of the relevant Postgres settings (synchronous_commit, shared_buffers, WAL knobs), a
// free-text load-conditions field, and the one-line repro command.
//
//   node emit-manifest.mjs --out bench.manifest.json [--repro "<command>"] [--load "<free text>"]
//
// NEVER writes a secret. Key env vars (PORTAL_API_KEY, PORTAL_URL, DATABASE_URL creds, SQD_RPC_KEY, …)
// are recorded by NAME with the sentinel "<from-env>"; DATABASE_URL is decomposed to host/db WITHOUT
// credentials. PORTAL_URL is treated like a secret because it can embed a private/tenant-specific host.

import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from './anchor-shim.mjs';

const DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(DIR, '..', '..');

// Env vars recorded BY NAME with the "<from-env>" sentinel, value NEVER read into the manifest. This
// covers credentials AND the Portal endpoint URL: PORTAL_URL can embed a private/tenant-specific Portal
// host, so a raw manifest must never leak a user's endpoint — it is recorded by name like a secret.
const SECRET_ENV = new Set([
  'PORTAL_API_KEY',
  'PORTAL_URL',
  'SQD_RPC_KEY',
  'REALTIME_RPC_KEY',
  'DATABASE_URL', // handled specially (host/db decomposed, creds stripped)
]);

// Non-secret run knobs recorded VERBATIM (value is meaningful and carries no credential).
const KNOB_ENV = [
  'PORTAL_CHECKS',
  'PORTAL_CHUNK_BLOCKS',
  'PORTAL_CHUNK_FIXED',
  'PORTAL_CHUNK_PINNED',
  'PORTAL_CONCURRENCY',
  'PORTAL_GATE_CONCURRENCY',
  'PORTAL_MAX_CONCURRENCY',
  'PONDER_LOG_LEVEL',
  'BENCH_RPC_BASE',
  'EULER_CHAINS',
  'EULER_REALTIME',
  'INCLUDE_RECEIPTS',
  'DATABASE_SCHEMA',
  'NODE_OPTIONS',
];

function sha256File(p) {
  if (!existsSync(p)) {
    return null;
  }

  return createHash('sha256').update(readFileSync(p)).digest('hex');
}

function sh(cmd, cmdArgs) {
  try {
    return execFileSync(cmd, cmdArgs, {
      cwd: REPO_ROOT,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch {
    return null;
  }
}

// Decompose DATABASE_URL to host/db WITHOUT credentials — never the user:password.
function dbTarget(url) {
  if (!url) {
    return null;
  }

  try {
    const u = new URL(url);

    return {
      host: u.hostname,
      port: u.port || null,
      database: u.pathname.replace(/^\//, '') || null,
      // explicitly note that credentials are present but withheld
      credentials: u.username ? '<from-env, withheld>' : null,
    };
  } catch {
    return { host: '<unparseable>', database: null };
  }
}

// cgroup limits as the environment reports them (v2 then v1 fallback). Best-effort — a missing file just
// yields null for that field (e.g. an unconstrained host).
function cgroupLimits() {
  const read = (p) => {
    try {
      return readFileSync(p, 'utf8').trim();
    } catch {
      return null;
    }
  };

  return {
    memoryMaxV2: read('/sys/fs/cgroup/memory.max'),
    memoryHighV2: read('/sys/fs/cgroup/memory.high'),
    cpuMaxV2: read('/sys/fs/cgroup/cpu.max'),
    memoryLimitV1: read('/sys/fs/cgroup/memory/memory.limit_in_bytes'),
  };
}

// Read-only snapshot of the relevant Postgres settings. Uses a READ-ONLY transaction and touches only
// pg_settings (no writes). Returns null if DATABASE_URL is unset or the query fails (best-effort).
async function pgSettings(url) {
  if (!url) {
    return null;
  }

  const wanted = [
    'synchronous_commit',
    'shared_buffers',
    'wal_level',
    'max_wal_size',
    'min_wal_size',
    'wal_compression',
    'full_page_writes',
    'checkpoint_timeout',
    'effective_cache_size',
    'work_mem',
    'maintenance_work_mem',
    'max_connections',
    'server_version',
  ];

  const pg = await import('pg').catch(() => null);
  if (!pg) {
    return { error: 'pg module not available' };
  }

  const Client = pg.default?.Client ?? pg.Client;
  const client = new Client({ connectionString: url });
  try {
    await client.connect();
    // read-only transaction — assert we cannot write.
    await client.query('begin transaction read only');
    const res = await client.query(
      'select name, setting, unit from pg_settings where name = any($1)',
      [wanted],
    );
    await client.query('commit');
    const out = {};
    for (const row of res.rows) {
      out[row.name] = row.unit ? `${row.setting} ${row.unit}` : row.setting;
    }

    return out;
  } catch (e) {
    return { error: String(e?.message ?? e).slice(0, 120) };
  } finally {
    await client.end().catch(() => {});
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outPath =
    typeof args.out === 'string' ? args.out : 'bench.manifest.json';
  const repro =
    typeof args.repro === 'string'
      ? args.repro
      : 'BENCH_RPC_BASE=http://127.0.0.1:8645 SQD_PONDER_TARBALL=<tgz> DATABASE_URL=<fresh> PORTAL_URL=<portal> PORTAL_API_KEY=<from-env> bash harness/bench/run-flagship.sh';
  const load = typeof args.load === 'string' ? args.load : '<not recorded>';

  const tarball = process.env.SQD_PONDER_TARBALL || null;
  const chainsPath = path.join(
    REPO_ROOT,
    'harness',
    'euler-multichain',
    'chains.json',
  );
  const anchorsPath = typeof args.anchors === 'string' ? args.anchors : null;

  // env knobs — record present ones verbatim; secret ones by name with the sentinel.
  const knobs = {};
  for (const k of KNOB_ENV) {
    if (process.env[k] !== undefined) {
      knobs[k] = process.env[k];
    }
  }

  const secrets = {};
  for (const k of SECRET_ENV) {
    if (k === 'DATABASE_URL') {
      continue;
    }
    if (process.env[k] !== undefined) {
      secrets[k] = '<from-env>';
    }
  }

  const manifest = {
    generatedAt: new Date().toISOString(),
    kind: 'portal-ponder deterministic 15-chain flagship bench manifest',
    tarball: {
      path: tarball ? path.basename(tarball) : null,
      sha256: tarball ? sha256File(tarball) : null,
      sizeBytes: tarball && existsSync(tarball) ? statSync(tarball).size : null,
    },
    repo: {
      mainSha: sh('git', ['rev-parse', 'HEAD']),
      branch: sh('git', ['rev-parse', '--abbrev-ref', 'HEAD']),
      dirty: sh('git', ['status', '--porcelain']) ? true : false,
    },
    chainsJson: {
      path: 'harness/euler-multichain/chains.json',
      sha256: sha256File(chainsPath),
    },
    anchorsFile: anchorsPath
      ? { path: path.basename(anchorsPath), sha256: sha256File(anchorsPath) }
      : null,
    env: {
      knobs,
      secretsByName: secrets,
      database: dbTarget(process.env.DATABASE_URL),
      eulerChainsState: process.env.EULER_CHAINS
        ? `set: ${process.env.EULER_CHAINS}`
        : 'unset (all 15 chains)',
    },
    host: {
      nodeVersion: process.version,
      platform: process.platform,
      arch: process.arch,
      cpuCount: (await import('node:os')).cpus?.().length ?? null,
      cgroup: cgroupLimits(),
    },
    postgres: await pgSettings(process.env.DATABASE_URL),
    loadConditions: load,
    repro,
  };

  writeFileSync(outPath, `${JSON.stringify(manifest, null, 2)}\n`);
  console.error(
    `emit-manifest: wrote ${outPath} (tarball=${manifest.tarball.path ?? 'none'} repo=${manifest.repo.mainSha?.slice(0, 8)} chainsJson=${manifest.chainsJson.sha256?.slice(0, 12)})`,
  );
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(`emit-manifest: ${e?.message ?? e}`);
    process.exit(1);
  });
}
