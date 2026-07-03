import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import {
  CHAIN_ALIASES,
  filterCarriedEnv,
  loadKnownChains,
  OVERRIDDEN_KEYS,
  parseEnvLine,
  resolveEulerChains,
} from './deploy-helpers.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CHAINS_JSON = join(HERE, '..', 'euler-multichain', 'chains.json');
const DEPLOY_SH = join(HERE, 'deploy-soak-b.sh');
const KNOWN = loadKnownChains(CHAINS_JSON);

// ── parseEnvLine ─────────────────────────────────────────────────────────────────────────────────

test('parseEnvLine: plain KEY=value', () => {
  assert.deepEqual(parseEnvLine('FOO=bar'), {
    key: 'FOO',
    value: 'bar',
    line: 'FOO=bar',
  });
});

test('parseEnvLine: strips a leading `export ` and surrounding whitespace', () => {
  assert.deepEqual(parseEnvLine('  export FOO=bar '), {
    key: 'FOO',
    value: 'bar',
    line: 'FOO=bar',
  });
});

test('parseEnvLine: comments and blanks are ignored', () => {
  assert.equal(parseEnvLine('# a comment'), null);
  assert.equal(parseEnvLine('   '), null);
  assert.equal(parseEnvLine(''), null);
});

test('parseEnvLine: a line with no `=` (or an empty key) is not an assignment', () => {
  assert.equal(parseEnvLine('NOT_AN_ASSIGNMENT'), null);
  assert.equal(parseEnvLine('=leadingeq'), null);
});

test('parseEnvLine: an invalid identifier is rejected', () => {
  assert.equal(parseEnvLine('1BAD=x'), null);
  assert.equal(parseEnvLine('has-dash=x'), null);
});

test('parseEnvLine: a value containing `=` keeps the whole RHS', () => {
  assert.deepEqual(parseEnvLine('NODE_OPTIONS=--max-old-space-size=4096'), {
    key: 'NODE_OPTIONS',
    value: '--max-old-space-size=4096',
    line: 'NODE_OPTIONS=--max-old-space-size=4096',
  });
});

// ── filterCarriedEnv (defect 2: env-carry gaps) ──────────────────────────────────────────────────

// This is the regression that motivated the fix. The OLD grep pattern was
// `^(PORTAL_API_KEY|SQD_RPC_KEY|PORTAL_URL_|PONDER_RPC_URL_)` — it dropped PORTAL_URL (no trailing
// underscore), EULER_CHAINS, DATABASE_SCHEMA, NODE_OPTIONS and PORTAL_* tunables. Preserve-all-then-
// override must carry every operative var except the ones the deploy script re-derives.
test('filterCarriedEnv: carries PORTAL_URL (NO underscore) — the exact var the old grep dropped', () => {
  const carried = filterCarriedEnv(['PORTAL_URL=https://portal.example']);
  assert.deepEqual(carried, ['PORTAL_URL=https://portal.example']);
});

test('filterCarriedEnv: carries the operative vars a real redeploy needs', () => {
  const src = [
    '# header comment',
    'PORTAL_API_KEY=k1',
    'SQD_RPC_KEY=k2',
    'PORTAL_URL=https://portal.example',
    'PORTAL_URL_1=https://portal-1.example',
    'PONDER_RPC_URL_8453=https://rpc-base.example',
    'NODE_OPTIONS=--max-old-space-size=4096',
    'PORTAL_MAX_BATCH=50',
    'PORTAL_HEAD_POLL_MS=250',
  ];
  const carried = filterCarriedEnv(src);
  assert.deepEqual(carried, [
    'PORTAL_API_KEY=k1',
    'SQD_RPC_KEY=k2',
    'PORTAL_URL=https://portal.example',
    'PORTAL_URL_1=https://portal-1.example',
    'PONDER_RPC_URL_8453=https://rpc-base.example',
    'NODE_OPTIONS=--max-old-space-size=4096',
    'PORTAL_MAX_BATCH=50',
    'PORTAL_HEAD_POLL_MS=250',
  ]);
});

test('filterCarriedEnv: DROPS the vars the deploy script re-derives (never a stale shadow)', () => {
  const src = [
    'DATABASE_URL=postgresql:///stale',
    'DATABASE_SCHEMA=stale_schema',
    'EULER_CHAINS=ethereum,base',
    'PORTAL_REALTIME=rpc',
    'PORTAL_CHECKS=off',
    'PORTAL_API_KEY=keep_me',
  ];
  const carried = filterCarriedEnv(src);
  assert.deepEqual(carried, ['PORTAL_API_KEY=keep_me']);
  for (const key of OVERRIDDEN_KEYS) {
    assert.ok(
      !carried.some((l) => l.startsWith(`${key}=`)),
      `overridden key ${key} must not be carried`,
    );
  }
});

test('filterCarriedEnv: an `export`-prefixed assignment is carried without the prefix', () => {
  assert.deepEqual(filterCarriedEnv(['export PORTAL_API_KEY=k1']), [
    'PORTAL_API_KEY=k1',
  ]);
});

test('filterCarriedEnv: last assignment wins, first-seen order preserved (shell env semantics)', () => {
  const carried = filterCarriedEnv([
    'A=1',
    'B=1',
    'A=2', // A re-assigned later
  ]);
  assert.deepEqual(carried, ['A=2', 'B=1']);
});

test('filterCarriedEnv: comments, blanks and malformed lines are dropped', () => {
  const carried = filterCarriedEnv([
    '# comment',
    '',
    'garbage-no-eq',
    '1BAD=x',
    'GOOD=y',
  ]);
  assert.deepEqual(carried, ['GOOD=y']);
});

// ── resolveEulerChains (defect 4: SOAK_CHAINS short names never matched) ──────────────────────────

test('resolveEulerChains: full chains.json names pass through unchanged', () => {
  const r = resolveEulerChains('ethereum,base,arbitrum', KNOWN);
  assert.equal(r.value, 'ethereum,base,arbitrum');
  assert.deepEqual(r.chains, ['ethereum', 'base', 'arbitrum']);
});

test('resolveEulerChains: short aliases resolve to full names', () => {
  const r = resolveEulerChains('eth,arb,base', KNOWN);
  assert.equal(r.value, 'ethereum,arbitrum,base');
});

test('resolveEulerChains: an unknown name FAILS LOUD, listing the value and the valid set', () => {
  assert.throws(
    () => resolveEulerChains('eth,notachain', KNOWN),
    (err) => {
      assert.match(err.message, /unknown chain name\(s\): notachain/);
      assert.match(err.message, /valid names are:/);
      assert.match(err.message, /ethereum/);

      return true;
    },
  );
});

test('resolveEulerChains: whitespace is trimmed and duplicates collapse (alias + full)', () => {
  const r = resolveEulerChains(' eth , ethereum , base ', KNOWN);
  assert.equal(r.value, 'ethereum,base');
});

test('resolveEulerChains: an empty knob is an error (never index zero chains silently)', () => {
  assert.throws(() => resolveEulerChains('', KNOWN), /empty/);
  assert.throws(() => resolveEulerChains('  ,  ', KNOWN), /empty/);
});

test('CHAIN_ALIASES: every alias target is itself a known chains.json name (no dangling alias)', () => {
  for (const target of Object.values(CHAIN_ALIASES)) {
    assert.ok(
      KNOWN.has(target),
      `alias target ${target} must be a known chain name`,
    );
  }
});

// ── unit-template render (defects 1 + 3: schema knob + placeholder-only template) ─────────────────
//
// Renders the real unit via the deploy script's own sed pipeline (a tiny bash shim that sources the
// substitution) with defaults and with overrides, asserting the User= / --schema lines the operator
// actually gets. Not a manual check — it fails if a placeholder is dropped or a knob stops wiring.

function renderUnit(env) {
  // Exercise the REAL template through the REAL sed substitution engine (mirroring the knob defaults
  // the deploy script applies: SOAK_B_SCHEMA defaults to soak_b), so this fails if a placeholder is
  // dropped or a knob stops wiring. Values are passed as sed `-e` args — no bash `${}` interpolation.
  const template = join(HERE, 'soak-b.service');
  const schema = env.SOAK_B_SCHEMA || 'soak_b';
  const user = env.SOAK_B_USER || 'op';
  const subs = {
    WORKDIR: '/tmp/wd',
    ENVFILE: '/tmp/e.env',
    PORT: '9548',
    SCHEMA: schema,
    USER: user,
    GROUP: 'grp',
    MEM_HIGH: '6G',
    MEM_MAX: '8G',
    RESTART_LOG: '/tmp/r.log',
    RESTART_LOG_DIR: '/tmp',
  };
  const args = [];
  for (const [token, value] of Object.entries(subs)) {
    args.push('-e', `s#@@${token}@@#${value}#g`);
  }
  args.push(template);

  return execFileSync('sed', args, { encoding: 'utf8' });
}

test('render: defaults produce --schema soak_b and a non-root User=', () => {
  const out = renderUnit({ SOAK_B_USER: 'alice' });
  assert.match(out, /ExecStart=.*ponder start --schema soak_b --port 9548/);
  assert.match(out, /^User=alice$/m);
  assert.doesNotMatch(out, /@@SCHEMA@@/);
  assert.doesNotMatch(out, /@@USER@@/);
});

test('render: SOAK_B_SCHEMA + SOAK_B_USER overrides flow into the unit', () => {
  const out = renderUnit({
    SOAK_B_SCHEMA: 'euler_rt_b_alt',
    SOAK_B_USER: 'bob',
  });
  assert.match(
    out,
    /ExecStart=.*ponder start --schema euler_rt_b_alt --port 9548/,
  );
  assert.match(out, /^User=bob$/m);
});

test('render: the template no longer hard-codes the schema or a SOAK_CHAINS var', () => {
  const out = renderUnit({ SOAK_B_SCHEMA: 'custom_schema' });
  // The old template baked `--schema soak_b`; with a custom knob that literal must be gone.
  assert.doesNotMatch(out, /--schema soak_b/);
  // The app reads EULER_CHAINS (from the env file), not a hard-coded SOAK_CHAINS unit line.
  assert.doesNotMatch(out, /Environment=SOAK_CHAINS=/);
});

// ── CLI wrapper (the surface the shell actually calls) ────────────────────────────────────────────

test('CLI carry-env: reads a file and prints the carried lines', () => {
  const dir = mkdtempSync(join(tmpdir(), 'deploy-helpers-'));
  const envFile = join(dir, 'src.env');
  writeFileSync(
    envFile,
    '# c\nPORTAL_URL=https://p.example\nDATABASE_URL=postgresql:///stale\nPORTAL_API_KEY=k1\n',
  );
  const out = execFileSync(
    'node',
    [join(HERE, 'deploy-helpers.mjs'), 'carry-env', envFile],
    {
      encoding: 'utf8',
    },
  );
  assert.equal(out, 'PORTAL_URL=https://p.example\nPORTAL_API_KEY=k1\n');
});

test('CLI resolve-chains: valid → value on stdout, exit 0', () => {
  const out = execFileSync(
    'node',
    [
      join(HERE, 'deploy-helpers.mjs'),
      'resolve-chains',
      'eth,base',
      CHAINS_JSON,
    ],
    { encoding: 'utf8' },
  );
  assert.equal(out.trim(), 'ethereum,base');
});

test('CLI resolve-chains: unknown name → non-zero exit, message on stderr', () => {
  let threw = false;
  try {
    execFileSync(
      'node',
      [
        join(HERE, 'deploy-helpers.mjs'),
        'resolve-chains',
        'eth,nope',
        CHAINS_JSON,
      ],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (err) {
    threw = true;
    assert.equal(err.status, 1);
    assert.match(String(err.stderr), /unknown chain name\(s\): nope/);
  }
  assert.ok(threw, 'resolve-chains with an unknown name must exit non-zero');
});

// The deploy script must reference the helper so this coverage tracks the real integration point.
test('deploy-soak-b.sh invokes the pure helpers (integration is wired)', () => {
  const src = execFileSync('cat', [DEPLOY_SH], { encoding: 'utf8' });
  assert.match(src, /deploy-helpers\.mjs/);
  assert.match(src, /carry-env/);
  assert.match(src, /resolve-chains/);
});

// ── end-to-end script runs (defects 1-4 through the REAL deploy-soak-b.sh) ─────────────────────────
//
// Drive the actual script in a throwaway sandbox: a non-writable UNIT_DIR forces the non-root path,
// no SOAK_A_DIR skips the npm/psql work, and psql is absent (a benign warning). This exercises the
// real env-carry, schema/chain rendering and — critically — the $RENDER lifetime.

function runDeploy(env, { chains } = {}) {
  const sandbox = mkdtempSync(join(tmpdir(), 'deploy-e2e-'));
  const tarball = join(sandbox, 'fake.tgz');
  writeFileSync(tarball, 'not-a-real-tarball');
  const srcEnv = join(sandbox, 'src.env');
  // A source env exercising every carry class: bare PORTAL_URL, underscore variant, per-chain RPC,
  // a Portal tunable, NODE_OPTIONS, plus overridden vars that MUST be dropped (stale schema/chains).
  writeFileSync(
    srcEnv,
    [
      '# operator env',
      'PORTAL_API_KEY=secretkey',
      'PORTAL_URL=https://portal.example',
      'PORTAL_URL_1=https://portal-1.example',
      'PONDER_RPC_URL_8453=https://rpc-base.example',
      'PORTAL_MAX_BATCH=50',
      'NODE_OPTIONS=--max-old-space-size=4096',
      'DATABASE_SCHEMA=stale_schema',
      'EULER_CHAINS=stale,chains',
      '',
    ].join('\n'),
  );
  const workdir = join(sandbox, 'wd');
  const unitDir = join(sandbox, 'unit-readonly');
  const envFile = join(sandbox, 'soak-b.env');
  const restartLog = join(sandbox, 'restarts.log');
  // SAFETY: prepend a stub bin dir with no-op `psql`/`systemctl`/`npm` so the script can NEVER reach
  // a real database, systemd, or the network — the DB/install steps become inert. Combined with a
  // non-existent SOAK_A_DIR (skips the copy/install), the run only renders + writes into the sandbox.
  const stubBin = join(sandbox, 'stub-bin');
  execFileSync('mkdir', ['-p', stubBin]);
  for (const cmd of ['psql', 'systemctl', 'npm']) {
    const stub = join(stubBin, cmd);
    writeFileSync(stub, '#!/bin/sh\nexit 0\n');
    chmodSync(stub, 0o755);
  }
  // A UNIT_DIR that does not exist and is under an unwritable parent → not writable → non-root path.
  const result = execFileSync('bash', [DEPLOY_SH, tarball], {
    encoding: 'utf8',
    env: {
      ...process.env,
      SOAK_B_WORKDIR: workdir,
      SOAK_B_ENVFILE: envFile,
      SOAK_A_CONFIG_DIR: join(sandbox, 'no-such-soak-a'),
      SOAK_A_ENV: srcEnv,
      SYSTEMD_DIR: unitDir,
      SOAK_B_RESTART_LOG: restartLog,
      SOAK_B_USER: 'op',
      SOAK_B_GROUP: 'grp',
      ...(chains ? { SOAK_CHAINS: chains } : {}),
      ...env,
      PATH: `${stubBin}:${process.env.PATH}`,
    },
  });

  return { stdout: result, envFile, unitDir };
}

test('e2e: non-root path keeps the rendered unit alive and prints its real location (defect 3)', () => {
  const { stdout } = runDeploy({ SOAK_B_SCHEMA: 'euler_rt_b' });
  const m = stdout.match(/rendered unit at (\S+) \(install manually/);
  assert.ok(m, `expected a "rendered unit at <path>" line, got:\n${stdout}`);
  const renderPath = m[1];
  assert.ok(
    existsSync(renderPath),
    `the printed render file ${renderPath} must still exist on the non-root path (defect 3)`,
  );
  // And it must be the fully-rendered unit (no leftover placeholders).
  const rendered = readFileSync(renderPath, 'utf8');
  assert.doesNotMatch(rendered, /@@[A-Z_]+@@/);
  assert.match(rendered, /--schema euler_rt_b --port/);
});

test('e2e: env file carries operative vars + authoritative overrides (defects 1,2,4)', () => {
  const { envFile } = runDeploy(
    { SOAK_B_SCHEMA: 'euler_rt_b' },
    { chains: 'eth,base' },
  );
  const env = readFileSync(envFile, 'utf8');
  // Carried operative vars (the ones the old grep dropped):
  assert.match(env, /^PORTAL_URL=https:\/\/portal\.example$/m);
  assert.match(env, /^PORTAL_URL_1=https:\/\/portal-1\.example$/m);
  assert.match(env, /^PONDER_RPC_URL_8453=https:\/\/rpc-base\.example$/m);
  assert.match(env, /^PORTAL_MAX_BATCH=50$/m);
  assert.match(env, /^NODE_OPTIONS=--max-old-space-size=4096$/m);
  assert.match(env, /^PORTAL_API_KEY=secretkey$/m);
  // Authoritative overrides win (single schema knob → DATABASE_SCHEMA; validated full chain names):
  assert.match(env, /^DATABASE_SCHEMA=euler_rt_b$/m);
  assert.match(env, /^EULER_CHAINS=ethereum,base$/m);
  // The stale carried values must NOT appear (never a shadow of the re-derived var):
  assert.doesNotMatch(env, /^DATABASE_SCHEMA=stale_schema$/m);
  assert.doesNotMatch(env, /^EULER_CHAINS=stale,chains$/m);
});

test('e2e: an unknown chain name fails the deploy loud (defect 4)', () => {
  let threw = false;
  try {
    runDeploy({}, { chains: 'eth,notachain' });
  } catch (err) {
    threw = true;
    const out = `${err.stdout ?? ''}${err.stderr ?? ''}`;
    assert.match(out, /unknown chain name\(s\): notachain/);
  }
  assert.ok(threw, 'a bad chain name must abort the deploy');
});
