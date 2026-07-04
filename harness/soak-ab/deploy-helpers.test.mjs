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
  assertNoMultilineValues,
  CHAIN_ALIASES,
  deriveDatabaseUrl,
  effectiveOverriddenKeys,
  filterCarriedEnv,
  loadKnownChains,
  OVERRIDDEN_KEYS,
  parseEnvLine,
  parseUnitEnvironmentKeys,
  resolveEulerChains,
} from './deploy-helpers.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const CHAINS_JSON = join(HERE, '..', 'euler-multichain', 'chains.json');
const DEPLOY_SH = join(HERE, 'deploy-soak-b.sh');
const UNIT_TEMPLATE = join(HERE, 'soak-b.service');
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

// ── F1: unit-template Environment= keys must never be carried (systemd EnvironmentFile precedence) ──
//
// systemd: an EnvironmentFile= value OVERRIDES an Environment= value. So any key the unit renders via
// Environment= must be in the carry-exclusion set — otherwise a STALE carried copy silently shadows
// the unit's freshly-rendered value (e.g. a stale SOAK_B_RESTART_LOG could point the restart-log
// write outside ReadWritePaths). We DERIVE the exclusion from the template so it can't drift.

test('parseUnitEnvironmentKeys: extracts every Environment=KEY from the unit text', () => {
  const unit = [
    '[Service]',
    'Environment=PORTAL_REALTIME=stream',
    'Environment=PONDER_LOG_LEVEL=info',
    'Environment=CI=true',
    'EnvironmentFile=/x/soak-b.env', // NOT an Environment= line — must be ignored
    '# Environment=COMMENTED=out', // a comment — must be ignored
  ].join('\n');
  const keys = parseUnitEnvironmentKeys(unit);
  assert.deepEqual([...keys].sort(), [
    'CI',
    'PONDER_LOG_LEVEL',
    'PORTAL_REALTIME',
  ]);
  assert.ok(
    !keys.has('EnvironmentFile'),
    'EnvironmentFile= is not an Environment= key',
  );
});

test('parseUnitEnvironmentKeys: handles several assignments on one Environment= line', () => {
  const keys = parseUnitEnvironmentKeys('Environment=A=1 B=2 C=3');
  assert.deepEqual([...keys].sort(), ['A', 'B', 'C']);
});

test('parseUnitEnvironmentKeys: honors systemd double/single quoting (no dropped keys)', () => {
  // systemd lets an assignment be wrapped in "…"/'…' so the value can contain spaces. A naive
  // whitespace split would parse `"FOO` (invalid identifier → silently dropped) and the tripwire
  // would then MISS FOO — the exact drift the derivation exists to prevent.
  const dq = parseUnitEnvironmentKeys('Environment="FOO=a b" BAR=c');
  assert.deepEqual([...dq].sort(), ['BAR', 'FOO']);
  const sq = parseUnitEnvironmentKeys("Environment='X=y z'");
  assert.deepEqual([...sq], ['X']);
});

test('parseUnitEnvironmentKeys: a backslash-escaped quote inside a "…" item does not end it', () => {
  // systemd honors \" inside double quotes — the item continues past it. A tokenizer that closed on
  // the \" would splinter the line and drop the keys AFTER it (BAR, BAZ), a silent tripwire miss.
  const bs = String.fromCharCode(92);
  const keys = parseUnitEnvironmentKeys(
    `Environment="FOO=a${bs}" b" BAR=c BAZ=d`,
  );
  assert.deepEqual([...keys].sort(), ['BAR', 'BAZ', 'FOO']);
});

test('effectiveOverriddenKeys: unions OVERRIDDEN_KEYS with the unit Environment= keys', () => {
  const unit = 'Environment=PONDER_LOG_LEVEL=info\nEnvironment=NEW_UNIT_KNOB=x';
  const eff = effectiveOverriddenKeys(unit);
  for (const k of OVERRIDDEN_KEYS) {
    assert.ok(eff.has(k), `${k} from OVERRIDDEN_KEYS must remain excluded`);
  }
  assert.ok(
    eff.has('PONDER_LOG_LEVEL'),
    'a unit Environment= key must be excluded',
  );
  assert.ok(
    eff.has('NEW_UNIT_KNOB'),
    'a NEW unit Environment= key must be excluded (no drift)',
  );
});

test('effectiveOverriddenKeys: with no unit text falls back to OVERRIDDEN_KEYS alone', () => {
  assert.deepEqual(
    [...effectiveOverriddenKeys()].sort(),
    [...OVERRIDDEN_KEYS].sort(),
  );
});

// TRIPWIRE: parse the REAL committed unit template and assert every Environment=-rendered key is in
// the effective exclusion set. Adding an `Environment=NEW=…` line to soak-b.service without it being
// covered by the derived exclusion fails HERE — this is the anti-drift guarantee for F1.
test('TRIPWIRE: every Environment= key in the real soak-b.service is excluded from the carry', () => {
  const unit = readFileSync(UNIT_TEMPLATE, 'utf8');
  const unitKeys = parseUnitEnvironmentKeys(unit);
  assert.ok(
    unitKeys.size > 0,
    'the unit template must render at least one Environment= key',
  );
  const eff = effectiveOverriddenKeys(unit);
  for (const key of unitKeys) {
    assert.ok(
      eff.has(key),
      `Environment= key ${key} rendered by soak-b.service must be excluded from the env carry ` +
        '(systemd EnvironmentFile= overrides Environment=; a stale carried copy would shadow it)',
    );
  }
  // And that a real carry using this exclusion drops those keys even when the source env has them stale.
  const staleSrc = [...unitKeys].map((k) => `${k}=STALE_${k}`);
  const carried = filterCarriedEnv(staleSrc, eff);
  assert.deepEqual(
    carried,
    [],
    'no unit Environment= key may survive the carry',
  );
});

// ── F3: multi-line / unterminated-quote source values must fail loud (never a truncated line) ──────

test('assertNoMultilineValues: a single-line env passes', () => {
  assert.doesNotThrow(() =>
    assertNoMultilineValues(
      'FOO=bar\nBAZ="quoted value"\nQ=\'single quoted\'\n',
    ),
  );
});

test("assertNoMultilineValues: a FOO='a<newline>b' value FAILS LOUD naming FOO", () => {
  const text = "PORTAL_API_KEY=k\nFOO='a\nb'\nBAR=ok\n";
  assert.throws(
    () => assertNoMultilineValues(text),
    (err) => {
      assert.match(
        err.message,
        /env var FOO has an unterminated ' quote \/ multi-line value/,
      );

      return true;
    },
  );
});

test('assertNoMultilineValues: an unterminated double quote FAILS LOUD naming the key', () => {
  assert.throws(
    () => assertNoMultilineValues('X=1\nMSG="hello\nworld"\n'),
    /env var MSG has an unterminated " quote/,
  );
});

test('assertNoMultilineValues: an unquoted value containing a stray quote-like char is fine', () => {
  // No LEADING quote → single-line token, cannot straddle a newline → must not trip the guard.
  assert.doesNotThrow(() =>
    assertNoMultilineValues("URL=https://a.example/x'y\n"),
  );
});

// A closing double-quote ESCAPED by a backslash does NOT terminate the value (its real content
// continues on the next line) — a bare last-char check would wrongly pass it. Backslash escaping is
// honored only inside double quotes; inside single quotes `\` is literal so `'a\'` DOES close.
const BS = String.fromCharCode(92); // backslash, built by code to avoid escaping confusion
const DQ = '"';
const SQ = "'";

test('assertNoMultilineValues: a double-quoted value whose final quote is backslash-escaped FAILS LOUD', () => {
  // D="abc\  → the final " is escaped by the \, so the value is unterminated (continues next line).
  assert.throws(
    () => assertNoMultilineValues(`D=${DQ}abc${BS}${DQ}\nNEXT=line\n`),
    /env var D has an unterminated " quote/,
  );
});

test('assertNoMultilineValues: an escaped inner quote with a real closing quote is fine', () => {
  // D="a\"b" → the inner \" is escaped, the trailing " is a real close → single-line, must not trip.
  assert.doesNotThrow(() =>
    assertNoMultilineValues(`D=${DQ}a${BS}${DQ}b${DQ}\n`),
  );
});

test("assertNoMultilineValues: single-quoted 'a\\' closes (backslash is literal in single quotes)", () => {
  // S='a\' → single quotes don't process the backslash, so the quote closes → must not trip.
  assert.doesNotThrow(() => assertNoMultilineValues(`S=${SQ}a${BS}${SQ}\n`));
});

test('assertNoMultilineValues: an UNQUOTED value ending in a trailing backslash FAILS LOUD (continuation)', () => {
  // FOO=abc\ + newline → systemd/shell splice the next line on; emitted truncated → refuse it.
  assert.throws(
    () => assertNoMultilineValues(`FOO=abc${BS}\nNEXT=line\n`),
    /env var FOO ends in a line-continuation backslash \/ multi-line value/,
  );
});

test('assertNoMultilineValues: an unquoted value ending in an EVEN run of backslashes is fine', () => {
  // FOO=abc\\ → an escaped literal backslash, not a continuation → must not trip.
  assert.doesNotThrow(() =>
    assertNoMultilineValues(`FOO=abc${BS}${BS}\nNEXT=line\n`),
  );
});

test('assertNoMultilineValues: a CLOSED quote followed by a trailing backslash FAILS LOUD (continuation)', () => {
  // FOO="abc"\ + newline → the quote closes but the value still ends in a continuation backslash, so
  // it splices the next line on. The quote-branch must not short-circuit past the backslash check.
  assert.throws(
    () => assertNoMultilineValues(`FOO=${DQ}abc${DQ}${BS}\nNEXT=line\n`),
    /env var FOO ends in a line-continuation backslash \/ multi-line value/,
  );
});

test('assertNoMultilineValues: a plainly-closed quoted value with a space is fine (no false positive)', () => {
  // The full-value trailing-backslash check must not trip a normal `KEY="a b"` (ends in a quote char).
  assert.doesNotThrow(() =>
    assertNoMultilineValues(`BAZ=${DQ}quoted value${DQ}\n`),
  );
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

// ── deriveDatabaseUrl (defect: redeploy clobbered a role-authenticated TCP DATABASE_URL) ──────────
//
// The env regen used to author `postgresql:///${DB_NAME}` unconditionally (peer auth), silently
// replacing a working role+password TCP URL. deriveDatabaseUrl must swap ONLY the database and
// preserve scheme/userinfo/host/port/query — via a real URL parser, never shell string surgery.

test('deriveDatabaseUrl: swaps only the database, preserving role, host, port and query', () => {
  const out = deriveDatabaseUrl(
    'postgresql://soakrole:pw@db.internal:6432/olddb?sslmode=require',
    'euler_rt_b',
  );
  assert.equal(
    out,
    'postgresql://soakrole:pw@db.internal:6432/euler_rt_b?sslmode=require',
  );
});

// THE class the issue pins: a password with reserved URL characters. Shell `${A%/*}/newdb` surgery
// corrupts these (splits on the `/` inside the password); a real URL parser preserves them exactly.
test('deriveDatabaseUrl: a reserved-characters-in-password URL round-trips (only the DB swaps)', () => {
  // Password is p@ss/w?rd#1 percent-encoded — every reserved char (@ / ? #) that shell surgery would
  // mangle. The userinfo, host, port and query must survive byte-for-byte; only the path changes.
  const src =
    'postgresql://role:p%40ss%2Fw%3Frd%231@db.host:5432/euler?sslmode=verify-full';
  const out = deriveDatabaseUrl(src, 'euler_rt_b');
  assert.equal(
    out,
    'postgresql://role:p%40ss%2Fw%3Frd%231@db.host:5432/euler_rt_b?sslmode=verify-full',
  );
  // And the derived URL still parses to the SAME password the source encoded (no corruption).
  assert.equal(new URL(out).password, 'p%40ss%2Fw%3Frd%231');
  assert.equal(new URL(out).pathname, '/euler_rt_b');
});

test('deriveDatabaseUrl: preserves the postgres:// scheme variant unchanged', () => {
  assert.equal(
    deriveDatabaseUrl('postgres://u:p@h:5432/old', 'euler_rt_b'),
    'postgres://u:p@h:5432/euler_rt_b',
  );
});

test('deriveDatabaseUrl: a peer-auth source (no host/userinfo) swaps only the DB', () => {
  // postgresql:///olddb → the empty-authority peer-auth form; still parseable, DB swapped in place.
  assert.equal(
    deriveDatabaseUrl('postgresql:///olddb', 'euler_rt_b'),
    'postgresql:///euler_rt_b',
  );
});

test('deriveDatabaseUrl: an unparseable source URL throws (caller decides the fallback)', () => {
  assert.throws(() => deriveDatabaseUrl('not a url', 'euler_rt_b'));
});

test('deriveDatabaseUrl: a dbName that is not a bare SQL identifier is refused (no injection)', () => {
  // A dbName carrying path/query syntax must never be smuggled into the URL — fail loud instead.
  assert.throws(
    () => deriveDatabaseUrl('postgresql://u:p@h/old', 'euler_rt_b?evil=1'),
    /not a bare SQL identifier/,
  );
  assert.throws(
    () => deriveDatabaseUrl('postgresql://u:p@h/old', 'a/b'),
    /not a bare SQL identifier/,
  );
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

// CLI derive-database-url: derive from the source env's DATABASE_URL, or exit-3 to signal fallback.
test('CLI derive-database-url: derives from the source env DATABASE_URL, DB swapped only', () => {
  const dir = mkdtempSync(join(tmpdir(), 'deploy-helpers-'));
  const envFile = join(dir, 'src.env');
  writeFileSync(
    envFile,
    'PORTAL_API_KEY=k\nexport DATABASE_URL=postgresql://role:p%40ss@db.host:5432/euler?sslmode=require\n',
  );
  const out = execFileSync(
    'node',
    [
      join(HERE, 'deploy-helpers.mjs'),
      'derive-database-url',
      envFile,
      'euler_rt_b',
    ],
    { encoding: 'utf8' },
  );
  assert.equal(
    out.trim(),
    'postgresql://role:p%40ss@db.host:5432/euler_rt_b?sslmode=require',
  );
});

test('CLI derive-database-url: no source DATABASE_URL → exit 3 (silent fallback signal), no stdout', () => {
  const dir = mkdtempSync(join(tmpdir(), 'deploy-helpers-'));
  const envFile = join(dir, 'src.env');
  writeFileSync(envFile, 'PORTAL_API_KEY=k\nPORTAL_URL=https://p.example\n');
  let threw = false;
  try {
    execFileSync(
      'node',
      [
        join(HERE, 'deploy-helpers.mjs'),
        'derive-database-url',
        envFile,
        'euler_rt_b',
      ],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (err) {
    threw = true;
    assert.equal(
      err.status,
      3,
      'no source DATABASE_URL must signal fallback via exit 3',
    );
    assert.equal(
      String(err.stdout).trim(),
      '',
      'must print nothing on the fallback signal',
    );
  }
  assert.ok(threw, 'a missing source DATABASE_URL must exit non-zero (3)');
});

test('CLI derive-database-url: an unparseable source DATABASE_URL → loud exit 1', () => {
  const dir = mkdtempSync(join(tmpdir(), 'deploy-helpers-'));
  const envFile = join(dir, 'src.env');
  writeFileSync(envFile, 'DATABASE_URL=this is not a url\n');
  let threw = false;
  try {
    execFileSync(
      'node',
      [
        join(HERE, 'deploy-helpers.mjs'),
        'derive-database-url',
        envFile,
        'euler_rt_b',
      ],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (err) {
    threw = true;
    assert.equal(
      err.status,
      1,
      'an unparseable source URL must be a loud abort (exit 1)',
    );
  }
  assert.ok(threw, 'an unparseable source DATABASE_URL must exit non-zero (1)');
});

// The deploy script must reference the helper so this coverage tracks the real integration point.
test('deploy-soak-b.sh invokes the pure helpers (integration is wired)', () => {
  const src = execFileSync('cat', [DEPLOY_SH], { encoding: 'utf8' });
  assert.match(src, /deploy-helpers\.mjs/);
  assert.match(src, /carry-env/);
  assert.match(src, /resolve-chains/);
  assert.match(src, /derive-database-url/);
});

// ── end-to-end script runs (defects 1-4 through the REAL deploy-soak-b.sh) ─────────────────────────
//
// Drive the actual script in a throwaway sandbox: a non-writable UNIT_DIR forces the non-root path,
// no SOAK_A_DIR skips the npm/psql work, and psql is absent (a benign warning). This exercises the
// real env-carry, schema/chain rendering and — critically — the $RENDER lifetime.

function runDeploy(
  env,
  { chains, srcEnvLines, noNode, psqlExit, psqlStderr, soakADir } = {},
) {
  const sandbox = mkdtempSync(join(tmpdir(), 'deploy-e2e-'));
  const tarball = join(sandbox, 'fake.tgz');
  writeFileSync(tarball, 'not-a-real-tarball');
  const srcEnv = join(sandbox, 'src.env');
  // A source env exercising every carry class: bare PORTAL_URL, underscore variant, per-chain RPC,
  // a Portal tunable, NODE_OPTIONS, plus overridden vars that MUST be dropped (stale schema/chains)
  // AND a stale copy of a unit Environment= key (SOAK_B_RESTART_LOG) that must not shadow the render.
  const defaultLines = [
    '# operator env',
    'PORTAL_API_KEY=secretkey',
    'PORTAL_URL=https://portal.example',
    'PORTAL_URL_1=https://portal-1.example',
    'PONDER_RPC_URL_8453=https://rpc-base.example',
    'PORTAL_MAX_BATCH=50',
    'NODE_OPTIONS=--max-old-space-size=4096',
    'DATABASE_SCHEMA=stale_schema',
    'EULER_CHAINS=stale,chains',
    'SOAK_B_RESTART_LOG=/stale/outside/readwritepaths.log',
    'PONDER_LOG_LEVEL=trace',
    '',
  ];
  writeFileSync(srcEnv, (srcEnvLines ?? defaultLines).join('\n'));
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
  // psqlExit (defect 1): override the psql stub to simulate a connection FAILURE (non-zero exit +
  // a diagnostic on stderr), proving the deploy fails LOUD naming PGADMIN_URL instead of dying with
  // a bare exit 2 under `set -euo pipefail` (the swallowed-diagnostic bug this fix closes).
  if (psqlExit != null) {
    const stub = join(stubBin, 'psql');
    const diag =
      psqlStderr ?? 'could not connect to server: Connection refused';
    writeFileSync(stub, `#!/bin/sh\necho '${diag}' >&2\nexit ${psqlExit}\n`);
    chmodSync(stub, 0o755);
  }
  // noNode (M2): run with a PATH that has every coreutil the script needs EXCEPT `node`, to prove the
  // deploy FAILS LOUD when node is absent (the removed grep fallback). Build an isolated bin dir of
  // symlinks to the real tools (node deliberately omitted) rather than trying to prune node out of the
  // real PATH — node lives in the same dir as the coreutils here, so a subtractive PATH is impossible.
  let pathValue = `${stubBin}:${process.env.PATH}`;
  if (noNode) {
    const nodelessBin = join(sandbox, 'nodeless-bin');
    execFileSync('mkdir', ['-p', nodelessBin]);
    for (const tool of [
      'bash',
      'sh',
      'sed',
      'grep',
      'mktemp',
      'id',
      'chmod',
      'touch',
      'cp',
      'rm',
      'mkdir',
      'date',
      'dirname',
      'tr',
      'cat',
      'env',
    ]) {
      const real = execFileSync('sh', ['-c', `command -v ${tool} || true`], {
        encoding: 'utf8',
      }).trim();
      if (real) {
        execFileSync('ln', ['-s', real, join(nodelessBin, tool)]);
      }
    }
    // stubBin (psql/systemctl/npm stubs) is on the path so those steps stay inert; `node` is NOT.
    pathValue = `${stubBin}:${nodelessBin}`;
  }
  // A UNIT_DIR that does not exist and is under an unwritable parent → not writable → non-root path.
  let result;
  try {
    result = execFileSync('bash', [DEPLOY_SH, tarball], {
      encoding: 'utf8',
      env: {
        ...process.env,
        SOAK_B_WORKDIR: workdir,
        SOAK_B_ENVFILE: envFile,
        SOAK_A_CONFIG_DIR: soakADir ?? join(sandbox, 'no-such-soak-a'),
        SOAK_A_ENV: srcEnv,
        SYSTEMD_DIR: unitDir,
        SOAK_B_RESTART_LOG: restartLog,
        SOAK_B_USER: 'op',
        SOAK_B_GROUP: 'grp',
        ...(chains ? { SOAK_CHAINS: chains } : {}),
        ...env,
        PATH: pathValue,
      },
    });
  } catch (err) {
    // Surface the sandbox paths on the failure path so abort-behavior tests can assert on them (e.g.
    // that a failed carry left NO partial env file behind).
    err.envFile = envFile;
    err.sandbox = sandbox;
    throw err;
  }

  return { stdout: result, envFile, unitDir, sandbox, workdir };
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

// F1 (M1) end-to-end: the deploy passes the unit template to carry-env, so a stale carried copy of a
// key the unit renders via Environment= (SOAK_B_RESTART_LOG, PONDER_LOG_LEVEL — both in the default
// source env above) must NOT land in the written env file. systemd's EnvironmentFile= would override
// the unit's Environment=, so a survivor here would silently shadow the freshly-rendered value.
test('e2e: stale copies of unit Environment= keys are dropped from the written env (F1)', () => {
  const { envFile } = runDeploy(
    { SOAK_B_SCHEMA: 'euler_rt_b' },
    { chains: 'eth,base' },
  );
  const env = readFileSync(envFile, 'utf8');
  // The env file legitimately re-authors PONDER_* / DATABASE_* itself; what must NOT survive is the
  // STALE carried value that would shadow the unit render.
  assert.doesNotMatch(
    env,
    /^SOAK_B_RESTART_LOG=\/stale\/outside\/readwritepaths\.log$/m,
    'a stale SOAK_B_RESTART_LOG must never be carried (unit renders it via Environment=)',
  );
  assert.doesNotMatch(
    env,
    /^PONDER_LOG_LEVEL=trace$/m,
    'a stale PONDER_LOG_LEVEL must never be carried (unit renders it via Environment=)',
  );
  // Sanity: the carry still works for a normal operative var alongside the dropped ones.
  assert.match(env, /^PORTAL_URL=https:\/\/portal\.example$/m);
});

// M2 end-to-end: node is REQUIRED. With node absent from PATH the deploy must FAIL LOUD (no silent
// grep fallback) — before it creates the DB or writes the env file.
test('e2e: node absent → deploy fails loud, no silent grep fallback (M2)', () => {
  let threw = false;
  try {
    runDeploy(
      { SOAK_B_SCHEMA: 'euler_rt_b' },
      { chains: 'eth,base', noNode: true },
    );
  } catch (err) {
    threw = true;
    const out = `${err.stdout ?? ''}${err.stderr ?? ''}`;
    assert.match(out, /node not found on PATH/);
  }
  assert.ok(
    threw,
    'a missing node must abort the deploy (the grep fallback was removed)',
  );
});

// L (F3) end-to-end: a multi-line / unterminated-quote value in the source env must abort the deploy
// naming the offending key, rather than emitting a truncated env line.
test('e2e: a multi-line source env value fails the deploy loud naming the key (F3)', () => {
  const srcEnvLines = [
    'PORTAL_API_KEY=secretkey',
    'PORTAL_URL=https://portal.example',
    "MULTILINE='line-one",
    "line-two'",
    '',
  ];
  let threw = false;
  try {
    runDeploy(
      { SOAK_B_SCHEMA: 'euler_rt_b' },
      { chains: 'eth,base', srcEnvLines },
    );
  } catch (err) {
    threw = true;
    const out = `${err.stdout ?? ''}${err.stderr ?? ''}`;
    assert.match(
      out,
      /env var MULTILINE has an unterminated ' quote \/ multi-line value/,
    );
    // And the aborted carry must leave NO partial secrets file behind: the env file is built in a
    // temp and mv'd into place only on success, so on this failure $ENVFILE must not exist at all.
    assert.ok(
      !existsSync(err.envFile),
      'a failed carry must not leave a partial env file behind',
    );
  }
  assert.ok(threw, 'a multi-line env value must abort the deploy');
});

// ── issue #35 defect 1: the DB-exists probe must fail LOUD when psql cannot connect ────────────────
//
// Under `set -euo pipefail` the old `EXISTS="$(psql … 2>/dev/null | tr …)"` died with a bare exit 2
// when psql could not connect — the pipeline failed under pipefail, `set -e` aborted, and 2>/dev/null
// had swallowed the only diagnostic. Now the probe runs outside the assignment, psql's exit is checked
// explicitly, and a connect failure aborts LOUD naming PGADMIN_URL and echoing psql's own diagnostic.
test('e2e: a psql connect failure aborts LOUD naming PGADMIN_URL, not a bare exit (issue #35 defect 1)', () => {
  let threw = false;
  try {
    runDeploy(
      {
        SOAK_B_SCHEMA: 'euler_rt_b',
        PGADMIN_URL: 'postgres://nobody@127.0.0.1:1/postgres',
      },
      {
        chains: 'eth,base',
        psqlExit: 2,
        psqlStderr:
          'psql: error: connection to server failed: FATAL: role "nobody" does not exist',
      },
    );
  } catch (err) {
    threw = true;
    const out = `${err.stdout ?? ''}${err.stderr ?? ''}`;
    assert.match(
      out,
      /could not connect to Postgres to check for database euler_rt_b/,
      `expected a loud connect-failure message, got:\n${out}`,
    );
    // Names PGADMIN_URL (the actionable knob) and surfaces psql's own diagnostic — not swallowed.
    assert.match(
      out,
      /PGADMIN_URL=postgres:\/\/nobody@127\.0\.0\.1:1\/postgres/,
    );
    assert.match(out, /role "nobody" does not exist/);
    // And it must NOT have proceeded to write the env file (aborted at the DB step).
    assert.ok(
      !existsSync(err.envFile),
      'a psql connect failure must abort before the env file is written',
    );
  }
  assert.ok(
    threw,
    'a psql connect failure must abort the deploy loud (not a silent bare exit)',
  );
});

// ── issue #35 defect 2: a role-authenticated TCP DATABASE_URL must be preserved, only the DB swapped ─
//
// The old regen authored `postgresql:///euler_rt_b` unconditionally (peer auth), silently clobbering
// a working role+password TCP URL from the source env. Now the new URL is DERIVED from the source's
// own DATABASE_URL — scheme/userinfo/host/port/query preserved, only the database swapped.
test('e2e: a role-authenticated TCP DATABASE_URL is preserved, only the DB swapped (issue #35 defect 2)', () => {
  const srcEnvLines = [
    'PORTAL_API_KEY=secretkey',
    'PORTAL_URL=https://portal.example',
    // A reserved-char password (p@ss/word, percent-encoded) over TCP — the class shell surgery breaks.
    'DATABASE_URL=postgresql://soakrole:p%40ss%2Fword@db.internal:6432/euler_rt?sslmode=require',
    '',
  ];
  const { envFile } = runDeploy(
    { SOAK_B_SCHEMA: 'euler_rt_b' },
    { chains: 'eth,base', srcEnvLines },
  );
  const env = readFileSync(envFile, 'utf8');
  // The derived URL keeps role/password/host/port/query verbatim; only the database becomes euler_rt_b.
  assert.match(
    env,
    /^DATABASE_URL=postgresql:\/\/soakrole:p%40ss%2Fword@db\.internal:6432\/euler_rt_b\?sslmode=require$/m,
  );
  // The peer-auth clobber must NOT have replaced the working TCP URL.
  assert.doesNotMatch(env, /^DATABASE_URL=postgresql:\/\/\/euler_rt_b$/m);
});

test('e2e: no source DATABASE_URL → falls back to the peer-auth form (issue #35 defect 2)', () => {
  // The default source env has no DATABASE_URL, so the derive helper signals fallback (exit 3) and the
  // script authors the peer-auth form — the only case where that form is correct.
  const { envFile } = runDeploy(
    { SOAK_B_SCHEMA: 'euler_rt_b' },
    { chains: 'eth,base' },
  );
  const env = readFileSync(envFile, 'utf8');
  assert.match(env, /^DATABASE_URL=postgresql:\/\/\/euler_rt_b$/m);
});

// ── issue #35 defect 3: a SOAK_A_DIR that exists but holds NO expected config must warn LOUD ────────
//
// The config-copy loop used to no-op silently when the source dir existed but contained none of the
// expected files, leaving a half-provisioned workdir. Now it warns loud naming the source dir.
test('e2e: a SOAK_A_DIR with none of the expected config warns loud (issue #35 defect 3)', () => {
  const emptyDir = mkdtempSync(join(tmpdir(), 'soak-a-empty-'));
  writeFileSync(join(emptyDir, 'README.txt'), 'not app config'); // exists, but no expected file
  const { stdout } = runDeploy(
    { SOAK_B_SCHEMA: 'euler_rt_b' },
    { chains: 'eth,base', soakADir: emptyDir },
  );
  assert.match(
    stdout,
    new RegExp(
      `${emptyDir.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')} exists but has none of the expected app config`,
    ),
    `expected a loud "exists but has none of the expected app config" warning, got:\n${stdout}`,
  );
});
