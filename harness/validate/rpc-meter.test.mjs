import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { copyFileSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { redactTarget } from './rpc-meter.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const METER = join(HERE, 'rpc-meter.mjs');

// A FAKE credential that (a) matches the meter's credential-like pattern (len ≥ 20, [A-Za-z0-9~_-]),
// including the `~` and `-` the real key shape uses, and (b) does NOT trip the repo's real-key
// secret scan — it uses a neutral `fake_key_` prefix rather than any live-credential prefix. Every
// assertion checks THIS literal never survives into a log line or the state file.
const FAKE_KEY = 'fake_key_FAKEFAKE~xy-0123zz';

// ── redactTarget: keyed URL → key gone, host + slug preserved ───────────────────────────────────

test('redactTarget: a keyed URL loses the credential tail but keeps scheme + host + slug', () => {
  const out = redactTarget(`https://rpc.example.invalid/eth/${FAKE_KEY}`);
  assert.equal(
    out.includes(FAKE_KEY),
    false,
    'the credential segment must not survive redaction',
  );
  assert.ok(out.startsWith('https://rpc.example.invalid'), 'host is preserved');
  assert.ok(out.includes('/eth/'), 'the chain slug segment is preserved');
  assert.ok(out.endsWith('/<redacted>'), 'the tail is the fixed marker');
});

test('redactTarget: a key containing ~ and - is fully removed', () => {
  const key = 'AbCdEf~gh-ijKlMnOp~qr-stUvWx';
  const out = redactTarget(
    `https://portal.example.invalid/ethereum-mainnet/${key}`,
  );
  assert.equal(out.includes(key), false, 'the ~/-bearing key must be gone');
  assert.equal(out.includes('~'), false, 'no ~ from the key leaks through');
  assert.ok(
    out.includes('/ethereum-mainnet/'),
    'the slug before the key is preserved',
  );
});

// ── redactTarget: a keyless public URL is left untouched ────────────────────────────────────────

test('redactTarget: a bare public RPC URL (no credential tail) is unchanged', () => {
  const url = 'https://ethereum-rpc.publicnode.com';
  assert.equal(redactTarget(url), url, 'a bare host URL survives verbatim');
});

test('redactTarget: a public URL with a short/structured last segment is unchanged', () => {
  const url = 'https://cloudflare-eth.com/v1/mainnet';
  assert.equal(
    redactTarget(url),
    url,
    'a short (<20) last segment is not credential-like → untouched',
  );
});

test('redactTarget: a public URL whose last segment is NOT credential-like keeps its path', () => {
  // "rpc" is short and not credential-like; the whole path must survive.
  const url = 'https://api.example.invalid/eth/rpc';
  assert.equal(redactTarget(url), url);
});

test('redactTarget: a trailing slash (empty last segment) is not treated as a credential', () => {
  const url = 'https://rpc.example.invalid/eth/';
  assert.equal(redactTarget(url), url, 'empty tail is not credential-like');
});

test('redactTarget: an unparseable target is fully replaced (never echoed opaque)', () => {
  assert.equal(redactTarget('not a url'), '<redacted>');
});

// ── integration: spawn the real meter with a fake key; the key must not leak to stdout or the file ─

// Start a throwaway upstream so the meter has something to forward to; we do not exercise proxying
// here, only that the meter comes up, prints its banner, and persists state — the two leak sites.
function startUpstream() {
  return new Promise((resolve) => {
    const srv = createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{}');
    });
    srv.listen(0, '127.0.0.1', () => {
      resolve({ srv, port: srv.address().port });
    });
  });
}

function waitForLine(child, matcher, timeoutMs) {
  return new Promise((resolve, reject) => {
    let stdout = '';
    const timer = setTimeout(() => {
      reject(new Error(`timed out; stdout so far: ${JSON.stringify(stdout)}`));
    }, timeoutMs);
    child.stdout.on('data', (c) => {
      stdout += c.toString('utf8');
      if (matcher.test(stdout)) {
        clearTimeout(timer);
        resolve(stdout);
      }
    });
    child.on('error', (e) => {
      clearTimeout(timer);
      reject(e);
    });
  });
}

// Grab a free ephemeral port and return it after closing the probe listener — the meter echoes the
// REQUESTED port verbatim, so we must pass it a concrete number (not 0) to drive its control
// endpoints afterward.
function freePort() {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.listen(0, '127.0.0.1', () => {
      const { port } = probe.address();
      probe.close(() => resolve(port));
    });
    probe.on('error', reject);
  });
}

test('meter startup banner and persisted state redact the credential (both leak sites)', async () => {
  const upstream = await startUpstream();
  const dir = mkdtempSync(join(tmpdir(), 'rpc-meter-'));
  const stateFile = join(dir, 'meter-state.json');
  const target = `http://127.0.0.1:${upstream.port}/eth/${FAKE_KEY}`;
  const meterPort = await freePort();

  const child = spawn(process.execPath, [METER], {
    env: {
      ...process.env,
      METER_TARGET: target,
      METER_PORT: String(meterPort),
      METER_FILE: stateFile,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  try {
    const banner = await waitForLine(child, /rpc-meter →/, 10_000);

    // LEAK SITE 1 — the startup echo. The fake key must be gone; the host must remain for provenance.
    assert.equal(
      banner.includes(FAKE_KEY),
      false,
      'startup banner must NOT contain the credential',
    );
    assert.ok(
      banner.includes('127.0.0.1'),
      'startup banner keeps the host for provenance',
    );
    assert.ok(
      banner.includes('/eth/<redacted>'),
      'startup banner shows the redacted target with slug preserved',
    );

    // LEAK SITE 2 — the persisted state file. Hit the reset endpoint to force a synchronous flush,
    // then read the file back. The `target` field must carry the redacted value, never the key.
    await fetch(`http://127.0.0.1:${meterPort}/__reset`, { method: 'POST' });

    const persisted = readFileSync(stateFile, 'utf8');
    assert.equal(
      persisted.includes(FAKE_KEY),
      false,
      'persisted state file must NOT contain the credential',
    );

    const doc = JSON.parse(persisted);
    assert.equal(
      doc.target,
      redactTarget(target),
      'the persisted target field is the redacted value',
    );
    assert.equal(doc.target.includes(FAKE_KEY), false);
    assert.ok(doc.target.includes('/eth/<redacted>'));
  } finally {
    child.kill('SIGKILL');
    upstream.srv.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ── integration: space in path — main() must still run (pathToFileURL guard) ─────────────────────
//
// The true failure mode of the old `file://${process.argv[1]}` guard is percent-encoding:
// import.meta.url is always a properly-encoded URL (spaces become %20, etc.), but the naive string
// concat is NOT encoded — so any path containing a space produces a mismatch and main() silently
// never runs.  Node.js resolves argv[1] to an absolute path regardless of how the script is invoked
// (relative or absolute), so a relative invocation is NOT a failure mode.
// pathToFileURL(process.argv[1]) encodes identically to how Node encodes import.meta.url, so the
// comparison holds for any path including those with spaces (or other percent-encodable characters).
test('meter starts correctly when the script path contains a space (percent-encoding guard)', async () => {
  const upstream = await startUpstream();
  const meterPort = await freePort();
  const target = `http://127.0.0.1:${upstream.port}/eth/${FAKE_KEY}`;

  // Create a temp dir whose name contains a space — this forces percent-encoding in the URL form.
  // The old string-concat guard would produce `file:///tmp/meter test-XXXXX/rpc-meter.mjs`
  // (unencoded space) which never equals import.meta.url `…/meter%20test-XXXXX/…`, so main()
  // silently never ran.  pathToFileURL encodes identically, so the comparison succeeds.
  const spaceDir = mkdtempSync(join(tmpdir(), 'meter test-'));
  const meterCopy = join(spaceDir, 'rpc-meter.mjs');
  copyFileSync(METER, meterCopy);

  const child = spawn(process.execPath, [meterCopy], {
    env: {
      ...process.env,
      METER_TARGET: target,
      METER_PORT: String(meterPort),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  try {
    const banner = await waitForLine(child, /rpc-meter →/, 10_000);

    assert.ok(
      /rpc-meter →/.test(banner),
      'startup banner must appear — main() must run even when the path contains a space',
    );
    assert.equal(
      banner.includes(FAKE_KEY),
      false,
      'startup banner must NOT contain the credential',
    );
  } finally {
    child.kill('SIGKILL');
    upstream.srv.close();
    rmSync(spaceDir, { recursive: true, force: true });
  }
});
