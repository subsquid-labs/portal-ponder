import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

// End-to-end guard for the issue-#78 diff-phase timeout in harness/diff/run.sh. We drive the REAL
// run.sh (not a copy) with the backfill stubbed — a fake `npm` that no-ops the install and a fake
// `ponder` that writes the completion line and creates the pglite dir — then point DIFF_SCRIPT at a
// differ that never finishes. With DIFF_TIMEOUT=2 the diff phase must be bounded: run.sh prints a
// clear "diff timed out after 2s" line, exits non-zero, and the EXISTING rescue path preserves the
// backfilled stores. MUTATION (origin/main): run.sh has NO timeout, so the sleeping differ runs
// unbounded and run.sh never returns — spawnSync's own timeout kills it (status null / signal set)
// and every assertion below fails. This is the exact wedge #78 fixes.

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, '..', '..');
const RUN_SH = join(ROOT, 'harness', 'diff', 'run.sh');

function haveTimeout() {
  const res = spawnSync('timeout', ['--version'], { stdio: 'ignore' });

  return res.status === 0;
}

test('#78 run.sh bounds the diff phase with DIFF_TIMEOUT and preserves stores on timeout', (t) => {
  if (!haveTimeout()) {
    t.skip(
      'GNU coreutils `timeout` not available — cannot exercise the diff-phase bound',
    );

    return;
  }

  const work = mkdtempSync(join(tmpdir(), 'run78-'));
  const preservedDirs = [];
  try {
    // Fake app dir run.sh copies into its workspace. It needs a package.json (the `cp -r` target)
    // and a fake `ponder` binary at node_modules/.bin/ponder.
    const app = join(work, 'app');
    const binDir = join(app, 'node_modules', '.bin');
    mkdirSync(binDir, { recursive: true });
    writeFileSync(
      join(app, 'package.json'),
      JSON.stringify({ name: 'diff-fake', version: '0.0.0', dependencies: {} }),
    );

    // Fake ponder: create the pglite dir run() exported as PGLITE_DIR, emit the exact completion line
    // run.sh polls for, then idle briefly so the poll loop sees the line while the process is alive.
    const ponder = join(binDir, 'ponder');
    writeFileSync(
      ponder,
      [
        '#!/usr/bin/env bash',
        'mkdir -p "$PGLITE_DIR"',
        ': > "$PGLITE_DIR/marker"',
        'echo "Completed indexing across all chains (fake test window)"',
        'sleep 5',
        '',
      ].join('\n'),
    );
    chmodSync(ponder, 0o755);

    // Fake npm on PATH: no-op the install run.sh performs before the diff phase.
    const fakeBin = join(work, 'fakebin');
    mkdirSync(fakeBin, { recursive: true });
    const npm = join(fakeBin, 'npm');
    writeFileSync(npm, '#!/usr/bin/env bash\nexit 0\n');
    chmodSync(npm, 0o755);

    // A differ that never finishes within the test's horizon — stands in for the whole-table wedge on
    // a dense window. Its self-exit (120s) comfortably outlasts the 60s spawn cap below, so on the
    // FIXED tree the 2s DIFF_TIMEOUT always kills it first (rc 124), and on origin/main (no timeout)
    // run.sh is provably still hung when spawnSync kills it. The self-exit only matters as a backstop:
    // if the mutation run orphans this grandchild (spawnSync SIGTERMs bash, not the foreground node),
    // it self-reaps within ~60s instead of lingering.
    const sleeper = join(work, 'sleeper-diff.mjs');
    writeFileSync(sleeper, 'setTimeout(() => process.exit(0), 120000);\n');

    const env = {
      ...process.env,
      PATH: `${fakeBin}:${process.env.PATH}`,
      PONDER_RPC_URL_1: 'http://127.0.0.1:1', // satisfies run.sh's ${VAR:?} guard; never contacted
      DIFF_APP: app,
      DIFF_SCRIPT: sleeper,
      DIFF_TIMEOUT: '2',
    };
    delete env.KEEP_WORKSPACES; // let the rescue path move the preserved stores
    delete env.DIFF_ARGS;

    const res = spawnSync('bash', [RUN_SH, '100', '100'], {
      env,
      encoding: 'utf8',
      timeout: 60000, // if run.sh fails to bound the diff (origin/main), this kills the hung run
    });
    const out = `${res.stdout || ''}${res.stderr || ''}`;

    // run.sh must have RETURNED (the diff was bounded). A null status means spawnSync had to kill a
    // hung run.sh — exactly the origin/main behavior this test locks out.
    assert.notEqual(
      res.status,
      null,
      `run.sh must return (diff bounded), not hang until the harness kills it — signal=${res.signal}\n${out}`,
    );
    assert.notEqual(
      res.status,
      0,
      `a timed-out diff must exit non-zero\n${out}`,
    );

    // the timeout must be visible in the output …
    assert.match(
      out,
      /diff timed out after 2s/,
      `run.sh must announce the diff-phase timeout clearly\n${out}`,
    );

    // … and the EXISTING rescue path must fire on the non-zero rc and preserve the backfilled stores.
    assert.match(
      out,
      /backfilled stores PRESERVED at:/,
      `the store-preservation rescue must fire on the timeout rc\n${out}`,
    );

    const m = out.match(/backfilled stores PRESERVED at:\s*(\S+)/);
    assert.ok(m, 'the rescue must print the preserved-stores path');
    const preserved = m[1];
    preservedDirs.push(preserved);
    assert.ok(
      existsSync(join(preserved, 'dbPortal')) &&
        existsSync(join(preserved, 'dbRpc')),
      `both backfilled stores must survive the timeout at ${preserved}`,
    );
  } finally {
    rmSync(work, { recursive: true, force: true });
    for (const d of preservedDirs) {
      rmSync(d, { recursive: true, force: true });
    }
  }
});
