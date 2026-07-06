import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { sumDoc, sumRequests, validRequests } from './budget-sum.mjs';
import { mergeWindows } from './record-result.mjs';

const win = (tag, requests, extra = {}) => ({
  window: { from: 0, to: 10, tag },
  pass: true,
  requests,
  ...extra,
});

// ── #10 record-result: a rerun must NOT erase the prior attempt's spent requests ────────────────

test('mergeWindows: a rerun folds the prior attempt into `attempts` (spend is never erased)', () => {
  const first = mergeWindows([], win('w0', 1000));
  assert.equal(first.length, 1);
  assert.equal(first[0].requests, 1000);
  assert.deepEqual(first[0].attempts, []);

  // rerun the SAME tag with a fresh cost — old code did `.filter(tag !== w.tag)` then push, which
  // DROPPED the prior 1000-request row. Now it must survive in attempts.
  const rerun = mergeWindows(first, win('w0', 1500));
  assert.equal(rerun.length, 1, 'still one verdict-bearing window for the tag');
  assert.equal(
    rerun[0].requests,
    1500,
    'latest attempt is the verdict-bearing record',
  );
  assert.equal(rerun[0].attempts.length, 1, 'the prior attempt is retained');
  assert.equal(rerun[0].attempts[0].requests, 1000);

  // a THIRD attempt keeps BOTH prior attempts (no history is lost across reruns)
  const third = mergeWindows(rerun, win('w0', 2000));
  assert.equal(third[0].requests, 2000);
  assert.deepEqual(
    third[0].attempts.map((a) => a.requests).sort((x, y) => x - y),
    [1000, 1500],
  );

  // a different tag is a separate window, untouched
  const other = mergeWindows(third, win('w1', 42));
  assert.equal(other.length, 2);
});

// ── #79 record-result: fold a prior record ONLY when its range matches (else it is a different
// window that collided on tag, and its verdict must not be buried as an "attempt") ───────────────

// A window carrying an explicit range, so same-tag records can differ by (from, to).
const rangeWin = (tag, from, to, requests) => ({
  window: { from, to, tag },
  pass: true,
  requests,
});

test('mergeWindows: a same-tag DIFFERENT-range record is kept as its own window, not folded (#79)', () => {
  const warnings = [];
  const realWarn = console.warn;
  console.warn = (msg) => warnings.push(msg);
  try {
    const start = mergeWindows([], rangeWin('rand#0', 0, 2_000, 100));
    // a colliding tag over an UNRELATED range (different seed/spec) — must NOT demote the first
    // window's verdict into the second window's attempts.
    const merged = mergeWindows(start, rangeWin('rand#0', 500_000, 502_000, 200));

    assert.equal(merged.length, 2, 'both windows survive as top-level entries');
    const ranges = merged
      .map((w) => `${w.window.from}-${w.window.to}`)
      .sort();
    assert.deepEqual(ranges, ['0-2000', '500000-502000']);
    for (const w of merged) {
      assert.deepEqual(w.attempts ?? [], [], 'neither window was folded away');
    }
  } finally {
    console.warn = realWarn;
  }

  assert.equal(warnings.length, 1, 'the tag collision is warned loudly');
});

test('mergeWindows: a same-tag SAME-range record still folds (rerun + `+shrunk` semantics intact)', () => {
  // A genuine rerun of the same window (identical range) still folds — the prior spend is retained
  // in `attempts`, exactly the behavior budget-sum relies on. This is the un-mutated rerun path.
  const first = mergeWindows([], rangeWin('rand#0', 0, 2_000, 100));
  const rerun = mergeWindows(first, rangeWin('rand#0', 0, 2_000, 150));

  assert.equal(rerun.length, 1, 'a same-range rerun stays one window');
  assert.equal(rerun[0].requests, 150, 'latest attempt is the verdict-bearing record');
  assert.equal(rerun[0].attempts.length, 1);
  assert.equal(rerun[0].attempts[0].requests, 100, 'prior spend retained');

  // run-cell.sh tags an auto-shrunk re-run `<tag>+shrunk` over a HALVED range — a distinct tag, so it
  // is a separate top-level window (never folded into its parent), and it also reruns cleanly.
  const withShrunk = mergeWindows(rerun, rangeWin('rand#0+shrunk', 0, 1_000, 40));
  assert.equal(withShrunk.length, 2, 'the +shrunk window is its own entry');

  const shrunkRerun = mergeWindows(withShrunk, rangeWin('rand#0+shrunk', 0, 1_000, 55));
  const shrunk = shrunkRerun.find((w) => w.window.tag === 'rand#0+shrunk');
  assert.equal(shrunkRerun.length, 2, 'still two windows after the +shrunk rerun');
  assert.equal(shrunk.requests, 55, 'latest +shrunk attempt is the verdict');
  assert.equal(shrunk.attempts[0].requests, 40, 'prior +shrunk spend retained');
});

test('sumDoc: counts the verdict-bearing window AND every attempt', () => {
  const doc = {
    windows: [
      { ...win('w0', 2000), attempts: [win('w0', 1000), win('w0', 1500)] },
      win('w1', 500),
    ],
  };
  // 2000 (current) + 1000 + 1500 (attempts) + 500 = 5000
  assert.equal(sumDoc(doc), 5000);
});

// ── #10 budget-sum: validate finite/non-negative, FAIL CLOSED on corrupt/non-finite ─────────────

// #22 — the budget guard FAILS CLOSED on a MISSING `requests` field too: an old/corrupt/tampered
// record that dropped its spend field must NOT be counted as zero (that silently undercounts spend).
// MUTATION: revert validRequests to `Number.isFinite(Number(v ?? 0)) && n>=0` (treat missing as 0) →
// the `validRequests(undefined)`/`sumDoc(missing)` assertions below flip and this test fails.
test('validRequests: only PRESENT finite non-negative integers are trusted (missing fails closed)', () => {
  assert.equal(validRequests(0), true);
  assert.equal(validRequests(1234), true);
  assert.equal(validRequests('999'), true);
  // a MISSING requests field is now UNTRUSTED — it must not be silently counted as zero spend
  assert.equal(validRequests(undefined), false);
  assert.equal(validRequests(null), false);
  assert.equal(validRequests(''), false);
  assert.equal(validRequests(-1), false);
  assert.equal(validRequests(1.5), false, 'a request count is an integer');
  assert.equal(validRequests(Number.NaN), false);
  assert.equal(validRequests(Number.POSITIVE_INFINITY), false);
  assert.equal(validRequests('not-a-number'), false);
});

test('sumDoc: a window/attempt MISSING its requests field fails closed → null', () => {
  // a verdict-bearing window with no `requests` at all (a dropped/old record)
  assert.equal(
    sumDoc({ windows: [{ window: { tag: 'w0' }, pass: true }] }),
    null,
    'a window with no requests field must not be counted as zero',
  );
  // present on the window but MISSING on an attempt → still fails closed
  assert.equal(
    sumDoc({
      windows: [
        {
          ...win('w0', 100),
          attempts: [{ window: { tag: 'w0' }, pass: true }],
        },
      ],
    }),
    null,
  );
});

test('sumDoc: a non-finite/negative requests value poisons the doc → null (fail closed)', () => {
  assert.equal(sumDoc({ windows: [win('w0', Number.NaN)] }), null);
  assert.equal(sumDoc({ windows: [win('w0', -5)] }), null);
  // a poisoned ATTEMPT (not just the current window) also fails closed
  assert.equal(
    sumDoc({ windows: [{ ...win('w0', 100), attempts: [win('w0', 'x')] }] }),
    null,
  );
});

test('sumRequests: sums valid files but FAILS CLOSED on a corrupt or non-finite file', () => {
  const dir = mkdtempSync(join(tmpdir(), 'budget-'));

  // two clean cells
  writeFileSync(
    join(dir, 'A.json'),
    JSON.stringify({ windows: [win('a', 1000)] }),
  );
  writeFileSync(
    join(dir, 'B.json'),
    JSON.stringify({ windows: [win('b', 2000)] }),
  );
  const clean = sumRequests(dir);
  assert.equal(clean.error, null);
  assert.equal(clean.total, 3000);

  // a corrupt (unparseable) file must NOT be silently skipped — that would hide real spend
  writeFileSync(join(dir, 'C.json'), '{ this is not json');
  const corrupt = sumRequests(dir);
  assert.notEqual(
    corrupt.error,
    null,
    'a corrupt results file fails the guard closed',
  );

  // a non-finite request value likewise fails closed. JSON.parse('1e400') → Infinity, which the
  // old `Number(w.requests ?? 0)` would have ADDED (poisoning the total to Infinity, > any budget,
  // yet the code never asserted finiteness). Write the raw literal so parse yields Infinity.
  const dir2 = mkdtempSync(join(tmpdir(), 'budget-'));
  writeFileSync(
    join(dir2, 'D.json'),
    '{ "windows": [ { "window": { "tag": "d" }, "requests": 1e400 } ] }',
  );
  const bad = sumRequests(dir2);
  assert.notEqual(
    bad.error,
    null,
    'a non-finite request value fails the guard closed',
  );
});

test('sumRequests: a missing results dir is legitimately zero (nothing spent), not an error', () => {
  const missing = sumRequests(join(tmpdir(), 'does-not-exist-xyz-123'));
  assert.equal(missing.error, null);
  assert.equal(missing.total, 0);
});
