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

test('validRequests: only finite, non-negative numbers are trusted', () => {
  assert.equal(validRequests(0), true);
  assert.equal(validRequests(1234), true);
  assert.equal(validRequests('999'), true);
  assert.equal(validRequests(undefined), true); // missing → 0, legitimate
  assert.equal(validRequests(-1), false);
  assert.equal(validRequests(Number.NaN), false);
  assert.equal(validRequests(Number.POSITIVE_INFINITY), false);
  assert.equal(validRequests('not-a-number'), false);
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
