// budget-sum.mjs — cumulative campaign request accounting for the run-cell.sh budget guard.
// Sums requests across EVERY window AND every prior attempt in every results/*.json and compares to
// budget.json's maxRequests.
//
//   node budget-sum.mjs            → prints the cumulative total (one integer)
//   node budget-sum.mjs --check    → exits 3 if the total already meets/exceeds the budget
//
// The guard is intentionally a floor check on *already-spent* requests: run-cell.sh refuses to start
// a new window once the campaign has met the ceiling, so a run can never blow past the budget by
// more than the in-flight window.
//
// THIS IS THE $-BUDGET GUARD — it must FAIL CLOSED. An unreadable/corrupt results file, a MISSING
// `requests` field, or a non-integer / negative request value is treated as untrusted (exit 3),
// never silently skipped or counted as zero: fail-open here spends real money. Every window/attempt
// record must carry its own finite non-negative integer `requests` — a dropped field would
// UNDERCOUNT the cumulative spend. sumRequests returns { total, error } — a non-null error means the
// numbers cannot be trusted and --check must refuse to start a window.

import { readdirSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const RESULTS = resolve(HERE, 'results');

// A single window/attempt's requests must be a finite, non-negative INTEGER that is actually PRESENT.
// A MISSING `requests` field is no longer treated as 0 — an old/corrupt/tampered record that dropped
// its spend field would silently UNDERCOUNT the cumulative total (spending real money the guard never
// saw). So the guard now FAILS CLOSED on missing/invalid too: every metered record must vouch for its
// own spend. (undefined, null, NaN, Infinity, negative, fractional, non-numeric → invalid.)
export function validRequests(v) {
  if (v === undefined || v === null || v === '') {
    return false;
  }

  const n = Number(v);

  return Number.isInteger(n) && n >= 0;
}

// Sum requests over a parsed results doc's windows AND each window's `attempts` history. Every
// window/attempt MUST carry its own valid `requests`. Returns null on the FIRST missing/invalid value
// so the caller fails closed (a dropped field cannot be silently counted as zero spend).
export function sumDoc(doc) {
  let total = 0;
  for (const w of doc.windows ?? []) {
    const rows = [w, ...(w.attempts ?? [])];
    for (const row of rows) {
      if (!validRequests(row.requests)) {
        return null;
      }

      total += Number(row.requests);
    }
  }

  return total;
}

// Walk results/*.json. Any read/parse error, or any doc with a non-finite request value, sets
// `error` so the guard fails closed — a corrupt file must NOT be silently skipped (that would hide
// real spend).
export function sumRequests(dir) {
  let total = 0;
  let files = [];
  try {
    files = readdirSync(dir).filter((f) => f.endsWith('.json'));
  } catch {
    // no results dir yet (nothing spent) is legitimately zero, not an error
    return { total: 0, error: null };
  }

  for (const f of files) {
    let doc;
    try {
      doc = JSON.parse(readFileSync(resolve(dir, f), 'utf8'));
    } catch (e) {
      return { total, error: `unreadable results file ${f}: ${e.message}` };
    }

    const sub = sumDoc(doc);
    if (sub === null) {
      return { total, error: `non-finite/negative requests in ${f}` };
    }

    total += sub;
  }

  return { total, error: null };
}

function budget() {
  const doc = JSON.parse(readFileSync(resolve(HERE, 'budget.json'), 'utf8'));

  return Number(doc.maxRequests);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { total, error } = sumRequests(RESULTS);

  if (process.argv.includes('--check')) {
    if (error) {
      console.error(`BUDGET GUARD: fail-closed — ${error}`);
      process.exit(3);
    }

    const max = budget();
    if (total >= max) {
      console.error(
        `BUDGET EXCEEDED: cumulative ${total} ≥ maxRequests ${max}`,
      );
      process.exit(3);
    }

    console.log(`${total}/${max}`);
  } else if (error) {
    // a bare read must also surface the failure (non-zero) rather than print a wrong total
    console.error(`BUDGET GUARD: fail-closed — ${error}`);
    process.exit(3);
  } else {
    console.log(String(total));
  }
}
