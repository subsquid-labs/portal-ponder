// budget-sum.mjs — cumulative campaign request accounting for the run-cell.sh budget guard.
// Sums window.requests across every results/*.json and compares to budget.json's maxRequests.
//
//   node budget-sum.mjs            → prints the cumulative total (one integer)
//   node budget-sum.mjs --check    → exits 3 if the total already meets/exceeds the budget
//
// The guard is intentionally a floor check on *already-spent* requests: run-cell.sh refuses to start
// a new window once the campaign has met the ceiling, so a run can never blow past the budget by
// more than the in-flight window.

import { readdirSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const RESULTS = resolve(HERE, 'results');

export function sumRequests(dir) {
  let total = 0;
  let files = [];
  try {
    files = readdirSync(dir).filter((f) => f.endsWith('.json'));
  } catch {
    return 0;
  }

  for (const f of files) {
    try {
      const doc = JSON.parse(readFileSync(resolve(dir, f), 'utf8'));
      for (const w of doc.windows ?? []) {
        total += Number(w.requests ?? 0);
      }
    } catch {
      // a partially written result file must not crash the guard
    }
  }

  return total;
}

function budget() {
  const doc = JSON.parse(readFileSync(resolve(HERE, 'budget.json'), 'utf8'));

  return Number(doc.maxRequests);
}

const total = sumRequests(RESULTS);

if (process.argv.includes('--check')) {
  const max = budget();
  if (total >= max) {
    console.error(`BUDGET EXCEEDED: cumulative ${total} ≥ maxRequests ${max}`);
    process.exit(3);
  }

  console.log(`${total}/${max}`);
} else {
  console.log(String(total));
}
