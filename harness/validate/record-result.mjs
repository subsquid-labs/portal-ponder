// record-result.mjs — merge one window result into results/<cellId>.json (create-or-update, keyed by
// window tag). Called by run-cell.sh / ctrl-cell.sh so no bash JSON assembly is needed (the diff
// tail can contain quotes/newlines — it is read from a file, not an argv).
//
//   node record-result.mjs <cellId> <tag> <from> <to> <pass:0|1> <requests> \
//        <durationSec> <matchedLogs> <autoShrunk:0|1> <diffTailFile>
//
// A RERUN of the same (cellId, tag) keeps the latest attempt as the verdict-bearing window record,
// but the PRIOR attempt is NOT discarded — it moves into that window's `attempts` history so its
// spent `requests` still count toward the cumulative budget. Erasing a prior attempt would let a
// rerun hide real spend from the $-budget guard (budget-sum sums every window AND every attempt).
//
// mergeWindows is exported (pure) so the append-only accounting is unit-tested.

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const RESULTS = resolve(HERE, 'results');

// Merge `record` (a fresh attempt for its tag) into the existing `windows` list. The prior window
// for that tag (and its own attempts history) is folded into the new record's `attempts` array, so
// no spent-requests row is ever lost — the returned windows carry the full attempt history.
export function mergeWindows(windows, record) {
  const prior = (windows ?? []).filter(
    (w) => w.window?.tag === record.window.tag,
  );
  const kept = (windows ?? []).filter(
    (w) => w.window?.tag !== record.window.tag,
  );
  const history = [];
  for (const p of prior) {
    for (const a of p.attempts ?? []) {
      history.push(a);
    }

    // the prior verdict-bearing record itself becomes an attempt (strip its nested attempts)
    const { attempts: _drop, ...bare } = p;
    history.push(bare);
  }

  kept.push({ ...record, attempts: history });

  return kept;
}

function main() {
  const [
    cellId,
    tag,
    from,
    to,
    pass,
    requests,
    durationSec,
    matchedLogs,
    autoShrunk,
    diffTailFile,
  ] = process.argv.slice(2);

  if (!cellId || !tag) {
    console.error(
      'usage: record-result.mjs <cellId> <tag> <from> <to> <pass> <requests> ...',
    );
    process.exit(2);
  }

  let diffTail = '';
  try {
    diffTail = readFileSync(diffTailFile, 'utf8').slice(-4000);
  } catch {
    // no tail captured (e.g. an early failure) — leave empty
  }

  const file = resolve(RESULTS, `${cellId}.json`);
  let doc = { cellId, updatedAt: '', windows: [] };
  try {
    doc = JSON.parse(readFileSync(file, 'utf8'));
  } catch {
    // first window for this cell
  }

  const record = {
    window: { from: Number(from), to: Number(to), tag },
    pass: pass === '1',
    requests: Number(requests),
    durationSec: Number(durationSec),
    matchedLogs: Number.isNaN(Number(matchedLogs)) ? null : Number(matchedLogs),
    autoShrunk: autoShrunk === '1',
    diffTail,
  };

  doc.cellId = cellId;
  doc.updatedAt = new Date().toISOString();
  // The head frontier/full-range windows resolved against — recorded for reproducibility (#18) so a
  // results file names the exact chain tip its windows were cut from.
  if (process.env.RESOLVED_HEAD) {
    doc.resolvedHead = Number(process.env.RESOLVED_HEAD);
  }

  doc.windows = mergeWindows(doc.windows, record);

  mkdirSync(RESULTS, { recursive: true });
  writeFileSync(file, `${JSON.stringify(doc, null, 2)}\n`);
  console.log(
    `recorded ${cellId}/${tag}  pass=${record.pass}  requests=${record.requests}`,
  );
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
