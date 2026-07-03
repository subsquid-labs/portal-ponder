// record-result.mjs — merge one window result into results/<cellId>.json (create-or-update, keyed by
// window tag). Called by run-cell.sh / ctrl-cell.sh so no bash JSON assembly is needed (the diff
// tail can contain quotes/newlines — it is read from a file, not an argv).
//
//   node record-result.mjs <cellId> <tag> <from> <to> <pass:0|1> <requests> \
//        <durationSec> <matchedLogs> <autoShrunk:0|1> <diffTailFile>

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const RESULTS = resolve(HERE, 'results');

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
doc.windows = (doc.windows ?? []).filter((w) => w.window?.tag !== tag);
doc.windows.push(record);

mkdirSync(RESULTS, { recursive: true });
writeFileSync(file, `${JSON.stringify(doc, null, 2)}\n`);
console.log(
  `recorded ${cellId}/${tag}  pass=${record.pass}  requests=${record.requests}`,
);
