#!/usr/bin/env node
// Render a Portal-layer coverage diff as a markdown table for a PR body.
//
//   node scripts/coverage-diff.mjs <head-summary.json>                     # absolute table
//   node scripts/coverage-diff.mjs <base-summary.json> <head-summary.json> # diff vs base
//
// Inputs are vitest v8 `coverage-summary.json` files (produced by
// `scripts/sync-upstream.sh <ver> --coverage`, at <core>/portal-coverage/coverage-summary.json).
// Per-file keys are absolute paths inside the grafted tree; we key rows by basename so base and
// head line up regardless of the temp workdir. With one argument (e.g. the base branch doesn't have
// the coverage tooling yet) it prints head absolutes with no deltas.
import { readFileSync } from 'node:fs';

const args = process.argv.slice(2);

if (args.length === 0 || args.length > 2) {
  console.error(
    'usage: node scripts/coverage-diff.mjs [<base-summary.json>] <head-summary.json>',
  );
  process.exit(2);
}

const load = (p) => JSON.parse(readFileSync(p, 'utf8'));
const headArg = args[args.length - 1];
const baseArg = args.length === 2 ? args[0] : null;
const head = load(headArg);
const base = baseArg ? load(baseArg) : { total: null };

const pct = (entry, metric) => entry?.[metric]?.pct ?? null;
const fmtPct = (v) => (v === null ? '—' : `${v.toFixed(1)}%`);
const fmtDelta = (b, h) => {
  if (b === null || h === null) {
    return '';
  }

  const d = h - b;

  if (Math.abs(d) < 0.1) {
    return '±0';
  }

  return `${d > 0 ? '+' : ''}${d.toFixed(1)}`;
};

// Row keys: 'All files' for total, basename for each file (union of base + head).
const fileKeys = new Map(); // basename -> { base, head }

const collect = (summary, side) => {
  for (const [k, entry] of Object.entries(summary)) {
    if (k === 'total') {
      continue;
    }

    const name = k.split('/').pop();
    const row = fileKeys.get(name) ?? {};
    row[side] = entry;
    fileKeys.set(name, row);
  }
};

collect(base, 'base');
collect(head, 'head');

const rows = [];
rows.push({
  label: '**All files**',
  base: base.total,
  head: head.total,
  changed: true,
});

for (const [name, { base: b, head: h }] of [...fileKeys].sort()) {
  const sB = pct(b, 'statements');
  const sH = pct(h, 'statements');
  const changed =
    sB === null || sH === null || Math.abs((sH ?? 0) - (sB ?? 0)) >= 0.1;
  rows.push({ label: name, base: b, head: h, changed });
}

const lines = [];
lines.push('## Coverage (Portal layer)');
lines.push('');
lines.push('| File | Stmts | Δ | Branch | Δ | Funcs | Δ |');
lines.push('|------|-------|---|--------|---|-------|---|');

for (const r of rows) {
  if (!r.changed) {
    continue;
  }

  const sB = pct(r.base, 'statements');
  const sH = pct(r.head, 'statements');
  const bB = pct(r.base, 'branches');
  const bH = pct(r.head, 'branches');
  const fB = pct(r.base, 'functions');
  const fH = pct(r.head, 'functions');
  lines.push(
    `| ${r.label} | ${fmtPct(sH)} | ${fmtDelta(sB, sH)} | ${fmtPct(bH)} | ${fmtDelta(bB, bH)} | ${fmtPct(fH)} | ${fmtDelta(fB, fH)} |`,
  );
}

if (base.total) {
  const totalDelta = head.total.statements.pct - base.total.statements.pct;

  if (totalDelta < -0.1) {
    lines.push('');
    lines.push('⚠️ Overall statement coverage decreased.');
  }
}

console.log(lines.join('\n'));
