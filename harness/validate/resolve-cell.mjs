// resolve-cell.mjs — read cells.json + expand a cell's windows (via windows.mjs) into a form
// run-cell.sh / ctrl-cell.sh can consume. Keeping this in Node (not bash) means the window logic is
// the same unit-tested code the campaign relies on.
//
//   node resolve-cell.mjs <cellId> [--head N] [--sh]
//
// Default output is the pretty JSON plan (config + resolved windows). `--sh` prints bash-sourceable
// `CELL_*` assignments; frontier / full-range windows resolve only when --head is supplied, else the
// plan reports CELL_NEEDS_HEAD=1 so the caller fetches the Portal head and re-resolves.

import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { resolveWindows } from './windows.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, '../..');

function loadCells() {
  return JSON.parse(readFileSync(resolve(HERE, 'cells.json'), 'utf8'));
}

function shQuote(v) {
  return `'${String(v).replaceAll("'", `'\\''`)}'`;
}

export function buildPlan(cellId, head) {
  const doc = loadCells();
  const cell = doc.cells.find((c) => c.id === cellId);
  if (!cell) {
    throw new Error(`no cell '${cellId}' in cells.json`);
  }

  const chain = cell.chain ? doc.chains[cell.chain] : undefined;
  const appPath = cell.app ? resolve(ROOT, doc.apps[cell.app]) : '';
  const portalUrl = chain
    ? `${doc.defaults.portalBase}/${chain.portalDataset}`
    : '';
  const opts = head === undefined ? {} : { head };
  const resolved = resolveWindows(cell, opts);
  const windows = resolved.filter(
    (w) => w.from !== undefined && w.to !== undefined,
  );
  const needsHead = resolved.some((w) => w.frontier || w.fullRange);

  return {
    id: cell.id,
    runner: cell.runner ?? 'run',
    appName: cell.app ?? '',
    appPath,
    chainId: cell.chainId ?? chain?.chainId ?? '',
    portalUrl,
    rpcBase: doc.defaults.rpcBase,
    rpcSlug: chain?.rpcSlug ?? '',
    eulerFactory: cell.app === 'euler' ? (chain?.eulerFactory ?? '') : '',
    erc20: cell.app === 'erc20' ? (chain?.erc20 ?? '') : '',
    // Pinned per-chain head from cells.json — run-cell.sh prefers this for frontier/full-range
    // windows (reproducibility) over a live /finalized-head fetch, unless explicitly overridden.
    pinnedHead: chain?.head ?? '',
    receipts: cell.receipts !== false,
    differ: cell.differ ?? '',
    appHash: cell.appHash === true,
    shrink: cell.autoShrink?.threshold ?? '',
    needsHead,
    windows,
    env: doc.defaults.env,
    // Per-cell env overrides (e.g. per-chain POOL_ADDRESS/ROUTER_ADDRESS for the traces app on a
    // non-eth chain). run-cell.sh exports these before launching the backfill. A cell that names a
    // non-default chain for an address-defaulted app but ships no addresses is REQUIRED to carry a
    // `requires` marker so run-cell.sh refuses to run it (rather than silently using eth defaults).
    cellEnv: cell.env ?? {},
    requires: cell.requires ?? '',
  };
}

export function emitSh(plan) {
  const line = (k, v) => `${k}=${shQuote(v)}`;
  const out = [
    line('CELL_ID', plan.id),
    line('CELL_RUNNER', plan.runner),
    line('CELL_APP_NAME', plan.appName),
    line('CELL_APP_PATH', plan.appPath),
    line('CELL_CHAIN_ID', plan.chainId),
    line('CELL_PORTAL_URL', plan.portalUrl),
    line('CELL_RPC_BASE', plan.rpcBase),
    line('CELL_RPC_SLUG', plan.rpcSlug),
    line('CELL_EULER_FACTORY', plan.eulerFactory),
    line('CELL_ERC20', plan.erc20),
    line('CELL_PINNED_HEAD', plan.pinnedHead),
    line('CELL_RECEIPTS', plan.receipts ? 'true' : 'false'),
    line('CELL_DIFFER', plan.differ),
    line('CELL_APP_HASH', plan.appHash ? '1' : ''),
    line('CELL_SHRINK', plan.shrink),
    line('CELL_NEEDS_HEAD', plan.needsHead ? '1' : ''),
    line('CELL_REQUIRES', plan.requires),
    line(
      'CELL_WINDOWS',
      plan.windows.map((w) => `${w.from}|${w.to}|${w.tag}`).join(' '),
    ),
  ];

  // Per-cell env overrides as sourceable `export KEY=value` lines (e.g. POOL_ADDRESS/ROUTER_ADDRESS
  // for a non-eth traces cell). Values are shell-quoted; keys are validated to be safe env names so a
  // malformed cells.json entry can never inject shell.
  const envLines = [];
  for (const [k, v] of Object.entries(plan.cellEnv)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(k)) {
      throw new Error(`invalid env key in cell ${plan.id}: ${k}`);
    }

    envLines.push(`export ${k}=${shQuote(v)}`);
  }
  out.push(line('CELL_ENV_EXPORTS', envLines.join('; ')));

  return out.join('\n');
}

function main() {
  const args = process.argv.slice(2);
  const cellId = args.find((a) => !a.startsWith('--'));
  if (!cellId) {
    console.error('usage: resolve-cell.mjs <cellId> [--head N] [--sh]');
    process.exit(2);
  }

  const headIdx = args.indexOf('--head');
  const head = headIdx >= 0 ? Number(args[headIdx + 1]) : undefined;
  const plan = buildPlan(cellId, head);

  if (args.includes('--sh')) {
    console.log(emitSh(plan));

    return;
  }

  console.log(JSON.stringify(plan, null, 2));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
