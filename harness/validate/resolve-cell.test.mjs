import assert from 'node:assert/strict';
import { test } from 'node:test';
import { buildPlan, emitSh } from './resolve-cell.mjs';

// #7 — the traces app defaults to Ethereum-mainnet Pool/Router addresses, so a non-eth traces cell
// (A-base) that ships no explicit addresses MUST fail loud rather than silently backfill wrong
// addresses. The plumbing: cells.json carries `requires`/`env`, resolve-cell.mjs exports them
// (CELL_REQUIRES / CELL_ENV_EXPORTS), run-cell.sh refuses the cell / exports the addresses.

test('buildPlan: A-base carries requires="explicit addresses" (a non-eth traces cell needs addresses)', () => {
  const plan = buildPlan('A-base');
  assert.equal(plan.chainId, 8453, 'A-base is a base-chain cell');
  assert.equal(
    plan.requires,
    'explicit addresses',
    'A-base must be marked as requiring explicit per-chain addresses',
  );
});

test('buildPlan: A-eth (the address-default chain) needs no requires marker', () => {
  const plan = buildPlan('A-eth');
  assert.equal(plan.chainId, 1);
  assert.equal(
    plan.requires,
    '',
    'the default-chain cell runs with the eth defaults',
  );
});

test('emitSh: emits CELL_REQUIRES so run-cell.sh can refuse an address-less non-eth cell', () => {
  const sh = emitSh(buildPlan('A-base'));
  // MUTATION: drop the `line('CELL_REQUIRES', plan.requires)` from emitSh → CELL_REQUIRES is never
  // emitted, run-cell.sh's requires-guard never fires, and A-base runs with eth defaults. This test
  // fails without that line.
  assert.match(
    sh,
    /CELL_REQUIRES='explicit addresses'/,
    'CELL_REQUIRES must be emitted for the requires-guard in run-cell.sh',
  );
});

test('emitSh: per-cell env overrides become sourceable export lines (per-chain addresses)', () => {
  // synthesize a plan carrying an env map (as a wired A-base would once addresses are verified)
  const plan = {
    id: 'A-base',
    runner: 'run',
    appName: 'traces',
    appPath: '/x',
    chainId: 8453,
    portalUrl: 'p',
    rpcBase: 'r',
    rpcSlug: 's',
    eulerFactory: '',
    erc20: '',
    pinnedHead: '',
    receipts: true,
    differ: '',
    appHash: false,
    shrink: '',
    needsHead: false,
    requires: '',
    windows: [],
    cellEnv: {
      POOL_ADDRESS: '0xPOOLbase',
      ROUTER_ADDRESS: '0xROUTERbase',
    },
  };
  const sh = emitSh(plan);
  assert.match(sh, /CELL_ENV_EXPORTS='export POOL_ADDRESS='/);
  assert.match(sh, /export ROUTER_ADDRESS=/);
  assert.match(sh, /0xPOOLbase/);
  assert.match(sh, /0xROUTERbase/);
});

test('emitSh: a malformed env key in a cell is rejected (no shell injection)', () => {
  const plan = {
    id: 'BAD',
    runner: 'run',
    appName: 'traces',
    appPath: '/x',
    chainId: 1,
    portalUrl: '',
    rpcBase: '',
    rpcSlug: '',
    eulerFactory: '',
    erc20: '',
    pinnedHead: '',
    receipts: true,
    differ: '',
    appHash: false,
    shrink: '',
    needsHead: false,
    requires: '',
    windows: [],
    cellEnv: { 'PROBE; rm -rf /': 'x' },
  };
  assert.throws(() => emitSh(plan), /invalid env key/);
});
