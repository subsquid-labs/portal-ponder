import { ponder } from "ponder:registry";
import { borrow, counter, deposit, liquidate, repay, vault, vaultStatus, withdraw } from "ponder:schema";
import { EVaultAbi } from "../abis/EVault";

const logId = (event: any) => `${event.transaction.hash}-${event.log.logIndex}`;
const base = (event: any) => ({ id: logId(event), vault: event.log.address, blockNumber: event.block.number, txHash: event.transaction.hash });

// computeAPYs — ported from the subgraph's src/utils/math.ts: per-second RAY interest rate → APY.
const SECONDS_PER_YEAR = 365.2425 * 86_400;
function computeAPYs(interestRate: bigint, cash: bigint, totalBorrows: bigint, interestFeeBps = 0) {
  const ratePerSecond = Number(interestRate) / 1e27; // RAY-scaled
  const borrowApy = (1 + ratePerSecond) ** SECONDS_PER_YEAR - 1;
  const denom = Number(totalBorrows + cash);
  const util = denom > 0 ? Number(totalBorrows) / denom : 0;
  const supplyApy = borrowApy * util * (1 - interestFeeBps / 1e4);
  return { borrowApy: Number.isFinite(borrowApy) ? borrowApy : 0, supplyApy: Number.isFinite(supplyApy) ? supplyApy : 0 };
}

// loadOrCreateEulerVault — the subgraph's ~15-call eth_call fan-out, gated by an EXISTENCE
// CHECK so it runs once per vault (the subgraph caches the same way; otherwise readContract
// hammers RPC). Portal serves the logs cheaply; these state reads still hit `rpc`.
async function ensureVault(event: any, context: any) {
  const id = event.log.address as `0x${string}`;
  if (await context.db.find(vault, { id })) return;
  const read = (functionName: string) =>
    context.client.readContract({ abi: EVaultAbi, address: id, functionName }).catch(() => null);
  const [asset, name, symbol, decimals, oracle, creator, evc] = await Promise.all([
    read("asset"), read("name"), read("symbol"), read("decimals"), read("oracle"), read("creator"), read("EVC"),
  ]);
  await context.db.insert(vault).values({
    id, asset, name, symbol, decimals: decimals == null ? null : Number(decimals),
    oracle, creator, evc, createdBlock: event.block.number,
  }).onConflictDoNothing();
}

// the subgraph's Counter: bump the per-action row AND the "global" singleton.
async function bump(context: any, type: string) {
  for (const id of [type, "global"]) {
    await context.db.insert(counter).values({ id, value: 1n }).onConflictDoUpdate((r: any) => ({ value: r.value + 1n }));
  }
}

ponder.on("EVault:Deposit", async ({ event, context }) => {
  await ensureVault(event, context);
  await context.db.insert(deposit).values({ ...base(event), sender: event.args.sender, owner: event.args.owner, assets: event.args.assets, shares: event.args.shares }).onConflictDoNothing();
  await bump(context, "deposit");
});
ponder.on("EVault:Withdraw", async ({ event, context }) => {
  await ensureVault(event, context);
  await context.db.insert(withdraw).values({ ...base(event), sender: event.args.sender, receiver: event.args.receiver, owner: event.args.owner, assets: event.args.assets, shares: event.args.shares }).onConflictDoNothing();
  await bump(context, "withdraw");
});
ponder.on("EVault:Borrow", async ({ event, context }) => {
  await ensureVault(event, context);
  await context.db.insert(borrow).values({ ...base(event), account: event.args.account, assets: event.args.assets }).onConflictDoNothing();
  await bump(context, "borrow");
});
ponder.on("EVault:Repay", async ({ event, context }) => {
  await ensureVault(event, context);
  await context.db.insert(repay).values({ ...base(event), account: event.args.account, assets: event.args.assets }).onConflictDoNothing();
  await bump(context, "repay");
});
ponder.on("EVault:Liquidate", async ({ event, context }) => {
  await ensureVault(event, context);
  await context.db.insert(liquidate).values({ ...base(event), liquidator: event.args.liquidator, violator: event.args.violator, collateral: event.args.collateral, repayAssets: event.args.repayAssets, yieldBalance: event.args.yieldBalance }).onConflictDoNothing();
  await bump(context, "liquidate");
});
ponder.on("EVault:VaultStatus", async ({ event, context }) => {
  const { borrowApy, supplyApy } = computeAPYs(event.args.interestRate, event.args.cash, event.args.totalBorrows);
  await context.db.insert(vaultStatus).values({
    ...base(event), totalShares: event.args.totalShares, totalBorrows: event.args.totalBorrows,
    cash: event.args.cash, interestRate: event.args.interestRate, supplyApy, borrowApy,
  }).onConflictDoNothing();
});

// NOTE: the full subgraph also tracks per-account balances (EVC.getAccountOwner, cached in the
// `account` table) + EulerEarn/EulerSwap/perspectives — omitted here for a focused core port.
