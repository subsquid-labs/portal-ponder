import { onchainTable, relations } from "@subsquid/ponder";

/**
 * Ported from the official Euler V2 subgraph (euler-xyz/euler-subgraph).
 * subgraph entity → Ponder table:
 *   Vault (mutable, populated by eth_calls)  → `vault`   (here: readContract on first sight)
 *   Deposit/Withdraw/Borrow/Repay/Liquidate  → immutable per-event log tables
 *   VaultStatus (with derived supply/borrowApy) → `vaultStatus`
 *   Counter (per-action + a "global" singleton) → `counter`
 *   Account (EVC sub-account → owner, cached)  → `account`
 */

// the live per-vault state — in the subgraph this is filled by ~15 eth_calls in loadOrCreateEulerVault
export const vault = onchainTable("vault", (t) => ({
  id: t.hex().primaryKey(), // vault (EVault proxy) address
  asset: t.hex(),
  name: t.text(),
  symbol: t.text(),
  decimals: t.integer(),
  oracle: t.hex(),
  creator: t.hex(),
  evc: t.hex(),
  createdBlock: t.bigint().notNull(),
}));

const logCols = (t: any) => ({
  id: t.text().primaryKey(), // `${txHash}-${logIndex}`
  vault: t.hex().notNull(),
  blockNumber: t.bigint().notNull(),
  txHash: t.hex().notNull(),
});
export const deposit = onchainTable("deposit", (t) => ({
  ...logCols(t),
  sender: t.hex().notNull(),
  owner: t.hex().notNull(),
  assets: t.bigint().notNull(),
  shares: t.bigint().notNull(),
}));
export const withdraw = onchainTable("withdraw", (t) => ({
  ...logCols(t),
  sender: t.hex().notNull(),
  receiver: t.hex().notNull(),
  owner: t.hex().notNull(),
  assets: t.bigint().notNull(),
  shares: t.bigint().notNull(),
}));
export const borrow = onchainTable("borrow", (t) => ({
  ...logCols(t),
  account: t.hex().notNull(),
  assets: t.bigint().notNull(),
}));
export const repay = onchainTable("repay", (t) => ({
  ...logCols(t),
  account: t.hex().notNull(),
  assets: t.bigint().notNull(),
}));
export const liquidate = onchainTable("liquidate", (t) => ({
  ...logCols(t),
  liquidator: t.hex().notNull(),
  violator: t.hex().notNull(),
  collateral: t.hex().notNull(),
  repayAssets: t.bigint().notNull(),
  yieldBalance: t.bigint().notNull(),
}));

// VaultStatus carries the derived supply/borrow APY (computeAPYs, ported from src/utils/math.ts)
export const vaultStatus = onchainTable("vault_status", (t) => ({
  id: t.text().primaryKey(),
  vault: t.hex().notNull(),
  totalShares: t.bigint().notNull(),
  totalBorrows: t.bigint().notNull(),
  cash: t.bigint().notNull(),
  interestRate: t.bigint().notNull(),
  supplyApy: t.real().notNull(),
  borrowApy: t.real().notNull(),
  blockNumber: t.bigint().notNull(),
}));

// the subgraph's load-or-create Counter aggregation (per action + a "global" row)
export const counter = onchainTable("counter", (t) => ({
  id: t.text().primaryKey(), // "deposit" | "withdraw" | … | "global"
  value: t.bigint().notNull(),
}));

// EVC sub-account → owner; the subgraph caches this so getAccountOwner is read once per sub-account
export const account = onchainTable("account", (t) => ({
  id: t.hex().primaryKey(),
  owner: t.hex(),
}));

// one matched relation pair (vault ↔ deposits) — how the subgraph's @derivedFrom reverse
// lookups port to Ponder: define both sides explicitly.
export const vaultRelations = relations(vault, ({ many }) => ({
  deposits: many(deposit),
}));
export const depositRelations = relations(deposit, ({ one }) => ({
  vaultRef: one(vault, { fields: [deposit.vault], references: [vault.id] }),
}));
