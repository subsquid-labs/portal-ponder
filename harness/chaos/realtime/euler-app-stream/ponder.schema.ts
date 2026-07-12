import { onchainTable } from '@subsquid/ponder';

export const vault = onchainTable('vault', (t) => ({
  id: t.hex().primaryKey(),
  asset: t.hex(),
  name: t.text(),
  symbol: t.text(),
  decimals: t.integer(),
  oracle: t.hex(),
  creator: t.hex(),
  evc: t.hex(),
  createdBlock: t.bigint().notNull(),
}));

export const deposit = onchainTable('deposit', (t) => ({
  id: t.text().primaryKey(),
  vault: t.hex().notNull(),
  blockNumber: t.bigint().notNull(),
  logIndex: t.integer().notNull(),
  txHash: t.hex().notNull(),
  sender: t.hex().notNull(),
  owner: t.hex().notNull(),
  assets: t.bigint().notNull(),
  shares: t.bigint().notNull(),
}));

export const counter = onchainTable('counter', (t) => ({
  id: t.text().primaryKey(),
  value: t.bigint().notNull(),
}));
