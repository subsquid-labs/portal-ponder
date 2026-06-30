import { onchainTable } from "@subsquid/ponder";

export const vault = onchainTable("vault", (t) => ({
  id: t.text().primaryKey(), // `${chain}:${address}`
  chain: t.text().notNull(),
  address: t.hex().notNull(),
  eventCount: t.integer().notNull(),
}));

export const vaultEvent = onchainTable("vault_event", (t) => ({
  id: t.text().primaryKey(),
  chain: t.text().notNull(),
  vault: t.hex().notNull(),
  type: t.text().notNull(),
  blockNumber: t.bigint().notNull(),
}));
