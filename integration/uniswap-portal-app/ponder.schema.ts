import { onchainTable } from "ponder";

// swap rows carry the receipt's gasUsed → proves RECEIPTS are synced
export const swap = onchainTable("swap", (t) => ({
  id: t.text().primaryKey(),
  pool: t.hex().notNull(),
  receiptGasUsed: t.bigint(),
  receiptStatus: t.integer(),
  blockNumber: t.bigint().notNull(),
}));

// router-call rows come from call traces → proves TRACES are synced
export const routerCall = onchainTable("router_call", (t) => ({
  id: t.text().primaryKey(),
  fn: t.text().notNull(),
  from: t.hex().notNull(),
  to: t.hex(),
  blockNumber: t.bigint().notNull(),
}));

// block-interval rows → proves BlockFilter sources are synced
export const blockTick = onchainTable("block_tick", (t) => ({
  number: t.bigint().primaryKey(),
  timestamp: t.bigint().notNull(),
}));
