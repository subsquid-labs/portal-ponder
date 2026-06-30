import { ponder } from "ponder:registry";
// no-op: we only diff the ponder_sync store (factory child logs + txs + receipts), which the
// backfill populates regardless of the handlers. 6 event filters → exercises the C1 multi-source path.
for (const ev of ["Deposit", "Withdraw", "Borrow", "Repay", "Liquidate", "VaultStatus"]) {
  ponder.on(`EVault:${ev}` as any, async () => {});
}
