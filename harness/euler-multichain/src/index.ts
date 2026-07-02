import { ponder } from "ponder:registry";

// SUPERSET: index every EVault event (Euler runs both ponder + subgraphs; the subgraph is only a
// subset). No-op handlers — the backfill populates the ponder_sync store regardless.
const EVENTS = [
  "Approval",
  "BalanceForwarderStatus",
  "Borrow",
  "ConvertFees",
  "DebtSocialized",
  "Deposit",
  "EVaultCreated",
  "GovSetCaps",
  "GovSetConfigFlags",
  "GovSetFeeReceiver",
  "GovSetGovernorAdmin",
  "GovSetHookConfig",
  "GovSetInterestFee",
  "GovSetInterestRateModel",
  "GovSetLTV",
  "GovSetLiquidationCoolOffTime",
  "GovSetMaxLiquidationDiscount",
  "InterestAccrued",
  "Liquidate",
  "PullDebt",
  "Repay",
  "Transfer",
  "VaultStatus",
  "Withdraw",
];
for (const ev of EVENTS) ponder.on(`EVault:${ev}` as any, async () => {});
