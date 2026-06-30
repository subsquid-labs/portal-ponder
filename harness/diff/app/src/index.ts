import { ponder } from "ponder:registry";
// no-op handlers — we only diff the ponder_sync store (logs/transactions/receipts/traces),
// which the backfill populates regardless of what the handlers do.
ponder.on("Pool:Swap", async () => {});
ponder.on("Router.swapExactTokensForTokens()", async () => {});
ponder.on("Router.swapExactETHForTokens()", async () => {});
ponder.on("Router.swapExactTokensForETH()", async () => {});
