import { ponder } from "ponder:registry";
import { routerCall, swap } from "ponder:schema";

// RECEIPTS: the swap handler reads event.transactionReceipt (only present because
// includeTransactionReceipts pulled + stored the receipt from Portal).
ponder.on("UsdcWethPool:Swap", async ({ event, context }) => {
  await context.db.insert(swap).values({
    id: event.id,
    pool: event.log.address,
    receiptGasUsed: event.transactionReceipt?.gasUsed ?? null,
    receiptStatus: event.transactionReceipt ? (event.transactionReceipt.status === "success" ? 1 : 0) : null,
    blockNumber: event.block.number,
  });
});

// TRACES: call-trace handlers fire from synced traces; event.trace is the call frame.
const onRouterCall = (fn: string) => async ({ event, context }: any) => {
  await context.db.insert(routerCall).values({
    id: event.id, fn, from: event.trace.from, to: event.trace.to ?? null, blockNumber: event.block.number,
  });
};
ponder.on("UniswapV2Router02.swapExactTokensForTokens()", onRouterCall("swapExactTokensForTokens"));
ponder.on("UniswapV2Router02.swapExactETHForTokens()", onRouterCall("swapExactETHForTokens"));
ponder.on("UniswapV2Router02.swapExactTokensForETH()", onRouterCall("swapExactTokensForETH"));
