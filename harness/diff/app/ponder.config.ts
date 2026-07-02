import { createConfig } from "@subsquid/ponder";
import { parseAbiItem } from "abitype";

// Differential app: indexes logs + receipts (V3 USDC/WETH pool) and traces (V2 Router) over a
// bounded range. Run it twice — once with PORTAL_URL_1 set (Portal path) and once without (the
// stock RPC path, since @subsquid/ponder falls through to createHistoricalSync when no portal:).
// Separate PGLITE_DIR per run → separate ponder_sync caches → harness/diff/diff.mjs compares them.
const v3Pool = [
  parseAbiItem(
    "event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)",
  ),
] as const;
const v2Router = [
  parseAbiItem(
    "function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[])",
  ),
  parseAbiItem(
    "function swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[])",
  ),
  parseAbiItem(
    "function swapExactTokensForETH(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[])",
  ),
] as const;

export default createConfig({
  database: {
    kind: "pglite",
    directory: process.env.PGLITE_DIR ?? "./.ponder/pglite",
  },
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,
      portal: process.env.PORTAL_URL_1 || undefined,
    },
  },
  contracts: {
    Pool: {
      chain: "mainnet",
      abi: v3Pool,
      address: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
      includeTransactionReceipts: true,
      startBlock: Number(process.env.PONDER_START),
      endBlock: Number(process.env.PONDER_END),
    },
    Router: {
      chain: "mainnet",
      abi: v2Router,
      address: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
      includeCallTraces: true,
      startBlock: Number(process.env.PONDER_START),
      endBlock: Number(process.env.PONDER_END),
    },
  },
});
