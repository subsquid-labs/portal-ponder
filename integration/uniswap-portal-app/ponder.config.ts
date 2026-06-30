import { parseAbiItem } from "abitype";
import { createConfig } from "ponder";

// Uniswap V3 USDC/WETH 0.05% pool — guaranteed dense Swap volume (exercises RECEIPTS).
const v3PoolAbi = [
  parseAbiItem("event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"),
] as const;

// Uniswap V2 Router02 — exercises TRACES via includeCallTraces (geth callTracer).
const v2RouterAbi = [
  parseAbiItem("function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[] amounts)"),
  parseAbiItem("function swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[] amounts)"),
  parseAbiItem("function swapExactTokensForETH(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[] amounts)"),
] as const;

const START = Number(process.env.PONDER_START ?? 22_200_000);
const END = Number(process.env.PONDER_END ?? 22_210_000);

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,
      portal: process.env.PORTAL_URL ?? "https://sqd.portal.sqd.dev/datasets/ethereum-mainnet",
    },
  },
  contracts: {
    UsdcWethPool: {
      chain: "mainnet",
      abi: v3PoolAbi,
      address: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
      includeTransactionReceipts: true, // ← receipts
      startBlock: START,
      endBlock: END,
    },
    UniswapV2Router02: {
      chain: "mainnet",
      abi: v2RouterAbi,
      address: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
      includeCallTraces: true, // ← traces
      startBlock: START,
      endBlock: END,
    },
  },
});
