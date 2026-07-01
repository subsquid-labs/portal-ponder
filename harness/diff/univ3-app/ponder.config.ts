import { parseAbiItem } from "abitype";
import { createConfig, factory } from "@subsquid/ponder";

// Uniswap V3: UniswapV3Factory (PoolCreated → child pools) + the pools' Swap/Mint/Burn + receipts.
// A different, very high-volume factory shape than Euler. No-op handlers → measures pure backfill.
const poolCreated = parseAbiItem("event PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)");
const poolAbi = [
  parseAbiItem("event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"),
  parseAbiItem("event Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)"),
  parseAbiItem("event Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)"),
] as const;

export default createConfig({
  database: { kind: "pglite", directory: process.env.PGLITE_DIR ?? "./.ponder/pglite" },
  chains: { mainnet: { id: 1, rpc: process.env.PONDER_RPC_URL_1, portal: process.env.PORTAL_URL_1 || undefined } },
  contracts: {
    Pool: {
      abi: poolAbi, chain: "mainnet",
      address: factory({ address: "0x1F98431c8aD98523631AE4a59f267346ea31F984", event: poolCreated, parameter: "pool" }),
      includeTransactionReceipts: true,
      startBlock: Number(process.env.PONDER_START ?? 12_369_621),
      endBlock: process.env.PONDER_END ? Number(process.env.PONDER_END) : undefined,
    },
  },
});
