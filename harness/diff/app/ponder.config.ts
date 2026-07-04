import { createConfig } from '@subsquid/ponder';
import { parseAbiItem } from 'abitype';

// Differential app: indexes logs + receipts (V3 USDC/WETH pool) and traces (V2 Router) over a
// bounded range. Run it twice — once with PORTAL_URL_1 set (Portal path) and once without (the
// stock RPC path, since @subsquid/ponder falls through to createHistoricalSync when no portal:).
// Separate PGLITE_DIR per run → separate ponder_sync caches → harness/diff/diff.mjs compares them.
const v3Pool = [
  parseAbiItem(
    'event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)',
  ),
] as const;
const v2Router = [
  parseAbiItem(
    'function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[])',
  ),
  parseAbiItem(
    'function swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[])',
  ),
  parseAbiItem(
    'function swapExactTokensForETH(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[])',
  ),
] as const;

// The Pool/Router address defaults below are Ethereum-mainnet contracts (chain id 1). Running this
// app on ANY other chain with those defaults would index nonexistent contracts and silently produce a
// vacuous store — so the address defaults are ONLY valid for the default chain. If CHAIN_ID is set to
// a non-default chain, POOL_ADDRESS and ROUTER_ADDRESS MUST be provided explicitly; otherwise fail
// loud at config load rather than silently backfilling cross-chain-wrong addresses (finding #7).
const DEFAULT_CHAIN_ID = 1;
const chainId = Number(process.env.CHAIN_ID ?? DEFAULT_CHAIN_ID);
const DEFAULT_POOL_ADDRESS = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640';
const DEFAULT_ROUTER_ADDRESS = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D';

if (chainId !== DEFAULT_CHAIN_ID) {
  const missing: string[] = [];
  if (!process.env.POOL_ADDRESS) {
    missing.push('POOL_ADDRESS');
  }
  if (!process.env.ROUTER_ADDRESS) {
    missing.push('ROUTER_ADDRESS');
  }
  if (missing.length > 0) {
    throw new Error(
      `traces app: CHAIN_ID=${chainId} is not the address-default chain (${DEFAULT_CHAIN_ID}), but ` +
        `${missing.join(' and ')} ${missing.length > 1 ? 'are' : 'is'} unset. The Pool/Router ` +
        `address defaults are Ethereum-mainnet contracts and would index nonexistent addresses on ` +
        `chain ${chainId}. Set explicit per-chain POOL_ADDRESS and ROUTER_ADDRESS (verify against ` +
        `the P0 capability report) before running this cell.`,
    );
  }
}

export default createConfig({
  database: {
    kind: 'pglite',
    directory: process.env.PGLITE_DIR ?? './.ponder/pglite',
  },
  chains: {
    mainnet: {
      // CHAIN_ID / POOL_ADDRESS / ROUTER_ADDRESS let the validation harness (harness/validate) reuse
      // this logs+receipts+traces shape on any chain the capability report clears for traces;
      // defaults keep the standalone eth run unchanged. A non-default CHAIN_ID without explicit
      // addresses is rejected above (fail-loud, no silent cross-chain defaults).
      id: chainId,
      rpc: process.env.PONDER_RPC_URL_1,
      portal: process.env.PORTAL_URL_1 || undefined,
    },
  },
  contracts: {
    Pool: {
      chain: 'mainnet',
      abi: v3Pool,
      address: (process.env.POOL_ADDRESS ??
        DEFAULT_POOL_ADDRESS) as `0x${string}`,
      includeTransactionReceipts: true,
      startBlock: Number(process.env.PONDER_START),
      endBlock: Number(process.env.PONDER_END),
    },
    Router: {
      chain: 'mainnet',
      abi: v2Router,
      address: (process.env.ROUTER_ADDRESS ??
        DEFAULT_ROUTER_ADDRESS) as `0x${string}`,
      includeCallTraces: true,
      startBlock: Number(process.env.PONDER_START),
      endBlock: Number(process.env.PONDER_END),
    },
  },
});
