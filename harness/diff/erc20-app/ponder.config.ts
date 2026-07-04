import { createConfig } from '@subsquid/ponder';
import { parseAbiItem } from 'abitype';

// High-volume SINGLE-contract log indexer: USDC Transfer + receipts. Tests dense per-block log
// volume + the receipt path (no factory), a different shape than the factory benches.
const transfer = parseAbiItem(
  'event Transfer(address indexed from, address indexed to, uint256 value)',
);

export default createConfig({
  database: {
    kind: 'pglite',
    directory: process.env.PGLITE_DIR ?? './.ponder/pglite',
  },
  chains: {
    mainnet: {
      // CHAIN_ID / ERC20_ADDRESS let the validation harness (harness/validate) reuse this
      // logs+receipts shape on any paid chain; defaults keep the standalone eth USDC run unchanged.
      id: Number(process.env.CHAIN_ID ?? 1),
      rpc: (process.env.PONDER_RPC_URL_1 ?? '').includes(',')
        ? process.env.PONDER_RPC_URL_1.split(',')
            .map((x) => x.trim())
            .filter(Boolean)
        : process.env.PONDER_RPC_URL_1,
      portal: process.env.PORTAL_URL_1 || undefined,
    },
  },
  contracts: {
    USDC: {
      abi: [transfer] as const,
      chain: 'mainnet',
      address: (process.env.ERC20_ADDRESS ??
        '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48') as `0x${string}`,
      includeTransactionReceipts: process.env.INCLUDE_RECEIPTS !== 'false',
      startBlock: Number(process.env.PONDER_START ?? 22_200_000),
      endBlock: process.env.PONDER_END
        ? Number(process.env.PONDER_END)
        : undefined,
    },
  },
});
