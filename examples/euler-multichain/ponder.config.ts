import { createConfig, factory } from '@subsquid/ponder';
import { parseAbiItem } from 'abitype';

const proxyCreated = parseAbiItem(
  'event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)',
);
const eVaultAbi = [
  parseAbiItem(
    'event Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)',
  ),
  parseAbiItem(
    'event Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)',
  ),
  parseAbiItem('event Borrow(address indexed account, uint256 assets)'),
  parseAbiItem('event Repay(address indexed account, uint256 assets)'),
  parseAbiItem(
    'event Liquidate(address indexed liquidator, address indexed violator, address collateral, uint256 repayAssets, uint256 yieldBalance)',
  ),
] as const;

const PORTAL = (slug: string) => `https://portal.sqd.dev/datasets/${slug}`;

// Zero-config defaults so `npm run dev` works from a fresh clone with no .env:
//  - portal: the free public Portal per chain (already defaulted via PORTAL()).
//  - rpc: keyless public nodes (realtime tip + state reads). The shared public RPC rate-limits
//    under load; set PONDER_RPC_URL_<chainId> to your own for real work.
//  - endBlock: each chain defaults to a short window (~DEMO_SPAN blocks from its factory deploy)
//    so the multichain demo finishes in ~1-2 min. Set PONDER_FULL=1 to backfill full history.
const DEMO_SPAN = Number(process.env.PONDER_DEMO_SPAN ?? 200_000);
const FULL = process.env.PONDER_FULL === '1';

// Bound the demo to [start, min(start + DEMO_SPAN, fullEnd)] unless PONDER_FULL=1.
const bound = (start: number, fullEnd: number) => {
  if (FULL) {
    return fullEnd;
  }

  return Math.min(start + DEMO_SPAN, fullEnd);
};

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc:
        process.env.PONDER_RPC_URL_1 ?? 'https://ethereum-rpc.publicnode.com',
      portal: PORTAL('ethereum-mainnet'),
    },
    base: {
      id: 8453,
      rpc: process.env.PONDER_RPC_URL_8453 ?? 'https://base-rpc.publicnode.com',
      portal: PORTAL('base-mainnet'),
    },
    arbitrum: {
      id: 42161,
      rpc:
        process.env.PONDER_RPC_URL_42161 ??
        'https://arbitrum-one-rpc.publicnode.com',
      portal: PORTAL('arbitrum-one'),
    },
  },
  contracts: {
    EVault: {
      abi: eVaultAbi,
      chain: {
        mainnet: {
          address: factory({
            address: '0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e',
            event: proxyCreated,
            parameter: 'proxy',
          }),
          startBlock: 20_429_973,
          endBlock: bound(20_429_973, 25_423_884),
        },
        base: {
          address: factory({
            address: '0x7F321498A801A191a93C840750ed637149dDf8D0',
            event: proxyCreated,
            parameter: 'proxy',
          }),
          startBlock: 18_000_000,
          endBlock: bound(18_000_000, 47_979_047),
        },
        arbitrum: {
          address: factory({
            address: '0x78Df1CF5bf06a7f27f2ACc580B934238C1b80D50',
            event: proxyCreated,
            parameter: 'proxy',
          }),
          startBlock: 255_000_000,
          endBlock: bound(255_000_000, 478_620_027),
        },
      },
    },
  },
});
