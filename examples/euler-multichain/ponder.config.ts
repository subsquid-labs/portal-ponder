import { createConfig, factory } from '@subsquid/ponder';
import { parseAbiItem } from 'abitype';

const proxyCreated = parseAbiItem(
  'event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)',
);
const eVaultFactoryAbi = [proxyCreated] as const;
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
//  - rpc: keyless *archive* public RPCs (drpc.org) — realtime tip + state reads. Archive is
//    required because reads happen at historical blocks. Rate-limited under load; set
//    PONDER_RPC_URL_<chainId> to your own for real work.
//  - endBlock: each chain defaults to a ~DEMO_SPAN-block window anchored at its FIRST EVault
//    ProxyCreated event, so the demo indexes real factory children (not an empty pre-deploy range).
//    Set PONDER_FULL=1 to backfill full history.
const DEMO_SPAN = Number(process.env.PONDER_DEMO_SPAN ?? 200_000);
const FULL = process.env.PONDER_FULL === '1';

const MAINNET_DEMO_START = 22_681_265;
const MAINNET_DEMO_END = 22_801_264;
const MAINNET_FULL_START = 20_429_973;
const MAINNET_FULL_END = 25_423_884;

// Base and Arbitrum: the GenericFactory is deployed far earlier than its first vault, so the demo
// starts at the first EVault ProxyCreated (verified against the Portal dataset) rather than the
// factory-deploy block — otherwise the ~DEMO_SPAN window lands in an empty pre-vault range and the
// chain indexes zero events. FULL still backfills from the factory-deploy block.
const BASE_DEMO_START = 36_016_000; // first EVault ProxyCreated on base: 36_016_090
const BASE_FULL_START = 18_000_000;
const BASE_FULL_END = 47_979_047;
const ARBITRUM_DEMO_START = 317_852_000; // first EVault ProxyCreated on arbitrum: 317_852_641
const ARBITRUM_FULL_START = 255_000_000;
const ARBITRUM_FULL_END = 478_620_027;

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
      rpc: process.env.PONDER_RPC_URL_1 ?? 'https://eth.drpc.org',
      portal: PORTAL('ethereum-mainnet'),
    },
    base: {
      id: 8453,
      rpc: process.env.PONDER_RPC_URL_8453 ?? 'https://base.drpc.org',
      portal: PORTAL('base-mainnet'),
    },
    arbitrum: {
      id: 42161,
      rpc: process.env.PONDER_RPC_URL_42161 ?? 'https://arbitrum.drpc.org',
      portal: PORTAL('arbitrum-one'),
    },
  },
  contracts: {
    EVaultFactory: {
      abi: eVaultFactoryAbi,
      chain: {
        mainnet: {
          address: '0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e',
          startBlock: FULL ? MAINNET_FULL_START : MAINNET_DEMO_START,
          endBlock: FULL ? MAINNET_FULL_END : MAINNET_DEMO_END,
        },
        base: {
          address: '0x7F321498A801A191a93C840750ed637149dDf8D0',
          startBlock: FULL ? BASE_FULL_START : BASE_DEMO_START,
          endBlock: bound(BASE_DEMO_START, BASE_FULL_END),
        },
        arbitrum: {
          address: '0x78Df1CF5bf06a7f27f2ACc580B934238C1b80D50',
          startBlock: FULL ? ARBITRUM_FULL_START : ARBITRUM_DEMO_START,
          endBlock: bound(ARBITRUM_DEMO_START, ARBITRUM_FULL_END),
        },
      },
    },
    EVault: {
      abi: eVaultAbi,
      chain: {
        mainnet: {
          address: factory({
            address: '0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e',
            event: proxyCreated,
            parameter: 'proxy',
          }),
          startBlock: FULL ? MAINNET_FULL_START : MAINNET_DEMO_START,
          endBlock: FULL ? MAINNET_FULL_END : MAINNET_DEMO_END,
        },
        base: {
          address: factory({
            address: '0x7F321498A801A191a93C840750ed637149dDf8D0',
            event: proxyCreated,
            parameter: 'proxy',
          }),
          startBlock: FULL ? BASE_FULL_START : BASE_DEMO_START,
          endBlock: bound(BASE_DEMO_START, BASE_FULL_END),
        },
        arbitrum: {
          address: factory({
            address: '0x78Df1CF5bf06a7f27f2ACc580B934238C1b80D50',
            event: proxyCreated,
            parameter: 'proxy',
          }),
          startBlock: FULL ? ARBITRUM_FULL_START : ARBITRUM_DEMO_START,
          endBlock: bound(ARBITRUM_DEMO_START, ARBITRUM_FULL_END),
        },
      },
    },
  },
});
