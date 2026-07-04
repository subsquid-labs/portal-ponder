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

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,
      portal: PORTAL('ethereum-mainnet'),
    },
    base: {
      id: 8453,
      rpc: process.env.PONDER_RPC_URL_8453,
      portal: PORTAL('base-mainnet'),
    },
    arbitrum: {
      id: 42161,
      rpc: process.env.PONDER_RPC_URL_42161,
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
          endBlock: 25_423_884,
        },
        base: {
          address: factory({
            address: '0x7F321498A801A191a93C840750ed637149dDf8D0',
            event: proxyCreated,
            parameter: 'proxy',
          }),
          startBlock: 18_000_000,
          endBlock: 47_979_047,
        },
        arbitrum: {
          address: factory({
            address: '0x78Df1CF5bf06a7f27f2ACc580B934238C1b80D50',
            event: proxyCreated,
            parameter: 'proxy',
          }),
          startBlock: 255_000_000,
          endBlock: 478_620_027,
        },
      },
    },
  },
});
