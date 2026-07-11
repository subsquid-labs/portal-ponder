import { createConfig, factory } from '@subsquid/ponder';
import { parseAbiItem } from 'abitype';
import { EVaultAbi } from './abis/EVault';

const proxyCreated = parseAbiItem(
  'event ProxyCreated(address indexed proxy, bool upgradeable, address implementation, bytes trailingData)',
);

const mockPortal = process.env.PORTAL_URL_1 ?? 'http://127.0.0.1:8701';
const rpc =
  process.env.PONDER_RPC_URL_1 === 'mock'
    ? `${mockPortal.replace(/\/$/, '')}/rpc`
    : process.env.PONDER_RPC_URL_1;

export default createConfig({
  database:
    process.env.CHAOS_PG_URL || process.env.DATABASE_URL
      ? {
          kind: 'postgres',
          connectionString:
            process.env.CHAOS_PG_URL ?? process.env.DATABASE_URL,
        }
      : {
          kind: 'pglite',
          directory: process.env.PGLITE_DIR ?? './.ponder/pglite',
        },
  chains: {
    mainnet: {
      id: Number(process.env.CHAIN_ID ?? 1),
      rpc,
      portal: mockPortal,
    },
  },
  contracts: {
    EVault: {
      abi: EVaultAbi,
      chain: 'mainnet',
      address: factory({
        address: (process.env.EULER_FACTORY ??
          '0x29a56a1b8214D9Cf7c5561811750D5cBDb45CC8e') as `0x${string}`,
        event: proxyCreated,
        parameter: 'proxy',
      }),
      includeTransactionReceipts: false,
      startBlock: Number(process.env.PONDER_START ?? 100),
      endBlock: process.env.PONDER_END
        ? Number(process.env.PONDER_END)
        : undefined,
    },
  },
});
