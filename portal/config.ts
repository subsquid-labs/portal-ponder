/**
 * Config-side glue for the native injection path (rung 3 of the migration).
 *
 * Stock Ponder drops unknown chain-config fields, so instead of a core type
 * change we route `portal` through a process-global registry. The client wraps
 * their config object once:
 *
 *   import { createConfig } from "ponder";
 *   import { withPortal } from "@your-org/ponder-portal";
 *   export default createConfig(withPortal({
 *     chains: { mainnet: { id: 1, rpc: process.env.RPC_1, portal: "https://portal.sqd.dev/datasets/ethereum-mainnet" } },
 *     contracts: { ... },   // unchanged
 *   }));
 *
 * The runtime injection (patch-package or a `module.register` load hook that
 * wraps createHistoricalSync) reads `getPortalDataset(chain.id)` to decide
 * whether to back a chain's historical sync with Portal. Handlers are untouched.
 */

/** chainId → Portal dataset URL, populated by withPortal(), read by the injection. */
export const portalRegistry = new Map<number, string>();

export type PortalChainConfig = {
  id: number;
  portal?: string;
  [k: string]: unknown;
};

/** Pre-process a Ponder config: record `portal` per chain and strip it before Ponder sees it. */
export function withPortal<
  T extends { chains?: Record<string, PortalChainConfig> },
>(config: T): T {
  for (const chain of Object.values(config.chains ?? {})) {
    if (chain && typeof chain.portal === 'string') {
      portalRegistry.set(chain.id, chain.portal);
      delete chain.portal;
    }
  }
  return config;
}

export const getPortalDataset = (chainId: number): string | undefined =>
  portalRegistry.get(chainId);
