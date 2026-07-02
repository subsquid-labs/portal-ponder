# @subsquid/ponder

[![npm](https://img.shields.io/npm/v/@subsquid/ponder?color=cb0000&logo=npm)](https://www.npmjs.com/package/@subsquid/ponder)
[![CI](https://github.com/subsquid-labs/portal-ponder/actions/workflows/ci.yml/badge.svg)](https://github.com/subsquid-labs/portal-ponder/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![status](https://img.shields.io/badge/status-beta-orange)](#going-to-production)
[![Telegram](https://img.shields.io/badge/chat-SquidDevs-2CA5E0?logo=telegram)](https://t.me/HydraDevs)

A drop-in fork of [Ponder](https://github.com/ponder-sh/ponder) that runs the historical backfill through the [SQD Portal](https://sqd.dev/portal/) instead of per-chain JSON-RPC — **several times faster**, across 130+ EVM networks. Realtime stays on your RPC, your handlers and schema don't change, and the switch is one line of config per chain.

Ponder is mature, production-proven, and a pleasure to work with. Historical backfill speed was the one gap — this closes it and changes nothing else.

> **~8× faster** full-history backfill · **15 chains, 28M events** in one app in **45 min** · logs **byte-identical** to `eth_getLogs`

```ts
// ponder.config.ts — add `portal:` per chain; nothing else changes
import { createConfig } from "@subsquid/ponder";

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,                          // realtime + state reads
      portal: "https://portal.sqd.dev/datasets/ethereum-mainnet", // historical backfill
    },
  },
  contracts: { /* unchanged */ },
});
```

```bash
npm install @subsquid/ponder   # same `ponder` bin — `ponder dev` / `ponder start` work as before
```

## Quickstart

Try it against the free public Portal — no key required:

```bash
git clone https://github.com/subsquid-labs/portal-ponder
cd portal-ponder/examples/euler-subgraph
npm install && npm run dev
```

This runs a real indexer — a subgraph ported to Ponder — and backfills its full history on Ethereum (~5M blocks, from the protocol's deploy at block 20,529,207) from the Portal. Watch the events/second in the Ponder dev UI. The example uses `portal.sqd.dev`, the free public Portal: ideal for trying the fork and for development, but shared and rate-limited under load. For production, see [Going to production](#going-to-production).

## Why it's faster, and why a fork

RPC was built for transactions, not for reading data. A backfill over RPC is a stream of point lookups — one `eth_getLogs` per topic, one `eth_getBlockByNumber` per matched block — and a factory that discovers thousands of contracts at runtime turns that into hundreds of thousands of small, serial requests. The Portal is built for the opposite: it answers an arbitrary `[from, to]` block range as one HTTP stream. Hundreds of thousands of round-trips become a handful of large streamed reads.

The speed comes from integrating at Ponder's range-oriented historical-sync seam, where one interval maps to one Portal range scan. That seam isn't part of Ponder's public API, so this is a small fork rather than a plugin — generated from upstream Ponder plus a short patch, so it tracks Ponder closely.

That's only half of it. A fast endpoint alone doesn't make a fast indexer — the fork's own engineering (read-ahead that keeps indexing, not fetch, the bottleneck; a shared controller that saturates the Portal at a fixed memory ceiling across every chain; factory discovery over ranges) is what extracts the speed. The full mechanics, and the honest single-thread ceiling, are in [**HOW-IT-WORKS.md**](HOW-IT-WORKS.md).

## Benchmarks

Full Ethereum history, deploy → head (~5M blocks, 457,931 events), on a dedicated Portal:

| Backfill | Wall-clock | Speedup |
|---|--:|--:|
| Stock RPC | ~38 min | 1× |
| The fork | **4m 45s** | **~8×** |

At scale, one app indexing a real protocol (Euler V2) across all **15 chains** it runs on backfilled **28,405,932 events in 45 minutes** on ~1 core and 16 GB, byte-verified complete against the Portal. Full write-up: [`REPORT.md`](harness/euler-multichain/REPORT.md) · methodology: [`BENCHMARKS.md`](harness/bench/BENCHMARKS.md).

## Compatibility

All of Ponder's source types are supported — logs, factories, transactions, receipts, traces, block intervals, accounts; `readContract` uses your RPC. The Portal serves [130+ EVM networks](https://docs.sqd.dev/en/data/all-networks); per-network capabilities and per-portal availability vary. Check an indexer before migrating:

```bash
npx tsx harness/compat/report.ts ./ponder.config.ts   # → READY / NEEDS_TRACES / NO_DATASET per source
```

Full adoption path — check, swap, run, validate, roll back — in [`MIGRATION.md`](MIGRATION.md).

## Ponder versions

`@subsquid/ponder@X.Y.Z-sqd.<rev>` is `ponder@X.Y.Z` plus the Portal layer, pinned to a known Ponder version; `-sqd.<rev>` ships a fork-side fix on the same Ponder version. The seam is stable across 0.15.17–0.16.6.

| Ponder | `@subsquid/ponder` | Status |
|---|---|---|
| 0.16.6 | `0.16.6-sqd.1` | **latest** |
| 0.15.17 | `0.15.17-sqd.1` | published |

`npm install @subsquid/ponder` installs the latest; pin `@X.Y.Z-sqd.<rev>` to match your Ponder version. How versions work: [`PUBLISHING.md`](PUBLISHING.md).

## Going to production

**The fork is in beta.** We've tested it extensively in-house — the 15-chain, 28M-event run above is one such test — and we'd value your testing and bug reports ([GitHub issues](https://github.com/subsquid-labs/portal-ponder/issues) · [SquidDevs on Telegram](https://t.me/HydraDevs)). The Portal underneath is not new: it's the enterprise-grade data layer behind **$20B+ in TVL**, used in production by teams like GMX, Morpho, and PancakeSwap.

The free public Portal is ideal for trying the fork and for development, but shares capacity across all users and is rate-limited under load. For production throughput and reliability, use a **dedicated Portal** — your own capacity, no shared limits. Dedicated Portals are set up with the SQD team today: [talk to us](https://sqd.dev/portal/). Self-served tiers (free, starter, growth) are coming soon.

## Examples

- [`euler-subgraph`](examples/euler-subgraph/) — a subgraph ported to Ponder + Portal (factories, `readContract`, aggregation). The Quickstart above.
- [`uniswap-portal`](examples/uniswap-portal/) — all five source types in one app (logs, receipts, traces, block intervals, accounts).
- [`euler-multichain`](examples/euler-multichain/) — a compact multichain factory indexer.

## Learn more

- [**How it works**](HOW-IT-WORKS.md) — the design story: why a streamed range beats per-topic RPC lookups, the historical-sync seam, the shared read-ahead controller, factory discovery over ranges, and where the single-thread ceiling honestly is. Operational reference: [`portal/INTEGRATION.md`](portal/INTEGRATION.md).
- [**Observability**](portal/INTEGRATION.md) — `PORTAL_METRICS_FILE` writes a per-chain JSON metrics file (throughput, bytes, errors, RPC-fallback); `PORTAL_GATE_LOG=1` logs the adaptive controller.
- **Portal-native realtime** (experimental) — realtime runs on your RPC by default; set `PORTAL_REALTIME=stream` to serve the tip from the Portal's fork-aware `/stream` instead of RPC.
- [**Versioning & releases**](PUBLISHING.md) — `@subsquid/ponder@<ponder-version>-sqd.<rev>`, generated from upstream Ponder + a per-version patch.

## Contributing & support

- **Bugs & feature requests** — [open a GitHub issue](https://github.com/subsquid-labs/portal-ponder/issues).
- **Questions & help** — [SquidDevs on Telegram](https://t.me/HydraDevs).
- **Pull requests welcome** — the repo holds only the Portal layer (`portal/`) and a per-version wiring patch; see [`PUBLISHING.md`](PUBLISHING.md) for how the fork is built.
