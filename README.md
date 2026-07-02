# @subsquid/ponder

[![npm](https://img.shields.io/npm/v/@subsquid/ponder?color=cb0000&logo=npm)](https://www.npmjs.com/package/@subsquid/ponder)
[![CI](https://github.com/subsquid-labs/portal-ponder/actions/workflows/ci.yml/badge.svg)](https://github.com/subsquid-labs/portal-ponder/actions/workflows/ci.yml)
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

This runs a real indexer and backfills Ethereum history from the Portal — watch the events/second in the Ponder dev UI. The example uses `portal.sqd.dev`, the free public Portal: ideal for trying the fork and for development, but shared and rate-limited under load. For production, see [Going to production](#going-to-production).

## Why it's faster, and why a fork

RPC was built for transactions, not for reading data. A backfill over RPC is a stream of point lookups — one `eth_getLogs` per topic, one `eth_getBlockByNumber` per matched block — and a factory that discovers thousands of contracts at runtime turns that into hundreds of thousands of small, serial requests. The Portal is built for the opposite: it answers an arbitrary `[from, to]` block range as one HTTP stream. Hundreds of thousands of round-trips become a handful of large streamed reads.

The speed comes from integrating at Ponder's range-oriented historical-sync seam, where one interval maps to one Portal range scan. That seam isn't part of Ponder's public API, so this is a small fork rather than a plugin — generated from upstream Ponder plus a short patch, so it tracks Ponder closely.

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
node --experimental-strip-types harness/compat/report.ts ./ponder.config.ts   # → READY / NEEDS_TRACES / NO_DATASET per source
```

Full adoption path — check, swap, run, validate, roll back — in [`MIGRATION.md`](MIGRATION.md).

## Going to production

The free public Portal is ideal for trying the fork and for development, but shares capacity across all users and is rate-limited under load. For production throughput and reliability, use a **dedicated Portal** — your own capacity, no shared limits. Dedicated Portals are set up with the SQD team today: [talk to us](https://sqd.dev/portal/). Self-served tiers (free, starter, growth) are coming soon.

## Examples

- [`euler-subgraph`](examples/euler-subgraph/) — a subgraph ported to Ponder + Portal (factories, `readContract`, aggregation). The Quickstart above.
- [`uniswap-portal`](examples/uniswap-portal/) — all five source types in one app (logs, receipts, traces, block intervals, accounts).
- [`euler-multichain`](examples/euler-multichain/) — a compact multichain factory indexer.

## Learn more

- [**How it works**](portal/INTEGRATION.md) — the historical-sync seam, read-ahead chunk buffer, factory discovery, adaptive concurrency, and memory backpressure.
- [**Observability**](portal/INTEGRATION.md) — `PORTAL_METRICS_FILE` writes a per-chain JSON metrics file (throughput, bytes, errors, RPC-fallback); `PORTAL_GATE_LOG=1` logs the adaptive controller.
- [**Versioning & releases**](PUBLISHING.md) — `@subsquid/ponder@<ponder-version>-sqd.<rev>`, generated from upstream Ponder + a per-version patch.

## Contributing & support

- **Bugs & feature requests** — [open a GitHub issue](https://github.com/subsquid-labs/portal-ponder/issues).
- **Questions & help** — [SquidDevs on Telegram](https://t.me/HydraDevs).
- **Pull requests welcome** — the repo holds only the Portal layer (`portal/`) and a per-version wiring patch; see [`PUBLISHING.md`](PUBLISHING.md) for how the fork is built.
