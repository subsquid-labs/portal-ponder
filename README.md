# @subsquid/ponder

A drop-in fork of [Ponder](https://github.com/ponder-sh/ponder) that runs the historical backfill through the [SQD Portal](https://sqd.dev/portal/) instead of per-chain JSON-RPC. Backfills run several times faster, and complete on chains where an RPC backfill is impractical. Realtime stays on your RPC, your handlers and schema don't change, and the switch is one line of config per chain.

**In numbers:** ~8× faster on a full Ethereum backfill · 15 chains and 28M events in one app in 45 minutes · Portal logs byte-identical to `eth_getLogs`. See [Benchmarks](#benchmarks).

## Quickstart

Try it in a couple of minutes against the free public Portal — no key required:

```bash
git clone https://github.com/subsquid-labs/portal-ponder
cd portal-ponder/examples/euler-subgraph
npm install
npm run dev
```

This runs a real indexer — the Euler V2 subgraph ported to Ponder — and backfills Ethereum history from the Portal. Watch the events/second in the Ponder dev UI; that throughput is the point. Add an Ethereum RPC for realtime as usual (the Portal backfill itself needs no key); see the [example README](examples/euler-subgraph/).

The example points at `https://portal.sqd.dev`, the **free public Portal**. It needs no key and is ideal for trying the fork and for development. It shares capacity across all users, so it is rate-limited and slower under load — for production throughput, see [Going to production](#going-to-production).

## Add it to an existing app

Point the config at the fork and add a `portal` URL per chain. Nothing else changes:

```ts
// ponder.config.ts
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
npm install @subsquid/ponder   # provides the same `ponder` bin — `ponder dev` / `ponder start` work as before
```

Versions are `<ponder-version>-sqd.<rev>`: `0.16.6-sqd.1` is `ponder@0.16.6` plus the Portal layer. Remove the `portal` line (or point the dependency back at `ponder`) to return to stock Ponder. Before migrating a larger app, run the [compatibility check](#compatibility).

## Why it's faster, and why a fork

RPC was built for transactions, not for reading data. A backfill over RPC is a stream of point lookups — one `eth_getLogs` per topic, one `eth_getBlockByNumber` per matched block — and a factory that discovers thousands of contracts at runtime turns that into hundreds of thousands of small, serial requests. The Portal is built for the opposite: it answers an arbitrary `[from, to]` block range as a single HTTP stream, reorg-safe and at constant memory. A backfill that RPC serves as hundreds of thousands of round-trips becomes a handful of large streamed reads — which is where the [8×](#benchmarks) comes from.

Reaching that means integrating at Ponder's range-oriented historical-sync seam, where one interval maps directly to one Portal range scan. That seam isn't part of Ponder's public API, so the integration is a small fork rather than a plugin. The fork is generated from upstream Ponder plus a short patch and tracks it closely — a thin plugin can follow once Ponder exposes the hook.

## How it works

The change is one module (`portal/portal.ts`, a `HistoricalSync` implementation) plus a short wiring patch that adds `portal?` to the chain config and routes to it when set.

- **Read-ahead chunk buffer.** Ponder requests small intervals; the Portal is latency-bound per request but has large parallel bandwidth. The fork fetches large aligned chunks, serves every interval from that cache, and prefetches the next *N* chunks concurrently so latency overlaps instead of serializing. This is the largest single gain.
- **Correctness-safe factory discovery.** Child addresses are discovered per chunk into a shared set; a data chunk fetches only once discovery has completed through its own block range, so out-of-order parallel fetches never miss a child event. Discovery is clamped to the factory's real start block.
- **Block-density auto-scaling.** Chunk size scales with the chain's head, keeping round-trips roughly constant across chains (Arbitrum's 478M blocks don't mean 19× the requests).
- **Server-side filtering.** Every row filter is pushed to the Portal's native filters (logs by address and topics, traces by call target and sighash, account transactions by from/to); field projection requests only the columns the sync store persists. The exception is block-interval sources, which the Portal cannot express as a modulo filter (range-scan + client filter).
- **Memory safety on dense sources.** Trace and block sources cap the chunk grid (`PORTAL_TRACE_CHUNK_BLOCKS`, default 25k) so a busy contract cannot exhaust memory.
- **Finality-gap fallback.** The Portal serves finalized data; if its head lags Ponder's target, intervals past it are delegated to the RPC sync, so the backfill stays complete.
- **Resilience.** Socket errors and HTTP 503/529/429 (honoring `Retry-After`) are retried with backoff.

Tunables: `PORTAL_CHUNK_BLOCKS`, `PORTAL_READAHEAD`, `PORTAL_TRACE_CHUNK_BLOCKS`, `PORTAL_API_KEY`. Full write-up: [`portal/INTEGRATION.md`](portal/INTEGRATION.md).

## Compatibility

**Source types** — all of Ponder's are supported: `logs`, log-`factory()`, `transactions`, `receipts` (`includeTransactionReceipts`), `traces` (`includeCallTraces` + transfer filters), `blocks: { interval }`, and `accounts: {}`. `readContract` calls in handlers use your `rpc` as usual.

**Chains** — the Portal serves [130+ EVM networks](https://docs.sqd.dev/en/data/all-networks) (ethereum, base, arbitrum, optimism, polygon, …). Per-network capabilities differ (traces and state-diffs aren't on every chain; some have block-range caveats), and dataset availability is per-portal.

**Check your indexer before migrating** with the compatibility report. It inspects a `ponder.config.ts` and, per source, confirms the type is supported, the target portal serves the chain's dataset, and the network has the needed capability:

```bash
PORTAL_API_KEY=… PORTAL_BASE=<your portal> \
  node --experimental-strip-types harness/compat/report.ts ./ponder.config.ts
```

For a clean event or factory indexer (Uniswap, Euler, …) it reports `READY NOW`. The full adoption path — compat check, swap the dependency, run, validate, roll back — is in [`MIGRATION.md`](MIGRATION.md).

## Benchmarks

Resync wall-clock (events indexed end to end) is the metric. These numbers are on a dedicated Portal; the free public Portal shares capacity and is slower under concurrency. Full analysis and the reproducible harness: [`harness/bench/BENCHMARKS.md`](harness/bench/BENCHMARKS.md).

**Single chain — full Ethereum Euler history** (deploy → head, ~5M blocks, 466 vaults, 457,931 events → pglite):

| Integration | Resync wall-clock | Speedup |
|---|---|---|
| Stock RPC (one stream per Ponder interval) | ~38 min | 1× |
| Read-ahead chunk buffer | ~17 min | ~2× |
| **Parallel prefetch (the fork)** | **4m 45s** | **~8×** |

**Multi-chain** — Ethereum, Base, and Arbitrum in one `ponder start`: ~1.05M events in ~16 min, 0 errors.

**Correctness** — Portal logs are byte-identical to `eth_getLogs` (differential test, Base WETH transfers, 380/380 logs, all fields); the transforms are unit-tested against captured Portal data.

## Flagship: every Euler V2 chain in one indexer

[`harness/euler-multichain/`](harness/euler-multichain/) is the reference deployment: all 15 Portal-supported Euler V2 chains in a single Ponder app, full history per chain, the complete 24-event EVault set, Portal backfill into Postgres. It is the production shape of an Euler indexer and the fork's hardest stress test — one Portal endpoint, one database, fifteen chains backfilling at once.

**28,405,932 events across all 15 chains, full history, in 45 minutes** on roughly one core and 16 GB (plus a separate Postgres), byte-verified complete against the Portal (60/60 sampled windows exact) and reproducible across two runs. Per-chain breakdown and analysis: [`harness/euler-multichain/REPORT.md`](harness/euler-multichain/REPORT.md).

## Going to production

The free public Portal needs no key and is ideal for trying the fork and for development. Because it shares capacity across everyone, it is rate-limited and slows down under concurrency — a large production backfill will feel that.

For production throughput and reliability, use a **dedicated Portal**: your own capacity, no shared rate limits. Dedicated Portals are set up with the SQD team today — [talk to us](https://sqd.dev/portal/). Self-served shared tiers (free, starter, and growth) are coming soon.

## Examples

Each is a real indexer that runs end to end on `@subsquid/ponder` (Portal backfill, realtime and `readContract` on `rpc`):

- [`examples/euler-subgraph/`](examples/euler-subgraph/) — the Euler V2 subgraph ported to Ponder and the Portal (factory templates, `readContract` state reads, aggregation). A worked *subgraph → Ponder* migration, and the Quickstart above.
- [`examples/uniswap-portal/`](examples/uniswap-portal/) — all five source types in one app (logs, receipts, traces, block-interval, accounts).
- [`examples/euler-multichain/`](examples/euler-multichain/) — a compact three-chain factory indexer (the benchmark app).

## Metrics & observability

The backfill emits its own metrics alongside Ponder's normal logging and UI. Set `PORTAL_METRICS_FILE=<path>` and the sync writes `<path>.<chainId>` (JSON, refreshed each interval):

```bash
PORTAL_METRICS_FILE=/tmp/portal-metrics ponder start
cat /tmp/portal-metrics.1 | jq     # chainId 1: wall-clock, chunk width, fetch counts, inserted rows, RPC-fallback intervals
```

Read stability from `fetch.errors` / `fetch.retries` / `rpcFallbackIntervals` (all 0 in a healthy run) and efficiency from bytes-per-event. `PONDER_LOG_LEVEL=debug` adds a per-interval `service=portal` line. The bench harness (`harness/bench/`) turns the metrics file into a comparable wall-clock / events-per-second / RSS table.

## Limitations

- **The bottleneck moves to indexing.** Once the Portal makes the backfill fast, decode and store dominate; use Postgres and parallel indexing for very large resyncs. This is orthogonal to the Portal.
- **Block-interval sources range-scan the window** (the Portal has no modulo filter), so they read more than their matched-block count. Use a tight interval or range.
- **`readContract`-heavy indexers stay RPC-bound** on those calls — the Portal accelerates logs and state, not handler contract reads.
- **It's a fork, not a plugin.** Ponder's internals aren't publicly exported, so a thin plugin awaits an upstream `HistoricalSync` hook. The seam is small and stable, so the fork tracks Ponder with a short patch — see [`PUBLISHING.md`](PUBLISHING.md#why-a-fork-not-a-thin-plugin).

## Versioning & releases

`@subsquid/ponder@X.Y.Z-sqd.<rev>` is `ponder@X.Y.Z` plus the Portal layer — the Ponder version is visible in the number, and `-sqd.<rev>` lets the fork ship a fix on the same Ponder version. The fork is generated, not hand-maintained: this repo holds only the Portal layer (`portal/`) and a per-version `wiring/<ver>.patch`; [`scripts/sync-upstream.sh`](scripts/sync-upstream.sh) clones `ponder@<ver>`, applies the layer, and builds. [`versions.json`](versions.json) is the supported-version matrix, and CI proves the seam against each. Details: [`PUBLISHING.md`](PUBLISHING.md).

## Contributing, feedback & bug reports

Questions, bugs, feedback, and contributions are all welcome:

- **Bugs & feature requests** — [open a GitHub issue](https://github.com/subsquid-labs/portal-ponder/issues).
- **Questions & help** — [SquidDevs on Telegram](https://t.me/HydraDevs).
- **Contributions** — pull requests are welcome. The repo holds only the Portal layer (`portal/`) and the per-version wiring patch; the published package is generated from upstream Ponder — see [`PUBLISHING.md`](PUBLISHING.md) for how the fork is built and how to add a Ponder version.

## Repo layout

```
portal/                  the Portal layer (the entire diff vs upstream Ponder)
  portal.ts                the Portal-backed HistoricalSync
  portal-transform.ts      pure NDJSON → Sync* transforms (+ tests)
  config.ts · INTEGRATION.md · wiring/<ver>.patch
scripts/sync-upstream.sh   generate @subsquid/ponder@<ver> from ponder@<ver> + the layer
versions.json              the @subsquid/ponder ↔ ponder version matrix
examples/                  runnable indexers (euler-subgraph · uniswap-portal · euler-multichain)
harness/bench/             benchmark base + instrumentation
harness/compat/            compatibility report (analyzer + docs-matrix snapshot)
harness/euler-multichain/  the 15-chain flagship + REPORT.md
MIGRATION.md · PUBLISHING.md
```
