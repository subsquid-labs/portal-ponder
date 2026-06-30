# @subsquid/ponder

A **drop-in fork of [Ponder](https://github.com/ponder-sh/ponder)** whose historical backfill streams from **[SQD Portal](https://docs.sqd.dev)** — a columnar, range-scan data lake — instead of JSON-RPC. Backfills run **~8× faster** (and finish on chains where RPC backfill is impractical); realtime/frontfill stays on your RPC. The switch is **one config line per chain**, and **your handlers and schema are unchanged**.

```bash
npm install @subsquid/ponder@0.16.6   # == ponder@0.16.6 + the Portal layer (versions mirror ponder)
```

```ts
// ponder.config.ts — import from the fork, add `portal:` per chain. Nothing else changes.
import { createConfig } from "@subsquid/ponder";

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,                          // realtime + state reads
      portal: "https://portal.sqd.dev/datasets/ethereum-mainnet", // ← historical backfill
    },
  },
  contracts: { /* unchanged */ },
});
```

`@subsquid/ponder` provides the same `ponder` bin, so `ponder dev` / `ponder start` work as before. Remove the `portal:` line (or point the dependency back at `ponder`) and you're on stock Ponder again.

---

## Why

Ponder's RPC backfill fans out **one `eth_getLogs` per event-topic per contract group**, then **one `eth_getBlockByNumber` (+receipts/traces) per matched block** — a point-lookup workload on a range-scan-shaped problem. Factory contracts (e.g. Euler — thousands of vaults discovered at runtime) explode the fan-out into hundreds of thousands of tiny, non-contiguous requests. And Ponder's adaptive RPC client *demotes* slow backends, so pointing `rpc:` at Portal-as-JSON-RPC reads Portal's range-scan latency as a "failing node" and spirals into stalls — which is why prior RPC-emulation shims underperform.

This fork integrates one level up, at Ponder's range-oriented **`HistoricalSync` seam** (`syncBlockRangeData` / `syncBlockData`, interval-scoped). One interval becomes one streamed columnar scan; nothing touches the RPC penalty machinery; realtime keeps using `rpc`. SQD Portal answers a whole `[from, to]` range in a single pass.

---

## Compatibility

**Source types — all of Ponder's are supported:** `logs`, log-`factory()`, `transactions`, `receipts` (`includeTransactionReceipts`), `traces` (`includeCallTraces` + transfer filters), `blocks: { interval }`, and `accounts: {}` (transaction from/to). `readContract` calls in handlers go to your `rpc` as usual.

**Chains:** Portal serves [300+ EVM networks](https://docs.sqd.dev/en/data/all-networks) (ethereum, base, arbitrum, optimism, polygon, …). Per-network **capabilities differ** — traces and state-diffs aren't on every chain, and some have block-range caveats (e.g. Optimism traces from the Bedrock block; Arbitrum has traces but no state-diffs). Dataset **availability is also per-portal** (different portals serve different subsets).

**Check your indexer before migrating** with the compatibility report — it inspects a `ponder.config.ts` and, per source, verifies (a) the type is supported, (b) the **target portal** serves that chain's dataset (live `/datasets`), and (c) the network has the needed capability, against the [authoritative docs matrix](https://docs.sqd.dev/en/data/all-networks) (snapshotted in [`harness/compat/networks.json`](harness/compat/networks.json)):

```bash
PORTAL_API_KEY=… PORTAL_BASE=<your portal> \
  node --experimental-strip-types harness/compat/report.ts ./ponder.config.ts
# → per-source READY / NEEDS_TRACES / NO_DATASET, with the per-chain capability + block-range notes
```

For a clean event + factory indexer (Uniswap, Euler, …) it reports `READY NOW`. See [`MIGRATION.md`](MIGRATION.md) for the full adoption path (compat check → swap dependency → run → validate → roll back).

---

## Benchmarks

Resync **wall-clock** is the metric (events indexed end-to-end), not raw scan rate. Numbers are on a dedicated SQD Portal; the free public Portal shares a CU pool and is slower under concurrency. Deep analysis + the reproducible harness: [`harness/bench/BENCHMARKS.md`](harness/bench/BENCHMARKS.md).

**Single-chain — full Ethereum Euler history** (deploy → head, ~5M blocks, 466 vaults, 457,931 real economic events → pglite):

| Integration | Resync wall-clock | speedup |
|---|---|---|
| Stock RPC (1 stream / Ponder interval) | ~38 min | 1× |
| Read-ahead chunk buffer | ~17 min | ~2× |
| **+ parallel prefetch (the fork)** | **4m 45s** | **~8×** |

**Multi-chain — ethereum + base + arbitrum** concurrently, one `ponder start`: **~1.05M events in ~16 min, 0 errors.** The twist: Arbitrum (478M blocks, ~19× Ethereum) finished *first* — block-density auto-scaling collapses its ~436 fixed-size chunks to ~23, so it backfills faster than Base.

**Indexer bench base** (`harness/bench/`, real/representative indexers, instrumented): a synthetic all-five-source-types Uniswap app scales **834 → 1,812 events/s** as the range grows 10× (179k events in 99s), stable at 1.6 GB RSS / 0 errors.

**Correctness:** Portal-derived logs are **byte-identical** to JSON-RPC `eth_getLogs` (differential test, Base WETH Transfers, 380/380 logs, all fields); transforms are unit-tested against real captured Portal NDJSON.

---

## How it works

The whole change is one module (`portal/portal.ts`, the `HistoricalSync` implementation) plus a 4-line wiring patch that adds `portal?` to the chain config and routes to it when set.

- **Read-ahead chunk buffer.** Ponder feeds small intervals; Portal is latency-bound per request but has huge parallel bandwidth. So we fetch large aligned **chunks** and serve every interval from cache, prefetching the next *N* chunks concurrently so per-request latency overlaps instead of serializing. This decoupling is the biggest win.
- **Decoupled, correctness-safe factory discovery.** Child addresses are discovered per-chunk into a shared set; a data chunk fetches only once discovery is complete *through its own block range*, so out-of-order parallel fetches never miss a child event. Discovery is clamped to the factory's real start block.
- **Block-density auto-scaling.** Chunk size scales with the chain's head, keeping round-trips ~constant across chains (Arbitrum's 478M blocks don't mean 19× the requests).
- **Max field/row leverage.** Every row filter is pushed to Portal's native server-side filters (logs by address+topics, traces by callTo/callFrom/sighash, account txs by from/to); field projection requests exactly the columns the sync store persists — strictly tighter than Ponder's whole-block RPC fetch. The one exception is block-interval (Portal has no modulo filter → range-scan + client filter).
- **Dense-source memory safety.** Trace/block sources auto-cap the chunk grid (`PORTAL_TRACE_CHUNK_BLOCKS`, default 25k) so a busy contract can't OOM.
- **Finality-gap fallback.** Portal serves finalized data; if its head ever lags Ponder's target, intervals past it are auto-delegated to the stock RPC sync, so the backfill stays complete.
- **Resilience.** Socket errors and HTTP 503/529/429 (with `Retry-After`) are retried with backoff.

Tunables: `PORTAL_CHUNK_BLOCKS`, `PORTAL_READAHEAD`, `PORTAL_TRACE_CHUNK_BLOCKS`, `PORTAL_API_KEY`. Full seam write-up: [`portal/INTEGRATION.md`](portal/INTEGRATION.md).

---

## Examples

Each is a real indexer that runs end-to-end on `@subsquid/ponder` (backfill from Portal, realtime + `readContract` on `rpc`):

- [`examples/euler-subgraph/`](examples/euler-subgraph/) — **the Euler V2 subgraph ported to Ponder + Portal** (factory templates → `factory()`, subgraph eth_calls → `readContract` with the once-per-vault cache, Counter aggregation, APY derivation). A reusable *subgraph → Ponder* migration guide.
- [`examples/uniswap-portal/`](examples/uniswap-portal/) — exercises **all five source types** in one app (logs + receipts + traces + block-interval + accounts).
- [`examples/euler-multichain/`](examples/euler-multichain/) — multi-chain Euler factory indexer (the benchmark app).

---

## Versioning & releases

`@subsquid/ponder@X.Y.Z` **is** `ponder@X.Y.Z` + the Portal layer — the version is the match. The fork is *generated*, not hand-maintained: this repo holds only the Portal layer (`portal/`) + a per-version `wiring/<ver>.patch`; [`scripts/sync-upstream.sh`](scripts/sync-upstream.sh) clones `ponder@<ver>`, applies it, and builds. [`versions.json`](versions.json) is the supported-version matrix, and CI proves the seam against each. Releases publish via GitHub Actions + npm Trusted Publishing (OIDC, no token). Details: [`PUBLISHING.md`](PUBLISHING.md).

---

## Limitations

- **The fetch bottleneck moves to the indexing layer.** Once Portal makes the backfill fast, decode + store (pglite) dominates; use Postgres / parallel indexing for very large resyncs. Orthogonal to Portal.
- **Block-interval sources range-scan the whole window** (Portal has no modulo filter) — heavier than their matched-block count; pick a tight interval/range or expect more bytes.
- **`readContract`-heavy indexers stay RPC-bound** on those calls — Portal accelerates logs/state, not your handler's contract reads.
- **It's a fork, not a plugin** (ponder's internals aren't publicly exported; a thin plugin awaits an upstream `HistoricalSync` hook). The seam is small and stable, so the fork tracks ponder with a tiny patch — see [`PUBLISHING.md`](PUBLISHING.md#why-a-fork-not-a-thin-plugin).

---

## Repo layout

```
portal/                  the Portal layer (the entire diff vs upstream ponder)
  portal.ts                the Portal-backed HistoricalSync
  portal-transform.ts      pure NDJSON→Sync* transforms (+ tests, __fixtures__/)
  config.ts · INTEGRATION.md · wiring/<ver>.patch
scripts/sync-upstream.sh   generate @subsquid/ponder@<ver> from ponder@<ver> + the layer
versions.json              the @subsquid/ponder ↔ ponder version matrix
examples/                  runnable indexers (euler-subgraph · uniswap-portal · euler-multichain)
harness/bench/             benchmark base + instrumentation (BENCHMARKS · CANDIDATES · results)
harness/compat/            compatibility report (analyzer + docs-matrix snapshot + tests)
.github/workflows/         ci (version matrix) + release (OIDC publish)
MIGRATION.md · PUBLISHING.md
packages/portal-sync/      legacy standalone Portal engine (pre-fork; powers the stress/dashboard harness)
```
