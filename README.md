# portal-ponder

A fork of [Ponder](https://github.com/ponder-sh/ponder)'s historical sync that backfills from **[SQD Portal](https://sqd.dev)** (a columnar, range-scan data lake) while realtime/frontfill stays on JSON-RPC. The switch is **one config line** — `portal: "<dataset-url>"` per chain — and **indexer handler code is unchanged**.

> Status: working prototype. The Portal-backed `HistoricalSync` is implemented, integrated into a local `@ponder/core` build, and runs real multi-chain Euler backfills end-to-end. Not production-hardened (see [Trade-offs](#trade-offs) and [TODO](#not-implemented--todo)).

---

## 1. Overview — what & why

Ponder's historical sync is built on JSON-RPC. To backfill, it fans out **one `eth_getLogs` per event-topic per contract group**, then **one `eth_getBlockByNumber` (+receipts/traces) per matched block**. That pattern is pessimal for two reasons:

- **It's a point-lookup workload on a range-scan-shaped problem.** Large/factory contracts (e.g. **Euler** — thousands of vaults discovered at runtime) explode the fan-out: a single 1000-block window costs ~17 getLogs; a full backfill is hundreds of thousands of tiny, non-contiguous requests. SQD Portal answers a whole `[from,to]` range in one streamed columnar scan — but only if you *let it*.
- **Ponder's RPC load-balancer actively penalizes Portal.** Its adaptive client demotes high-latency backends and, on a 429/timeout, deactivates the endpoint with exponential backoff. Portal's range-scan latency (seconds) reads as a "failing node," so a naive `rpc: <portal-as-json-rpc>` setup spirals into stalls. (This is why prior RPC-emulation shims underperform: they sit at the EIP-1193 `request()` layer, where the orchestrator has already shattered the range into point lookups.)

**This fork integrates one level up — at Ponder's range-oriented `HistoricalSync` seam** (`syncBlockRangeData`/`syncBlockData`, both interval-scoped). One interval becomes one self-paced Portal stream; nothing touches the RPC penalty machinery; realtime keeps using `rpc`. Result: backfills that are **~8× faster** on a real Euler indexer, and that scale to chains (Arbitrum) where RPC backfill is impractical.

---

## 2. What's implemented & how

The entire core change is **~310 lines in one new file + 13 lines of wiring**:

| Piece | Where | Notes |
|---|---|---|
| `createPortalHistoricalSync` | `integration/core-fork/portal.ts` | Implements Ponder's `HistoricalSync` interface against Portal |
| Config + runtime wiring | `integration/core-fork/wiring.patch` | adds `portal?` to `ChainConfig`/`Chain`; branches `createHistoricalSync` → `createPortalHistoricalSync` at `runtime/historical.ts` when `chain.portal` is set |
| Demo indexer | `integration/euler-portal-app/` | real Euler `eVaultFactory`, multi-chain, untouched handler code |
| Standalone engine + bench | `packages/portal-sync/`, `harness/` | a self-contained Portal client/transform/metrics used to validate & benchmark Portal directly |

**How `portal.ts` works (the interesting bits):**

- **Read-ahead chunk buffer.** Ponder feeds small intervals (~20–100k blocks); Portal is *latency-bound per request* but has huge *parallel bandwidth*. So we fetch large aligned **chunks** and serve every Ponder interval from cache. The first interval in a chunk triggers a fetch; the rest are instant cache hits. This decouples Portal's fetch granularity from Ponder's interval granularity — the single biggest win (intervals grow to Ponder's 100k max, ~ms cache hits).
- **Parallel prefetch (depth N).** The next N chunks are fetched concurrently so Portal's per-request latency *overlaps* instead of serializing. Throughput climbs once the pipeline fills.
- **Decoupled, correctness-safe factory discovery.** Factory children are discovered per-chunk into a shared set; a data chunk only fetches once discovery is complete *through its own block range* (`ensureDiscoveredThrough`). So out-of-order parallel data fetches never miss a child event. Discovery is clamped to the factory's real start block (not 0).
- **Block-density auto-scaling.** Chunk size scales with the chain's head: Arbitrum (~478M blocks ≈ 19× Ethereum) gets ~19× bigger block-chunks, keeping the number of round-trips ~constant across chains. CU is charged per *Portal data-chunk* (data-density based), so bigger *block*-chunks don't cost more CU — they just cut round-trips.
- **Network-error resilience.** Under parallel load, `SocketError: other side closed` / `ECONNRESET` are routine; `stream()` retries them (and HTTP 503/529/429 with `Retry-After`) with backoff.
- **Include-driven field projection.** Block fields = required-NOT-NULL columns ∪ only the nullable fields a filter's `include` references; discovery projects only what child-extraction reads. Minimises overfetch.
- **Transactions** are pulled via Portal's `transaction` relation and inserted, so `event.transaction` is populated (Ponder's event profiler requires it).

### Trade-offs

- **It's a fork/patch of `@ponder/core`, not a plugin.** The `HistoricalSync` selection is hardcoded in the runtime; there's no DI hook. Distribute via `patch-package` or a thin published `@your-org/ponder-core`. The seam itself (`HistoricalSync`, `SyncStore`, `Sync* = viem` types) is small and stable.
- **After the fetch is fast, the bottleneck moves to the indexing layer** (decode + store). The ~5-min single-chain number below is partly pglite write time; use Postgres / parallel indexing for very large resyncs. This is orthogonal to the Portal integration.
- **Absolute speedups depend on the Portal tier.** Numbers here are on a dedicated Portal; the free public Portal shares a CU pool and is much slower under concurrency.
- **Portal serves finalized data**; realtime/frontfill stays on `rpc` by design.

### Not implemented / TODO

Honest list — these are where a contributor would start:

- **Receipts** (`eth_getBlockReceipts` equivalent): the `transaction`/field plumbing is there, but receipt fields aren't wired to `insertTransactionReceipts` for `includeTransactionReceipts` sources.
- **Traces** (`debug_traceBlock` / callTracer): Portal serves Parity-style traces; the Parity→callTracer transform is prototyped in `packages/portal-sync/src/transform.ts` and verified against mainnet block 21M, but **not** wired into the core module. Note: Portal can't distinguish CREATE vs CREATE2 (Ponder ignores trace `type` in matching, so indexing is unaffected).
- **Block / transaction / transfer filters**: only **log** filters and **log factories** are handled in `portal.ts` today.
- **Finality-gap RPC fallback**: if Portal's finalized head lags Ponder's target finalized block, the thin gap should fall back to the stock RPC sync. Currently assumes Portal reaches the target.
- **Chunk sizing by data volume** rather than the current block-count heuristic; **CU-budget-aware** prefetch depth.
- **Upstreaming**: ideally `HistoricalSync` becomes a documented injection point in Ponder so this isn't a fork.

---

## 3. Benchmarks

All against a **dedicated SQD Portal** (`cu_per_epoch` 10M / 1200s ≈ 8,333 CU/s, ~2,090 workers), indexing **real Euler** economic events (Deposit/Withdraw/Borrow/Repay/Liquidate) discovered via the real `eVaultFactory`, into pglite. Numbers are noisy (shared portal load) — treat as order-of-magnitude.

**Single-chain — full Ethereum Euler history** (deploy 20,429,973 → head, ~5M blocks, 466 vaults, **457,931 events**):

| Integration | Resync wall-clock | vs naive |
|---|---|---|
| Naive (1 stream / Ponder interval) | ~38 min | 1× |
| Read-ahead buffer (depth 1) | ~17 min | ~2× |
| **Read-ahead + parallel prefetch (depth 6)** | **4m 45s** | **~8×** |

**Multi-chain — Ethereum + Base + Arbitrum concurrently** (full Euler history per chain, one `ponder start`, ~16 min total wall-clock, **0 errors**):

| Chain | Blocks (head) | Events indexed | Finished |
|---|---|---|---|
| ethereum | 25.4M | 457,931 | ~10 min |
| base | 48.0M | 346,198 | ~16 min (long pole) |
| arbitrum | **478.6M** (~19× ETH) | 244,645 | **~7.5 min — *first*** |
| **total** | | **~1.05M events** | **~16 min wall-clock** |

The headline result is the twist: **Arbitrum — the chain we expected to dominate — finished *first*.** With a fixed 500k-block chunk it would need ~436 chunks (≈40× more round-trips than ETH) and crawl; block-density auto-scaling collapses that to ~23 chunks, so its 478M-block range backfills faster than Base's. (Base became the long pole, partly shared-portal load during this run.)

**Raw Portal scan rate** (stress harness, ballast not indexing): peak ~10M blocks/s, ~3.5M sustained at concurrency 30, 0 errors — included for context but *not* the metric that matters; resync wall-clock is.

**Correctness:** Portal-derived logs are byte-identical to JSON-RPC `eth_getLogs` (differential test on Base WETH Transfers, 380/380 logs, all fields).

Reproduce: `integration/euler-portal-app/` is the demo indexer; `harness/` has the stress test + dashboard + differential. Set `PORTAL_API_KEY` and the per-chain `PONDER_RPC_URL_*` env vars (Portal handles backfill; RPC is finality/realtime only).

---

## 4. Tests & contributing

**Tests — honest state:**
- One **fixture-based regression test**: `integration/core-fork/portal.test.ts` pins the "matched log's transaction is fetched & inserted" bug (a fixture Portal NDJSON block → asserts `event.transaction` is populated). Runs against a local HTTP server, no chain needed.
  - Ponder's default test suite has an anvil/Foundry global setup; run this test in isolation with a minimal config (`vite.portal.config.ts` in the patched core, no `globalSetup`).
- **Coverage is thin.** Correctness today leans on the differential harness + the end-to-end Euler runs, not unit tests. **Wanted:** unit tests for the transforms (NDJSON→`Sync*`), the filter→Portal-query builder, chunk-index math, auto-scaling, and the discovery/data ordering. Every bug fix should ship with a fixture + regression (this is the convention).

**Where to pick up (with or without an agent):**
1. `integration/core-fork/portal.ts` is the whole core — start there. `wiring.patch` shows the 4-file integration into `@ponder/core`.
2. Highest-value next work: **receipts**, then **traces** (transform already prototyped), then the **finality-gap fallback**.
3. `packages/portal-sync/` + `harness/` let you exercise Portal directly (no Ponder build) — fastest loop for transform/perf work.
4. To run the real thing: clone `ponder-sh/ponder`, apply `wiring.patch`, drop in `portal.ts`, build core, point a project's `ponder.config.ts` at a `portal:` dataset.

Contributions welcome — it's a prototype with a clear seam and a clear TODO list. Keep changes honest: measure resync wall-clock (not scan rate), and add a regression for every fix.

---

## Layout

```
MIGRATION.md             how a client adopts this (parity → backfill-only → native → managed)
integration/core-fork/   portal.ts (the native fork), wiring.patch, portal.test.ts   ← full perf
integration/euler-portal-app/   real multi-chain Euler demo indexer
integration/ponder-core.md      step-by-step integration guide
packages/portal-sync/
  transport.ts           portalTransport() — config-only delivery (any version, incl 0.15.x)
  config.ts              withPortal() + registry — native-injection glue
  {portal-client,query,transform,metrics}.ts   standalone Portal engine
harness/compat/          compatibility-report tool (report.ts) + analyzer + tests
harness/                 stress test + live dashboard, multichain, differential, gates
```

**Adoption:** start with the [compatibility report](harness/compat/report.ts) (does Portal serve your indexer's data?), then [`MIGRATION.md`](MIGRATION.md). The config-only `portalTransport` works on any Ponder version including Euler's 0.15.x.
