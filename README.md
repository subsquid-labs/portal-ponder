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
| `createPortalHistoricalSync` | `portal/portal.ts` | Implements Ponder's `HistoricalSync` interface against Portal |
| Config + runtime wiring | `portal/wiring/0.16.6.patch` | adds `portal?` to `ChainConfig`/`Chain`; branches `createHistoricalSync` → `createPortalHistoricalSync` at `runtime/historical.ts` when `chain.portal` is set |
| Demo indexer | `examples/euler-multichain/` | real Euler `eVaultFactory`, multi-chain, untouched handler code |
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

- **It's a fork/patch of `@ponder/core`, not a plugin.** The `HistoricalSync` selection is hardcoded in the runtime; there's no DI hook. Distribute via `patch-package` or a thin published `@subsquid/ponder`. The seam itself (`HistoricalSync`, `SyncStore`, `Sync* = viem` types) is small and stable.
- **After the fetch is fast, the bottleneck moves to the indexing layer** (decode + store). The ~5-min single-chain number below is partly pglite write time; use Postgres / parallel indexing for very large resyncs. This is orthogonal to the Portal integration.
- **Absolute speedups depend on the Portal tier.** Numbers here are on a dedicated Portal; the free public Portal shares a CU pool and is much slower under concurrency.
- **Portal serves finalized data**; realtime/frontfill stays on `rpc` by design. If Portal's finalized head ever lags Ponder's target, intervals past it are **auto-delegated to the stock RPC historical sync** (so the backfill is still complete; `PORTAL_FINALIZED_HEAD` forces this for testing).
- **No JSON-RPC / transport "shim".** *Deliberate.* A `rpc: portalTransport(...)` shim sits at the per-request EIP-1193 layer — after the orchestrator has shattered ranges into non-contiguous point lookups, which is exactly what Portal is slow at. The whole speedup comes from integrating at the range-oriented `HistoricalSync` seam. So there's one delivery path (the native seam), not a "config-only but slow" fallback.

### Implemented

logs, log-factories, transactions, **receipts** (`includeTransactionReceipts`), **traces** (`includeCallTraces` + transfer filters), **block-interval** sources (`blocks: { … interval }`), **account transaction** sources (`accounts: { … }` — from/to).

**Max field/row leverage.** Every row filter is pushed to Portal's native filters server-side — logs by `address`+`topics`, traces by `callTo`/`callFrom`/`callSighash`, account txs by `from`/`to` — so the wire carries only matching rows (the one exception is block-interval, which has no modulo filter in Portal, so it range-scans + filters client-side). Field projection (`fields`) requests exactly the columns the sync store persists (`input`/`value`/`nonce`/`gas`/… are NOT NULL there) and no more — strictly tighter than Ponder's RPC path, which pulls whole blocks. Receipt fields are added only when a source sets `includeTransactionReceipts`. Transforms handle Portal's split encoding (decimal `status`/`type`, hex gas/value) and Parity→callTracer (callType from `action.callType`; CREATE/CREATE2 indistinguishable, but Ponder ignores trace `type` in matching). Covered by unit tests against real captured fixtures + an end-to-end Uniswap run (receipts on the USDC/WETH V3 pool, traces on the V2 Router). Trace sources **auto-cap the chunk grid** to `PORTAL_TRACE_CHUNK_BLOCKS` (default 25k) — traces are ~100× denser than logs, so a wide chunk over a busy contract would otherwise OOM (verified: the Uniswap e2e completes under a 2 GB heap with the default 500k grid auto-capped).

### Not implemented / TODO

- **Per-network capability** (traces/stateDiffs + block-range caveats) is checked by the compat report against the [authoritative docs matrix](https://docs.sqd.dev/en/data/all-networks) (snapshotted in `harness/compat/networks.json`); dataset **existence is per-portal** (checked live against the target portal's `/datasets`). A trace source on a chain without traces, or a block-range caveat (e.g. Optimism pre-Bedrock), is surfaced.
- **Chunk sizing by data volume** (vs the block-count heuristic); **CU-budget-aware** prefetch depth.
- **Upstreaming**: a documented `HistoricalSync` hook in Ponder so this isn't a fork.

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

Reproduce: `examples/euler-multichain/` is the demo indexer; `harness/` has the stress test + dashboard + differential. Set `PORTAL_API_KEY` and the per-chain `PONDER_RPC_URL_*` env vars (Portal handles backfill; RPC is finality/realtime only).

---

## 4. Tests & contributing

**Tests — honest state:**
- **Transform unit tests** (`portal/portal-transform.test.ts`, 12 tests) run over **real Portal NDJSON captured at eth block 21M** (in `__fixtures__/`) and pin every flagged type mismatch: decimal `status`/`type` → hex, gas/value stay hex, trace `callType` read from `action.callType` (→ DELEGATECALL), staticcall `value:null`, CREATE `init`/`code`, suicide → SELFDESTRUCT, stateDiff `prev:null` ⟺ `+`, DFS trace ordering, the trace-chunk memory cap, and the finality-gap decision.
- **Analyzer tests** (`harness/compat/analyze.test.ts`, 6 tests): docs-capability gate (a chain with `traces:false` flags trace sources; Arbitrum's traces are READY because the docs say so), per-portal existence (a portal that doesn't serve the dataset → `NO_DATASET`), and block-range notes (Optimism Bedrock surfaced).
- **Integration regression** (`portal.test.ts`): a fixture Portal block → asserts `event.transaction` is populated (runs against a local HTTP server, no chain; isolate via `vite.portal.config.ts`, no Foundry `globalSetup`).
- **End-to-end**: `integration/uniswap-portal-app/` backfills all five source types from Portal in one run — receipts (V3 pool swaps carry `receiptGasUsed`/`status`), traces (V2 Router calls reconstructed from call-traces), block-interval (a tick every 1000th block: exactly 22.200M, 22.201M …), and account transactions (txs to WETH, all verified `to == WETH`); plus the differential harness + multi-chain Euler runs.
- **Still wanted:** unit tests for the filter→Portal-query builder, chunk-index math, auto-scaling, and discovery/data ordering. Every bug fix ships with a fixture + regression (the convention).

**Where to pick up (with or without an agent):**
1. `portal/portal.ts` is the whole core — start there. `wiring/<ver>.patch` shows the 4-file integration into `@ponder/core`.
2. Highest-value next work: **chunk-sizing by data volume** (vs block-count) + CU-budget-aware prefetch; then upstreaming the `HistoricalSync` hook.
3. `packages/portal-sync/` + `harness/` let you exercise Portal directly (no Ponder build) — fastest loop for transform/perf work.
4. To run the real thing: clone `ponder-sh/ponder`, apply `wiring/<ver>.patch`, drop in `portal.ts`, build core, point a project's `ponder.config.ts` at a `portal:` dataset.

Contributions welcome — it's a prototype with a clear seam and a clear TODO list. Keep changes honest: measure resync wall-clock (not scan rate), and add a regression for every fix.

---

## How it ships — `@subsquid/ponder`, a drop-in fork of `ponder`

A client replaces `ponder` with **`@subsquid/ponder`** (same `ponder` bin) and adds one `portal:` line
per chain; the historical backfill goes through Portal, realtime stays on `rpc`. The fork **version
mirrors ponder exactly** (`@subsquid/ponder@X.Y.Z` = `ponder@X.Y.Z` + the Portal layer), and it's
*generated*, not hand-maintained — see [`PUBLISHING.md`](PUBLISHING.md) + [`versions.json`](versions.json).

## Layout

```
README / MIGRATION / PUBLISHING.md   overview · client adoption · how the fork is built+versioned+published
versions.json            the @subsquid/ponder ↔ ponder version matrix (source of truth for CI + releases)
portal/                  THE PORTAL LAYER — the entire diff vs upstream ponder:
  portal.ts                the Portal-backed HistoricalSync (the speedup)
  portal-transform.ts      pure NDJSON→Sync* transforms  (+ portal-transform.test.ts, __fixtures__/)
  config.ts                withPortal() helper · INTEGRATION.md  the seam write-up
  wiring/<ver>.patch       the 4 touch-points, one patch per ponder version
scripts/sync-upstream.sh   clone ponder@<ver> + apply the layer + build → @subsquid/ponder@<ver>
.github/workflows/ci.yml   version matrix: apply the layer + run Portal tests on each supported ponder version
examples/                  runnable indexers (uniswap-portal · euler-multichain · the Euler subgraph port)
harness/bench/             benchmark base + instrumentation (BENCHMARKS · CANDIDATES · results)
harness/compat/            compatibility report (analyzer + networks.json docs snapshot + tests)
packages/portal-sync/      legacy standalone Portal engine (pre-fork; powers the stress/dashboard harness)
```

**Adoption:** start with the [compatibility report](harness/compat/report.ts) — per source it checks that the **target portal** serves the chain's dataset (live `/datasets`, since different portals serve different subsets) and that the network has the needed capabilities (traces) per the [authoritative docs matrix](https://docs.sqd.dev/en/data/all-networks) — then [`MIGRATION.md`](MIGRATION.md). We deliberately don't ship a slow JSON-RPC transport shim (see the trade-off note above).
