# How `@subsquid/ponder` works

`@subsquid/ponder` changes where Ponder gets its data. By default it moves only the **historical backfill** from per-chain JSON-RPC to the [SQD Portal](https://sqd.dev/portal/); realtime, reorg handling, handlers, schema, and the `ponder` CLI are untouched, so the switch is one line of config and trivial to roll back. Two things follow, and both matter to a pipeline that leans on RPC today. The backfill is the overwhelming majority of an indexer's RPC calls, so moving it to the Portal **cuts the RPC budget to almost nothing** — and the resync runs **several times faster**. Realtime can move to the Portal too, an opt-in mode ([below](#realtime-and-the-path-to-zero-rpc)) that takes RPC out of the sync path entirely — and for the many indexers that make no on-chain state calls, out of the pipeline altogether.

The speed is worth understanding, because it comes from two layers, not one:

- **The Portal makes fast _possible_.** The [SQD Portal](https://sqd.dev/portal/) is an HTTP access layer to the SQD Network. It answers an arbitrary `[from, to]` block range as a single streamed pass, so the backfill stops being a storm of point lookups.
- **The fork's engineering is what _extracts_ that speed.** Pointing Ponder straight at a fast endpoint is slow, or runs out of memory. Most of this document is the mechanics that turn a fast endpoint into a fast indexer: a chunk cache and read-ahead that make the network disappear as a constraint, a shared controller that saturates the Portal at a fixed memory ceiling across every chain at once, and a factory discovery that finds thousands of child contracts over ranges instead of one at a time.

This is a design document. For the exact tuning knobs, defaults, and metrics fields, see the operational reference, [`portal/INTEGRATION.md`](portal/INTEGRATION.md); for the measured numbers, [`harness/euler-multichain/REPORT.md`](harness/euler-multichain/REPORT.md). It's beta.

## The problem: an RPC backfill is a stream of point lookups

JSON-RPC was built to serve transactions and single-object reads, not to scan history. So a backfill over RPC decomposes into small, serial round-trips:

- One `eth_getLogs` **per event topic**, paged across the range in windows the provider will accept.
- One `eth_getBlockByNumber` **per matched block**, to attach the header and timestamp every event needs.
- More per-item calls for transactions, receipts, and traces when a source needs them.

Each call is a round-trip that re-establishes almost no context, and the total scales with the data. A single protocol's Ethereum history — 885,893 matched logs across ~252,000 distinct blocks, deploy to head (the committed `F-full` cell, VALIDATION.md §3.2) — needs a header fetch for each of those blocks, so the header fetches _alone_ are hundreds of thousands of serial requests. A **factory** makes it worse in a way that matters: you can't query a child contract's logs until you've discovered the child, and children are created continuously across history, so you first scan for addresses and then fan out per-child log queries. This is the shape RPC forces — many small requests, in sequence, mostly waiting.

## The Portal model: a range as one streamed pass

The Portal inverts that shape. You POST a query that names a `[from, to]` block range, the row filters (addresses, topics, call targets, from/to), and the exact columns you want back. It replies with **one streamed response** — newline-delimited JSON, one line per block, already filtered and projected on the server. You drain it line by line.

Four properties fall out of that, and each one is a lever the fork pulls:

1. **Round-trips collapse.** One request covers arbitrarily many blocks. Hundreds of thousands of point lookups become a handful of large streamed reads.
2. **A single scan is constant-memory.** You never materialize the range; you consume it as it arrives.
3. **Backfill is reorg-free.** The Portal serves only *finalized* data, so historical rows never need to be walked back.
4. **One request is already parallel.** The Portal can fan a single stream out across many internal chunk workers, at no extra metered cost — the meter counts data touched, not the width of the range.

That is Layer 1. It makes a fast backfill *possible*. It does not, by itself, make Ponder fast — a literal "one interval, one request" port would leave almost all of that speed on the table. The rest of this document is Layer 2: how the fork actually extracts it.

## Where the fork plugs in: the historical-sync seam

Ponder's engine already drives the backfill as a sequence of block **intervals**, each handed to an implementation of the internal `HistoricalSync` interface — two methods:

- `syncBlockRangeData({ interval, … })` — fetch and persist what an interval needs; return its matched logs.
- `syncBlockData({ interval, … })` — finalize the interval; return the highest block that carried data.

An interval is a `[from, to]` range. A range is exactly what the Portal answers in one pass. The impedance match is close to perfect, and it's the whole reason this integration is small. The fork implements that same interface in [`portal/portal.ts`](portal/portal.ts) (`createPortalHistoricalSync`) and Ponder selects it per chain with a one-line branch in `runtime/historical.ts`:

```ts
const historicalSync = params.chain.portal
  ? createPortalHistoricalSync(params)   // stream the interval from the Portal
  : createHistoricalSync(params);        // stock RPC path, unchanged
```

The Portal sync receives the **same** `params` as the stock one — `chain`, `rpc`, `childAddresses`, and the chain's full `eventCallbacks` — and reads Ponder's own filters and factory maps directly. Nothing about handlers, schema, or config is translated.

**Why a fork and not a plugin.** That seam is an internal interface, not part of Ponder's public API; there is no extension point to register an alternative historical sync. So the integration is a fork — but a deliberately thin one. The repository carries only the `portal/` layer plus a short wiring patch ([`portal/wiring/`](portal/wiring/)) that adds a `portal?` field to the chain config, threads it through `buildConfig` and the internal `Chain` type, and swaps the one constructor above. The published package is *generated* from upstream Ponder plus that patch, so it tracks Ponder closely — the seam has held identical across 0.15.17–0.17.0 ([`versions.json`](versions.json)).

## Making the network disappear: chunking and read-ahead

**The naive integration is barely faster than RPC.** Ponder's intervals are small relative to a full history, and a Portal request is latency-bound per call. Map one interval to one request and you pay the round-trip on every small interval and use almost none of the Portal's bandwidth — you've swapped RPC's point lookups for slightly larger ones.

So the fork decouples *fetch* granularity from Ponder's *interval* granularity. It fetches **large aligned chunks** — `PORTAL_CHUNK_BLOCKS`, default 500,000 blocks — and serves every interval from an in-memory cache keyed by chunk index. Two refinements keep that honest across very different chains:

- **Density scaling.** Chunk width scales up with a chain's block count (roughly `head ÷ 25,000,000`, capped at 25M blocks), so a high-block chain like Arbitrum — about 19× Ethereum's blocks — takes proportionally *fewer* round-trips instead of 19× as many 500k chunks. Because the Portal meters by data touched, not block width, wider chunks are free.
- **Density caps.** Trace and block-interval sources return far denser data, so their chunks are capped to a trace-safe width (default 2,000 blocks) — otherwise a single chunk could try to buffer a busy contract's entire trace set.

Then chunks are **prefetched**. As an interval finishes, the fork issues the next chunks concurrently, so the Portal's per-request latency overlaps indexing instead of stalling in front of it. Chunks behind the current interval are evicted as it advances, so memory tracks a sliding window, not the whole history.

The goal of all of this is a single sentence: **keep each chain's read-ahead buffer full, so that indexing is bottlenecked by local decode and database writes and never by awaiting a fetch.** When it works, fetch is always ahead — the network has effectively disappeared as a constraint. Whether it works is directly observable (see *Where the ceiling is*).

## Saturating the Portal at a fixed memory ceiling: the shared controller

Read-ahead raises an obvious question on a multichain app: how deep, and across how many chains?

**The naive answer breaks.** Give each of 15 chains its own private read-ahead and you get one of two failures. Either the buffers are unbounded and the process **runs out of memory**, or 15 chains each fan out concurrently against the one shared endpoint and the app **rate-limits itself** into 429s. A per-chain design fights itself.

The fix follows from a fact the naive design ignores: every chain streams from the **same** Portal endpoint, so request concurrency and buffered memory are *one shared budget*, not fifteen private ones. The fork routes all chains through a single shared, lazily-created controller (`portal-gate.ts` — a pure AIMD/row-budget reducer behind a process-shared shell) with two self-tuning, zero-config controls.

**Adaptive concurrency (AIMD).** The client cannot know the endpoint's live capacity — it varies by Portal, by load, and over time — so the controller *discovers* it. It starts at 16 in-flight requests, adds 2 after every 8 clean responses up to a ceiling of 48, and **halves** — down to a floor of 8 — on any back-pressure signal: an HTTP 429, any 5xx, a 409 on the finalized stream, a `retry-after`, or a dropped or timed-out connection. This is the same additive-increase / multiplicative-decrease discipline as Ponder's native RPC limiter, but **global**, because the endpoint is shared. Transient failures retry with exponential back-off; only a genuinely unrecoverable response fails the run.

**Memory backpressure.** Concurrency is bounded above; memory is bounded by a **row budget** — `PORTAL_MAX_ROWS_IN_MEM`, default 250,000 buffered records, roughly 1.5–2.5 GB once Ponder's derived copies are counted. Read-ahead prefetches until the shared buffer reaches the budget, then pauses; as indexing consumes chunks they are evicted and the buffer refills. The cap is on *total* buffered rows, so the fork **saturates the Portal at a fixed memory ceiling regardless of how many chains stream at once** — fifteen chains run in the same envelope as one. Read-ahead always keeps at least the immediate next chunk ready per chain, and reaches deeper only while the shared buffer is below budget: a fast Portal keeps every chain fed, and a constrained box never blows up.

These two controls are the difference between "a fast endpoint" and "a fast indexer that stays up." The exact knobs and defaults live in [`portal/INTEGRATION.md`](portal/INTEGRATION.md); they are not something you should need to touch.

## Discovering thousands of factory children over ranges

Factories are where RPC round-trips explode, and where a careless Portal port would explode too. A factory emits child contracts continuously across history; you cannot fetch a child's events until you know the child exists. Scan per child, or scan `[0, head]` as one slow sequential stream, and you've rebuilt the RPC problem.

The fork's move is to **decouple the discovery timeline from the data timeline**.

- **Discovery** is one wide scan of the factory's creation event (an EVault factory's `ProxyCreated`, say) over `[deploy, head]`, pinned to the factory's *real deploy block* rather than block 0. The Portal serializes a single stream in block order, so the scan is split into disjoint block windows (default 8) issued **concurrently** — and each window additionally fans out across up to 100 Portal-side chunk workers, at no extra cost. A scan that would be one slow front-to-back pass instead runs in parallel across the whole span.
- **Data** for a chunk is fetched only once discovery has completed *through that chunk's block range*. Read-ahead pulls chunks out of order, but no child event is ever missed, because the child set is provably complete up to each chunk's blocks before that chunk streams. Discovery reuses Ponder's own factory logic (`isLogFactoryMatched`, `getChildAddress`), so the child set is identical to what stock Ponder would derive.

The payoff is the clever part. Discovered addresses are pushed into the Portal's **server-side** log filter (`address` + `topic0..3`) and batched, so thousands of children resolve to a handful of streamed reads rather than thousands of per-address lookups. One sharp edge is worth showing, because handling it is what keeps the approach viable: Ponder emits **one filter per event**, so a contract with an N-event interface repeats the same (possibly large) child-address list N times, and concatenated those requests can exceed the Portal's 256 KB request-body cap. The fork **merges** filters that share an address set, unioning their `topic0` — an identical result set in an N-times-smaller body. Even Ethereum's ~900 Euler vaults across the full 24-event EVault interface compile to about 41 KB, comfortably under the cap.

## Realtime, and the path to zero RPC

Because the Portal serves only finalized data, the split is clean: **historical owns `[start, finalized]`; realtime owns `(finalized, tip]`.** Which side of that line your RPC serves is what decides the budget.

**By default, realtime stays on your RPC** — Ponder's own realtime sync and reorg handling, byte-for-byte the stock path. But once the backfill has moved to the Portal, serving the tip is *all* your RPC does (that, plus chain setup and `readContract`): a handful of requests per new block, instead of the millions a historical resync demanded. That's why the default alone already takes an RPC-heavy pipeline to a near-zero budget — the expensive part is gone and what remains is cheap. The Portal is never in the realtime path unless you opt in.

**The finality-gap fallback** covers the one seam where the two could disagree. The Portal's finalized head can briefly lag Ponder's target finalized block, so any interval that reaches past the Portal head — or that runs while the head probe is failing — is delegated *whole* to the stock RPC `createHistoricalSync`, so the tip is never silently under-served. (`PORTAL_FINALIZED_HEAD` pins the head for tests and ops.)

**Portal-native realtime removes RPC from the sync path entirely.** Set `PORTAL_REALTIME=stream` and the tip streams from the Portal's fork-aware `/stream` (hot blocks) too, so neither backfill nor realtime touches an RPC. `clampFinalizedToPortalHead` lowers Ponder's finalized boundary to the Portal head, so historical stops exactly there and realtime streams `(portal-head, tip]`; reorgs are reconciled from the stream's parent-hash chain and surfaced as Ponder's own `reorg` / `finalize` events, so handlers and checkpointing are unchanged. It's opt-in and experimental today — the RPC path is the default and untouched when the flag is off — but it's a first-class direction, not an afterthought.

One honest caveat sets the ceiling: handlers that read on-chain state with `readContract` still call your RPC for those reads. Plenty of indexers make none — pure event and factory indexers never touch state — and for them all-Portal is literally that: zero RPC, one data source. For the rest, the entire backfill-and-sync firehose is off RPC and only a thin slice of state calls remains, so the budget-and-speed win holds either way; state reads just keep a small, bounded RPC footprint.

## Why it's fast — and where the ceiling honestly is

Put the two layers together — a range as one streamed pass, fanned out in parallel and prefetched ahead of indexing — and a single-chain backfill of full Ethereum Euler history (`[20529207, 25436954]`, 4,907,748 blocks, 885,893 logs) finishes in **1819 s (~30 min)** on the Portal, against **6543 s (~109 min)** over stock RPC on the same host: **3.6×** (the committed `F-full` cell, VALIDATION.md §3.2). Both legs are fetch-bound, and the sync-store rows come out byte-identical.

The more interesting result is what happens next, because it names the real ceiling. **Once fetch is fast, the bottleneck moves entirely into Ponder, which indexes on a single thread.** The reference run makes this concrete: one app indexing all 15 Portal-supported Euler V2 chains, full history, **28,405,932 events**, in **51m 47s** (the reproducible deterministic run, 2026-07-06), on an indexer capped at 16 GB and 2 cores (about one core of real work). During that run:

- The node process pinned **one core at ~92%** while roughly **93 of the box's 96 cores sat idle**.
- **Postgres was faster than the indexer** — its backends idled in `ClientRead`, waiting for data.
- **The Portal outran both** — its fetch queue drained to idle with the buffer full, while a single event loop worked through the tail.

So more RAM or cores don't buy wall-time here; they're already idle. A counter-intuitive A/B from the earlier 2026-07-01 runs drives it home: a **modest, well-tuned** configuration (16 GB / 2 cores, fixed chunks, tuned Postgres) beat an **over-provisioned** one (32 GB heap, default Postgres) on the *identical* 28.4M events — 44m 55s vs 67m 10s, 9.2 GB vs 19.0 GB peak, 10,513 vs 7,024 events/s. The lesson: don't over-provision the indexer; right-size it and tune the database. The lever for going faster is **sharding chains across processes** — running the event-heaviest chain on its own to add an indexing thread — not a bigger machine. Full write-up: [`harness/euler-multichain/REPORT.md`](harness/euler-multichain/REPORT.md).

## Correctness: byte-identical to `eth_getLogs`

None of this is worth anything if the data differs from what an archive node would return. It doesn't. Logs (by address and topics) and account transactions (by from / to) are pushed to the Portal's native server-side filters, the field projection requests exactly the columns Ponder's sync store persists, and the Portal's number/quantity encodings are normalized so a stored block, transaction, receipt, or trace is indistinguishable from the RPC path (which always carries `nonce`, `mixHash`, `sha3Uncles`, `totalDifficulty`). Two row filters are applied client-side, and the first is the sharpest correctness detail in the system. **Traces** are fetched *whole* — the request carries only the parent-transaction relation, no trace filter — and matched after they arrive, because Ponder numbers each trace by its pre-order (depth-first) rank within the transaction's *entire* call tree. A server-side trace filter would return only the matched subset, so that rank would be computed over a partial tree and come out wrong; fetching every trace and filtering after ranking is what keeps `trace_index` byte-identical to the RPC path. The second is the **block-interval** (offset/modulo) test, which the Portal has no native equivalent for.

This is proven, not asserted. The differential harness ([`harness/diff`](harness/diff)) indexes the same bounded range **twice on this fork** — once with `portal:` set (the Portal path) and once without it (the stock RPC path; the only difference is the backfill source) — into two separate `ponder_sync` stores, then diffs every row of `logs`, `transactions`, `transaction_receipts`, and `traces`. Exit `0` means identical. The 15-chain run adds two independent lines of evidence: **60 of 60** sampled windows matched the Portal ground truth exactly, and two runs with *different* chunking, heap, and Postgres configs produced the **identical** 28,405,932-event total — a data-loss bug would diverge, and it doesn't.

## Where the code lives

The layer is organised around explicit, provable **invariants** — a functional core (pure, property-tested) behind an imperative shell. See [`portal/INVARIANTS.md`](portal/INVARIANTS.md) for the catalog (INV-1…INV-18) that ties doc ⟷ code ⟷ test together.

| File | What it holds |
|---|---|
| [`portal/portal.ts`](portal/portal.ts) | `createPortalHistoricalSync` — the orchestration shell: wires the modules below, owns the chunk cache, stash, delegation, and the seam methods with invariant checks. |
| [`portal/portal-config.ts`](portal/portal-config.ts) | Parse + validate all `PORTAL_*` env once into a frozen `PortalConfig` (INV-14). |
| [`portal/portal-errors.ts`](portal/portal-errors.ts), [`portal/portal-invariant.ts`](portal/portal-invariant.ts) | The typed error taxonomy and the runtime `invariant()` checks. |
| [`portal/portal-client.ts`](portal/portal-client.ts) | The Portal HTTP shell: `finalizedHead()`, `stream()`, error mapping, retry, field degradation, the shared `ndjsonLines()`. |
| [`portal/portal-gate.ts`](portal/portal-gate.ts) | The shared AIMD concurrency + row-budget controller as a pure reducer + async shell. |
| [`portal/portal-filters.ts`](portal/portal-filters.ts) | The frozen per-chain fetch-spec — the single source of log/tx/trace requests + field projections, shared with realtime. |
| [`portal/portal-chunks.ts`](portal/portal-chunks.ts) | Pure chunk-grid math (tiling, density scaling, read-ahead / eviction plans). |
| [`portal/portal-discovery.ts`](portal/portal-discovery.ts) | The factory-child discovery state machine (advance-on-success). |
| [`portal/portal-assemble.ts`](portal/portal-assemble.ts) | The pure range assembler (interval exactness, full-tree trace ranking, `closest`). |
| [`portal/portal-transform.ts`](portal/portal-transform.ts) | Pure Portal-NDJSON → Ponder `Sync*` transforms, unit-tested against captured fixtures. |
| [`portal/portal-metrics.ts`](portal/portal-metrics.ts) | The per-chain stats shape, metrics-file writer, and gate-log ticker. |
| [`portal/portal-realtime.ts`](portal/portal-realtime.ts), [`portal/portal-realtime-wire.ts`](portal/portal-realtime-wire.ts) | The opt-in Portal `/stream` realtime path and its adapter into Ponder's realtime runtime. |
| [`portal/wiring/`](portal/wiring/) | The short per-version upstream patch — the `portal` config field and the one-line selector. |

For the invariant catalog, see [`portal/INVARIANTS.md`](portal/INVARIANTS.md). For the tuning knobs, metrics fields, and defaults, see [`portal/INTEGRATION.md`](portal/INTEGRATION.md). For the full measured run, see [`harness/euler-multichain/REPORT.md`](harness/euler-multichain/REPORT.md).
