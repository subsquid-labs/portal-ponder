# How the Portal backfill works

`@subsquid/ponder` changes one thing about Ponder: the source of the **historical** backfill. Instead of replaying history over JSON-RPC — one `eth_getLogs` per topic, one `eth_getBlockByNumber` per matched block — a chain with a `portal:` source streams its history from the [SQD Portal](https://sqd.dev/portal/). The Portal is an HTTP access layer to the SQD Network: it answers an arbitrary `[from, to]` block range as a single streamed response, so hundreds of thousands of small round-trips collapse into a handful of large streamed reads.

Everything else is unchanged. Handlers, schema, config, the `ponder` CLI, realtime, and Ponder's reorg handling all stay the same. This document describes the layer that lives in `portal/`: where it plugs into Ponder, how it keeps every chain fed without exhausting memory, and how to observe it.

## The historical-sync seam

Ponder's engine drives the backfill by handing its historical sync one block interval at a time. That contract is two methods on `HistoricalSync`:

- `syncBlockRangeData({ interval, ... })` — fetch and persist the data an interval needs, return its matched logs.
- `syncBlockData({ interval, ... })` — finalize the interval and return the highest block that carried data.

The fork implements exactly this interface in `portal/portal.ts` (`createPortalHistoricalSync`) and selects it per chain in Ponder's `runtime/historical.ts`:

```ts
const historicalSync = params.chain.portal
  ? createPortalHistoricalSync(params)   // stream the interval from the Portal
  : createHistoricalSync(params);        // stock RPC path, unchanged
```

The Portal sync receives the **same** `params` as the stock sync — `chain`, `rpc`, `childAddresses`, and the chain's full `eventCallbacks` (its complete filter set). No handler or config shape is translated; the fork reads Ponder's own filters and factory map directly. Because it satisfies Ponder's existing interface rather than a public API, the integration is a small fork generated from upstream Ponder plus a short wiring patch (`portal/wiring/`), and it tracks Ponder closely — the seam is identical across the tested range (0.15.17–0.16.6, see [`versions.json`](../versions.json)).

Within an interval the two methods split the work:

- `syncBlockRangeData` resolves the interval to the Portal chunks that cover it, transforms the rows falling inside `[interval[0], interval[1]]` into Ponder's `Sync*` shapes, inserts the logs immediately, and stashes the interval's blocks, transactions, receipts, and traces keyed by the interval.
- `syncBlockData` flushes that stash — blocks, then transactions, receipts, and traces — and returns the highest block with data so Ponder can advance its checkpoint.

### The read-ahead chunk buffer

Ponder feeds intervals that are small relative to a full history. A Portal request, on the other hand, is latency-bound per call but has very large parallel bandwidth, so serving one small interval per request would waste it. The fork instead fetches **large aligned chunks** and serves every interval from an in-memory cache keyed by chunk index. A chunk covers `PORTAL_CHUNK_BLOCKS` blocks (default 500,000), and the fork scales that up by the chain's block density: it multiplies the base width by a density factor of `head ÷ 25,000,000` (that divisor is a fixed mainnet-scale reference height, so a ~25M-block chain gets 1×), then caps the resulting chunk at 25M blocks. The two 25M figures are distinct — one is the density divisor, the other the maximum chunk width — and only coincidentally share a value. So a high-block-rate chain like Arbitrum takes proportionally fewer round-trips rather than 19× as many 500k chunks. The Portal charges by data touched, not by block width, so wider chunks cut round-trips at no extra cost. Set `PORTAL_CHUNK_FIXED` to disable density scaling. Trace and block-interval sources return much denser data and are capped to `PORTAL_TRACE_CHUNK_BLOCKS` (default 2,000) so a single chunk can't buffer a busy contract's entire trace set.

Chunks are also **prefetched**: as an interval finishes, the fork issues the next chunks concurrently (up to `PORTAL_READAHEAD` deep, default 6) so the Portal's per-request latency overlaps with indexing instead of serializing in front of it. How far read-ahead actually runs is bounded by the shared memory budget below, not by a fixed count. Chunks behind the current interval are evicted as it advances.

## The backfill → realtime handoff

Historical sync owns `[start, finalized]`; realtime owns `(finalized, tip]`. The Portal serves only **finalized** data, which keeps the split clean.

**Realtime stays on your RPC by default.** When the backfill reaches the finalized head, Ponder hands off to its own realtime sync over `rpc`, with its own reorg handling — unchanged from stock Ponder. The Portal is never in the realtime path unless you opt in.

**Finality-gap fallback.** The Portal's finalized head can occasionally lag Ponder's target finalized block. Any interval that reaches past the Portal head — or that runs while the head probe is failing — is delegated *whole* to the stock RPC `createHistoricalSync`, so the tip is never silently under-served. This is why `rpc` must stay configured. `PORTAL_FINALIZED_HEAD` pins the head for testing and ops.

**Portal-native realtime** (opt-in, experimental). Set `PORTAL_REALTIME=stream` to serve the tip from the Portal's fork-aware `/stream` instead of RPC. In this mode `clampFinalizedToPortalHead` lowers Ponder's finalized block to the Portal head, so historical stops exactly there and realtime streams `[portal-head+1 → tip]`, reconciling reorgs from the stream's parent-hash chain. The RPC finality-gap fallback is then neither needed nor used. Your RPC is still used for chain setup and `readContract`. With the flag unset or `rpc`, this path is inert and the RPC realtime path is byte-for-byte unchanged.

## The shared controller

Every chain streams from the **same** Portal endpoint, so request concurrency and buffered memory are one shared budget, not a per-chain one. A 15-chain app that gave each chain its own private read-ahead is exactly what exhausts memory and trips rate limits. The fork therefore routes all chains through a single module-scope controller (`portalGate`) that governs two things, both self-tuning and zero-config:

### Adaptive concurrency (AIMD)

The controller caps how many Portal requests are in flight across all chains and adapts that cap to the endpoint's live capacity, which the client cannot know ahead of time and which drifts over time. It follows additive-increase / multiplicative-decrease:

- **Start** at `PORTAL_START_CONCURRENCY` (16).
- **Increase** by 2 after every 8 clean responses, up to `PORTAL_MAX_CONCURRENCY` (48).
- **Halve** — down to `PORTAL_MIN_CONCURRENCY` (8) — on any back-pressure signal: HTTP 429, any 5xx, a 409 on the finalized stream, or a dropped/timed-out connection.

This mirrors Ponder's native RPC AIMD, but it is global because the endpoint is shared. A request that back-off marks as transient is retried with exponential back-off (honoring `retry-after`); only a genuinely unrecoverable response fails the run.

### Memory backpressure

Concurrency is bounded above; memory is bounded by a **row budget**. `PORTAL_MAX_ROWS_IN_MEM` (default 250,000) caps the log/transaction/trace/block records held live across every chain's read-ahead — roughly 1.5–2.5 GB once Ponder's derived copies are counted. Read-ahead prefetches until the shared buffer reaches the budget, then pauses; as indexing consumes chunks they are evicted and the buffer refills. So the buffer stays full enough that indexing rarely waits on a fetch, while total memory stays capped no matter how many chains run at once. Read-ahead always keeps at least the immediate next chunk ready per chain, and goes deeper only while the shared buffer is below budget. Raise the budget together with a larger Node `--max-old-space-size`; lower it when the indexer is memory-constrained.

## Factory discovery

A factory source (an EVault factory that emits thousands of child vaults, say) has to be discovered before its children's events can be fetched. The fork decouples the **discovery** timeline from the **data** timeline:

- Discovery is a wide scan of the factory's creation event over `[deploy, head]`, pinned to the factory's real deploy block rather than block 0. The Portal serializes a single stream in block order, so the scan is split into `PORTAL_DISCOVERY_WINDOWS` (default 8) disjoint windows issued concurrently; each window additionally fans out across `PORTAL_BUFFER_SIZE` (default 100) chunk workers on the Portal side, at no extra cost.
- A data chunk only fetches once discovery has completed *through its own block range*. Data chunks may be fetched out of order, but no child event is ever missed, because the child set is known to be complete up to each chunk's blocks before that chunk streams.

Discovered child addresses are pushed into the Portal's server-side log filter (`address` + `topic0..3`), so a factory with thousands of children still resolves to a small number of streamed reads rather than per-address lookups.

## Correctness: byte-identity with `eth_getLogs`

Row filters split between the Portal and the client. Logs (address + topics) and account transactions (from / to) are pushed to the Portal's native server-side filters, so only matching rows cross the wire. Traces and block-intervals are matched **client-side**, for two different reasons. Traces are fetched *whole* and filtered here by call target / caller / sighash — deliberately: Ponder assigns each trace a `trace_index` equal to its pre-order DFS rank over the transaction's *entire* call tree, so a matched trace has to keep its position in that full tree. Pushing the trace filter to the Portal would return only the matched subset and collapse the rank to a filter-local one (a lone deep match would rank `0` instead of its true, say, `7`). Block-intervals are matched client-side because the Portal has no modulo (offset/interval) filter. Field projection requests exactly the columns Ponder's sync store persists, and header fields are normalized so a stored block is byte-identical to the RPC path (which always carries `nonce`, `mixHash`, `sha3Uncles`, `totalDifficulty`).

The result is that Portal-derived rows are **byte-identical** to what the stock RPC backfill would store, across logs, transactions, receipts, and traces. The differential test in [`harness/diff`](../harness/diff) proves it: it indexes the same bounded range twice on this fork — once with `portal:` set (the Portal path) and once without it (the stock RPC path; the only difference is the backfill source) — into two separate `ponder_sync` stores, then diffs every row of `logs`, `transactions`, `transaction_receipts`, and `traces`. Exit `0` means identical.

## Observability

Two opt-in channels expose what the backfill and the shared controller are doing. Neither is on by default.

### Per-chain metrics — `PORTAL_METRICS_FILE`

Set `PORTAL_METRICS_FILE` to a path and the fork writes one JSON file per chain, at `<path>.<chainId>`, rewritten as each interval finalizes and holding cumulative counters for the run:

| Field | Meaning |
|---|---|
| `chain`, `chainId` | Chain name and id. |
| `wallMs` | Wall-clock since this chain's first interval. |
| `chunkBlocks` | Effective chunk width after density scaling. |
| `portalFinalizedHead` | The Portal's finalized head (or `null` if unknown). |
| `fetch` | `dataChunks`, `discChunks`, `http`, `bytes`, `errors`, `retries`, `cacheHits`, `maxInflight`. |
| `timing` | `gateWaitMs`, `fetchMs`, `transformMs` (cumulative). |
| `portalGate` | Controller snapshot: `limit`, `active`, `rows`. |
| `inserted` | Rows written: `logs`, `blocks`, `txs`, `receipts`, `traces`. |
| `rpcFallbackIntervals` | Intervals delegated to RPC across the finality gap. |

The `timing` split isolates where each request's time goes: `gateWaitMs` is time blocked on the shared concurrency budget, `fetchMs` is Portal I/O (POST plus stream drain), and `transformMs` is NDJSON-to-`Sync*` decode. Database-write time is Ponder's and is reported by Ponder, not here.

### Controller log — `PORTAL_GATE_LOG`

Set `PORTAL_GATE_LOG=1` to log the shared controller every 20 seconds:

```
[portalGate] concurrency_limit=48 active=12 buffered_rows=180000
```

`concurrency_limit` is the current adaptive ceiling, `active` the requests in flight, and `buffered_rows` the read-ahead depth against the memory budget. When `buffered_rows` sits near `PORTAL_MAX_ROWS_IN_MEM`, the fetch is ahead and indexing is the bottleneck — the Portal is not.

## Tuning

The defaults run well without configuration. These environment variables override them.

| Variable | Default | Effect |
|---|---|---|
| `PORTAL_START_CONCURRENCY` | `16` | Initial in-flight request limit before AIMD adapts. |
| `PORTAL_MAX_CONCURRENCY` | `48` | Upper bound on concurrent Portal requests. |
| `PORTAL_MIN_CONCURRENCY` | `8` | Lower bound the back-off will not cross. |
| `PORTAL_MAX_ROWS_IN_MEM` | `250000` | Shared buffered-row budget for read-ahead (~1.5–2.5 GB). Raise with a larger `--max-old-space-size`. |
| `PORTAL_READAHEAD` | `6` | Max chunks prefetched ahead of the indexer, per chain. |
| `PORTAL_CHUNK_BLOCKS` | `500000` | Base block range per request; scaled by block density unless fixed. |
| `PORTAL_CHUNK_FIXED` | unset | Set to any value to disable density-based chunk scaling. |
| `PORTAL_TRACE_CHUNK_BLOCKS` | `2000` | Chunk width when a chain has trace or block-interval sources (denser data). |
| `PORTAL_BUFFER_SIZE` | `100` | Chunk workers the Portal fans a single request across. |
| `PORTAL_DISCOVERY_WINDOWS` | `8` | Disjoint windows a factory scan is split into and fetched concurrently. |
| `PORTAL_REALTIME` | unset | Set to `stream` to serve realtime from the Portal `/stream` instead of RPC. |
| `PORTAL_API_KEY` | unset | Sent as `x-api-key` on every Portal request (for a keyed or dedicated Portal). |
| `PORTAL_FINALIZED_HEAD` | unset | Pin the Portal finalized head (testing/ops); overrides the `/finalized-head` probe. |
| `PORTAL_METRICS_FILE` | unset | Write per-chain JSON metrics to `<path>.<chainId>`. |
| `PORTAL_GATE_LOG` | unset | Set to `1` to log the shared controller every 20 s. |

Raising concurrency does not help when indexing is the bottleneck, which for a full-history resync it usually is — the fetch is already ahead. The knobs that matter most in practice are the memory budget (`PORTAL_MAX_ROWS_IN_MEM`) on a constrained box and, for a keyed deployment, `PORTAL_API_KEY`.

## Performance

Once the Portal serves the backfill, a resync is bound by **indexing**, not fetching. Ponder indexes on a single thread, so the practical ceiling for a large multichain app is that one thread.

The reference run — all 15 Portal-supported Euler V2 chains, full history, **28,405,932 events**, byte-verified complete against the Portal — finished in **44m 55s** on an indexer capped at 16 GB and 2 cores (peak 9.2 GB, about one core of real work). Postgres was a separate, throughput-tuned database, and the backfill ran against a **dedicated (provisioned) Portal** — the free public `portal.sqd.dev` is shared and rate-limited under load, so it backfills slower. Over the same run the Portal's fetch queue drained to idle with the buffer full while a single event loop worked through the tail: the Portal outruns the indexer, and more RAM or cores sit idle.

The A/B in that run is the useful lesson. A modest, well-tuned configuration beat an over-provisioned one:

| | Over-provisioned | Modest — recommended |
|---|---|---|
| Wall time | 67m 10s | **44m 55s** |
| Indexer peak memory | 19.0 GB | **9.2 GB** |
| Indexer cap | 32 GB heap, density chunks | **16 GB / 2 cores**, fixed 300k chunks |
| Postgres | default | **tuned** |
| Avg throughput | 7,024 ev/s | **10,513 ev/s** |

Both runs indexed the identical 28.4M events; only the configuration differed. The lever for going faster is not a bigger heap — it is **sharding chains across processes** to lift the single-thread indexing ceiling (for example, running the event-heaviest chain on its own). The full write-up is in [`harness/euler-multichain/REPORT.md`](../harness/euler-multichain/REPORT.md).
