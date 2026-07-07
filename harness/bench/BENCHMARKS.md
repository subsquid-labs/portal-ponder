# Portal backfill benchmark base

A base of real/representative Ponder indexers, each run as a bounded **Portal backfill** under
instrumentation, so we can track our two priorities — **stability** (clean completion, no
errors/retries/OOM) and **backfill speed** (wall-clock, events/sec) — and drive optimization.

## Run it

```bash
# apps live in the patched-ponder workspace (so they link the Portal-fork core)
export PONDER_EXAMPLES=/path/to/ponder/examples
export PORTAL_API_KEY=…                 # dedicated portal
export PONDER_RPC_URL_1=…  PONDER_RPC_URL_8453=…   # SQD RPC per chain (finality + readContract)
node --experimental-strip-types harness/bench/bench-all.ts          # all
node --experimental-strip-types harness/bench/bench-all.ts uniswap,traces   # subset (comma list)
# → harness/bench/results.json
```

## What's in the base

| indexer | protocol | source types | chain |
|---|---|---|---|
| `uniswap-portal` (+ HEAVY) | Uniswap V3/V2 (synthetic) | logs + receipts + traces + block-interval + accounts — **all five** | mainnet |
| `feature-call-traces` | Multicall3 (first-party) | `includeCallTraces` | mainnet |
| `feature-blocks` | Chainlink (first-party) | `blocks:{interval}` | mainnet |
| `euler-mainnet` | Euler v2 lending | log-`factory()` (EVault proxies) | mainnet |
| `v4-ponder` | Uniswap V4 (marktoda) | logs singleton + token `readContract` | base |

`uniswap-portal` is the only bench exercising receipts/traces/accounts together (those are
near-absent in public indexers — see `CANDIDATES.md`). The rest are real first-party / third-party
Ponder apps, Portal-routed by adding one `portal:` line per chain.

## Methodology

- **Chunk = bench range.** Each run sets `PORTAL_CHUNK_BLOCKS` to its block span so the numbers
  are the *pure* backfill of that range. The production chunk is large (density-scaled to 500k–1M)
  and **amortizes over a full multi-interval backfill**; on a small bounded range it would over-fetch
  the whole chunk, which is a bench artifact, not a real cost.
- **Read-ahead = 1.** Read-ahead prefetches chunks *past* the bounded endBlock (waste in a bounded
  run; it only pays off open-ended).
- **Measured:** wall-clock + events/sec (from Ponder's `event_count`), peak RSS (process-tree),
  errors/retries/OOM; plus Portal-side http requests, bytes, dataChunks, cacheHits, rpcFallback,
  and inserted rows — from `PORTAL_METRICS_FILE`.
- **Caveat:** events/sec mixes Portal fetch + Ponder decode/store (pglite). Low ev/s with low
  Portal bytes ⇒ the bottleneck is indexing or `readContract`, not Portal.

## Results

Dedicated portal, mainnet/base via SQD RPC, chunk = bench range, read-ahead 1 (see `results.json` for raw):

```
indexer                          ok  wall(s)   events    ev/s   rssMB  http      MB  chunks  err  retry
-----------------------------------------------------------------------------------------------------------
uniswap-portal (all 5, 5k blk)    ✓     16.4    13,635     834    744     8    104.7      3    0    0
uniswap-portal HEAVY (50k blk)    ✓     98.9   179,245   1,812   1631    12   1102.5      4    0    0
feature-call-traces (traces)      ✓      7.1        17       2    630     2      0.6      3    0    0
feature-blocks (block-interval)   ✓     85.3     2,001      23    630     2     28.4      3    0    0
euler-mainnet (factory+lending)†  ✓     14.4         0       0    635     -      -        -    -    -
v4-ponder (uniswap v4, base)†     ✓     12.3         5       0    637     2      1.6      3    0    0
```

`†` ran cleanly but landed in a **low-activity window** (Euler vaults are discovered from deploy but
have little early event volume; base-v4 is thin there) — they validate the migration/run, not throughput.
Pick an active window for a throughput number.

**Read of the table:** the all-sources app scales from 834 → **1,812 ev/s** as the range grows 10× (steady-state
indexing + chunk amortization), staying stable at **1.6 GB RSS / 0 errors** on 179k events. Trace bytes
amortize too (7.7 → 6 KB/event). `feature-blocks` is the outlier — 85 s for 2,001 ticks — the block-source
`includeAllBlocks` scan (optimization #1 below).

## Full-history backfill — F-full flagship cell

The paid validation cell **F-full** (VALIDATION.md §3.2) also yields the first end-to-end
**full-history** backfill timing: the entire recorded Euler v2 history on eth mainnet,
`[20529207, 25436954]` — **4,907,748 blocks** — backfilled two ways on the same host, serially:

| Leg | What it is | Wall-clock |
|---|---|---|
| Portal | the fork backfilling from the SQD Portal | **1819 s** |
| Stock RPC | genuine `ponder@0.16.6` over a metered JSON-RPC endpoint | **6543 s** |

→ **3.6× faster** on the Portal leg, same range, same host, same store backend.

**Caveats (read before quoting the number):**

- **Single run**, not an averaged benchmark — a datapoint, not a gate.
- **Serial, same host**: the two legs ran one after the other on the same machine, so neither
  contended with the other, but host-level variance is uncontrolled across a single pair.
- **Network / endpoint-bound**: both legs are dominated by fetch, so the ratio reflects the specific
  Portal dataset and the specific metered RPC endpoint used, not a pure engine comparison.
- **PGlite store backend** (embedded), production chunking `PORTAL_CHUNK_BLOCKS=500000` (unlike the
  bench base above, which sets the chunk to the bounded range — here it is the real production value).
- The stock leg made **576,207 metered JSON-RPC requests** to reach ground truth.

The correctness verdict for this same run (byte-identical sync-store rows across all families) and its
full chain of custody are in **VALIDATION.md §3.2**.

## Findings (stability + speed)

- **Stability is solid:** 0 errors / 0 retries across every bench; the dense-source chunk cap keeps
  RSS bounded (≤~1 GB) where an uncapped run hit **3.7 GB / near-OOM** on a block source.
- **`uniswap-portal` (all five source types) is the standout:** dense real Uniswap activity, hundreds
  of events/sec, ~100 MB fetched for a 5k-block window — clean and fast.
- **Block-interval sources are the slowest path.** `includeAllBlocks` has no server-side stride
  filter, so a `blocks:{interval:N}` source scans **every** block in range to keep every Nth — bytes
  and wall-clock scale with the full range, not the matched count.
- **Traces are cheap when filtered.** Pushing `callTo`/`callFrom`/`callSighash` server-side keeps a
  trace source's bytes proportional to *matched* traces (Multicall3 ≈ sub-MB); the cost shows up only
  for very hot traced contracts (V2 Router).
- **Some real indexers are `readContract`-bound, not Portal-bound** (v4-ponder fetches token metadata
  per new token over RPC). Portal accelerates the log backfill; those reads are orthogonal.
- **Bounded factory benches are activity-sensitive:** factory child discovery runs from the factory
  deploy, but a young/quiet window yields few downstream events — pick an active window for throughput.

## Optimization backlog (next steps — prioritized)

1. **Block-source fetch.** Avoid `includeAllBlocks` whole-range scans for sparse intervals: fetch the
   matched block numbers directly (batched), or adopt a server-side stride if Portal adds one. Largest
   backfill-speed gain for `blocks:{interval}` sources.
2. **Clamp chunk + read-ahead to the sync target.** Don't fetch/prefetch past the historical end block;
   size the chunk to remaining work near the tail. (Done in-bench via env; should be intrinsic.)
3. **Trace memory/throughput at hot contracts.** Stream-insert traces per sub-range instead of buffering
   a whole chunk; tune `PORTAL_TRACE_CHUNK_BLOCKS` by trace density, not block count.
4. **Indexing-layer throughput** (pglite decode/store) caps ev/s once fetch is fast — Postgres + parallel
   indexing for very large resyncs (orthogonal to Portal, but it's the next ceiling after fetch).
5. **CU-aware prefetch depth** for open-ended backfills (scale read-ahead to the Portal CU budget).

`results.json` holds the raw metrics for tracking deltas as these land.
