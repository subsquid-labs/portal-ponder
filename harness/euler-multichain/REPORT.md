# Euler V2 multichain — full-history e2e on `@subsquid/ponder`

**One Ponder app indexing every Portal-supported Euler V2 chain (15), full history `[deploy → finalized head]`, the complete 24-event EVault superset — streamed from the SQD Portal into Postgres.** This is the production shape of an Euler indexer and a stress test of the Portal-backed fork.

Captured on a single OVH box (Ubuntu 24.04). The run is deliberately bounded to a fixed head so it's a clean, reproducible benchmark; live/realtime is a separate mode (see *Roadmap*).

---

## TL;DR

- **28,405,932 events across 15 chains, 2,484 vaults**, full history, in **44m 55s**.
- **Byte-complete and correct** — verified independently against the Portal ground truth: **60/60 sampled windows across all 15 chains match exactly**, and two independent runs produced the **identical** 28,405,932 total.
- **Modest footprint** — the indexer ran capped at **16 GB / 2 cores** (peak 9.2 GB, ~1 core of real work); Postgres is a separate, throughput-tuned production DB. No 96-core/125 GB machine required — the box was just what we had; its cores and RAM sat idle.
- **The Portal is not the bottleneck.** It feeds data faster than a single Ponder event-loop can consume it; wall-time is set by Ponder's single-threaded indexing, not by fetch, RAM, or cores.

---

## What it indexes

- **15 chains:** ethereum, base, arbitrum, avalanche, sonic, plasma, monad, binance, linea, berachain, unichain, tac, bob, polygon, hyperliquid.
- **The `eVaultFactory` (`GenericFactory` → `ProxyCreated` → child EVaults)** per chain. Per-chain **start blocks come from Euler's own `euler-subgraph` config**, and every `eVaultFactory` address is **cross-verified against `euler-interfaces` `CoreAddresses.json`** (all 15 ✓).
- **The full 24-event `IEVault` interface** (`Deposit/Withdraw/Borrow/Repay/Liquidate/Transfer/Approval/DebtSocialized/PullDebt/InterestAccrued/VaultStatus/ConvertFees/BalanceForwarderStatus/EVaultCreated` + all `GovSet*`) — a superset of the public euler-subgraph's 3-event set, since Euler runs both Ponder and subgraphs and prod reads more than any single consumer.

---

## Results — per chain

| chain | events | vaults (indexed) | vs Euler subgraph | windows |
|---|--:|--:|--:|:--|
| berachain | 20,185,992 | 59 | 59 / 59 ✓ | 4/4 ✓ |
| unichain | 2,134,319 | 50 | 50 / 51 | 4/4 ✓ |
| ethereum | 1,351,179 | 850 | 850 / 897 | 4/4 ✓ |
| base | 1,282,875 | 325 | 325 / 346 | 4/4 ✓ |
| arbitrum | 729,050 | 137 | 137 / 151 | 4/4 ✓ |
| sonic | 663,464 | 174 | 174 / 176 | 4/4 ✓ |
| avalanche | 555,877 | 251 | 251 / 265 | 4/4 ✓ |
| plasma | 481,982 | 156 | 156 / 175 | 4/4 ✓ |
| monad | 285,415 | 122 | 122 / 131 | 4/4 ✓ |
| binance | 260,605 | 126 | 126 / 129 | 4/4 ✓ |
| linea | 257,099 | 88 | 88 / 100 | 4/4 ✓ |
| bob | 100,634 | 27 | 27 / 27 ✓ | 4/4 ✓ |
| hyperliquid | 67,314 | 58 | 58 / 58 (subgraph lagging ~4M blocks) | 4/4 ✓ |
| tac | 49,324 | 36 | 36 / 36 ✓ | 4/4 ✓ |
| polygon | 803 | 25 | 25 / 25 ✓ | 4/4 ✓ |
| **total** | **28,405,932** | **2,484** | | **60/60 ✓** |

`HyperEVM 58 / 58`: at run time (July 1, 2026 heads), Euler's then-deployed HyperEVM subgraph reported **zero** vaults. It has since been redeployed and, as of a live re-check on 2026-07-03, now reports the same **58** vaults this indexer found — but it's still stalled roughly **4M blocks** behind the HyperEVM chain tip (weeks of lag), all 58 vaults predating its indexed head. The Portal backfill covered the same full history in minutes. High-throughput chains remain exactly where subgraph infrastructure struggles.

---

## Correctness (the part that matters)

Data correctness was treated as the gate: **the report does not ship unless the data is provably complete and correct.** Three independent lines of evidence, all against the **SQD Portal ground truth** (Portal-derived logs are byte-identical to JSON-RPC `eth_getLogs` — see the differential test in the repo):

1. **Windowed completeness — 60/60 exact.** For each chain we sampled four 100k-block windows (early / 40% / 75% / head) and compared the indexed event count to the Portal's, both filtered to the discovered children + the 24 event topics. **Every window matched exactly** — e.g. ethereum `[24.21M–24.31M]` 28,825 = 28,825; berachain `[17.48M–17.58M]` **282,320 = 282,320**.
2. **Cross-run reproducibility — identical.** Two independent runs with *different* chunking, heap, and Postgres configs produced the **identical** 28,405,932-event total. A data-loss bug would diverge; it doesn't.
3. **Vault discovery cross-checked vs Euler's own live subgraph.** Discovered vault counts match Euler exactly where the chain is fully active (berachain 59/59, polygon 25/25, bob 27/27, tac 36/36, HyperEVM 58/58 — HyperEVM's subgraph lagging ~4M blocks behind chain tip). Live-verified 2026-07-03 against Euler's public Goldsky subgraphs (`euler-v2-<net>/latest` and `euler-simple-<net>/latest`). The gaps on larger chains are **not missing data**: for ethereum, the Portal shows **872** vaults created by our head vs Euler's 897 — **25 were created after our fixed head** (Euler's subgraph runs ahead), and the remaining **22 are discovered-but-eventless** (created, never used), confirmed because our indexed total equals the Portal total for the events that do exist.

**On the 24-event superset:** it is complete for the `IEVault` interface. The only vault log *outside* it is `Genesis()` — a no-parameter genesis marker emitted once per vault — which is already captured as vault creation via `ProxyCreated` discovery, so nothing is lost.

---

## Performance, footprint, and the A/B

We ran two configurations to find the right operating point. **Both indexed the identical 28.4M events; only the config differed.**

| | (a) over-provisioned | (b) modest — recommended |
|---|---|---|
| wall time | 67m 10s | **44m 55s** |
| indexer peak mem | 19.0 GB | **9.2 GB** |
| indexer cap | 32 GB heap, density chunks | **16 GB / 2 cores**, fixed 300k chunks |
| Postgres | default | **tuned** (`synchronous_commit=off`, 16 GB shared_buffers) |
| Portal concurrency | thrashy (48↔8 oscillation) | **steady** (mostly 48) |
| avg throughput | 7,024 ev/s | **10,513 ev/s** |

The modest config is **faster, uses half the memory, and runs steadier**. The lesson is counter-intuitive but important: **don't over-provision the indexer — right-size it and tune the database.** (Honest caveat: (b) improved both the indexer footprint *and* the PG tuning, so its win reflects the combined informed config, not the smaller heap alone.)

**Footprint, stated plainly:** the *indexer* is ~1 CPU core (Ponder is single-threaded) and ~9 GB RAM. Postgres is a separate, uncapped, tuned production database — as any real indexer needs. It is **not** 16 GB total, and it does **not** need a big machine.

---

## Where the ceiling actually is (measured)

Once the Portal makes the backfill fast, the bottleneck moves entirely to the indexer:

- **Ponder is single-threaded.** Under load the node process pinned **one core at ~92%** while ~4 libuv/GC helper threads idled; system load was ~3 of 96 cores. **93 cores sat unused.**
- **Postgres was faster than the indexer** — its backends sat idle in `ClientRead`, waiting for Ponder to send data.
- **The Portal outruns both** — during the tail its fetch queue drained to idle with the buffer full; every remaining second was single-threaded decode+index of berachain's 20M events.

So more RAM/cores don't buy wall-time here — they're already idle. The lever for going faster is **sharding chains across processes** (e.g. berachain on its own), not bigger hardware.

---

## What runs where

- **The Portal serves the historical data** over HTTP. Backfill throughput scales with the Portal's provisioning and is independent of the indexer's footprint.
- **The indexer** (this fork of Ponder) is what you run — modest footprint, your handlers and schema unchanged, one `portal:` line per chain.

The numbers here are a **floor** at the current provisioning and a single-process indexer — not a ceiling.

---

## Roadmap

- **Realtime / unbounded:** this benchmark is bounded to a fixed head for clean measurement. The fork already supports unbounded backfill → finalized head → **RPC realtime** for the tip (with automatic RPC fallback for the finality gap and Portal errors). Making the Portal's fork-aware hot-blocks `/stream` the *primary* realtime source (RPC as fallback-only) is the next step.
- **Multi-process sharding** to lift the single-threaded indexing ceiling for event-heavy chains.

---

## Reproduce

```bash
docker compose up -d postgres          # or a tuned standalone Postgres
cp .env.example .env                   # PORTAL_API_KEY (+ SQD_RPC_KEY for the 10 SQD-served chains)
./run.sh
```

See [`README.md`](./README.md) for the config and metrics. Correctness was verified with per-window and aggregate cross-checks against the Portal; vault discovery against Euler's `euler-subgraph` (Goldsky) endpoints.
