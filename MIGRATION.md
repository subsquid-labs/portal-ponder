# Migrating a Ponder indexer onto Portal

**What you get:** a Ponder app's historical backfill goes **~8× faster** (and finishes on chains where RPC backfill is impractical), the data is **byte-identical to RPC** (proven before you change anything), and **handlers + schema are unchanged**. Realtime/frontfill stays on your existing RPC. It's reversible — remove a line and you're back on RPC.

This is one path, with one de-risking step in front of it.

---

## Step 0 — Prove it (compatibility + parity report)

No production change. Point the report at the client's config:

```bash
cd <client-ponder-project>
PORTAL_API_KEY=<key> node <path>/harness/compat/report.ts ./ponder.config.ts --differential <client-rpc-url>
```

It does two things:
1. **Coverage** — per source: (a) does our backfill implement the type (logs, log-factories, transactions, receipts, traces, block-interval, account transactions — every Ponder source type today); (b) does the **target portal** serve that chain's dataset — checked live against *its* `/datasets`, because **different portals serve different subsets**, so point `PORTAL_BASE` at the portal the client will actually use; (c) does the network have the data a source needs (traces), per the [authoritative docs matrix](https://docs.sqd.dev/en/data/all-networks) of 300+ networks (`traces`/`stateDiffs` flags + block-range notes, e.g. Optimism's Bedrock cutoff). A trace source on a chain with `traces:false` is flagged; block-range notes are surfaced for you to check against your `startBlock`.
2. **Parity** — fetches the same `eth_getLogs` from Portal and from the client's RPC and confirms they're byte-identical.

Output is a per-source `READY` / `NEEDS_*` verdict. For the common case (event + factory indexers like Uniswap/Euler) it's `READY NOW`. This report is the onboarding artifact and the trust gate — a client sees parity proven on *their* indexer before touching anything.

---

## Step 1 — Turn on the boost

**Swap the dependency, add one line per chain.** `@subsquid/ponder` is a drop-in fork of `ponder`
(same `ponder` bin, same API) that just *also* accepts a `portal:` field per chain; handlers and schema
are untouched.

```jsonc
// package.json — pin the fork to the ponder version you're on (the versions mirror exactly)
"dependencies": { "@subsquid/ponder": "0.16.6" }   // == ponder@0.16.6 + Portal layer
```

```ts
// ponder.config.ts — import from the fork; add `portal:` per chain. Nothing else changes.
import { createConfig } from "@subsquid/ponder";

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,                                // realtime + state reads stay here
      portal: "https://portal.sqd.dev/datasets/ethereum-mainnet",       // ← backfill from Portal
    },
  },
  contracts: { /* unchanged */ },
});
```

When a chain has `portal` set, the fork routes the historical backfill through Portal and keeps realtime
on `rpc`; and if Portal's finalized head ever lags the target, the gap auto-falls-back to the stock `rpc`
sync, so it stays complete. (Pin the fork to your exact ponder version — `@subsquid/ponder@X.Y.Z` is built
from `ponder@X.Y.Z`; see [`versions.json`](versions.json) for the supported set.)

Run it. The first backfill is the wow: the ~5M-block Ethereum Euler history that takes ~38 min on RPC
completes in **~5 min**; multi-chain (eth+base+arbitrum) Euler in ~16 min, all concurrent. See `README.md`
for the measured numbers.

**Validate & roll back:** watch the first backfill (wall-clock + zero client-facing errors); for deep
checks, diff the `ponder_sync` tables of a Portal run vs an RPC run over the same range. Rollback = point
the dependency back at `ponder` and remove the `portal:` lines.

---

## Why there's no JSON-RPC / transport "shim" option

A tempting "config-only" delivery is to present Portal as a custom `rpc:` transport. **We deliberately don't**, because it sits at the per-request EIP-1193 layer — *after* Ponder's orchestrator has shattered each range into thousands of non-contiguous point lookups (`eth_getLogs` per topic, `eth_getBlockByNumber` per block). Point lookups are exactly what Portal is slow/unstable at; a shim would make a client's *first* experience "Portal is slow" — the opposite of the pitch, and the very failure mode the prior eRPC+proxy approach hit.

The whole ~8× comes from integrating one level up, at Ponder's **range-oriented `HistoricalSync` seam**, where one interval = one streamed columnar scan. So there's a single path on purpose: the native seam, or nothing.

---

## Version coverage

The `HistoricalSync` seam (`syncBlockRangeData`/`syncBlockData`) is **identical in shape from 0.15.17 → 0.16.6**, so one Portal implementation covers Euler's 0.15.x and current. The package ships a CI matrix that runs the regression suite against each supported Ponder version, so an upgrade can't silently break a client — this is why a versioned package is far less work than maintaining N forks.

## Coverage & gaps

| Ponder source | Served by Portal backfill |
|---|---|
| logs, log-factories | ✅ |
| transactions | ✅ |
| receipts (`includeTransactionReceipts`) | ✅ |
| traces (`includeCallTraces`) / transfers | ✅ |
| block-interval sources (`blocks: { … interval }`) | ✅ |
| account transaction (from/to) sources (`accounts: { … }`) | ✅ |
| state diffs | n/a — Portal has them, but Ponder has no state-diff source (available via the standalone engine only) |

The compatibility report tells you which bucket each of a client's sources falls in, so you never enable a boost that would miss data.
