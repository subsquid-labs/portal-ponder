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
1. **Coverage** — checks every source against (a) what our backfill implements (logs, log-factories, transactions, receipts, traces today; block-interval and account-transaction sources not yet) and (b) what Portal actually **serves for that chain and block-range**. Trace/state-diff coverage is per-network *and* per-range — Portal has [300+ networks](https://docs.sqd.dev/en/data/all-networks) with `traces`/`stateDiffs` flags and caveats (e.g. Optimism traces only from the Bedrock block 105235063; Arbitrum/Polygon lack ancient-block traces). The report **probes this live**, so a trace source whose `startBlock` predates Portal's trace coverage is flagged instead of silently green-lit.
2. **Parity** — fetches the same `eth_getLogs` from Portal and from the client's RPC and confirms they're byte-identical.

Output is a per-source `READY` / `NEEDS_*` verdict. For the common case (event + factory indexers like Uniswap/Euler) it's `READY NOW`. This report is the onboarding artifact and the trust gate — a client sees parity proven on *their* indexer before touching anything.

---

## Step 1 — Turn on the boost

Two lines in `ponder.config.ts`; handlers and schema untouched:

```ts
import { createConfig } from "ponder";
import { withPortal } from "@your-org/ponder-portal";

export default createConfig(withPortal({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,                 // realtime + state reads stay here
      portal: "https://portal.sqd.dev/datasets/ethereum-mainnet",  // backfill from Portal
    },
  },
  contracts: { /* unchanged */ },
}));
```

`withPortal()` records `portal` per chain; the package's runtime injection swaps Ponder's internal `createHistoricalSync` for the Portal-backed one when a chain has `portal` set (delivered as a `module.register` hook or a thin `ponder-portal` CLI — `package.json`: `"start": "ponder-portal start"`). Realtime keeps using `rpc`.

Run it. The first backfill is the wow: the ~5M-block Ethereum Euler history that takes ~38 min on RPC completes in **~5 min**; multi-chain (eth+base+arbitrum) Euler in ~16 min, all concurrent. See `README.md` for the measured numbers.

**Validate & roll back:** watch the first backfill (wall-clock + zero client-facing errors); for deep checks, diff the `ponder_sync` tables of a Portal run vs an RPC run over the same range. Rollback = remove `withPortal(...)` + the `portal:` line.

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
| block-interval sources | ⬜ (next) |
| account transaction (from/to) sources | ⬜ (transfers work via traces) |
| state diffs | n/a — Portal has them, but Ponder has no state-diff source (available via the standalone engine only) |

The compatibility report tells you which bucket each of a client's sources falls in, so you never enable a boost that would miss data.
