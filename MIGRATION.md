# Migrating a Ponder indexer onto `@subsquid/ponder`

**What you get:** the historical backfill goes **~8× faster** (and finishes on chains where RPC backfill is impractical), the data is **byte-identical to RPC**, and **handlers + schema are unchanged**. Realtime/frontfill stays on your existing RPC, and it's reversible. One de-risking step, then one change.

> Which source types and chains are supported — and how to read the per-network capability notes — is in the README's [Compatibility](README.md#compatibility) section. This guide is the adoption path.

---

## Step 0 — Check compatibility (optional)

A read-only pre-flight, in seconds and with no RPC: does the target portal serve your chains and source types? It changes nothing. Point `PORTAL_BASE` at the portal you'll actually use (dataset availability is per-portal):

```bash
cd <your-ponder-project>
PORTAL_BASE=<portal> npx tsx <path>/harness/compat/report.ts ./ponder.config.ts
```

It prints a per-source `READY` / `NEEDS_*` verdict — for event and factory indexers (Uniswap, Euler, …) it's `READY NOW`. Coverage checks whether the source type is supported, whether the portal serves that chain's dataset, and whether the network has the needed capability (traces / state-diffs, with block-range caveats).

**Optional — prove byte-parity.** Add `--differential <your-rpc-url>` to fetch the same `eth_getLogs` from the Portal and your RPC and confirm they're byte-identical. This one calls your RPC, so it is slower and depends on the RPC being responsive — run it when you want the proof, or skip it and go straight to the backfill.

---

## Step 1 — Switch the dependency, add one line per chain

```jsonc
// package.json — pin the fork; <ponder-version>-sqd.<rev> == ponder@<version> + the Portal layer
"dependencies": { "@subsquid/ponder": "0.16.6-sqd.1" }   // == ponder@0.16.6 + the Portal layer
```

```ts
// ponder.config.ts — import from the fork; add `portal:` per chain. Nothing else changes.
import { createConfig } from "@subsquid/ponder";

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: process.env.PONDER_RPC_URL_1,                          // realtime + state reads stay here
      portal: "https://portal.sqd.dev/datasets/ethereum-mainnet", // ← backfill from Portal
    },
  },
  contracts: { /* unchanged */ },
});
```

Same `ponder` bin, so `ponder dev` / `ponder start` are unchanged. When a chain has `portal` set, the historical backfill streams from Portal and realtime keeps using `rpc`; if Portal's finalized head ever lags the target, the gap auto-falls-back to the stock `rpc` sync so the backfill stays complete.

**Pin to your exact ponder version** — `@subsquid/ponder@X.Y.Z` is built from `ponder@X.Y.Z`; the supported set is in [`versions.json`](versions.json). The seam is identical across the tested range, so an upgrade is a version bump (CI proves each version before release — see [`PUBLISHING.md`](PUBLISHING.md)).

---

## Step 2 — Validate & roll back

- Watch the first backfill: wall-clock vs the previous RPC run, and **zero errors**.
- For a deep check, diff the `ponder_sync` tables of a Portal run vs an RPC run over the same range — they should match (logs are byte-identical; see the differential test).
- **Rollback** is a one-liner: point the dependency back at `ponder` and remove the `portal:` lines.
