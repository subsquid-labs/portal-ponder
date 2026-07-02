# Migrating a Ponder indexer onto `@subsquid/ponder`

**What you get:** the historical backfill goes **~8× faster** (and finishes on chains where RPC backfill is impractical), the data is **byte-identical to RPC**, and **handlers + schema are unchanged**. Realtime/frontfill stays on your existing RPC, and it's reversible. One de-risking step, then one change.

> Which source types and chains are supported — and how to read the per-network capability notes — is in the README's [Compatibility](README.md#compatibility) section. This guide is the adoption path.

---

## Step 0 — Check compatibility (optional)

A read-only pre-flight, in seconds and with no RPC: does the target portal serve your chains and source types? It changes nothing. Point `PORTAL_BASE` at the portal you'll actually use (dataset availability is per-portal):

```bash
cd <your-ponder-project>
PORTAL_BASE=<portal> npx tsx <path-to-portal-ponder>/harness/compat/report.ts ./ponder.config.ts
```

`harness/…` is repo tooling, not part of the `@subsquid/ponder` npm package — clone the repo first (as the [README Quickstart](README.md#quickstart) does): `git clone https://github.com/subsquid-labs/portal-ponder`, then point `<path-to-portal-ponder>` at it.

It prints a per-source `READY` / `NEEDS_*` / `NO_DATASET` verdict — for event and factory indexers (Uniswap, Euler, …) it's `READY NOW`. Coverage checks whether the source type is supported, whether the portal serves that chain's dataset, and whether the network has the needed capability — only **traces** gates a verdict (with block-range caveats); state-diffs and realtime are shown for information but never block a source.

**Optional — prove byte-parity.** The report above only checks coverage; it doesn't compare data. To actually prove Portal rows equal RPC rows, use the repo's differential tooling (same clone as above — it isn't in the npm package). The full sync-store diff indexes the same bounded range twice — once through the Portal, once through your RPC — and diffs every `logs` / `transactions` / `transaction_receipts` / `traces` row:

```bash
PONDER_RPC_URL_1=<eth archive RPC w/ debug_traceBlockByNumber> bash harness/diff/run.sh [start end]
```

Exit `0` means the two stores are byte-identical. For a lighter single-query check, `harness/compare/differential.ts` fetches one `(address, topic, range)` from both the Portal and your RPC and asserts they match (env-driven: `RPC_URL`, `DATASET`, `ADDRESS`, `TOPIC0`, `FROM`, `TO`). Both call your RPC, so they're slower and depend on it being responsive — run them when you want the proof, or skip straight to the backfill.

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

> **Dev vs production Portal.** `portal.sqd.dev` is the free public Portal — great for development and evaluation, but shared and rate-limited under load. For production throughput and reliability, use a dedicated Portal (see the README's [Going to production](README.md#going-to-production)).

**Pin to your exact ponder version** — `@subsquid/ponder@X.Y.Z-sqd.<rev>` is built from `ponder@X.Y.Z` (the `-sqd.<rev>` suffix is required — a bare `@X.Y.Z` is not an installable release); the supported set is in [`versions.json`](versions.json). The seam is identical across the tested range, so an upgrade is a version bump (CI proves each version before release — see [`PUBLISHING.md`](PUBLISHING.md)).

---

## Step 2 — Validate & roll back

- Watch the first backfill: wall-clock vs the previous RPC run, and **zero errors**.
- For a deep check, diff the `ponder_sync` tables of a Portal run vs an RPC run over the same range — they should match (logs are byte-identical; see the differential test).
- **Rollback** is a one-liner: point the dependency back at `ponder` and remove the `portal:` lines.
