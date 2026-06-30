# Migrating a Ponder indexer onto Portal ("boost Ponder with Portal")

Goal: make a Ponder app's **historical backfill** read from SQD Portal (range-scan, ~8× faster on factory/large-contract indexers) while **realtime stays on your existing RPC**. The migration is **additive, reversible, and needs no handler/schema changes** — at most a few lines in `ponder.config.ts`.

This doc is the operational playbook. For *what/why/how it works*, see `README.md`.

---

## The ladder (each rung is reversible)

| Rung | What | Risk | Delivery |
|---|---|---|---|
| 0. **Parity** | Prove Portal data == your RPC data, byte-for-byte | none (read-only) | `harness/compat` |
| 1. **Backfill-only boost** | Historical sync via Portal; realtime unchanged | low | `portalTransport` (config-only) |
| 2. **Native** | Full ~8× via the `HistoricalSync` seam | medium | `withPortal` + injection |
| 3. **Managed** | Dedicated Portal CU + SLA, CI gates, dashboard | — | service |

Rollback at any point = delete the lines you added. Realtime risk never changes (it's always your RPC).

---

## Step 0 — Compatibility report (always run first)

Tells you which sources Portal can serve **today** (logs, factories, transactions) vs. what's blocked (receipts, traces, block/account sources), and whether each chain has a Portal dataset.

```bash
cd <your-ponder-project>
PORTAL_API_KEY=<key> node --experimental-strip-types <path>/harness/compat/report.ts ./ponder.config.ts \
  [--differential <your-rpc-url>]    # optional: proves Portal-vs-RPC log parity on a sample
```

Read the verdict:
- **READY** — every source is log/factory/transaction-based → safe to boost the whole app.
- **PARTIAL** — boost the ready chains/sources now; leave the rest (receipts/traces) on RPC until those land.
- **BLOCKED** — no Portal dataset for your chains, or all sources need unimplemented features.

The report is also the onboarding artifact: keep it, and re-run it after any feature lands.

---

## Step 1 — Backfill-only boost, config-only (`portalTransport`)

**Works on any Ponder version, including 0.15.x — no fork, no patch, nothing to deploy.** A viem Transport that serves `eth_getLogs`/`eth_getBlockByNumber` from Portal and falls back to your RPC for everything else (receipts, traces, state, realtime).

```ts
// ponder.config.ts
import { createConfig } from "ponder";
import { portalTransport } from "@your-org/ponder-portal";

export default createConfig({
  chains: {
    mainnet: {
      id: 1,
      rpc: portalTransport({
        dataset: "https://portal.sqd.dev/datasets/ethereum-mainnet",
        fallbackRpc: process.env.PONDER_RPC_URL_1!, // realtime + everything Portal doesn't serve
      }),
    },
  },
  contracts: { /* unchanged */ },
});
```

That's the entire change. Trade-off: the orchestrator still drives per-request granularity, so this won't hit the native ~8× — but it kills the `eth_getLogs` fan-out and warms per-block lookups from a range cache, and it's in-process (vs. deploying eRPC + the rust shim). **This is the recommended starting point and the right answer for 0.15.x.**

Validate, then leave it: re-run the differential, watch the first backfill, compare wall-clock.

---

## Step 2 — Native seam, full performance (`withPortal` + injection)

For the clean ~8× (read-ahead chunk buffer, parallel prefetch, block-density auto-scaling), the Portal sync must replace Ponder's internal `createHistoricalSync`. Two client-side lines + an injection mechanism.

```ts
// ponder.config.ts
import { createConfig } from "ponder";
import { withPortal } from "@your-org/ponder-portal";

export default createConfig(withPortal({
  chains: {
    mainnet: { id: 1, rpc: process.env.PONDER_RPC_URL_1!, portal: "https://portal.sqd.dev/datasets/ethereum-mainnet" },
  },
  contracts: { /* unchanged */ },
}));
```

`withPortal()` records `portal` per chain in a registry (Ponder drops unknown chain fields, so we don't need a core type change). The injection then wraps `createHistoricalSync`:

```ts
chain-has-portal ? createPortalHistoricalSync(params) : createHistoricalSync(params)
```

Two ways to inject (both centralized in one versioned package — *this* is why a package beats N forks):
- **patch-package** (postinstall): a ~3-line version-matched patch to `runtime/historical.js` + the `portal.ts` module. Auto-applies on install. The patch is tiny because the seam is stable (see version matrix).
- **`module.register` load hook** (`ponder` → `ponder-portal` in your `package.json` scripts): wraps the internal `createHistoricalSync` at runtime, no installed files touched.

Use Step 2 for indexers where backfill time is the bottleneck and the ~2× of the Transport isn't enough.

---

## Choosing Transport (1) vs Native (2)

- On **0.15.x**, or where you can't patch/hook: **Transport**. It's the only config-only option and works everywhere.
- Where you want max speed and can add an install step: **Native**.
- Both keep realtime on `rpc` and handlers untouched. You can start on Transport and move to Native later without touching handlers.

---

## Validation & rollback

- **Parity:** `report.ts --differential <rpc>` compares Portal vs RPC `eth_getLogs` on a sample; for deeper checks, diff the `ponder_sync` tables of a Portal run vs an RPC run over the same range (`harness/compare/differential.ts` is the pattern).
- **Watch the first backfill:** wall-clock to finalized, and 0 client-facing errors.
- **Rollback:** Transport → restore `rpc: <url>`. Native → remove `withPortal(...)` + the package/patch. Instant, no data migration (the `ponder_sync` cache is RPC/Portal-agnostic).

---

## Version coverage

The `HistoricalSync` seam (`syncBlockRangeData`/`syncBlockData`) is **identical in shape from 0.15.17 → 0.16.6**, so one Portal implementation covers them. The package should ship a CI matrix that builds + runs the regression suite against each supported Ponder version, so an upgrade can't silently break a client. The Transport path has **no** version coupling (pure public `rpc: Transport` API).

---

## Honest gaps & roadmap

Today the Portal backfill serves **logs, log-factories, transactions**. Not yet: **receipts**, **traces** (Parity→callTracer transform prototyped, not wired), **block-interval** and **account/transfer** sources. The compatibility report flags any source that needs these. Priority order to widen coverage: receipts → traces → block/account sources. Longer term: propose a documented `historicalSync` provider hook upstream so future Ponder versions need no injection at all.
