# Integrating `createPortalHistoricalSync` into `@ponder/core`

Three localized edits turn `packages/portal-sync` into a drop-in core fork. Verified against `ponder-sh/ponder` HEAD (cloned). Realtime is untouched.

## 1. Config surface — `packages/core/src/config/index.ts`

Add an optional `portal` field to `ChainConfig` (~line 92):

```diff
 type ChainConfig<chain> = {
   id: chain extends { id: infer id extends number } ? id | number : number;
   rpc: string | string[] | Transport | undefined;
+  /** SQD Portal dataset URL for historical backfill. Realtime still uses `rpc`. */
+  portal?: string;
   ws?: string;
   ...
 };
```

Thread it through `Chain` in `internal/types.ts` (~line 299) and the config build in `build/config.ts`.

## 2. Selection — `packages/core/src/runtime/historical.ts:1224`

```diff
- const historicalSync = createHistoricalSync(params);
+ const historicalSync = params.chain.portal
+   ? createPortalHistoricalSync({
+       chainId: params.chain.id,
+       dataset: params.chain.portal,
+       metrics: params.common.portalMetrics,         // see §3
+       sources: toPortalSources(params.eventCallbacks.map((c) => c.filter), params.chain),
+       childAddresses: params.childAddresses,         // reuse Ponder's live factory map
+     })
+   : createHistoricalSync(params);
```

`toPortalSources(filters, chain)` maps Ponder's `Filter`/`Factory` (`internal/types.ts:54-183`) to this fork's `PortalSources`:
- `LogFilter` → `logFilters[]` (address + topic0..3)
- `Factory` → `factories[]` (factory address + discovery `eventSelector` + `childAddressLocation` → `child` rule + the child contract's event topic0s)
- `TraceFilter`/`TransferFilter` → `traceFilters[]`
- `includeTransactionReceipts` → `includeReceipts`

The runtime's adaptive `estimate()` (`runtime/historical.ts:1340`, range `[25, 100_000]`) feeds the Portal sync ever-larger intervals automatically, so throughput ramps as the backfill proceeds.

## 3. Observability — register Portal metrics

In `internal/metrics.ts`, construct a `PortalMetrics` and expose it on `common`. Append `portalMetrics.prometheus()` to the `/metrics` handler in `server/` so Portal stream/CU/worker-pressure counters sit beside Ponder's RPC metrics. (Native Ponder metrics are RPC-bucket-centric and cannot see these.)

## Block/tx/receipt split

This fork's `syncBlockRangeData` already fetches logs **and** blocks/txs/receipts/traces in the same range stream (Portal returns them inline), then `syncBlockData` is a light finalizer. To match Ponder's exact two-method contract, move the block/tx/receipt/trace writes into `syncBlockData({ interval, logs })` and have `syncBlockRangeData` request only `fields.log` — both are one Portal stream per interval either way.

## Backfill → frontfill handoff

Historical owns `[start, finalized]`, realtime owns `(finalized, tip]`. Portal's `/finalized-stream` never 409s, so backfill needs no reorg logic. If Portal's finalized head lags Ponder's target finalized block, the thin gap `(portalFinalizedHead, finalized]` falls back to the stock RPC `createHistoricalSync` (keep `rpc` configured for this + realtime + state reads). In practice the gap is small.

## Distribution

Publish as `@your-org/ponder-core`. Consumers swap one dependency and add `portal:` per chain in `ponder.config.ts`; **handlers and schema are unchanged.**
