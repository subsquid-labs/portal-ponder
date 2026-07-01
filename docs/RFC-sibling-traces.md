# RFC: `transactionTraces` relation on the EVM trace filter

**To:** SQD Portal / Network / Query-engine team
**From:** Portal-Ponder fork maintainers (`@subsquid/ponder`)
**Status:** Draft
**Affects:** `sqd-query` (repo `subsquid/data`), `subsquid/sqd-portal`, `subsquid/worker-rs`
**Re-ingest required:** **No** (query-execution change only)

---

## 1. Summary

Add one boolean relation to the EVM **trace** request — `transactionTraces` — that, for every filter-matched trace, also returns the **other traces of the same transaction** (i.e. all traces of any transaction containing ≥1 matching trace).

It is the traces→traces mirror of the two relations that already exist:
- `logs[].transactionTraces` — a matched log → all traces of its tx,
- `traces[].transactionLogs` — a matched trace → all logs of its tx.

Mechanically it is a **semi-join on `(block_number, transaction_index)`** using the existing `Rel::Join` path. No new storage, no data-format change, no re-ingest, no query-planner change.

## 2. Motivation

Consumers that need a trace's **position in the full call tree** cannot get it from a filtered trace stream today.

Concretely: [Ponder](https://ponder.sh) (and the `@subsquid/ponder` Portal fork) assign each trace a `trace_index` = **pre-order DFS rank over the transaction's entire call tree**, computed *before* any filtering. When a client pushes a trace filter (`callTo`/`callFrom`/`callSighash`) to Portal, the worker returns **only the matched subset**, so the client sees a lone matched trace and can only rank it as index `0` — when its true full-tree position might be `7`. The index is part of Ponder's trace primary key and event IDs, so this is a correctness gap (indexes diverge from a JSON-RPC `debug_traceBlock` backfill).

Existing relations do **not** solve it:
- `subtraces` / trace parents return only **descendants / ancestors** of the matched trace — never its left-siblings' subtrees, whose sizes are exactly what the pre-order index depends on.
- `transactionTraces` on the **log** filter solves it only for indexers whose match is a log, not a trace.

**Today's workaround (in the fork): drop the server-side trace filter and fetch *all* traces of *all* transactions in the range**, then filter client-side. It is correct but costs a **~100× trace-data / CU overfetch** and forces a much smaller chunk size. Measured on the fork: trace-heavy backfills become the one path where Portal's efficiency advantage collapses.

`transactionTraces` keeps server-side filtering (row-group/page pruning still prunes on the *matching* scan) and expands only the matched transactions' trees. The ~100× overfetch collapses to "traces of matched txs only."

## 3. Proposed API

On the EVM trace request object (alongside `transaction`, `transactionLogs`, `subtraces`):

```jsonc
{
  "traces": [
    {
      "type": ["call"],
      "callTo": ["0x…"],
      "callSighash": ["0x…"],
      "transactionTraces": true   // ← NEW: also return sibling traces of the matched tx
    }
  ],
  "fields": { "trace": { "callFrom": true, "callTo": true, "traceAddress": true, /* … */ } }
}
```

Naming follows the existing convention: `transactionTraces` on the trace filter is symmetric with the existing `transactionLogs` (trace→tx-logs) and with `logs[].transactionTraces` (log→tx-traces). Default `false` (fully backwards-compatible in intent; see §6 for the wire-compat caveat).

**Output semantics (no change needed by us):** the worker already re-sorts each result table by its primary key; for traces that is `(block_number, transaction_index, trace_address)`, and lexicographic `trace_address` order **is** pre-order DFS. So the returned traces of a matched tx already arrive in DFS order — the consumer reconstructs `trace_index` by trivially enumerating each `(block_number, transaction_index)` group.

## 4. Technical design

The query DSL + execution engine live in `subsquid/data`, crate **`sqd-query`** (`crates/query`); both `worker-rs` and `sqd-portal` depend on it. Trace filtering runs **only** on the worker, against `traces.parquet`.

The change is a verbatim copy of the existing `transaction_logs` relation, retargeted to the `traces` table.

**(a) `crates/query/src/query/eth.rs`** — add one field to `TraceRequest` (near `eth.rs:569-590`):

```rust
pub transaction_traces: bool,   // JSON: transactionTraces
```

and in `TraceRequest::relations` (near `eth.rs:624-645`), one block mirroring `transaction_logs` (`eth.rs:632-638`) but joining `"traces"`:

```rust
if self.transaction_traces {
    scan.join(
        "traces",
        vec!["block_number", "transaction_index"],
        vec!["block_number", "transaction_index"],
    );
}
```

That is the whole functional change. `requires_traces()` (`eth.rs:697-701`) is already `true` whenever `traces[]` is non-empty, so it needs no change.

**Why it's cheap and safe:**
- `scan.join(...)` builds a `Rel::Join` (`plan/plan.rs:786-849`); `eval_join` (`plan/rel.rs:96-132`) is a polars **semi-join** on the key columns — it reads the matched rows' `(block_number, transaction_index)`, reads the `traces` table's key columns + `row_index` in memory, and emits the matching row indexes. It does **not** depend on physical row contiguity, so the traces' filter-columns-first on-disk sort order (`data/crates/data/src/evm/tables/trace.rs:51`) is irrelevant.
- Self-join (`input == output == "traces"`) is fully supported: the cached `traces.parquet` (`scan/parquet/chunk.rs:30`) is scanned twice; matched rows + sibling rows both flow into the traces `RowList`, a `BTreeSet<RowIndex>` (`plan/row_list.rs:9`) → auto-sorted + deduped. `PlanBuilder::simplify()` early-returns for the normal filtered case (`plan.rs:614`), and `is_full_rel` is false here, so no mis-optimization.
- `transaction_index` is already a **stored, stats-indexed** column on traces (`trace.rs:7-46,54`), so the join key needs no new data.

**(b) `worker-rs`** — bump the `sqd-query` dependency (`Cargo.toml:68`) to a rev containing (a). No code change: `run_query`/`compile`/`execute` are generic (`controller/worker.rs:108-183`).

**(c) `sqd-portal`** — bump the `sqd-query` dependency (`Cargo.toml:76`, currently an older rev than the worker) to a rev containing (a). **Mandatory**: the portal strictly parses the query (`types/request.rs:35`) and re-serializes from the typed struct when it strips `parentBlockHash` on non-first chunks (`request.rs:100`); an unknown field is rejected with HTTP 400 (`http_server.rs:1103`) *before any worker is hit*, and would otherwise be dropped on re-serialization. The portal then forwards the query verbatim, so no other portal logic changes. This also enables the feature on the **hot-block path** for free — `data/crates/hotblocks` reuses the same `Plan::execute` (`crates/hotblocks/src/query/running.rs:196`).

**(d) `qplan`** — **no change.** The JSON trace query path uses `PlainQuery`→`sqd-query`; qplan is only on the SQL path (`worker.rs:186 execute_sql_query`).

**(e) data format / index** — **no change, no re-ingest.** The self-join runs against existing parquet; cost ≈ the existing `transactionTraces` (on logs) join.

## 5. Cost / performance

- Server-side predicate pruning (row-group + page stats, `scan/parquet/file.rs:85-141`) still applies to the **matching** scan, so we still skip the vast majority of trace rows.
- The added work is one in-memory semi-join reading the `traces` key columns for the chunk + emitting sibling rows for matched txs — the same shape as the already-shipped `transactionTraces`/`transactionLogs` joins.
- Net: replaces the fork's current "all traces of all txs" (~100×) with "all traces of **matched** txs" — a large CU/bandwidth reduction for every trace-filtered consumer, not just Ponder.

## 6. Rollout & compatibility

This is the only non-trivial part.

- **Version signal:** the sole mechanism is the worker semver in `Heartbeat.version` (`sqd-network/crates/messages/proto/messages.proto:22`); portals can filter workers by version (`specs/network-rfc/04_network_communication.md:81`), and the scheduler assigns a global worker **min-version**. There is **no per-query API-version or per-feature capability field** (`Query` proto `messages.proto:31-43`).
- **`deny_unknown_fields` ⇒ hard break, non-graceful.** The `request!`/`field_selection!` macros stamp `#[serde(deny_unknown_fields)]` (`crates/query/src/query/util.rs:19,46,72`). A query carrying `transactionTraces` sent to an **un-upgraded portal** → immediate HTTP 400; to an **un-upgraded worker** → `BadRequest`, which the portal treats as **non-retriable** (`controller/stream.rs:1076-1085`) — it does not re-route to another (upgraded) worker holding the same chunk, and surfaces 400. So a client cannot safely use the flag until **every** worker serving the dataset **and** the portal are upgraded.

**Proposed rollout:**
1. Land `transaction_traces` in `sqd-query` (behind the field default `false`).
2. Release worker + portal builds that pin the new `sqd-query` rev.
3. Roll the worker fleet for the target dataset(s); raise the scheduler's global worker **min-version** past the release once coverage is complete.
4. (Optional, ideal) add a **per-feature capability** to the heartbeat / a **query API-version** field so clients can negotiate and portals can route trace-`transactionTraces` queries only to capable workers — this would make the feature (and future query-API additions) safe to ship without a fleet-wide flag day. Out of scope for the minimal change, but recommended as the durable fix for the "hard-break on any query-API extension" problem.

Until step 3 completes for a dataset, `@subsquid/ponder` keeps its existing safe default (fetch-all-traces workaround) and only sends `transactionTraces` when a per-dataset capability is confirmed.

## 7. Alternatives considered

- **Client fetches all traces (status quo workaround).** Correct; ~100× overfetch. What this RFC removes.
- **`subtraces` / parents relations.** Return descendants/ancestors only — cannot reconstruct pre-order index (miss left-sibling subtree sizes).
- **A precomputed per-trace DFS index column at ingest.** Would remove the join entirely, but requires a data-format change + full re-ingest across the network — far more expensive than a query-time self-join, and the join is already `O(traces in chunk)` in memory.
- **Shim-side expansion in `sqd-portal-evm-api`.** Possible but re-introduces per-request fan-out at the layer Portal is designed to avoid; the worker-side join is strictly better and benefits all clients.

## 8. Impact & non-goals

- **Benefits any trace consumer** needing full-tree context (call-tree reconstruction, trace ordering, geth-parity indexes), not just Ponder.
- **Non-goals:** changing trace storage/sort order; adding a DFS-index column; the per-feature capability negotiation (recommended separately in §6.4).

## Appendix: reproduction of the divergence

`@subsquid/ponder`'s byte-identity differential (`harness/diff/`) over eth mainnet `[22200000, 22200030]` shows, with server-side trace filtering, the same matched trace stored at `trace_index = 0` (Portal, filter-local) vs `7` (JSON-RPC `debug_traceBlock`, full-tree). Dropping the server-side filter makes them identical — at ~100× trace overfetch. `transactionTraces` would make them identical at the cost of one semi-join.
