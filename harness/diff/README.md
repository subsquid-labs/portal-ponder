# Byte-identity test — Portal vs RPC

Proves the Portal backfill stores data **byte-identical** to the stock RPC backfill, across every path: **logs, transactions, receipts, traces**.

It indexes the same bounded range twice on `@subsquid/ponder` — once *with* `portal:` (the Portal path) and once *without* it (the stock RPC path; same package, the only difference is the backfill source) — into two separate `ponder_sync` stores, then diffs every row of `logs` / `transactions` / `transaction_receipts` / `traces`.

## Run

```bash
# needs an eth archive RPC that supports debug_traceBlockByNumber (for the trace path)
PONDER_RPC_URL_1=https://your-eth-archive-rpc  bash harness/diff/run.sh
```

Options:

```bash
PONDER_RPC_URL_1=…  \
PORTAL_URL_1=https://portal.sqd.dev/datasets/ethereum-mainnet  \  # default; or a dedicated portal
PORTAL_API_KEY=…  \                                               # for a keyed portal
  bash harness/diff/run.sh 22200000 22200300                      # explicit [start, end]
```

`SQD_PONDER_TARBALL=/path/to/subsquid-ponder-X.tgz` installs a local fork build (from `scripts/sync-upstream.sh`) instead of the published `@subsquid/ponder` — useful while iterating before a release.

Exit `0` = byte-identical; non-zero prints the divergent rows (per table) and fails — so it's CI-able and catches any drift in the log/tx/receipt/trace paths.

## Apps & env

The diff apps are no-op indexers — they exist to populate `ponder_sync`, which is what gets diffed
(their `ponder.schema.ts` defines only an empty `noop` table). All three are env-driven so the
validation harness can reuse a shape on any chain (defaults keep the standalone eth runs unchanged):

| app | shape | env knobs (defaults = eth) |
|---|---|---|
| `erc20-app` | logs + receipts (USDC Transfer) | `CHAIN_ID`, `ERC20_ADDRESS` |
| `euler-app` | factory + logs + txs + receipts (Euler EVault) | `CHAIN_ID`, `EULER_FACTORY` |
| `app` (traces) | logs + receipts + **traces** (V3 pool + V2 router) | `CHAIN_ID`, `POOL_ADDRESS`, `ROUTER_ADDRESS` |

### A-cell source coverage (documented deviation)

The campaign's `A-*` "all-source-types" cells are served by the traces app, which covers **logs +
receipts + traces**. It does **not** yet cover the **account** (tx `from`/`to`) or **block-interval**
source types — a purpose-built app for those two is still owed. This is a known, intentional
deviation; each `A-*` cell in `cells.json` records the real coverage in `sourceTypes` and the gap in
`sourceTypesNotCovered`. Do not read an `A-*` PASS as proof of account/block-interval parity.

> **`--app-hash` is not a determinism signal for these apps.** Because the apps write no user rows
> (only the `noop` table), `diff-batched.mjs --app-hash` reports an explicit **NO-USER-TABLES**
> verdict (non-zero exit), never a vacuous PASS. A real app-table determinism checkpoint needs an app
> that writes deterministic rows.

## Validated at scale (Euler V2 factory suite, private portal + SQD RPC)

| range | logs | txs | receipts | blocks | byte-identical | Portal | RPC | speedup |
|------:|-----:|----:|---------:|-------:|:---:|------:|----:|--------:|
| 500k  | 1,904 | 699 | 699 | 695 | ✅ all paths | 40s | 155s | ~3.9× |
| 1M    | 18,473 | 6,904 | 6,904 | 6,768 | ✅ all paths | 58s | 709s | ~12× |

Portal scales sublinearly (range-scan); per-`getLogs` RPC degrades on the denser later blocks, so the advantage widens with range.
