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

## Validated at scale (Euler V2 factory suite, private portal + SQD RPC)

| range | logs | txs | receipts | blocks | byte-identical | Portal | RPC | speedup |
|------:|-----:|----:|---------:|-------:|:---:|------:|----:|--------:|
| 500k  | 1,904 | 699 | 699 | 695 | ✅ all paths | 40s | 155s | ~3.9× |
| 1M    | 18,473 | 6,904 | 6,904 | 6,768 | ✅ all paths | 58s | 709s | ~12× |

Portal scales sublinearly (range-scan); per-`getLogs` RPC degrades on the denser later blocks, so the advantage widens with range.
