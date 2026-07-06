# Deterministic bench kit

Reproducible, zero-external-RPC tooling for the 15-chain flagship benchmark of
`harness/euler-multichain`. The flagship app runs a bounded `[deploy, pinnedHead]` backfill where the
**SQD Portal carries all data**; the only RPC traffic in a clean run is, per chain, `1× eth_chainId` +
a handful of `eth_getBlockByNumber` for the startup anchors (latest, finalized-target, deploy, head).
This kit serves those anchors from a **committed snapshot of real chain headers** via a local shim, so
a bench run has no external RPC dependence and is reproducible from the snapshot alone. Everything is
**bash + node only** (the `pg`-touching tools use `import('pg')`, resolved from the app's
`node_modules` after `npm install`, exactly like `harness/chaos/pg-digest.mjs`).

```
capture-anchors.mjs   one-time capture of REAL headers (latest / finalized-target / deploy / head) → anchors-<date>.json
anchors-<date>.json   the COMMITTED reproducibility anchor (headers + host-only provenance + sha256 manifest line)
anchor-map.mjs        PURE request→pinned-header mapping (fail-loud on anything off the surface)  [unit-tested]
anchor-shim.mjs       local JSON-RPC server around anchor-map (one port, chain via /<id> or ?chain=); --selftest
metrics-parse.mjs     PURE /metrics Prometheus parse + run summary (completion, wall time, rpc counts)  [unit-tested]
emit-result.mjs       scrape /metrics → bench.result.json (wall time, per-chain blocks, rpc requests/errors)
emit-manifest.mjs     bench.manifest.json (tarball+sha, repo sha, chains/anchors sha, env-by-name, cgroup, pg_settings)
db-fresh.mjs          read-only preflight: is DATABASE_URL a fresh ponder store?
run-flagship.sh       the run driver: preflight → shim → ponder start → poll completion → emit result + manifest
parity-check.mjs      READ-ONLY equivalence of a bench DB vs a reference DB over ponder_sync (per-chain + totals)  [unit-tested]
```

## The shim contract (fail-loud)

`anchor-shim.mjs` serves ONLY, per chain:

- `eth_chainId` → the chain id from `chains.json`.
- `eth_getBlockByNumber` for the `latest` tag, the `finalized`/`safe` tags (mapped to the
  finalized-target header), and the **exact** block numbers pinned in the snapshot (deploy, head,
  latest, finalized-target — matched by canonical hex, so `0x00A` / `0xa` / `10` all resolve).

Any other method, tag (`earliest`/`pending`), or un-pinned block number returns a JSON-RPC error **and**
a loud `anchor-shim UNEXPECTED …` line on stderr — an unforeseen call is learned about, never served
junk. Ponder 0.16.6 fetches the finalized target **by number** (`latest − finalityBlockCount`), not by
the `finalized` tag; the tag is served defensively anyway. Finality counts: 65 eth / 200 polygon /
240 arbitrum / 30 default (`getFinalityBlockCount`).

## The end-capped invariant

`capture-anchors.mjs` asserts per chain that **`finalizedTarget ≥ head`** (i.e. snapshotted
`latest − finalityBlockCount ≥ pinnedHead`). This is what makes every chain present as end-capped at
startup, so the run stays a bounded `[deploy, head]` backfill and never enters realtime cutover. A
chain where it fails is reported loudly and (without `--allow-uncapped`) fails the capture — a snapshot
that would let a chain run unbounded is never silently shipped.

## Commands

```bash
# 1. one-time capture (FREE, public RPCs — NOT the bench). Writes anchors-<date>.json + the invariant table.
node harness/bench/capture-anchors.mjs                     # all 15 chains → harness/bench/anchors-<today>.json
node harness/bench/capture-anchors.mjs --only polygon      # one chain
node harness/bench/capture-anchors.mjs --allow-uncapped    # capture all + report failures instead of aborting

# 2. run the shim standalone (the driver can also start it)
node harness/bench/anchor-shim.mjs --anchors harness/bench/anchors-<date>.json --port 8645

# 3. drive a run (parameterized entirely by env; launch it inside your own supervisor / systemd-run scope)
BENCH_RPC_BASE=http://127.0.0.1:8645 ANCHORS_FILE=harness/bench/anchors-<date>.json \
SQD_PONDER_TARBALL=/path/to/subsquid-ponder-*.tgz DATABASE_URL=postgres://…/fresh_db \
PORTAL_URL=<portal-base> PORTAL_API_KEY=<key> \
  bash harness/bench/run-flagship.sh            # full 15-chain run
  bash harness/bench/run-flagship.sh --smoke    # single tiny chain (polygon) — validates the shim surface first

# 4. parity-check a bench DB against a reference DB (read-only)
node harness/bench/parity-check.mjs --bench postgres://…/bench --reference postgres://…/ref
```

### Run-driver env

| var | meaning | default |
|---|---|---|
| `BENCH_RPC_BASE` | shim base, e.g. `http://127.0.0.1:8645` (probed at preflight) | — (required) |
| `ANCHORS_FILE` | the snapshot the driver starts the shim from (if not already up) | — |
| `SQD_PONDER_TARBALL` | fork build under test | — (required) |
| `DATABASE_URL` | a FRESH postgres DB (driver refuses a dirty one) | — (required) |
| `PORTAL_URL` / `PORTAL_API_KEY` | Portal base + key (the only run-time data source) | — (required) |
| `BENCH_SCHEMA` / `BENCH_PORT` | ponder `--schema` / `--port` | `euler_bench` / `42069` |
| `BENCH_OUT_DIR` | where result/manifest/metrics land | `./bench-out` |
| `BENCH_POLL_SECONDS` / `BENCH_TIMEOUT_SECONDS` | completion poll interval / timeout | `15` / `9000` |
| `BENCH_LOAD` | free-text load-conditions note for the manifest | `<not recorded>` |

**Preflight guards (fail loud):** `EULER_CHAINS` must be UNSET for a full run (it silently subsets the
15 chains — `--smoke` is the one exception, and it sets `EULER_CHAINS=polygon` itself); `DATABASE_URL`
must be a fresh ponder store; the shim must be reachable. No box paths are hardcoded — the driver is
meant to run inside a caller's process supervisor and never calls `systemd-run` itself.

## Bench-mode transport switch

`harness/euler-multichain/ponder.config.ts` gains one env: when **`BENCH_RPC_BASE`** is set, every
chain's ONLY RPC transport is `${BENCH_RPC_BASE}/${chainId}` (the shim) — no `freeRpcs` fallback, fail
loud. With the env unset the config's behaviour is byte-for-byte unchanged.

## Tests

Pure logic is unit-tested with `node:test` (the harness convention); the shim also ships a `--selftest`
that spawns on a random port and exercises its whole surface (chainId / latest / finalized / safe /
pinned numbers / un-pinned number / unknown method / unknown chain / health) — CI-runnable, touches no
real service and no committed file.

```bash
node --test harness/bench/*.test.mjs          # 22 unit tests (anchor-map, metrics-parse, parity-check)
node harness/bench/anchor-shim.mjs --selftest  # 11 in-process HTTP checks
```
