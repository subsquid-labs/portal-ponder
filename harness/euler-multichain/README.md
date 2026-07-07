# Euler multichain — full-history e2e

Index **every Portal-supported Euler V2 chain (15) in a single Ponder app**, full history
`[deploy → finalized head]` per chain, streaming from the **SQD Portal** backfill into **Postgres**. This
is the production shape of an Euler indexer and a stress test of the `@subsquid/ponder` fork: one
shared Portal endpoint, one database, fifteen chains backfilling concurrently.

## What it indexes
- The `eVaultFactory` (`GenericFactory` — `ProxyCreated` → child EVaults) per chain, with start blocks
  taken from **Euler's own subgraph config** (`euler-xyz/euler-subgraph`) and every factory address
  cross-verified against `euler-xyz/euler-interfaces` `CoreAddresses.json`.
- The **full 24-event EVault superset** (`Deposit/Withdraw/Borrow/Repay/Liquidate/Transfer/Approval/
  VaultStatus/InterestAccrued/DebtSocialized/PullDebt/ConvertFees/BalanceForwarderStatus/EVaultCreated`
  + all `GovSet*`), from the euler-interfaces ABI. Euler runs **both** ponder and subgraphs in prod and
  the public subgraph indexes only a subset (`Transfer/Borrow/Repay`), so a superset is the faithful
  apples-to-apples target. Log-based, no receipts; `event.transaction.hash` for IDs.
  Vault discovery is cross-checked against Euler's live Goldsky subgraph per chain (see REPORT.md).

## Chains (15)
`ethereum · binance · unichain · polygon · monad · sonic · tac · hyperliquid · base · plasma ·
arbitrum · avalanche · linea · bob · berachain`
(Swell + Morph are deployed but have no Portal dataset yet.)

## Run
```bash
docker compose up -d postgres
cp .env.example .env          # fill in PORTAL_API_KEY (+ SQD_RPC_KEY for the 10 SQD-served chains)
./run.sh
```
Postgres is the production DB path (separate process, own memory). Without `DATABASE_URL` it falls
back to in-process **pglite** — fine for a few chains, but a fast 15-chain backfill can OOM pglite
because its writer shares the Node heap. Use Postgres for the full run.

## How it works
- **`chains.json`** — public data only: dataset name, factory address, finalized head, SQD RPC slug,
  and keyless public RPCs. Secret-scanned clean.
- **`ponder.config.ts`** — reads `chains.json`, injects `SQD_RPC_KEY` + `PORTAL_API_KEY` from the
  environment, and declares one `EVault` contract across all 15 chains. `EULER_CHAINS=a,b` limits
  the set.
- Portal does the backfill per chain; the RPC (SQD-first, then a round-robin of public RPCs) is only
  for chain-id validation + the finality tail.

## Metrics — where it saturates
The fork writes a per-chain metrics file (`metrics/portal.<chainId>`) with the **saturation
breakdown**: `timing.{gateWaitMs, fetchMs, transformMs}` (concurrency back-pressure vs Portal I/O vs
NDJSON→Sync decode), `fetch.{bytes, http, dataChunks}`, and the live `portalGate` (adaptive
concurrency + buffered rows). `PORTAL_GATE_LOG=1` logs the AIMD concurrency + memory backpressure
live. Pair with Postgres `pg_stat_activity` wait-events to see whether the DB is I/O-bound,
CPU-bound, or *waiting on the client* (i.e. upstream decode is the limit).

### Hardware & wall-time
The Portal is **not** the bottleneck here — Ponder indexes on a single thread, so local decode and DB
write are, and that ceiling does not move with RAM or cores. The captured run capped the indexer at
**16 GB / 2 cores** on an otherwise large box, and that modest cap was **faster** than a 32 GB
configuration (**44m 55s** vs 67m 10s): extra heap and a deeper read-ahead buffer just sit idle when a
single CPU thread is draining them. The lever past this ceiling is **sharding** — splitting chains
across processes — not bigger hardware.

See **[REPORT.md](./REPORT.md)** for the captured full run — throughput, per-chain reach, the
saturation analysis, and the 16 GB vs 32 GB comparison.
