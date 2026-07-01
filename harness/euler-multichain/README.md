# Euler multichain — flagship e2e

Index **every Portal-supported Euler V2 chain (15) in a single Ponder app**, full history
`[0 → finalized head]` per chain, streaming from the **SQD Portal** backfill into **Postgres**. This
is the production shape of an Euler indexer and a stress test of the `@subsquid/ponder` fork: one
shared Portal endpoint, one database, fifteen chains backfilling concurrently.

## What it indexes
- The `eVaultFactory` (`GenericFactory` — `ProxyCreated` → child EVaults) per chain.
- The 6 EVault events — `Deposit, Withdraw, Borrow, Repay, Liquidate, VaultStatus` — log-based,
  exactly how the real euler-subgraph indexes (no receipts; `event.transaction.hash` for IDs).

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

## How it works (no secrets in the repo)
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

See **[REPORT.md](./REPORT.md)** for a captured full run — throughput, per-chain reach, and the
saturation analysis.
