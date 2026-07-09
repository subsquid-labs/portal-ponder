# Uniswap → Ponder → Portal (all five source types)

Exercises **every Ponder source type in one app**, all backfilled through **SQD Portal**:

- **logs** — Uniswap V3 USDC/WETH 0.05% pool `Swap` events (dense volume),
- **receipts** — `includeTransactionReceipts` on the V3 pool,
- **traces** — `includeCallTraces` on the Uniswap V2 Router02 (geth `callTracer`),
- **block intervals** — a source firing every 1000 blocks (`BlockFilter`),
- **accounts** — transactions to/from WETH (`TransactionFilter`).

Run it with **zero config** — no `.env`, no keys — over a bounded 10k-block window
(`22,200,000 → 22,210,000`), finishing in ~1–2 minutes:

```bash
npm install && npm run dev
```

Two keyless data planes are wired by default: **history from the free public Portal**
(`portal.sqd.dev`) and the **realtime tip + state reads from a public archive RPC**
(`eth.drpc.org`). Both are shared and rate-limited under load — fine for this bounded demo, but
for a longer backfill or production set your own RPC and window:

```bash
PONDER_RPC_URL_1=<your-rpc> PONDER_START=22200000 PONDER_END=22300000 npm run dev
```

See [`.env.example`](.env.example) for all overridable values.
