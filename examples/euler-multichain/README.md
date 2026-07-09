# Euler multichain → Ponder → Portal

A compact **multichain factory indexer**: Euler V2 EVault deposits/withdraws/borrows/repays/
liquidations across **Ethereum, Base, and Arbitrum** in one Ponder app, with each chain's
historical backfill routed through **SQD Portal**. One `factory()` per chain discovers the child
EVaults; the five action events are indexed into immutable log tables.

Run it with **zero config** — no `.env`, no keys — and it finishes in about a minute:

```bash
npm install && npm run dev
```

Two keyless data planes per chain are wired by default: **history from the free public Portal**
(`portal.sqd.dev`, dataset per chain) and the **realtime tip + state reads from public archive
RPCs** (`{eth,base,arbitrum}.drpc.org` — archive, because reads happen at historical blocks).
Both are shared and rate-limited under load — fine for this bounded demo.

By default the Ethereum leg reuses the verified live window
(`22,681,265 → 22,801,264`) and inserts vault rows directly from `ProxyCreated`, so first-run
output shows real vault discovery immediately. The Base and Arbitrum legs are wired identically
and keep their existing short factory-deploy windows; in the measured default run they completed
cleanly but produced 0 rows, so a later window probe should pick stronger non-mainnet demo ranges.
For a longer backfill or production, set your own RPC per chain and widen the window:

```bash
# your own RPCs (recommended beyond the demo) + full history on every chain
PONDER_RPC_URL_1=<eth-rpc> PONDER_RPC_URL_8453=<base-rpc> \
PONDER_RPC_URL_42161=<arb-rpc> PONDER_FULL=1 npm run dev
```

`PONDER_DEMO_SPAN` (default `200000`) sets the Base and Arbitrum demo windows;
`PONDER_FULL=1` backfills each chain's full recorded history instead. See
[`.env.example`](.env.example).

Verified (fresh clone, zero env, July 9, 2026): SQD Portal completed all three default backfills
in **55s**. GraphQL returned **39 vaults** and **887 action events**, all on the Ethereum leg
(504 deposits, 163 withdraws, 185 borrows, 35 repays, 0 liquidations); Base and Arbitrum returned
0 vaults and 0 events in their current short windows.

## See the result

Paste this query into `http://localhost:42069/graphql` after `npm run dev` completes:

```graphql
query MultichainCounts {
  vaults {
    totalCount
  }
  mainnetVaults: vaults(where: { chain: "mainnet" }) {
    totalCount
  }
  baseVaults: vaults(where: { chain: "base" }) {
    totalCount
  }
  arbitrumVaults: vaults(where: { chain: "arbitrum" }) {
    totalCount
  }
  vaultEvents {
    totalCount
  }
  mainnetEvents: vaultEvents(where: { chain: "mainnet" }) {
    totalCount
  }
  baseEvents: vaultEvents(where: { chain: "base" }) {
    totalCount
  }
  arbitrumEvents: vaultEvents(where: { chain: "arbitrum" }) {
    totalCount
  }
  deposits: vaultEvents(where: { type: "deposit" }) {
    totalCount
  }
  withdraws: vaultEvents(where: { type: "withdraw" }) {
    totalCount
  }
  borrows: vaultEvents(where: { type: "borrow" }) {
    totalCount
  }
  repays: vaultEvents(where: { type: "repay" }) {
    totalCount
  }
  liquidates: vaultEvents(where: { type: "liquidate" }) {
    totalCount
  }
}
```

The verified response returned `39` total vaults, `39` mainnet vaults, `0` Base vaults, `0`
Arbitrum vaults, `887` total events, and the per-action counts above.
