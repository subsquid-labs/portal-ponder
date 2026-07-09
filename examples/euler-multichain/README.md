# Euler multichain → Ponder → Portal

A compact **multichain factory indexer**: Euler V2 EVault deposits/withdraws/borrows/repays/
liquidations across **Ethereum, Base, and Arbitrum** in one Ponder app, with each chain's
historical backfill routed through **SQD Portal**. One `factory()` per chain discovers the child
EVaults; the five action events are indexed into immutable log tables.

Run it with **zero config** — no `.env`, no keys — and it finishes in ~1–2 minutes:

```bash
npm install && npm run dev
```

Two keyless data planes per chain are wired by default: **history from the free public Portal**
(`portal.sqd.dev`, dataset per chain) and the **realtime tip + state reads from public RPCs**
(`*-rpc.publicnode.com`). Both are shared and rate-limited under load — fine for this bounded demo.
By default each chain indexes a ~200k-block window from its factory deploy. For a longer backfill
or production, set your own RPC per chain and widen the window:

```bash
# your own RPCs (recommended beyond the demo) + full history on every chain
PONDER_RPC_URL_1=<eth-rpc> PONDER_RPC_URL_8453=<base-rpc> \
PONDER_RPC_URL_42161=<arb-rpc> PONDER_FULL=1 npm run dev
```

`PONDER_DEMO_SPAN` (default `200000`) sets the per-chain block window; `PONDER_FULL=1` backfills
each chain's full recorded history instead. See [`.env.example`](.env.example).
