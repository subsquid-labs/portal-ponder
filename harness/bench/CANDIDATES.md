# Production Ponder indexers — discovery catalog

A sweep of real open-source Ponder indexers, grouped by protocol and ranked for porting onto the Portal-backed backfill. Excludes Envio/HyperIndex (`config.yaml`), subgraphs, and tutorials.

**Cross-cutting findings**
- **No `awesome-ponder` list / showcase exists** (Ponder is now by Marble). The base must be assembled from `ponder-sh/ponder` `examples/` + GitHub code-search.
- **Config API split:** `≤0.10` uses `networks`/`transport`/`network:`; **`≥0.11` uses `chains`/`rpc`/`chain:`**. Our fork is 0.16.6, so 0.11+ apps drop in; older ones need a config migration.
- **Source-type rarity in the wild:** `includeCallTraces` appears in **one** public repo (sentio case_5); `includeTransactionReceipts` in ~two (gibsfinance, visualizevalue/mint); **`accounts:{}` in none**. Real DeFi indexers are overwhelmingly **logs + `factory()`** (+ occasional `blocks:{interval}`). → our synthetic `uniswap-portal` (all five source types) is the only way to bench receipts/traces/accounts together.

## Tier 1 — port first (modern API, Portal chain, self-contained pglite)

| Repo | ★ | ver | chain(s) | source types | why |
|---|---|---|---|---|---|
| **ponder-sh/ponder `examples/feature-*`** | (first-party) | ws | mainnet/sepolia/base | `accounts`, `blocks:{interval:5}`, `includeCallTraces`, `factory()` | canonical per-source-type units; **in this bench** |
| **marktoda/v4-ponder** | 18 | 0.12 | mainnet/**arb**/op/base/polygon | logs `filter:[Initialize,Swap]` singleton + token `readContract` | heavy, all Portal chains, newest API; **in this bench (base)** |
| **sentioxyz/open-blockchain-indexer-benchmark** | 22 | 0.16 | mainnet | `blocks` (pure), `blocks:{interval:1}`, **`includeCallTraces`** (case_5) | purpose-built bounded source-type matrix; Apache-2.0 |
| **visualizevalue/mint** | 39 | 0.16 | mainnet | **`factory()` + `includeTransactionReceipts`** | only production repo hitting factory+receipts together; MIT |
| **morpho-org/ponder-for-morpho-v1** | 7 | 0.15 | mainnet/base/arb/op/polygon | logs + **`factory()`** (Morpho Blue + MetaMorpho vaults) | heaviest self-contained lending; MIT |
| **Frankencoin-ZCHF/ponder** | — | 0.16 | mainnet/polygon/arb/op/base | logs + **`factory()`** (CDP positions) | official CDP, newest Ponder, pglite |
| **BasePaint/basepaint-ponder** | 29 | 0.16 | base | logs ×7 (NFT/art) | Base anchor; flip hardcoded postgres→pglite |

## Tier 2 — real, light setup (Postgres swap / monorepo / older API)

| Repo | ★ | ver | chain(s) | source types | caveat |
|---|---|---|---|---|---|
| nounsDAO/nouns-monorepo | 685 | 0.12 | mainnet | logs + `factory()` (Streams) + governor | needs `@nouns/sdk` build; deep history |
| Uniswap/the-compact-indexer | 48 | 0.10 | mainnet/base/op | logs + `blocks:{interval:1}` | old API; live at marble.live |
| Uniswap/hybrid-allocator / tribunal-indexer | 0–1 | 0.10 | mainnet/base/**arb**/op | logs + `blocks:{interval:1}` | cleanest blocks+arbitrum exemplar; old API |
| centrifuge/api-v3 | 4 | 0.16 | eth/base/arb/op + | many `factory()` + `blocks:{interval}` + bridge adapters | RWA anchor; **Postgres** + registry fetch |
| relayprotocol/relay-vaults | 0 | 0.16 | multi-L2 | `factory()` ×3 + `blocks:{interval:100}` + multichain | AGPL; Postgres hardcoded |
| ecp-eth/comments-monorepo | 23 | 0.12 | base | logs + **`includeTransactionReceipts`** | social + receipts; monorepo |
| mripani/stablecoin-apy-indexer | 1 | 0.8 | mainnet | `filter` + discovery + dynamic + `blocks:{interval:50}` | Aave+Morpho+Maple; old 0.8 API |
| smallyunet/prediction-market-indexer | 0 | 0.7 | polygon | logs (Polymarket CTF) | real prediction market; old API |
| networked-art/cryptopunks | 8 | 0.16 | mainnet | logs ×9 + in-handler oracle `readContract` | canonical oracle-read pattern |
| gibsfinance/bridge-routing | 0 | 0.14 | eth + | logs + **`includeTransactionReceipts`** | only other receipts user; Postgres |
| scope-sh/ponder-entrypoint | 16 | 0.3 | eth/arb/op/base/polygon | logs (ERC-4337 EntryPoint) | the AA/UserOp workload; old 0.3 API |

## Confirmed gaps (no production Ponder indexer found)
GMX · Synthetix · Gains · Hyperliquid · dYdX · Vertex · Drift · Aevo · options (Lyra/Premia) · Pendle · Gearbox · Notional · Yearn · EigenLayer · Symbiotic · Rocket Pool · ether.fi · Renzo · Spark · Sky/Maker · Blur · Reservoir · Sound.xyz · Lens · Safe/4337 bundlers · Across (toys only) · Stargate/Hop/Connext. These ship subgraphs/Envio/bespoke infra. Bridges that *do* have Ponder: Synapse (sanguine), Helix, Relay, Gibs.

_The bench currently runs the **in this bench** rows; the rest are the porting backlog._
