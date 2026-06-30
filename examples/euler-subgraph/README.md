# Euler subgraph → Ponder → Portal

A faithful port of the official **Euler V2 subgraph** ([`euler-xyz/euler-subgraph`](https://github.com/euler-xyz/euler-subgraph)) to Ponder, with the historical backfill routed through **SQD Portal** — i.e. the full *subgraph → Ponder → ponder-portal* path. It exercises every migration concern in one example.

Run (drop-in `@subsquid/ponder`; backfill from Portal, `readContract` + realtime on `rpc`):

```bash
PORTAL_URL_1=https://portal.sqd.dev/datasets/ethereum-mainnet \
PONDER_RPC_URL_1=<archive-rpc> PONDER_START=20529207 PONDER_END=20620000 \
ponder start
```

Verified: 4 vaults discovered via the factory (eUSDT-2 / eUSDC-2 / ewstETH-2, with on-chain
`asset`/`symbol`/`decimals`), deposits/borrows/withdraws indexed, the Counter aggregation, and
APY-derived `VaultStatus` — backfilled in ~1m over 91k blocks.

## How each subgraph construct maps

| subgraph (`euler-xyz/euler-subgraph`) | Ponder | notes |
|---|---|---|
| **GenericFactory template** — `EulerVaultFactory` data source emits `ProxyCreated(proxy,…)`, handler calls `EulerVault.create(proxy)` | **`factory({ address, event, parameter: "proxy" })`** in `ponder.config.ts` | the headline. child EVault discovery, 1:1. |
| EVault `Deposit/Withdraw/Borrow/Repay/Liquidate` events | `ponder.on("EVault:<Event>", …)` → immutable log tables | `id = `${txHash}-${logIndex}``, same as the subgraph. |
| **eth_calls** in `loadOrCreateEulerVault` (`asset/name/symbol/decimals/oracle/creator/EVC`, …) | **`context.client.readContract`**, gated by a `db.find(vault)` existence check | the subgraph's `.bind()`/`.try_*` → `readContract`. The existence check = the subgraph's once-per-vault cache; without it `readContract` hammers RPC. **Portal serves the logs; these state reads still hit `rpc`.** |
| `Counter` (load-or-create per action + a `"global"` singleton) | `db.insert(counter).onConflictDoUpdate(r => ({ value: r.value+1n }))` | no special singleton concept needed. |
| `VaultStatus.supplyApy/borrowApy` derived via `computeAPYs` (`src/utils/math.ts`) | a pure `computeAPYs()` in the handler | per-second RAY rate → APY, ported as-is. |
| `@derivedFrom` reverse relations (`AccountAggrVault.vaults`) | `relations()` — both sides defined explicitly (`vault` ↔ `deposit` shown) | Ponder has no auto-reverse; define the pair. |
| `dataSource.context()` static addresses (EVC + perspectives) | module constants / a `chainId → addresses` map | the context only ships static chain-level addresses. |

## Scope

This is a focused **core** port (factory + the 5 action events + `VaultStatus`/APY + vault metadata
+ Counter). The full subgraph additionally does per-account balance tracking (`EVC.getAccountOwner`,
cached in an `account` table), the EVC `CallWithContext` selector decoding, EulerEarn + EulerSwap
factories, and the perspectives registry — each maps the same way (more `factory()` + handlers +
`readContract`), omitted here to keep the example legible.
