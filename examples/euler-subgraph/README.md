# Euler subgraph → Ponder → Portal

A faithful port of the official **Euler V2 subgraph** ([`euler-xyz/euler-subgraph`](https://github.com/euler-xyz/euler-subgraph)) to Ponder, with the historical backfill routed through **SQD Portal** — i.e. the full *subgraph → Ponder → Portal* path. It exercises every migration concern in one example.

Run it with **zero config** — no `.env`, no keys — and it finishes in about a minute:

```bash
npm install && npm run dev
```

Two keyless data planes are wired by default: **history from the free public Portal**
(`portal.sqd.dev`) and the **realtime tip + `readContract` state reads from a public archive
RPC** (`eth.drpc.org` — archive, because the vault reads happen at historical blocks). Both are
shared and rate-limited under load — fine for this bounded demo, but for a longer backfill or
production, set your own RPC and widen the window:

```bash
PONDER_RPC_URL_1=<your-archive-rpc> PONDER_END=25436954 npm run dev
```

Set `PONDER_FULL=1` to run the complete history with no endBlock bound.

Verified (fresh clone, zero env, July 9, 2026): the default 120k-block window
(`22,681,265 → 22,801,264`) completed its SQD Portal backfill in **58s**. It discovered **39
vaults** from factory `ProxyCreated` logs, indexed **887 action rows** (504 deposits, 163
withdraws, 185 borrows, 35 repays, 0 liquidations), and derived **900 `VaultStatus` rows**. Vault
rows are inserted as soon as the factory emits `ProxyCreated`, so the visible vault count does not
depend on a later deposit/borrow or on public RPC metadata reads succeeding.

## See the result

After `npm run dev` reports the completed backfill, run:

```bash
npm run summary
```

Measured output from the default run:

```text
39 vaults · 504 deposits · 163 withdraws · 185 borrows · 35 repays · 0 liquidations · 900 vault status updates · 887 actions indexed from the SQD Portal
```

Or paste this query into `http://localhost:42069/graphql`:

```graphql
query DemoCounts {
  vaults {
    totalCount
  }
  deposits {
    totalCount
  }
  withdraws {
    totalCount
  }
  borrows {
    totalCount
  }
  repays {
    totalCount
  }
  liquidates {
    totalCount
  }
  vaultStatuss {
    totalCount
  }
}
```

The verified response returned `39` vaults, `504` deposits, `163` withdraws, `185` borrows, `35`
repays, `0` liquidations, and `900` `vaultStatuss`.

## How each subgraph construct maps

| subgraph (`euler-xyz/euler-subgraph`) | Ponder | notes |
|---|---|---|
| **GenericFactory template** — `EulerVaultFactory` data source emits `ProxyCreated(proxy,…)`, handler calls `EulerVault.create(proxy)` | **`EVaultFactory:ProxyCreated`** inserts the vault row, and **`factory({ address, event, parameter: "proxy" })`** still discovers child EVault logs | the key mapping; child EVault discovery, 1:1, with vault rows visible immediately. |
| EVault `Deposit/Withdraw/Borrow/Repay/Liquidate` events | `ponder.on("EVault:<Event>", …)` → immutable log tables | `id = `${txHash}-${logIndex}``, same as the subgraph. |
| **eth_calls** in `loadOrCreateEulerVault` (`asset/name/symbol/decimals/oracle/creator/EVC`, …) | **`context.client.readContract`** in the factory handler, best-effort per call | the subgraph's `.bind()`/`.try_*` → `readContract`. **Portal serves the logs; these state reads still hit `rpc`, and vault row creation does not depend on them.** |
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
