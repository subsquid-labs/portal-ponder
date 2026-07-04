# VALIDATION.md — data-correctness evidence for portal-ponder

**Status: living document (skeleton + evidence to date).** portal-ponder is a fork of Ponder's
historical sync that backfills from the SQD Portal instead of (or alongside) JSON-RPC. This document
is the public evidence record a downstream integrator can read to decide how far to trust the fork's
data. Its guiding rule is **candor**: every divergence and open problem found so far is featured here,
not hidden, and every number below traces to a repo artifact or a public GitHub issue. Where a claim
is not yet proven, this document says so explicitly.

---

## 1. Purpose and trust model

### What "validated" means here

The correctness claim under test is narrow and mechanical:

> For the same app config and block range, a store backfilled through the **Portal** path is
> **byte-identical** to a store backfilled through the stock **JSON-RPC** path — the same rows, in the
> same tables, with the same field-level encodings — and it stays identical under interruption
> (crash/resume) and, where they overlap at finality, under realtime ingestion.

"Byte-identical" is measured at the sync-store row level across all five row families the fork
writes — **logs, transactions, receipts, traces, blocks** — plus, on the flagship and one factory
cell, an app-table checkpoint hash. The stock RPC path is treated as **ground truth**; independent
third-party sources (public archive nodes) are used to break ties when the two paths disagree.

### What is explicitly NOT claimed yet

This is a skeleton published while the validation campaign is in flight. As of this writing the
following are **pending** and must not be read as proven:

- The full paid **byte-diff matrix** (all cells in `harness/validate/cells.json`) — only the plumbing
  smoke cell has completed; the flagship full-range cell (`F-full`) is in progress.
- The **flagship benchmark gate** (backfill speed reproduced within a stated tolerance of the
  published baseline) — tracked separately in `BENCHMARKS.md`, not asserted here.
- A **long-duration soak** sign-off. The A/B soak differ runs hourly and its findings are already
  public issues (below), but no multi-day green-soak claim is made in this document yet.
- **Zero-RPC realtime** without an experimental label. The Portal `/stream` realtime path is under
  active hardening (see the stream-realtime issues and PR #26); this document does not certify it for
  unattended production use.

---

## 2. Methodology — the evidence layers

Correctness here is not a single test; it is a stack of independent layers, each catching a different
class of defect. A claim is only as strong as the layer that backs it.

### Layer A — Unit + invariant tests, on every supported upstream version

The Portal layer (`portal/`) is organised around **explicit, numbered invariants** (INV-1 … INV-16),
each with a stable, grep-able identity across **doc ⟷ code ⟷ test**. The catalog and its rationale
live in [`portal/INVARIANTS.md`](portal/INVARIANTS.md); the runtime asserts them under
`PORTAL_CHECKS` (`on` = O(1) tripwires that throw `InvariantViolation`; `strict` = additional
whole-structure O(n) checks used in CI), and the suite proves them with property-based tests
(`fast-check`, seed-pinned for deterministic runs).

The suite runs **against both supported upstream Ponder versions** — `0.16.6` and `0.15.17` — by
grafting the Portal layer onto a pinned Ponder checkout:

```bash
scripts/sync-upstream.sh 0.16.6  --test
scripts/sync-upstream.sh 0.15.17 --test
```

(config `portal/vite.portal.config.ts`, files `portal/*.test.ts`). The seam
(`HistoricalSync.syncBlockRangeData` / `syncBlockData`) is verified identical in shape across that
version range (`versions.json`).

### Layer B — Mutation-verified regression tests

Every fix in this repo ships with a regression test that **fails on the old code** — the test is run
against the pre-fix state to confirm it actually catches the defect it claims to. The invariant
catalog records these ("Mutation-verified: reverting the eviction fails …"), and the pull-request
history carries the evidence tables. This is the repo's standing review bar: a "green" claim is not
accepted without a test that would go red without the change.

### Layer C — Chaos kill-loop (crash/resume identity)

A bounded Portal backfill is repeatedly `SIGKILL`ed and resumed until the range completes; the
resumed store is then byte-diffed against a clean, uninterrupted baseline, and the sync intervals are
asserted to tile the range exactly (no gap, no overlap). Tooling: `harness/chaos/kill-loop.sh` +
`harness/chaos/verify-resume.sh` (+ a fault-injection proxy, `harness/chaos/proxy.mjs`). Results in
§4.

### Layer D — A/B dual-implementation soak with an hourly differ

Two independently-synced, production-shaped stores are compared hourly on their **finalized overlap
window**:

- **Leg A** — realtime ingestion over **JSON-RPC** (the conventional Ponder path).
- **Leg B** — realtime ingestion over the **Portal `/stream`** path.

The differ (`harness/soak-ab/ab-diff.mjs`) asserts, at finality: **logs** strict row-set + field
identity (the primary signal, must be 0); **blocks** strict row-set + field identity
(`total_difficulty` excluded); **transactions** exactly an expected class (leg B may be missing
*parent* transactions for realtime-ingested spans — each referenced by a leg-A log — and any
transaction present on **both** sides must be byte-identical); per-1000-block checkpoint-hash equality;
and `_ponder_checkpoint` monotonicity. Any divergence outside the pre-declared tolerated classes is a
hard failure. Because both legs ingest the *same* chain independently, a disagreement localises the
defect to one leg — and, so far, the divergences found have been **leg-A (RPC) defects**, with leg B
matching third-party evidence (§5).

### Layer E — Paid validation matrix vs stock RPC (ground truth)

Fork-vs-stock byte diffs across stratified block windows and app shapes, defined in
`harness/validate/cells.json` and run by `harness/validate/run-cell.sh` (and `ctrl-cell.sh` for the
inertness control). The stock RPC path is the oracle; a cell **PASSES** only on an exit-0 byte diff
across all row families (with the documented, narrow tolerances — e.g. `total_difficulty` excluded,
RPC-only inert event-less blocks reported-not-failed). Matrix status in §3.

### Layer F — Third-party spot confirmation

Where the RPC oracle and the Portal path disagree, the tie is broken against an **independent** public
archive node (and, per the campaign design, count-parity and field-level spot checks from public
explorers/analytics). Third-party evidence is treated as corroboration, not oracle: a mismatch must
reduce to a pre-declared benign class or it is a finding. This layer is what let the A/B differ
attribute its divergences to the RPC leg rather than the Portal leg (§5).

---

## 3. Validation matrix

One row per cell in [`harness/validate/cells.json`](harness/validate/cells.json). **Status** is
`DONE`, `IN PROGRESS`, or `PENDING` as of this writing. Repro commands reference only repo files and
documented environment variables; the paid cells require a Portal tarball and an RPC key supplied by
the operator (see `harness/validate/README.md`).

| Cell | App | Chain(s) | Sources | Windows | Status | Repro |
|------|-----|----------|---------|---------|--------|-------|
| `SMOKE` | erc20 | eth | logs, receipts | 1 tiny (12 blocks) | **DONE (SMOKE)** | `RPC_URL_OVERRIDE=<free-rpc> bash harness/validate/run-cell.sh SMOKE` |
| `CTRL` | erc20 | eth | logs, receipts | 10 × 1k (seeded) | PENDING | `UPSTREAM_PONDER_VERSION=0.16.6 bash harness/validate/ctrl-cell.sh CTRL` |
| `L-eth` | erc20 | eth | logs, receipts | 4×2k + 4×5k (seeded) | PENDING | `bash harness/validate/run-cell.sh L-eth` |
| `L-base` | erc20 | base | logs, receipts | 4×2k + 4×5k (seeded) | PENDING | `bash harness/validate/run-cell.sh L-base` |
| `L-arbitrum` | erc20 | arbitrum | logs, receipts | 4×2k + 4×5k (seeded) | PENDING | `bash harness/validate/run-cell.sh L-arbitrum` |
| `L-polygon` | erc20 | polygon | logs, receipts | 4×2k + 4×5k (seeded) | PENDING | `bash harness/validate/run-cell.sh L-polygon` |
| `L-bsc` | erc20 | bsc | logs, receipts | 4×2k + 4×5k (seeded) | PENDING | `bash harness/validate/run-cell.sh L-bsc` |
| `L-avalanche` | erc20 | avalanche | logs, receipts | 4×2k + 4×5k (seeded) | PENDING | `bash harness/validate/run-cell.sh L-avalanche` |
| `F-full` | euler | eth | factory, logs, transactions, receipts | full range [deploy → pinned head] | **IN PROGRESS** | `bash harness/validate/run-cell.sh F-full` |
| `F-base` | euler | base | factory, logs, transactions, receipts | 4×50k (seeded) | PENDING | `bash harness/validate/run-cell.sh F-base` |
| `F-arbitrum` | euler | arbitrum | factory, logs, transactions, receipts | 4×50k (seeded) | PENDING | `bash harness/validate/run-cell.sh F-arbitrum` |
| `F-bsc` | euler | bsc | factory, logs, transactions, receipts | 4×50k (seeded) | PENDING | `bash harness/validate/run-cell.sh F-bsc` |
| `F-avalanche` | euler | avalanche | factory, logs, transactions, receipts | 4×50k (seeded) | PENDING | `bash harness/validate/run-cell.sh F-avalanche` |
| `F-polygon` | euler | polygon | factory, logs, transactions, receipts | 4×50k (seeded) | PENDING | `bash harness/validate/run-cell.sh F-polygon` |
| `T-eth` | traces | eth | logs, receipts, traces | 20×300 (seeded) | PENDING | `bash harness/validate/run-cell.sh T-eth` |
| `A-eth` | traces | eth | logs, receipts, traces † | 20×1k (seeded) | PENDING | `bash harness/validate/run-cell.sh A-eth` |
| `A-base` | traces | base | logs, receipts, traces † | 20×1k (seeded) | PENDING | `POOL_ADDRESS=… ROUTER_ADDRESS=… bash harness/validate/run-cell.sh A-base` |
| `U-eth` | univ3 | eth | factory, logs, receipts | 10×500 (seeded) | PENDING | `bash harness/validate/run-cell.sh U-eth` |
| `E-eth` | euler | eth | factory, logs, receipts | ~12 edge windows (format eras, chunk-grid ±2, deploy floor, empty, frontier) | PENDING | `bash harness/validate/run-cell.sh E-eth` |
| `R` | (raw breadth) | eth, base, arbitrum, polygon, bsc, avalanche | raw | 30 spots / chain | PENDING | `harness/compare/differential.ts` (not `run-cell.sh`) |

Notes:
- **`SMOKE`** proved `run-cell.sh` end-to-end on public endpoints (§3.1). It is deliberately outside
  the paid matrix.
- **`CTRL`** is the *inertness* control: genuine upstream `ponder@0.16.6` vs the fork with the Portal
  path unset, same config → proves the Portal patch does not perturb the stock RPC path. It runs via
  `ctrl-cell.sh`, not `run-cell.sh`.
- **`F-full`** is the flagship byte diff — full Euler history on eth, `[20529207 → 25436954]`, head
  **pinned in `cells.json` for reproducibility**, every sync-store row diffed by the constant-memory
  `diff-batched.mjs` plus an app-table checkpoint hash. **In progress; no result claimed here yet.**
- **`A-eth` / `A-base`** († ): the traces app covers logs + receipts + traces but does **not** yet
  cover the *accounts* (tx from/to) or *block-interval* source types — a known, documented deviation
  (`sourceTypesNotCovered` in `cells.json`; `harness/diff/README.md`). `A-base` additionally requires
  explicit per-chain Pool/Router addresses and `run-cell.sh` refuses to run it without them.
- Window strategies (`seeded-random`, `chunk-grid`, `deploy-floor`, `format-era`, `frontier`,
  `full-range`) are expanded deterministically by `harness/validate/windows.mjs` (unit-tested), so
  every window is reproducible from its seed.

### 3.1 Matrix cell — SMOKE (DONE, 2026-07-04)

The plumbing smoke cell ran `run-cell.sh` end-to-end on the **public** Portal and a **free** public
RPC (no paid endpoints): **PASS**, with **byte-identical stores across all five row families**,
**136 matched logs**, **37 metered RPC requests**, **33 s** wall time. This validates the cell
runner, the request meter, and the byte-diff plumbing — it is a plumbing proof, not a matrix data
point.

Repro:

```bash
SQD_PONDER_TARBALL=<tarball> RPC_URL_OVERRIDE=<free-eth-rpc> \
  bash harness/validate/run-cell.sh SMOKE
```

---

## 4. Chaos / resume acceptance (Layer C) — ACCEPTED 2026-07-04

The chaos kill-loop was accepted against the campaign's acceptance criteria. Aggregate result:

- **203 `SIGKILL`s** delivered across **80 attempted / 41 completed** backfill runs.
- **41 / 41 clean resumes** — every completed backfill finished after being killed and resuming from
  its persisted `ponder_sync` state.
- Final stores **byte-identical** to an unkilled baseline across **logs, transactions, receipts,
  traces, blocks**.
- **Every sync interval fragment tiled the range exactly** (including the factory-discovery
  fragments) — no gap, no overlap.
- Zero `InvariantViolation` under `PORTAL_CHECKS=strict`.

Conditions: Poisson kill schedule (mean 30 s), `MIN_KILLS=2` enforced per completed backfill (a run
that finished without being killed proves nothing about resume and is rejected), chain 1 (ethereum)
range `[20529207, 20579207]`, build `0.16.6-sqd.2`, PGlite throwaway stores, **public** Portal +
**public** RPC endpoints.

Repro (per the harness; the loop is invoked repeatedly to reach the aggregate kill/resume counts):

```bash
# kill+resume a bounded Portal backfill until it completes (≥ MIN_KILLS kills required)
SQD_PONDER_TARBALL=<tarball> CHAOS_FROM=20529207 CHAOS_TO=20579207 \
TRIGGER=poisson-45s MEAN=30 MIN_KILLS=2 CHAOS_DB=<throwaway-store> \
  bash harness/chaos/kill-loop.sh

# then byte-diff the resumed store vs a clean baseline + assert intervals tile exactly
SQD_PONDER_TARBALL=<tarball> \
  bash harness/chaos/verify-resume.sh <throwaway-store> 20529207 20579207
```

`verify-resume.sh` refuses to diff against a baseline whose metadata (app / range / portal / tarball
content hash) does not match the chaos run, and refuses a chaos store that did not clear the kill
floor — so a stale baseline or an under-killed store cannot produce a silent false pass. The
fault-injection proxy (`harness/chaos/proxy.mjs`: 429 bursts, 5xx storms, TCP reset mid-NDJSON,
stalls, truncated gzip, malformed lines, finalized-head freeze/regression/flap) is available for the
adversarial-network scenarios and is unit-tested (`proxy.test.mjs`).

---

## 5. Findings log

Every divergence and anomaly surfaced by the layers above is recorded here with its public issue and
current state. **In cross-validation so far, the divergences found have been RPC-leg (leg-A) defects;
the Portal leg matched third-party evidence in each confirmed case.** This is a factual record of what
has been observed, not a claim that the Portal path is defect-free.

### 5.1 Cross-validation findings (A/B soak differ, Layer D)

| Issue | State | Finding | Attribution / third-party |
|-------|-------|---------|---------------------------|
| [#27](../../issues/27) | OPEN | RPC-mode realtime stores `access_list` as **NULL** for realtime-ingested typed txs — **including txs that had a real access list on chain**. Upstream Ponder tolerates a provider that omits the `accessList` key and persists NULL permanently. The Portal `/stream` leg **preserves** the access list. | Leg-A defect. Third-party confirmed via an independent public node; the Portal leg matches the real on-chain access list. Loud-fail hardening for the provider-omits-key case landed via **[#31](../../issues/31)** (merged). |
| [#32](../../issues/32) | OPEN | A **single** transaction with a **fabricated-empty** `access_list` (`[]`) in one of two independently-synced stores (chain 42161). | A single anomalous row surfaced by the differ; pinned as a known-bad row (see §5.3) so it does not mask new drift. |
| [#36](../../issues/36) | OPEN | RPC-mode realtime store **missing on-chain logs** (and block rows) of long-established addresses at **scattered recent blocks** — the loss is `(block, address)`-scoped. The Portal store is **complete**. | Leg-A defect. Third-party confirmed; the Portal leg holds the missing rows. |
| [#23](../../issues/23) | OPEN | RPC-mode realtime **deterministic crash** when a single block's full-block `eth_getLogs` response exceeds viem's 10 MiB body cap (dense blocks). The Portal `/stream` leg streams through such blocks. | Leg-A (RPC transport) failure mode; documented as an objective datapoint where the Portal path is more robust. |
| [#33](../../issues/33) | OPEN | Stream-realtime: a canonical block fataled with "unknown parent" where the parent **was** the canonical block at N−1 (no reorg involved). | Stream-realtime robustness finding on the **Portal** leg — recorded openly. Part of the stream-realtime hardening tracked by PR #26 and follow-ups; the stream path is under an experimental label until closed out. |

Two closed items for completeness:

- [#28](../../issues/28) — **CLOSED.** Stream realtime leaked an abort listener per `sleep()` call on
  the long-lived signal (a `MaxListenersExceededWarning` storm). Resolved.
- [#31](../../issues/31) — **MERGED.** Fail-loud when a provider omits `accessList` on typed
  transactions (the fix path for the #27 shape).

### 5.2 Stream-realtime correctness wave

PR **[#26](../../pull/26)** (merged 2026-07-04) landed a wave of stream-realtime correctness fixes:
same-block child logs, a finality anchor, reorg pruning of factory children, a finalized-head pin, and
population of parent transactions on the stream wire. The realtime path continues to be treated as
**experimental** pending the longer soak; #33 remains open against it.

### 5.3 Benign / tolerated diff classes (declared, bounded, removable)

The A/B differ tolerates a small, **explicitly enumerated** set of already-understood divergences so
that a *new* divergence is never masked. Each class is narrowly scoped, reported in the status JSON,
and designed to be removed once its underlying issue is fixed. The exact semantics are documented in
`harness/soak-ab/ab-diff.mjs`.

| Class | Scope | Why tolerated | Removal condition |
|-------|-------|---------------|-------------------|
| **realtime parent-tx gap** | leg B missing *parent* transactions for realtime-ingested spans, each referenced by a leg-A log; any tx present on **both** sides must be byte-identical | The stream wire did not carry parent txs for these spans (the verified pre-#26 wire gap). This is an availability gap on leg B, never wrong data — the shared txs are byte-identical. | Closes as the parent-tx population from PR #26 covers the span; a non-referenced onlyA tx, or any shared-tx byte diff, is a hard FAIL. |
| **access_list-null (#27)** | a single already-diverged **shared** tx whose *only* divergence is `access_list` (every other column byte-identical: ex-`access_list` md5s equal) | The RPC leg persisted NULL where the Portal leg has the real list (#27). Tolerated **only** while the divergence stays access_list-only. | If any *second* column ever diverges on that row, it stops being tolerated → hard FAIL. Removed when #27 is fixed. |
| **pinned known-bad row (#32)** | exactly one transaction hash on chain 42161 with the fabricated-empty `[]` shape | Isolates the single anomalous #32 row so it does not mask new drift. | The pin protects **only** the measured `[]`-vs-concrete-list shape — it does **not** tolerate an A-NULL / B-non-null drift or a B-side rot on that hash. Removed with #32. |
| **leg-A onlyB row-loss (#36)** | `onlyB` log/block rows (present in leg B, missing in leg A) at/above a per-chain realtime-era floor; **chain 1 only** (the only chain where the loss was observed) | Leg A silently lost on-chain rows leg B holds (#36); below the floor leg A's store came from the complete-by-construction historical backfill path. | A chain with `onlyB` rows but **no** configured floor is a hard FAIL (unknown chains are never default-tolerated). Removed when leg A is repaired or the leg is retired. |

**Candor about the limit of cross-validation.** Within a tolerated span the A/B differ *by itself*
cannot distinguish leg-A row loss (leg A dropped a real on-chain row) from a hypothetical leg-B
row fabrication — both surface identically as an `onlyB` row. What breaks the tie is the **third-party
spot audit** (Layer F): in the confirmed cases (#36, #27) leg B's rows matched an independent node
byte-for-byte, establishing leg A as the lossy side. The status JSON carries a bounded sample of
tolerated block numbers per table specifically to keep that audit reproducible.

---

## 6. Current status — what a reader can rely on today

**Proven today (with reproducible evidence in this repo):**

- The Portal layer's invariants (INV-1 … INV-16) hold under property-based tests **on both supported
  upstream Ponder versions** (`0.16.6`, `0.15.17`), and every fix is backed by a mutation-verified
  regression test.
- **Crash/resume is byte-safe** at the accepted chaos scale: 203 kills, 41/41 clean resumes,
  byte-identical to an unkilled baseline across all five row families, intervals tiling exactly, zero
  invariant violations (§4).
- The fork-vs-stock **byte-diff plumbing** is proven end-to-end by the SMOKE cell (byte-identical
  across all five row families on public endpoints, §3.1).
- The A/B dual-implementation soak is **actively cross-validating** the Portal path against the RPC
  path hourly, and every divergence it has found is a **public, tracked issue** (§5) — with the Portal
  leg matching third-party evidence in each confirmed case.

**Pending / not yet claimed:**

- The **full paid byte-diff matrix** (§3) — only SMOKE is DONE; `F-full` is in progress; the rest are
  PENDING. **This document will update the `F-full` row (and the others) as each cell completes.**
- The **flagship benchmark gate** (speed reproduced within tolerance of the published baseline) —
  see `BENCHMARKS.md`; not asserted here.
- A **multi-day green-soak** sign-off and **GA of the zero-RPC realtime (stream) path** — the stream
  path remains experimental while #33 and the longer soak are open.

Read this document as: *the mechanical byte-equality of the Portal historical-backfill path is
strongly evidenced and its crash-resume behavior is accepted; the full multi-chain paid matrix,
benchmark parity, and long-soak/stream GA are still in flight and are called out honestly above.*

---

## 7. Reproducing this evidence

All tooling is `bash` + `node` only (no extra dependencies) and lives in this repo:

| Evidence | Tool | Doc |
|----------|------|-----|
| Unit + invariant suite (both versions) | `scripts/sync-upstream.sh <ver> --test` | `CLAUDE.md`, `portal/INVARIANTS.md` |
| Chaos kill-loop + resume | `harness/chaos/kill-loop.sh`, `verify-resume.sh`, `proxy.mjs` | `harness/validate/README.md` §Chaos |
| Validation matrix (fork vs stock) | `harness/validate/run-cell.sh`, `ctrl-cell.sh`, `cells.json` | `harness/validate/README.md` |
| A/B soak differ | `harness/soak-ab/ab-diff.mjs` | `harness/validate/README.md` §Soak, `ab-diff.mjs` header |

Paid cells require operator-supplied endpoints (a Portal tarball and an RPC key); the harness meters
every request and enforces a cumulative budget guard (`harness/validate/budget.json`,
`budget-sum.mjs`). See `harness/validate/README.md` for the full environment contract.

---

*This is a living document. Sections 3 (matrix), 4 (chaos), and 5 (findings) are updated as the
campaign produces new evidence. Where a result is not yet in, the row says so — an empty result is
never presented as a pass.*
