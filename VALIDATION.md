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

- The full paid **byte-diff matrix** (all cells in `harness/validate/cells.json`) — the plumbing smoke
  cell (§3.1), the flagship full-range cell (`F-full`, §3.2), the inertness control (`CTRL`, §3.3), and
  the eth Layer-L cell (`L-eth`, §3.4 — PASS under the documented `block.size` tolerance, #76) have
  completed; the remaining cells are still pending.
- The **flagship benchmark gate** (backfill speed reproduced within a stated tolerance of the
  published baseline) — tracked separately in a separate benchmarks document (pending publication),
  not asserted here.
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

The Portal layer (`portal/`) is organised around **explicit, numbered invariants** (INV-1 … INV-18),
each with a stable, grep-able identity across **doc ⟷ code ⟷ test**. The catalog and its rationale
live in [`portal/INVARIANTS.md`](portal/INVARIANTS.md); the runtime asserts them under
`PORTAL_CHECKS` (`on` = O(1) tripwires that throw `InvariantViolation`; `strict` = additional
whole-structure O(n) checks used in CI), and the suite proves them with property-based tests
(`fast-check`, seed-pinned for deterministic runs).

The suite runs **against all three supported upstream Ponder versions** — `0.15.17`, `0.16.6`, and
`0.16.7` — by grafting the Portal layer onto a pinned Ponder checkout:

```bash
scripts/sync-upstream.sh 0.15.17 --test
scripts/sync-upstream.sh 0.16.6  --test
scripts/sync-upstream.sh 0.16.7  --test
```

(config `portal/vite.portal.config.ts`, files `portal/*.test.ts`). The seam
(`HistoricalSync.syncBlockRangeData` / `syncBlockData`) is verified identical in shape across that
version range (`versions.json`), and CI's seam matrix (derived from `compat.tested`) runs all three on
every push. The newest, `0.16.7`, was registered by **[#74](../../pull/74)** (2026-07-06) on a
**seam-identity + full-suite** basis: the `0.16.6` wiring patch applies to the `0.16.7` tree
byte-identically (zero rejects, all 10 files) and the full Portal suite (**272/272**) passes on the
graft; the lone upstream delta `0.16.6 → 0.16.7` is a single DB-layer PR (`ponder-sh/ponder#2314`,
live-query notification batching) with **zero Portal-graft-surface overlap**. As with `0.15.17`, that
basis is **not** a fresh RPC byte-diff or cross-validation — the §3 / §5 byte-diff and A/B evidence
remains on `0.16.6`.

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
RPC-only inert event-less blocks reported-not-failed, and the upstream-dataset `block.size` off-by-one
at the RLP 2^16 boundary tolerated per [#76](../../issues/76)/[#77](../../pull/77), §5.3/§5.6). Matrix
status in §3.

### Layer F — Third-party spot confirmation

Where the RPC oracle and the Portal path disagree, the tie is broken against an **independent** public
archive node (and, per the campaign design, count-parity and field-level spot checks from public
explorers/analytics). Third-party evidence is treated as corroboration, not oracle: a mismatch must
reduce to a pre-declared benign class or it is a finding. This layer is what let the A/B differ
attribute its divergences to the RPC leg rather than the Portal leg (§5). **All Layer-F evidence to
date — the 15-chain count parity, the independent Goldsky vault cross-check, and the public-node
field-level tie-breaks — is consolidated in §5.7, together with a candid boundary on what was designed
but not executed.**

---

## 3. Validation matrix

One row per cell in [`harness/validate/cells.json`](harness/validate/cells.json). **Status** is
`DONE`, `IN PROGRESS`, `PENDING`, or `RETIRED` (a cell withdrawn from the matrix because its design
cannot produce evidence — the `F-*` seeded-window factory cells, §5.5). Repro commands reference only repo files and
documented environment variables; the paid cells require a Portal tarball and an RPC key supplied by
the operator (see `harness/validate/README.md`).

| Cell | App | Chain(s) | Sources | Windows | Status | Repro |
|------|-----|----------|---------|---------|--------|-------|
| `SMOKE` | erc20 | eth | logs, receipts | 1 tiny (12 blocks) | **DONE (SMOKE)** | `RPC_URL_OVERRIDE=<free-rpc> bash harness/validate/run-cell.sh SMOKE` |
| `CTRL` | erc20 | eth | logs, receipts | 10 × 1k (seeded) | **DONE (2026-07-06)** | `UPSTREAM_PONDER_VERSION=0.16.6 bash harness/validate/ctrl-cell.sh CTRL` |
| `L-eth` | erc20 | eth | logs, receipts | 4×2k + 4×5k (seeded) | **DONE (2026-07-06) — PASS under documented tolerance (#76)** | `bash harness/validate/run-cell.sh L-eth` |
| `L-base` | erc20 | base | logs, receipts | 4×2k + 4×5k (seeded) | **FAIL-FAST (by design), 2026-07-07 — #83** | `bash harness/validate/run-cell.sh L-base` |
| `L-base-logs` | erc20 | base | logs | 4×2k + 4×5k (seeded) | **PASS (16/16) on post-#110 build, 2026-07-10** — was DIFFER-FAIL 2026-07-07 (fabricated `"[]"`); the #110/#111 honest-NULL fix flips it byte-identical modulo the tolerated upstream `access_list` column gap (#83-family, §3.5.1, §5.6) | `bash harness/validate/run-cell.sh L-base-logs` |
| `L-arbitrum` | erc20 | arbitrum | logs, receipts | 4×2k + 4×5k (seeded) | PENDING — **blocked for receipts by #83** (logs-only variant pending) | `bash harness/validate/run-cell.sh L-arbitrum` |
| `L-arbitrum-logs` | erc20 | arbitrum | logs | 4×2k + 4×5k (seeded) | **PASS (8/8) on post-#110 build, 2026-07-10** — logs + transactions byte-identical modulo the tolerated `access_list` column gap (#83-family); the FAIL→PASS flip (was 2 PASS / 6 FAIL pre-fix) proves the #110/#111 fix (§5.6) | `bash harness/validate/run-cell.sh L-arbitrum-logs` |
| `L-polygon` | erc20 | polygon | logs, receipts | 4×2k + 4×5k (seeded) | **DONE (2026-07-08) — PASS: 7/7 comparable windows byte-identical incl. receipts; 4 windows RPC-oracle-incomplete (no byte-diff) — §3.6** | `bash harness/validate/run-cell.sh L-polygon` |
| `L-bsc` | erc20 | bsc | logs, receipts | 4×2k + 4×5k (seeded) | **DONE (2026-07-08) — PASS: 16/16 windows byte-identical incl. receipts, under the generalized size-only `block.size` tolerance (#107) — §3.7** | `bash harness/validate/run-cell.sh L-bsc` |
| `L-avalanche` | erc20 | avalanche | logs, receipts | 4×2k + 4×5k (seeded) | PENDING — **blocked for receipts by #83** (logs-only variant pending) | `bash harness/validate/run-cell.sh L-avalanche` |
| `L-avalanche-logs` | erc20 | avalanche | logs | 4×2k + 4×5k (seeded) | **PASS (8/8) on post-#110 build, 2026-07-10** — byte-identical modulo the tolerated `access_list` column gap (#83-family); §5.6 | `bash harness/validate/run-cell.sh L-avalanche-logs` |
| `F-full` | euler | eth | factory, logs, transactions, receipts | full range [deploy → pinned head] | **DONE (2026-07-06)** | `bash harness/validate/run-cell.sh F-full` |
| `F-base` | euler | base | factory, logs, transactions, receipts | 4×50k (seeded) | **RETIRED — vacuous by design (§5.5)** — same anchored-discovery flaw as `F-polygon` (discovery anchored at `window.from`); `F-full` is the sole real factory cell | `bash harness/validate/run-cell.sh F-base` |
| `F-arbitrum` | euler | arbitrum | factory, logs, transactions, receipts | 4×50k (seeded) | **RETIRED — vacuous by design (§5.5)** — same anchored-discovery flaw as `F-polygon`; `F-full` is the sole real factory cell | `bash harness/validate/run-cell.sh F-arbitrum` |
| `F-bsc` | euler | bsc | factory, logs, transactions, receipts | 4×50k (seeded) | **RETIRED — vacuous by design (§5.5)** — same anchored-discovery flaw as `F-polygon`; `F-full` is the sole real factory cell | `bash harness/validate/run-cell.sh F-bsc` |
| `F-avalanche` | euler | avalanche | factory, logs, transactions, receipts | 4×50k (seeded) | **RETIRED — vacuous by design (§5.5)** — same anchored-discovery flaw as `F-polygon`; `F-full` is the sole real factory cell | `bash harness/validate/run-cell.sh F-avalanche` |
| `F-polygon` | euler | polygon | factory, logs, transactions, receipts | 4×50k (seeded) | **RETIRED — vacuous by design (§5.5).** Ran 4/4 `pass=true` but **0=0 rows every table** (empty in-window factory scan, ~75 req/window) — not counted as a factory PASS | `bash harness/validate/run-cell.sh F-polygon` |
| `T-eth` | traces | eth | logs, receipts, traces | 20×300 (seeded) | PENDING | `bash harness/validate/run-cell.sh T-eth` |
| `A-eth` | traces | eth | logs, receipts, traces † | 20×1k (seeded) | PENDING | `bash harness/validate/run-cell.sh A-eth` |
| `A-base` | traces | base | logs, receipts, traces † | 20×1k (seeded) | PENDING | `POOL_ADDRESS=… ROUTER_ADDRESS=… bash harness/validate/run-cell.sh A-base` |
| `U-eth` | univ3 | eth | factory, logs, receipts | 10×500 (seeded) | PENDING | `bash harness/validate/run-cell.sh U-eth` |
| `E-eth` | euler | eth | factory, logs, receipts | ~12 edge windows (format eras, chunk-grid ±2, deploy floor, empty, frontier) | PENDING | `bash harness/validate/run-cell.sh E-eth` |
| `R` | (raw breadth) | eth, base, arbitrum, polygon, bsc, avalanche | raw | 30 spots / chain | PENDING | `harness/compare/differential.ts` (not `run-cell.sh`) |

Notes:
- **`SMOKE`** proved `run-cell.sh` end-to-end (§3.1). It is deliberately outside the paid matrix.
- **`CTRL`** is the *inertness* control (gate **G2**): genuine upstream `ponder@0.16.6` vs the fork
  with the Portal path unset, same config → proves the Portal patch does not perturb the stock RPC
  path. It runs via `ctrl-cell.sh`, not `run-cell.sh`. **DONE (2026-07-06): byte-identical across all
  row families on 10 / 10 windows — verdict, numbers, and chain of custody in §3.3.**
- **`L-eth`** is the eth Layer-L cell (erc20 on eth, `4×2k` + `4×5k` seeded windows). **DONE
  (2026-07-06): PASS under the documented `block.size` tolerance ([#76](../../issues/76)/[#77](../../pull/77))**
  — every preserved store byte-identical on `logs` / `transactions` / `transaction_receipts` /
  `traces` (strict tables), with a bounded, self-retiring count of tolerated `block.size` rows per
  window. The first run caught the upstream-dataset defect; the full record — first-run verdicts,
  the #78 differ-perf wedge, and the re-verdict — is in §3.4.
- **`L-base`** is the base Layer-L cell. It ran **2026-07-07** and **failed fast on 8 / 8 windows by
  design** — the `base-mainnet` Portal dataset does **not** serve the `transactions.logs_bloom` column
  a receipts backfill needs **below a ~45.4 M block boundary** (the gap is range-scoped, not chain-wide —
  the seeded windows sit below it), so the fork's dataset-completeness guard refuses the range rather than
  serving an incomplete receipt row. This is an **upstream dataset gap ([#83](../../issues/83))**, not a
  fork defect; the guard did exactly its job. The same gap affects `L-arbitrum` and `L-avalanche`
  (`arbitrum-one` / `avalanche-mainnet` gate it below their own boundaries — probe-confirmed served by
  460 M and 89.4 M respectively), so those
  two are **blocked for receipts** on the seeded windows and each has a **logs-only variant** (`L-base-logs` / `L-arbitrum-logs` /
  `L-avalanche-logs`) that drops receipts but keeps the *identical* windows for comparability. Full
  record — the 8-window run, the verbatim error, the probe table, and the disposition — in §3.5.
  The **`L-base-logs`** logs-only variant has since **run (2026-07-07)** and surfaced a *second*,
  distinct base-mainnet dataset gap in the same #83 family — the dataset also lacks
  `transactions.access_list` — and, on top of it, a genuine **fork defect**: the old fork *fabricated* an
  empty `"[]"` for the dropped column (the #27 anti-pattern), traced to being systematic on arbitrum and
  **FIXED** by [#110](../../pull/110)/[#111](../../pull/111). The load-bearing `logs` are byte-identical
  throughout; full record in §3.5.1 and §5.6.
- **`F-full`** is the flagship byte diff — full Euler history on eth, `[20529207 → 25436954]`, head
  **pinned in `cells.json` for reproducibility**, every sync-store row diffed by the constant-memory
  `diff-batched.mjs` plus an app-table checkpoint hash. **DONE (2026-07-06): byte-identical across all
  row families — verdict, numbers, chain of custody, and the app-hash caveat in §3.2.**
- **`F-base` / `F-arbitrum` / `F-bsc` / `F-avalanche` / `F-polygon`** (the seeded-window Euler
  factory cells) are **RETIRED — vacuous by design**. The euler diff app registers its factory with
  `startBlock = window.from`, so child-vault discovery is **anchored at each window's start** — a
  window yields child data only if a vault-creation fires *inside* it. Euler factories create vaults
  sparsely (polygon: ~25 vaults across 2.5 M blocks), so the seeded `4×50k` windows (~8 % coverage)
  miss the creations and both the Portal and RPC legs do the **same** empty in-window discovery → a
  trivial `0 = 0` "pass" that proves nothing about the Portal path. `F-polygon` demonstrated this
  empirically (4/4 windows, `0` rows every table, ~75 req/window — §5.5). **`F-full`** (eth,
  full-range from the factory-deploy floor, ~872 vaults — §3.2) is the sole cell that exercises real
  factory discovery; the dense **`L-*` `erc20` cells** are the reliable random-window path (any
  window hits dense transfers). These five cells are **withdrawn rather than run** — a paid run would
  only reproduce the vacuous scan.
- **`A-eth` / `A-base`** († ): the traces app covers logs + receipts + traces but does **not** yet
  cover the *accounts* (tx from/to) or *block-interval* source types — a known, documented deviation
  (`sourceTypesNotCovered` in `cells.json`; `harness/diff/README.md`). `A-base` additionally requires
  explicit per-chain Pool/Router addresses and `run-cell.sh` refuses to run it without them.
- Window strategies (`seeded-random`, `chunk-grid`, `deploy-floor`, `format-era`, `frontier`,
  `full-range`) are expanded deterministically by `harness/validate/windows.mjs` (unit-tested), so
  every window is reproducible from its seed.

### 3.1 Matrix cell — SMOKE (DONE, 2026-07-04)

The plumbing smoke cell ran `run-cell.sh` end-to-end against the **public** Portal dataset endpoint
and the operator's **metered RPC endpoint** (37 requests, trivial spend): **PASS**, with
**byte-identical stores across all five row families**, **136 matched logs**, **37 metered RPC
requests**, **33 s** wall time. This validates the cell runner, the request meter, and the
byte-diff plumbing — it is a plumbing proof, not a matrix data point. The cell also runs on a free
public RPC via `RPC_URL_OVERRIDE` (see repro command below).

Repro:

```bash
SQD_PONDER_TARBALL=<tarball> RPC_URL_OVERRIDE=<free-eth-rpc> \
  bash harness/validate/run-cell.sh SMOKE
```

### 3.2 Matrix cell — F-full (DONE, 2026-07-06)

The flagship paid cell: the **full recorded history of the Euler v2 app on eth mainnet** (chain 1),
range `[20529207, 25436954]` — **4,907,748 blocks**, head **pinned in `cells.json`** — backfilled two
ways and diffed at the sync-store row level. The Portal leg reads the SQD Portal; the stock leg is
genuine `ponder@0.16.6` over a **metered JSON-RPC endpoint**, treated as ground truth. Both legs ran
serially on the same host with production chunking (`PORTAL_CHUNK_BLOCKS=500000`) into PGlite stores.

**Verdict — byte-identical across every row family diffed:**

| Row family | Portal | Stock RPC | Result |
|------------|--------|-----------|--------|
| `logs` | 885,893 | 885,893 | identical |
| `transactions` | 276,674 | 276,674 | identical |
| `transaction_receipts` | 276,674 | 276,674 | identical |
| `traces` | 0 | 0 | identical (this app configures no trace source) |
| `blocks` | 252,396 | 252,396 | 252,396 shared, all match (event-bearing blocks) |

`RESULT_JSON {"fail":false}`.

**Speed.** The Portal backfill leg completed in **1819 s** vs the stock-RPC leg's **6543 s** — a
**3.6× speed-up** on the same host, same range, same store backend. This is a single serial run, so it
is a datapoint, not a benchmark gate; see `harness/bench/BENCHMARKS.md` for the run's caveats.

**Spend.** The stock-RPC leg made **576,207 metered JSON-RPC requests** (the `cells.json` advisory
estimate was 1,030,000; the real count was well under it), roughly $5–6 of metered spend. The Portal
leg is not RPC-metered.

**Chain of custody — stated plainly, because the honesty is the evidence.** This cell did **not**
complete via `run-cell.sh` end-to-end in one clean pass; it reached its byte-identical verdict through
a documented recovery. The full custody chain:

1. **First window attempt was killed at 60.5%.** An operator restart delivered a `SIGTERM` to the
   Portal backfill 60.5% through the range (1223 s in, 6 metered requests). The partial attempt is
   preserved in the cell's local attempt history (the per-cell `results/*.json` are gitignored operator
   artifacts, not committed).
2. **Second attempt — both backfills completed, then the in-cell differ wedged.** Both the Portal and
   the stock-RPC legs backfilled to completion. The **then-committed** `diff-batched.mjs` — whose keyset
   pagination did **not** follow the `chain_id`-prefixed sync-store primary keys — diffed `logs`
   byte-identical, then **wedged on `transactions`** (days of CPU with no forward progress). The run was
   frozen and both stores were copied to the operator's evidence archive before anything could be lost.
3. **The differ bug was root-caused and fixed on `main`.** The keyset-pagination defect is
   [#58](../../issues/58); the fix (make the ORDER BY and tuple-WHERE lead with the `chain_id`-prefixed
   PK so each page is a single forward index scan) merged as **[#59](../../pull/59)**.
4. **The verdict is the offline re-diff of the evidence copies with the fixed tool.** Re-running the
   fixed-keyset differ over the two archived stores produced the byte-identical result above. That
   fixed tool's **first** offline run itself **hung** — not on the keyset logic, but in PGlite 0.2.13's
   WASM allocator, which spins forever when a 50,000-row `select *` page carries ~300 MB of toasted
   input (detoast volume; [#63](../../issues/63)). The **same rows in 5,000-row pages** complete at ~1.5 s/page; re-paginated,
   the full re-diff of all tables finished in ~65 s (output-file create→last-write timestamps in the evidence archive). Both tool variants' `sha256` digests were recorded
   in the operator's evidence archive at run time (the fixed-keyset differ before the page-size change:
   `9d1756d5df7f0ee484dbd34ed2ba3ff55de21944dad872789b26c225a5bcccea`; after:
   `bbc93163b82cd63deb52761b35bd57361b5529834d9f4c277203c8897574b719`).
5. **Net.** The repro command in the matrix stays valid **going forward**: [#59](../../pull/59) removed
   the `transactions` wedge, and the detoast hang is durably fixed by the **byte-aware page sizing
   merged in [#72](../../pull/72)** (`diff-batched.mjs`, §5.5), which supersedes the interim fixed
   5,000-row page. A *fresh* end-to-end run is therefore now unobstructed — but it costs ~$6 of metered
   RPC and ~2.5 h of backfill, so the verdict above stands on the archived-store re-diff rather than on
   a re-run.

**App-hash caveat (a real limitation, not a pass).** `cells.json` sets `appHash: true` for F-full, but
the app-table determinism hash for this run was **vacuous**: the diff harness apps write **no
user-table rows** (only `ponder_sync`), so the hash was equal on both legs
(`d3cd7d216da429ef006bdeb611f0ffad`) purely because **zero** nonempty user tables existed on either
side. The app-table sub-claim is **not** satisfied by this run — the byte-identity above is proven at
the sync-store row level only. Follow-up: give the euler diff app a deterministic user table so a
real, nonempty checkpoint hash can be compared.

Repro (a fresh end-to-end run; the operator supplies the tarball and the metered RPC key — see
`harness/validate/README.md`):

```bash
SQD_PONDER_TARBALL=<tarball> SQD_RPC_KEY=<paid-rpc-key> \
  bash harness/validate/run-cell.sh F-full
```

### 3.3 Matrix cell — CTRL (DONE, 2026-07-06)

The **inertness control** (gate **G2**). Unlike every other cell, it does **not** test the Portal
path — it tests that leaving the Portal path **off** perturbs nothing: the fork with its Portal
environment **unset** is diffed against **genuine upstream `ponder@0.16.6`** at the same config over
the same live block windows, both legs reading the same **metered JSON-RPC endpoint**. A pass means
the only variable — *is the fork's patch present in the binary* — makes **no observable difference** to
the stock RPC path. This is the control that lets the paid cells attribute any Portal-vs-RPC
divergence to the Portal *path* rather than to the fork merely being a different build.

**Verdict — 10 / 10 windows byte-identical:**

- **10 of 10** seeded-random eth windows passed (`pass:true`), each **byte-identical across
  `logs` / `transactions` / `transaction_receipts` / `traces` and every event-bearing `block`** — the
  fork-with-Portal-unset store equals the upstream-`0.16.6` store exactly, on every window.
- **114,921 matched logs** across the 10 windows; **39,937 metered JSON-RPC requests** total.
- **0 auto-shrinks, 0 retry attempts** — every window passed on its first diff; the cell never had to
  shrink a window or re-run one.
- Per-window wall time **152–165 s** — a tight band with no outliers, consistent with two builds doing
  identical pure-RPC work.

**Chain of custody — clean, and that is the point.** This cell completed **straight through
`ctrl-cell.sh`** with no killed attempts, no wedged diffs, and no tolerated rows. The verdict is the
cell's own results document: ten window records, each `pass:true`, each recording a byte-identity diff
over all five row families. Because both legs are stock RPC and the *only* difference between them is
whether the fork's (unset) Portal patch is compiled in, an all-identical result **is** the claim:
**the Portal patch is inert when the Portal environment is unset, proven on live RPC across 10
seeded-random eth windows.**

Repro:

```bash
UPSTREAM_PONDER_VERSION=0.16.6 bash harness/validate/ctrl-cell.sh CTRL
```

### 3.4 Matrix cell — L-eth (DONE, 2026-07-06 — PASS under the documented #76 tolerance)

The eth **Layer-L** cell: the erc20 app on eth mainnet (chain 1), backfilled two ways (Portal vs a
**metered JSON-RPC** ground truth) and diffed at the sync-store row level across two seeded-random
window specs — **4 × 2 000 blocks (seed 101)** and **4 × 5 000 blocks (seed 102)** — plus the
harness's **auto-shrink** re-runs. **This is the cell where the gate earned its keep: it caught a real
upstream-dataset defect ([#76](../../issues/76)) and a real harness perf defect ([#78](../../issues/78)).**
The first run is recorded **as-is** below — failures and all — because a gate that reports only its
clean runs is not a gate.

*Reconstruction note ([#79](../../issues/79)).* The results document folds the four seed-101 2k
windows into the `attempts` arrays of the same-tagged seed-102 5k windows (a **tag collision** across
the two specs — both emit `rand#0…rand#3`), so the full run is recovered from **windows *and* attempts
together**. It is a results-labelling defect only: the budget sums and per-window verdicts are
unaffected (§5.5). Reconstructed that way, there are **11 recorded window records**.

**First run — recorded as-is (the pre-#77 differ).** Of the 11 records, exactly **one passed
byte-identical outright**; every other diff either **failed on `blocks` only** or **wedged** (#78). In
**every** blocks-only failure the `logs` / `transactions` / `transaction_receipts` / `traces` tables
were **byte-identical** and the sole differing column was **`block.size`**, always with
`rpc = portal + 1`, always on blocks whose canonical size is **≥ 65 540** — the signature of the
upstream-dataset off-by-one now tracked as [#76](../../issues/76).

| Window (spec) | Matched logs | First-run verdict | Differing field |
|---------------|-------------:|-------------------|-----------------|
| `rand#0` 2k `[19203200, 19205200]` | 13,117 | **PASS — byte-identical outright** | — |
| `rand#1` 2k `[20145674, 20147674]` | 16,911 | FAIL — blocks-only (9 rows) | `block.size` only |
| `rand#2` 2k `[19809831, 19811831]` | 28,741 | FAIL — blocks-only (1 row) | `block.size` only |
| `rand#3` 2k `[19963239, 19965239]` | 20,951 | FAIL — blocks-only (5 rows) | `block.size` only |
| `rand#0` 5k `[20028730, 20033730]` | 45,668 | FAIL — blocks-only (20 rows) | `block.size` only |
| `rand#1` 5k `[19813237, 19818237]` | 73,881 | **diff WEDGED, killed (#78)** | (not reached) |
| `rand#1+shrunk` `[19813237, 19815737]` | 41,078 | FAIL — blocks-only (5 rows) | `block.size` only |
| `rand#2` 5k `[20130205, 20135205]` | 53,799 | FAIL — blocks-only (11 rows) | `block.size` only |
| `rand#2+shrunk` `[20130205, 20132705]` | 22,088 | FAIL — blocks-only (4 rows) | `block.size` only |
| `rand#3` 5k `[20474824, 20479824]` | 77,468 | **diff WEDGED, killed (#78)** | (not reached) |
| `rand#3+shrunk` `[20474824, 20477324]` | 37,012 | FAIL — blocks-only (3 rows) | `block.size` only |

**The #78 wedge (a harness perf defect, not a data defect).** The two largest stores —
`rand#1` 5k (73,881 matched logs) and `rand#3` 5k (77,468 matched logs) — **wedged in the differ**: it
printed `logs … identical` and then made no forward progress. The `rand#1` live diff was **killed
after ~47 min** and both stores were **preserved**. Offline, that store's `logs` table alone takes
~2 min and the full diff exceeds 300 s, whereas the **entire** diff of the 53,799-log `rand#2` store
finishes in **6.7 s** — a pathological cliff between ~54 k and ~74 k matched rows, filed as
[#78](../../issues/78). The harness's **auto-shrink** bounded the cost of the largest windows by
re-running each on a smaller (half-size) leading window that diffs to completion — which is why
`rand#1+shrunk`, `rand#2+shrunk`, and `rand#3+shrunk` appear above.

**Re-verdict under the merged #77 tolerance.** [#77](../../pull/77) merged the precisely-scoped,
self-retiring `block.size` tolerance (§5.3, §5.6). The tolerant differ was re-run **over every
preserved store with no re-backfill** (a diff-only re-run against the preserved `ponder_sync`
stores): the eight stores whose first-run diff had completed re-verdicted with the **fixed cell
differ**, and the two monster stores that wedged it (#78) re-verdicted with the **byte-aware batched
differ** from [#72](../../pull/72), which sidesteps the #78 cliff by paged reads.
Result: **every store is byte-identical across `logs` / `transactions` / `transaction_receipts` /
`traces`**, with a small bounded count of tolerated `block.size` rows per window and the **strict
(non-block) tables byte-identical in every store**:

| Preserved store (window) | Matched logs | Tolerated `block.size` rows |
|--------------------------|-------------:|----------------------------:|
| `rand#1` 2k `[20145674, 20147674]` | 16,911 | 9 |
| `rand#2` 2k `[19809831, 19811831]` | 28,741 | 1 |
| `rand#3` 2k `[19963239, 19965239]` | 20,951 | 5 |
| `rand#0` 5k `[20028730, 20033730]` | 45,668 | 20 |
| `rand#1` 5k `[19813237, 19818237]` (monster store) | 73,881 | 10 |
| `rand#1+shrunk` `[19813237, 19815737]` | 41,078 | 5 |
| `rand#2` 5k `[20130205, 20135205]` | 53,799 | 11 |
| `rand#2+shrunk` `[20130205, 20132705]` | 22,088 | 4 |
| `rand#3` 5k `[20474824, 20479824]` (monster store) | 77,468 | 10 |
| `rand#3+shrunk` `[20474824, 20477324]` | 37,012 | 3 |

The two **monster stores** that wedged the old differ (#78) re-verdicted with the byte-aware batched
differ **byte-identical, 10 tolerated `block.size` rows each, in 8.3 s and 9.5 s** respectively
(73,881 logs / 52,670 txs and 77,468 logs / 51,533 txs) — a **>200×** recovery on exactly the stores
that provoked the wedge, which also validates the #78 fix direction (paged reads). The `rand#0` 2k
store `[19203200, 19205200]` (13,117 logs) needed no re-verdict: it passed **byte-identical outright**,
`block.size` included.

**Verdict — PASS under the documented tolerance.** **10 / 10 preserved stores pass under the #77
tolerance**, and with the one outright-clean window that accounts for **11 / 11 window records**. Every
first-run failure is **fully explained by #76** — a `block.size`-only, `rpc = portal + 1`,
size-≥-65 540 divergence in the *upstream dataset* — and the **strict tables are byte-identical in
every store**. The `block.size` tolerance is declared, bounded, reported per-window, and
**self-retiring** the moment the upstream dataset is fixed (§5.3, §5.6). Cell totals: **11 recorded
window records, 70,546 metered JSON-RPC requests**.

Repro (the operator supplies the tarball and the metered RPC key — see `harness/validate/README.md`):

```bash
SQD_PONDER_TARBALL=<tarball> SQD_RPC_KEY=<paid-rpc-key> \
  bash harness/validate/run-cell.sh L-eth
```

### 3.5 Matrix cell — L-base (FAIL-FAST by design, 2026-07-07 — upstream dataset gap #83)

The base **Layer-L** cell: the erc20 app on base mainnet (chain 8453, USDC
`0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913`), logs+receipts, over the same two seeded-random window
specs as `L-eth` — **4 × 2 000 blocks (seed 201)** and **4 × 5 000 blocks (seed 202)**. It is recorded
here **exactly as it ran** — a failure — because a gate that reports only its clean runs is not a gate,
and because this failure is the honest headline: the fork's dataset-completeness guard **did exactly
its job**.

**What ran — 8 / 8 windows failed fast.** Every window failed within seconds, each making exactly
**6 metered JSON-RPC requests** (**48 total**), with **no matched-log count** reached, **no
auto-shrink** attempted (the failure precedes any diff), and **zero shrink attempts**:

| Window (tag) | Range | Result | Requests | Duration |
|--------------|-------|--------|---------:|---------:|
| `rand#201.0@2000` | `[31019460, 31021460]` | **FAIL-FAST (#83)** | 6 | 30 s |
| `rand#201.1@2000` | `[30915789, 30917789]` | **FAIL-FAST (#83)** | 6 | 37 s |
| `rand#201.2@2000` | `[30022073, 30024073]` | **FAIL-FAST (#83)** | 6 | 56 s |
| `rand#201.3@2000` | `[37474954, 37476954]` | **FAIL-FAST (#83)** | 6 | 25 s |
| `rand#202.0@5000` | `[26059226, 26064226]` | **FAIL-FAST (#83)** | 6 | 29 s |
| `rand#202.1@5000` | `[39409152, 39414152]` | **FAIL-FAST (#83)** | 6 | 82 s |
| `rand#202.2@5000` | `[27500647, 27505647]` | **FAIL-FAST (#83)** | 6 | 40 s |
| `rand#202.3@5000` | `[31915218, 31920218]` | **FAIL-FAST (#83)** | 6 | 41 s |

The verbatim error (from the run, and reproduced post-run against window `rand#201.0`
`[31019460, 31021460]`):

```
Error: Portal dataset for mainnet is missing [transaction.logsBloom (logs_bloom)] on blocks
[31019460,31021460], which contain matched data your indexer needs — a Portal dataset-completeness
gap. Failing fast rather than serving incomplete data; report the gap to SQD, or start your indexer
past the affected range.
```

**Root cause — one missing column, range-scoped per chain.** On the SQD Portal datasets `base-mainnet`,
`arbitrum-one`, and `avalanche-mainnet`, the `logs_bloom` column on the `transactions` table is served
only **at and above a per-chain block boundary** and returns a 400 below it; `ethereum-mainnet`,
`polygon-mainnet`, and `binance-mainnet` serve it across their whole range. The gap is therefore
**range-scoped, not chain-wide**: a receipts backfill whose range sits entirely at/above the boundary
completes normally, one reaching below it fails fast. A direct Portal probe (no auth needed) confirms
that **below the boundary** the gap is **exactly this one column** — the other receipt fields (`status`,
`cumulativeGasUsed`, `effectiveGasPrice`, `gasUsed`, `contractAddress`) all return 200 with data on the
three affected datasets:

| dataset | `transaction.logsBloom` probe |
|---------|-------------------------------|
| `ethereum-mainnet` | **200** — `logsBloom` served |
| `polygon-mainnet` | **200** — `logsBloom` served |
| `binance-mainnet` | **200** — `logsBloom` served |
| `base-mainnet` | **400** — `couldn't parse request: column 'logs_bloom' is not found in 'transactions'` |
| `arbitrum-one` | **400** — same |
| `avalanche-mainnet` | **400** — same |

**The gap is range-scoped — the same probe at/above the boundary returns 200.** The 400s above are
sampled *below* each chain's boundary; at and above it the identical request serves `logs_bloom`.
Confirmed by direct probes (2026-07-08, no auth): `base-mainnet` **400 at block 45,000,000 → 200 at
45,398,144 and 46,000,000**; `arbitrum-one` **400 at 450 M → 200 at 460 M**; `avalanche-mainnet` **400 at
88 M → 200 at 89.4 M**. So each chain's boundary sits just below its probe-confirmed served block —
**base in (45.0 M, 45,398,144], arbitrum in (450 M, 460 M], avalanche in (88 M, 89.4 M]** (the probes
bound the interval; the exact first-served block within it was not bisected): a receipts-enabled
backfill whose `fromBlock` is at/above the confirmed served block (base 45,398,144, arbitrum 460 M,
avalanche 89.4 M) works today. The seeded `L-base` windows (§3.5, blocks 30–39 M) all fall **below** base's boundary — which is
why they fail fast, and why range-scoped is the accurate framing, not a chain-wide block.

**Why fail-fast is the designed correct behavior.** A Ponder app with `includeTransactionReceipts:
true` needs `transaction.logsBloom` for byte-complete receipt rows (`RECEIPT_FIELDS` in
`portal/portal-filters.ts`); `receipts.logsBloom` is **NOT NULL and load-bearing**, and
`portal/portal-transform.ts` deliberately **never substitutes a placeholder** for it. The
schema-degradation path drops a 400'd column and retries only for *droppable* fields; `logsBloom` is
recorded as **non-droppable**, so once a matched range is found the historical sync **fails the range**
rather than persisting a receipt row with a fabricated bloom that would silently diverge from RPC
ground truth. Serving incomplete data would defeat the entire byte-identity claim this document
rests on — so the crash is correct, and the error is **actionable** (report the gap, or start past the
range).

**Repro.** The underlying dataset gap reproduces with a single `curl`, no harness at all
(swap `<dataset>` for `base-mainnet` / `arbitrum-one` / `avalanche-mainnet` to see the 400, or
`ethereum-mainnet` for the 200):

```bash
curl -s -X POST "https://portal.sqd.dev/datasets/<dataset>/finalized-stream" \
  -H 'content-type: application/json' \
  -d '{"type":"evm","fromBlock":31019460,"toBlock":31019460,"fields":{"transaction":{"logsBloom":true}},"transactions":[{}]}'
```

The whole-cell failure reproduces through the harness (the operator supplies any base archive RPC):

```bash
DIFF_APP=harness/diff/erc20-app CHAIN_ID=8453 INCLUDE_RECEIPTS=true \
ERC20_ADDRESS=0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 \
PORTAL_URL_1=https://portal.sqd.dev/datasets/base-mainnet \
PONDER_RPC_URL_1=<any base archive RPC> \
  bash harness/diff/run.sh 31019460 31021460
```

**Disposition.** This is an **upstream dataset gap**, reported and tracked in
[#83](../../issues/83), which was **closed as completed on 2026-07-07**; a re-probe the same day shows
`base-mainnet` **still returns 400** on `transaction.logsBloom`, so the underlying gap **persists** —
the closure is administrative, not an upstream fix, and the matrix **continues logs-only** on the
affected chains. The matrix does **not** re-prove
the known gap on the other two affected chains; instead it **continues logs-only** on base, arbitrum,
and avalanche via the new `L-base-logs` / `L-arbitrum-logs` / `L-avalanche-logs` cells — identical
windows to their blocked receipts counterparts, `receipts: false`, so the Portal-vs-RPC byte-identity
of the logs / transactions rows is still proven on those chains. Receipts-enabled Portal backfills on
these three chains are **unusable only for ranges that reach below the per-chain boundary**; a backfill
whose `fromBlock` sits at or above the boundary (probe-confirmed served: base ≥ 45,398,144, arbitrum ≥
460 M, avalanche ≥ 89.4 M — each chain's true boundary sits just below its confirmed block) serves
`logs_bloom` and completes normally — so the actionable remedy the fail-fast error already emits,
*start the indexer past the affected range*, resolves it today. Extending the served
range downward is the preferred upstream remediation (the column already exists on eth / polygon / bsc,
so it is dataset backfill work, not schema design); a fork-side bloom synthesis from the transaction's
full log set is *possible future work* but is not planned. `L-base` above is kept as the **record of
what ran** — its seeded windows (blocks 30–39 M) sit below base's boundary.

### 3.5.1 Matrix cell — L-base-logs (differ-FAIL on the pre-fix build → **PASS 16/16 on the post-#110 re-run** — base-mainnet dataset lacks access_list, #83-family; surfaced a fork defect FIXED by #110/#111)

The logs-only variant of `L-base`: the erc20 app on base mainnet (chain 8453, USDC
`0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913`), **`receipts: false`**, over the **same 8 seeded-random
windows** as `L-base` — **4 × 2 000 blocks (seed 201)** and **4 × 5 000 blocks (seed 202)**. It ran
**2026-07-07** and is recorded here **exactly as it ran** — a differ-FAIL — because a gate that reports
only its clean runs is not a gate. Like `L-base` (§3.5) it sits on a base-mainnet dataset gap in the
**same family as [#83](../../issues/83)** (here the dropped `access_list` column), on a
**droppable/nullable** column, so the cell runs to completion and the load-bearing `logs` are
byte-identical. **This cell also surfaced a genuine fork defect**, however: the old fork *fabricated* an
empty `"[]"` for the dropped column instead of storing NULL — the #27 anti-pattern — which was traced to
being **systematic on arbitrum** (§5.6) and **FIXED** in [#110](../../pull/110)/[#111](../../pull/111).
See the disposition below and §5.6 for the full account.

**Update (2026-07-10) — re-run PASSES on the post-#110 build.** The tables below record the cell
**exactly as it first ran** (pre-fix, differ-FAIL) — the honest FAIL record stays. After #110/#111
merged, the identical windows were re-run on a fork build carrying the fix and **all 16 differ records
PASS** (0 fail): the Portal side now stores honest **NULL** for the dropped column (not a fabricated
`"[]"`), which the #113 `access_list`-column-gap tolerance accepts while every other column stays
byte-identical. This is the FAIL→PASS proof of the fork fix (full account + the arbitrum/avalanche
companions in §5.6). The upstream dataset column gap itself is unchanged (#83-family).

**What ran — 8 / 8 windows failed the byte-diff, but ran to completion.** Every window synced fully
(unlike `L-base`, which failed fast in 6 requests before any diff), then the byte-differ flagged
**`transactions.access_list`** *after* the sync. Each window **auto-shrank once** (the differ halves a
failed window to localise the divergence) and the shrunk half reproduced the **identical** divergence —
**16 differ records, all FAIL**. In every record `logs` / `transaction_receipts` (0) / `traces` (0) /
`blocks` were **byte-identical**; the **only** divergence is `transactions.access_list`. Per-window
(original, pre-shrink) request counts:

| Window (tag) | Range | Result | Requests |
|--------------|-------|--------|---------:|
| `rand#201.0@2000` | `[31019460, 31021460]` | **DIFFER-FAIL — `access_list` only** | 2033 |
| `rand#201.1@2000` | `[30915789, 30917789]` | **DIFFER-FAIL — `access_list` only** | 2034 |
| `rand#201.2@2000` | `[30022073, 30024073]` | **DIFFER-FAIL — `access_list` only** | 2032 |
| `rand#201.3@2000` | `[37474954, 37476954]` | **DIFFER-FAIL — `access_list` only** | 2035 |
| `rand#202.0@5000` | `[26059226, 26064226]` | **DIFFER-FAIL — `access_list` only** | 5024 |
| `rand#202.1@5000` | `[39409152, 39414152]` | **DIFFER-FAIL — `access_list` only** | 5063 |
| `rand#202.2@5000` | `[27500647, 27505647]` | **DIFFER-FAIL — `access_list` only** | 5043 |
| `rand#202.3@5000` | `[31915218, 31920218]` | **DIFFER-FAIL — `access_list` only** | 5047 |

The divergence is identical on every window: the Portal store has `transactions.access_list = "[]"`
(empty) where the RPC ground truth carries the populated access list, on base typed txs. Across all 16
records (the 8 originals plus their 8 auto-shrunk halves) the cell made **42 553 metered requests**.

**Root cause — one missing column, range-scoped (live probe 2026-07-07; re-confirmed 2026-07-08).**
Below base-mainnet's ~45 M boundary — a boundary in the **same ~45 M region as `logs_bloom`** (§3.5;
the `accessList` probes bound it only to (45.0 M, 46.0 M]) — the `base-mainnet` dataset does not serve
the `access_list` column; at/above it the column is served (`transaction.accessList` **400 at block
45,000,000 → 200 at 46,000,000**). The seeded windows here run
below the boundary. Requesting `transaction.accessList` below the boundary returns:

```
HTTP 400  Bad request: couldn't parse request: column 'access_list_size' is not found in 'transactions'
```

Without that field the same request returns **200** and serves txs fine; the `ethereum-mainnet` control
serves `accessList` (**200**) — which is why the `L-eth` cell (§3.4) passed on `transactions`. This is
the **same failure mode as [#83](../../issues/83)** (base-mainnet lacks `transactions.logs_bloom`), on a
different column:

| dataset | `transaction.accessList` probe |
|---------|--------------------------------|
| `ethereum-mainnet` (control) | **200** — `accessList` served |
| `base-mainnet` | **400** — `couldn't parse request: column 'access_list_size' is not found in 'transactions'` |

**Disposition — an upstream column gap that the old fork turned into a *fork defect*, now FIXED
([#110](../../pull/110)/[#111](../../pull/111)).** `transaction.accessList` is in the fork's
**DROPPABLE_FIELDS** (`portal/portal-filters.ts`): NULLABLE in Ponder's sync-store and non-load-bearing
(Ponder never reads it internally), so when a dataset lacks it the field *should* degrade to NULL. But
at the time this cell ran, the fork's transform did **not** degrade to NULL — it **fabricated an empty
`"[]"`** (`accessList: type >= 1 ? (tx.accessList ?? []) : undefined`, then `encode.ts:95` stored the
truthy `[]` as the string `"[]"`). That is a real **fork defect** (the #27 anti-pattern: a known-empty
value fabricated from an absent one), not the benign no-op it was first read as — the RPC/stock path
stores **NULL** here, not `"[]"`. The fix landed in **[#110](../../pull/110)** (store NULL when the
column is dropped) and **[#111](../../pull/111)** (gate on the exact EIP access-list type set, mirroring
the RPC path); full mechanism and evidence in §5.6. **What the fix does *not* change:** because the
upstream `base-mainnet` dataset still drops the column, the Portal store now holds **NULL** where an
RPC-backfill store holds the populated list — the byte-diff **still FAILs** on `transactions.access_list`
(the Portal side is now *honest* rather than *fabricated*, but not *identical* to RPC). The load-bearing
`logs` remain byte-identical throughout. This is the same underlying upstream gap as
[#83](../../issues/83) on a droppable column; the differ is **strict on `transactions`** (no `accessList`
tolerance), so it reports FAIL.

**Fail-fast (#83) vs differ-FAIL (this), the exact distinction.** In `L-base` (§3.5) the missing column
is `logs_bloom`, which is **load-bearing / NOT NULL / never fabricated**, so the fork's guard **fails
fast** (6 requests/window, before any diff; receipts **unusable**; **not** tolerable). Here the missing
column is `access_list`, which is **droppable / nullable**, so the sync **runs to completion** and the
differ flags the divergence **after the fact** — the distinction is *whether the backfill completes*,
not whether the field is correct. (On correctness: the old fork wrote a *fabricated* `"[]"` here — the
fork defect fixed by #110/#111 above — so this is not the benign no-op the completing sync might
suggest.)

**Relation to #27 and #32 — distinguish them.** **#27** is a different leg entirely: **RPC-mode
realtime** (Layer D, Leg-A), where the RPC *provider* omits `accessList` so the RPC leg persists NULL
while the Portal `/stream` leg **preserves** the real list (§5.1) — opposite leg, opposite direction, do
not merge it with this. **#32**, however, is the **same** historical-backfill fork-fabrication shape
this cell exhibits: the Portal side wrote `"[]"` for a dropped `access_list`. #32 saw it as a single row
only because it compared two Portal-backfill stores that fabricated `[]` identically and agreed; the
Portal-vs-RPC comparison (here, and the arbitrum probe in §5.6) exposes it as systematic. The
**fork-side fabrication behind both #32 and this cell is now fixed** to honest NULL by
[#110](../../pull/110)/[#111](../../pull/111) (§5.6); the upstream column gap remains.

**Repro.** The underlying dataset gap reproduces with a single `curl`, no harness (swap
`base-mainnet` for `ethereum-mainnet` to see the 200):

```bash
curl -s -X POST "https://portal.sqd.dev/datasets/base-mainnet/finalized-stream" \
  -H 'content-type: application/json' \
  -d '{"type":"evm","fromBlock":31019460,"toBlock":31019460,"fields":{"transaction":{"accessList":true}},"transactions":[{}]}'
```

The whole-cell run reproduces through the harness (the operator supplies any base archive RPC):

```bash
SQD_PONDER_TARBALL=<tarball> SQD_RPC_KEY=<paid-rpc-key> \
  bash harness/validate/run-cell.sh L-base-logs
```

### 3.6 Matrix cell — L-polygon (PASS, 2026-07-08 — first clean receipts byte-identity on a second independent chain)

The polygon **Layer-L** cell: the erc20 app on polygon mainnet (chain 137, native USDC
`0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359`), logs+receipts, over the same two seeded-random window
specs as `L-eth`/`L-base` — **4 × 2 000 blocks (seed 401)** and **4 × 5 000 blocks (seed 402)** drawn
from `[60 000 000, 89 000 000]` — plus the harness's **auto-shrink** re-runs. This is the **first
Layer-L cell to reach a clean receipts byte-identity on a chain other than eth**: unlike
`base`/`arbitrum`/`avalanche` (§3.5), the `polygon-mainnet` dataset serves **both** #83/#90 gap columns
(`logs_bloom`, `access_list`) across its whole range, so the full receipts path is comparable here — a
free public-portal pre-flight confirmed both columns return `200` with real data at both range extremes
before any paid spend.

**What ran — every comparable window byte-identical.** Of the 8 seeded windows, **4 produced a complete
A/B comparison and all 4 were byte-identical**; the other 4 could not be compared because the stock
**JSON-RPC ground-truth leg** (the oracle) shut down mid-backfill before the diff (below). Auto-shrink
re-ran the three largest passing windows on a half-size leading window. Reconstructed that way there are
**11 window records: 7 byte-identical, 4 RPC-oracle-incomplete, and zero byte-diffs.** Every window in
which a byte comparison was possible passed:

| Window (tag) | Range | Matched logs | Receipts (portal = rpc) | Verdict |
|--------------|-------|-------------:|------------------------:|---------|
| `rand#401.1@2000` | `[81995048, 81997048]` | 45,220 | 30,288 | **byte-identical** |
| `rand#401.3@2000` | `[85947279, 85949279]` | 50,288 | 32,038 | **byte-identical** |
| `rand#401.3+shrunk` | `[85947279, 85948279]` | 24,634 | 15,543 | **byte-identical** |
| `rand#402.1@5000` | `[83447717, 83452717]` | 83,519 | 52,359 | **byte-identical** |
| `rand#402.1+shrunk` | `[83447717, 83450217]` | 42,143 | 26,808 | **byte-identical** |
| `rand#402.3@5000` | `[88568193, 88573193]` | 100,908 | 65,344 | **byte-identical** |
| `rand#402.3+shrunk` | `[88568193, 88570693]` | 50,149 | 32,632 | **byte-identical** |

Each passing window is byte-identical across `logs` / `transactions` / `transaction_receipts` /
`traces` / `blocks` (event-bearing blocks included) with **no tolerance applied** — the `block.size`
#76 divergence seen on eth did not appear on any polygon window.

**The 4 no-verdict windows — the RPC *oracle* leg failed to complete, not the fork.** Four windows
recorded `pass:false` with the identical signature: the **Portal leg completed** (10–19 s), then the
JSON-RPC leg emitted `✗ rpc did not complete` and `Started shutdown sequence` **before** the byte diff
was ever reached, so no matched-log count and no A/B comparison exist for them:

| Window (tag) | Range | Portal leg | RPC leg |
|--------------|-------|-----------|---------|
| `rand#401.0@2000` | `[69979353, 69981353]` | completed (13 s) | did not complete |
| `rand#401.2@2000` | `[72816949, 72818949]` | completed (10 s) | did not complete |
| `rand#402.0@5000` | `[72041851, 72046851]` | completed (19 s) | did not complete |
| `rand#402.2@5000` | `[70420462, 70425462]` | completed (19 s) | did not complete |

These are a **reliability property of the stock RPC oracle** on polygon at these ranges, **not a
portal-ponder correctness fault**: the fork's own backfill completed in every one, and **no window in
the entire cell produced a byte-diff**. They are recorded here as **no-verdict** (baseline incomplete)
rather than as failures — a gate that reports only its clean runs is not a gate, and the honest reading
is "of every window where the oracle produced data to compare against, the Portal store was
byte-identical."

**Verdict — PASS: 7 / 7 comparable windows byte-identical, receipts included.** L-polygon is the first
demonstration of the full receipts path — `transaction_receipts` with `logs_bloom` and `access_list`
present — reproduced **byte-identical on a second independent chain**, closing the gap left by `L-base`
(where those columns are range-gapped, §3.5). Cell totals: **11 window records, ≈ 49,900 metered
requests** (cumulative campaign spend **779,265** requests, well under the 4 000 000 ceiling).

Repro (the operator supplies the tarball and the metered RPC key — see `harness/validate/README.md`):

```bash
SQD_PONDER_TARBALL=<tarball> SQD_RPC_KEY=<paid-rpc-key> \
  bash harness/validate/run-cell.sh L-polygon
```

---

### 3.7 Matrix cell — L-bsc (PASS, 2026-07-08 — receipts byte-identity on a third independent chain, under the generalized #107 size-only tolerance)

The bsc **Layer-L** cell: the erc20 app on BNB Smart Chain (chain 56, canonical USDT / BSC-USD
`0x55d398326f99059fF775485246999027B3197955`), logs+receipts, over the same two seeded-random window
specs as `L-eth`/`L-polygon` — **4 × 2 000 blocks (seed 501)** and **4 × 5 000 blocks (seed 502)** drawn
from `[40 000 000, 107 000 000]` — plus the harness's **auto-shrink** re-runs. It is the **third
independent chain to reach receipts byte-identity** (after eth §3.4, under the original `block.size`
tolerance, and polygon §3.6, clean), and the **first cell to exercise the *generalized* size-only
`block.size` tolerance ([#107](../../pull/107)) at scale** — where the eth-era #76 tolerance handled a
handful of derived rows, bsc diverges on the non-consensus `block.size` field across **up to 3 105
blocks in a single window**.

**What ran — every window byte-identical, receipts included.** Unlike polygon (§3.6), the stock
JSON-RPC oracle completed **every** window here, so all **16 window records produced a full A/B
comparison and all 16 passed** (8 seeded windows + their 8 auto-shrink half-window re-runs). Every
window is byte-identical across `logs` / `transactions` / `transaction_receipts` / `traces`; the
`blocks` table is either byte-identical or matches with a bounded count of **size-only** rows tolerated
— each such row has an **identical block hash and identical content in every field except the
non-consensus, node-derived `size`** (the divergence class documented for eth in
[#76](../../issues/76) and generalized in [#107](../../pull/107); the tolerance is hash-anchored, so a
real divergence cannot be masked). The 8 primary windows:

| Window (tag) | Range | Matched logs | Receipts (portal = rpc) | `block.size`-only tolerated | Verdict |
|--------------|-------|-------------:|------------------------:|----------------------------:|---------|
| `rand#501.0@2000` | `[97964639, 97966639]` | 113,050 | 52,186 | 47 | **byte-identical** |
| `rand#501.1@2000` | `[52108122, 52110122]` | 734,621 | 344,016 | 0 | **byte-identical** |
| `rand#501.2@2000` | `[91710545, 91712545]` | 127,103 | 64,729 | 420 | **byte-identical** |
| `rand#501.3@2000` | `[103240378, 103242378]` | 190,065 | 79,744 | 838 | **byte-identical** |
| `rand#502.0@5000` | `[64272501, 64277501]` | 488,658 | 240,218 | 0 | **byte-identical** |
| `rand#502.1@5000` | `[104753440, 104758440]` | 465,725 | 187,494 | 3,105 | **byte-identical** |
| `rand#502.2@5000` | `[66363735, 66368735]` | 314,276 | 164,532 | 0 | **byte-identical** |
| `rand#502.3@5000` | `[105192697, 105197697]` | 274,600 | 126,714 | 2,461 | **byte-identical** |

Each of the 8 **auto-shrink** re-runs (a half-size leading window over the same start block, triggered
when a window exceeds 50 000 matched rows) reproduced the same verdict byte-identical — across the full
cell, **6 windows carried zero tolerated rows, 10 carried a bounded size-only count, and there were
zero byte-diffs anywhere**.

**Why the size-only rows are pervasive here, and why that is not a fault.** On bsc the node-reported
`block.size` (the RPC-oracle leg) and the Portal-derived value diverge across long stretches of blocks
— a pervasive but purely `size`-field offset — so a single window can carry thousands of tolerated rows
where eth carried a handful. `block.size` is a **non-consensus, node-local re-derivation**: it is not
committed to in the block hash and is not an input to any downstream index, so a divergence in it alone
— with the block hash and every consensus field identical — is a property of the two data sources' size
accounting, not of the fork's correctness. The #107 predicate tolerates a row **only** when `size` is
the *sole* differing field and the hashes match; any additional divergence fails the window
(mutation-verified in the differ tests). The volume of tolerated rows is therefore evidence of a
pervasive-but-benign source difference, not of masking.

**Verdict — PASS: 16 / 16 windows byte-identical, receipts included.** L-bsc is the **third independent
chain** on which the full receipts path (`transaction_receipts`) reproduces byte-identical, and the
first at which the generalized #107 size-only tolerance is exercised at volume — with the safety-anchor
(hash + sole-diff) intact, so the pass is a genuine byte-identity result, not a widened tolerance hiding
drift. Cell totals: **16 window records, 84,819 metered requests** (cumulative campaign spend
**864,084** requests, ~22% of the 4 000 000 ceiling).

Repro (the operator supplies the tarball and the metered RPC key — see `harness/validate/README.md`):

```bash
SQD_PONDER_TARBALL=<tarball> SQD_RPC_KEY=<paid-rpc-key> \
  bash harness/validate/run-cell.sh L-bsc
```

---

## 4. Chaos / resume acceptance (Layer C)

The crash/resume evidence runs in two tiers against the same block range, each proving a distinct
property:

- **Tier 0 — PGlite, byte-identity (ACCEPTED 2026-07-04, §4.1).** A throwaway PGlite store is killed
  and resumed until complete, then **byte-diffed** against a clean baseline. Proves `SIGKILL`
  atomicity, restart idempotence, and byte-identical completion — but, at its parameterization, **not**
  attributable resume-from-partial-persisted-state.
- **Tier 1 — native Postgres, logical-digest identity + partial-resume (ACCEPTED 2026-07-05, §4.2).**
  A crash-durable Postgres store (fsync on) is killed and resumed under **small fixed chunks** so the
  durable store advances in a staircase; each kill's durable coverage is snapshotted, and the resumed
  store is checked for **logical-digest** identity against a baseline. This tier **closes the gap Tier
  0 scopes out**: it records kills landing at genuine partial durable coverage and completions that
  resumed from partial persisted state.

### 4.1 Tier 0 — PGlite byte-identity (ACCEPTED 2026-07-04)

The chaos kill-loop was accepted against the campaign's acceptance criteria. Aggregate result:

- **203 `SIGKILL`s** delivered across **80 attempted / 41 completed** backfill runs.
- **41 / 41 completed backfills survived kill-and-resume** — every completed backfill was `SIGKILL`ed
  at least twice mid-flight and still reached a correct, complete final store on resume.
- Final stores **byte-identical** to an unkilled baseline across **logs, transactions, receipts,
  traces, blocks**.
- **Every sync interval fragment tiled the range exactly** (including the factory-discovery
  fragments) — no gap, no overlap.
- Zero `InvariantViolation` under `PORTAL_CHECKS=strict`. (Both `kill-loop.sh` and `verify-resume.sh` export `PORTAL_CHECKS=strict`; an `InvariantViolation` is fatal to the run, so 41/41 clean completions entail zero violations.)

**What this proves — and what it does not.** The evidence above establishes three things precisely:
**(1) `SIGKILL`-atomicity** — a kill never leaves torn state; rows and their sync-interval fragments
commit together (all-or-nothing per commit), so the store never overstates coverage. **(2) Restart
idempotence** — a killed process, restarted, converges to the same correct store regardless of when
it was killed. **(3) Byte-identical completion** — the resumed store equals an uninterrupted baseline
across all five row families with intervals tiling exactly.

It does **not**, as parameterized, prove **attributable resume-from-partial-persisted-state**. Under
[#50](../../issues/50), the fork's historical path makes its **first durable commit only after
full-range discovery plus the entire first data chunk** (default `PORTAL_CHUNK_BLOCKS` 500k); for a
range inside one chunk the durable store goes **0% → 100% in a single transaction, seconds before
completion**. The chaos range `[20529207, 20579207]` (50k blocks) sits inside one chunk, so at almost
every kill instant the durable store was **either empty or already complete** — a restart re-paid the
discovery scan and re-streamed the chunk from zero rather than continuing from a persisted partial
watermark. With ~one atomic commit per backfill, the great majority of the 203 kills exercised
**restart-from-zero**; any kills that happened to land in the sub-second commit window and produced a
genuine mid-range resume are **statistically expected but were not recorded or attributed** by this
run (`kill-loop.sh` counts kills regardless of the durable coverage present at kill time — see
[#50](../../issues/50)). So "resume from a persisted partial `ponder_sync` state" is **plausible but
unproven here**; what is proven is atomicity, idempotence, and byte-identical completion.

> **This gap is now closed by Tier 1 (§4.2, ACCEPTED 2026-07-05).** By re-parameterizing to small
> fixed chunks the durable store advances in a staircase, and the Postgres-tier campaign recorded
> **155 kills at partial durable coverage** and **17 completed backfills that resumed from partial
> persisted state** to a logically-identical final store. The Tier-0 scoping above stands as the exact
> record of what Tier 0 *alone* proves; attributable resume-from-partial is proven below. Issue #50
> itself (the first-commit granularity / availability shape) was **subsequently fixed by
> [#91](../../pull/91)** (merged `ebb573ca`): a fetch-plane quantum slow-start (`fetchQuantum` starts
> at `PORTAL_WARMUP_BLOCKS`, default 25 000, and doubles to `chunkBlocks`) plus a resume-seeded
> discovery watermark bound the first durable commit to seconds even on the **default-chunk**
> historical path, so the zero-progress-livelock under sub-window crash loops no longer occurs. Tier 1
> proved *attributable resume-from-partial* independently — with small chunks that force a staircase —
> and remains the standing proof of that property; #91 additionally fixed the availability shape.

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

### 4.2 Tier 1 — native Postgres, attributable resume-from-partial (ACCEPTED 2026-07-05)

Tier 0 proves atomicity, idempotence, and byte-identical completion, but — because its 50k range fits
inside one 500k chunk — it cannot witness a resume from a *persisted partial* store ([#50](../../issues/50)).
Tier 1 was built to close exactly that gap on a **crash-durable native Postgres 16 backend** (`fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`; recorded as `postgres16-fsync-on` in the campaign
metadata), re-parameterized so the durable store advances in a **staircase** that kills can reliably
land inside.

**Why Postgres, and why a logical digest.** The earlier attempt to prove partial-resume ran on PGlite,
which runs single-user Postgres with `fsync` **off** and is **not crash-durable** under repeated
`SIGKILL` — its WAL tears after ~6–7 kill/resume cycles ([#52](../../issues/52)), so a PGlite-backed
campaign stops on store durability before it can accumulate acceptance counts (a finding about the
PGlite *backend*, not the fork). Tier 1 therefore uses a real Postgres cluster and kills only the
**ponder app process** (never the database). On a crash-durable backend, a `SIGKILL` mid-write is
recovered by **WAL replay** on the next start, which legitimately changes the **physical bytes** on
disk (WAL segment offsets, checkpoint records, free-space map, hint bits, page LSNs) while the
**logical row content** is identical. A byte-compare of two datadirs would therefore be *wrong* here —
it would flag a correctly-recovered store as different. Store identity is a deterministic **logical
digest** over `ponder_sync` row content (`harness/chaos/pg-digest.mjs`), plus the same intervals-tile-
exactly SQL check as Tier 0.

**Parameterization (staircase-forcing).** `PORTAL_CHUNK_BLOCKS=2000`, `PORTAL_CHUNK_FIXED=1`,
`PORTAL_READAHEAD=1` — small, fixed chunks with no read-ahead, so each chunk commits durably before
the next is fetched and the durable store climbs in ~2k-block steps rather than 0→100% in one
transaction. Chain 1 (ethereum) range `[20529207, 20579207]`, build `0.16.6-sqd.2`, `PORTAL_CHECKS=strict`,
**public** Portal + **public** RPC endpoints, Poisson kill schedule (per-run mean recalibrated toward
landing kills mid-staircase). A per-kill **coverage snapshot** records the durable coverage at each
kill; a kill with `0 < coverage < 100%` counts as a **kill at partial coverage**, and a completion
whose resume started from partial persisted coverage counts as a **completion-from-partial**.

**The logical-digest identity, precisely.** Per table, `md5` over the ordered concatenation of
`md5(logical_jsonb(row))`, where `logical_jsonb(row) = to_jsonb(row)` **minus any surrogate serial
`id`**, taken under a **total, natural-key `ORDER BY`** (natural-key columns first, then the full
logical-jsonb text as a tie-break so rows tie only when their entire content is identical). The store
digest is `md5` over the sorted `table=perTableDigest:rowcount` lines, so table set and per-table row
**count** both bind into the identity. Cache/wall-clock tables (`rpc_request_results`,
`kysely_migration*`) are excluded at the table level; a listed table that is **absent** is a hard
error (fail-closed schema shape). The baseline digest's determinism is proven by digesting the
completed store twice and once more **after a `pg_ctl` restart** — all three identical
(`build-baseline-pg.sh`).

**Aggregate result (status = pass):**

- **203 `SIGKILL`s** delivered across **242 attempted / 27 completed-and-verified** backfill runs
  (36 runs total: **27 pass / 8 neutral / 1 fail** — see the candor notes below on the neutral and
  fail classes).
- **155 kills landed at partial durable coverage** (`0 < coverage < 100%`) — the evidence Tier 0
  structurally could not produce.
- **17 completed backfills resumed from partial persisted state** and reached a **logically-identical**
  final store (digest byte-equal to the unkilled baseline, intervals tiling `[20529207, 20579207]`
  exactly).
- Zero `InvariantViolation` under `PORTAL_CHECKS=strict`; zero store-durability failures (no post-kill
  store was ever unreadable — the crash-durability the tier set out to test).

Acceptance thresholds (all cleared): kills ≥ 200 (**203**), completed-verified ≥ 25 (**27**), kills at
partial coverage ≥ 25 (**155**), completions-from-partial ≥ 1 (**17**).

**Candor — the two runs the campaign kept as evidence.** The numeric totals above are computed over
the valid runs; two runs carry an explicit story and are worth reading in full, because the story is
the product.

- **Run 2 — kept as a `fail`; its kills excluded from the totals.** Verify found `factory_addresses`
  at exactly **2× rows** (16 vs 8, the content identical but duplicated). Root cause was a **driver
  defect**, not a fork defect: the kill/reap path leaked a *rogue second concurrent ponder writer*, so
  two live writers briefly shared the store. It is retained in the run ledger as a **`fail`** (candor —
  it is evidence about the *harness*), but its 24 kills / 23 partial-coverage kills are **excluded from
  the acceptance totals** (a two-writer store proves nothing about single-writer resume). The driver
  was then hardened to a single-writer **DB-boundary gate** (a `pg_stat_activity` check that refuses to
  (re)launch while any other backend is connected to the run DB). The *store-level* lesson was real and
  separate: `sync-store` `factory_addresses` had **no idempotence/uniqueness story**, so a second
  writer could durably duplicate the child set → filed as [#53](../../issues/53); the write-side fix
  **merged in [#54](../../issues/54)** (2026-07-05).
- **Run 20 — a digest false-FAIL, reclassified to `pass`.** A `SIGKILL` rolled back a
  `factory_addresses` flush transaction (rows gone), but the Postgres **serial `SEQUENCE`** — which is
  **non-transactional** — had already advanced; the resume re-flushed **identical content at shifted
  serial ids** (ids 9–16 vs the baseline's 1–8). The *original* digest hashed `to_jsonb(row)` verbatim,
  so it bound the surrogate id and reported a mismatch on a store that had in fact resumed **perfectly**.
  The fix makes the digest exclude surrogate serial ids and order by the natural key (see the identity
  definition above); the selftest was extended to assert **id-shift invariance** while a block-number
  mutation and a run-2-style duplication **still diverge** (a negative control — run 2 still fails
  post-fix). The baseline digest was recomputed (only the two id-bearing tables' per-table digests
  changed), and run 20 re-verified **PASS** and was reclassified. This is a **methodology finding**, not
  a fork finding: *logical* identity must exclude non-transactional surrogate serials — a companion to
  the WAL-replay/physical-bytes caveat that motivates the logical digest in the first place.

The 8 **neutral** runs are calibration misses — completions with fewer than the required minimum kills
(a run that finishes without being sufficiently killed proves nothing about resume and is neither a
pass nor a fail); they contribute no kills to the totals.

Repro (per the harness; the campaign loop invokes many runs to reach the aggregate counts — see §7):

```bash
# build the crash-durable baseline once (small-fixed-chunk params; proves digest determinism)
SQD_PONDER_TARBALL=<tarball> CHAOS_APP=<pg-app> \
CHAOS_META_MJS=harness/chaos/chaos-meta.mjs \
  bash harness/chaos/build-baseline-pg.sh

# run the Postgres-tier kill/resume campaign (Poisson kills, per-kill coverage snapshots)
SQD_PONDER_TARBALL=<tarball> CHAOS_APP=<pg-app> \
CHAOS_PORTAL=<public-portal-dataset> CHAOS_RPC=<public-rpc> \
  bash harness/chaos/chaos-pg-driver.sh
```

### 4.3 Tier 1 — re-acceptance on the current `main` build (ACCEPTED 2026-07-06)

The §4.2 acceptance ran on 2026-07-05, before the wave of Portal fixes that followed it. To confirm
those fixes did not regress crash/resume — and to test a stronger property — a **second full Tier-1
campaign** was run **2026-07-06** against the build of `main` **@ `248f41e`** (the build *after* that
fix wave), using the **same methodology, parameters, range, backend, and harness as §4.2**:
`postgres16-fsync-on` (`fsync` / `synchronous_commit` / `full_page_writes` on); `PORTAL_CHUNK_BLOCKS=2000`,
`PORTAL_CHUNK_FIXED=1`, `PORTAL_READAHEAD=1`; chain 1 (ethereum) range `[20529207, 20579207]`; Poisson
kill schedule with the adaptive per-run mean clamped to **4–7 s**; store identity by the same logical
digest (`harness/chaos/pg-digest.mjs`) plus the intervals-tile-exactly check. The repro is identical to
§4.2's (`build-baseline-pg.sh` + `chaos-pg-driver.sh`), against the same private tarball and public
Portal/RPC endpoints.

**Aggregate result (status = pass):**

- **236 `SIGKILL`s** delivered across **273 attempted / 31 completed-and-verified** backfill runs
  (37 runs total: **31 pass / 6 neutral / 0 fail** — no failures, no freezes, no unreadable post-kill
  stores).
- **166 kills landed at partial durable coverage** (`0 < coverage < 100%`).
- **17 completed backfills resumed from partial persisted state** and reached a **logically-identical**
  final store (digest byte-equal to the baseline, intervals tiling `[20529207, 20579207]` exactly).
- Zero `InvariantViolation` under `PORTAL_CHECKS=strict`; zero store-durability failures.

Acceptance thresholds (all cleared): kills ≥ 200 (**236**), completed-verified ≥ 25 (**31**), kills at
partial coverage ≥ 25 (**166**), completions-from-partial ≥ 1 (**17**).

**Cross-build store equivalence — the strongest result here.** Every completed run's logical digest
equalled the frozen baseline digest `360af5126a0efffc49b871594b8ac3ea`. That baseline was built by the
**earlier** `main` build (`458dc8c`), while this campaign ran on `248f41e`; because every completed run
on `248f41e` reproduced it exactly, the re-acceptance additionally proves that the `248f41e` build
reproduces the `458dc8c` build's `ponder_sync` **row content byte-for-byte** over the campaign range —
two different `main` builds producing byte-identical sync-store content, not merely one build being
self-consistent under kills.

**Candor.**

- **6 of 37 runs are `neutral` — calibration misses**, exactly the §4.2 class: each **completed with
  fewer than the required minimum kills** (here **zero** kills), so it is neither a pass nor a fail and
  **contributes no kills to the totals**. Every run that cleared the kill floor ran to a verified
  completion and **PASSed**; there were **zero** fails and **zero** freezes across the campaign.
- **The campaign's aggregate metadata carries the label `chaos-3-pg`** — a hard-coded default in the
  driver (`v3-pg`), which was **deliberately reused unchanged** from the §4.2 campaign. The label is
  cosmetic, is disclosed here, and affects no count.
- **The longest runs were Poisson tails, not stalls.** The two longest runs (**373 s** at 39 kills;
  **350 s** at 33 kills) simply accrued the most kills and resume cycles under the Poisson schedule —
  wall time scales with kill count — and both completed and verified PASS.

---

### 4.4 Tier 1 — dense/factory-source re-acceptance on the current `main` build (ACCEPTED 2026-07-08)

To stress the dense-source / factory-child discovery path (many concurrent `dataChunk` fan-outs feeding a large first interval) under the Poisson kill-loop, a **Phase-2 chaos campaign** was run **2026-07-08** on the current `main` build. It re-runs the Layer-C acceptance logic but explicitly exercises the discovery-heavy code paths fixed in #50/#92/#96, verifying byte/logical store identity to a clean baseline after resume. Harness commit is `82c71e6`; tarball is `subsquid-ponder-0.16.6-sqd.2.tgz` (sha256 `7bf524d3dee791b3b8a1576a932129d02b0dd2bdf91d28714227881f9fc1f037`). Backend is `postgres16` (`fsync=on`), chain 1 (ethereum) range `[20529207, 20579207]` (span 50000). Poisson kill trigger targeted ~2 kills/run. The baseline logical digest is `360af5126a0efffc49b871594b8ac3ea`. Wall clock: `2026-07-08T04:56:58Z` → `2026-07-08T05:47:10Z` (~50 min). Artifacts are in `chaos-3/phase2-artifacts/` (`finalVerdict: PASS`).

**Acceptance thresholds vs. observed (all four cleared):**

| metric | threshold | observed |
|---|---|---|
| kills | ≥ 200 | 202 |
| completed-and-verified runs | ≥ 25 | 33 |
| kills at partial coverage | ≥ 25 | 153 |
| completions resuming from partial coverage | ≥ 1 | 21 |

**Run tally:** 45 runs executed against a 150-run cap — **33 pass / 12 neutral / 0 fail**. The 12 "neutral" runs are benign 0-kill runs: the 50k range completed before any Poisson kill fired (small range × Poisson variance). They are NOT stalls — neighboring runs killed 3/14/5/2/7 fine.

**Independent re-verification by the orchestrator (load-bearing):** `driverInvariants` = **0** across all 45 runs; zero `verdict=fail`; every `*.invariant-events.jsonl` file is empty. Of the 45 runs, 33 generated `run-*.verify.log` files, and **all 33** contain the baseline digest `360af5…` alongside a "digest identical" / "logically identical" line. There is **zero** mismatch / "NOT identical" / `InvariantViolation` across any verify log. The `completedVerified=33` count is thus exactly equal to 33 real digest-identical logs — the count is not fabricated.

**What this proves:**

1. **Dense/factory-source kill→resume STORE IDENTITY** holds: 33/33 completed runs are logically byte-identical to the clean baseline after crash+resume. (PROVEN — the headline result.)
2. **Warmup-bounded time-to-first-durable-COMMIT** (the #50 guarantee) is owned by the DATA plane (the quantum bound on data-fetch `to`) and is independent of discovery shape. That guarantee continues to hold on this path. (PROVEN — cited as the #50 headline; it is a DATA-PLANE / first-commit property, not a discovery-scan property.)
3. **Geometric (doubling) discovery slow-start** is observed for watermark-following discovery: the late trace sequence shows the quantum doubling `2000→4000→6000→12000`. (PROVEN for the watermark-following case.)

**Candor.**

- **This section does NOT assert, and deliberately does not claim, that a fresh dense process's first discovery scan is warmup-bounded.** On attempt 1 of a fresh process, the first `portal-dense-discovery-scan` covers the ENTIRE range in ~8 windows (a grid-aligned `[20528000, 20579207]`, ~51.2k blocks, starting just below the configured floor `20529207` — do not reconcile it against the 50000-block data range) because the concurrent `dataChunk` fan-out supplies a full-range `needTo` when Ponder feeds a large first interval. This is expected (a consequence of `planDiscovery`'s `to = max(needTo, min(endHint, through+quantum))` tracking a large interval, not the watermark) — it is NOT a #50 regression and NOT a fork bug, because a wide discovery scan fetches no data, does not delay first commit, and only front-loads factory-child discovery I/O.

---

## 5. Findings log

Every divergence and anomaly surfaced by the layers above is recorded here with its public issue and
current state. **In the A/B soak cross-validation (Layer D) so far, the divergences found have been
RPC-leg (leg-A) defects; the Portal leg matched third-party evidence in each confirmed case.** On the
Portal side, the Layer-E paid matrix surfaced **one genuine portal-ponder fork defect** — the
historical-backfill path **fabricated an empty `access_list` (`"[]"`)** for typed txs whose upstream
dataset drops the `access_list` column, asserting a known-empty list where chain-truth carries a
populated one (the #27 anti-pattern) — now **FIXED** in [#110](../../pull/110)/[#111](../../pull/111)
to store honest **NULL** (§5.6). The **remaining** Portal-side items are **not fork defects but
*upstream SQD dataset* issues**, surfaced by the same matrix and publicly reproducible: a `block.size`
off-by-one at the RLP 2^16 boundary — tolerated under a precisely-scoped, self-retiring class pending
the upstream fix ([#76](../../issues/76), §5.3 and §5.6) — and a **missing `transactions.logs_bloom`
column** on the base / arbitrum / avalanche datasets, which makes receipts-enabled Portal backfills
**fail fast by design** on those chains ([#83](../../issues/83), §3.5 and §5.6). This is a factual
record of what has been observed, not a claim that the Portal path is defect-free.

### 5.1 Cross-validation findings (A/B soak differ, Layer D)

| Issue | State | Finding | Attribution / third-party |
|-------|-------|---------|---------------------------|
| [#27](../../issues/27) | OPEN | RPC-mode realtime stores `access_list` as **NULL** for realtime-ingested typed txs — **including txs that had a real access list on chain**. Upstream Ponder tolerates a provider that omits the `accessList` key and persists NULL permanently. The Portal `/stream` leg **preserves** the access list. | Leg-A defect. Third-party confirmed via an independent public node; the Portal leg matches the real on-chain access list. Loud-fail hardening for the provider-omits-key case landed via **[#31](../../issues/31)** (merged). |
| [#32](../../issues/32) | CLOSED | A **single** transaction with a **fabricated-empty** `access_list` (`[]`) in one of two independently-synced stores (chain 42161). | A single anomalous row surfaced by the differ; pinned as a known-bad row (see §5.3) so it does not mask new drift. **Update:** the Portal *historical-backfill* fork side no longer fabricates `"[]"` for a dropped `access_list` column — [#110](../../pull/110)/[#111](../../pull/111) store honest **NULL** instead (§5.6). The Layer-E Portal-vs-RPC comparison also shows this shape is **systematic on arbitrum**, not a single row (§5.6), so #32's single-hash pin under-covers. **#32 is now CLOSED** — the fork defect is fixed and proven **FAIL→PASS on all three affected chains** (arbitrum/base/avalanche, §5.6); the residual Portal-`NULL`-vs-RPC-populated divergence is the upstream #83-family column gap (honest NULL now, not fabrication). |
| [#36](../../issues/36) | CLOSED | RPC-mode realtime store **missing on-chain logs** (and block rows) of long-established addresses at **scattered recent blocks** — the loss is `(block, address)`-scoped. The Portal store is **complete**. | Leg-A defect. Third-party confirmed; the Portal leg holds the missing rows. **CLOSED** — root cause identified as fork-side/client-side (leg A ran `0.16.6-sqd.1`, predating two interacting fixes: on restart the in-memory child registry re-seeded only from the tail chunk's floor, so children registered **below** it went unmatched and their rows were dropped while the interval still read complete), fixed on `main`. |
| [#23](../../issues/23) | OPEN | RPC-mode realtime **deterministic crash** when a single block's full-block `eth_getLogs` response exceeds viem's 10 MiB body cap (dense blocks). The Portal `/stream` leg streams through such blocks. | Leg-A (RPC transport) failure mode; documented as an objective datapoint where the Portal path is more robust. |
| [#33](../../issues/33) | CLOSED | Stream-realtime: a canonical block fataled with "unknown parent" where the parent **was** the canonical block at N−1 (no reorg involved). | Stream-realtime robustness finding on the **Portal** leg — **not a reorg** (third-party-confirmed the fatals were on canonical blocks). **CLOSED 2026-07-04** by the 409 delivered-hash-ring / step-down fork negotiation (§5.8 (B)); the stream path remains under an experimental label. |

Two closed items for completeness:

- [#28](../../issues/28) — **CLOSED.** Stream realtime leaked an abort listener per `sleep()` call on
  the long-lived signal (a `MaxListenersExceededWarning` storm). Resolved.
- [#31](../../issues/31) — **MERGED.** Fail-loud when a provider omits `accessList` on typed
  transactions (the fix path for the #27 shape).

### 5.2 Stream-realtime correctness wave

PR **[#26](../../pull/26)** (merged 2026-07-04) landed a wave of stream-realtime correctness fixes:
same-block child logs, a finality anchor, reorg pruning of factory children, a finalized-head pin, and
population of parent transactions on the stream wire. The realtime path continues to be treated as
**experimental** pending the longer soak; [#33](../../issues/33) (the false-`unknown-parent` fatal it
hardened against — **not a reorg**) is now **closed** (§5.8).

### 5.3 Benign / tolerated diff classes (declared, bounded, removable)

Both differ paths — the **A/B soak differ** (Layer D) and the **paid-matrix byte-diff** (Layer E) —
tolerate a small, **explicitly enumerated** set of already-understood divergences so that a *new*
divergence is never masked. Each class is narrowly scoped, reported in the diff's status output, and
designed to be removed once its underlying issue is fixed. The A/B classes (the first four rows) are
documented in `harness/soak-ab/ab-diff.mjs`; the two paid-matrix classes (the last two rows) — the
`block.size` derivation artifact (#76, merged in [#77](../../pull/77)) and the `access_list` column gap
(#83/#32) — live in both byte-diff paths (`harness/diff/diff.mjs` and `harness/validate/diff-batched.mjs`).

| Class | Scope | Why tolerated | Removal condition |
|-------|-------|---------------|-------------------|
| **realtime parent-tx gap** | leg B missing *parent* transactions for realtime-ingested spans, each referenced by a leg-A log; any tx present on **both** sides must be byte-identical | The stream wire did not carry parent txs for these spans (the verified pre-#26 wire gap). This is an availability gap on leg B, never wrong data — the shared txs are byte-identical. | Closes as the parent-tx population from PR #26 covers the span; a non-referenced onlyA tx, or any shared-tx byte diff, is a hard FAIL. |
| **access_list-null (#27)** | a single already-diverged **shared** tx whose *only* divergence is `access_list` (every other column byte-identical: ex-`access_list` md5s equal) | The RPC leg persisted NULL where the Portal leg has the real list (#27). Tolerated **only** while the divergence stays access_list-only. | If any *second* column ever diverges on that row, it stops being tolerated → hard FAIL. Removed when #27 is fixed. |
| **pinned known-bad row (#32)** | exactly one transaction hash on chain 42161 with the fabricated-empty `[]` shape | Isolates the single anomalous #32 row so it does not mask new drift. Note: the *fork-side* fabrication that produced this shape is now fixed on the historical-backfill path — [#110](../../pull/110)/[#111](../../pull/111) store NULL, not `"[]"`, when the upstream dataset drops the column (§5.6); the pin is left in place conservatively pending #32's own closure. | The pin protects **only** the measured `[]`-vs-concrete-list shape — it does **not** tolerate an A-NULL / B-non-null drift or a B-side rot on that hash. Removed with #32. |
| **leg-A onlyB row-loss (#36)** | `onlyB` log/block rows (present in leg B, missing in leg A) at/above a per-chain realtime-era floor; **chain 1 only** (the only chain where the loss was observed) | Leg A silently lost on-chain rows leg B holds (#36); below the floor leg A's store came from the complete-by-construction historical backfill path. | A chain with `onlyB` rows but **no** configured floor is a hard FAIL (unknown chains are never default-tolerated). Removed when leg A is repaired or the leg is retired. |
| **upstream-dataset block.size (#76)** *(paid matrix, Layer E)* | a shared `blocks` row whose **only** divergence is `block.size` with `rpc = portal + 1`, on blocks of canonical size **≥ 65 540** (the RLP 2^16 boundary) | The upstream SQD Portal dataset serves `block.size` one byte low at the boundary (§5.6); the fork persists it faithfully, and every other block column plus all `logs` / `transactions` / `transaction_receipts` / `traces` rows are byte-identical. | Any *second* differing column, the opposite delta direction, or a sub-threshold size is a hard FAIL. **Self-retiring**: it matches nothing once the upstream size is correct. Removed when [#76](../../issues/76) is fixed. |
| **access_list column gap (#83/#32)** *(paid matrix, Layer E)* | a shared `transactions` row whose **only** divergence is `access_list`, where the **Portal side is SQL NULL**, on **base-mainnet / arbitrum-one / avalanche-mainnet** (chain_id 8453 / 42161 / 43114) | The upstream SQD Portal dataset **drops** the `transactions.access_list` column on these three chains (#83-family gap, §5.6); our fork stores an **honest SQL NULL** there ([#110](../../pull/110)/[#111](../../pull/111) — never a fabricated `"[]"`), while the stock-RPC leg persists the real list. Every other `transactions` column plus all `logs` / `blocks` rows are byte-identical. | Bounded to **Portal-IS-NULL only** — a **non-NULL** Portal value that differs from RPC (in particular a reappearing `"[]"`, the exact **#110 regression sentinel**) is a hard FAIL, as is any *second* differing column or the same shape on an **out-of-scope chain** (which serves the column). **Self-retiring**: drops away if the Portal ever serves `access_list`. Removed when [#83](../../issues/83) is fixed. |

**Candor about the limit of cross-validation.** Within a tolerated span the A/B differ *by itself*
cannot distinguish leg-A row loss (leg A dropped a real on-chain row) from a hypothetical leg-B
row fabrication — both surface identically as an `onlyB` row. What breaks the tie is the **third-party
spot audit** (Layer F): in the confirmed cases (#36, #27) leg B's rows matched an independent node
byte-for-byte, establishing leg A as the lossy side. The status JSON carries a bounded sample of
tolerated block numbers per table specifically to keep that audit reproducible.

### 5.4 Chaos-discovered findings (Layer C)

| Issue | State | Finding | Attribution / layer |
|-------|-------|---------|---------------------|
| [#50](../../issues/50) | **FIXED ([#91](../../pull/91), merged `ebb573ca`)** | **First-durable-commit granularity.** The fork's historical path made its first durable sync-store commit only after **full-range factory discovery + the entire first data chunk** stream (default `PORTAL_CHUNK_BLOCKS` 500k); for a range inside one chunk the durable store went **0% → 100% in one transaction, seconds before completion**. A restart loop shorter than that window made **zero forward progress** and re-paid discovery + chunk re-stream each cycle (upstream, which commits proportionally to a 25-block first interval, creeps forward instead). An availability/progress regression vs upstream, with a zero-progress-livelock shape under sub-window crash loops. **Correctness was unaffected**: coverage never overstated and rows+intervals still committed atomically (that invariant held under all 203 chaos kills). | Discovered by the chaos campaign (Layer C): a 60-kill Poisson run (mean 5 s) ended with a **provably byte-empty store** — every restart began from zero — root-caused to the first-commit granularity and filed as [#50](../../issues/50). **Fixed by [#91](../../pull/91)** (fetch-plane only; commit plane / sync-store / schema untouched): a `fetchQuantum` slow-start (starts at `PORTAL_WARMUP_BLOCKS`, default 25 000, doubling to `chunkBlocks`; `=0` restores legacy shape) + resume low-edge trim + resume-seeded discovery watermark bound the first durable commit to seconds on the default-chunk path, so the livelock no longer occurs. Discovery-geometry chaos coverage added in [#92](../../issues/92)/[#96](../../pull/96) (§4.4). |
| [#52](../../issues/52) | **CLOSED (superseded by [#56](../../pull/56) native-PG Tier 1)** | **PGlite backend is not crash-durable under repeated `SIGKILL`.** The Tier-0 store (PGlite) runs single-user Postgres with `fsync` **off**; its WAL **tears after ~6–7 kill/resume cycles** (`InitWalRecovery → StartupXLOG` abort), so a PGlite-backed campaign cannot accumulate the counts needed for attributable resume-from-partial. | A finding about the **harness backend**, not the fork: it motivated the crash-durable native-Postgres Tier 1 (§4.2). Filed as [#52](../../issues/52). **Closed** because native-PG Tier 1 (§4.2, [#56](../../pull/56)) superseded PGlite for the acceptance evidence — **not** because PGlite became durable: the WAL-tear finding remains accurate and stays correctly scoped as a **Tier-0 backend limitation** (§5.4). |
| [#53](../../issues/53) | OPEN (write-side fix **merged [#54](../../issues/54)**) | **`sync-store` `factory_addresses` has no idempotence/uniqueness story.** A second concurrent writer durably **duplicates the child set** (`factory_addresses` ×2, identical content). | Surfaced by Tier 1 **run 2**, whose *driver* defect leaked a rogue second concurrent writer (since fixed with a single-writer DB-boundary gate). App-invisible but it breaks store-identity tooling; the **write-side hardening merged in [#54](../../issues/54)** (2026-07-05). Run 2 is retained candidly as a `fail` (§4.2); its kills are excluded from the acceptance totals. |
| (methodology; no issue) | RESOLVED in-harness | **Logical store identity must exclude non-transactional surrogate serials.** A `SIGKILL` can roll back a `factory_addresses` flush while the Postgres serial **`SEQUENCE`** (non-transactional) has already advanced, so a correct resume re-flushes **identical content at shifted serial ids**. A digest over `to_jsonb(row)` verbatim binds the surrogate id and **false-FAILs** a perfectly-resumed store. | Surfaced by Tier 1 **run 20** (§4.2). Fixed by digesting `to_jsonb(row)` **minus surrogate serial ids**, ordered by the natural key (`pg-digest.mjs`); selftest extended for **id-shift invariance** with block-number mutation and run-2-style duplication as negative controls; baseline recomputed; run 20 re-verified **PASS**. A companion to the WAL-replay/physical-bytes caveat (§4.2) that motivates a *logical* rather than *physical* identity. |

This finding (#50) is what **Tier 0** (§4.1) could not see past: because the 50k Tier-0 range fits
inside one 500k chunk, its kills overwhelmingly hit an empty-or-complete durable store, so
*attributable* resume-from-partial state was not witnessed there. The **re-parameterized campaign** it
called for — small fixed chunks (`PORTAL_CHUNK_BLOCKS` 2k) that force staircase durable commits,
per-kill coverage snapshots, and an acceptance criterion requiring kills observed with
`0 < coverage < 100%` — **ran and closed that gap on the crash-durable Postgres backend**: see
**Tier 1 (§4.2, ACCEPTED 2026-07-05)** — 155 kills at partial coverage, 17 completions from partial
persisted state, logically-identical final stores. Issue #50 itself was **subsequently fixed by
[#91](../../pull/91)** (quantum warmup + resume-seeded discovery, merged `ebb573ca`), which bounds the
first durable commit on the default-chunk path; Tier 1 proved attributable resume-from-partial
independently, with small chunks that force a staircase, and remains the standing proof of that
property.

### 5.5 Validation-tool findings (Layer E harness)

These are findings about the **validation harness itself** — the byte-diff tooling and the matrix
design — surfaced while running the paid matrix (Layer E). They are separate from the data findings
above: neither the fork nor the ground-truth store is at fault — the harness was — and none of them
changed a verdict. They are recorded here for the same reason as the chaos methodology findings (§5.4):
the harness is part of the evidence, and its bugs belong in the open too.

| Issue | State | Finding | Attribution / layer |
|-------|-------|---------|---------------------|
| [#58](../../issues/58) | RESOLVED (**merged [#59](../../pull/59)**) | **Differ keyset pagination did not follow the sync-store PK.** The batched byte-diff's keyset cursor ordered/compared by columns that were **not** the `chain_id`-prefixed sync-store primary key, so on a large table the planner could not resolve each page as a single forward index scan; on the F-full full-history diff (§3.2) the tool diffed `logs` byte-identical, then **wedged on `transactions`** (days of CPU, no progress). | A tool defect (Layer E), not a data defect — both stores were intact (the offline re-diff with the fixed tool proved them byte-identical). Root-caused and fixed in **[#59](../../pull/59)**: ORDER BY + tuple-WHERE now lead with the `chain_id`-prefixed PK. Pinned by a DB-free SQL-shape test (`diff-batched.test.mjs`). |
| [#63](../../issues/63) | RESOLVED (**merged [#72](../../pull/72)**) | **PGlite 0.2.13 WASM-allocator detoast-volume hang.** A single `select *` page whose toasted input runs to ~300 MB (which a 50,000-row page of the widest sync-store tables reaches over full-history windows) spins forever in PGlite 0.2.13's WASM allocator (the *detoast* step, not the query). The **same rows in 5,000-row pages** complete at ~1.5 s each. Surfaced by the fixed-keyset differ's first offline re-diff of the F-full evidence stores (§3.2), which hung after `logs` proved identical. | A finding about the **harness's embedded store backend**, not the fork or the diff logic. The interim mitigation shrank the differ page size `BATCH` from 50,000 to a fixed 5,000 rows (the F-full re-diff completed in ~65 s at that size). The **durable fix merged in [#72](../../pull/72)** replaces the fixed row count — only ever a proxy for detoast volume — with **byte-aware page sizing**: after each page the differ measures its rows' average serialized width and sizes the **next** page's row `limit` from it (`nextBatchSize(observedAvgRowBytes, targetBytes, floor, ceiling)` = `floor(target / avg)` clamped to `[5000, 50000]`; degenerate observations fall back to the floor), targeting a bounded per-query payload (default 32 MB, ~10× under the ~300 MB wedge threshold; override via `--byte-target` / `DIFF_BYTE_TARGET`), so fat-calldata tables page narrow and slim tables page wide under one byte budget. The keyset **cursor is unchanged** — the tuple-WHERE resumes strictly past the previous page's tail row regardless of page size, so the yielded row stream is identical for any limit sequence; only the `limit` varies. Pinned by the DB-free SQL-shape / sizing tests (`diff-batched.test.mjs`). |
| [#78](../../issues/78) | **CLOSED (fixed [#82](../../pull/82), merged `dcc782aa`)** | **Cell differ pathologically slow between ~54 k and ~74 k matched rows.** On the L-eth cell (§3.4) the *entire* diff of a 53,799-log store completes in **6.7 s**, but a 73,881-log store's diff **wedged** — its `logs` table alone ~2 min offline and the full diff >300 s — a superlinear cliff, not a gradual slowdown. Two L-eth diffs (`rand#1` and `rand#3` 5k) wedged; the live `rand#1` diff was **killed after ~47 min**. | A tool defect (Layer E), not a data defect — **the preserved stores re-verdicted byte-identical** with the byte-aware batched differ ([#72](../../pull/72)) in **8.3 s / 9.5 s** (§3.4), so no verdict changed and no store was lost. Worked around at run time by the harness's auto-shrink (bounded half-window re-runs); the durable **fix landed in [#82](../../pull/82)** (merged `dcc782aa`, 2026-07-07), which defaults the cell diff to [#72](../../pull/72)'s paged/byte-aware differ — the same paged reads that demonstrated the >200× recovery on the exact stores that wedged. |
| [#79](../../issues/79) | **CLOSED (fixed [#80](../../pull/80), merged `b1affeec`)** | **Results-doc tag collision across specs.** When one cell runs two window specs that reuse the same window **tags** (L-eth's seed-101 2k and seed-102 5k both emit `rand#0…rand#3`), the results JSON **folds** one spec's window records into the same-tagged records' `attempts` arrays, so the two specs' verdicts read as retries of each other. Surfaced on L-eth (§3.4), where the four 2k windows appear as `attempts` of the four 5k windows. | A **results-labelling** defect in the harness (Layer E), not a data defect: **budget sums and per-window verdicts are unaffected** — the full run is recovered by reading windows **and** attempts together (as §3.4 does), and every recorded window keeps its own range / verdict / request count. The **fix landed in [#80](../../pull/80)** (merged `b1affeec`, 2026-07-06), which makes seeded-random window tags unique across specs so records no longer fold together. |
| (matrix design; no issue) | **VACUOUS — no verdict recorded** | **Random-window factory (F-*) cells can score a vacuous 0-rows-both-sides "pass."** The `F-polygon` cell reported 4 / 4 windows `pass=true` — but with **`portal = 0`, `rpc = 0` rows across *every* table** (`logs` / `transactions` / `transaction_receipts` / `traces` / `blocks`), ~75 requests/window (an empty factory-discovery scan). A 0=0 match is **not byte-identity evidence** — it proves nothing about the Portal path. | A **harness/matrix-design** finding (Layer E), **not** a fork or dataset defect. Root cause (code-confirmed): `harness/diff/euler-app/ponder.config.ts` uses Ponder's `factory()` with `startBlock = window.from`, so factory child discovery is **anchored at the window start** — children created *before* a random isolated window are never discovered, and a window yields child data only if a `ProxyCreated` fires *inside* it. Polygon's Euler factory deployed at block 86932985 with only ~25 vaults across 2.5M blocks, so the seeded 4×50k windows (~8% coverage) all missed the creations → both Portal and RPC do the **same** empty in-window discovery → a trivial 0=0 match. This makes **all five random-window `F-*` cells** (`F-base`/`F-arbitrum`/`F-bsc`/`F-avalanche`/`F-polygon`) vacuous-prone by the same design; only **`F-full`** (eth, full-range from the factory deploy, ~872 vaults — §3.2) exercises real factory discovery, and the **`L-*` `erc20` cells** (a fixed dense token per chain) are the reliable random-window path since any window hits dense transfers. **No verdict was recorded off this vacuous pass** — `F-polygon` is *not* counted as a factory PASS; it is logged candidly here as vacuous, and all five `F-*` seeded-window cells are marked **`RETIRED`** in the matrix (§3) rather than left as open `PENDING` work. |

### 5.6 Validation-matrix data findings (Layer E) — upstream-dataset gaps

The paid matrix (Layer E) surfaced two confirmed **upstream SQD Portal dataset** gaps and, on top of
the `access_list` gap, **one genuine portal-ponder fork defect** — all caught precisely because the
gate does not wave data through. The first gap ([#76](../../issues/76)) is a `block.size` off-by-one
the L-eth cell (§3.4) flagged, held the stores for, and re-verdicted clean once bounded. The second
([#83](../../issues/83)) is a **missing `logs_bloom` column** on three datasets that the L-base cell
(§3.5) surfaced as an **8 / 8 fail-fast** — the fork's dataset-completeness guard refusing to serve
incomplete data, exactly as designed. The third case (same #83 family, dropped `access_list` column) is
where the matrix caught a real **fork defect** layered on top of the upstream gap: when the dataset
drops `access_list`, the old fork **fabricated an empty `"[]"`** for typed txs (the #27 anti-pattern) —
wrong vs chain-truth on arbitrum, where those access lists are populated. That fabrication is now
**FIXED** in [#110](../../pull/110)/[#111](../../pull/111) (store honest **NULL**), so the Portal side
no longer *lies*; the divergence *vs RPC ground truth* still remains because the upstream dataset gap
persists. Details, evidence, and the exact honesty boundary are in the row below.

| Issue | State | Finding | Attribution / layer |
|-------|-------|---------|---------------------|
| [#76](../../issues/76) | **CLOSED (tolerated via [#77](../../pull/77), generalized [#107](../../pull/107)); upstream dataset defect persists** | **Upstream Portal dataset `block.size` off-by-one at the RLP 2^16 boundary.** On blocks whose canonical RLP-encoded size is **≥ 65 540**, the Portal dataset reports `block.size` **one byte low** (`rpc = portal + 1`); every other `blocks` column, and every `logs` / `transactions` / `transaction_receipts` / `traces` row, is byte-identical. Surfaced by the **L-eth** cell (§3.4): of the 11 first-run records, 8 failed on `blocks` **only** and the 2 whose diffs wedged (#78) re-verdicted with the same signature — all 10 preserved stores carry it, and the one remaining window passed outright. **Publicly reproducible** against any public archive node — e.g. block **19963775** (in the `rand#3` 2k window) reports `size` **66755** from the Portal endpoint vs **66756** from a public RPC. | A defect in the **upstream dataset** (the SQD Portal), not the portal-ponder fork or the diff tool: the fork faithfully persists what the dataset serves, and the RPC ground truth breaks the tie. Tolerated by a precisely-scoped, self-retiring diff class (§5.3) **merged in [#77](../../pull/77)** (later generalized to any block in [#107](../../pull/107), §5.6), under which the L-eth strict tables re-verdict clean. **Closed-as-tolerated, not upstream-fixed**: the upstream dataset still serves `block.size` one byte low (block 19963775 still reports 66755 vs 66756); #76 was closed because the divergence is **benign** (non-consensus `size` field) and **handled by the documented tolerance**, not because upstream corrected the dataset. |
| [#83](../../issues/83) | CLOSED (completed 2026-07-07); underlying gap persists — re-probed 2026-07-07, `base-mainnet` still **400** on `logs_bloom`; matrix continues logs-only | **Upstream Portal datasets missing `transactions.logs_bloom` on base / arbitrum / avalanche.** The `base-mainnet`, `arbitrum-one`, and `avalanche-mainnet` datasets do **not** serve the `logs_bloom` column on the `transactions` table (probe returns **400** `couldn't parse request: column 'logs_bloom' is not found in 'transactions'`), while `ethereum-mainnet` / `polygon-mainnet` / `binance-mainnet` serve it (**200**). The gap is **exactly this one column** — the other receipt fields (`status`, `cumulativeGasUsed`, `effectiveGasPrice`, `gasUsed`, `contractAddress`) all probe 200 on the affected datasets. Surfaced by the **L-base** cell (§3.5, 2026-07-07): all **8 / 8** windows **failed fast** (48 metered requests total, 6/window) with the dataset-completeness error, because a `includeTransactionReceipts: true` app needs `transaction.logsBloom` and `receipts.logsBloom` is NOT NULL / never substituted (`portal-filters.ts` / `portal-transform.ts`). **Publicly reproducible** with a single `curl` against `portal.sqd.dev` (§3.5), no harness. | A gap in the **upstream dataset** (the SQD Portal), not the portal-ponder fork: the fork's dataset-completeness guard **correctly fails fast** rather than persisting a receipt row with a fabricated bloom that would silently diverge from RPC ground truth. Reported and tracked in [#83](../../issues/83). The matrix does not re-prove the known gap on receipts; it **continues logs-only** on the three chains via `L-base-logs` / `L-arbitrum-logs` / `L-avalanche-logs` (identical windows, `receipts: false`). Removed when the datasets add the column (already present on eth / polygon / bsc, so it is dataset backfill work). |
| #83-family (upstream column gap) + [#110](../../pull/110)/[#111](../../pull/111) (fork fix) | Upstream gap **OPEN**; fork defect **FIXED (merged [#110](../../pull/110), [#111](../../pull/111))** | **Upstream Portal datasets drop `transactions.access_list` on base / arbitrum / avalanche — and the old fork *fabricated* an empty `"[]"` for it.** Requesting `transaction.accessList` returns **400** `couldn't parse request: column 'access_list_size' is not found in 'transactions'` on those datasets (base re-verified 2026-07-07); `ethereum-mainnet` serves it **200**. Surfaced by the **L-base-logs** cell (§3.5.1): all **8 / 8** windows (16 differ records incl. auto-shrinks, 42 553 metered requests) ran to completion and **differ-FAIL** on **`transactions.access_list` only** — the Portal store held `"[]"` where RPC has the populated list; `logs` / receipts(0) / traces(0) / `blocks` byte-identical. The **root cause was two-part**: the upstream dataset drops the column (a #83-family gap), **and** the fork then converted that absence into a *fabricated* empty array. `portal-transform.ts` had `accessList: type >= 1 ? (tx.accessList ?? []) : undefined`; when the droppable-field degradation omitted the column, `tx.accessList` arrived `undefined`, `?? []` fabricated `[]`, and `encode.ts:95` stored the truthy `[]` as the string `"[]"` (not NULL) — asserting "this tx has a KNOWN-EMPTY access list" when chain-truth is a POPULATED (or, at minimum, UNKNOWN) list. That is the **#27 anti-pattern** (a known-empty value fabricated from an absent one) and **is a portal-ponder fork defect**, distinct from the pure upstream gaps (#76, #83) the fork persists faithfully. | **Fork side FIXED; upstream gap persists.** [#110](../../pull/110) (commit `a4438b6`) stores **NULL** (undefined → SQL NULL) when the column is dropped and keeps `[]` only when Portal actually returned an array. [#111](../../pull/111) (commit `abeda27`) narrowed the type predicate from `Number(tx.type) >= 1` to the **exact EIP set that carries an access list** — `ACCESS_LIST_TX_TYPES = new Set([1,2,3,4])` (EIP-2930/1559/4844/7702), gate `= …has(Number(tx.type)) && Array.isArray(tx.accessList)` — mirroring ponder's RPC-path `standardizeTransactions` (`rpc/actions.ts`) exactly, so legacy (`0x0`), OP-stack deposit (`0x7e`), and system envelopes now store NULL like the RPC path instead of `"[]"`. Mutation-verified (test flips RED→GREEN), both-version gates green (0.16.6 + 0.16.7, 303/303), committee-unanimous. **Honesty boundary — this did *not* make these chains byte-identical to RPC on `access_list`:** the upstream dataset still doesn't serve the column, so the Portal store now holds **NULL** while an RPC-backfill store holds the **populated list** — the divergence *vs RPC ground truth REMAINS* (the upstream #83-family gap). What changed is that the Portal side is now **honest** (NULL = "we don't have this column") instead of **lying** (`"[]"` = "this tx has an empty access list"). See the arbitrum evidence + #32 refinement in the next row. |
| refines [#32](../../issues/32) (chain 42161) | #32 **CLOSED 2026-07-10** (fork defect fixed + proven FAIL→PASS); upstream column gap → #83-family; fork side **FIXED** by [#110](../../pull/110)/[#111](../../pull/111) | **The fabricated-`"[]"` shape is *systematic* on arbitrum, not a single row.** An **L-arbitrum-logs probe** (chain 42161, `app=erc20`, dense-USDC windows, `receipts: false`) hit the divergence: window `[358633493, 358635493]` had `logs` byte-identical (5172 = 5172) and `blocks` identical (1286), but `transactions` diverged on **34 rows, `access_list` field only** (Portal `"[]"`, RPC the populated list); a second window `[308759033, 308761033]` reproduced it — **2 / 2 windows, systematic**. **Chain-truth confirmed against an independent public node** (`arb1.arbitrum.io/rpc`): block 358633596, tx `0x504a05d5…` (type `0x2`) carries a **20-entry** access list — so the RPC-backfill store *and* the public node agree (populated), and the old Portal store was the wrong side. Access lists are rare on arbitrum (≈1 non-empty per block), so effectively every real-AL tx in-window was being emptied. | This **refines/challenges [#32](../../issues/32)**: #32 was filed as a *single* fabricated-`[]` tx found by comparing two independently-synced stores — but because both were *Portal-backfill* stores they fabricated `[]` identically and **agreed**, so the differ saw n=1. The Portal-vs-RPC comparison exposes it as **systematic on arbitrum**, so #32's single-hash `knownBadRows` pin **under-covers the blast radius**. #32 is now **CLOSED** on the fork side (the fabricated-`"[]"` defect is fixed by [#110](../../pull/110)/[#111](../../pull/111) and **proven gone** — see the validation note below); the residual Portal-NULL-vs-RPC-populated divergence is the upstream #83-family column gap. *(The probe that surfaced this finding was later completed as a full FAIL→PASS matrix run on a post-#110 build — see the validation note directly below the table.)* |

**Validation — the fork fix proven FAIL→PASS on all three gap chains (2026-07-10).** After #110/#111
merged, the three logs-only gap-chain cells were re-run on a fork build carrying the fix and byte-diffed
against a stock JSON-RPC backfill over the same seeded windows: **`L-arbitrum-logs` 8 / 8**,
**`L-avalanche-logs` 8 / 8**, and **`L-base-logs` 16 / 16** windows **PASS** (0 fail; ~14.8k / 19.3k /
42.5k metered requests respectively). The arbitrum cell — including the exact window
`[358633493, 358635493]` that #32's probe flagged (one of its failing windows) — was **2 PASS / 6 FAIL**
(`access_list`-only) on the pre-fix build, and the base cell was **16 / 16 FAIL** (§3.5.1); both flip to
full PASS — a clean before/after on the same windows and the same differ. The
mechanism is exactly the honesty boundary above: pre-fix the Portal side emitted a fabricated `"[]"`,
which the [#113](../../pull/113) differ **hard-fails** as a regression sentinel; post-fix it emits SQL
**NULL**, which the #113 `access_list`-column-gap tolerance **accepts** (the column is genuinely absent
upstream) — every other column stays byte-identical. This is what **closes [#32](../../issues/32)** on
the fork side (the fabricated-`"[]"` defect it reported is gone and proven gone); the residual
Portal-NULL-vs-RPC-populated divergence is the unchanged upstream #83-family dataset gap. *(These are now
completed clean matrix cells and carry their §3.x/matrix PASS rows above.)*

**Why the L-base cell fails fast, and why that is correct.** Unlike the tolerated `block.size` class
above, the `logs_bloom` gap is **not tolerable**: `receipts.logsBloom` is a NOT NULL, bloom-load-bearing
column that the fork deliberately **never** fabricates (`portal-transform.ts`), so once a range contains
matched data the historical sync must **refuse it** rather than persist an incomplete receipt row.
Serving a placeholder bloom would silently break the byte-identity claim this whole document rests on —
so the crash is the guard doing exactly its job, with an actionable error (report the gap, or start the
indexer past the affected range). The matrix therefore does not burn budget re-proving a known upstream
gap: it runs the **logs-only variants** on base / arbitrum / avalanche (which still prove the
Portal-vs-RPC byte-identity of logs and transactions on those chains) and leaves the receipts cells as
the honest record that the gap exists. Candor point: **the cell failed, the failure is a genuine
upstream dataset gap, and the fork's guard did exactly its job** — refusing to serve incomplete data is
the behavior a billion-TVL integrator wants.

**Why the block.size class is tolerated rather than failed.** The tolerance is not a blanket "ignore blocks" waiver:
it accepts a `blocks` row as matching **only** when the *sole* divergence is `block.size`, the RPC
value is exactly the Portal value **+ 1**, and the block's canonical size is at/above the 2^16 boundary
where the bug lives — any second differing column, any other delta direction, or a sub-threshold size
is still a hard FAIL. It is reported per-window (the L-eth re-verdict prints the tolerated count for
every store, §3.4) and it **removes itself** the moment the upstream dataset serves the correct size.
This is the same discipline as the A/B differ's tolerated classes (§5.3): declared, bounded, and
designed to be deleted. The candor point is the headline: **the byte-diff gate caught a real
upstream-dataset defect that a coarser check would have waved through** — that is evidence the gate
works, not a blemish on it.

### 5.7 Layer-F third-party corroboration — consolidated

A campaign acceptance requirement is **count parity across all 15 chains paired with independent
third-party corroboration**. The evidence for it is real but was scattered across §5.1, §5.6, the
benchmark report, and the A/B differ's status JSON. This subsection consolidates it in one place so a
reader can confirm the requirement is met **by evidence, not by waiver** — while being fully candid
about the two further third-party sweeps that were *designed but not executed*.

Three distinct things are kept strictly separate below, because conflating them is precisely the error
this section exists to prevent:

- **(a) internal count-parity** — a fresh Portal-only backfill vs the frozen internal reference store.
  Both are *internal* stores, so this is **not itself third-party**; it is the count half the
  requirement pairs *with* third-party evidence.
- **(b) genuinely third-party corroboration** — Euler's own public Goldsky subgraphs (count) and
  independent public archive nodes (field-level tie-breaks).
- **(c) Layer-E Portal-vs-RPC byte-identity** — the paid matrix (§3, §5.6). This is **not Layer F**;
  it is cross-referenced only as context for where the field-level tie-breaks (below) sit.

#### (A) Count parity across all 15 chains — the "15/15" half *(internal, not third-party)*

Source of truth: [`harness/bench/results/flagship-2026-07-06/parity-report.json`](harness/bench/results/flagship-2026-07-06/parity-report.json)
(`pass: true`, `diffs: []`, **62 cells**, `generatedAt` 2026-07-06). A fresh Portal-only backfill of
all 15 chains was compared against the **frozen reference store** over `ponder_sync`: per-chain `logs`
(`logCount` / `minBlock` / `maxBlock` / `distinctTx`) for every chain, plus **whole-store** `blocks` and
`transactions` totals. **All 62 aggregate cells identical, 0 diffs.**

| Aggregate | bench | reference | match |
|-----------|------:|----------:|:-----:|
| whole-store `blocks` total | **4,646,445** | 4,646,445 | ✓ |
| whole-store `transactions` total | **5,007,056** | 5,007,056 | ✓ |
| Σ per-chain `logs` (log rows) | **28,405,932** | 28,405,932 | ✓ |
| distinct chains compared | 15 | 15 | ✓ |

This is a **whole-store aggregate parity of two internal stores** — the complete stored range of every
chain, not sampling — so it establishes the count-parity-15/15 half. It is **not** itself third-party
(both stores are ours); it is what the requirement pairs *with* the third-party corroboration below.
Full account: [`harness/euler-multichain/REPORT.md`](harness/euler-multichain/REPORT.md) §Correctness
item 0.

#### (B) Independent third-party count corroboration — Euler's own Goldsky subgraphs

This is **genuinely third-party**: the discovered vault set was cross-checked against **Euler's public
Goldsky subgraphs** (`euler-v2-<net>/latest` and `euler-simple-<net>/latest`), **live-verified
2026-07-03** ([`harness/euler-multichain/REPORT.md`](harness/euler-multichain/REPORT.md) §Correctness
item 3). On the chains whose Euler subgraph is itself caught up, the discovered vault count matches
exactly:

| chain | vaults (this indexer) | vs Euler Goldsky subgraph |
|-------|---------------------:|:--------------------------|
| berachain | 59 | **59 / 59 ✓** |
| polygon | 25 | **25 / 25 ✓** |
| bob | 27 | **27 / 27 ✓** |
| tac | 36 | **36 / 36 ✓** |

**hyperliquid — a match, but with a caveat, not a clean one.** At the pinned benchmark head (2026-07-01)
Euler's then-deployed HyperEVM subgraph reported **zero** vaults; it was **redeployed**, and a live
re-check on **2026-07-03** reports the same **58** this indexer found — but that redeployed subgraph is
itself still **stalled ~4M blocks behind the HyperEVM tip** (all 58 predate its indexed head). So the
58/58 corroboration is real *as of the 2026-07-03 re-check*, but it is **not** the "steady-state
subgraph agrees" case the four rows above are — it is disclosed here rather than folded into that table
precisely because a stalled/redeployed third-party source is weaker corroboration than a caught-up one.

The larger-chain gaps are **not missing data** — they resolve to created-after-our-fixed-head plus
discovered-but-eventless. For ethereum the Portal shows **872** vaults created by our fixed head vs
Euler's **897**: **25 were created *after* our fixed head** (Euler's subgraph runs ahead of the pinned
benchmark head), and the remaining **22 are discovered-but-eventless** (created, never used) —
confirmed because our indexed event total equals the Portal total for the events that do exist. (The
per-chain benchmark table also lists an *indexed* vault count of 850 for ethereum, a distinct quantity
from the 872 discovered-by-head used for this gap breakdown; the two are not conflated.) This is
third-party count corroboration against the data owner's *own* published indexer.

#### (C) Field-level third-party tie-breaks — independent public archive nodes

Where the RPC oracle and the Portal path disagreed on a *field*, the tie was broken against an
**independent public archive node** — establishing which side carried chain-truth. These already appear
in §5.1 / §5.6; consolidated here:

| Field / issue | Third-party source | The tie-break | Cross-ref |
|---------------|--------------------|---------------|-----------|
| arbitrum `access_list` (refines [#32](../../issues/32)) | public node `arb1.arbitrum.io/rpc` | block **358633596**, tx `0x504a05d5…` (type `0x2`) carries a **20-entry** access list — public node *and* RPC-backfill store agree (populated); the old Portal `"[]"` was the wrong side (fork defect, now NULL — [#110](../../pull/110)/[#111](../../pull/111)) | §5.6 |
| ethereum `block.size` ([#76](../../issues/76)) | any public archive node | block **19963775** reports `size` **66755** from the Portal endpoint vs **66756** from a public RPC — **publicly reproducible**; the upstream dataset serves it one byte low at the RLP 2^16 boundary, the fork persists it faithfully | §5.3 / §5.6 |
| A/B soak spot-audit ([#27](../../issues/27), [#36](../../issues/36)) | independent public node (Layer D) | in the confirmed cases leg-B (Portal) rows matched an independent public node **byte-for-byte**, establishing leg-A (RPC realtime) as the **lossy** side; `harness/soak-ab/ab-diff.mjs` exports a bounded `toleratedIssue36Sample` in the status JSON so this audit stays **reproducible** against a third-party node | §5.1 / §5.3 |

#### (D) Publicly-reproducible upstream-dataset gap probes ([#83](../../issues/83))

The `logs_bloom` column gap on base / arbitrum / avalanche is corroborated by **anyone**: it reproduces
with a single public `curl` against `portal.sqd.dev` (§3.5), no harness and no auth — a `400`
`column 'logs_bloom' is not found in 'transactions'` below each chain's boundary vs a `200` on
ethereum / polygon / binance. This makes the *gaps* themselves independently verifiable, not just
asserted from our own runs. Cross-ref §3.5 / §5.6.

#### (E) Candor — designed but NOT executed

The campaign methodology also **designed** two further third-party sweeps that were **not run**:

1. **Etherscan V2 `getLogs` field-level spot-checks** (~30 windows) plus `txlist` for the accounts cell.
2. **Dune full-range per-chain totals.**

Both were **not executed** — **no third-party explorer/analytics API key was provisioned for this
campaign** — and they are recorded here as **optional further corroboration, not load-bearing**. What
substantiates the third-party requirement is the count-parity (A) **plus** the independent Goldsky
corroboration (B) **plus** the public-node field-level tie-breaks (C). Stating this plainly is
deliberate: silently omitting a designed-but-unrun check is the exact **#27 anti-pattern** (asserting a
known-empty result from an absent one) that this project treats as a cardinal sin — so the boundary is
named, not soft-pedaled.

**What is proven / what remains optional:**

- **Proven:** 15/15 whole-store count parity (A); independent Goldsky vault-count corroboration on the
  fully-active chains, gaps explained not missing (B); public-node field-level tie-breaks that put
  chain-truth on the Portal side of every confirmed A/B divergence (C); publicly-`curl`-reproducible
  upstream gap probes (D).
- **Optional (not run):** Etherscan `getLogs`/`txlist` spot-checks and Dune per-chain totals (E).

**Verdict.** The **count-parity-across-15-chains paired with independent third-party corroboration**
requirement is **met by evidence**, not by waiver: the 15/15 aggregate parity (A) is corroborated
independently by Euler's own Goldsky subgraphs at the count level (B) and by independent public archive
nodes at the field level (C), with the upstream gaps themselves publicly reproducible (D). The honest
boundary is that third-party corroboration is treated throughout as **corroboration, not oracle** (§2,
Layer F) — the stock JSON-RPC path remains the byte-identity oracle — and that the additional
Etherscan and Dune sweeps (E) remain **optional further corroboration, not executed and not
load-bearing**.

---

### 5.8 Reorg & finality correctness — consolidated

Reorg-correctness evidence exists, but it was scattered — the finalized-only backfill design lives in
`HOW-IT-WORKS.md`, the realtime reconciliation logic in `portal/portal-realtime.ts` /
`portal/portal-realtime-wire.ts` with ~30 unit tests, the crash/resume durability in Layer C (§4), and
the finalized-overlap agreement in Layer D (§2.D, §5.1–§5.3). This subsection consolidates it so a
reader can confirm what is proven **without conflating three separate things**, and can see the
deliberate limit stated plainly. The verdict: **reorg-correctness rests on finalized-only backfill
(reorg-free by construction) plus tested realtime reconciliation, with a reorg below the finalized
floor a deliberate fail-loud (operator re-syncs) rather than a silent rollback** — the stream path that
carries the realtime half is **experimental** (§1, §5.2), not certified for unattended production use.

The three things kept strictly separate below are: **(A)** the historical backfill, which never sees a
reorg by construction; **(B)** the realtime reconciliation, which is **code + unit-test** evidence for
the *logic* that handles forks; and **(C)** the A/B soak, which is **empirical** evidence that the two
independent legs agree on the finalized overlap — hourly, within declared tolerances, on an
experimental path. Conflating "the logic is tested" with "we survived a live deep reorg in production"
is exactly the error this section exists to prevent.

#### (A) Historical backfill is reorg-free by construction

The Portal serves **only finalized data** for the historical range, so historical rows are **write-once
and never walked back**: `HOW-IT-WORKS.md` states it directly — *"Backfill is reorg-free. The Portal
serves only finalized data, so historical rows never need to be walked back"* — and the sync split is
clean by design: **historical owns `[start, finalized]`; realtime owns `(finalized, tip]`**
(`HOW-IT-WORKS.md`). There is no reorg path in the backfill because there is no unfinalized data in it.

This is a **design/construction** argument, not a test result, and is labelled as such. It is
**corroborated** — not proven — by the Layer-C chaos evidence (§4): 203 Tier-0 `SIGKILL`s across 41/41
completed backfills reached a store **byte-identical** to an unkilled baseline, and Tier 1 added
attributable resume-from-partial on a crash-durable backend (§4.2–§4.3). **Important boundary: Layer C
tests crash/resume *durability*, it does NOT inject reorgs.** It shows the backfill commits atomically
and resumes correctly across kills; it says nothing about reorg handling, and is cited here only as
corroboration that the write-once store behaves as the reorg-free design claims. The two must not be
conflated.

#### (B) Realtime reorg reconciliation — code + mutation-verified unit tests

When realtime moves to the Portal `/stream` path (`PORTAL_REALTIME=stream`), reorgs are reconciled from
the stream's parent-hash chain and surfaced as **Ponder's own `reorg` / `finalize` events**, so
handlers and checkpointing are unchanged (`HOW-IT-WORKS.md`). The reconciliation core is **pure and
unit-tested** (`portal/portal-realtime.ts`, the "pure reorg / finalize core"): `reconcile()` classifies
each newly-streamed block against the local unfinalized chain as `append` / `duplicate` / `reorg` /
`gap`, and `takeFinalized()` splits the unfinalized chain at a newly-finalized number. A **fork point
below the finalized floor has no safe recovery, so it is FATAL rather than rewound** — the code comments
and the wire's floor logic (`portal/portal-realtime-wire.ts`) state this directly. This is **evidence
for the LOGIC**, backed by ~30 reorg/finality unit tests across `portal/portal-realtime.test.ts` and
`portal/portal-realtime-wire.test.ts`. The load-bearing behaviours and their exact tests:

| Behaviour (what the logic must do) | Evidencing test (exact name, file) |
|------------------------------------|------------------------------------|
| classify append / duplicate / reorg (earlier common ancestor) / deep-fork-to-base / gap | `reconcile: append extends the tip (and the anchored empty chain)` · `reconcile: duplicate tip is idempotent (re-delivery)` · `reconcile: reorg forks off an earlier common ancestor, reorged blocks after it` · `reconcile: deep-fork reorg to the base` · `reconcile: gap when the parent is unknown (beyond our window)` — `portal/portal-realtime.test.ts` |
| depth-1 fork **at the finality boundary** reorgs off the anchor, not a fatal gap | `reconcile: a depth-1 fork at the finality boundary reorgs off the ANCHOR instead of a fatal gap` · `reconcile: an EMPTY window with an anchor appends ONLY a child of the anchor (else duplicate/gap)` — `portal/portal-realtime.test.ts` |
| finalized split at a number | `takeFinalized: splits the chain at the finalized number` — `portal/portal-realtime.test.ts` |
| a re-streamed fork emits a `reorg` to the common ancestor end-to-end | `portalRealtimeEvents: a re-streamed fork emits a reorg to the common ancestor` — `portal/portal-realtime.test.ts` |
| an unknown-parent **gap is FATAL**, not silently skipped | `portalRealtimeEvents: an unknown-parent gap is FATAL, not silently skipped (finding 7)` — `portal/portal-realtime.test.ts` |
| a 1-block **orphan at tip heals** via 409 fork negotiation (no gap fatal) | `portalRealtimeEvents: a 1-block orphan at tip heals via 409 fork negotiation — orphan N−1 then canonical N becomes reorg + appends, no gap fatal (issue #33 T1)` · `getPortalRealtimeEventGenerator: a 1-block orphan at tip HEALS via 409 fork negotiation — the wire emits reorg→block→block with hex LightBlocks and the #26 child-prune fires on the reorg (issue #33 T5)` — `portal/portal-realtime.test.ts` / `portal/portal-realtime-wire.test.ts` |
| 409 fork negotiation **steps the cursor down** and terminates (no-match step-down; oscillation cap; deep step-down to floor; bodyless 409 still drives) | `streamHotBlocks: a 409 whose previousBlocks match NOTHING steps the cursor down one block per retry and fatals at the finalized floor — nothing yielded past the fork (issue #33 T3 step-down)` · `streamHotBlocks: an OSCILLATING 409 loop (server keeps re-409ing a rewind the ring confirms at the SAME height → no cursor progress) fatals at the 10 no-progress cap (issue #33 T3 cap)` · `streamHotBlocks: a no-match step-down descending MORE than 10 heights reaches the FLOOR fatal, NOT the no-progress cap — a deep negotiation runs "until a match or the floor" (issue #33 T3 deep step-down / F2)` · `streamHotBlocks: a BODYLESS 409 (res.body === null) still DRIVES the fork negotiation — it does NOT silently re-poll forever (issue #33 F1)` — `portal/portal-realtime.test.ts` |
| a fork point **below the finalized floor is FATAL with no rewind** | `streamHotBlocks: a 409 fork point BELOW the finalized floor is FATAL with no rewind (issue #33 T4 below-finality)` — `portal/portal-realtime.test.ts` |
| a **wrong-fork finalize is FATAL** (canonical hash mismatches the local block) | `portalRealtimeEvents: a finalize whose canonical hash mismatches the local block is FATAL (wrong-fork finalize)` — `portal/portal-realtime.test.ts` |
| a reorg **prunes reorged-out factory children** from the running map | `getPortalRealtimeEventGenerator: a reorg PRUNES reorged-out children from the running map and narrows the filter` — `portal/portal-realtime-wire.test.ts` |
| stream-mode finalized boundary **never RAISES** above RPC finalized; **floor never regresses** on restart | `clampFinalizedToPortalHead: Portal at/ahead of RPC finalized → no clamp (never RAISES the boundary)` · `clampFinalizedToPortalHead: a probed head BELOW the persisted floor is clamped UP to the floor — a restart against a lagging replica must not re-stream already-finalized (unrevertable) blocks` · `clampFinalizedToPortalHead: the floor overrides a PORTAL_FINALIZED_HEAD pin below it — a pin below persisted finality must not re-open the double-indexing hole` — `portal/portal-realtime-wire.test.ts` |
| **foreign-checkpoint restart** maps a foreign checkpoint's timestamp to a local floor (omnichain, [#57](../../issues/57)) | `deriveFinalityFloor: a FOREIGN checkpoint is TIMESTAMP-MAPPED to the local chain’s highest block at/below that timestamp — the floor is a LOCAL block, not the foreign height (issue #57)` · `deriveFinalityFloor → clampFinalizedToPortalHead: the timestamp-mapped floor actually CLAMPS the stream-mode boundary UP — a lagging replica must not re-stream already-finalized blocks after an omnichain restart (issue #57)` — `portal/portal-realtime-wire.test.ts` |

These tests are **mutation-verified** (each fails on the pre-fix code) and gate on **both supported
upstream Ponder versions** via `scripts/sync-upstream.sh <ver> --test`, in line with this document's
standing evidence rule (§6: *"every fix is backed by a mutation-verified regression test"*). Much of
this logic landed as the **stream-realtime correctness wave, PR [#26](../../pull/26)** (§5.2), whose
follow-up hardening (the 409 step-down / floor / diagnostics) is captured by the `issue #33`-tagged
tests above.

**On [#33](../../issues/33) — not a reorg.** [#33](../../issues/33) is often mis-remembered as a reorg
bug; it is not. Its own title records it: *"stream realtime: canonical block fataled with 'unknown
parent' — parent was the canonical block at N−1 (no reorg involved)"*. It was a **false
`unknown-parent` fatal on a canonical N−1 block**, with **no chain reorg involved** — hardened by the
409 delivered-hash-ring / step-down fork negotiation (the `issue #33 T1…T5` tests above) and now
**CLOSED**. It is listed here so it is not misread as an open reorg defect.

#### (C) A/B soak — finalized-overlap identity (empirical, hourly, experimental path)

The Layer-D A/B soak (§2.D) runs two independently-synced legs — **Leg A** realtime over JSON-RPC and
**Leg B** realtime over the Portal `/stream` path — and the differ (`harness/soak-ab/ab-diff.mjs`)
compares them **hourly on their finalized overlap window** `[cutover, min(finalizedA, finalizedB) −
margin]`. On that window it asserts strict `logs` and `blocks` row-set + field identity
(`total_difficulty` excluded) and byte-identity of any transaction present on **both** sides, within a
small set of **pre-declared tolerated classes** (§5.3). This is **empirical** evidence that the two
independent realtime paths — including their reorg/finalize handling — **converge to the same finalized
history**: a disagreement localises to one leg, and so far every confirmed divergence has been a
**leg-A (RPC realtime) defect**, with the Portal leg matching third-party evidence (§5.1, §5.7 (C)).
Relevant here: the RPC-realtime-mode issues [#27](../../issues/27) (realtime `access_list` stored NULL
including for txs with a real on-chain list) and [#23](../../issues/23) (deterministic crash when a
block's full-block `eth_getLogs` exceeds viem's 10 MiB cap) are on the **RPC** path that the Portal
`/stream` path specifically avoids — the stream leg preserves those access lists and streams through
those dense blocks — so they are stated accurately as a **Portal-path strength**, not as reorg defects.

**Boundaries — what (C) does and does not show.** The differ is **hourly on the finalized-overlap
window**, not continuous and not over the unfinalized tip; it establishes that the *finalized results*
agree, which is where correctness must hold. The stream leg it exercises is **experimental** (§1,
§5.2). Two currently-open issues on that path are **robustness, not reorg-correctness**, and are named
so as not to overstate the claim: [#48](../../issues/48) is a stream-wire teardown/undici abort-hang
race (a shutdown-unwind hardening item), and [#53](../../issues/53) is a `factory_addresses`
idempotence gap that is app-invisible but breaks store-identity tooling (a sync-store tooling item).
Neither is a reorg-correctness defect.

#### What this section does NOT claim

Candor is the product; the delta is stated, not hidden (the [#27](../../issues/27) anti-pattern is
hiding a delta):

- **Stream-realtime is NOT declared production-ready.** It is **experimental** (§1, §5.2) — this
  document *"does not certify it for unattended production use."* The reorg logic being tested does not
  change that label.
- **A reorg deeper than the finalized floor is FATAL by design** — the process fails loud and the
  operator restarts to re-sync. This is a **deliberate safety choice, not a recovered/healed
  scenario**: a fork below finality has no safe rollback, so re-syncing from finalized truth is safer
  than attempting to walk back committed rows. Do not read the `below-finality` FATAL tests as "we
  recover from deep reorgs" — they prove we **fail loud** on them.
- **The realtime reorg evidence is UNIT-TEST + short-soak, not a multi-week live-reorg record.** No
  live deep reorg was **injected against the running soak**; (B) proves the reconciliation *logic* and
  (C) proves *finalized-overlap agreement* on the soak's naturally-occurring traffic. There is **no
  claim of empirical live deep-reorg survival** beyond what the tests and the hourly differ show.
- **Layer C (§4) proves crash-durability, NOT reorg recovery.** The chaos kill-loop injects `SIGKILL`s,
  not reorgs; it corroborates the write-once backfill store (A) and must not be cited as reorg evidence.

**Verdict.** Reorg-correctness is **met for the historical path by construction** (finalized-only,
write-once — (A)) and **evidenced for the realtime path by mutation-verified unit tests of the
reconciliation logic (B) plus hourly finalized-overlap A/B agreement (C)**. The honest boundary is that
the realtime half runs on an **experimental** path, that a reorg below the finalized floor is a
**deliberate fail-loud** rather than a healed recovery, and that no **live deep reorg** was injected
against the running soak — the evidence is the tested logic and the differ, stated as exactly that.

---

## 6. Current status — what a reader can rely on today

**Proven today (with reproducible evidence in this repo):**

- The Portal layer's invariants (INV-1 … INV-18) hold under property-based tests **on all three
  supported upstream Ponder versions** (`0.15.17`, `0.16.6`, `0.16.7` — the last registered by
  [#74](../../pull/74) on a seam-identity + full-suite basis, §2), and every fix is backed by a
  mutation-verified regression test.
- **Crash/resume is safe, and resume-from-partial-persisted-state is now proven.** Tier 0 (PGlite,
  §4.1): 203 kills across 41/41 completed backfills, `SIGKILL`-atomic and restart-idempotent,
  byte-identical to an unkilled baseline across all five row families, intervals tiling exactly, zero
  invariant violations. Tier 1 (crash-durable native Postgres, §4.2, ACCEPTED 2026-07-05) closes the
  remaining gap: with small fixed chunks the durable store advances in a staircase, and the campaign
  recorded **155 kills at partial durable coverage** and **17 completions that resumed from partial
  persisted state** to a **logically-identical** final store (surrogate-id-excluding digest + exact
  interval tiling). So *attributable* resume-from-partial is evidenced today — the property Tier 0
  alone could not witness (the [#50](../../issues/50) granularity shape, since fixed by
  [#91](../../pull/91)). This Tier-1 acceptance was **reproduced on the
  current `main` build (`248f41e`) on 2026-07-06 (§4.3, ACCEPTED)** — same methodology, all thresholds
  cleared again (**236 kills, 31 verified completions, 166 at partial coverage, 17 resumed from
  partial**, zero failures) — and every completed run reproduced the earlier build's baseline digest
  exactly, so the two `main` builds are **byte-identical at the sync-store row level** over the range
  (cross-build store equivalence).
- The fork-vs-stock **byte-diff plumbing** is proven end-to-end by the SMOKE cell (byte-identical
  across all five row families on public endpoints, §3.1), and the **flagship `F-full` cell is DONE**
  (§3.2): the full recorded Euler v2 history on eth mainnet (`[20529207, 25436954]`, 4.9M blocks) is
  **byte-identical** to a stock `ponder@0.16.6` JSON-RPC backfill across `logs` / `transactions` /
  `transaction_receipts` / `traces` and every event-bearing `block` (885,893 / 276,674 / 276,674 / 0 /
  252,396 rows). The chain of custody (a killed first attempt, a differ that wedged pre-[#59](../../pull/59),
  and an offline re-diff of the archived stores with the fixed tool) is stated in full in §3.2, as is
  the **app-hash caveat** — the app-table determinism hash was vacuous (no user rows written), so the
  byte-identity is proven at the sync-store row level only.
- Two further paid cells are now **DONE**. The **inertness control `CTRL`** (§3.3) is **10 / 10**
  seeded-random eth windows byte-identical (114,921 matched logs, 39,937 metered requests, zero
  shrinks/retries), proving the Portal patch is **inert when the Portal environment is unset**. The
  **eth Layer-L cell `L-eth`** (§3.4) is **PASS under the documented `block.size` tolerance**: all
  **10 / 10** preserved stores are byte-identical on `logs` / `transactions` / `transaction_receipts` /
  `traces` (strict tables), with every first-run `blocks`-only failure fully explained by the
  upstream-dataset off-by-one [#76](../../issues/76) — the first case where the **gate caught a real
  upstream data defect** (and, separately, a real harness perf defect [#78](../../issues/78)), which is
  the evidence the gate works.
- The A/B dual-implementation soak is **actively cross-validating** the Portal path against the RPC
  path hourly, and every divergence it has found is a **public, tracked issue** (§5) — with the Portal
  leg matching third-party evidence in each confirmed case.
- **Reorg & finality correctness** rests on a **reorg-free historical backfill by construction**
  (finalized-only, write-once) plus **mutation-verified unit tests** of the realtime reconciliation
  logic and **hourly finalized-overlap A/B agreement** — with a reorg below the finalized floor a
  **deliberate fail-loud**, on the experimental stream path; consolidated in §5.8.

**Pending / not yet claimed:**

- The **full paid byte-diff matrix** (§3) — SMOKE, the flagship `F-full`, the inertness control `CTRL`,
  and the eth Layer-L cell `L-eth` are DONE (§3.1–§3.4); the remaining cells are PENDING. **This
  document will update each row as its cell completes.**
- The **flagship benchmark gate** (speed reproduced within tolerance of the published baseline) —
  see a separate benchmarks document (pending publication); not asserted here.
- A **multi-day green-soak** sign-off and **GA of the zero-RPC realtime (stream) path** — the stream
  path remains experimental while #33 and the longer soak are open.

Read this document as: *the mechanical byte-equality of the Portal historical-backfill path is
strongly evidenced and its crash-resume behavior is accepted — including attributable
resume-from-partial-persisted-state on a crash-durable backend (§4.2); the full multi-chain paid
matrix, benchmark parity, and long-soak/stream GA are still in flight and are called out honestly
above.*

---

## 7. Reproducing this evidence

All tooling is `bash` + `node` only (no extra dependencies) and lives in this repo:

| Evidence | Tool | Doc |
|----------|------|-----|
| Unit + invariant suite (all three versions) | `scripts/sync-upstream.sh <ver> --test` | `CLAUDE.md`, `portal/INVARIANTS.md` |
| Chaos kill-loop + resume (Tier 0, PGlite byte-diff) | `harness/chaos/kill-loop.sh`, `verify-resume.sh`, `proxy.mjs` | `harness/validate/README.md` §Chaos |
| Chaos resume-from-partial (Tier 1, Postgres logical-digest) | `harness/chaos/chaos-pg-driver.sh`, `build-baseline-pg.sh`, `pg-ctl-chaos.sh`, `verify-resume-pg.sh`, `pg-digest.mjs`, `snapshot-coverage-pg.mjs`, `check-intervals-pg.mjs` | §4.2 (this doc); tool headers |
| Validation matrix (fork vs stock) | `harness/validate/run-cell.sh`, `ctrl-cell.sh`, `cells.json` | `harness/validate/README.md` |
| A/B soak differ | `harness/soak-ab/ab-diff.mjs` | `harness/validate/README.md` §Soak, `ab-diff.mjs` header |

Paid cells require operator-supplied endpoints (a Portal tarball and an RPC key); the harness meters
every request and enforces a cumulative budget guard (`harness/validate/budget.json`,
`budget-sum.mjs`). See `harness/validate/README.md` for the full environment contract.

The Tier-1 (Postgres) chaos tools are fully env-parameterized — no absolute paths are baked in.
`SQD_PONDER_TARBALL` (the tarball under test) and `CHAOS_APP` (a Postgres-backed ponder app dir whose
store connection string is read from `CHAOS_PG_URL`) are required; everything else defaults
(`CHAOS_PORTAL`/`CHAOS_RPC` public endpoints, `CHAOS_FROM`/`CHAOS_TO` the range, `CHAOS_CHUNK_BLOCKS`/
`CHAOS_CHUNK_FIXED`/`CHAOS_READAHEAD` the staircase params, `CHAOS_PGPORT`/`CHAOS_WORK` the throwaway
cluster on a dedicated port under a scratch workspace). `pg-ctl-chaos.sh` initializes and manages that
crash-durable, single-purpose cluster; `build-baseline-pg.sh` builds the reusable baseline (and proves
the digest is deterministic across a `pg_ctl` restart); `chaos-pg-driver.sh` runs the campaign loop and
also has a `selftest` mode that exercises the digest's id-shift invariance / mutation / duplication
detection against a live cluster with no backfill. Each tool's header documents its own env contract.

---

*This is a living document. Sections 3 (matrix), 4 (chaos), and 5 (findings) are updated as the
campaign produces new evidence. Where a result is not yet in, the row says so — an empty result is
never presented as a pass.*
