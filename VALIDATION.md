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
  cell and the flagship full-range cell (`F-full`, §3.2) have completed byte-identical; the remaining
  cells are still pending.
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
| `F-full` | euler | eth | factory, logs, transactions, receipts | full range [deploy → pinned head] | **DONE (2026-07-06)** | `bash harness/validate/run-cell.sh F-full` |
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
- **`SMOKE`** proved `run-cell.sh` end-to-end (§3.1). It is deliberately outside the paid matrix.
- **`CTRL`** is the *inertness* control: genuine upstream `ponder@0.16.6` vs the fork with the Portal
  path unset, same config → proves the Portal patch does not perturb the stock RPC path. It runs via
  `ctrl-cell.sh`, not `run-cell.sh`.
- **`F-full`** is the flagship byte diff — full Euler history on eth, `[20529207 → 25436954]`, head
  **pinned in `cells.json` for reproducibility**, every sync-store row diffed by the constant-memory
  `diff-batched.mjs` plus an app-table checkpoint hash. **DONE (2026-07-06): byte-identical across all
  row families — verdict, numbers, chain of custody, and the app-hash caveat in §3.2.**
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
   input (detoast volume). The **same rows in 5,000-row pages** complete at ~1.5 s/page; re-paginated,
   the full re-diff of all tables finished in ~65 s (output-file create→last-write timestamps in the evidence archive). Both tool variants' `sha256` digests were recorded
   in the operator's evidence archive at run time (the fixed-keyset differ before the page-size change:
   `9d1756d5df7f0ee484dbd34ed2ba3ff55de21944dad872789b26c225a5bcccea`; after:
   `bbc93163b82cd63deb52761b35bd57361b5529834d9f4c277203c8897574b719`).
5. **Net.** The repro command in the matrix stays valid **going forward**: [#59](../../pull/59) removed
   the `transactions` wedge, and **this document's PR** removes the detoast hang by shrinking the
   differ's page size to 5,000 rows (`diff-batched.mjs`, §5.5). A *fresh* end-to-end run is therefore
   now unobstructed — but it costs ~$6 of metered RPC and ~2.5 h of backfill, so the verdict above
   stands on the archived-store re-diff rather than on a re-run.

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
> itself (the first-commit granularity / availability shape) remains **open and unaffected** — Tier 1
> works *around* it with small chunks rather than fixing it.

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

### 5.4 Chaos-discovered findings (Layer C)

| Issue | State | Finding | Attribution / layer |
|-------|-------|---------|---------------------|
| [#50](../../issues/50) | OPEN | **First-durable-commit granularity.** The fork's historical path makes its first durable sync-store commit only after **full-range factory discovery + the entire first data chunk** stream (default `PORTAL_CHUNK_BLOCKS` 500k); for a range inside one chunk the durable store goes **0% → 100% in one transaction, seconds before completion**. A restart loop shorter than that window makes **zero forward progress** and re-pays discovery + chunk re-stream each cycle (upstream, which commits proportionally to a 25-block first interval, creeps forward instead). An availability/progress regression vs upstream, with a zero-progress-livelock shape under sub-window crash loops. **Correctness is unaffected**: coverage never overstates and rows+intervals still commit atomically (that invariant held under all 203 chaos kills). | Discovered by the chaos campaign (Layer C): a 60-kill Poisson run (mean 5 s) ended with a **provably byte-empty store** — every restart began from zero — root-caused to the first-commit granularity and filed as [#50](../../issues/50). |
| [#52](../../issues/52) | OPEN | **PGlite backend is not crash-durable under repeated `SIGKILL`.** The Tier-0 store (PGlite) runs single-user Postgres with `fsync` **off**; its WAL **tears after ~6–7 kill/resume cycles** (`InitWalRecovery → StartupXLOG` abort), so a PGlite-backed campaign cannot accumulate the counts needed for attributable resume-from-partial. | A finding about the **harness backend**, not the fork: it motivated the crash-durable native-Postgres Tier 1 (§4.2). Filed as [#52](../../issues/52). |
| [#53](../../issues/53) | OPEN (write-side fix **merged [#54](../../issues/54)**) | **`sync-store` `factory_addresses` has no idempotence/uniqueness story.** A second concurrent writer durably **duplicates the child set** (`factory_addresses` ×2, identical content). | Surfaced by Tier 1 **run 2**, whose *driver* defect leaked a rogue second concurrent writer (since fixed with a single-writer DB-boundary gate). App-invisible but it breaks store-identity tooling; the **write-side hardening merged in [#54](../../issues/54)** (2026-07-05). Run 2 is retained candidly as a `fail` (§4.2); its kills are excluded from the acceptance totals. |
| (methodology; no issue) | RESOLVED in-harness | **Logical store identity must exclude non-transactional surrogate serials.** A `SIGKILL` can roll back a `factory_addresses` flush while the Postgres serial **`SEQUENCE`** (non-transactional) has already advanced, so a correct resume re-flushes **identical content at shifted serial ids**. A digest over `to_jsonb(row)` verbatim binds the surrogate id and **false-FAILs** a perfectly-resumed store. | Surfaced by Tier 1 **run 20** (§4.2). Fixed by digesting `to_jsonb(row)` **minus surrogate serial ids**, ordered by the natural key (`pg-digest.mjs`); selftest extended for **id-shift invariance** with block-number mutation and run-2-style duplication as negative controls; baseline recomputed; run 20 re-verified **PASS**. A companion to the WAL-replay/physical-bytes caveat (§4.2) that motivates a *logical* rather than *physical* identity. |

This finding (#50) is what **Tier 0** (§4.1) could not see past: because the 50k Tier-0 range fits
inside one 500k chunk, its kills overwhelmingly hit an empty-or-complete durable store, so
*attributable* resume-from-partial state was not witnessed there. The **re-parameterized campaign** it
called for — small fixed chunks (`PORTAL_CHUNK_BLOCKS` 2k) that force staircase durable commits,
per-kill coverage snapshots, and an acceptance criterion requiring kills observed with
`0 < coverage < 100%` — **ran and closed that gap on the crash-durable Postgres backend**: see
**Tier 1 (§4.2, ACCEPTED 2026-07-05)** — 155 kills at partial coverage, 17 completions from partial
persisted state, logically-identical final stores. Issue #50 itself remains open and unaffected; Tier 1
works around it with small chunks rather than fixing it.

### 5.5 Validation-tool findings (Layer E harness)

These are findings about the **byte-diff tooling itself**, surfaced while running the paid matrix
(Layer E). They are separate from the data findings above: neither the fork nor the ground-truth store
is at fault — the tool was — and none of them changed a verdict. They are recorded here for the same
reason as the chaos methodology findings (§5.4): the harness is part of the evidence, and its bugs
belong in the open too.

| Issue | State | Finding | Attribution / layer |
|-------|-------|---------|---------------------|
| [#58](../../issues/58) | RESOLVED (**merged [#59](../../pull/59)**) | **Differ keyset pagination did not follow the sync-store PK.** The batched byte-diff's keyset cursor ordered/compared by columns that were **not** the `chain_id`-prefixed sync-store primary key, so on a large table the planner could not resolve each page as a single forward index scan; on the F-full full-history diff (§3.2) the tool diffed `logs` byte-identical, then **wedged on `transactions`** (days of CPU, no progress). | A tool defect (Layer E), not a data defect — both stores were intact (the offline re-diff with the fixed tool proved them byte-identical). Root-caused and fixed in **[#59](../../pull/59)**: ORDER BY + tuple-WHERE now lead with the `chain_id`-prefixed PK. Pinned by a DB-free SQL-shape test (`diff-batched.test.mjs`). |
| (harness tool; no issue) | RESOLVED in-harness | **PGlite 0.2.13 WASM-allocator detoast-volume hang.** A single `select *` page whose toasted input runs to ~300 MB (which a 50,000-row page of the widest sync-store tables reaches over full-history windows) spins forever in PGlite 0.2.13's WASM allocator (the *detoast* step, not the query). The **same rows in 5,000-row pages** complete at ~1.5 s each. Surfaced by the fixed-keyset differ's first offline re-diff of the F-full evidence stores (§3.2), which hung after `logs` proved identical. | A finding about the **harness's embedded store backend**, not the fork or the diff logic. Fixed in-harness by shrinking the differ page size `BATCH` from 50,000 to 5,000 rows (`diff-batched.mjs`); the pinned SQL-shape assertions were updated to `limit 5000`. With the smaller page, the full F-full re-diff completed in ~65 s. |

---

## 6. Current status — what a reader can rely on today

**Proven today (with reproducible evidence in this repo):**

- The Portal layer's invariants (INV-1 … INV-16) hold under property-based tests **on both supported
  upstream Ponder versions** (`0.16.6`, `0.15.17`), and every fix is backed by a mutation-verified
  regression test.
- **Crash/resume is safe, and resume-from-partial-persisted-state is now proven.** Tier 0 (PGlite,
  §4.1): 203 kills across 41/41 completed backfills, `SIGKILL`-atomic and restart-idempotent,
  byte-identical to an unkilled baseline across all five row families, intervals tiling exactly, zero
  invariant violations. Tier 1 (crash-durable native Postgres, §4.2, ACCEPTED 2026-07-05) closes the
  remaining gap: with small fixed chunks the durable store advances in a staircase, and the campaign
  recorded **155 kills at partial durable coverage** and **17 completions that resumed from partial
  persisted state** to a **logically-identical** final store (surrogate-id-excluding digest + exact
  interval tiling). So *attributable* resume-from-partial is evidenced today — the property Tier 0
  alone could not witness (the [#50](../../issues/50) granularity shape, which remains open and is
  worked around by Tier 1's small chunks, not fixed).
- The fork-vs-stock **byte-diff plumbing** is proven end-to-end by the SMOKE cell (byte-identical
  across all five row families on public endpoints, §3.1), and the **flagship `F-full` cell is DONE**
  (§3.2): the full recorded Euler v2 history on eth mainnet (`[20529207, 25436954]`, 4.9M blocks) is
  **byte-identical** to a stock `ponder@0.16.6` JSON-RPC backfill across `logs` / `transactions` /
  `transaction_receipts` / `traces` and every event-bearing `block` (885,893 / 276,674 / 276,674 / 0 /
  252,396 rows). The chain of custody (a killed first attempt, a differ that wedged pre-[#59](../../pull/59),
  and an offline re-diff of the archived stores with the fixed tool) is stated in full in §3.2, as is
  the **app-hash caveat** — the app-table determinism hash was vacuous (no user rows written), so the
  byte-identity is proven at the sync-store row level only.
- The A/B dual-implementation soak is **actively cross-validating** the Portal path against the RPC
  path hourly, and every divergence it has found is a **public, tracked issue** (§5) — with the Portal
  leg matching third-party evidence in each confirmed case.

**Pending / not yet claimed:**

- The **full paid byte-diff matrix** (§3) — SMOKE and the flagship `F-full` are DONE (§3.1, §3.2); the
  remaining cells are PENDING. **This document will update each row as its cell completes.**
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
| Unit + invariant suite (both versions) | `scripts/sync-upstream.sh <ver> --test` | `CLAUDE.md`, `portal/INVARIANTS.md` |
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
