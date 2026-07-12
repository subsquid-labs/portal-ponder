# Stream-Realtime Production Campaign — `PORTAL_REALTIME=stream` from EXPERIMENTAL to PRODUCTION-READY

Status: CTO blueprint, ratified 2026-07-11. This document is the campaign's single source of truth:
the gap register, the evidence gates (RG0–RG6), the sequencing, and the acceptance criteria that gate
the README label flip. It follows the same evidence discipline that certified the Portal backfill
(VALIDATION.md: byte-diff matrix, chaos Layer C, A/B soak Layer D) — adapted to the realtime leg.

---

## 1. Executive framing

**What "production-ready realtime" means here.** A billion-TVL protocol can run
`PORTAL_REALTIME=stream` unattended under a process supervisor and rely on three properties:

1. **Correctness** — every row the stream path writes in the realtime region is byte-identical to
   what the RPC realtime path writes for the same finalized history; reorgs and finality are
   reconciled to the same converged state.
2. **Fail-loud** — there is *no* input, fault, or timing under which the path silently drops data,
   silently stalls, or silently switches semantics. Every unrecoverable state is a bounded, loud,
   diagnosable fatal whose restart provably resumes byte-identical.
3. **Recoverability** — `SIGKILL` at any instant (mid-append, mid-reorg, mid-redelivery, mid-defer,
   at the historical→realtime cutover) resumes to a store byte-identical to an uninterrupted run.

The current state honestly summarized: the reconcile/finalize *logic* is strong — pure, anchored,
mutation-tested (~30 unit tests catalogued in VALIDATION §5.8(B)); the finality-boundary machinery
(`clampFinalizedToPortalHead`, `deriveFinalityFloor`, persisted floor) is well-covered at unit level;
a live single-chain A/B soak (VALIDATION §5.8(C)) is accruing empirical finalized-overlap parity. What
is missing is (a) a small set of **liveness holes** (silent-stall paths that contradict fail-loud),
(b) one **suspected correctness bug** at the isolated-path cutover, and (c) essentially all
**system-level evidence**: realtime chaos, multichain soak, fail-loud injection coverage.

**Gate model.** Six gates, mirroring the backfill's G-series discipline. The label flip sits at RG6
and nowhere else; it is gated on the complete dossier, not on judgment.

| Gate | Name | One-line exit criterion |
|------|------|------------------------|
| RG0 | Ground truth ratified | This plan merged; RT-G13 (cutover floor) verified true-bug-or-not with a written verdict. **MET** — plan merged ([#157](../../pull/157)); RT-G13 verified **INERT / NOT-A-BUG** (upstream guards isolated adoption — verdict in the RT-G13 register entry below), so RT-2 reclassifies to should-fix (defense / doc-parity). |
| RG1 | Must-fix code landed | RT-1/RT-2/RT-3 merged: mutation-verified tests, committee review, both-version gates green. RT-1 ([#161](../../pull/161)) + RT-3 ([#162](../../pull/162)) merged; **RT-2 landed** ([#163](../../pull/163)) — isolated-cutover finality `floor` shipped for parity across all 5 wiring patches (INV-25), test pins the load-bearing break-before-adopt guard INDEPENDENTLY of the inert floor (M1/M2 mutation-verified), both-version `--test` gates green (361 passed each). |
| RG2 | Fail-loud audit complete | Fatal-injection suite + silent-gap fuzzer green in CI; no enumerated silent path survives |
| RG3 | Realtime chaos passed | ≥200 kills across ≥6 timing classes, 100 % clean resumes, byte-identical digests. **MET (Phase A, [#158](../../pull/158))** — 238 kills / 7 timing-class sub-runs, 238/238 clean, byte-identical, 0 dup FINALIZED; K6-cutover + K2-spread non-vacuity self-certified. Numbers + candid mock-fidelity caveats in VALIDATION §5.11. |
| RG4 | Multichain stream soak passed | ≥72 h multi-chain stream soak (incl. one fast chain + omnichain), crash drills clean, parity clean |
| RG5 | A/B soak evidence complete | 7-day single-chain A/B soak done + ≥72 h re-soak on the patched build, zero unexplained diffs |
| RG6 | Dossier & label flip | VALIDATION §5.8 rewritten, INVARIANTS rows landed, README label flipped in the dossier PR |

RG1–RG3 are mock-driven and start immediately. RG4/RG5 use live Portal + the existing A/B soak
protocol. The in-flight 7-day A/B soak window (completing 2026-07-13) is a fixed cornerstone: the
running soak is **not disturbed** before it completes.

---

## 2. Correctness gap register

IDs `RT-G1…RT-G15`. RT-G1–G10 validate/correct the briefing packet's draft register against the code;
RT-G11–G15 are gaps the packet missed or underweighted. Severity legend: **must-fix** (code change
gates RG1), **should-fix** (lands during the campaign, doesn't gate the label by itself),
**evidence** (code is right; the gap is proof), **already-covered** (cite the proof; no work).

### RT-G1 — Deep reorg beyond the unfinalized window → `gap` fatal
- **Code**: `reconcile` returns `gap`; `portalRealtimeEvents` throws with `windowDump` + `diagDump`
  (portal-realtime.ts, the `r.kind === 'gap'` branch). Deliberate fail-loud limit, documented in
  VALIDATION §5.8 and README.
- **Evidence today**: unit — `portalRealtimeEvents: an unknown-parent gap is FATAL, not silently
  skipped (finding 7)`; diagnostic-message test (`issue #33 F4`).
- **Verdict**: code **already-covered**; the gap is **evidence** — no end-to-end proof that the fatal
  + restart resumes byte-identical. → RG3 kill class K6 (restart-after-gap-fatal) covers it.

### RT-G2 — `RING_CAP` eviction during a deep 409 step-down (the F5 availability edge)
- **Code**: `pruneRing` caps the delivered-hash ring at `RING_CAP = 2048`; if a step-down reaches an
  evicted height, the next armed request finds `ring.get(cursor − 1) === undefined` and throws the
  loud "no delivered-hash ring entry" fatal. Never wrong data — but the edge is *reachable*: the B1
  defer bound (default 600 000 ms) permits the window to grow past 2048 on a sub-300 ms chain
  (Arbitrum-class: ~2400 blocks in 10 min).
- **Evidence today**: none for the eviction path specifically.
- **Verdict**: **should-fix (RT-3)** — raise `RING_CAP` 2048 → 8192 so the edge is unreachable within
  the B1 bound for any chain ≥ ~75 ms block time (a `Map<number,string>` of 8192 entries is
  negligible), **plus** a mutation-verified test that forces eviction and asserts the loud fatal
  (mutation: restore 2048 with a >2048 window → the test must fail on the silent/absent-fatal shape).
  Ruling on the packet's Q1: **no dynamic sizing** — complexity without a correctness payoff; the
  invariant that matters is "loud restart, never wrong data", and that is preserved.

### RT-G3 — 409 oscillation
- **Verdict**: **already-covered**. The packet's "no test" claim is wrong:
  `streamHotBlocks: an OSCILLATING 409 loop … fatals at the 10 no-progress cap (issue #33 T3 cap)`
  and the fatal-message diag test (`issue #33 F4`), plus the deep-step-down-to-floor test (`T3 deep
  step-down / F2`). No work.

### RT-G4 — Wrong-fork finalize (canonical hash mismatch)
- **Verdict**: **already-covered**. `portalRealtimeEvents: a finalize whose canonical hash mismatches
  the local block is FATAL (wrong-fork finalize)`; the defer-above-tip companion
  (`review B1` test). No work.

### RT-G5 — Finalize-defer streak (B1 watchdog)
- **Verdict**: **already-covered at unit level**. Two tests: the moving-head bounded-deferral fatal
  (`delta review B1`) and the streak-clears-on-catch-up non-accumulation test. Empirical exercise of
  the watchdog under real brownouts folds into RG4. No code work.

### RT-G6 — Redelivery handshake deadlock
- **Verdict**: **already-covered at unit level** — watchdog fatal test
  (`a redelivery that never lands is bounded by a watchdog and fails loud`), held-finalize ordering
  test (`review B2`), env-knob precedence test (`resolveRedeliveryTimeoutMs`). **Residual**: the
  watchdog's unwind depends on the aborted body read settling — exactly issue #48 (RT-G8). Folded
  into RT-1.

### RT-G7 — Mid-stream replica skew
- **Analysis** (code walk): every armed request carries `parentBlockHash`; a skewed replica's
  divergent 200 reconciles as `reorg` (correct rollback), `duplicate` (idempotent skip), or `gap`
  (loud fatal); a skewed 409 negotiation terminates at the cap or the floor fatal; a *regressed*
  `finalizedHead()` probe is inert (`takeFinalized` below the anchor finds no tip; the wire's
  `emitFinalize` is monotonic-guarded). No silent-corruption path found.
- **Verdict**: **evidence** — the risk is availability churn (spurious reorg/fatal rate), measurable
  only against the real load-balanced Portal. → RG4 measures fatal/churn rates; no code work unless
  the soak falsifies the analysis.

### RT-G8 — undici abort-hang (issue #48, OPEN)
- **State**: documented residual — the two terminal `abort()` sites in the wire can, under
  nodejs/undici#4089, leave the pending body `read()` unsettled, degrading the watchdog's loud fatal
  into a silent stall. Empirically safe on the pinned runtime (13-cell matrix + 1220 race iterations,
  zero hangs), so hardening, not a live defect.
- **Verdict**: **must-fix as part of RT-1** (see §3): the label claims unattended production; a
  *documented* path from watchdog-fatal to silent-stall contradicts fail-loud. The fix shape is
  already spec'd in #48 (cancel-first teardown / race the read against our own timer) with a
  regression-test shape (mocked never-settling read → fatal still surfaces on deadline).

### RT-G9 — Foreign (omnichain) checkpoint floor mapping
- **Verdict**: code **already-covered** — six unit tests on `deriveFinalityFloor` /
  `checkpointBlockNumber` including the end-to-end clamp composition (`issue #57`). The direction-of-
  error argument for strict `<` (floor lands at/below true local finality) is sound as written in the
  wire docstring. Ruling on the packet's Q4: **no cross-chain barrier** — it would serialize all
  chains on the slowest, re-creating the head-of-line blocking stream mode exists to remove. Residual
  is **evidence**: RG4 runs omnichain crash-recovery drills and asserts no double-index/no gap.

### RT-G10 — Silent tip stall on endless 204s
- **Code**: `streamHotBlocks` re-polls 204s forever; a permanently lagging dataset stalls the indexer
  silently. Compounded by RT-G12 (finalize polls never run without block delivery — so even the
  existing B1 watchdog cannot fire during the stall).
- **Verdict**: **must-fix (RT-1)**. Ruling on the packet's Q3: add a **progress watchdog conditioned
  on evidence of progress elsewhere** — fatal only when the Portal head (`finalizedHead()` /
  head probe) has advanced ≥ N blocks while `/stream` delivered nothing for the bound; a genuinely
  quiet or halted chain stays idle (parity with RPC realtime, which also idles). Never trust an SLA
  where an invariant can stand.

### RT-G11 (new) — No idle bound on the realtime `/stream` body read
- **Code**: `ndjsonLines` supports an `idleMs` stall guard (used by the historical client) but the
  realtime call passes none — `portal-client.ts` says it directly: *"Absent ⇒ no stall guard
  (realtime path)"*. A wedged connection (headers OK, body never delivers, no FIN/RST) hangs the
  `for await` at the read **forever**: no blocks, no finalize polls, no watchdogs — the exact silent
  freeze class the backfill client was hardened against (PR #16 lineage).
- **Verdict**: **must-fix (RT-1)** — pass an idle bound on the realtime read; on idle expiry,
  **reconnect** (routine, cheap, the loop already resumes from `cursor`), don't fatal. The bound must
  comfortably exceed normal inter-block quiet (default ~120 s, configurable).

### RT-G12 (new) — Finalize polling is gated on block delivery
- **Code**: the finalize poll (`finalizedHead()` + `takeFinalized` + B1 machinery) lives inside
  `portalRealtimeEvents`' `for await` body — it runs only *after a block is delivered*. During any
  delivery outage (204 streak, wedged read, fetch-error retry loop) finality processing and its
  watchdogs are suspended entirely. This is the enabling defect behind RT-G10's silence.
- **Verdict**: **must-fix (RT-1)** — decouple the poll cadence from delivery. Preferred design (keeps
  the sequential generator model): `streamHotBlocks` yields lightweight heartbeat ticks when no data
  is flowing (each 204/idle cycle); `portalRealtimeEvents` runs its finalize-poll branch on ticks
  without touching reconcile. Invariant to encode (INV-candidate, §5): *finalize polling and its
  watchdogs run at their cadence irrespective of block delivery*.

### RT-G13 (new) — Isolated-path cutover clamp passes no finality floor  ⚠ suspected correctness bug
- **Code**: the multichain cutover refetch clamps with `floor:` = the previously adopted boundary
  (wiring patch, `getHistoricalEventsMultichain` hunk), exactly as the `clampFinalizedToPortalHead`
  docstring requires ("callers pass … the previously adopted boundary (cutover)"). The **isolated**
  cutover refetch (`getHistoricalEventsIsolated` hunk) calls the clamp **without `floor`**. If a
  lagging Portal replica answers the cutover probe stale-LOW, the boundary can regress below the
  previously adopted one and realtime re-streams `(head, prevBoundary]` — double-indexing finalized,
  unrevertable rows — on **single-chain apps**, the most common deployment shape.
- **Uncertainty, flagged honestly**: whether upstream's isolated path adopts a *lower* refetched
  finalized block (vs only ever raising it) is unverified — the hunk context ends before the
  adoption logic. **Verify empirically first** (RG0): read the grafted upstream
  `getHistoricalEventsIsolated` post-refetch adoption; write the failing test if adoption is
  unconditional.
- **Verdict**: **must-verify → must-fix if confirmed (RT-2)**: pass
  `floor: hexToNumber(params.syncProgress.finalized.number)` mirroring the multichain site. Even if
  upstream is proven to guard, land the floor anyway as a cheap docstring-parity/defense fix — but
  then classified should-fix and the test asserts the floor is inert.
- **RG0 verdict — VERIFIED, NOT-A-BUG (isolated adoption is guarded, not unconditional).** Grafted
  upstream `packages/core/src/runtime/historical.ts` (`getHistoricalEventsIsolated`; raw-upstream
  0.16.9 lines 827–835, grafted lines shift down where the Portal clamp is inserted above the guard)
  `break`s *before* the boundary assignment whenever
  `hexToNumber(finalizedBlock.number) − hexToNumber(params.syncProgress.finalized.number) <= params.chain.finalityBlockCount`,
  and only otherwise runs `params.syncProgress.finalized = finalizedBlock`. A stale-LOW clamp yields a
  **negative** delta, which is always `<= finalityBlockCount` (≥ 0), so the `break` fires and the
  below-floor boundary is **never adopted**. The multichain site's `floor` is load-bearing only because
  *its* post-catchup assignment is unconditional (`syncProgress.finalized = finalizedBlocks[i]` for
  every chain once catchup fires); the isolated site has no such unconditional path. Confirmed by two
  blind reviewers (independent reasoning + a grafted-source read across all pinned versions —
  0.15.17 / 0.16.6–0.16.9, same guard shape) and re-verified first-hand against raw upstream 0.16.9.
  **Reclassified must-verify→must-fix ⇒ should-fix (RT-2, defense / doc-parity):** still land the
  `floor` mirroring the multichain site, but the mutation-verified test **pins the upstream guard**
  (weaken the `<= finalityBlockCount` comparison ⇒ a stale-low block gets adopted ⇒ the test fails),
  **not** the floor — removing the floor alone cannot fail the test, since the guard already prevents
  the regression.

### RT-G14 (new) — Restart/resume of the realtime region: zero empirical evidence
- **Analysis**: the design argument is sound — crash recovery reverts unfinalized app rows to the
  finalized checkpoint; the startup clamp + persisted floor (`deriveFinalityFloor`) re-derives the
  boundary; the stream re-serves `(finalized, tip]`. But *no test or run has ever killed and resumed
  the realtime region* — the chaos Layer C evidence is historical-only, and VALIDATION §5.8 says so.
- **Verdict**: **evidence** — this is RG3's core purpose. Kill classes K1–K6 (§4c).

### RT-G15 (new) — Historical→realtime cutover boundary, end-to-end
- **Analysis**: unit-level coverage of the clamp sites is good (never-raise, floor-up, pin-override
  tests); INV-18 governs cutover skip. Missing: end-to-end proof that under a *moving* head at
  cutover the seam produces no gap and no overlap, and that a kill exactly at cutover resumes clean.
- **Verdict**: **evidence** — RG3 kill class K5 (kill at cutover) + an RG2 seam test that walks a
  scripted mock through cutover with the head advancing mid-refetch and asserts contiguous,
  non-overlapping coverage.

### Register summary

| ID | Severity | Disposition |
|----|----------|-------------|
| RT-G1 | evidence | RG3 K6 |
| RT-G2 | should-fix | RT-3 (RING_CAP 8192 + eviction fatal test) |
| RT-G3 | already-covered | cite tests; no work |
| RT-G4 | already-covered | cite tests; no work |
| RT-G5 | already-covered (unit) | RG4 exercises live |
| RT-G6 | already-covered (unit) | residual → RT-1 (#48) |
| RT-G7 | evidence | RG4 measures |
| RT-G8 | must-fix | RT-1 (cancel-first teardown, #48) |
| RT-G9 | already-covered (unit) | RG4 omnichain drills |
| RT-G10 | must-fix | RT-1 (progress watchdog) |
| RT-G11 | must-fix | RT-1 (read idle bound → reconnect) |
| RT-G12 | must-fix | RT-1 (poll decoupled from delivery) |
| RT-G13 | verified INERT (RG0) → should-fix → **LANDED (RT-2, [#163](../../pull/163))** | RT-2 (floor shipped for parity across all 5 wiring patches → INV-25; Pin A pins the floor, Pin B pins the upstream break-before-adopt guard, mutation-verified independent — M1 fails Pin A only, M2 fails Pin B only) |
| RT-G14 | evidence | RG3 |
| RT-G15 | evidence | RG3 K5 + RG2 seam test |

---

## 3. The must-fix work items (handoff-ready specs)

### RT-1 — Stream liveness & teardown hardening (one PR; the campaign's only substantial code change)

**Goal**: no delivery-outage state can silence the stream path; every terminal path is loud even
under the documented undici race. Closes RT-G8, RT-G10, RT-G11, RT-G12.

**Scope** (files): `portal/portal-realtime.ts` (shell + event loop), `portal/portal-realtime-wire.ts`
(teardown sites), `portal/portal-client.ts` only if the idle plumbing needs a parameter pass-through.
**No changes to `reconcile`/`takeFinalized` or any reorg/finalize semantics.**

Four sub-changes:
1. **Idle-bounded realtime read** (RT-G11): pass `idleMs` (default 120 000, env-tunable
   `PORTAL_STREAM_IDLE_MS`, positive-integer validated exactly like `resolveRedeliveryTimeoutMs`) to
   the realtime `ndjsonLines` read. Idle expiry ⇒ close + reconnect from `cursor` (NOT fatal), with a
   debug log. Invariant: a slow-but-alive stream is never cut (the guard re-arms per chunk — already
   `ndjsonLines`' semantics).
2. **Delivery-decoupled finalize cadence** (RT-G12): heartbeat ticks from `streamHotBlocks` when no
   data flows (on each 204 cycle and idle-reconnect), so `portalRealtimeEvents` runs its finalize
   poll + B1 watchdog on cadence during outages. Implementation latitude allowed; the binding
   invariant: *the finalize poll and its watchdogs run at ≤ 2× `finalizePollMs` cadence regardless of
   block delivery*. Ticks must not touch reconcile or the window.
3. **Delivery-progress watchdog** (RT-G10): fatal (loud, with `diagDump`) when the probed Portal head
   has advanced ≥ a threshold while zero blocks were delivered for ≥ a bound (default 600 000 ms,
   aligned with B1; env-tunable). A quiet/halted chain (head static) never trips it. The silent
   `fetch`-error retry loop counts as non-delivery (and gains a rate-limited warn log).
4. **Cancel-first terminal teardown** (RT-G8 / #48): make the watchdog/teardown unwind independent of
   the aborted read settling — own the reader and `cancel()` (the PR #16-proven pattern) or race the
   pending read against the watchdog's own timer. Port the regression-test shape from #48: a mocked
   never-settling post-abort read must still surface the loud fatal within the deadline.

**Acceptance**: (a) mutation-verified tests per sub-change — each must fail on pre-fix code (e.g.
neuter the tick emission → the 204-stall finalize test must hang/fail; type-valid neuters only);
(b) the full existing realtime suite green, both supported ponder versions via
`scripts/sync-upstream.sh <ver> --test`; (c) biome clean; (d) committee review with a **blind
multi-track design check** on sub-change 2 (the only structural change) before implementation;
(e) INVARIANTS row (see §5) and HARDENING-LOG entries in the same PR.

### RT-2 — Isolated-path cutover finality floor (RT-G13; conditional on RG0 verification)

**Goal**: the isolated cutover clamp can never adopt a boundary below the previously adopted one.
**Scope**: the isolated-cutover hunk in every `portal/wiring/*.patch` (0.15.17–0.16.9) — add
`floor: hexToNumber(params.syncProgress.finalized.number)` to the `clampFinalizedToPortalHead` call,
mirroring the multichain site.
**Acceptance**: a mutation-verified test proving that with a stale-LOW probed head at isolated
cutover the boundary does not regress (fails with the floor removed); if RG0 verification shows
upstream already guards adoption, the test instead pins the guard and the change is documentation-
parity. Both-version gates; the patches for all pinned versions updated together.
**Status — LANDED ([#163](../../pull/163)).** RG0 confirmed the isolated path is already INERT-safe
(break-before-adopt guard), so RT-2 shipped the `floor` for cross-site parity + defense-in-depth (INV-25)
across all 5 wiring patches (0.15.17–0.16.9, added `+` block byte-identical). Per RG0 the test does NOT
assert the boundary through the real path (the floor equals the guard's compare value, so it MASKS a guard
mutation → a behavioral test would be vacuous); instead `portal-cutover-guard.test.ts`
"INV-25 / RT-2: isolated-cutover finality floor" pins the two protections INDEPENDENTLY at the grafted
source — **Pin A** the shipped floor, **Pin B** the load-bearing break-before-adopt guard. Mutation-verified:
M1 (drop the floor line) fails Pin A only; M2 (weaken the guard `<=`→`>=`) fails Pin B only — the pins are
independent. Both-version `--test` gates green (0.16.6 + 0.15.17, 361 passed each; cutover-guard 42 tests);
biome clean.

### RT-3 — RING_CAP headroom + eviction-path proof (RT-G2)

**Goal**: the F5 availability edge is unreachable within the B1 bound on any supported chain, and the
eviction fatal is proven loud. **Scope**: `RING_CAP` 2048 → 8192 in `portal/portal-realtime.ts` + one
unit test driving a >CAP window into a step-down that reaches an evicted height and asserting the
"no delivered-hash ring entry" fatal. **Acceptance**: mutation-verified (revert to 2048 under the
test's window → different, silent shape caught); doc comment (the F5 CANDOR block) updated with the
new arithmetic.

---

## 4. Evidence plan (BAR §2(a)–(e) → runnable artifacts)

### (a) The 7-day A/B soak — EXISTS, running
Single-chain, leg A = RPC realtime, leg B = Portal stream, hourly finalized-overlap differ
(VALIDATION §5.8(C)). Completes 2026-07-13. **Untouched until then.** Acceptance: 7 days, zero
unexplained diffs on the finalized overlap (pre-declared tolerated classes only), both legs' fatal
and restart counts logged and attributed.
**Carry-over rule (rigor over convenience)**: this soak certifies the *design* as-was. Because RT-1..3
land after it started, the **shipped artifact** needs its own soak: RG5 additionally requires a
**≥72 h A/B re-soak on the patched build** with the same differ and zero unexplained diffs. Evidence
gathered on pre-fix code does not certify post-fix code.

### (b) Byte-parity of the realtime region incl. reorg windows — EXISTS (differ) + BUILD (reorg targeting)
1. The hourly differ already asserts row-set + field identity on the finalized overlap — the region
   that both legs' reorg/finalize handling must converge to. KEEP.
2. **BUILD — reorg-window targeting**: a small analysis script that mines both legs' logs for
   observed reorg events (and the soak chain's known reorg heights), then runs the differ
   specifically over `[reorg_height − margin, reorg_height + margin]` windows and reports per-window
   verdicts. Acceptance: every organic reorg window observed during RG4/RG5 diffs clean.
3. Injected 1-block and depth-k reorg parity at the *store* level is covered deterministically by the
   RG3 mock harness digest checks (below) — no live injection needed.

### (c) Realtime chaos kill/resume — BUILD (the campaign's main infra build)
**Artifact**: `harness/chaos/` realtime scenario driver: a deterministic **mock Portal**
(scripted `/stream` NDJSON with forks, 409 negotiations incl. `previousBlocks` samples, 204 phases,
redelivery re-serves, `/finalized-head` script) + the indexer under test on a local Postgres +
a kill controller with **phase-aware timing** (the mock exposes "phase reached" markers so kills land
deterministically in the intended state, not by sleep-guessing).
**Kill classes**: K1 mid-append (normal 200 flow) · K2 mid-reorg, split before/after the `reorg`
event is emitted downstream · K3 during the redelivery await (child discovered, block N suppressed,
incl. a `heldFinalize` pending) · K4 during a finalize-defer streak · K5 at the historical→realtime
cutover (during the refetch/clamp) · K6 restart-after-fatal (the `gap` fatal and the wrong-fork
finalize fatal: assert the process exits non-zero, then resumes clean).
**Acceptance (numbers)**: ≥ 200 kills total; ≥ 25 per reachable class; **100 % clean resumes** (a
single unclean resume = FAIL, freeze artifacts, file issue); every cycle's final sync-store +
app-table digest **byte-identical** to the unkilled baseline (`pg-digest.mjs` pattern reused); zero
double-indexed finalized rows (digest catches). Mock-fidelity caveat recorded candidly in the dossier:
RG3 proves crash-timing safety against *scripted* Portal semantics; live-protocol semantics are RG4/
RG5's job.

### (d) Multichain stream soak — BUILD (deploy config; infra pattern exists)
After the 7-day soak completes: a **3–5 chain** stream-mode soak on the patched build, ≥ 72 h,
including **one fast chain** (Arbitrum-class — exercises RING_CAP/B1 interplay live, RT-G2/RT-G5) and
**one omnichain-ordered multi-chain app** (exercises `deriveFinalityFloor` foreign-checkpoint
mapping, RT-G9). During the soak: **≥ 3 crash-recovery drills** (`kill -9` mid-run at arbitrary
times, one deliberately during an observed defer/reorg window if one occurs). Acceptance: zero
unexplained parity diffs (hourly differ where an A-leg exists; store self-consistency + third-party
spot checks otherwise); every drill resumes clean with no double-index and no gap (interval audit via
`check-intervals` pattern); watchdog-fatal rate measured and every fatal attributed + clean-resumed
(availability target: ≤ 2 attributable fatals/chain/day, none silent).

### (e) Fail-loud audit — BUILD (mostly test-suite extension)
1. **Fatal-injection suite**: the enumerated fatal paths each have an asserting test. Already exist:
   gap fatal, 409 oscillation cap, floor fatal, below-floor 409, wrong-fork finalize, B1 defer bound,
   redelivery watchdog, deterministic 4xx, armed-ring missing entry. NEW (from RT-1/RT-3): progress-
   watchdog fatal, idle-reconnect (non-fatal but observable), never-settling-read teardown, eviction
   fatal, isolated-cutover floor.
2. **Silent-gap fuzzer** (BUILD): property test feeding randomized NDJSON sequences (dropped block,
   duplicated block, out-of-order delivery, mutated parentHash/hash, mid-line truncation, interleaved
   204/409) into `portalRealtimeEvents` + a model checker asserting the trichotomy: every input is
   either *appended correctly* (model chain matches), *reconciled as reorg/duplicate*, or **fatals** —
   never silently skipped. Seeded/repro-able. Acceptance: ≥ 10⁵ sequences in CI-nightly mode with
   zero trichotomy violations.

---

## 5. Invariants & docs to land with the code

- **INV-22 (candidate) — Realtime liveness fail-loud**: *every no-progress state of the stream path
  (no delivery, no finalize advance) is bounded by a watchdog that either restores progress
  (reconnect) or fatals loudly with diagnostics; finalize polling runs at its cadence irrespective of
  block delivery.* Enforced: RT-1. Checked: the RT-1 test set + fuzzer.
- **INV-23 (candidate) — Finality-boundary monotonicity**: *the adopted stream-mode finality boundary
  never regresses — not across restarts (persisted floor), not at either cutover site (in-run
  floor).* Enforced: `clampFinalizedToPortalHead` + RT-2. Checked: the clamp floor tests + RT-2 test.
- Renumber if IDs are taken on main at PR time (IDs are code-enforced; check `origin/main` repo-wide
  before assigning).
- **VALIDATION §5.8 rewrite** (RG6): restructure into (A) logic evidence (unit catalog, updated), (B)
  fail-loud audit (RG2 artifacts), (C) chaos (RG3 numbers), (D) soaks (RG4/RG5 numbers + candid
  findings log incl. every fatal observed), (E) the stated limits that REMAIN by design (deep-reorg
  gap fatal → operator re-sync; capability gate refuses non-log sources — `assertStreamModeSupported`
  stays the contract). Candor is the product.
- **README**: flip the two "experimental" markers (Correctness section + Learn-more bullet) to
  production-ready **in the dossier PR only**, with the same evidence-linking style as the backfill
  paragraphs. State the supported envelope plainly: log sources (+ parent txs), no receipts/traces at
  the tip (gated, loud), supervisor-restart operational model.

## Rulings on the packet's five open questions

1. **RING_CAP**: keep the hard fatal, no dynamic sizing; raise to 8192 + prove the eviction fatal
   (RT-3). The invariant is loud-restart-never-wrong-data, and it holds.
2. **Watchdog fatals vs RPC fallback**: **fatal-restart, definitively.** A silent in-process
   transport switch destroys the per-path evidence model (a hybrid run has no baseline), can overload
   an RPC never provisioned for stream-grade traffic, and contradicts the fork's fail-loud brand. The
   supervisor-restart pattern is already chaos-proven for backfill and becomes chaos-proven for
   realtime at RG3. Document the operator contract (restart policy + backoff) in the dossier.
3. **204-streak watchdog**: yes, but progress-conditioned (fatal only when the head advances while
   delivery is zero) — never fatal a genuinely quiet chain; parity with RPC realtime idling (RT-1).
4. **Foreign checkpoint mapping**: keep timestamp mapping with strict `<`; no cross-chain barrier
   (it would serialize all chains on the slowest). Resolve residual risk empirically at RG4.
5. **Label staging**: **single binary flip at RG6.** No intermediate "beta" state — the label's gate
   transferred from human judgment to the evidence dossier, and an intermediate label re-introduces
   judgment without evidence. The README's *prose* may grow candid interim claims as gates pass
   ("chaos-verified crash/resume" after RG3), but the label itself flips once, atomically with the
   dossier PR.

---

## 6. Sequencing / roadmap

**Phase 0 — now → 2026-07-13 (soak window; running soak untouched, no paid RPC needed):**
1. RG0: merge this plan; run the RT-G13 verification (read grafted upstream isolated-cutover
   adoption; write the pinning/failing test). Cheap, highest information value — the register's only
   suspected *correctness* bug.
2. RT-1 design check (blind multi-track on sub-change 2) → implement → committee review → merge.
3. RT-3 (small) and RT-2 (once RG0 verdict is in) → merge.
4. RG2: fatal-injection additions + the silent-gap fuzzer.
5. RG3 harness build (mock Portal + kill controller) — **the critical path**; start immediately,
   develop/calibrate while RT-1 is in review; full campaign runs on the merged build.
6. Prepare the RG4 multichain config + the patched-build tarball for the post-Jul-13 soak swap.

**Phase 1 — after 2026-07-13:** harvest the 7-day soak verdict (RG5 part 1); redeploy the soak on the
patched build (72 h re-soak, RG5 part 2) and launch the RG4 multichain soak in parallel; run RG3 to
its acceptance numbers; execute the RG4 crash drills.

**Phase 2 — ~2026-07-17 onward:** assemble the dossier (VALIDATION §5.8 rewrite, INVARIANTS,
HARDENING-LOG), reorg-window differ reports, chaos + soak numbers; RG6 dossier PR with the label flip;
release.

**Explicitly deferred (do NOT work on now):** dynamic RING_CAP sizing; RPC-fallback-on-watchdog;
cross-chain finalize barriers; walk-parents auto-recovery for the gap fatal (noted in code as a
possible follow-up — out of scope, the fatal is the certified behavior); receipts/traces at the tip
(capability-gated, a separate product decision); any Portal-server-side work.

## 7. Risk register

| Risk | Mitigation |
|------|------------|
| RT-1 destabilizes a proven engine | Additive watchdogs only; zero reconcile/semantics changes; blind multi-track design review; full suite both-version gates |
| Patched build invalidates the 7-day soak evidence | Explicit carry-over rule: 7-day soak proves the design, 72 h patched re-soak proves the artifact; RT-1 kept minimal & additive to support the argument |
| Mock-Portal fidelity (RG3 proves the mock, not the Portal) | Encode documented `/stream` semantics (409 `previousBlocks` as a *sample*, repeated-409 tolerance) as conformance fixtures; keep RG4/RG5 live evidence primary for protocol semantics; state the split candidly in the dossier |
| Fast-chain behavior unrepresented until late | Arbitrum-class chain mandatory in RG4; RT-3 removes the known arithmetic edge beforehand |
| undici race resurfaces on future Node | RT-1 sub-change 4 removes the dependency on abort settling entirely; regression test pins it |
| Replica-skew churn makes stream mode fatal-happy in production (availability, not correctness) | RG4 measures fatal rates with an explicit target (≤ 2 attributable/chain/day); if exceeded, tune bounds (idle, progress thresholds) before RG6 — bounds are env-tunable by design |
| Portal-side behavior drift post-certification | Conformance fixtures + the standing A/B soak protocol remain runnable post-flip; dossier documents the re-run commands |
