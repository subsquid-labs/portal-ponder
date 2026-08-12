# Seams hardening — standing plan

**Status: DORMANT — awaiting explicit human GO. Do not execute any phase, and do not merge this PR,
without a written approval from a human maintainer on this PR (see [Activation](#activation--the-human-gate)).**

This is a standing verdict + roadmap, produced by a deep architecture review (2026-07-07, three
independent model reviews + an empirical state-ownership map + a test refactor-safety analysis over
main @ `a486c86`). It answers one question — *does the `portal/` layer need a deep refactor before
growing an open-source community?* — and plans the work that IS worth doing.

---

## Verdict

**No deep refactor. No rewrite. A targeted seams-hardening in three phases, after the next release
ships.** All review tracks converged independently on the same diagnosis:

- **The architecture is sound.** 7 of 15 modules are genuinely pure; the import graph is a clean
  DAG; state lives in factory closures with no domain-data globals; the gate is a textbook
  pure-reducer behind a shell. The functional-core / imperative-shell claim is real.
- **The pain is real but located.** What reads as "scattered state" is five specific coherence
  seams held together by protocol discipline instead of types (see [The five seams](#the-five-seams)).
  What reads as "ad-hoc invariants" is a catalog that mixes ~6–8 true design invariants with a much
  larger body of regression case law. What reads as "too complex for a human" is largely
  archaeology-in-code: long incident-history comments sitting where a newcomer needs a short map.
- **Release comes first.** The restructuring is orthogonal to the pending fixes; shipping is not.
  Nothing in this plan rides along with a release.
- **The safety net makes this affordable.** The outermost verification layers — the byte-identity
  differential vs RPC, the 15-chain frozen-store parity, the chaos kill-loop, the soak A/B differ —
  are behavioral by construction and survive any internal restructure. The largest unit suite
  (`portal.test.ts`) drives only the public seam against a mock Portal: ~90–95% of it survives
  module merges/splits and state-ownership moves unchanged. The known casualties are small and
  must be **ported, not dropped** (see per-phase gates).

**What this plan is not:** a license to rewrite. The pure modules (`portal-assemble`,
`portal-chunks`, `portal-config`, `portal-cutover-guard`, `portal-errors`, `portal-filters`,
`portal-transform`), the public `createPortalHistoricalSync` seam, the invariant-ID system, and the
mutation-test discipline are explicitly out of scope. They are what makes this library trustworthy.

## The five seams

The empirical state-ownership map found exactly five places where one fact is tracked in multiple
structures that must be kept coherent by discipline. These — not the module structure — are the
refactor targets. (Line references are against `a486c86` and will drift; re-locate before work.)

| # | Seam | Where | Why it bites |
|---|------|-------|--------------|
| 1 | `childAddresses` has **three writers** across two subsystems | `portal-discovery.ts` (`scanWindow`), `portal-realtime-wire.ts` (`applyDiscovered` + reorg prune), read by filters/assemble/`portal.ts` | The single most cross-cutting mutable structure; Ponder-owned, mutated live by fork code |
| 2 | Child-discovery fact **triple-shadowed** | live `childAddresses` ⟷ discovery `pendingChildren` ⟷ shell `pendingFlushes` | Coherence rests on the take/restore protocol surviving Ponder's transaction retries |
| 3 | `coveredTo` shadows the client's stream cursor | `portal.ts` cache entries vs `portal-client.ts` `stream()` | The optimistic bump before an extend await is the historically bug-generating shape |
| 4 | `logs` array + `logsRevision` counter | `portal-realtime-wire.ts`, read in `portal-realtime.ts` | Mutate one without the other → silently stale server-side filter |
| 5 | Finalized head represented **four ways** | `portal.ts` `portalHead`, realtime `anchor`/floor, wire `lastFinalized`, `diag` mirror | Same finality fact, independent representations across two paths |

## Roadmap

### Phase R1 — the reading layer *(docs/comments only; zero regression risk)*

Goal: a newcomer forms a correct mental model in one sitting.

1. **Split `portal/INVARIANTS.md` into constitution + case law.** The constitution: the true
   design invariants (roughly INV-2, INV-3, INV-6, INV-9, INV-10, INV-12, plus the resource
   policies INV-7/8/13 stated as one paragraph each). The case law — fix genealogy, wave numbers,
   counterexample narratives, mutation evidence — moves verbatim to a new
   `portal/HARDENING-LOG.md`, cross-linked per invariant. Nothing is deleted; it is re-shelved.
2. **De-archaeologize code comments.** Long incident-history comments in `portal.ts` (e.g. the
   `pendingFlushes` and `discFloorBlock` block comments) shrink to a 3–5 line *what/why now* plus a
   pointer into the hardening log. The history survives; it stops being the first read.
3. **`CONTRIBUTING.md` built around the invariant-ID on-ramp** — `grep INV-N` takes you from
   statement to enforcement to runtime check to proving test. Every review track independently
   called this the repo's standout property; it becomes the contributor front door.

Gate: docs-only diff; committee review for accuracy against code; no code changes permitted.

### Phase R2 — the five seams *(the real work; five PRs, strictly one seam per PR)*

Ordered by comprehension-gain / regression-risk:

1. **`portal-flush.ts`** — extract `persistPendingChildren` + `storeFactoryKey` + the
   `pendingFlushes` journal from `portal.ts` into one module owning the discovery/flush protocol
   behind a typed interface. Closes seams #1-write-path and #2.
2. **Named shell state** — `dataCache`/`stash`/`delegated`/head scalars become a `ShellState`
   object with intention-named methods (evict, delegate, markCovered, …); `portal.ts` becomes thin
   wiring. The gate-accounting tests in `portal-shell.test.ts` (G1/G3/S1, ~200 lines) are the sole
   backpressure coverage and **must be ported, not deleted**.
3. **Typed `RangeCoverage` in `portal-client`** — `stream()` returns what it actually covered
   instead of callers recording coverage optimistically. This is the architectural home of the
   #47-class completeness guard and closes seam #3. The tactical #47 fix (shipped separately,
   before this plan activates) is folded in, not duplicated.
4. **Realtime split** — extract the 409 fork negotiation as a pure reducer
   (`negotiate409(state, response) → {cursor, fatal?}`) and wrap `logs`+`logsRevision` in a small
   handle that bumps atomically. Closes seam #4, shrinks seam #5.
5. **`ChildRegistry` facade** — a single-writer wrapper around Ponder's `childAddresses`;
   discovery owns it, the realtime wire and the delegation path go through it. Closes seam #1.

Per-PR gate (every PR, no exceptions): behavior-frozen (no semantics change); independent
review by the standing committee; mutation-verified tests for anything moved; the full
version-matrix suite green (`scripts/sync-upstream.sh <ver> --test` for every `versions.json` row);
a fresh byte-identity `F-full` differential run. After PR 2 and PR 5 additionally: a chaos
kill-loop re-acceptance. A PR that cannot show its casualties were ported does not merge.

### Phase R3 — winning hearts *(parallel with R2, cheap)*

README contributor path; "add a chain" / "add a filter" guides; an architecture diagram of the
module DAG and the two data paths; a curated `good first issue` set carved from the open backlog.

## Activation — the human gate

- **This plan is high-stakes and dormant by default.** Executing any phase — including the
  docs-only R1 — requires an **explicit written GO from a human maintainer as a comment on this
  PR** naming the phase (e.g. "GO R1", "GO R2.1–R2.3"). Silence is not approval. No agent,
  including the CTO-role agent, may self-authorize activation, and this PR itself is merged only
  by, or on the recorded instruction of, a human maintainer.
- **Who picks it up, and when.** After the pending release wave has shipped and its post-release
  soak is stable, the CTO-role agent should re-read this plan, refresh the line references and the
  seam map against current main, and post a short readiness assessment on this PR — *that is the
  signal for the human to consider a GO, not an approval by itself.* On a GO, the orchestrator
  executes the named phase per the gates above, one PR at a time, and reports each merge on this PR.
- **Autonomous work allowed on this branch without a GO:** maintaining the plan itself — refreshing
  drifted references, appending findings that affect the verdict, recording status below. Nothing
  outside this document.
- **Standing invalidation rule:** if a new correctness wave opens (a critical/high data-correctness
  issue on the portal path), R2 freezes until it closes; R1/R3 may continue if already approved.

## Status log

| Date | Event |
|------|-------|
| 2026-07-07 | Plan authored from the architecture review at `a486c86`. Dormant; awaiting human GO. |
