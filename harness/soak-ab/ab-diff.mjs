// ab-diff.mjs — hourly finalized-overlap differ for Soak A (RPC realtime) vs Soak B (Portal-native
// realtime, PORTAL_REALTIME=stream). Over [cutover, min(finalizedA, finalizedB) - margin] per chain:
//   • logs         : strict row-set + field identity (PRIMARY — must be 0)
//   • blocks       : STRICT row-set + field identity (total_difficulty excluded) — in the finalized
//     overlap a one-sided block is a real gap, not a tolerated inert block, so it FAILS
//   • transactions (shared): full-row identity — a tx in BOTH stores must be byte-identical
//   • transactions : asserted to be EXACTLY the expected class — B may be MISSING parent txs for
//     realtime-ingested spans (the verified stream wire gap), and every such tx must be referenced
//     by an A-side log. Any B-extra tx, or an A-only tx no log references, is a FAIL — EXCEPT a B-extra
//     tx whose block is WHOLLY ABSENT from leg A (issue #36 tx facet): the same leg-A block loss the
//     logs/blocks class tolerates, one table deeper, reported + PASS-compatible (classifyOnlyBTx).
//   • per-1000-block ordered md5 checkpoint hashes both sides (persisted for drift tracking)
//   • _ponder_checkpoint monotonicity across runs
//   • persist-stagnation guard (issue #38): a one-sided freeze of the newest-persisted-row block
//     TIMESTAMP between the two legs is a HARD per-chain FAIL — it fires even when the WINDOWED diff
//     reads PASS/PENDING, closing the frozen-window blind spot that hid issue #36's real blast radius.
// Writes soak-ab-status.json {ts, chains, verdict, diffClasses, lagA, lagB, counters} for the monitor.
//
// Two "PG connections" = two `psql` processes (no npm driver → runs on the box with node + psql).
//   DATABASE_URL_A=… DATABASE_URL_B=… CHAINS=1,8453,42161 CUTOVER=<block> \
//   STATUS_FILE=soak-ab-status.json node harness/soak-ab/ab-diff.mjs
//
// AB_SCHEMA_B (REQUIRED in the real deployment): Soak B runs `ponder start --schema <app-schema>`
// (e.g. soak_b), so its `_ponder_checkpoint` table lives in that schema — NOT on psql's default
// search_path. The checkpoint monotonicity guard's query MUST qualify the table with that schema
// or it errors every run and silently falls back to the sync-store block max (dead guard). Set
// AB_SCHEMA_B to Soak B's DATABASE_SCHEMA value. Empty ⇒ unqualified `_ponder_checkpoint` (the
// legacy behavior, for a store on the default search_path). The identifier is sanitized before it
// reaches SQL.
//
// AB_STAGNATION_MAX_SKEW_S (optional): max tolerated skew, in seconds, between the two legs' newest
// persisted-row block timestamps before the persist-stagnation guard (issue #38) FAILs the chain.
// Unset ⇒ 2h (AB_STAGNATION_DEFAULT_MAX_SKEW_S). A garbage/non-positive value fails loud (like
// AB_SCHEMA_B) rather than silently disabling the guard.

import { spawn } from 'node:child_process';
import { readFileSync, renameSync, writeFileSync } from 'node:fs';
import { streamingDiff } from '../validate/diff-batched.mjs';

// ── pure, unit-tested core ─────────────────────────────────────────────────────────────────────

// KNOWN, FULLY-TOLERATED shared-tx divergence classes. Each entry is a NARROW, reportable, removable
// exception to the otherwise-strict shared-tx byte-identity check — never a general mask. Deleting an
// entry restores full strictness with no other code change (classifySharedTx returns 'mismatch' for
// anything not matched by a live entry).
//
// issue27AccessListNull — issue #27, root-caused in its authoritative comment: a non-compliant RPC
// provider omits the `accessList` key on typed realtime txs; upstream ponder tolerates the missing key
// and persists NULL (`encode.ts`: `accessList ? JSON.stringify(...) : null`) permanently. Side A
// (RPC-transport realtime) therefore stores access_list NULL for a span of realtime-ingested typed txs
// while side B (Portal /stream) stored the chain-true value. Every OTHER column of those rows is
// byte-identical (proven: md5 over `to_jsonb(t) - 'access_list'` equal on all three chains). The
// tolerance is scoped to EXACTLY that shape (see classifySharedTx) — A-side NULL, B-side non-null, all
// other columns identical, at/above the measured realtime-span floor per chain, within the (currently
// open) window. `perChainFloor` values are the measured min block_number of the class per chain;
// `toBlock` is null until the fork-side fix deploys and the realtime window closes — set it then, and
// once the whole span is finalized-and-past, DELETE this entry to restore full strictness.
export const TOLERATED_CLASSES = {
  issue27AccessListNull: {
    issue: 'https://github.com/subsquid-labs/portal-ponder/issues/27',
    perChainFloor: { 1: 25445239, 8453: 48092254, 42161: 479635494 },
    toBlock: null,
  },
};

// KNOWN-BAD single rows — a DIFFERENT, even narrower tolerance than the issue #27 class above. Where
// TOLERATED_CLASSES.issue27AccessListNull covers a whole span keyed by shape (A-side NULL vs B non-null),
// this list pins EXACT individual tx hashes for one-off, fully-evidenced divergences that are NOT the
// issue #27 shape. Each entry tolerates a single already-diverged row ONLY while the divergence stays
// access_list-only (every OTHER column still byte-identical: ex-access_list md5s equal). If ANY second
// column ever diverges on the row, classifySharedTx stops tolerating it and returns 'mismatch' → hard
// FAIL, so the pin can never protect a row that rots further.
//
// issue #32 — a single fabricated-empty access_list row surfaced by the A/B cross-validation differ:
// chain 42161 (Arbitrum One), block 469300066, tx
// 0x0af5f9831bff6430dca4197962554f7f4779da2bb4f533844b4224953e7ab5fe. Side A stored access_list='[]'
// (a fabricated empty list); side B stored the chain-true 63-entry list; ALL other columns are
// byte-identical. UNLIKE issue #27, BOTH sides are NON-NULL here ('[]' is a concrete value, not NULL) —
// so the predicate does NOT reuse issue #27's `aAccessListNull === true` clause. It DOES, however,
// enforce the EVIDENCED shape by requiring BOTH sides concrete: aAccessListNull === false AND
// bAccessListNull === false. That both-non-null guard (see classifySharedTx) is what stops the pin from
// tolerating an A-NULL / B-non-null drift (or a B-side rot to NULL) on this exact hash — the pin
// protects ONLY the '[]'-vs-concrete-list shape it was measured for, never any access_list divergence.
// The A-side row is left in place as evidence (see the issue). DELETE this entry when the row is
// repaired or its mechanism is explained.
export const knownBadRows = [
  {
    hash: '0x0af5f9831bff6430dca4197962554f7f4779da2bb4f533844b4224953e7ab5fe',
    chain: 42161,
    issue: 'https://github.com/subsquid-labs/portal-ponder/issues/32',
  },
];

// KNOWN, FULLY-TOLERATED onlyB row-loss class for the LOGS and BLOCKS tables — issue #36. This is a
// SEPARATE config object from TOLERATED_CLASSES above on purpose: that one tolerates a SHARED-tx
// access_list divergence (issue #27); this one tolerates onlyB ROWS (rows present in leg B, MISSING in
// leg A) in the logs/blocks tables. The two are semantically distinct — issue #27's floor is the
// realtime-span floor of the access_list-null shape; issue #36's floor is the realtime-era start below
// which leg A's store came from the complete-by-construction historical backfill path. They must NOT
// share a config even though chain 1's numeric floor coincides today: entangling them would let a
// change to one silently move the other's tolerance boundary.
//
// issue36OnlyBRowLoss — issue #36: leg A (RPC realtime) silently LOST on-chain log rows and block rows
// that leg B (Portal stream) holds, at scattered recent blocks on chain 1, INSIDE the finalized
// overlap. Third-party confirmed: leg B is chain-true (its rows match on-chain receipts byte-for-byte);
// leg A is the lossy side. The loss is a genuine A-side gap, so it surfaces as onlyB rows the strict
// differ (correctly) FAILs on — and the count GROWS while leg A keeps missing rows. That is by design:
// the class is toBlock-open (null) so it never silently caps. Tolerated ONLY at/above the per-chain
// realtime-era floor: BELOW the floor leg A's store came from the historical backfill path (complete by
// construction from the Portal), so any A-missing row below the floor is a HARD FAIL, never this class.
// A chain with onlyB rows but NO configured floor is a HARD FAIL too (an unknown chain is never
// default-tolerated — the #30 missing-floor semantic). Ships with chain 1 ONLY (the only chain observed
// lossy on this class; 8453/42161 are currently clean). `perChainFloor` is the measured realtime-era
// start per chain; `toBlock` stays null until leg A is repaired or the leg is retired. REMOVE this
// entry when issue #36 is resolved (A repaired or the RPC-realtime leg retired) to restore full
// strictness with no other code change (classifyOnlyBRow returns 'mismatch' for everything then).
//
// CANDOR — the limit of what cross-validation alone can prove here: within the tolerated span the A/B
// differ CANNOT, by itself, distinguish leg-A row loss (leg A dropped an on-chain row leg B holds — the
// diagnosed cause) from a hypothetical leg-B FABRICATION of a row of the same (chain, block>=floor)
// shape (leg B inventing a row that was never on chain). Both surface identically as an onlyB row at/
// above the floor. What breaks the tie is a THIRD-PARTY spot audit — comparing leg B's tolerated rows
// against an independent node/receipt (as done when issue #36 was filed: leg B's rows matched on-chain
// receipts byte-for-byte, so leg A is the lossy side). To keep that audit possible WITHOUT psql access
// to the raw diff, the status JSON carries a bounded spot-audit sample of tolerated block numbers per
// table (toleratedIssue36Sample — see sampleToleratedOnlyB): a human or script can re-run the audit on
// a handful of rows against a third-party node at any time. This tolerance is only ever as sound as
// that external control — it is a REPORTED, spot-auditable exception, never a proof of correctness.
export const TOLERATED_ONLYB_CLASSES = {
  issue36OnlyBRowLoss: {
    issue: 'https://github.com/subsquid-labs/portal-ponder/issues/36',
    perChainFloor: { 1: 25445239 },
    toBlock: null,
  },
};

// Classify ONE onlyB row (a logs or blocks row present in leg B but MISSING in leg A) whose only
// identity is its block number and chain. Returns:
//   • 'tolerated' — the exact issue #36 shape: at/above the per-chain realtime-era floor, within the
//     (open) window. PASS-compatible, counted SEPARATELY, loudly reported.
//   • 'mismatch'  — ANYTHING else → a hard onlyB row → FAIL, exactly as before the class existed.
// Pure + exported so every adversarial case is unit-tested and mutation-verified in isolation. The
// predicate is a conjunction, each clause refusing to mask a distinct thing:
//   1. floor entry EXISTS for this chain — a missing floor (unknown chain) is an explicit HARD FAIL,
//      never a default-tolerate (the #30 missing-floor semantic).
//   2. blockNumber >= floor — at/above the realtime-era floor. BELOW it leg A's rows came from the
//      complete-by-construction historical backfill, so an A-missing row there is a real, hard gap.
//   3. toBlock === null || blockNumber <= toBlock — within the (open) window; once toBlock is set, a
//      row past it is NOT tolerated → 'mismatch'.
// A missing/deleted TOLERATED_ONLYB_CLASSES.issue36OnlyBRowLoss entry → no floors → 'mismatch' for all.
export function classifyOnlyBRow(
  { blockNumber },
  chain,
  classes = TOLERATED_ONLYB_CLASSES,
) {
  const entry = classes?.issue36OnlyBRowLoss;
  if (!entry) {
    return 'mismatch';
  }

  // Missing floor for this chain ⇒ HARD FAIL — never default-tolerate an unknown chain.
  const floor = entry.perChainFloor?.[chain];
  if (!Number.isFinite(floor)) {
    return 'mismatch';
  }

  const block = Number(blockNumber);
  const withinWindow =
    entry.toBlock === null ||
    entry.toBlock === undefined ||
    block <= Number(entry.toBlock);

  const tolerated = Number.isFinite(block) && block >= floor && withinWindow;

  return tolerated ? 'tolerated' : 'mismatch';
}

// Classify ONE onlyB TRANSACTION (a tx present in leg B but MISSING in leg A) against the issue #36
// class. This is the transactions facet of the SAME leg-A loss the logs/blocks classifier already
// tolerates: when leg A dropped a whole block it dropped that block's logs AND its txs (12:22Z hourly
// cross-validation: 30 B-only chain-1 txs, all in blocks WHOLLY ABSENT from leg A, upper span exactly
// matching the tolerated logs/blocks span). Returns:
//   • 'tolerated' — the exact issue #36 shape: the tx's block is WHOLLY ABSENT from leg A (no leg-A
//     block row at that height) AND the block is at/above the per-chain realtime-era floor within the
//     (open) window. PASS-compatible, counted SEPARATELY, loudly reported.
//   • 'mismatch'  — ANYTHING else → a hard onlyB tx → FAIL, exactly as before the class existed.
// STRICTER than the logs/blocks predicate by ONE extra conjunct — the wholly-absent-block gate:
//   • `absentBlocks` is the set of block numbers WHOLLY ABSENT from leg A (leg B has the block row, leg
//     A has none), i.e. the blocks-table onlyB set the blocks classifier already computes. A tx whose
//     block is NOT in that set — a tx-level-only loss where leg A HAS the block but is MISSING just this
//     tx — is a genuinely NEW divergence class and MUST keep failing loudly, so it stays 'mismatch'.
//     This is the whole point of the strictness: tolerate only the block-loss shape we have evidenced
//     and root-caused, never a bare tx gap in an A-present block.
// The remaining conjuncts REUSE classifyOnlyBRow's floor/window logic verbatim (same floor config, same
// #30 missing-floor semantic, same open-window handling) so the two facets can never drift apart: a
// wholly-A-absent block BELOW the floor is still a HARD FAIL (below the floor leg A's store came from
// the complete-by-construction historical backfill, so any A-missing row there is a real gap, never
// this class). `absentBlocks` MUST be complete: a caller that could not enumerate every wholly-absent
// block (e.g. the blocks onlyB collector was CAPPED) MUST NOT call this as tolerating — pass an empty
// set / treat every onlyB tx as a hard FAIL, so an incomplete absent set can never widen the tolerance.
// Pure + exported so every adversarial case is unit-tested and mutation-verified in isolation.
export function classifyOnlyBTx(
  { blockNumber },
  chain,
  absentBlocks,
  classes = TOLERATED_ONLYB_CLASSES,
) {
  const absent =
    absentBlocks instanceof Set ? absentBlocks : new Set(absentBlocks ?? []);
  // The wholly-absent-block gate — the extra conjunct over classifyOnlyBRow. A tx whose block leg A
  // HAS (present in A's blocks table) is a tx-level-only loss → NEW divergence class → HARD FAIL.
  if (!absent.has(Number(blockNumber))) {
    return 'mismatch';
  }

  // Reuse the floor/window predicate verbatim (floor config, #30 missing-floor semantic, open window):
  // a wholly-absent block still has to clear the realtime-era floor to be this class.
  return classifyOnlyBRow({ blockNumber }, chain, classes);
}

// Decide the FINAL verdict of a logs/blocks table diff once its onlyB rows have been classified against
// the issue #36 class. `diff` is the streamingDiff result (onlyA/onlyB/mismatch are COUNTS). `onlyBRows`
// is the array of that diff's onlyB rows with { blockNumber } (fetched by the bounded targeted query in
// diffLogs/diffBlocks). Returns { fail, toleratedOnlyB: { count, perChain }, hardOnlyB, onlyA, mismatch }.
//
// The class is NARROW by construction — it tolerates ONLY the pure onlyB-loss shape:
//   • onlyA > 0        → leg B invented a row leg A never saw → HARD FAIL (never this class; the class is
//                        A-loses-rows-B-has, never B-extra). Refuses the inverted asymmetry.
//   • mismatch > 0     → a SHARED row's fields diverge → HARD FAIL (a different, real divergence class).
//   • any onlyB row classifyOnlyBRow → 'mismatch' (below floor / unknown chain / past window) → HARD FAIL.
// The run FAILs unless EVERY onlyB row is tolerated AND there is no onlyA and no shared mismatch. So a
// run mixing 88 tolerated onlyB rows with 1 below-floor onlyB row still FAILs — the below-floor row is
// not masked by its tolerated siblings. Pure + exported so each clause is mutation-verified in isolation.
export function classifyOnlyBDiff(diff, onlyBRows, chain, classes) {
  const perChain = {};
  let toleratedCount = 0;
  let hardOnlyB = 0;
  for (const row of onlyBRows ?? []) {
    const verdict = classifyOnlyBRow(row, chain, classes);
    if (verdict === 'tolerated') {
      toleratedCount += 1;
      perChain[chain] = (perChain[chain] ?? 0) + 1;
    } else {
      hardOnlyB += 1;
    }
  }

  const onlyA = diff?.onlyA ?? 0;
  const mismatch = diff?.mismatch ?? 0;
  const fail = onlyA > 0 || mismatch > 0 || hardOnlyB > 0;

  return {
    fail,
    toleratedOnlyB: { count: toleratedCount, perChain },
    hardOnlyB,
    onlyA,
    mismatch,
  };
}

// BACKSTOP CROSS-CHECK for the onlyB collector wiring. classifyOnlyBDiff classifies the rows the
// streamingDiff onOnlyB hook COLLECTED (onlyBRows); its verdict is only trustworthy if that array is
// EVERY onlyB row the diff counted. `diff.onlyB` is the diff's own independent COUNT of B-only rows.
// If the two disagree while the collector was NOT capped, some onlyB rows were silently dropped before
// classification (a wiring bug — a skipped hook call, a lost row) — so the collector's per-row
// tolerance verdict is built on an INCOMPLETE set and MUST NOT be trusted: this is a HARD FAIL that
// NAMES itself in the status JSON (collectorMismatch: { expected, collected }). When capped, a
// collected < onlyB gap is EXPECTED (the cap stopped collecting past ONLYB_ROW_CAP) and the existing
// capped→FAIL path already covers it — so the cross-check only fires on the un-capped path. Pure +
// exported so the backstop is mutation-verified in isolation (neuter it → a test must fail).
export function crossCheckOnlyBCollector(diffOnlyB, collectedCount, capped) {
  if (capped) {
    return { ok: true };
  }

  const expected = diffOnlyB ?? 0;
  if (expected !== collectedCount) {
    return {
      ok: false,
      collectorMismatch: { expected, collected: collectedCount },
    };
  }

  return { ok: true };
}

// AUDITABILITY (issue #36 candor): the maximum sample size of tolerated onlyB block numbers surfaced
// in the status JSON. Small on purpose — it is a spot-audit anchor for a human/script to cross-check a
// handful of leg-B rows against a third-party node, NOT the full set (which stays out of the status
// JSON to keep it bounded). 5 is enough to seed an audit; min/max bracket the whole span.
export const TOLERATED_SAMPLE_SIZE = 5;

// Build a BOUNDED spot-audit sample of tolerated onlyB rows' block numbers for the status JSON: the
// first TOLERATED_SAMPLE_SIZE block numbers plus the min and max over the WHOLE tolerated set (so the
// span is bracketed even though the sample is truncated). This is the auditability control for the
// candor limitation documented on TOLERATED_ONLYB_CLASSES: within the tolerated span, cross-validation
// alone cannot distinguish leg-A row loss from a hypothetical leg-B fabrication of the same (chain,
// block>=floor) shape — the resolving control is a third-party spot audit, and this sample is what a
// human/script anchors that audit on WITHOUT psql access to the raw diff. Empty set ⇒ null (nothing to
// audit). Pure + exported: the sample MUST stay bounded at TOLERATED_SAMPLE_SIZE and min/max must span
// the whole set (mutation: unbound the sample or break min/max → a test fails).
export function sampleToleratedOnlyB(rows) {
  const blocks = [];
  for (const r of rows ?? []) {
    const b = Number(r.blockNumber);
    if (Number.isFinite(b)) {
      blocks.push(b);
    }
  }
  if (blocks.length === 0) {
    return null;
  }

  return {
    sample: blocks.slice(0, TOLERATED_SAMPLE_SIZE),
    min: Math.min(...blocks),
    max: Math.max(...blocks),
    count: blocks.length,
  };
}

// The expected-class assertion for the transactions table. `onlyA` = txs A has but B lacks; `onlyB`
// = txs B has but A lacks; `referenced` = the subset of onlyA that an A-side log points at;
// `sharedMismatch` = txs present on BOTH sides whose full-row hash diverges (default 0). A tx that
// EXISTS in both A and B must be byte-identical — the same finalized transaction indexed two ways.
export function classifyTxDiff(
  onlyA,
  onlyB,
  referenced,
  sharedMismatch = 0,
  toleratedIssue27 = { count: 0, perChain: {} },
  knownBadRowsTally = { count: 0, perChain: {}, perHash: {} },
) {
  const ref = referenced instanceof Set ? referenced : new Set(referenced);
  const unexpectedB = [...onlyB];
  const unreferencedA = onlyA.filter((h) => !ref.has(h));
  const fail =
    unexpectedB.length > 0 || unreferencedA.length > 0 || sharedMismatch > 0;

  return {
    fail,
    class: fail ? 'UNEXPECTED' : 'realtime-parent-tx-gap',
    expectedMissing: onlyA.length,
    unexpectedB,
    unreferencedA,
    sharedMismatch,
    // Reported, never fails: shared txs whose ONLY divergence is issue #27 (access_list NULL on A,
    // chain-true on B). Counted here so the run verdict is PASS-compatible while still visible.
    toleratedIssue27,
    // Reported, never fails: shared txs pinned in `knownBadRows` (issue #32) — a single evidenced,
    // access_list-only divergence per exact hash. SEPARATE from toleratedIssue27; PASS-compatible.
    knownBadRows: knownBadRowsTally,
  };
}

// Fold the issue #36 onlyB-transaction tolerance into a classifyTxDiff result AFTER the wholly-A-absent
// block set is known (it comes from the blocks-table diff, which runs in parallel with diffTx — so this
// composition happens in compareChain, exactly as the logs/blocks onlyB tolerance does, never inside
// diffTx). `txClass` is a classifyTxDiff result; `onlyBTxRows` are its onlyB txs carried as
// { hash, blockNumber } (diffTx now collects the block number too); `absentBlocks` is the set of block
// numbers WHOLLY ABSENT from leg A (the blocks onlyB set). Returns a NEW tx class with the tolerated
// onlyB txs REMOVED from `unexpectedB` and folded into `toleratedIssue36`, and fail/class recomputed:
//   • an onlyB tx classifyOnlyBTx → 'tolerated' (its block is wholly A-absent AND at/above the floor,
//     within the open window) is PASS-compatible, counted SEPARATELY, no longer a fail.
//   • any OTHER onlyB tx (block EXISTS in A → tx-level-only loss, below floor, unknown chain, past
//     window) stays in `unexpectedB` → HARD FAIL, exactly as before the class existed.
// The onlyA / unreferencedA (realtime-parent-tx-gap) and sharedMismatch / issue #27 / knownBadRows
// facets are UNTOUCHED — this only reclassifies onlyB txs. Fail-closed on an incomplete absent set: if
// the caller could not enumerate every wholly-absent block (blocks onlyB CAPPED), it passes an empty
// `absentBlocks` and NO onlyB tx is tolerated. If `onlyBTxRows` is missing (a diffTx that didn't carry
// them, or no onlyB txs), the class is returned unchanged. Pure + exported so the fold — the tolerated
// partition, the fail recomputation, and the untouched-facet guarantee — is mutation-verified in isolation.
export function classifyOnlyBTxDiff(
  txClass,
  onlyBTxRows,
  chain,
  absentBlocks,
  classes = TOLERATED_ONLYB_CLASSES,
) {
  const rows = onlyBTxRows ?? [];
  if (rows.length === 0) {
    return txClass;
  }

  const absent =
    absentBlocks instanceof Set ? absentBlocks : new Set(absentBlocks ?? []);
  const perChain = {};
  let toleratedCount = 0;
  const hardUnexpectedB = [];
  for (const row of rows) {
    const verdict = classifyOnlyBTx(row, chain, absent, classes);
    if (verdict === 'tolerated') {
      toleratedCount += 1;
      perChain[chain] = (perChain[chain] ?? 0) + 1;
    } else {
      hardUnexpectedB.push(row.hash);
    }
  }

  // Recompute the verdict from the HARD onlyB txs only — the tolerated ones no longer fail. The onlyA
  // (unreferencedA) and sharedMismatch causes are unchanged, read straight off the incoming class.
  const fail =
    hardUnexpectedB.length > 0 ||
    txClass.unreferencedA.length > 0 ||
    (txClass.sharedMismatch ?? 0) > 0;

  return {
    ...txClass,
    fail,
    class: fail ? 'UNEXPECTED' : 'realtime-parent-tx-gap',
    unexpectedB: hardUnexpectedB,
    // Reported, never fails: onlyB txs leg A lost with its wholly-absent blocks, tolerated per issue #36
    // (block wholly absent from leg A AND at/above the per-chain realtime-era floor). Mirrors the
    // logs/blocks toleratedIssue36 shape so aggregateToleratedIssue36 rolls all three tables the same way.
    toleratedIssue36: { count: toleratedCount, perChain },
  };
}

// Drop diffTx's internal `onlyBTxRows` carrier (the raw onlyB txs with block numbers, used only to fold
// the issue #36 tx tolerance in compareChain) from a tx class before it goes into the status JSON, so the
// persisted diffClasses stay bounded — only the tolerated COUNT/perChain surfaces, never the raw row list.
// Pure + exported so the strip contract is asserted directly.
export function stripTxOnlyBRows(txClass) {
  if (!txClass || !('onlyBTxRows' in txClass)) {
    return txClass;
  }

  const { onlyBTxRows: _drop, ...rest } = txClass;

  return rest;
}

// Classify ONE shared tx whose FULL-row md5 diverges between side A and side B. Returns:
//   • 'knownBadRow' — the tx hash is pinned in `knownBadRows` (issue #32) AND the chain matches AND
//     BOTH sides are concrete (neither access_list NULL) AND only access_list differs (ex-AL md5s
//     equal). Reported + PASS-compatible, counted SEPARATELY.
//   • 'tolerated'   — the exact, fully-root-caused issue #27 shape (span-keyed, see below).
//   • 'mismatch'    — ANYTHING else → a hard sharedMismatch → FAIL, exactly as before either class.
// Pure + exported so every adversarial case is unit-tested and mutation-verified in isolation.
//
// knownBadRow is checked FIRST and is deliberately NARROWER on identity (exact hash + chain) than
// issue #27, and pins a DIFFERENT null-shape: issue #32's row has BOTH sides NON-NULL (A='[]',
// B chain-true). So instead of issue #27's `aAccessListNull === true`, its predicate requires BOTH
// concrete — aAccessListNull === false AND bAccessListNull === false — which refuses to tolerate an
// A-NULL / B-non-null drift or a B-side rot to NULL on this hash (only the measured '[]'-vs-list shape
// is pinned). Its other structural guard is exAlMd5A === exAlMd5B — every OTHER column still
// byte-identical. If a second column ever diverges on that row (ex-AL md5s differ), OR either side goes
// NULL, the pin stops protecting it → 'mismatch' → hard FAIL, so a rotting row is never masked.
//
// The issue #27 predicate is a conjunction of five clauses, each refusing to mask a distinct thing:
//   1. exAlMd5A === exAlMd5B   — md5 over `to_jsonb(t) - 'access_list'`. If ANY second column also
//      differs, these differ → 'mismatch'. This is what stops the tolerance widening to "any diff on a
//      row that also happens to have an access_list gap".
//   2. aAccessListNull === true — the loss is on side A (the RPC-realtime leg that nulled the key). If
//      A is non-null-but-different from B, this is false → 'mismatch' (a real access_list divergence).
//   3. bAccessListNull === false — side B holds a concrete value (the chain-true one). An inverted
//      asymmetry (B null, A non-null) is NOT this class → 'mismatch'.
//   4. blockNumber >= floor      — at/above the measured realtime-span floor for THIS chain. A missing
//      floor entry (unknown chain) is an explicit HARD FAIL, never a default-tolerate.
//   5. toBlock === null || blockNumber <= toBlock — within the (open) window; once toBlock is set, a
//      row past it is NOT tolerated → 'mismatch'.
// A missing/deleted TOLERATED_CLASSES.issue27AccessListNull entry → no floors → 'mismatch' for all.
export function classifySharedTx(
  { hash, blockNumber, exAlMd5A, exAlMd5B, aAccessListNull, bAccessListNull },
  chain,
  classes = TOLERATED_CLASSES,
  badRows = knownBadRows,
) {
  // knownBadRows (issue #32): an EXACT-hash pin. Tolerated IFF the hash is listed for THIS chain AND
  // BOTH sides are concrete (neither access_list NULL — the measured '[]'-vs-list shape) AND only
  // access_list differs (ex-AL md5s equal). The both-non-null guard refuses to tolerate an A-NULL /
  // B-non-null drift or a B-side rot to NULL on this hash — it is the DIFFERENT null-shape from
  // issue #27 (which requires aAccessListNull === true), not the absence of a null check.
  const pinned = (badRows ?? []).find(
    (r) => r.hash === hash && r.chain === chain,
  );
  if (
    pinned &&
    aAccessListNull === false &&
    bAccessListNull === false &&
    exAlMd5A === exAlMd5B
  ) {
    return 'knownBadRow';
  }

  const entry = classes?.issue27AccessListNull;
  if (!entry) {
    return 'mismatch';
  }

  // Missing floor for this chain ⇒ HARD FAIL — never default-tolerate an unknown chain.
  const floor = entry.perChainFloor?.[chain];
  if (!Number.isFinite(floor)) {
    return 'mismatch';
  }

  const block = Number(blockNumber);
  const withinWindow =
    entry.toBlock === null ||
    entry.toBlock === undefined ||
    block <= Number(entry.toBlock);

  const tolerated =
    exAlMd5A === exAlMd5B &&
    aAccessListNull === true &&
    bAccessListNull === false &&
    Number.isFinite(block) &&
    block >= floor &&
    withinWindow;

  return tolerated ? 'tolerated' : 'mismatch';
}

// A psql child is only a trustworthy data source if it exited cleanly. A non-zero exit (bad SQL,
// connection refused, auth failure) or a spawn error must NEVER be read as "zero rows" — that is the
// false-PASS class: an empty/partial stream silently compared as a completed diff. Pure so it can be
// asserted directly.
export function psqlExitVerdict({ code, signal, spawnError }) {
  if (spawnError) {
    return { ok: false, reason: `psql spawn failed: ${spawnError}` };
  }
  if (signal) {
    return { ok: false, reason: `psql killed by signal ${signal}` };
  }
  if (code !== 0) {
    return { ok: false, reason: `psql exited ${code}` };
  }

  return { ok: true };
}

// Split an array of hashes into fixed-size chunks (default 5000) for a chunked IN-list lookup. Pure +
// exported: the referenced-parent-tx lookup must verify ANY onlyA size, so it splits into bounded IN
// lists rather than skipping the lookup above a fixed threshold (the old `onlyA.length <= 200_000`
// gate classified every parent-tx gap in a large healthy soak as unreferenced → false FAIL).
export function chunk(items, size = 5000) {
  const out = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }

  return out;
}

// Collect the referenced subset of `hashes` by running `lookup(batch)` over EVERY chunk and unioning
// the results. `lookup` is an async fn batch → referenced-hashes-in-batch (the psql IN-list query in
// diffTx). Pure over its callback + exported so a mutation that stops after the first chunk (leaving
// later chunks' referenced hashes unfound) is caught by a test rather than surfacing as a false FAIL.
export async function collectReferenced(hashes, lookup, size = 5000) {
  const referenced = [];
  for (const batch of chunk(hashes, size)) {
    const found = await lookup(batch);
    for (const h of found) {
      referenced.push(h);
    }
  }

  return referenced;
}

// _ponder_checkpoint (or any progress value) must never go backwards across runs.
export function checkpointMonotonic(values) {
  for (let i = 1; i < values.length; i++) {
    if (BigInt(values[i]) < BigInt(values[i - 1])) {
      return {
        ok: false,
        at: i,
        prev: String(values[i - 1]),
        cur: String(values[i]),
      };
    }
  }

  return { ok: true };
}

// ── ponder checkpoint encoding (verified against ponder@0.16.6 source) ──────────────────────────
// packages/core src/utils/checkpoint.ts (published: package/dist/esm/utils/checkpoint.js). The 75-char
// `_ponder_checkpoint.latest_checkpoint` (VARCHAR(75), created in database/index.js) is:
//   blockTimestamp(10) chainId(16) blockNumber(16) transactionIndex(16) eventType(1) eventIndex(16)
// so blockNumber occupies 0-based offsets [26,42) — 1-based SQL substring position 27, length 16.
// We extract the BLOCK-NUMBER field (a plain integer height, Number-safe and on the SAME scale as the
// overlapBound sync-store block max), NOT Number(whole-encoded-checkpoint): the full ~75-digit value
// overflows Number's ~16 significant digits, so same-second block rewinds are invisible AND, after a
// later fallback to the small-scale block max, a checkpoint-scale prior value would wrongly read as a
// rewind (false FAIL). The block field keeps the monotonicity series coherent across fallback flapping.
export const CHECKPOINT_BLOCK_OFFSET_0 = 26; // 0-based char offset of blockNumber (10 + 16)
export const CHECKPOINT_BLOCK_LEN = 16; // BLOCK_NUMBER_DIGITS
// 1-based position for SQL substring(str from <pos> for <len>).
export const CHECKPOINT_BLOCK_SQL_POS = CHECKPOINT_BLOCK_OFFSET_0 + 1; // 27

// Extract the block-number field from a raw 75-char encoded checkpoint string. Returns the height as a
// decimal string (no Number() — the field is <=16 digits but the caller wants an exact, BigInt-safe
// value), or null for a malformed/short/absent checkpoint. Pure + exported: mutate the offset/len and
// the extraction test reads the wrong slice and fails.
export function extractCheckpointBlock(raw) {
  if (typeof raw !== 'string') {
    return null;
  }

  const seg = raw.slice(
    CHECKPOINT_BLOCK_OFFSET_0,
    CHECKPOINT_BLOCK_OFFSET_0 + CHECKPOINT_BLOCK_LEN,
  );
  if (seg.length !== CHECKPOINT_BLOCK_LEN || !/^\d+$/.test(seg)) {
    return null;
  }

  return String(BigInt(seg));
}

// Decide whether the `_ponder_checkpoint` query yielded a REPORTABLE progress value, given the row
// COUNT for the chain and the extracted max block. The count is what distinguishes the three states a
// bare `coalesce(max(...),0)` conflates:
//   • rowCount === 0 → NO checkpoint row for this chain → not usable; the caller falls back to the
//     sync-store block max (an older store, or the chain never committed a checkpoint).
//   • rowCount > 0 → a REAL committed checkpoint exists → usable, and value = maxBlock EVEN IF 0. A
//     valid checkpoint whose blockNumber field is exactly 0 is a real, reportable progress value —
//     the strongest form of the rewind this guard exists to catch (a resume that rewound the
//     committed checkpoint to block 0 while the sync store still holds a high block max). Reporting 0
//     lets monotonicity FAIL it against prior real progress; falling back to the (still-high) block
//     max would silently PASS that rewind. So a zero max with rows present is usable, NOT a fallback.
// Pure + exported so the rows-present-with-zero-max case is mutation-verified directly.
export function checkpointDecision(rowCount, maxBlock) {
  if (!(Number.isFinite(rowCount) && rowCount > 0)) {
    return { usable: false, value: null };
  }

  const value = Number(maxBlock);

  return {
    usable: Number.isFinite(value),
    value: Number.isFinite(value) ? value : null,
  };
}

// A schema name goes verbatim into `"<schema>"._ponder_checkpoint` — reject anything that is not a
// bare SQL identifier so it can never carry an injection payload. Empty ⇒ null (caller emits the
// unqualified table name, the legacy default-search_path behavior). Pure + exported.
export function sanitizeSchemaIdent(schema) {
  if (schema === undefined || schema === null || schema === '') {
    return null;
  }
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(schema)) {
    throw new Error(
      `AB_SCHEMA_B is not a valid SQL identifier: ${JSON.stringify(schema)}`,
    );
  }

  return schema;
}

// ── persist-stagnation guard (issue #38) ─────────────────────────────────────────────────────────
//
// The finalized-overlap window is [lo, min(maxA, maxB) - margin]. If ONE leg stops persisting sync
// rows (crash wedge, silent blindness like issue #36, an ingest stall), the window `hi` FREEZES at
// the stalled leg's block max, so every row of divergence ABOVE hi is out of scope by construction —
// the per-chain verdict stays PASS while real loss accumulates invisibly (issue #36: chain 8453 read
// PASS for ~17h with 32,825 blocks / ~476 log rows of real divergence sitting above the frozen hi).
//
// A MUTUAL freeze is BENIGN: `max(number)` only advances when the app's filters match a new event, so
// a sparse app can leave BOTH legs quiet on the same chain at the same time (chain 1 did exactly
// that). Absolute staleness is therefore NOT the signal — the signal is ONE-SIDED SKEW between the
// two legs' progress. We measure that skew with the newest-row block TIMESTAMPS, not block-number
// deltas: 32k blocks is ~17h on chain 8453 but ~2h on chain 42161, so a block-count threshold cannot
// be chain-agnostic, whereas ponder_sync.blocks.timestamp (on-chain time, Unix seconds) is one clock
// across every chain.

/**
 * Default max tolerated skew between the two legs' newest-row block timestamps, in SECONDS.
 * 7200s = 2h — a CONSCIOUS choice comfortably above the worst crash-recovery time we expect
 * (~45–60 min: a Soak B restart re-syncs its realtime lag before its newest block timestamp catches
 * back up), so a healthy leg recovering from a restart never trips the guard, while a genuine
 * one-sided wedge (a leg that has stopped persisting for hours) is caught long before issue #36's
 * ~17h blind window could reopen. Tunable via AB_STAGNATION_MAX_SKEW_S for chains/apps with a
 * different recovery profile.
 */
export const AB_STAGNATION_DEFAULT_MAX_SKEW_S = 7200;

// Parse AB_STAGNATION_MAX_SKEW_S. Unset ⇒ the documented default. A present-but-garbage value
// (non-numeric, non-finite, or non-positive — 0 disables the guard, a negative is nonsense) FAILS
// LOUD naming the var, exactly as sanitizeSchemaIdent does for AB_SCHEMA_B: a config guard must never
// silently disable itself. Pure + exported so every branch is asserted without touching the env.
export function readStagnationThreshold(raw) {
  if (raw === undefined || raw === null || raw === '') {
    return AB_STAGNATION_DEFAULT_MAX_SKEW_S;
  }

  const n = Number(raw);
  if (!(Number.isFinite(n) && n > 0)) {
    throw new Error(
      `AB_STAGNATION_MAX_SKEW_S must be a positive number of seconds: ${JSON.stringify(raw)}`,
    );
  }

  return n;
}

// Pure per-chain persist-stagnation decision from the two legs' newest-row block TIMESTAMPS (Unix
// seconds), a WALL-CLOCK `nowMs`, and the CROSS-RUN state persisted from the prior run (`prev`, or
// null on the first run / for a chain with no prior state). `maxA`/`maxB` are the newest-row block
// NUMBERS (carried through for the counters JSON, so a frozen window is legible even below
// threshold). `tsA`/`tsB` are the newest-row block timestamps, or null/undefined when that leg has NO
// rows for the chain (SQL max(timestamp) → NULL).
//
// WHY CROSS-RUN STATE (review findings 1 High + 4 Med, one mechanism). A one-shot skew reading cannot
// tell a TRANSIENT lag (a leg mid-recovery, catching up) from a genuine WEDGE (a leg that has stopped
// and stays stopped). Two run observations tell them apart by DIRECTION:
//   • a wedged leg's newest-row ts does NOT advance between runs (frozen), or the skew keeps GROWING
//     (falling further behind) → a real one-sided stall → FAIL.
//   • a recovering leg's newest-row ts DOES advance AND the skew SHRINKS → catching up → non-fail.
// So the FIRST run to observe an over-threshold skew ARMS (records this run's ts) and defers the
// verdict one run rather than FAILing blind — a conscious ONE-RUN detection delay (see the PR candor
// section). This cannot fail-open on a slow/trickling wedge: a leg persisting a trickle still falls
// further behind a healthy leg, so the skew GROWS run over run and the growing-skew clause catches it.
// In-window divergence (both legs live, drifting inside [lo,hi]) is NOT this guard's job — that is the
// windowed row/bucket diff's; this guard only closes the FROZEN-WINDOW blind spot above `hi`.
//
// ── UNIFIED EVIDENCE RECORD (review delta 3, ruling D1) ──────────────────────────────────────────────
//
// The prior design carried TWO DISJOINT state shapes — a both-populated { tsA, tsB, skew } and a
// one-leg-empty { emptySide, firstEmptyAtMs, populatedTs } — and switched on which one `prev` happened
// to be. That switching LAUNDERED evidence across SHAPE flips (delta-3 HOLE 1): an empty↔populated
// oscillation could FAIL once, then every shape flip read `prev` in "the wrong shape", found no usable
// prior, and re-armed from scratch — so a stuck leg oscillating empty↔one-stale-row read non-fail
// FOREVER while the other leg advanced. Same bug CLASS as the round-2 stale-side-flip, now across state
// shapes. And the non-fail labels never checked advancement (HOLE 2): 'lagging-constant' was emitted for
// a BOTH-FROZEN pair (comment claimed "both advanced"), and 'catching-up' for an A-frozen/B-regressed
// pair (skew shrank via a one-sided regression, nobody advancing).
//
// The fix is ONE per-chain state shape, written EVERY run, regime derived from the CURRENT observation
// only, prior evidence used REGARDLESS of the prior run's regime:
//   { tsA, tsB, skew, emptySinceA: { atMs } | null, emptySinceB: { atMs } | null,
//     wedgeFailedSince: ms | null, wedgeStaleSide: 'A' | 'B' | null, wedgeReason: string | null }
//   • tsA / tsB — each leg's LAST-KNOWN newest-row ts (null only if that leg has NEVER had a row). When
//     a leg is EMPTY this run, its ts is CARRIED FORWARD from prior evidence (D1 carry-forward rule) so
//     a shape flip never drops what we knew — the advancement check still has a baseline to compare.
//   • skew — |tsA − tsB| when both are known this run, else null (unmeasurable with an empty leg).
//   • emptySinceA / emptySinceB — PER-LEG (review delta 4, F5): { atMs } for a leg that is empty this
//     run and has been since `atMs` (armed on the FIRST empty observation for THAT leg, preserved while
//     it stays empty, cleared when it is observed populated). Per-leg so a flip on the OTHER side never
//     resets this leg's timer — alternating-empty-sides can no longer re-arm forever.
//   • wedgeFailedSince — the wall-clock ms of the FIRST fail in the current unrecovered wedge episode,
//     or null when not wedged. STICKY (D2): once set it persists across every regime/shape until a
//     GENUINE RECOVERY clears it.
//   • wedgeStaleSide — the leg identified as stalled at the FIRST fail of the current wedge episode,
//     carried forward UNCHANGED while the wedge is live (review delta 4, F1/N1). Recovery/attribution
//     read THIS carried side, never a per-run recomputed olderSide — so a wiped-while-ahead leg cannot
//     flip the recovery check onto the wrong leg.
//   • wedgeReason — the TRUE first-fail reason of the current wedge episode ('frozen', 'skew-growing',
//     'one-sided-empty'), written ONCE at the first fail and carried forward (review delta 4, N2). The
//     sticky 'wedge-unrecovered' returns render `originalReason` from this carried field, so the alert
//     reads the wedge it actually WAS — not a reason re-derived from the current shape.
// INVARIANT (D1): NO branch may discard evidence fields it does not itself use. Every return builds
// `nextState` from the carried-forward fields, so a shape or regime transition PRESERVES everything. A
// field is null ONLY where genuinely unobservable, never because a branch "didn't need it".
//
// ── STICKY WEDGE FAIL (ruling D2 + review delta 4 restructure) ───────────────────────────────────────
//
// The FIRST branch of the decision is the sticky-wedge gate: if `prev.wedgeFailedSince` is set there are
// EXACTLY TWO exits, and every shape-handling branch (empty-arming, both-empty, fresh over-threshold)
// runs ONLY AFTER it — so NO future branch can bypass stickiness by construction (this kills the whole
// class of round-1..4 findings where some shape branch laundered a live wedge).
//   (a) GENUINE RECOVERY — provable ONLY with FINITE current evidence: BOTH legs populated this run AND
//       (skew ≤ threshold, OR the CARRIED wedge-stale leg strictly advanced vs its last-known ts AND the
//       skew did not grow). Clears the wedge → 'catching-up' / 'both-populated-in-skew'.
//   (b) OTHERWISE — FAIL 'wedge-unrecovered', carrying ALL evidence forward unchanged (wedgeFailedSince,
//       wedgeStaleSide, wedgeReason, and both carried ts). An EMPTY observation (one leg or both empty)
//       can NEVER reach (a): it lacks the finite both-populated evidence recovery requires, so it always
//       lands in (b). This is what kills the oscillation and the wiped-while-ahead / both-empty-transient
//       holes: the empty↔stale-row flip never presents both-populated recovery evidence, so it stays FAIL
//       at r3/r4/r5, exactly as intended. A reappearing leg that genuinely advances with a non-growing
//       skew recovers via (a).
//
// ── LABEL HONESTY + ADVANCEMENT (ruling D3, no wedgeFailedSince set) ─────────────────────────────────
//
// Over-threshold, not (yet) a sticky fail, prev evidence present:
//   • 'catching-up'        — the OLDER (stale) leg STRICTLY advanced AND the skew SHRANK. Non-fail.
//   • 'lagging-constant'   — BOTH legs strictly advanced AND the skew neither shrank nor grew (a steady
//                            lag; the stale leg IS moving so `hi` advances with it and the WINDOWED diff
//                            owns that regime — this guard is only for FROZEN windows).
//   • FAIL 'frozen'        — one leg frozen since prev while the OTHER advanced (a one-sided wedge).
//   • FAIL 'skew-growing'  — both moved but the gap widened (a trickling wedge losing ground).
//   • 'mutually-quiescent' — NEITHER leg strictly advanced over threshold (a MUTUAL freeze). Non-fail:
//                            this is the pre-existing mutual-freeze exemption — both legs down is an
//                            OPS-LEVEL alarm (an ingest/box outage), OUT OF SCOPE for an A-vs-B DIVERGENCE
//                            guard, which exists only to catch ONE leg stalling while the OTHER advances.
//                            A LEADER-REGRESSION + FROZEN-STALE pair (skew shrank because the leading leg
//                            REGRESSED newest-ts — a legitimate one-sided realtime reorg-prune — while the
//                            stale leg did NOT advance) lands here too, NOT 'catching-up': nobody advanced,
//                            so the gap "closing" is an artefact of a reorg, not recovery. The NEXT run's
//                            advancement check discriminates (if the stale leg is truly frozen it FAILs
//                            'frozen' once the leader re-advances) — a conscious ONE-RUN delay.
//
// Returns { fail, reason, staleSide, skew, tsA, tsB, maxA, maxB, nextState, originalReason? }.
// `nextState` is the unified evidence record to persist for THIS chain for the next run (never mutates
// `prev`); it is null only when there is NOTHING to carry (both legs empty AND no live wedge). `reason`
// is a short machine tag (e.g. 'frozen', 'skew-growing', 'lagging-constant', 'mutually-quiescent',
// 'wedge-unrecovered', 'skew-above-threshold-arming', 'catching-up', 'one-sided-empty', 'empty-arming',
// 'both-populated-in-skew', 'both-empty'). A sticky 'wedge-unrecovered' return also carries
// `originalReason` (the true first-fail reason of the wedge episode). Pure + exported so every branch is
// mutation-verified without a DB.
//
// STATE SHAPE VERSIONING (review delta 4, F4 candor): the `nextState` shape above is v1 as of the FIRST
// deploy of this guard. The deployed differ runs merged main, which has NO `_stagnation` checkpoint key,
// so NO intermediate state shape was ever persisted in production — there is nothing to migrate. An
// UNKNOWN or legacy `prev` shape (e.g. a hand-edited checkpoint) reads as all-null evidence and DELIBERATELY
// re-arms: a documented ONE-RUN detection delay in the FAIL-SAFE direction (a wedge is re-detected the next
// run; it is never fabricated). We intentionally do NOT write migration code for a shape that was never shipped.
export function stagnationDecision({
  maxA,
  tsA,
  maxB,
  tsB,
  threshold,
  prev = null,
  nowMs = Date.now(),
}) {
  const a = tsToSeconds(tsA);
  const b = tsToSeconds(tsB);
  const mA = maxToNumber(maxA);
  const mB = maxToNumber(maxB);
  const base = { tsA: a, tsB: b, maxA: mA, maxB: mB };

  // Prior evidence, read from the UNIFIED shape regardless of the prior run's regime. `prev` may be an
  // unknown/legacy shape or corrupt — read defensively, treating any missing/non-finite field as
  // unobservable (null), never as a phantom. Missing/corrupt prev ⇒ every prior field null ⇒ fail-safe
  // ARMING (D1 carry-forward: null baseline defers, never fails-open).
  const prevTsA = prev && Number.isFinite(prev.tsA) ? prev.tsA : null;
  const prevTsB = prev && Number.isFinite(prev.tsB) ? prev.tsB : null;
  // NOTE (review delta 5, finding 1): we deliberately do NOT read `prev.skew` for growth comparison.
  // The MEASURED skew is stored in state for telemetry, but it is null after any empty-run gap, so
  // direction is judged from the CARRIED gap (prevGap/curGap below) which persists through empty gaps.
  const prevWedgeFailedSince =
    prev && Number.isFinite(prev.wedgeFailedSince)
      ? prev.wedgeFailedSince
      : null;
  // The CARRIED wedge attribution (review delta 4, F1/N1/N2): the stalled leg and the true first-fail
  // reason of the LIVE wedge episode, written once at the first fail and threaded forward UNCHANGED.
  // Recovery and sticky attribution read THESE — never a per-run recomputed olderSide (which a
  // wiped-while-ahead leg would flip onto the wrong leg).
  const prevWedgeStaleSide =
    prev && (prev.wedgeStaleSide === 'A' || prev.wedgeStaleSide === 'B')
      ? prev.wedgeStaleSide
      : null;
  const prevWedgeReason =
    prev && typeof prev.wedgeReason === 'string' ? prev.wedgeReason : null;

  // D1 carry-forward: a leg empty THIS run keeps its last-known ts from prior evidence (null if never
  // observed) so the advancement check always has a baseline. A populated leg uses this run's reading.
  const carriedA = a === null ? prevTsA : a;
  const carriedB = b === null ? prevTsB : b;

  // Per-leg emptiness timers (review delta 4, F5). Each leg's `emptySince.atMs` is armed on the FIRST
  // run it is observed empty (and not already armed), preserved while it stays empty, cleared when it is
  // observed populated. A flip on the OTHER leg never touches this one — alternating-empty-sides can no
  // longer re-arm forever. Read the prior arms defensively.
  const prevEmptySinceA =
    prev?.emptySinceA && Number.isFinite(prev.emptySinceA.atMs)
      ? prev.emptySinceA
      : null;
  const prevEmptySinceB =
    prev?.emptySinceB && Number.isFinite(prev.emptySinceB.atMs)
      ? prev.emptySinceB
      : null;
  // CONSCIOUS CHOICE (review delta 5, finding 3 — SUSTAINED as fail-closed): a BOTH-empty run arms BOTH
  // per-leg timers, so a mutual outage CONSUMES the one-sided-empty grace window before any one-sided
  // condition exists. This is the INTENDED F5 semantic and is fail-closed: each timer factually measures
  // THAT leg's emptiness duration, and a leg still empty past the grace window while the OTHER leg has
  // recovered and advances genuinely SHOULD alarm — the emptiness that a downstream consumer sees is
  // real regardless of whether the sibling was also down earlier. CAVEAT (accepted): on a bootstrap
  // deploy where A starts first and B a few minutes later, B's timer arms during the both-empty window,
  // so the first run after A advances can produce ONE false one-sided-empty FAIL — self-clearing once B
  // persists a row. Fail-closed and one-shot, so this stands; the pinning test locks the behavior.
  const emptySinceA = a === null ? (prevEmptySinceA ?? { atMs: nowMs }) : null;
  const emptySinceB = b === null ? (prevEmptySinceB ?? { atMs: nowMs }) : null;

  // MEASURED skew is |a − b| only when BOTH legs are populated THIS run; unmeasurable (null) with an
  // empty leg. This is the TELEMETRY / THRESHOLD number: it is stored in nextState and drives the
  // skew ≤ threshold recovery check and the skew clauses. It is NOT used for growth comparison — see
  // the CARRIED-GAP basis below (review delta 5, finding 1).
  const skew = a !== null && b !== null ? Math.abs(b - a) : null;

  // Did each leg's LAST-KNOWN ts strictly advance vs the prior run's last-known ts? An empty leg (ts
  // carried forward unchanged) is by construction NOT advancing. A regression (reorg-prune) is NOT
  // advancing — the fail-closed reading. A leg with no prior baseline cannot be judged advanced.
  const aAdvanced = prevTsA !== null && carriedA !== null && carriedA > prevTsA;
  const bAdvanced = prevTsB !== null && carriedB !== null && carriedB > prevTsB;

  // CARRIED-GAP basis for growth/shrink direction (review delta 5, finding 1). Growth must be judged
  // from the last-known gap between the CARRIED timestamps, NOT the measured `skew`: `skew` is null on
  // any run with an empty leg, and `prev.skew` is therefore null after an empty-run gap — using it
  // throws away a genuinely GROWN last-known gap (the carried tsA/tsB persist through empty gaps and
  // prove the gap widened). `gap(x, y)` = |x − y| only when BOTH are finite, else null (unknown, never
  // coerced to 0). prevGap is the gap at the END of the prior run (prev carried ts); curGap is this
  // run's carried gap. Both persist through empty legs, so growth is visible across an empty-run gap.
  const prevGap = gap(prevTsA, prevTsB);
  const curGap = gap(carriedA, carriedB);

  // Direction from the CARRIED gap, null-safe (review delta 5, finding 1): `gapGrew` is true ONLY when
  // BOTH gaps are finite and the current strictly exceeds the prior — a null (unknown) gap on either
  // side defers rather than fails/clears open. `gapShrank` is the null-safe mirror. These replace the
  // former `skew`-based comparisons at every growth/shrink site so an empty-run gap can no longer mask
  // a grown last-known gap (the fail-open) nor manufacture a false shrink.
  const gapGrew = gapGrewFrom(prevGap, curGap);
  const gapShrank = gapShrankFrom(prevGap, curGap);

  // Assemble the unified nextState from carried-forward evidence. The wedge fields are threaded per
  // branch (a fresh fail sets them; a recovery clears them; an unrecovered run preserves them). The
  // INVARIANT (D1): every return path funnels through `evidence(...)` so no field is silently dropped.
  const evidence = (wedgeFailedSince, wedgeStaleSide, wedgeReason) => ({
    tsA: carriedA,
    tsB: carriedB,
    skew,
    emptySinceA,
    emptySinceB,
    wedgeFailedSince,
    wedgeStaleSide,
    wedgeReason,
  });

  // ─────────────────────────────────────────────────────────────────────────────────────────────────
  // STICKY-WEDGE GATE (review delta 4 restructure) — the FIRST branch. If a wedge is live, there are
  // EXACTLY TWO exits, and no shape-handling branch below can be reached without passing here first. So
  // stickiness cannot be bypassed by construction.
  // ─────────────────────────────────────────────────────────────────────────────────────────────────
  if (prevWedgeFailedSince !== null) {
    // GENUINE RECOVERY requires FINITE current evidence: BOTH legs populated this run. An empty
    // observation (one leg or both empty) lacks that evidence and can NEVER clear a wedge.
    const bothPopulated = a !== null && b !== null;
    // The wedged/stale leg, from CARRIED evidence — never a per-run recomputed side. Did IT advance?
    const wedgeStaleAdvanced =
      prevWedgeStaleSide === 'A'
        ? aAdvanced
        : prevWedgeStaleSide === 'B'
          ? bAdvanced
          : false;
    const skewWithinThreshold = skew !== null && skew <= threshold;
    // Recovery via stale-leg advance requires the CARRIED gap to NOT be growing (review delta 5,
    // finding 1). `!gapGrew` reads the carried-timestamp gap, which survives the empty-run gap that
    // makes `prev.skew` null — so a stale leg that ticked forward while the overall gap kept WIDENING
    // no longer counts as recovery (the former `!skewGrew` fail-open cleared it).
    const genuinelyRecovered =
      bothPopulated &&
      (skewWithinThreshold || (wedgeStaleAdvanced && !gapGrew));

    if (!genuinelyRecovered) {
      // (b) still wedged — FAIL, carrying ALL evidence forward unchanged.
      return {
        fail: true,
        reason: 'wedge-unrecovered',
        staleSide: prevWedgeStaleSide,
        skew,
        ...base,
        nextState: evidence(
          prevWedgeFailedSince,
          prevWedgeStaleSide,
          prevWedgeReason,
        ),
        originalReason: prevWedgeReason ?? 'frozen',
      };
    }

    // (a) genuine recovery — the wedge clears. Fall through into the shape branches below with the wedge
    // cleared: they will label this run's regime honestly ('catching-up' / 'both-populated-in-skew' /
    // 'lagging-constant') and set nextState.wedgeFailedSince = null. (Both-populated is guaranteed here.)
  }

  // ── both legs empty → benign mutual/not-started ──
  // A both-empty run cannot be reached with a LIVE wedge (the sticky gate FAILed it above). With no
  // wedge and nothing persisted on EITHER leg there is no one-sided divergence to point at (an ops-level
  // "everything is down" state, out of scope). Carry forward whatever last-known ts we had (may be null).
  if (a === null && b === null) {
    const nextState = evidence(null, null, null);
    // nextState is null ONLY when there is genuinely nothing to carry (no ts ever seen, no wedge).
    const nothingToCarry = carriedA === null && carriedB === null;

    return {
      fail: false,
      reason: 'both-empty',
      staleSide: null,
      skew: null,
      ...base,
      nextState: nothingToCarry ? null : nextState,
    };
  }

  // ── one leg empty, the other populated ──
  // A LIVE wedge cannot be here either (the sticky gate FAILed an empty run above — an empty run never
  // presents both-populated recovery evidence). So this only ever ARMS a FRESH one-sided-empty wedge.
  if (a === null || b === null) {
    const emptySide = a === null ? 'A' : 'B';
    const emptySince = emptySide === 'A' ? emptySinceA : emptySinceB;
    const emptyLongEnough = nowMs - emptySince.atMs > threshold * 1000;
    // The populated (live) leg's motion: did it advance vs its own last-known ts? A MUTUAL freeze
    // (the live leg also not advancing) is NOT a one-sided wedge.
    const liveAdvanced = emptySide === 'A' ? bAdvanced : aAdvanced;

    // FRESH one-sided-empty wedge: empty past the grace window AND the live leg advanced. If the live
    // leg is also frozen, that is a mutual freeze → non-fail (armed, not failed).
    const fail = emptyLongEnough && liveAdvanced;

    return {
      fail,
      reason: fail ? 'one-sided-empty' : 'empty-arming',
      staleSide: fail ? emptySide : null,
      skew: null,
      ...base,
      nextState: fail
        ? evidence(nowMs, emptySide, 'one-sided-empty')
        : evidence(null, null, null),
    };
  }

  // ── both legs populated ──
  if (skew <= threshold) {
    // Healthy / within tolerance. Skew ≤ threshold is a GENUINE RECOVERY (D2), so a wedge that cleared
    // in the sticky gate lands here. Surface the real computed skew (finding 3: telemetry, not null).
    // staleSide null (a below-threshold lag has an older leg but no stall). Emptiness arms clear.
    return {
      fail: false,
      reason: 'both-populated-in-skew',
      staleSide: null,
      skew,
      ...base,
      nextState: evidence(null, null, null),
    };
  }

  // skew > threshold, both populated, and no live wedge (the sticky gate either FAILed or recovered).
  // Whether we have a prior baseline to judge direction:
  const haveBaseline = prevTsA !== null && prevTsB !== null;

  // No prior baseline (first over-threshold observation, or prev never had both legs) → ARM, defer the
  // verdict one run (candor: one-run detection delay).
  if (!haveBaseline) {
    return {
      fail: false,
      reason: 'skew-above-threshold-arming',
      staleSide: null,
      skew,
      ...base,
      nextState: evidence(null, null, null),
    };
  }

  // We have a baseline and no live wedge (or it just recovered). Decide THIS run's regime from
  // advancement (D3 honesty). A leg that regressed or held still did NOT advance (fail-closed).
  const oneLegFrozenWhileOtherAdvanced =
    (!aAdvanced && bAdvanced) || (!bAdvanced && aAdvanced);
  // The stale (older-ts) leg THIS run, used only for 'catching-up' attribution below (recovery no longer
  // depends on it — the sticky gate owns recovery). Equal / unknown ⇒ no side.
  let olderSide = null;
  if (carriedA < carriedB) {
    olderSide = 'A';
  } else if (carriedB < carriedA) {
    olderSide = 'B';
  }
  const staleAdvanced =
    olderSide === 'A' ? aAdvanced : olderSide === 'B' ? bAdvanced : false;
  // Growth/shrink from the CARRIED gap (review delta 5, finding 1), NOT the measured `skew`: `prev.skew`
  // is null after any empty-run gap, so a genuinely GROWN last-known gap would be invisible (the former
  // fail-open) and a shrink could be manufactured. `gapGrew`/`gapShrank` read the carried tsA/tsB gap,
  // which persists through empty gaps, and are null-safe (unknown gap ⇒ neither grew nor shrank).
  const skewIncreased = gapGrew;
  const skewDecreased = gapShrank;
  const bothAdvanced = aAdvanced && bAdvanced;
  const neitherAdvanced = !aAdvanced && !bAdvanced;

  // FAIL iff a one-sided wedge (one frozen, other advancing) OR the skew grew (trickling wedge losing
  // ground). Both fail-closed. A fresh fail sets a new wedge episode (sticky from here). N1: staleSide
  // names the leg that FAILED TO ADVANCE — the frozen one — computed from per-leg advancement, NOT
  // olderSide (which mis-names the newer-but-frozen leg).
  //
  // CONSCIOUS RULING (review delta 5, finding 2 — SUSTAINED as fail-closed): this `frozen` predicate
  // fires even in the "leader frozen while the STALE leg catches up" shape — a sticky wedge can recover
  // in the gate above and then this fresh predicate immediately names the (now-frozen) former leader.
  // At the deployment's HOURLY cadence this is CORRECT, not laundering: a leg whose newest-row timestamp
  // does not advance across a FULL inter-run interval has genuinely stopped persisting for that interval,
  // so N1 names the truly-frozen leg and the recovery-then-fresh-wedge is an honest NEW episode (a fresh
  // wedgeFailedSince). CAVEAT (accepted): back-to-back MANUAL runs seconds apart can false-alarm here —
  // a leg that simply had no new block in those seconds reads "frozen". The timer cadence is hourly and
  // a false FAIL is the SAFE direction for a differ, so this stands; the pinning test locks the exact
  // behavior so any future change to it is a conscious decision.
  const fail = oneLegFrozenWhileOtherAdvanced || skewIncreased;
  let reason;
  let freshStaleSide = null;
  if (fail) {
    if (oneLegFrozenWhileOtherAdvanced) {
      reason = 'frozen';
      // The frozen leg is the one that did NOT advance while the other did.
      freshStaleSide = aAdvanced ? 'B' : 'A';
    } else {
      reason = 'skew-growing';
      // A trickling wedge losing ground: the stale (older) leg is the one falling behind.
      freshStaleSide = olderSide;
    }
  } else if (neitherAdvanced) {
    // NEITHER leg advanced over threshold → a MUTUAL freeze (D3): out of scope for an A-vs-B divergence
    // guard (both legs down is an ops alarm). A leader-regression + frozen-stale pair (skew shrank but
    // nobody advanced) also lands here, NOT 'catching-up' — the next run's advancement check
    // discriminates (a conscious one-run delay).
    reason = 'mutually-quiescent';
  } else if (staleAdvanced && skewDecreased) {
    // The OLDER (stale) leg strictly advanced AND the gap narrowed → genuine recovery in progress.
    reason = 'catching-up';
  } else if (bothAdvanced && !skewDecreased && !skewIncreased) {
    // BOTH legs advanced and the skew held flat → a steady lag, non-fail (the stale leg IS moving so
    // `hi` advances with it; the WINDOWED diff owns that regime — this guard is only for frozen windows).
    reason = 'lagging-constant';
  } else {
    // Remaining non-fail shape: only the LEADER advanced (skew shrank without the stale leg advancing —
    // e.g. the stale leg held still while the leader raced ahead is caught above as 'frozen'; the only
    // way to reach here is skew-shrank-without-stale-advance where the leader's motion narrowed the gap
    // via a leader ts that moved toward the stale one, i.e. a leader regression already routed to
    // 'mutually-quiescent'). Fold to 'mutually-quiescent' (nobody-of-interest advanced) — never a
    // silent mislabel. Kept explicit so no advancement pattern falls through unnamed.
    reason = 'mutually-quiescent';
  }

  return {
    fail,
    staleSide: fail ? freshStaleSide : null,
    reason,
    skew,
    ...base,
    nextState: fail
      ? evidence(nowMs, freshStaleSide, reason)
      : evidence(null, null, null),
  };
}

// The last-known gap between two CARRIED timestamps: |x − y| ONLY when BOTH are finite, else null
// (review delta 5, finding 1). Growth/shrink direction is judged from THIS gap, not the measured
// `skew`, because carried timestamps persist through an empty-run gap where `skew` (and hence
// `prev.skew`) is null. A null return means UNKNOWN — never coerced to 0 — so a comparison against it
// defers rather than fabricating or clearing a wedge.
function gap(x, y) {
  return Number.isFinite(x) && Number.isFinite(y) ? Math.abs(x - y) : null;
}

// One null-safe gap-GREW comparison, used at EVERY growth site (review delta 5, finding 1): returns
// true ONLY when BOTH the prior and current carried gap are finite numbers and the current STRICTLY
// exceeds the prior. A null (unknown) gap on either side is NOT "growing" — this defers rather than
// manufacturing a false skew-growing FAIL (F3) or masking a real one behind a null prev.skew.
function gapGrewFrom(prevGap, curGap) {
  return (
    typeof prevGap === 'number' &&
    Number.isFinite(prevGap) &&
    typeof curGap === 'number' &&
    Number.isFinite(curGap) &&
    curGap > prevGap
  );
}

// The null-safe MIRROR (review delta 5, finding 1): gap strictly SHRANK — true ONLY when BOTH carried
// gaps are finite and the current is strictly below the prior. A null (unknown) gap on either side is
// NOT "shrinking", so a 'catching-up' label is never inferred from an unmeasurable prior gap.
function gapShrankFrom(prevGap, curGap) {
  return (
    typeof prevGap === 'number' &&
    Number.isFinite(prevGap) &&
    typeof curGap === 'number' &&
    Number.isFinite(curGap) &&
    curGap < prevGap
  );
}

// Coerce a newest-row block timestamp to a finite number of seconds, or null when the leg has no rows
// (SQL NULL, empty string, or an unparseable value). Never returns a phantom 0 for a missing row.
function tsToSeconds(ts) {
  if (ts === undefined || ts === null || ts === '') {
    return null;
  }

  const n = Number(ts);

  return Number.isFinite(n) ? n : null;
}

// Coerce a newest-row block number for the counters JSON; null when absent/unparseable.
function maxToNumber(max) {
  if (max === undefined || max === null || max === '') {
    return null;
  }

  const n = Number(max);

  return Number.isFinite(n) ? n : null;
}

// Compare per-bucket checkpoint hashes: shared buckets must match (determinism); buckets on only one
// side are reported (overlap edges), not failed.
export function compareBucketHashes(aBuckets, bBuckets) {
  const a =
    aBuckets instanceof Map ? aBuckets : new Map(Object.entries(aBuckets));
  const b =
    bBuckets instanceof Map ? bBuckets : new Map(Object.entries(bBuckets));
  const mismatches = [];
  for (const [bucket, ha] of a) {
    if (b.has(bucket) && b.get(bucket) !== ha) {
      mismatches.push({ bucket, a: ha, b: b.get(bucket) });
    }
  }
  const onlyA = [...a.keys()].filter((k) => !b.has(k)).length;
  const onlyB = [...b.keys()].filter((k) => !a.has(k)).length;

  return { ok: mismatches.length === 0, mismatches, onlyA, onlyB };
}

// Given a bucket comparison (from compareBucketHashes) AND leg B's per-bucket md5 recomputed with the
// tolerated issue #36 onlyB rows EXCLUDED (bBucketsExTolerated — see bucketHashesExcluding), decide
// which shared-bucket mismatches are FULLY EXPLAINED by the tolerated onlyB rows and which remain hard.
//
// This is EXACT attribution, NOT a count heuristic. A bucket md5 is
// `md5(string_agg(md5(row) order by ...))` over the bucket's rows. A mismatched bucket is "explained"
// IFF A's bucket md5 equals B's bucket md5 recomputed over (B's rows MINUS the tolerated onlyB rows in
// that bucket): equality proves those tolerated rows are the ENTIRE difference between the two sides in
// that bucket — no shared-row field drift, no untolerated onlyB row, no onlyA row hiding underneath.
// This closes the compensating-pair hole a count-delta check leaves open (a bucket with one tolerated
// onlyB row AND one untolerated shared-row mismatch nets to the same row-count but the recomputed md5
// still differs from A's, so it stays FAIL). If a mismatched bucket has NO tolerated onlyB row at all,
// or the recomputed md5 still differs, it is UNEXPLAINED → the run stays FAIL.
//
// Returns { ok, explained: [buckets], unexplained: [{bucket,a,b}] }. ok === true IFF every shared-bucket
// mismatch is explained (unexplained is empty). Pure + exported so each attribution case is mutation-
// verified in isolation without psql.
export function classifyBucketMismatches(compare, bBucketsExTolerated) {
  const exB =
    bBucketsExTolerated instanceof Map
      ? bBucketsExTolerated
      : new Map(Object.entries(bBucketsExTolerated ?? {}));
  const explained = [];
  const unexplained = [];
  for (const m of compare?.mismatches ?? []) {
    // "explained" IFF removing the tolerated onlyB rows from B's bucket makes it byte-identical to A's
    // bucket. exB must HAVE the bucket (a tolerated onlyB row fell in it) AND its recomputed md5 must
    // equal A's md5. A bucket absent from exB had NO tolerated onlyB row removed, so nothing explains
    // its mismatch → unexplained. Requiring the recomputed value to equal A's (not merely to change)
    // is what refuses to explain a bucket whose divergence is (also) something other than the loss.
    if (exB.has(m.bucket) && exB.get(m.bucket) === m.a) {
      explained.push(m.bucket);
    } else {
      unexplained.push(m);
    }
  }

  return { ok: unexplained.length === 0, explained, unexplained };
}

// Restart accounting from the systemd ExecStartPre log (one UTC-timestamp line per (re)start). In
// stream mode a fatal unknown-head / reorg-gap exits 75 and systemd restarts — designed recovery, so
// only the RATE matters: >3 restarts in the trailing hour is a crash-loop alert.
export function restartStats(lines, nowMs = Date.now()) {
  const stamps = [];
  for (const line of lines) {
    const m = line.trim().match(/^(\d{4}-\d\d-\d\dT[\d:.]+Z)/);
    if (m) {
      stamps.push(Date.parse(m[1]));
    }
  }
  stamps.sort((x, y) => x - y);

  const hourAgo = nowMs - 3_600_000;
  const restartsLastHour = stamps.filter((t) => t >= hourAgo).length;
  const lastRestartAt =
    stamps.length > 0
      ? new Date(stamps[stamps.length - 1]).toISOString()
      : null;

  return {
    restartCount: stamps.length,
    lastRestartAt,
    restartsLastHour,
    crashLoop: restartsLastHour > 3,
  };
}

// ── psql adapters ──────────────────────────────────────────────────────────────────────────────

const SEP = '\x1f'; // unit separator — never appears in the hex/numeric fields we select

// Stream rows of a query as string[] (fields split on SEP), constant memory. `-v ON_ERROR_STOP=1`
// makes a mid-query server error a non-zero exit rather than a partial stream; we additionally await
// the child's terminal state and throw on any non-clean exit (spawn error / signal / non-zero code)
// so a failed query can NEVER be silently read as "zero rows" (the false-PASS class).
async function* psqlRows(url, sql) {
  const proc = spawn(
    'psql',
    [
      url,
      '-X',
      '-q',
      '-A',
      '-t',
      '-v',
      'ON_ERROR_STOP=1',
      '-F',
      SEP,
      '-c',
      sql,
    ],
    {
      stdio: ['ignore', 'pipe', 'inherit'],
    },
  );

  let spawnError = null;
  proc.on('error', (e) => {
    spawnError = e.message;
  });

  const exited = new Promise((resolve) => {
    proc.on('close', (code, signal) => {
      resolve({ code, signal });
    });
  });

  let buf = '';
  for await (const chunk of proc.stdout) {
    buf += chunk.toString('utf8');
    let nl = buf.indexOf('\n');
    while (nl >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (line.length > 0) {
        yield line.split(SEP);
      }

      nl = buf.indexOf('\n');
    }
  }

  if (buf.trim().length > 0) {
    yield buf.split(SEP);
  }

  const { code, signal } = await exited;
  const verdict = psqlExitVerdict({ code, signal, spawnError });
  if (!verdict.ok) {
    throw new Error(verdict.reason);
  }
}

async function psqlScalar(url, sql) {
  for await (const row of psqlRows(url, sql)) {
    return row[0];
  }

  return null;
}

async function psqlList(url, sql) {
  const out = [];
  for await (const row of psqlRows(url, sql)) {
    out.push(row[0]);
  }

  return out;
}

// Turn a keyed hash stream ({key:[...], hash}) into rows streamingDiff can compare: the hash is the
// only field, so a field mismatch on a shared key surfaces as a row mismatch.
function hashRowsIter(url, sql, keyIdx) {
  return (async function* () {
    for await (const row of psqlRows(url, sql)) {
      yield {
        key: keyIdx.map((i) => BigInt(row[i])),
        hash: row[row.length - 1],
      };
    }
  })();
}

const keyFn = (r) => r.key;

// ── per-chain comparison ─────────────────────────────────────────────────────────────────────

async function overlapBound(url, chain) {
  const v = await psqlScalar(
    url,
    `select coalesce(max(number),0) from ponder_sync.blocks where chain_id=${chain}`,
  );

  return Number(v ?? 0);
}

// Newest persisted row for the chain: its block NUMBER and block TIMESTAMP (Unix seconds). A SINGLE-ROW
// `order by number desc limit 1` (NOT two independent max() aggregates) so the number and timestamp
// come from the SAME row — the actual newest block, its true height AND its true time, never a max
// height paired with a max timestamp from a different row (finding 7). An EMPTY store yields ZERO rows
// → both null, letting stagnationDecision tell "no rows" apart from a legitimate block/timestamp of 0
// (never a phantom 0). ponder_sync.blocks persists blocks in on-chain order, so the highest-number row
// is the newest block. Returns { maxBlock, ts } as strings|null (the pure decision helper coerces
// them). Used ONLY by the issue #38 persist-stagnation guard; the window hi still comes from
// overlapBound's coalesce(...,0), unchanged.
async function legNewestRow(url, chain) {
  let row = null;
  for await (const r of psqlRows(
    url,
    `select number::text, timestamp::text ` +
      `from ponder_sync.blocks where chain_id=${chain} order by number desc limit 1`,
  )) {
    row = r;
    break;
  }

  return {
    maxBlock: row?.[0] ?? null,
    ts: row?.[1] ?? null,
  };
}

// Real indexing progress for the monotonicity guard: ponder's own `_ponder_checkpoint` row (in the
// APP schema, not ponder_sync) is the source of truth for how far indexing has committed. The encoded
// `latest_checkpoint` is a fixed-width string whose blockNumber field sits at offsets [26,42) (see the
// verified layout above) — we extract THAT field as a plain block height. max(ponder_sync.blocks.number)
// is only how far the SYNC store reached, which can move independently of committed indexing progress;
// a resume that rewound the COMMITTED checkpoint but kept sync-store rows would slip past a block-max
// guard. Falls back to the sync-store block max only if `_ponder_checkpoint` is absent/unreadable
// (older store, or wrong schema) so the guard is never silently disabled.
async function checkpointProgress(url, chain, schema) {
  // `_ponder_checkpoint` lives in Soak B's APP schema (ponder start --schema <schema>), which is NOT
  // on psql's default search_path — so we QUALIFY the table with the (sanitized) schema. Empty schema
  // ⇒ unqualified (legacy default-search_path stores). One query returns TWO values: the ROW COUNT for
  // this chain and the blockNumber SUBSTRING of the encoded checkpoint (position CHECKPOINT_BLOCK_SQL_POS,
  // length CHECKPOINT_BLOCK_LEN) cast to numeric — a Number-safe height on the SAME scale as the
  // overlapBound fallback, so the monotonicity series stays coherent even when a run flaps between the
  // two sources.
  //
  // Why BOTH values: a bare `coalesce(max(...),0)` cannot tell a chain with NO checkpoint row (max is
  // NULL → 0) apart from a VALID checkpoint whose blockNumber field is exactly 0 (a resume that rewound
  // the committed checkpoint to block 0 while the sync store still holds a high block max). The zero-max
  // rewind is the STRONGEST form of the rewind this guard exists to catch — the row count distinguishes
  // it (checkpointDecision): rows present ⇒ report the value even if 0, so monotonicity FAILs it; zero
  // rows ⇒ fall back.
  const table = schema
    ? `"${schema}"._ponder_checkpoint`
    : '_ponder_checkpoint';
  try {
    let row = null;
    for await (const r of psqlRows(
      url,
      `select count(*)::text, ` +
        `coalesce(max(substring(latest_checkpoint from ${CHECKPOINT_BLOCK_SQL_POS} for ${CHECKPOINT_BLOCK_LEN})::numeric),0)::text ` +
        `from ${table} where chain_id=${chain}`,
    )) {
      row = r;
      break;
    }

    const rowCount = Number(row?.[0] ?? 0);
    const maxBlock = row?.[1] ?? '0';
    const decision = checkpointDecision(rowCount, maxBlock);
    if (decision.usable) {
      return decision.value;
    }
    // rows === 0 for this chain: NO committed checkpoint (older store, or the chain never committed).
    // We fall back to the sync-store block max below rather than disabling the guard. CANDID RESIDUAL:
    // an APP-schema FULL RESET that leaves ZERO checkpoint rows (e.g. the table truncated/re-created)
    // is INDISTINGUISHABLE here from a store that never had a checkpoint — both read rows===0, so we
    // fall back to the (still-high) sync-store block max and monotonicity can PASS silently, masking
    // that rewind. This is the guard's remaining blind spot; detecting a zero-ROW reset needs
    // cross-run source tracking (was there a checkpoint row last run that is gone now?) — out of scope.
  } catch {
    // _ponder_checkpoint not resolvable (older store / different schema) — fall through to the
    // sync-store block max below rather than disabling the guard.
  }

  return overlapBound(url, chain);
}

// A B-only row's key is [block_number] (blocks) or [block_number, log_index] (logs) — the first key
// component is ALWAYS the block number. Collect the onlyB rows the streamingDiff callback streams so the
// issue #36 floor-gate can classify each by block number. The rows are held in an array, so this is
// bounded by the onlyB count — small by construction for the tolerated class (issue #36 measured 89 log
// + 18 block rows) and, if it ever exploded past ONLYB_ROW_CAP, that is a NEW divergence shape that must
// FAIL LOUD anyway, never a silently-tolerated one; the cap keeps memory bounded while flipping to a
// hard fail. `cap` is injectable (default ONLYB_ROW_CAP) so the capped→FAIL path is testable WITHOUT
// lowering the production constant. Exported so the collector wiring is driven directly in a test.
// Returns { onlyBRows: [{ blockNumber, logIndex? }], capped: () => boolean, onOnlyB }.
export const ONLYB_ROW_CAP = 100_000;

export function collectOnlyB(cap = ONLYB_ROW_CAP) {
  const onlyBRows = [];
  let capped = false;
  const onOnlyB = (row) => {
    if (onlyBRows.length >= cap) {
      capped = true;

      return;
    }

    const key = row.key ?? [];
    onlyBRows.push({
      blockNumber: Number(key[0]),
      logIndex: key.length > 1 ? Number(key[1]) : undefined,
    });
  };

  return { onlyBRows, capped: () => capped, onOnlyB };
}

async function diffLogs(urlA, urlB, chain, lo, hi) {
  const sql =
    `select block_number, log_index, md5(to_jsonb(t)::text) from ponder_sync.logs t ` +
    `where chain_id=${chain} and block_number between ${lo} and ${hi} order by block_number, log_index`;
  const collector = collectOnlyB();
  const diff = await streamingDiff(
    hashRowsIter(urlA, sql, [0, 1]),
    hashRowsIter(urlB, sql, [0, 1]),
    {
      keyFn,
      mode: 'strict',
      onOnlyB: collector.onOnlyB,
    },
  );

  return { diff, onlyBRows: collector.onlyBRows, capped: collector.capped() };
}

async function diffBlocks(urlA, urlB, chain, lo, hi) {
  // total_difficulty is excluded in SQL (meaningless post-Merge, RPC-dependent). In the FINALIZED
  // overlap [cutover, min(finalizedA,finalizedB)-margin] both realtime paths must have every block:
  // a one-sided block here is a real gap (a block one store missed), so this is STRICT, not the
  // 'blocks' tolerance harness/diff/diff.mjs uses for the stock RPC path's inert event-less blocks
  // — those never appear in the realtime A/B stores, which only persist event-bearing blocks.
  const sql = () =>
    `select number, md5((to_jsonb(t)-'total_difficulty')::text) from ponder_sync.blocks t ` +
    `where chain_id=${chain} and number between ${lo} and ${hi} order by number`;
  const collector = collectOnlyB();
  const diff = await streamingDiff(
    hashRowsIter(urlA, sql(), [0]),
    hashRowsIter(urlB, sql(), [0]),
    {
      keyFn,
      mode: 'strict',
      onOnlyB: collector.onOnlyB,
    },
  );

  return { diff, onlyBRows: collector.onlyBRows, capped: collector.capped() };
}

// The tx-stream SELECT column contract, in ORDER. This is the SINGLE source of truth binding the SQL
// projection to the positional args diffTx feeds classifySharedTx: the array index here IS the row[]
// index consumed downstream. Reordering the SELECT without reordering these — or vice versa — silently
// feeds classifySharedTx the wrong column (e.g. block_number read as the hash, the null-flag read as an
// md5), corrupting the tolerance classification with no error. The tripwire test asserts buildTxSql's
// SELECT list matches this order AND that these indices equal diffTx's destructuring contract, so any
// permutation of either fails a cheap unit test instead of surfacing as a mis-tolerated production row.
//   0 hash            → classifySharedTx `hash`          + the merge/onlyA/onlyB key
//   1 fullRowMd5      → the shared-tx byte-identity check (a.value[1] !== b.value[1])
//   2 blockNumber     → classifySharedTx `blockNumber`   (issue #27 floor/window)
//   3 exAccessListMd5 → classifySharedTx `exAlMd5A/B`    (md5 over `to_jsonb(t)-'access_list'`)
//   4 accessListNull  → classifySharedTx `aAccessListNull/bAccessListNull` (psql 't'/'f' flag)
export const TX_SELECT_COLUMNS = [
  { name: 'hash', sql: '"hash"' },
  { name: 'fullRowMd5', sql: 'md5(to_jsonb(t)::text)' },
  { name: 'blockNumber', sql: 'block_number' },
  { name: 'exAccessListMd5', sql: "md5((to_jsonb(t)-'access_list')::text)" },
  { name: 'accessListNull', sql: '(access_list is null)' },
];

// Positional row[] indices classifySharedTx's args are destructured from — the contract diffTx and the
// tripwire test both bind to, so the two can never drift apart unnoticed. Each MUST equal the
// TX_SELECT_COLUMNS position of the same field.
export const TX_COL = {
  hash: 0,
  fullRowMd5: 1,
  blockNumber: 2,
  exAccessListMd5: 3,
  accessListNull: 4,
};

// Build the per-side per-tx query from TX_SELECT_COLUMNS (the ONLY place the projection is spelled), so
// the column ORDER can never drift from the destructuring contract without the tripwire test catching
// it. Pure over (chain, lo, hi) + exported: the test asserts the SELECT list, in order, without psql.
// Per side per tx: hash, full-row md5, block_number, ex-access_list md5 (the jsonb-minus idiom used for
// total_difficulty in diffBlocks), and (access_list IS NULL) as psql's t/f flag. The extra columns are
// what classifySharedTx needs to tell the tolerated issue #27 shape apart from any real divergence
// WITHOUT loosening the strict full-row identity for every other column.
export function buildTxSql(chain, lo, hi) {
  const projection = TX_SELECT_COLUMNS.map((c) => c.sql).join(', ');

  return (
    `select ${projection} ` +
    `from ponder_sync.transactions t ` +
    `where chain_id=${chain} and block_number between ${lo} and ${hi} order by "hash"`
  );
}

// tx class: merge the two tx streams (each row = [hash, md5(full row)]) to onlyA/onlyB AND to a
// SHARED set whose row hashes must be byte-identical, then ask A which onlyA hashes a log references.
// A tx present on both sides is the SAME finalized transaction indexed two ways — every field must
// match. The full-row md5 mirrors the strict transactions identity diff-batched.mjs already asserts:
// no column is excluded (unlike blocks' total_difficulty, no transactions column legitimately differs
// between the RPC-sourced A store and the Portal-sourced B store — both write the same ponder_sync
// schema from the same finalized chain data).
async function diffTx(urlA, urlB, chain, lo, hi) {
  const txSql = buildTxSql(chain, lo, hi);
  const onlyA = [];
  const onlyB = [];
  // onlyB txs carried WITH their block number (issue #36 tx facet): the wholly-A-absent-block tolerance
  // is applied in compareChain once the blocks-onlyB set is known, so diffTx must surface each onlyB
  // tx's block, not just its hash. `onlyB` (bare hashes) stays the input to classifyTxDiff unchanged.
  const onlyBTxRows = [];
  let sharedMismatch = 0;
  const toleratedIssue27 = { count: 0, perChain: {} };
  // perHash carries the EXACT pin identity a chain-only tally loses: which pinned hash fired, so
  // aggregateKnownBadRows can mark a specific configured pin unmatched (its (hash, chain) fired 0
  // times) rather than treating the whole chain as matched. perChain stays for the human breakdown.
  const knownBadRowsTally = { count: 0, perChain: {}, perHash: {} };
  const ia = psqlRows(urlA, txSql)[Symbol.asyncIterator]();
  const ib = psqlRows(urlB, txSql)[Symbol.asyncIterator]();
  let a = await ia.next();
  let b = await ib.next();
  while (!a.done || !b.done) {
    const ha = a.done ? null : a.value[TX_COL.hash];
    const hb = b.done ? null : b.value[TX_COL.hash];
    if (hb === null || (ha !== null && ha < hb)) {
      onlyA.push(ha);
      a = await ia.next();
    } else if (ha === null || hb < ha) {
      onlyB.push(hb);
      onlyBTxRows.push({ hash: hb, blockNumber: b.value[TX_COL.blockNumber] });
      b = await ib.next();
    } else {
      // shared tx (same hash on both sides) — the full-row md5 must be identical, EXCEPT for the one
      // fully-root-caused, reported tolerated class (issue #27); anything else is a hard mismatch.
      if (a.value[TX_COL.fullRowMd5] !== b.value[TX_COL.fullRowMd5]) {
        const verdict = classifySharedTx(
          {
            hash: a.value[TX_COL.hash],
            blockNumber: a.value[TX_COL.blockNumber],
            exAlMd5A: a.value[TX_COL.exAccessListMd5],
            exAlMd5B: b.value[TX_COL.exAccessListMd5],
            aAccessListNull: a.value[TX_COL.accessListNull] === 't',
            bAccessListNull: b.value[TX_COL.accessListNull] === 't',
          },
          chain,
        );
        if (verdict === 'tolerated') {
          toleratedIssue27.count += 1;
          toleratedIssue27.perChain[chain] =
            (toleratedIssue27.perChain[chain] ?? 0) + 1;
        } else if (verdict === 'knownBadRow') {
          knownBadRowsTally.count += 1;
          knownBadRowsTally.perChain[chain] =
            (knownBadRowsTally.perChain[chain] ?? 0) + 1;
          // Tally per HASH too so a specific pin's fate is exact (not merely "some pin on this chain
          // fired"). Keyed by the shared tx hash — identical on both sides for a knownBadRow.
          const firedHash = a.value[TX_COL.hash];
          knownBadRowsTally.perHash[firedHash] =
            (knownBadRowsTally.perHash[firedHash] ?? 0) + 1;
        } else {
          sharedMismatch += 1;
        }
      }

      a = await ia.next();
      b = await ib.next();
    }
  }

  // Which onlyA hashes are referenced by an A-side log (a legit realtime parent-tx gap)? Verify EVERY
  // onlyA hash regardless of count by chunking the IN list into bounded queries (no fixed threshold):
  // the old `onlyA.length <= 200_000` skip left `referenced=[]` on a large healthy soak, so
  // classifyTxDiff saw every gap as unreferenced and mass-FAILed a healthy run. collectReferenced
  // accumulates across every chunk so any onlyA size is fully verified.
  const referenced = await collectReferenced(onlyA, (batch) => {
    const inList = batch.map((h) => `'${h.replace(/'/g, '')}'`).join(',');

    return psqlList(
      urlA,
      `select distinct transaction_hash from ponder_sync.logs where chain_id=${chain} and transaction_hash in (${inList})`,
    );
  });

  const txClass = classifyTxDiff(
    onlyA,
    onlyB,
    referenced,
    sharedMismatch,
    toleratedIssue27,
    knownBadRowsTally,
  );

  // Carry the onlyB tx rows (with block numbers) so compareChain can fold the issue #36 tx tolerance
  // once it holds the blocks-onlyB (wholly-A-absent) set — the same compose-in-compareChain pattern the
  // logs/blocks onlyB tolerance uses. classifyTxDiff's own verdict here is the PRE-tolerance view.
  return { ...txClass, onlyBTxRows };
}

async function bucketHashes(url, chain, lo, hi, bucket) {
  const sql =
    `select (block_number/${bucket})::text, md5(string_agg(md5(to_jsonb(t)::text), ',' order by block_number, log_index)) ` +
    `from ponder_sync.logs t where chain_id=${chain} and block_number between ${lo} and ${hi} group by 1 order by 1`;
  const map = new Map();
  for await (const row of psqlRows(url, sql)) {
    map.set(row[0], row[1]);
  }

  return map;
}

// A SQL predicate excluding a set of specific log rows by their (block_number, log_index) identity, for
// recomputing leg B's per-bucket md5 with the tolerated issue #36 onlyB rows removed. `rows` are the
// tolerated onlyB LOG rows ({ blockNumber, logIndex }); block-table onlyB rows have no log_index and are
// irrelevant to the LOGS bucket hashes, so they are dropped. Emits `and not ((block_number, log_index)
// in ((b0,i0),(b1,i1),…))`. Empty ⇒ '' (no exclusion). Numbers are coerced via Number() and formatted
// as integers, so no string injection is possible from these internally-derived keys. Pure + exported
// so the exclusion contract is asserted without psql. The bucket-explained attribution in
// classifyBucketMismatches depends on this excluding EXACTLY the tolerated rows and nothing else — an
// over- or under-exclusion here would mis-attribute a bucket, so its shape is mutation-verified.
export function buildBucketExclusionFilter(rows) {
  const pairs = [];
  for (const r of rows ?? []) {
    const b = Number(r.blockNumber);
    const i = Number(r.logIndex);
    if (!Number.isFinite(b) || !Number.isFinite(i)) {
      continue;
    }

    pairs.push(`(${b}, ${i})`);
  }
  if (pairs.length === 0) {
    return '';
  }

  return ` and not ((block_number, log_index) in (${pairs.join(', ')}))`;
}

// Recompute leg B's per-bucket LOG md5 with the tolerated issue #36 onlyB rows EXCLUDED, so
// classifyBucketMismatches can test whether each mismatched bucket's ENTIRE divergence is those tolerated
// rows (A's md5 === B-minus-tolerated md5) and nothing else. Same query as bucketHashes but with the
// exclusion predicate. Only called when there ARE mismatched buckets AND tolerated onlyB log rows exist,
// so the extra query is off the healthy path.
//
// SEPARATE-SNAPSHOT RESIDUAL (non-blocking, deliberately out of scope): this recompute and the earlier
// bucketHashes/diff queries are DISTINCT psql statements, each its own MVCC snapshot — leg B's rows
// could in principle change between them, so the bucket-md5 attribution is not read from ONE frozen
// view. The mitigating reality: the differ compares ONLY the FINALIZED overlap [cutover,
// min(finalizedA,finalizedB)-margin], where both legs' rows are STABLE between queries in the same run —
// finalized data is append-only and not rewritten — EXCEPT during an active repair (a re-ingest that
// rewrites finalized rows). So outside a repair the race window is practically empty, and a repair
// in-flight would surface as a transient bucket/row mismatch that clears on the next hourly run rather
// than a false PASS. Folding every per-side query into a single repeatable-read snapshot would close the
// window fully but is a larger refactor (one long-lived connection/transaction per side, threaded
// through every adapter) and is intentionally NOT done here.
async function bucketHashesExcluding(url, chain, lo, hi, bucket, excludeRows) {
  const exclusion = buildBucketExclusionFilter(excludeRows);
  const sql =
    `select (block_number/${bucket})::text, md5(string_agg(md5(to_jsonb(t)::text), ',' order by block_number, log_index)) ` +
    `from ponder_sync.logs t where chain_id=${chain} and block_number between ${lo} and ${hi}${exclusion} group by 1 order by 1`;
  const map = new Map();
  for await (const row of psqlRows(url, sql)) {
    map.set(row[0], row[1]);
  }

  return map;
}

// The real module functions compareChain calls, gathered into one deps object so a test can drive the
// REAL compareChain with stubs (D3 wiring coverage). The production call sites pass nothing, so `deps`
// defaults to these and behaviour is unchanged; a test overrides only the seams it needs. Every DB-
// touching adapter compareChain uses is here — swapping them for in-memory stubs makes compareChain's
// verdict composition (OR-of-stagnation-and-windowed) testable without a psql.
export const COMPARE_CHAIN_DEPS = {
  overlapBound,
  checkpointProgress,
  legNewestRow,
  diffLogs,
  diffBlocks,
  diffTx,
  bucketHashes,
  bucketHashesExcluding,
};

export async function compareChain(
  urlA,
  urlB,
  chain,
  cutover,
  margin,
  bucket,
  schemaB,
  stagnationThreshold,
  // Cross-run stagnation state for THIS chain from the prior run (or null); wall clock; deps seam.
  prevStagnation = null,
  nowMs = Date.now(),
  deps = COMPARE_CHAIN_DEPS,
) {
  const [boundA, boundB, progressB, newestA, newestB] = await Promise.all([
    deps.overlapBound(urlA, chain),
    deps.overlapBound(urlB, chain),
    // Soak B's COMMITTED indexing progress from `_ponder_checkpoint` — the real value the
    // monotonicity guard asserts, not merely how far the sync store reached (see checkpointProgress).
    deps.checkpointProgress(urlB, chain, schemaB),
    // Newest persisted (block, timestamp) per leg — for the issue #38 persist-stagnation guard.
    deps.legNewestRow(urlA, chain),
    deps.legNewestRow(urlB, chain),
  ]);
  const hi = Math.min(boundA, boundB) - margin;
  const lo = cutover;
  const out = {
    chain,
    lo,
    hi,
    lagA: 0,
    lagB: 0,
    // Soak B's committed indexing checkpoint — the per-run progress value monotonicity is asserted
    // against (never rewinds across hourly runs).
    progressB,
    verdict: 'PASS',
    classes: {},
  };
  out.lagA = boundA - Math.min(boundA, boundB);
  out.lagB = boundB - Math.min(boundA, boundB);

  // Persist-stagnation guard (issue #38). Computed alongside the window so it fires regardless of
  // window state: a one-sided wedge is exactly the state where `hi` can be frozen or below `lo`, and
  // the whole point is that this FAILs when the WINDOWED diff would read PASS/PENDING. It takes the
  // prior run's per-chain state (direction-aware decision) + the wall clock (empty-leg timing). maxA/
  // maxB and both newest-row timestamps are surfaced in the per-chain classes (and thus the counters
  // JSON) so a frozen window is legible at a glance even below threshold.
  const stagnation = stagnationDecision({
    maxA: newestA.maxBlock,
    tsA: newestA.ts,
    maxB: newestB.maxBlock,
    tsB: newestB.ts,
    threshold: stagnationThreshold,
    prev: prevStagnation,
    nowMs,
  });
  const persistStagnation = {
    fail: stagnation.fail,
    reason: stagnation.reason,
    // The original wedge reason carried through a sticky 'wedge-unrecovered' fail (D2), so the alert
    // layer can render what the wedge WAS. Absent for a fresh (non-sticky) reason.
    originalReason: stagnation.originalReason,
    staleSide: stagnation.staleSide,
    skewSeconds: stagnation.skew,
    thresholdSeconds: stagnationThreshold,
    maxA: stagnation.maxA,
    maxB: stagnation.maxB,
    tsA: stagnation.tsA,
    tsB: stagnation.tsB,
  };
  out.classes.persistStagnation = persistStagnation;
  // The state to persist for this chain for the next run (loaded/saved via CHECKPOINT_FILE by main).
  out.stagnationState = stagnation.nextState;

  if (hi < lo) {
    // No finalized overlap yet — the windowed diff cannot run. This is PENDING on the window axis, but
    // the stagnation guard still stands: OR-compose so a fired guard FAILs even here (a one-sided wedge
    // is precisely a state that can hold hi below lo). classes.persistStagnation is already attached.
    out.verdict = stagnation.fail ? 'FAIL' : 'PENDING';
    out.classes.note = `no finalized overlap yet (lo=${lo} hi=${hi})`;

    return out;
  }

  const [logsRes, blocksRes, tx] = await Promise.all([
    deps.diffLogs(urlA, urlB, chain, lo, hi),
    deps.diffBlocks(urlA, urlB, chain, lo, hi),
    deps.diffTx(urlA, urlB, chain, lo, hi),
  ]);
  const [ba, bb] = await Promise.all([
    deps.bucketHashes(urlA, chain, lo, hi, bucket),
    deps.bucketHashes(urlB, chain, lo, hi, bucket),
  ]);
  const buckets = compareBucketHashes(ba, bb);

  // Reclassify each table's onlyB rows against the tolerated issue #36 class. A row-loss on leg A
  // (onlyB) at/above the per-chain realtime-era floor is tolerated + counted; any onlyA, shared
  // mismatch, below-floor onlyB, or unknown-chain onlyB stays a HARD FAIL. A capped onlyB stream (an
  // onlyB set larger than ONLYB_ROW_CAP — a NEW, much bigger divergence shape) is a hard FAIL: we did
  // NOT classify every row, so we must not tolerate the class.
  const logsClass = classifyOnlyBDiff(logsRes.diff, logsRes.onlyBRows, chain);
  const blocksClass = classifyOnlyBDiff(
    blocksRes.diff,
    blocksRes.onlyBRows,
    chain,
  );
  // BACKSTOP: the tolerance verdict above trusts logsRes.onlyBRows / blocksRes.onlyBRows to be EVERY
  // onlyB row the diff counted. Cross-check that collected array against the diff's own onlyB COUNT: a
  // silent hook-wiring drop (some onlyB row never reached the collector) would leave classifyOnlyBDiff
  // deciding on an INCOMPLETE set — so a mismatch (when NOT capped) is a HARD FAIL that names itself.
  const logsCollector = crossCheckOnlyBCollector(
    logsRes.diff.onlyB,
    logsRes.onlyBRows.length,
    logsRes.capped,
  );
  const blocksCollector = crossCheckOnlyBCollector(
    blocksRes.diff.onlyB,
    blocksRes.onlyBRows.length,
    blocksRes.capped,
  );
  const logsFail = logsClass.fail || logsRes.capped || !logsCollector.ok;
  const blocksFail =
    blocksClass.fail || blocksRes.capped || !blocksCollector.ok;

  // Fold the issue #36 onlyB-TRANSACTION tolerance (12:22Z hourly cross-validation: 30 B-only chain-1
  // txs, ALL in blocks WHOLLY ABSENT from leg A — the same leg-A loss the logs/blocks class already
  // tolerates, one table deeper). An onlyB tx is tolerated IFF its block is wholly absent from leg A AND
  // at/above the per-chain floor (see classifyOnlyBTx); any other onlyB tx (block EXISTS in A → a
  // tx-level-only loss, a genuinely NEW divergence class) stays a HARD FAIL.
  //
  // The wholly-A-absent block set is the blocks-onlyB set (blocks present in B, absent in A). FAIL-CLOSED
  // when that evidence is incomplete: if the blocks onlyB stream was CAPPED (blocksRes.capped) OR its
  // collector cross-check failed (!blocksCollector.ok), the absent set is not authoritative, so we pass
  // an EMPTY set and tolerate NO onlyB tx — an untrustworthy evidence source must never widen tolerance.
  // (Same separate-snapshot residual as the logs/blocks tolerance — the finalized overlap is append-only
  // and stable between the blocks and tx queries outside an active repair; a repair in flight surfaces as
  // a transient that clears next run, never a false PASS. See bucketHashesExcluding's SEPARATE-SNAPSHOT note.)
  const absentBlocks =
    blocksRes.capped || !blocksCollector.ok
      ? new Set()
      : new Set(blocksRes.onlyBRows.map((r) => Number(r.blockNumber)));
  const txClass = classifyOnlyBTxDiff(tx, tx.onlyBTxRows, chain, absentBlocks);

  // The tolerated onlyB LOG rows (block-table onlyB rows have no log_index and do not affect the log
  // bucket hashes) — the set to remove from leg B's buckets when attributing a bucket mismatch. Only
  // rows the class actually TOLERATED are removed; an untolerated onlyB row is NOT removed, so a bucket
  // containing one stays unexplained → FAIL.
  const toleratedLogRows = logsRes.onlyBRows.filter(
    (r) => classifyOnlyBRow(r, chain) === 'tolerated',
  );
  // The tolerated onlyB BLOCK rows (issue #36) — used only for the bounded spot-audit sample in the
  // status JSON (block-table rows have no log_index and never enter the bucket-hash attribution).
  const toleratedBlockRows = blocksRes.onlyBRows.filter(
    (r) => classifyOnlyBRow(r, chain) === 'tolerated',
  );

  // checkpointBuckets knock-on: a bucket md5 mismatch FULLY explained by the tolerated onlyB log rows
  // must not fail the verdict; any bucket mismatch NOT fully explained stays FAIL. Attribution is EXACT
  // (recompute leg B's bucket md5 with the tolerated rows removed and require it to equal leg A's md5),
  // never a count heuristic — see classifyBucketMismatches. Only run the recompute when there ARE
  // mismatched buckets AND tolerated log rows to remove; otherwise the mismatches are unexplained as-is.
  let bucketClass = {
    ok: buckets.ok,
    explained: [],
    unexplained: buckets.mismatches,
  };
  if (!buckets.ok && toleratedLogRows.length > 0) {
    const bbExcl = await deps.bucketHashesExcluding(
      urlB,
      chain,
      lo,
      hi,
      bucket,
      toleratedLogRows,
    );
    bucketClass = classifyBucketMismatches(buckets, bbExcl);
  }
  const bucketsFail = !bucketClass.ok;

  out.classes = {
    // Carried through the window path too (issue #38): a PASSing windowed diff still surfaces the
    // per-chain persist-stagnation counters so a below-threshold one-sided skew (a frozen window that
    // has not yet crossed the guard) is legible at a glance in the status JSON.
    persistStagnation,
    logs: {
      fail: logsFail,
      onlyA: logsRes.diff.onlyA,
      onlyB: logsRes.diff.onlyB,
      mismatch: logsRes.diff.mismatch,
      shared: logsRes.diff.shared,
      // Reported, never fails: onlyB log rows leg A lost, tolerated per issue #36 (at/above the
      // per-chain realtime-era floor). Counted so the verdict is PASS-compatible while still visible.
      toleratedIssue36: logsClass.toleratedOnlyB,
      capped: logsRes.capped,
      // A bounded, spot-auditable sample of the tolerated onlyB block numbers (issue #36) — so a human
      // or script can cross-check leg B's tolerated rows against a third-party node WITHOUT psql access
      // to the raw diff (the ONLY control that distinguishes leg-A loss from a hypothetical leg-B
      // fabrication of the same shape — see TOLERATED_ONLYB_CLASSES).
      toleratedIssue36Sample: sampleToleratedOnlyB(toleratedLogRows),
      // Present ONLY when the backstop fired — a silent onlyB collector drop names itself here so the
      // HARD FAIL cannot be mistaken for anything else.
      ...(logsCollector.ok
        ? {}
        : { collectorMismatch: logsCollector.collectorMismatch }),
    },
    blocks: {
      fail: blocksFail,
      onlyA: blocksRes.diff.onlyA,
      onlyB: blocksRes.diff.onlyB,
      mismatch: blocksRes.diff.mismatch,
      // Reported, never fails: onlyB block rows leg A lost, tolerated per issue #36.
      toleratedIssue36: blocksClass.toleratedOnlyB,
      capped: blocksRes.capped,
      // Bounded spot-audit sample of the tolerated onlyB block numbers (issue #36), as for logs above.
      toleratedIssue36Sample: sampleToleratedOnlyB(toleratedBlockRows),
      ...(blocksCollector.ok
        ? {}
        : { collectorMismatch: blocksCollector.collectorMismatch }),
    },
    // `onlyBTxRows` is diffTx's internal carrier for the tolerance fold above — stripped here so the
    // status JSON stays bounded (only the tolerated COUNT/perChain surfaces, never the raw row list).
    transactions: stripTxOnlyBRows(txClass),
    checkpointBuckets: {
      ok: bucketClass.ok,
      mismatches: buckets.mismatches.length,
      // How many mismatched buckets were fully explained by tolerated issue #36 onlyB rows, and how
      // many remain hard — so the attribution is VISIBLE in the status JSON, never silent.
      explainedByIssue36: bucketClass.explained.length,
      unexplained: bucketClass.unexplained.length,
    },
  };
  // OR-composition of the verdict (D1 — no early return on a fired guard): the FULL windowed diff runs
  // even when the stagnation guard fires, so its tolerated classes / pins / bucket hashes are all still
  // computed and reported. A fired guard FORCES FAIL (stagnation FAIL wins over a windowed PASS/PENDING)
  // but NEVER suppresses a windowed failure's own reporting — a stagnation-only FAIL and a windowed FAIL
  // compose to one FAIL, each visible in its own class. classes.persistStagnation is attached in every
  // path above.
  if (
    logsFail ||
    blocksFail ||
    txClass.fail ||
    bucketsFail ||
    stagnation.fail
  ) {
    out.verdict = 'FAIL';
  }

  return out;
}

async function main() {
  const urlA = process.env.DATABASE_URL_A;
  const urlB = process.env.DATABASE_URL_B;
  if (!urlA || !urlB) {
    console.error('ab-diff: set DATABASE_URL_A and DATABASE_URL_B');
    process.exit(2);
  }

  const chains = (process.env.CHAINS ?? '1,8453,42161')
    .split(',')
    .map((s) => Number(s.trim()));
  const cutover = Number(process.env.CUTOVER ?? 0);
  const margin = Number(process.env.FINALITY_MARGIN ?? 64);
  const bucket = Number(process.env.BUCKET ?? 1000);
  // Schema Soak B's `_ponder_checkpoint` lives in (its DATABASE_SCHEMA / `ponder start --schema`
  // value). Unset ⇒ unqualified (default search_path). Sanitized — it goes into SQL. A bad identifier
  // fails loud here (before any per-chain query) rather than silently disabling the checkpoint guard.
  const schemaB = sanitizeSchemaIdent(process.env.AB_SCHEMA_B ?? '');
  // Max tolerated one-sided persist skew (issue #38), in seconds. Garbage/non-positive fails loud
  // here (before any per-chain query), same as AB_SCHEMA_B above — a config guard never silently
  // disables itself. Unset ⇒ the documented 2h default.
  const stagnationThreshold = readStagnationThreshold(
    process.env.AB_STAGNATION_MAX_SKEW_S,
  );
  const statusFile = process.env.STATUS_FILE ?? 'soak-ab-status.json';
  const checkpointFile =
    process.env.CHECKPOINT_FILE ?? 'soak-ab-checkpoints.json';

  // The CHECKPOINT_FILE is the cross-run ledger: it holds the per-chain checkpoint monotonicity series
  // AND (new, issue #38) a per-chain persist-stagnation state section under the `_stagnation` top-level
  // key. An ABSENT `_stagnation` key ⇒ no prior state (fully backward compatible with existing files —
  // the key is numeric-chain-disjoint, so the monotonicity loop below never treats it as a chain). Load
  // it ONCE here and pass each chain's prior state into compareChain (direction-aware decision).
  const prior = loadPriorCheckpoints(checkpointFile);
  const priorStagnation =
    prior && typeof prior._stagnation === 'object' && prior._stagnation !== null
      ? prior._stagnation
      : {};
  const nowMs = Date.now();

  const results = [];
  for (const chain of chains) {
    try {
      results.push(
        await compareChain(
          urlA,
          urlB,
          chain,
          cutover,
          margin,
          bucket,
          schemaB,
          stagnationThreshold,
          priorStagnation[chain] ?? null,
          nowMs,
        ),
      );
    } catch (e) {
      results.push({ chain, verdict: 'ERROR', classes: { error: e.message } });
    }
  }

  // Checkpoint monotonicity across runs: Soak B's per-chain progress must never rewind between
  // hourly runs. A regression (a resume/restart that lost ground) is a hard FAIL, wired into both
  // the verdict/exit code and the alerts — not merely logged.
  const nextCheckpoints = { ...prior };
  const regressions = [];
  const nextStagnation = {};
  for (const r of results) {
    // Carry forward this chain's persist-stagnation state for the next run's direction check. A chain
    // that errored before the guard ran leaves stagnationState undefined → nothing to carry.
    if (r.stagnationState) {
      nextStagnation[r.chain] = r.stagnationState;
    }

    if (r.progressB === undefined) {
      continue;
    }

    const history = Array.isArray(prior[r.chain]) ? prior[r.chain] : [];
    const series = [...history, r.progressB];
    const mono = checkpointMonotonic(series);
    if (!mono.ok) {
      regressions.push({ chain: r.chain, prev: mono.prev, cur: mono.cur });
      r.verdict = 'FAIL';
      r.classes = { ...r.classes, checkpointRegression: mono };
    }

    // keep a bounded tail so the file does not grow unbounded across a long soak
    nextCheckpoints[r.chain] = series.slice(-64);
  }
  // Persist the new per-chain stagnation state section (replaces the prior one wholesale — each run
  // recomputes every diffed chain's nextState from scratch, so a chain no longer armed clears itself).
  nextCheckpoints._stagnation = nextStagnation;

  const verdict = results.some(
    (r) => r.verdict === 'FAIL' || r.verdict === 'ERROR',
  )
    ? 'FAIL'
    : results.every((r) => r.verdict === 'PASS')
      ? 'PASS'
      : 'PENDING';

  // Soak B restart signal (from the systemd ExecStartPre log).
  const restartLog =
    process.env.RESTART_LOG ?? `${process.env.HOME ?? '.'}/soak-b-restarts.log`;
  let restarts = {
    restartCount: 0,
    lastRestartAt: null,
    restartsLastHour: 0,
    crashLoop: false,
  };
  try {
    restarts = restartStats(readFileSync(restartLog, 'utf8').split('\n'));
  } catch {
    // no restart log yet — Soak B not started, or first boot
  }

  const alerts = composeAlerts(results, regressions, restarts);

  const toleratedIssue27 = aggregateToleratedIssue27(results);
  const knownBadRowsAgg = aggregateKnownBadRows(results);
  const toleratedIssue36 = aggregateToleratedIssue36(results);

  const status = {
    ts: new Date().toISOString(),
    chains: results.map((r) => r.chain),
    verdict,
    restartCount: restarts.restartCount,
    lastRestartAt: restarts.lastRestartAt,
    restartsLastHour: restarts.restartsLastHour,
    alerts,
    diffClasses: Object.fromEntries(results.map((r) => [r.chain, r.classes])),
    lagA: Object.fromEntries(results.map((r) => [r.chain, r.lagA ?? null])),
    lagB: Object.fromEntries(results.map((r) => [r.chain, r.lagB ?? null])),
    checkpointRegressions: regressions,
    // Aggregate of the reported-but-tolerated issue #27 shared-tx class across all chains. A run whose
    // only divergence is this class is PASS (never fails the verdict); the count keeps it VISIBLE.
    toleratedIssue27,
    // Aggregate of the reported knownBadRows pins (issue #32) across all chains — a SEPARATE counter
    // from toleratedIssue27. Also PASS-compatible; kept VISIBLE so the pinned rows never go quiet. Carries
    // perHash (per-pin fire counts) and unmatched (pins whose exact (hash, chain) fired 0 times) so each
    // pin's fate is exact in the status JSON, not merely a chain-level roll-up.
    knownBadRows: knownBadRowsAgg,
    // Aggregate of the reported-but-tolerated issue #36 onlyB row-loss class (logs + blocks) across all
    // chains — a SEPARATE counter from the two above. A run whose only divergence is this class is PASS;
    // the count keeps leg A's ongoing row loss VISIBLE and loud so it can never become quiet. Split per
    // table (logs/blocks) and per chain so the growth is legible at a glance.
    toleratedIssue36,
    counters: Object.fromEntries(
      results.map((r) => [r.chain, chainCounters(r)]),
    ),
  };

  writeFileSync(statusFile, `${JSON.stringify(status, null, 2)}\n`);
  // The checkpoint file is the monotonicity ledger — a torn write (process killed mid-write) would
  // corrupt it and, on the next run, either fail-closed on the parse error or lose the prior series
  // (silently disabling the rewind guard). Write it atomically: full temp file + rename, so a reader
  // ever sees only the old complete file or the new complete file, never a partial one.
  writeJsonAtomic(checkpointFile, nextCheckpoints);
  console.log(JSON.stringify(status, null, 2));
  const toleratedLine = formatToleratedIssue27Line(toleratedIssue27);
  if (toleratedLine) {
    console.log(toleratedLine);
  }

  const knownBadRowsLine = formatKnownBadRowsLine(knownBadRowsAgg);
  if (knownBadRowsLine) {
    console.log(knownBadRowsLine);
  }

  const toleratedIssue36Line = formatToleratedIssue36Line(toleratedIssue36);
  if (toleratedIssue36Line) {
    console.log(toleratedIssue36Line);
  }

  process.exit(verdict === 'FAIL' ? 1 : 0);
}

// Sum the per-chain issue #27 tolerated tallies from every chain result into one {count, perChain}.
// Pure + exported: the status JSON's top-level counter and the human line both read this, so a
// mutation that miscounts is caught directly rather than only through a full run.
export function aggregateToleratedIssue27(results) {
  const perChain = {};
  let count = 0;
  for (const r of results) {
    const tol = r?.classes?.transactions?.toleratedIssue27;
    if (!tol) {
      continue;
    }

    count += tol.count ?? 0;
    for (const [chain, n] of Object.entries(tol.perChain ?? {})) {
      perChain[chain] = (perChain[chain] ?? 0) + n;
    }
  }

  return { count, perChain };
}

// One human-readable line for a run that carried tolerated issue #27 rows (empty string ⇒ print
// nothing). The wording is deliberately loud about REMOVAL so the class cannot quietly become
// permanent. Pure + exported so the message contract is asserted.
export function formatToleratedIssue27Line(tolerated) {
  if (!tolerated || (tolerated.count ?? 0) <= 0) {
    return '';
  }

  const breakdown = Object.entries(tolerated.perChain ?? {})
    .map(([chain, n]) => `${chain}:${n}`)
    .join(', ');

  return (
    `TOLERATED (known issue #27 — REMOVE when the fix deploys and the window closes): ` +
    `${tolerated.count} access_list-null rows (${breakdown})`
  );
}

// Sum the per-chain, per-TABLE issue #36 onlyB-row-loss tallies (logs + blocks + transactions) from every
// chain result into one { count, logs, blocks, transactions, perChain }. `logs`/`blocks`/`transactions`
// are per-table {count, perChain} rolls; `perChain` is the combined per-chain total; `count` is the grand
// total. Pure + exported so the status JSON's top-level counter and the human line both read this, and a
// miscount is caught directly. Note the block-table, log-table and tx-table onlyB rows are DISTINCT rows
// (a block gap is not a log gap is not a tx gap — even for the SAME wholly-absent block, its block row,
// its log rows and its tx rows are three different rows), so summing them is a true total, not
// double-counting.
export function aggregateToleratedIssue36(results) {
  const perChain = {};
  const logs = { count: 0, perChain: {} };
  const blocks = { count: 0, perChain: {} };
  const transactions = { count: 0, perChain: {} };
  const rollTable = (dst, tol) => {
    if (!tol) {
      return;
    }

    dst.count += tol.count ?? 0;
    for (const [chain, n] of Object.entries(tol.perChain ?? {})) {
      dst.perChain[chain] = (dst.perChain[chain] ?? 0) + n;
      perChain[chain] = (perChain[chain] ?? 0) + n;
    }
  };
  for (const r of results) {
    rollTable(logs, r?.classes?.logs?.toleratedIssue36);
    rollTable(blocks, r?.classes?.blocks?.toleratedIssue36);
    rollTable(transactions, r?.classes?.transactions?.toleratedIssue36);
  }

  return {
    count: logs.count + blocks.count + transactions.count,
    logs,
    blocks,
    transactions,
    perChain,
  };
}

// One human-readable line for a run that carried tolerated issue #36 onlyB rows (empty string ⇒ print
// nothing). Loud about REMOVAL — names issue #36 and states the removal condition (A repaired OR the leg
// retired) — so leg A's ongoing row loss can never quietly become permanent. Pure + exported so the
// message contract is asserted.
export function formatToleratedIssue36Line(tolerated) {
  if (!tolerated || (tolerated.count ?? 0) <= 0) {
    return '';
  }

  const breakdown = Object.entries(tolerated.perChain ?? {})
    .map(([chain, n]) => `${chain}:${n}`)
    .join(', ');

  return (
    `TOLERATED (known issue #36 — REMOVE when issue #36 is resolved (A repaired or leg retired)): ` +
    `${tolerated.count} onlyB rows leg A lost (logs:${tolerated.logs?.count ?? 0} blocks:${tolerated.blocks?.count ?? 0} transactions:${tolerated.transactions?.count ?? 0}; per-chain ${breakdown})`
  );
}

// One loud alert line per chain whose persist-stagnation guard FIRED (issue #38) — the stalled leg
// named, the skew and threshold shown. Reads the per-chain persistStagnation class each compareChain
// attaches. Pure + exported so the alert wording and the "only fires on FAIL" contract are asserted
// directly, without a DB. A chain result with no persistStagnation (e.g. an ERROR before the guard
// ran) contributes nothing.
export function stagnationAlerts(results) {
  const lines = [];
  for (const r of results) {
    const s = r?.classes?.persistStagnation;
    if (!s?.fail) {
      continue;
    }

    const stalled = s.staleSide === 'A' ? 'leg A' : 'leg B';
    // Say what was PROVEN — the reason tag drives a self-describing clause. A frozen or skew-growing
    // wedge is a two-leg-populated freeze (skew shown); a one-sided-empty wedge is the empty leg
    // persisting nothing for N seconds while the other advances (no skew — the leg has no rows). A
    // sticky 'wedge-unrecovered' fail (D2) resolves through its carried originalReason so it reads as
    // the wedge it still is, plus an explicit "still unrecovered" note. F6: if a wedge-unrecovered fail
    // carries no originalReason (a robustness gap), fall back to the reason itself rather than rendering
    // an undefined-driven generic clause.
    const effectiveReason =
      s.reason === 'wedge-unrecovered'
        ? (s.originalReason ?? s.reason)
        : s.reason;
    const unrecovered =
      s.reason === 'wedge-unrecovered' ? ' (still unrecovered)' : '';
    // F6: only show the skew clause when the skew is a finite measured number. A null skew (an empty-leg
    // wedge has no measurable skew) must NOT render as "skew nulls > Ns" — omit the clause entirely.
    const hasSkew =
      typeof s.skewSeconds === 'number' && Number.isFinite(s.skewSeconds);
    const skewClause = hasSkew
      ? `; skew ${s.skewSeconds}s > ${s.thresholdSeconds}s`
      : '';
    let what;
    if (effectiveReason === 'one-sided-empty') {
      // Review delta 5, finding 3: claim only what is KNOWN. Emptiness duration is per-leg (this leg
      // has persisted no rows for over the window); the other leg's advancement is the CURRENT
      // observation, not a claim about the whole window — the window may have overlapped a both-empty
      // outage, so we do not assert the other leg advanced for its entirety.
      what =
        `has persisted NO rows for over ${s.thresholdSeconds}s (empty leg) and the other leg is ` +
        `advancing (one-sided wedge)${unrecovered}`;
    } else if (effectiveReason === 'skew-growing' && hasSkew) {
      what =
        `is falling further behind — newest-row timestamp skew GROWING to ${s.skewSeconds}s ` +
        `> ${s.thresholdSeconds}s${unrecovered}`;
    } else {
      // 'frozen' (or any other fail reason): the stale leg's newest row is not advancing. Show the skew
      // clause only when a finite skew is present (F6: a null skew omits it, never "skew nulls > Ns").
      what = `has stopped persisting rows (newest-row timestamp FROZEN${skewClause})${unrecovered}`;
    }
    lines.push(
      `persist-stagnation: chain ${r.chain} — ${stalled} ${what}; ` +
        `maxA=${s.maxA} maxB=${s.maxB} tsA=${s.tsA} tsB=${s.tsB}`,
    );
  }

  return lines;
}

// Whether a chain result FAILed for a WINDOWED reason — a hard divergence in its OWN diff classes
// (logs / blocks / transactions / checkpoint buckets), as opposed to a stagnation-only or checkpoint-
// regression FAIL. Finding 3: the generic 'finalized-diff' alert must be emitted iff at least one chain
// FAILed for a windowed reason, regardless of stagnation lines on other chains — with D1 (no early
// return) every FAILing chain's windowed classes are present, so we can decide this per chain from the
// classes. An ERROR result (no classes) counts as a windowed hard-fail (the diff could not complete —
// exactly the kind of unexpected failure the generic line points a human at). Pure + exported.
export function chainWindowedFail(r) {
  if (r?.verdict === 'ERROR') {
    return true;
  }
  if (r?.verdict !== 'FAIL') {
    return false;
  }

  const c = r.classes ?? {};

  return Boolean(
    c.logs?.fail ||
      c.blocks?.fail ||
      c.transactions?.fail ||
      (c.checkpointBuckets && c.checkpointBuckets.ok === false),
  );
}

// Compose the FULL alert list from the run results, the checkpoint regressions, and the restart stats.
// Pure + exported (D3/D4): the alert composition — crash-loop, checkpoint-regression lines, per-chain
// stagnation lines, and the generic finalized-diff line's SUPPRESSION logic — is asserted directly.
//
// The generic 'finalized-diff' line (finding 3) is emitted IFF at least one chain FAILed for a WINDOWED
// reason (chainWindowedFail), regardless of stagnation lines on OTHER chains: a stagnation-only FAIL has
// its own precise line and must NOT also trigger the vague generic one, but a real windowed FAIL on
// chain Y must still surface it even while chain X carries a stagnation line. Checkpoint regressions
// have their own precise lines and are NOT a windowed-diff cause, so they do not (by themselves) emit
// the generic line either.
export function composeAlerts(results, regressions, restarts) {
  const alerts = [];
  if (restarts?.crashLoop) {
    alerts.push(
      `crash-loop: ${restarts.restartsLastHour} restarts in the last hour (>3)`,
    );
  }
  for (const reg of regressions ?? []) {
    alerts.push(
      `checkpoint-regression: chain ${reg.chain} rewound ${reg.prev} → ${reg.cur} between runs`,
    );
  }
  // One NAMED line per stalled chain (issue #38) — a one-sided wedge a frozen window would hide behind
  // a PASS is loud and self-describing.
  for (const line of stagnationAlerts(results)) {
    alerts.push(line);
  }
  // The generic finalized-diff line: emit iff SOME chain has a windowed hard-fail. Per-chain from the
  // classes (D1 makes every FAILing chain's windowed classes present) — so chain X stagnation-only +
  // chain Y windowed FAIL emits BOTH the X stagnation line above AND this generic line.
  if ((results ?? []).some(chainWindowedFail)) {
    alerts.push(
      'finalized-diff: an unexpected finalized-overlap divergence (see diffClasses)',
    );
  }

  return alerts;
}

// The per-chain `counters` status-JSON entry: the window bounds + verdict, PLUS the persist-stagnation
// summary (issue #38 finding 6 — the PR body already claimed maxA/maxB/tsA/tsB/skew live here; this
// aligns the code with the body). A frozen window is legible at a glance from the counters alone, even
// below threshold. A result with no persistStagnation class (an ERROR before the guard ran) omits the
// stagnation fields. Pure + exported so the counters contract is asserted.
export function chainCounters(r) {
  const base = { lo: r.lo, hi: r.hi, verdict: r.verdict };
  const s = r?.classes?.persistStagnation;
  if (!s) {
    return base;
  }

  return {
    ...base,
    maxA: s.maxA,
    maxB: s.maxB,
    tsA: s.tsA,
    tsB: s.tsB,
    stagnationSkewSeconds: s.skewSeconds,
    stagnationReason: s.reason,
  };
}

// Sum the per-chain knownBadRows (issue #32) tallies from every chain result into one
// {count, perChain, perHash} AND flag any CONFIGURED pin that matched NOTHING this run. A pin matches
// nothing when its row was repaired (no longer diverges) or was on a chain the run didn't diff — that
// is NOT a failure, but it MUST stay visible so a stale pin cannot rot silently (it should then be
// REMOVED). `unmatched` is the subset of `pins` whose EXACT (hash, chain) fired zero times, so the
// status JSON always shows every pin's fate — even when two pins share a chain, a repaired one is
// surfaced while its co-chain sibling that DID fire is not. SEPARATE from aggregateToleratedIssue27 —
// the two counters are threaded independently. Pure + exported so the status JSON's top-level counter,
// the aggregated perHash, the unmatched set, and the human line are mutation-verified directly.
export function aggregateKnownBadRows(results, pins = knownBadRows) {
  const perChain = {};
  const perHash = {};
  let count = 0;
  for (const r of results) {
    const tol = r?.classes?.transactions?.knownBadRows;
    if (!tol) {
      continue;
    }

    count += tol.count ?? 0;
    for (const [chain, n] of Object.entries(tol.perChain ?? {})) {
      perChain[chain] = (perChain[chain] ?? 0) + n;
    }
    // Aggregate per-hash fire counts so pin fate is exact: a pin is matched IFF its OWN hash fired,
    // never merely because some other pin on the same chain did.
    for (const [hash, n] of Object.entries(tol.perHash ?? {})) {
      perHash[hash] = (perHash[hash] ?? 0) + n;
    }
  }

  // A configured pin is "unmatched" when its EXACT (hash, chain) contributed zero matches this run.
  // diffTx tallies per hash for the chain it diffed, so a nonzero perHash[hash] proves that specific
  // pin fired — no chain-level over-count. (A hash is a global identifier; the (hash, chain) pairing in
  // the pin is what the classifier matched on, so a fired hash unambiguously identifies its pin.)
  const unmatched = (pins ?? []).filter((p) => (perHash[p.hash] ?? 0) === 0);

  return { count, perChain, perHash, unmatched };
}

// One human-readable line for a run that carried knownBadRows pins (empty string ⇒ print nothing). Loud
// about REMOVAL and names issue #32 so a pinned single row cannot quietly become permanent. Also loud
// about UNMATCHED pins (a pin that fired zero times this run — its row was likely repaired) so a stale
// pin is never silent: it prints even when the matched count is 0, precisely so a repaired-but-not-yet-
// removed entry stays VISIBLE. Pure + exported so the message contract is asserted.
export function formatKnownBadRowsLine(tolerated) {
  const count = tolerated?.count ?? 0;
  const unmatched = tolerated?.unmatched ?? [];
  if (count <= 0 && unmatched.length === 0) {
    return '';
  }

  const breakdown = Object.entries(tolerated?.perChain ?? {})
    .map(([chain, n]) => `${chain}:${n}`)
    .join(', ');

  const matchedPart =
    count > 0 ? `${count} pinned access_list-only rows (${breakdown})` : 'none';

  let line =
    `KNOWN-BAD ROWS (issue #32 — REMOVE the knownBadRows entry when the row is repaired or explained): ` +
    matchedPart;

  if (unmatched.length > 0) {
    const stale = unmatched.map((p) => `${p.chain}:${p.hash}`).join(', ');
    line += ` — ${unmatched.length} UNMATCHED pin(s) fired 0 times this run (repaired? REMOVE): ${stale}`;
  }

  return line;
}

// Atomic JSON write: serialize to a sibling temp file, then rename over the target (rename is atomic
// within a filesystem). Exported so a mutation that skips the rename is caught by a test.
export function writeJsonAtomic(file, data) {
  const tmp = `${file}.tmp.${process.pid}`;
  writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`);
  renameSync(tmp, file);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(e);
    process.exit(2);
  });
}

// re-export for a caller that wants to persist prior checkpoints and assert monotonicity across runs
export function loadPriorCheckpoints(file) {
  try {
    return JSON.parse(readFileSync(file, 'utf8'));
  } catch {
    return {};
  }
}
