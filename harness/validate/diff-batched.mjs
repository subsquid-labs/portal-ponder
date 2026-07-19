// Streaming / constant-memory byte-identity diff of two ponder_sync stores — the F-full variant of
// harness/diff/diff.mjs. Where diff.mjs loads whole tables into memory, this walks each table in
// ORDER BY key pages (keyset pagination) and merge-compares the two ordered streams, so peak memory
// is one page per side regardless of table size (the Euler-eth full history is millions of rows). The
// page size is BYTE-AWARE (issue #63): each page's row limit adapts from the previous page's observed
// row width to keep the per-query detoast payload bounded (default ≤32MB, override --byte-target),
// because PGlite's WASM allocator wedges on a too-large single-query detoast volume. Tolerances match
// diff.mjs exactly:
//   - logs / transaction_receipts / traces : strict set + field identity
//   - transactions : strict one-sided + field identity, plus the scoped access_list column-gap
//     tolerance (issues #83, #32) — a shared tx whose ONLY differing column is `access_list`, with the
//     Portal side (A) SQL NULL, on a #83-family chain (base/arbitrum/avalanche), is tolerated, not
//     failed; a non-NULL Portal value (incl. #110's fabricated "[]"), any second differing column, or
//     an out-of-scope chain still FAILS (self-retiring). Mirrors diff.mjs `transactionsVerdict`.
//   - blocks : total_difficulty excluded; ASYMMETRIC by default — a portal-only block (A) FAILS (the
//     Portal path invented a block RPC never saw); only an rpc-only block (B) is tolerated (the stock
//     RPC path stores inert event-less blocks it traced); a shared-key field mismatch always FAILS,
//     EXCEPT the known upstream block.size derivation artifact (issues #76, #106): a shared block whose
//     ONLY differing field is `size`, over a present+equal hash, is tolerated, not failed — any second
//     differing field (including hash) still FAILS (self-retiring). Mirrors harness/diff/diff.mjs
//     `blocksVerdict` exactly.
//     This asymmetry is CALIBRATED FOR portal-vs-RPC (A=portal, B=rpc). For a portal-vs-PORTAL diff
//     (e.g. the chaos verify: a resumed store vs a clean baseline, both Portal-built) there is no
//     inert-block asymmetry — a B-only (baseline-only) block means the resumed store is MISSING a
//     block, which MUST fail. Pass STRICT_BLOCKS=1 (or --strict-blocks) to force the `blocks` table
//     to mode:'strict' so a one-sided block on EITHER side fails.
//
// Modes:
//   node diff-batched.mjs <pgliteDirA> <pgliteDirB> [--app-hash]   diff sync stores (exit 0/1)
//   node diff-batched.mjs --app-hash <pgliteDir> [--schema NAME]   ordered md5 over app tables
//   STRICT_BLOCKS=1 node diff-batched.mjs A B                       blocks table strict (portal-vs-portal)
//   --byte-target <bytes> (or DIFF_BYTE_TARGET env)                 per-query payload target (default 32MB)
//
// The pure comparison + hashing core is exported for unit tests (fixture rows, no database).

import { createHash } from 'node:crypto';

// ── pure core (exported, tested on fixtures) ───────────────────────────────────────────────────

// A JSON.stringify replacer so a bigint ANYWHERE (top-level column OR nested, e.g. a composite
// {key:[bigint]} sample row) serializes to its decimal string instead of throwing. DB rows carry
// only scalar columns, so this is a no-op there; it just makes the sampler total.
const bigintSafe = (_k, v) => (typeof v === 'bigint' ? v.toString() : v);

// Normalize a row to a stable JSON string: sorted keys, bigint→decimal, bytes→hex, dropped columns
// removed. Identical logical rows across the two pglite engines produce identical strings.
export function normRow(row, drop) {
  const o = {};
  for (const k of Object.keys(row).sort()) {
    if (drop?.has(k)) {
      continue;
    }

    const v = row[k];
    if (typeof v === 'bigint') {
      o[k] = v.toString();
    } else if (v instanceof Uint8Array) {
      o[k] = Buffer.from(v).toString('hex');
    } else {
      o[k] = v;
    }
  }

  return JSON.stringify(o, bigintSafe);
}

// Lexicographic compare of two composite keys (arrays of bigint|number|string).
export function cmpKey(a, b) {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a[i];
    const y = b[i];
    if (x < y) {
      return -1;
    }
    if (x > y) {
      return 1;
    }
  }

  return a.length - b.length;
}

// Turn an array into an async iterator so the array (test) and DB (CLI) paths share one merge.
async function* fromArray(rows) {
  for (const r of rows) {
    yield r;
  }
}

// ── known upstream derivation artifact: block.size only-diff (issues #76, #106) ──────────────────
// Mirrors harness/diff/diff.mjs exactly. `size` is a node-DERIVED, non-consensus header field — NOT
// committed by the block hash, so different sources legitimately compute it differently. Two dataset
// signatures have been observed, both with an IDENTICAL block hash and byte-identical
// logs/transactions/receipts/traces:
//   • #76 (eth-mainnet): a fixed 2-byte RLP length-of-length prefix reports every block whose RLP
//     payload crosses 2^16 (canonical size ≥ 65540) one byte short — portal.size === rpc.size − 1.
//   • #106 (BSC / chain 56): pervasive portal === rpc + 1 (opposite sign, below 65540) plus
//     occasional large size-only deltas — same block hash on both sides throughout.
// Both are the same thing: a source-side size derivation artifact, never a Portal content defect. So
// we tolerate a shared block row whose SOLE differing field is `size`, regardless of the delta's
// magnitude or sign.
//
// SAFETY INVARIANT (why this cannot mask a real divergence): `hash` is a COMPARED field. The block
// hash commits every consensus field, so an equal, non-empty hash on both sides ⟺ the same canonical
// block — a size-only difference over a matching hash is provably a derivation artifact. We anchor on
// exactly that: tolerate only when both rows carry a present (non-null/non-empty) and EQUAL `hash`.
// Any SECOND differing field — including `hash` itself (a reorg/wrong-block divergence) — is the
// second diff, so the predicate returns false and the caller FAILS. Self-retiring: once a source
// aligns its size derivation the rows compare equal and this never fires.

// portalRow / rpcRow are normalized block row-strings (normRow output: sorted keys, bigint→decimal,
// bytes→hex, total_difficulty dropped). Returns true iff `size` is the SOLE differing field AND both
// rows carry a present, equal `hash` (the safety anchor above). Delta magnitude and sign are
// irrelevant — a matching hash proves the canonical block is identical (issues #76, #106).
function sizeOnlyDiffTolerated(portalRow, rpcRow) {
  const p = JSON.parse(portalRow);
  const r = JSON.parse(rpcRow);

  let diffField = null;
  for (const k of new Set([...Object.keys(p), ...Object.keys(r)])) {
    if (p[k] === r[k]) continue;

    if (diffField !== null) return false;

    diffField = k;
  }

  if (diffField !== 'size') return false;

  // Safety anchor: a size-only diff is a derivation artifact ONLY over an identical canonical block,
  // which a present, equal hash proves. A missing/empty hash on either side is not anchored → FAIL.
  const { hash } = p;

  return hash !== null && hash !== undefined && hash !== '' && hash === r.hash;
}

// ── known representational diff: pre-London blocks.base_fee_per_gas null-vs-0 (cell U-eth) ────────
// Mirrors harness/diff/diff.mjs exactly. EIP-1559 (the London hard fork, eth-mainnet block 12,965,000)
// introduced the `baseFeePerGas` block-header field. PRE-London blocks have NO such field — a full
// node's `eth_getBlockByNumber` omits `baseFeePerGas` entirely (geth-verified absent on block
// 12453996). The two sync paths render that absence differently:
//   • the Portal-backfill leg (A) stores the honest SQL NULL — "the field does not exist" — matching
//     the design intent in portal/portal-filters.ts (baseFeePerGas pre-1559 → store as null).
//   • the stock-RPC leg (B), through ponder-core's RPC store, COERCES the absent field to 0, so it
//     lands as the bigint 0 (normRow renders 0n → the decimal string "0").
// So on a pre-London block a `blocks` row can differ on `base_fee_per_gas` alone — Portal NULL vs RPC
// "0" — while the block hash, every other column, and all logs/transactions/receipts/traces are
// byte-identical. Portal's NULL is the CANONICALLY-CORRECT value (the field is truly absent); the RPC
// baseline's "0" is the outlier. Observed once in cell U-eth (UniswapV3 factory-stress, ethereum): 10
// windows, 9 byte-identical, one all-pre-London window [12453996, 12454496] exhibiting exactly this.
//
// SAFETY INVARIANT (why this cannot mask a real base-fee bug): the tolerance fires ONLY when the Portal
// side (A) is SQL NULL — the honest "field absent" value — AND the RPC side (B) is exactly "0" (the
// coerced-absent outlier), anchored on a present, non-empty, EQUAL `hash` (identical canonical block
// proof, exactly as the size tolerance), AND ONLY on an in-scope chain (BASE_FEE_PRELONDON_CHAINS —
// eth-mainnet 1, the sole chain with a pre-1559 era observed in the matrix, scoped exactly like the
// access_list gap). A POST-London block has a real non-zero base fee, so a genuine Portal base-fee
// defect makes the Portal side NON-NULL (or the RPC side non-"0") and the predicate returns false →
// real MISMATCH → FAIL. Any SECOND differing field also returns false (column-scoped, not row-scoped),
// a missing/empty/unequal hash defeats the anchor, and an out-of-scope chain never gets this leniency
// (a future non-eth chain exhibiting the class FAILs → prompting review + an evidence-backed addition
// to the set, never a silent mask). Self-retiring: if the RPC path ever stops coercing absent→0
// (stores NULL too), the rows compare equal and this never fires.

// The chain_id set with a pre-1559 era where an absent baseFeePerGas is coerced to 0 by the RPC-sync
// path (eth-mainnet 1 only — the sole chain observed with a pre-London window in the matrix; grow with
// evidence exactly like ACCESS_LIST_GAP_CHAINS). chain_id is a COMPARED column present in every blocks
// row (the PK leads with it), so the scope is read from the row itself — no env threading. A chain
// outside this set is never tolerated.
export const BASE_FEE_PRELONDON_CHAINS = new Set([1]);

// portalRow / rpcRow are normalized block row-strings (normRow output: sorted keys, bigint→decimal,
// bytes→hex, total_difficulty dropped). Returns true iff the row's chain_id is an in-scope pre-London
// chain (eth-mainnet 1), `base_fee_per_gas` is the SOLE differing field, the Portal side (A) is SQL
// NULL, the RPC side (B) is exactly "0", AND both rows carry a present, equal `hash` (the safety anchor
// above). An out-of-scope chain, a non-NULL Portal value, a non-"0" RPC value, a second differing
// field, or a missing/unequal hash all return false → FAIL (cell U-eth).
function baseFeeNullVsZeroTolerated(portalRow, rpcRow) {
  const p = JSON.parse(portalRow);
  const r = JSON.parse(rpcRow);

  // Scope guard: only chains with a pre-1559 era (eth-mainnet 1). chain_id is normalized to a decimal
  // string (bigint→decimal in normRow); compare numerically against the scope set.
  if (!BASE_FEE_PRELONDON_CHAINS.has(Number(p.chain_id))) return false;

  let diffField = null;
  for (const k of new Set([...Object.keys(p), ...Object.keys(r)])) {
    if (p[k] === r[k]) continue;

    if (diffField !== null) return false;

    diffField = k;
  }

  if (diffField !== 'base_fee_per_gas') return false;

  // Regression sentinel: tolerate ONLY the honest pre-London class — Portal side SQL NULL (field truly
  // absent) against the RPC baseline's coerced "0". A non-NULL Portal base fee, or an RPC side that is
  // not exactly "0", is a real divergence and must FAIL (a post-London base-fee bug is never masked).
  if (p.base_fee_per_gas !== null || r.base_fee_per_gas !== '0') return false;

  // Safety anchor: the null-vs-0 diff is a representational artifact ONLY over an identical canonical
  // block, which a present, equal hash proves. A missing/empty hash on either side is not anchored → FAIL.
  const { hash } = p;

  return hash !== null && hash !== undefined && hash !== '' && hash === r.hash;
}

// ── known upstream dataset gap: transactions.access_list column dropped (issues #83, #32) ─────────
// Mirrors harness/diff/diff.mjs exactly. The SQD Portal dataset for base-mainnet, arbitrum-one, and
// avalanche-mainnet DROPS the `transactions.access_list` column (upstream #83-family gap, refining
// #32). On those chains the Portal-backfill leg (A) therefore stores an HONEST SQL NULL for that
// column — the fork records the dropped value faithfully as NULL, not a fabricated `"[]"` (the old
// fork defect, fixed by #110/#111). The stock-RPC leg (B) backfills from a full node and stores the
// REAL populated access list. So on exactly these three chains a `transactions` row can differ on the
// `access_list` column alone, while every other column and all logs/blocks are byte-identical.
//
// REGRESSION-SENTINEL INVARIANT (why this cannot mask a real divergence, incl. the #110 fork defect):
// the tolerance fires ONLY when the Portal side (A) is SQL NULL — the honest dropped-column value. If
// the Portal side is NON-NULL and differs from RPC (in particular a reappearing fabricated `"[]"`, the
// exact #110 defect), the predicate returns false → real MISMATCH → FAIL. Two differing NON-NULL
// values are NEVER tolerated. Any SECOND differing column also returns false, so the tolerance is
// column-scoped, not row-scoped. Self-retiring: if the Portal ever serves the column the rows compare
// equal and this never fires. SCOPED to the #83-family chains only — a chain that DOES serve the
// column must never get this leniency, so an access_list divergence there is a hard FAIL.

// The chain_id set the SQD Portal drops transactions.access_list for (base-mainnet 8453, arbitrum-one
// 42161, avalanche-mainnet 43114 — see harness/validate/cells.json). chain_id is a COMPARED column
// present in every transactions row (the PK leads with it), so the scope is read from the row itself —
// no env threading. A chain outside this set is never tolerated.
export const ACCESS_LIST_GAP_CHAINS = new Set([8453, 42161, 43114]);

// portalRow / rpcRow are normalized transactions row-strings (normRow output: sorted keys,
// bigint→decimal, bytes→hex). Returns true iff `access_list` is the SOLE differing field, the Portal
// side (A) is SQL NULL, AND the row's chain_id is an in-scope #83-family chain. The RPC side (B) may be
// anything (a populated list, "[]", …) — only Portal-IS-NULL is tolerated (issues #83, #32).
function accessListColumnGapTolerated(portalRow, rpcRow) {
  const p = JSON.parse(portalRow);
  const r = JSON.parse(rpcRow);

  // Scope guard: only the #83-family chains that drop the column. chain_id is normalized to a decimal
  // string (bigint→decimal in normRow); compare numerically against the scope set.
  if (!ACCESS_LIST_GAP_CHAINS.has(Number(p.chain_id))) return false;

  let diffField = null;
  for (const k of new Set([...Object.keys(p), ...Object.keys(r)])) {
    if (p[k] === r[k]) continue;

    if (diffField !== null) return false;

    diffField = k;
  }

  if (diffField !== 'access_list') return false;

  // Regression sentinel: tolerate ONLY the honest dropped-column value (Portal side SQL NULL). A
  // non-NULL Portal value that differs from RPC — e.g. a reappearing fabricated "[]" (#110) — is a real
  // divergence and must FAIL. Two differing non-NULL values are never tolerated.
  return p.access_list === null;
}

// Streaming merge-compare of two key-ordered async row streams. Modes:
//   • 'strict' : ANY only-one-side row fails, plus any shared-key field mismatch.
//   • 'blocks' : ASYMMETRIC, mirroring harness/diff/diff.mjs `blocksVerdict`. A is the Portal store
//     and B is the stock-RPC store (see diffStores / run.sh: dirA=portal, dirB=rpc). A portal-only
//     block (onlyA) is a block the Portal path invented that RPC never saw → FAIL. An rpc-only block
//     (onlyB) is a tolerated inert event-less block the stock RPC path traced but never referenced →
//     reported, not failed. A shared-key field mismatch is always a FAIL, EXCEPT the known upstream
//     block.size derivation artifact (issues #76, #106) — a shared block whose ONLY differing field is
//     `size`, over a present+equal hash, is counted in res.sizeTolerated, not res.mismatch, and does
//     NOT fail — AND the pre-London base_fee_per_gas null-vs-0 representational diff (cell U-eth) — a
//     shared block on an in-scope chain (BASE_FEE_PRELONDON_CHAINS — eth-mainnet chain_id 1, scoped
//     like the access_list gap, not universally) whose ONLY differing field is `base_fee_per_gas`,
//     Portal side (A) SQL NULL vs RPC side (B) "0", over a present+equal hash, counted in
//     res.baseFeeTolerated, not res.mismatch. Any second differing field (including hash), a non-NULL
//     Portal base fee, a non-"0" RPC base fee, or an out-of-scope chain still FAILS (self-retiring).
//     Before the #19 fix 'blocks' was vacuous — it failed only on a shared
//     mismatch and let a portal-only block sail through the F-full differ.
//   • 'transactions' : STRICT one-sided semantics (a portal-only OR rpc-only tx FAILS, exactly like
//     strict), plus ONE tolerated shared-key class — the upstream access_list column gap (issues #83,
//     #32). A shared tx whose ONLY differing column is `access_list`, where the Portal side (A) is SQL
//     NULL and the row's chain_id is a #83-family chain (base/arbitrum/avalanche), is counted in
//     res.accessListTolerated, not res.mismatch, and does NOT fail. A non-NULL Portal value (incl. a
//     reappearing fabricated "[]" — the #110 defect), any second differing column, or an out-of-scope
//     chain still FAILS (regression sentinel; self-retiring). All other tables stay strict.
// Returns counters and small samples (never the whole diff) so memory stays bounded.
//
// `onOnlyB` (optional): a callback invoked with each B-only row's value AS the merge encounters it, in
// key order. It exists ONLY so the A/B differ can floor-gate B-only rows against a tolerated class
// (issue #36) WITHOUT collecting the whole B-only set into memory here — the callback streams one row
// at a time, so this function stays constant-memory. Default undefined ⇒ the existing strict/blocks
// callers (chaos verify, the F-full store diff) are byte-for-byte unaffected: no callback, no behavior
// change. The callback NEVER alters this function's fail verdict — it is a pure observer; the caller
// decides tolerance from the rows it collects.
export async function streamingDiff(
  iterA,
  iterB,
  { keyFn, drop, mode = 'strict', onOnlyB },
) {
  const itA = iterA[Symbol.asyncIterator]();
  const itB = iterB[Symbol.asyncIterator]();
  let a = await itA.next();
  let b = await itB.next();

  const res = {
    fail: false,
    aCount: 0,
    bCount: 0,
    onlyA: 0,
    onlyB: 0,
    mismatch: 0,
    sizeTolerated: 0,
    baseFeeTolerated: 0,
    accessListTolerated: 0,
    shared: 0,
    samples: [],
  };
  const sample = (kind, text) => {
    if (res.samples.length < 6) {
      res.samples.push(`${kind}: ${text.slice(0, 300)}`);
    }
  };

  while (!a.done || !b.done) {
    const ka = a.done ? null : keyFn(a.value);
    const kb = b.done ? null : keyFn(b.value);
    let step;
    if (kb === null) {
      step = -1;
    } else if (ka === null) {
      step = 1;
    } else {
      step = cmpKey(ka, kb);
    }

    if (step < 0) {
      res.aCount++;
      res.onlyA++;
      // A-only (portal-only) fails under strict, blocks, AND transactions: strict/transactions tolerate
      // no one-sided rows, and a portal-only block is one the Portal path invented that RPC never saw
      // (asymmetric blocks). The transactions access_list tolerance is a SHARED-key class only — a
      // one-sided tx is still a hard fail.
      if (mode === 'strict' || mode === 'blocks' || mode === 'transactions') {
        res.fail = true;
        sample('A-only', normRow(a.value, drop));
      }
      a = await itA.next();
    } else if (step > 0) {
      res.bCount++;
      res.onlyB++;
      // B-only (rpc-only) fails under strict AND transactions (a strict table — a one-sided tx is a real
      // gap); under blocks it is the tolerated inert event-less block the stock RPC path traced but
      // never referenced.
      if (mode === 'strict' || mode === 'transactions') {
        res.fail = true;
        sample('B-only', normRow(b.value, drop));
      }
      // Stream this B-only row to the observer (issue #36 floor-gate), one row at a time — never
      // collected here, so memory stays bounded. A pure observer: it does NOT change res.fail.
      if (onOnlyB) {
        onOnlyB(b.value);
      }
      b = await itB.next();
    } else {
      res.aCount++;
      res.bCount++;
      res.shared++;
      const na = normRow(a.value, drop);
      const nb = normRow(b.value, drop);
      if (na !== nb) {
        // Tolerated shared-key classes, each scoped to exactly one mode; strict tolerates nothing.
        //   • blocks mode: the upstream size-only derivation artifact (issues #76, #106); AND the
        //     pre-London base_fee_per_gas null-vs-0 representational diff (cell U-eth) — Portal NULL vs
        //     RPC "0" over an equal hash.
        //   • transactions mode: the upstream access_list column gap (issues #83, #32) — Portal-side
        //     NULL on a #83-family chain, access_list the sole differing column.
        // Anything else (a second differing column, a non-NULL Portal access_list incl. #110's "[]",
        // an out-of-scope chain) FAILS.
        if (mode === 'blocks' && sizeOnlyDiffTolerated(na, nb)) {
          res.sizeTolerated++;
        } else if (mode === 'blocks' && baseFeeNullVsZeroTolerated(na, nb)) {
          res.baseFeeTolerated++;
        } else if (
          mode === 'transactions' &&
          accessListColumnGapTolerated(na, nb)
        ) {
          res.accessListTolerated++;
        } else {
          res.mismatch++;
          res.fail = true;
          sample('A', na);
          sample('B', nb);
        }
      }
      a = await itA.next();
      b = await itB.next();
    }
  }

  return res;
}

// Array convenience wrapper used by the tests — inputs must already be key-ordered.
export function mergeCompare(rowsA, rowsB, opts) {
  return streamingDiff(fromArray(rowsA), fromArray(rowsB), opts);
}

// Ordered md5 over a set of rows (canonical order = by key). Same rows in any input order → same
// hash, so it is a determinism checkpoint for app tables.
export function hashRows(rows, keyFn, drop) {
  const lines = rows
    .map((r) => ({ k: keyFn(r), s: normRow(r, drop) }))
    .sort((x, y) => cmpKey(x.k, y.k))
    .map((x) => x.s);
  const h = createHash('md5');
  for (const line of lines) {
    h.update(line);
    h.update('\n');
  }

  return h.digest('hex');
}

// ── DB adapters + CLI (only run when invoked directly; pglite is imported lazily so the pure
//    exports above load with zero dependencies in the repo test runner) ─────────────────────────

// Key columns + comparison drop-set per sync-store table. Each `keys` array is EXACTLY the sync-store
// primary-key column order — every PK leads with `chain_id` (logs_pkey (chain_id, block_number,
// log_index); transactions_pkey / transaction_receipts_pkey (chain_id, block_number,
// transaction_index); traces_pkey (chain_id, block_number, transaction_index, trace_index);
// blocks_pkey (chain_id, number)). The keyset ORDER BY + tuple-WHERE MUST match the PK column order
// so every page is a streaming index scan on the PK, not a full-table sort (issue #58). With the
// chain_id prefix the ordering is the PK order: comparison is unchanged for a single-chain store
// (chain_id is a constant column) and well-defined per (chain_id, …) tuple for a multi-chain store.
// `chain_id` is a COMPARE key, never a drop key — identical stores share identical chain_id values,
// and an A/B pair of the same chain has it equal by construction.
export const TABLES = {
  logs: { keys: ['chain_id', 'block_number', 'log_index'], mode: 'strict' },
  transactions: {
    keys: ['chain_id', 'block_number', 'transaction_index'],
    // 'transactions' = strict one-sided + the scoped access_list column-gap tolerance (issues #83/#32,
    // Portal-NULL only, on base/arbitrum/avalanche); see streamingDiff modes + accessListColumnGapTolerated.
    mode: 'transactions',
  },
  transaction_receipts: {
    keys: ['chain_id', 'block_number', 'transaction_index'],
    mode: 'strict',
  },
  traces: {
    keys: ['chain_id', 'block_number', 'transaction_index', 'trace_index'],
    mode: 'strict',
  },
  blocks: {
    keys: ['chain_id', 'number'],
    mode: 'blocks',
    drop: new Set(['total_difficulty']),
  },
};

// The sync-store primary-key column order per table (source of truth: ponder's sync-store schema —
// every *_pkey leads with chain_id). Exported so a test can pin each TABLES spec's keyset to the PK
// column order without a database: dropping chain_id from any spec, or reordering the tuple, breaks
// the streaming-index-scan guarantee this differ depends on (issue #58).
export const SYNC_STORE_PKS = {
  logs: ['chain_id', 'block_number', 'log_index'],
  transactions: ['chain_id', 'block_number', 'transaction_index'],
  transaction_receipts: ['chain_id', 'block_number', 'transaction_index'],
  traces: ['chain_id', 'block_number', 'transaction_index', 'trace_index'],
  blocks: ['chain_id', 'number'],
};

// Resolve the per-table comparison specs for a run. `strictBlocks` promotes the `blocks` table from
// its default ASYMMETRIC 'blocks' mode to 'strict': the asymmetry only holds for portal-vs-RPC (a
// tolerated inert RPC-only block); for a portal-vs-PORTAL diff (chaos resume vs baseline) a one-sided
// block on EITHER side is a real gap and must fail. Pure + exported so a test can assert the override
// flips ONLY the blocks mode (and a mutation that ignores the flag is caught).
export function resolveTableSpecs(strictBlocks) {
  if (!strictBlocks) {
    return TABLES;
  }

  const out = {};
  for (const [table, spec] of Object.entries(TABLES)) {
    out[table] = table === 'blocks' ? { ...spec, mode: 'strict' } : spec;
  }

  return out;
}

// ── byte-aware page sizing (issue #63) ─────────────────────────────────────────────────────────
//
// Why a FIXED row limit is unsafe. PGlite 0.2.13's WASM allocator spins forever (100% CPU, flat RSS,
// no error) on a single `select *` page whose detoasted volume is too large — the F-full transactions
// table carries ~982MB of TOASTed calldata, so one 50k-row page hauls ~300MB of `input` through the
// WASM heap and wedges the tool. The same rows read in bounded-volume pages (memory reclaimed between
// queries) complete in seconds. #64 mitigated this by hard-pinning the limit to 5,000, which is safe
// for fat tables but leaves ~10× throughput on the table for slim ones (blocks, receipts).
//
// The durable fix: keep the per-query PAYLOAD bounded (not the row count). After each page we hold its
// rows, so we measure their average serialized byte width and size the NEXT page to hit a target
// payload (default 32MB), clamped to a [floor, ceiling] row window. Bounded payload ⇒ bounded detoast
// volume ⇒ no wedge; the adaptive limit lets slim tables run wide and fat tables run narrow, all under
// the same byte budget.
//
// The keyset CURSOR is entirely unchanged — the tuple-WHERE resumes strictly after the previous page's
// tail row regardless of how many rows a page held, so the yielded row STREAM is identical for any
// limit sequence. Only the `limit` value varies between pages.

// Per-query payload target: keep one page's detoast volume comfortably under the ~300MB that wedges
// the WASM heap. 32MB leaves ~10× headroom and still lets slim tables page wide.
const DEFAULT_BYTE_TARGET = 32 * 1024 * 1024;

// Row-limit clamp. FLOOR guarantees forward progress even if a single row is pathologically wide (a
// page is never smaller than this, so the cursor always advances). CEILING caps the limit so a
// degenerate tiny-average estimate can't ask for a multi-million-row page (and matches the historically
// safe 50k upper bound for slim tables). The FIRST page has no observation yet, so it starts at the
// FLOOR — the conservative choice that cannot wedge on an unknown-width table.
const MIN_BATCH = 5_000;
const MAX_BATCH = 50_000;

// Cheap, deterministic per-row byte width ≈ the row's detoast volume. We size against the RAW payload
// PGlite must materialize, so bytes columns count as their raw byte length (the fat `input` calldata is
// a Uint8Array — its byteLength IS the detoast cost). Strings count as their UTF-8 byte length; bigints
// and numbers as their decimal-digit length; everything else via a JSON fallback. No hashing, no full
// normRow — one linear pass over the row's own values, so measuring a page is O(page bytes) and adds no
// query. Deterministic: identical rows always measure identically.
export function rowBytes(row) {
  let n = 0;
  for (const k of Object.keys(row)) {
    const v = row[k];
    if (v instanceof Uint8Array) {
      n += v.byteLength;
    } else if (typeof v === 'string') {
      n += Buffer.byteLength(v);
    } else if (typeof v === 'bigint' || typeof v === 'number') {
      n += String(v).length;
    } else if (v === null || v === undefined) {
      n += 1;
    } else {
      n += Buffer.byteLength(JSON.stringify(v, bigintSafe));
    }
  }

  return n;
}

// Pure sizing policy: given the average serialized byte width observed on the PREVIOUS page, return the
// row limit for the NEXT page so its payload targets `targetBytes`, clamped to [floor, ceiling]. Kept
// pure + exported so the whole adaptation (target, clamp, degenerate-input handling) is unit-testable
// without a database — the WASM hang itself is not reproducible at test scale.
//
// Degenerate observations (0 / negative / NaN / non-finite avg — e.g. a page of all-empty rows, or a
// first page with no observation) carry no width signal, so we fall back to the FLOOR: the conservative
// limit that cannot wedge on an unknown- or zero-width table. A finite positive average yields
// floor(targetBytes / avg), clamped into [floor, ceiling].
export function nextBatchSize(
  observedAvgRowBytes,
  targetBytes = DEFAULT_BYTE_TARGET,
  floor = MIN_BATCH,
  ceiling = MAX_BATCH,
) {
  if (!Number.isFinite(observedAvgRowBytes) || observedAvgRowBytes <= 0) {
    return floor;
  }

  const est = Math.floor(targetBytes / observedAvgRowBytes);
  if (est < floor) {
    return floor;
  }
  if (est > ceiling) {
    return ceiling;
  }

  return est;
}

// Resolve the per-query byte target from CLI flag (`--byte-target <bytes>`) then env
// (`DIFF_BYTE_TARGET`), falling back to the default. A missing, non-numeric, or non-positive value
// falls through to the next source (never a silent 0 that would collapse every page to the floor).
// Pure + exported so the override precedence is unit-testable without a process. Returns the resolved
// positive integer byte target.
export function parseByteTarget(
  argv,
  env = {},
  fallback = DEFAULT_BYTE_TARGET,
) {
  const flag = argv.indexOf('--byte-target');
  if (flag >= 0) {
    const n = Number(argv[flag + 1]);
    if (Number.isFinite(n) && n > 0) {
      return Math.floor(n);
    }
  }

  const fromEnv = Number(env.DIFF_BYTE_TARGET);
  if (Number.isFinite(fromEnv) && fromEnv > 0) {
    return Math.floor(fromEnv);
  }

  return fallback;
}

const toBig = (v) => (typeof v === 'bigint' ? v : BigInt(v));

// Build one keyset page's SQL for `table` ordered by `keys` with a per-page `limit`. When `hasCursor`,
// adds a row-wise tuple WHERE so the page resumes strictly after the previous tail: (k0,k1,…) >
// ($1,$2,…). The ORDER BY and the tuple LHS list the columns in `keys` order — which is the sync-store
// PK column order (chain_id-first) — so the planner satisfies both the ordering and the range with a
// single forward index scan on the PK, no per-page sort (issue #58). The `limit` varies between pages
// (byte-aware sizing, issue #63) but is the ONLY thing that varies — the cursor WHERE/ORDER BY are
// fixed. Pure + exported so a test can pin the generated ORDER BY / tuple-WHERE shape per table WITHOUT
// a database.
export function buildKeysetSql(table, keys, hasCursor, limit) {
  const cols = keys.map((k) => `"${k}"`).join(', ');
  const order = `order by ${cols}`;
  let where = '';
  if (hasCursor) {
    // (k0,k1,…) > (last0,last1,…) — row-wise tuple comparison for a stable keyset cursor.
    const lhs = `(${cols})`;
    const rhs = `(${keys.map((_, i) => `$${i + 1}`).join(', ')})`;
    where = `where ${lhs} > ${rhs}`;
  }

  return `select * from ponder_sync."${table}" ${where} ${order} limit ${limit}`;
}

// Keyset-paginated async row stream: pulls a byte-aware page at a time ordered by `keys`, holding at
// most one page in memory. The FIRST page uses the floor limit (no width observation yet); each
// subsequent page's limit is sized from the PREVIOUS page's observed average row width so its payload
// targets `byteTarget` (issue #63) — this is the ONLY thing that varies between pages. `keys` are the
// sync-store PK columns (chain_id + block/index) → BigInt-comparable. The keyset cursor advances by the
// previous page's tail row and is INDEPENDENT of the page size, so the yielded stream is identical for
// any limit sequence.
export async function* keysetRows(
  db,
  table,
  keys,
  byteTarget = DEFAULT_BYTE_TARGET,
) {
  let last = null;
  let limit = MIN_BATCH;
  for (;;) {
    const sql = buildKeysetSql(table, keys, last !== null, limit);
    const params = [];
    if (last) {
      for (const k of keys) {
        params.push(last[k]);
      }
    }

    const { rows } = await db.query(sql, params);
    if (rows.length === 0) {
      return;
    }

    // Measure THIS page's average serialized width before yielding, then size the NEXT page's limit
    // from it. The cursor (below) is unaffected — only `limit` changes.
    let pageBytes = 0;
    for (const r of rows) {
      pageBytes += rowBytes(r);
    }

    const avg = pageBytes / rows.length;
    const wasFull = rows.length >= limit;

    for (const r of rows) {
      yield r;
    }

    // A short page (fewer rows than we asked for) means the table is exhausted — stop. Tested against
    // the limit THIS page was fetched with, exactly as before; only the value of `limit` is now
    // adaptive, never how the end-of-table test works.
    if (!wasFull) {
      return;
    }

    limit = nextBatchSize(avg, byteTarget);

    const tail = rows[rows.length - 1];
    last = {};
    for (const k of keys) {
      last[k] = tail[k];
    }
  }
}

function keyFnFor(keys) {
  return (row) => keys.map((k) => toBig(row[k]));
}

async function diffStores(
  dirA,
  dirB,
  { PGlite, strictBlocks = false, byteTarget = DEFAULT_BYTE_TARGET },
) {
  const dbA = await PGlite.create(dirA);
  const dbB = await PGlite.create(dirB);
  let fail = 0;
  const specs = resolveTableSpecs(strictBlocks);
  console.log(
    `\nbatched byte-identity diff  (A=${dirA}  vs  B=${dirB})${strictBlocks ? '  [STRICT_BLOCKS: portal-vs-portal]' : ''}  [byte-target=${byteTarget}]\n`,
  );

  for (const [table, spec] of Object.entries(specs)) {
    const keyFn = keyFnFor(spec.keys);
    const streamA = keysetRows(dbA, table, spec.keys, byteTarget);
    const streamB = keysetRows(dbB, table, spec.keys, byteTarget);
    const r = await streamingDiff(streamA, streamB, {
      keyFn,
      drop: spec.drop,
      mode: spec.mode,
    });
    const ok = !r.fail;
    if (!ok) {
      fail = 1;
    }

    let extra;
    if (spec.mode === 'blocks') {
      extra =
        `  shared=${r.shared} match` +
        (r.sizeTolerated
          ? `, ${r.sizeTolerated} tolerated (upstream size-only derivation, issues #76/#106)`
          : '') +
        (r.baseFeeTolerated
          ? `, ${r.baseFeeTolerated} tolerated (pre-London base_fee null-vs-0, canonical absent)`
          : '') +
        (r.onlyB ? `, +${r.onlyB} inert event-less (RPC-only)` : '') +
        (r.onlyA ? `, +${r.onlyA} portal-only` : '') +
        (r.mismatch ? `  | ${r.mismatch} shared MISMATCH` : '');
    } else if (r.fail) {
      extra = `  | portal-only=${r.onlyA} rpc-only=${r.onlyB} mismatch=${r.mismatch}`;
    } else if (r.accessListTolerated) {
      // transactions mode: byte-identical except the tolerated access_list column gap — surface the
      // count so a tolerated cell is never reported as a silent "identical" (issues #83/#32).
      extra = `  identical (${r.accessListTolerated} tolerated: Portal access_list column gap, issues #83/#32)`;
    } else {
      extra = '  identical';
    }
    console.log(
      `  ${ok ? '✅' : '❌'} ${table.padEnd(20)} portal=${String(r.aCount).padStart(8)}  rpc=${String(r.bCount).padStart(8)}${extra}`,
    );
    for (const s of r.samples) {
      console.log(`       ${s}`);
    }
  }

  await dbA.close();
  await dbB.close();
  console.log(
    fail
      ? '\n❌ DIVERGENCE — not byte-identical (see rows above)\n'
      : '\n✅ BYTE-IDENTICAL across logs / transactions / receipts / traces (event-bearing blocks too)\n',
  );
  console.log(`RESULT_JSON ${JSON.stringify({ fail: !!fail })}`);

  return fail;
}

// Ordered md5 over every user table in `schema` (the app tables ponder writes from ponder.schema.ts).
// Deterministic column order + ORDER BY the row's canonical text repr → identical data ⇒ identical
// hash on both sides, independent of physical row order. This is the determinism checkpoint.
async function appHash(dir, schema, { PGlite }) {
  const db = await PGlite.create(dir);
  const resolvedSchema = schema ?? (await pickAppSchema(db));
  // Only the USER tables (from ponder.schema.ts) are the determinism signal. ponder reserves the
  // `_` prefix for its own tables (_ponder_checkpoint, _ponder_meta, _reorg__*), which carry
  // per-run state (build id, checkpoints) that legitimately differs between two runs — exclude them.
  const { rows: tables } = await db.query(
    `select table_name from information_schema.tables
       where table_schema=$1 and table_type='BASE TABLE' and substr(table_name,1,1) <> '_'
       order by table_name`,
    [resolvedSchema],
  );

  const out = { schema: resolvedSchema, tables: {}, rowCounts: {} };
  const combined = createHash('md5');
  let nonEmptyTables = 0;
  for (const { table_name: t } of tables) {
    const { rows: cols } = await db.query(
      `select column_name from information_schema.columns
         where table_schema=$1 and table_name=$2 order by column_name`,
      [resolvedSchema, t],
    );
    if (cols.length === 0) {
      continue;
    }

    const repr = cols
      .map((c) => `coalesce("${c.column_name}"::text,'∅')`)
      .join(`||'|'||`);
    const { rows } = await db.query(
      `select count(*)::int as n, coalesce(md5(string_agg(r, chr(10) order by r)), 'empty') as h
         from (select ${repr} as r from "${resolvedSchema}"."${t}") s`,
    );
    const h = rows[0].h;
    const n = rows[0].n;
    out.tables[t] = h;
    out.rowCounts[t] = n;
    if (n > 0) {
      nonEmptyTables += 1;
    }

    combined.update(`${t}:${h}\n`);
  }

  await db.close();
  out.combined = combined.digest('hex');
  // The determinism checkpoint is only meaningful over tables the app actually WROTE. The diff apps
  // ship a no-op `noop` table (they exist to populate ponder_sync, not user tables), so an app hash
  // over zero nonempty user tables is VACUOUS — it must not read as a meaningful PASS.
  out.nonEmptyTables = nonEmptyTables;

  return out;
}

// Verdict for the two-store --app-hash comparison. A meaningful PASS requires BOTH sides to have at
// least one nonempty user table AND identical combined hashes. Zero nonempty user tables on either
// side is NOT a pass — it is an explicit NO-USER-TABLES verdict (the diff apps write no user rows),
// so the determinism checkpoint can never be silently vacuous. Pure + exported for unit tests.
export function appHashVerdict(ha, hb) {
  if (ha.nonEmptyTables === 0 || hb.nonEmptyTables === 0) {
    return {
      ok: false,
      verdict: 'NO-USER-TABLES',
      reason:
        'app hash is vacuous — no nonempty user tables to compare (the diff apps write only ' +
        'ponder_sync, no user rows). Use an app that writes deterministic rows for a real checkpoint.',
    };
  }

  if (ha.combined !== hb.combined) {
    return { ok: false, verdict: 'DIVERGE', reason: 'app-table hashes differ' };
  }

  return { ok: true, verdict: 'PASS', reason: 'app tables identical' };
}

// The app schema = the one non-system, non-ponder_sync schema ponder created for this instance.
async function pickAppSchema(db) {
  const { rows } = await db.query(
    `select schema_name from information_schema.schemata
       where schema_name not in ('ponder_sync','information_schema','pg_catalog','pg_toast','public')
         and schema_name not like 'pg_%' order by schema_name`,
  );
  if (rows.length === 0) {
    return 'public';
  }

  return rows[0].schema_name;
}

async function main() {
  const argv = process.argv.slice(2);
  const { PGlite } = await import('@electric-sql/pglite');

  if (argv[0] === '--app-hash') {
    const dir = argv[1];
    const schemaFlag = argv.indexOf('--schema');
    const schema = schemaFlag >= 0 ? argv[schemaFlag + 1] : undefined;
    if (!dir) {
      console.error(
        'usage: diff-batched.mjs --app-hash <pgliteDir> [--schema NAME]',
      );
      process.exit(2);
    }

    console.log(
      JSON.stringify(await appHash(dir, schema, { PGlite }), null, 2),
    );

    return;
  }

  // Positional dirs are every arg that is not a flag or a flag's value. Only --byte-target takes a
  // value; drop it and the token after it so the dirs resolve wherever the flag sits on the line.
  const positionals = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--byte-target') {
      i += 1;

      continue;
    }
    if (arg.startsWith('--')) {
      continue;
    }

    positionals.push(arg);
  }

  const [dirA, dirB] = positionals;
  if (!dirA || !dirB) {
    console.error(
      'usage: diff-batched.mjs <pgliteDirA> <pgliteDirB> [--app-hash] [--strict-blocks] [--byte-target <bytes>]',
    );
    process.exit(2);
  }

  // STRICT_BLOCKS=1 or --strict-blocks promotes the blocks table to strict for a portal-vs-PORTAL
  // diff (e.g. chaos resume vs baseline) where a one-sided block on EITHER side is a real gap.
  const strictBlocks =
    process.env.STRICT_BLOCKS === '1' || argv.includes('--strict-blocks');
  // --byte-target <bytes> (or DIFF_BYTE_TARGET) overrides the per-query payload target (issue #63).
  const byteTarget = parseByteTarget(argv, process.env);
  const fail = await diffStores(dirA, dirB, {
    PGlite,
    strictBlocks,
    byteTarget,
  });

  if (argv.includes('--app-hash')) {
    const [ha, hb] = await Promise.all([
      appHash(dirA, undefined, { PGlite }),
      appHash(dirB, undefined, { PGlite }),
    ]);
    const v = appHashVerdict(ha, hb);
    console.log(
      `\napp-table determinism hash  portal=${ha.combined} (${ha.nonEmptyTables} nonempty)  rpc=${hb.combined} (${hb.nonEmptyTables} nonempty)`,
    );
    console.log(v.ok ? `✅ ${v.reason}` : `❌ ${v.verdict}: ${v.reason}`);
    if (!v.ok) {
      // a vacuous (NO-USER-TABLES) or divergent app hash is a non-zero exit: the determinism
      // checkpoint must never silently pass on zero user rows.
      process.exit(1);
    }
  }

  process.exit(fail);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(e);
    process.exit(2);
  });
}
