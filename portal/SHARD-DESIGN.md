# SHARD-DESIGN.md — issue #194: factory child-address query sharding

**Status:** HISTORICAL-plane fix IMPLEMENTED on `fix/194-factory-shard` (committee-gate pending merge).
Realtime `/stream` sharding remains scoped-out (§4 Q1) — a tracked follow-up; realtime keeps its
current fail-loud + RPC-realtime escape. The tx (from/to) path is unchanged (§3.4, §4 Q2) — follow-up.

**Implementation notes (what the code does, vs the design above):**
- `SHARD_BODY_BUDGET = MAX_RAW_QUERY_SIZE − SHARD_SAFETY_MARGIN`, with `SHARD_SAFETY_MARGIN = 8 KiB`
  (a fixed few-KiB allowance per §3.2/§4 Q4). The margin covers the `{...query, fromBlock, toBlock}`
  envelope client.stream adds (~tens of bytes) plus any server-side accounting slack; each shard is
  serialized whole and required `< SHARD_BODY_BUDGET`, so shards land with clear headroom below the cap.
- **Partition granularity = the merged `logs[]` array ELEMENTS, never inside one element.** Each element
  is already ≤ `PORTAL_MAX_ADDRESSES` (1000) addresses ≈ 45 KiB (`logRequestsFor` batches at that
  width), so a single element always fits the budget — `PORTAL_MAX_ADDRESSES` COEXISTS as the
  within-shard batch size, the byte budget is the shard boundary (§4 Q3, coexist). `shardLogs` greedy
  bin-packs elements in merged order; when the whole array fits one body it returns EXACTLY ONE shard
  whose `logs` IS the input array (same objects, same order) ⇒ byte-identical to `logQuery()`.
- **Scope of "log filters":** BOTH factory-child AND plain-address log filters shard (the design's
  scope exclusion is the tx path, not plain-address log filters). Both feed one merged `logs` array;
  `shardLogs` partitions the union of all address-bearing log requests (§3.2 multi-filter).
- `logQuery()` is retained (single-body view; `= logQueryShards()[0]` below the wall) so callers/tests
  that don't shard are unaffected. Both build from ONE `mergedLogRequests()` source so they can't drift.
- The pre-#194 portal.ts-level "over-limit body fails loud" behavior is intentionally REMOVED for log
  filters (they now shard). The client-level size guard (`client.stream`, portal-client.ts:488) is
  UNCHANGED and still fails loud on a single un-shardable body — it protects the tx path.
- Completeness gate mutation-VERIFIED by hand: neutering the gate (`break` after the first shard in
  `runStreams`) turned both completeness-gate tests RED (test-red, not build-red) — the later shard's
  rows were silently dropped; restoring the shard loop made them green.

---

**Original design status:** design + failing repro. The fix is committee-gated
(full 4-model, correctness-critical surface) before implementation.

**The bug (field report, coordinator-verified):** a factory app hard-stops once its discovered
child-address set is large enough that the Portal query **body** exceeds Portal's 256 KiB raw-query
cap (`MAX_RAW_QUERY_SIZE`), at roughly **~5.8k children**. The largest factory case validated to
date is 872 children ("F-full"), so the wall has never been exercised. Pendle is factory-heavy
(markets / SY / PT / YT), so per-chain child counts will exceed 5.8k and hit this wall immediately.

---

## 1. MAP — exact code path, both planes

### 1.1 Historical (backfill) plane — the wall

```
discovery grows childAddresses            portal-discovery.ts (Discovery.commit → childAddresses map)
   │  ChildAddresses = Map<FactoryId, Map<Address, number>>            portal-filters.ts:34
   ▼
spec.logQuery()                            portal-filters.ts:392-406
   │  logFilters.flatMap(f => logRequestsFor(f, childAddresses))       portal-filters.ts:394
   │    └ logRequestsFor: factory filter → Array.from(childAddresses.get(id).keys())   :195
   │      then BATCHES into PortalLogRequest[] of PORTAL_MAX_ADDRESSES (1000) each      :205-209
   │  mergeLogRequests(...)  — collapses same-address+topic1..3, unions topic0          :219-245
   │  → returns a SINGLE PortalQuery whose `logs: PortalLogRequest[]` holds ALL batches :397-405
   ▼
portal.ts runStreams → client.stream(lq, from, to, ...)               portal.ts:369-393
   ▼
client.stream: body = JSON.stringify({ ...stripFields(query), fromBlock, toBlock })   portal-client.ts:668-672
   │  → the WHOLE `logs` array (every batch) is serialized into ONE body
   ▼
fetchBatch(body, cursor, to): if (body.length > MAX_RAW_QUERY_SIZE) throw  portal-client.ts:488-510
   → PERMANENT HARD STOP (a plain Error; see §1.4)
```

**Why batching + merge do NOT help.** `PORTAL_MAX_ADDRESSES=1000` (`portal-filters.ts:62`) splits the
child set into `ceil(N/1000)` `PortalLogRequest` entries (`portal-filters.ts:205-209`), and
`mergeLogRequests` collapses per-event duplication (`portal-filters.ts:219-245`) — but **all** of
those entries land in the same `logs: PortalLogRequest[]` array of the one `PortalQuery`
(`portal-filters.ts:397-405`), which `client.stream` serializes whole
(`portal-client.ts:668`). The body's dominant term is the concatenated address strings, so
`body.length ≈ N × ~45 bytes` (one lowercased `"0x…"` address + comma ≈ 45 bytes). `256×1024 / 45 ≈
5825` → the ~5.8k wall. Batching bounds the *per-entry* count, never the *total*.

**Range bisection cannot help.** The cap is on request **bytes**, and the address list is
range-independent — the same `logs` array ships for every `[fromBlock, toBlock]`. `client.stream`
already knows this: on the server-side `PortalQueryTooLargeError` it does NOT bisect, it re-throws
loud (`portal-client.ts:676-682`).

### 1.2 The completeness join point (historical) — where a chunk is marked done

`portal.ts` `dataChunk` builds one cache entry per chunk `idx`. The chunk promise:

```
const p = (async () => {
  await discovery.ensure(ensureTo, ...);        // INV-3: children ≤ this window known   portal.ts:673
  invariant('INV-3', discoveryReady(to), ...);                                          portal.ts:676
  const cd = createChunkData();
  await runStreams(cd, from, to, token);        // ← ALL four streams drain here          portal.ts:684
  return cd;
})();
const entry = { promise: p, specId: spec.id, coveredFrom: from, coveredTo: to, token };
dataCache.set(idx, entry);                       // cached by idx ALONE (INV-1)            portal.ts:695
```

`runStreams` (`portal.ts:326-457`) sequentially drains the log, trace, block, and tx streams into
`cd`. **The chunk is "done" precisely when `runStreams` returns and `p` resolves** — that resolution
is the single completeness gate. The chunk is then cached by idx alone (INV-1), and the interval it
serves is marked cached by ponder's core once assembly consumes `cd`. **This is the exact join point
the sharding fix must gate on ALL shards** (see §3.3): if `runStreams` returns before every shard's
rows are merged into `cd`, the chunk caches an incomplete data set → the un-streamed shard's rows are
silently lost — the INV-3/INV-11 silent-data-loss class.

### 1.3 Realtime (`/stream`) plane — the SAME wall, different fail-loud

The realtime wire builds its filter with the SAME builder:

```
buildPortalLogRequests(eventCallbacks, childAddresses)   portal-realtime-wire.ts:592, rebuilt :712-716
   → logs: PortalLogRequest[]  (per-filter requests + one discovery request per factory)  portal-filters.ts:263-278
   ▼
portal-realtime.ts streamHotBlocks:
   body = JSON.stringify({ type:'evm', fromBlock, includeAllBlocks:true, fields, logs: args.logs })  portal-realtime.ts:423-437
```

The realtime path builds the whole `logs` array into ONE `/stream` body with **no proactive
size-check** (unlike the historical `fetchBatch` guard). The code comment at
`portal-realtime.ts:381-386` already acknowledges the shape: *"Its `logs` is O(total children) — up
to ~100k filter rows on a busy factory chain (~4.6MB serialized)"* — i.e. the maintainers know the
realtime body is O(total children) and unbounded.

When that body exceeds the cap, Portal returns a 400 "query is too large". Realtime's 4xx handler
(`portal-realtime.ts:534-556`) treats any non-droppable-field 4xx as deterministic and **fails loud**:

```
throw new Error(`Portal /stream rejected the realtime query (HTTP ${res.status})… — deterministic,
  not retried. PORTAL_REALTIME=stream cannot serve this configuration; unset it to use RPC realtime.`)
```

So **realtime IS affected** — it hits the same wall and hard-stops, but with an RPC-realtime escape
hatch (`unset PORTAL_REALTIME=stream`) rather than a total dead-end. That escape hatch means realtime
is *degradable today* (fall back to RPC realtime), whereas historical backfill has no fallback — so
**historical is the higher-priority fix**, but a complete #194 fix should shard realtime too so a
factory-heavy chain can stay on Portal `/stream`.

### 1.4 The #23 "filtered fallback" does NOT interact with this wall

The `#23`/`#145` "filtered fallback" (`realtime-fallback-e2e.test.ts:6,17,218-241`) is about a
**RESPONSE** being too large: an *unfiltered* full-block `eth_getLogs` fallback (the RPC path used to
fill a Portal gap) whose response body exceeds a limit → re-issue the request **with topics** (the
"filtered fallback") to shrink the *response*. It throws `ResponseBodyTooLargeError`, not a
request-body cap. That is orthogonal to #194's **request-body** 256 KiB wall and provides no
mitigation for it. Confirmed: the realtime `/stream` request-body wall is un-addressed by #23.

### 1.5 Two distinct historical fail-loud sites (both permanent)

There are two request-body-too-large fail-louds in the historical client, and they behave differently:

- **Proactive guard** (`portal-client.ts:488-510`): fires when `body.length > MAX_RAW_QUERY_SIZE`
  *before* any POST. It throws a **plain `Error`** — NOT `PortalQueryTooLargeError` — so it is **not**
  caught by the `if (err instanceof PortalQueryTooLargeError)` handler at `portal-client.ts:676`. It
  propagates straight out of `stream` as a permanent stop.
- **Server 400 → `PortalQueryTooLargeError`** (`portal-client.ts:593` → caught `:676-682`): fires if a
  body *under* our local cap estimate still 400s server-side. Re-thrown loud with the
  `PORTAL_MAX_ADDRESSES` lever.

Either way the chain permanently stops. The repro asserts the proactive guard (the reachable path for
a >5.8k factory, since our estimate ≈ the server's).

---

## 2. FAILING REPRO (the FAIL→PASS anchor)

`portal/portal-shard.test.ts` — pure/unit at the portal-filters ↔ portal-client seam, no network.
Three tests:

1. **`#194: a >5.8k-child factory blows the historical logQuery body past MAX_RAW_QUERY_SIZE`** —
   synthesizes a factory with 6000 distinct children, compiles the fetch-spec, and asserts:
   `logQuery().logs` holds all 6000 addresses across `ceil(6000/1000)=6` batch entries in ONE array,
   and the serialized body `> MAX_RAW_QUERY_SIZE`.
2. **`#194: the wall scales with the child count, not the batch count (a <5.8k factory fits)`** — 872
   children (the largest validated case) fits one body (`< MAX_RAW_QUERY_SIZE`); 6000 overflows. This
   is the **byte-identity / no-op control anchor**: the fix must NOT shard the 872 case.
3. **`#194: portal-client fails loud on the oversized factory body (the current hard stop)`** —
   drains `client.stream(query, …)` with a `fetchImpl` that must never be reached; asserts the drain
   rejects with `/MAX_RAW_QUERY_SIZE|too large/i` and that no POST was attempted (`posted === false`).

**How it proves the wall on today's code:** on the current code these assertions PASS *as descriptions
of the broken behaviour* — the body genuinely exceeds the cap and the client genuinely throws. When
the sharding fix lands, tests (1) and (3)'s current expectations INVERT (a sharded plan yields
multiple bodies each under the cap; the stream drains without a fail-loud). At that point these tests
are rewritten to assert the sharded behaviour, and the "body > cap for a single unsharded body" claim
moves to a lower-level assertion on the pre-shard builder. Test (2)'s fits-one-body / no-op control
survives unchanged and becomes the byte-identity guard.

> Repro run evidence is recorded in the PR / ledger (harness: `scripts/sync-upstream.sh 0.17.1
> --test`, config `portal/vite.portal.config.ts`).

---

## 3. DESIGN — query sharding with completeness preserved

### 3.1 Goal & shape

Partition a factory's child-address set across MULTIPLE Portal query **requests** (multiple
POSTs / `/stream` opens), each request's body `< MAX_RAW_QUERY_SIZE`, then **UNION** the results. The
union is naturally lossless at the data layer: each shard is a disjoint subset of the address filter,
the rows returned are additive, and downstream assembly re-matches every row against the full filter
set anyway (`buildRawLogMatcher`, `portal.ts:349`).

### 3.2 Partitioning algorithm (byte-budgeted, not count-budgeted)

The wall is on **bytes**, and the body carries more than addresses (topics, field projections, block
bounds, and — critically — the SAME topic/field overhead is repeated in every shard). So sharding must
be **byte-budgeted**:

1. Compute the **fixed overhead** of a query body with an EMPTY address list for this spec: the
   `fields` projection, `type`, `includeAllBlocks`, `fromBlock`/`toBlock`, and the non-address filter
   dimensions (topic0..3, `transaction:true`). Call it `H`.
2. Compute the **per-address cost** `A` (a lowercased `"0x…"` element + separator ≈ 45 bytes; use the
   real serialized length, not a constant, to stay exact across address casings/lengths).
3. The **address budget per shard** is `floor((MAX_RAW_QUERY_SIZE − H − safetyMargin) / A)`, with a
   safety margin (e.g. a few KiB) for JSON structural bytes and any server-side accounting that counts
   slightly more than our estimate. This budget REPLACES the fixed `PORTAL_MAX_ADDRESSES=1000` cut as
   the *shard* boundary (the 1000-cut may remain as the *within-body* batch entry size, or be folded
   in — an open question, §4).
4. Greedily pack children into shards of ≤ that budget. Each shard is a full `PortalQuery` with the
   SAME `fields`/topics but a DISJOINT address subset. `mergeLogRequests` still runs *within* each
   shard.

**Multi-factory / multi-filter chains.** A chain's `logQuery` unions requests across all log filters
AND all factories. The byte budget must account for the full merged `logs` array — sharding partitions
the *union of all address-bearing requests*, not one factory in isolation. A request with NO address
list (a plain match-all-topic filter) is tiny and rides in shard 0.

### 3.3 Streaming shards & the completeness gate (THE INV-1/INV-3/INV-11 argument)

This is the correctness core. The invariant to preserve, verbatim from `portal-filters.ts:13-18` and
INVARIANTS.md INV-1: *"Chunks are cached by idx ALONE, so every chunk MUST be filter-complete — else a
filter that first needs an already-cached chunk is never streamed yet its interval is marked done."*

The completeness join point (§1.2) is `runStreams` returning. **The gate: `runStreams` must not return
until EVERY shard for the log source has fully drained into `cd`.** Concretely, the log branch of
`runStreams` (`portal.ts:369-393`) changes from *one* `client.stream(lq, …)` loop to a loop over the
shard plan:

```
for (const shardQuery of spec.logQueryShards()) {     // ← N queries, each body < cap
  for await (const blocks of client.stream(shardQuery, from, to, { neededMissing, onRows })) {
    …merge blocks into cd exactly as today…
  }
}
// runStreams returns ONLY after this outer loop completes → the chunk promise resolves ONLY then
```

Because the chunk promise resolves *after* the shard loop, and `dataCache.set(idx, entry)` /
interval-cached happens on that resolution, **the chunk is never marked done between shards.** A shard
failure (any `client.stream` throw) propagates out of `runStreams`, rejects `p`, and the existing G1
eviction (`portal.ts:698-701`) frees the token and deletes the cache entry → a later interval retries
the WHOLE chunk (all shards) fresh. There is no partial-commit window.

**Why this preserves INV-1 (filter-completeness by idx).** The shard set is a partition of the SAME
frozen `childAddresses` snapshot the un-sharded query would read (the `logQueryShards()` builder reads
the live map at call time, identical to `logQuery()` today). Union over the partition = the un-sharded
result set. The chunk is cached by idx only after the union is complete, so a later cache hit on that
idx serves a filter-complete chunk — exactly the INV-1 contract, just assembled from N POSTs instead
of 1.

**Why this preserves INV-3 (discovery-before-data).** Unchanged: `discovery.ensure` + the INV-3
invariant assert still run BEFORE any shard streams (`portal.ts:673-682`), so every child ≤ the fetch
window is known before the shard plan is built. Sharding only changes how the (already-complete) child
set is transported, not when it is discovered.

**Why this preserves INV-11 (merge equivalence).** `mergeLogRequests` still runs within each shard;
the union across shards matches exactly the same logs as the un-sharded merged set (each address
appears in exactly one shard; topics are replicated identically). The INV-11 model test extends to a
"union over a partition matches the same logs as the whole" property (§3.5).

### 3.4 The tx-filter case that CANNOT be split (portal-client.ts:508)

The fail-loud message at `portal-client.ts:508` distinguishes: *"Log filters are already
merged+batched … if this is a tx filter, its from/to set is too large to fit one request and cannot be
safely split — narrow the filter."* This is the key scoping decision.

**Sharding applies to LOG / factory-child filters ONLY, in the historical plane.** Rationale:

- A **log** query's shards each return a disjoint subset of matched logs; the union is complete and
  order-independent — assembly re-sorts/re-matches. Splitting is safe.
- A **tx** (account from/to) query: `txQuery` (`portal-filters.ts:427-449`) pushes merged from/to
  sets. In principle the from/to *address* set is also splittable the same way (disjoint address
  subsets, union the matched txs). BUT: is a factory ever the source of a from/to set large enough to
  hit the wall? `txRequestsFor` (`portal-filters.ts:313-334`) DOES expand factory children into
  `from`/`to` (`isAddressFactory(a)` → `childAddresses.get(a.id).keys()`), so a factory-driven tx
  filter CAN grow unbounded too. **Open question (§4): does #194 need tx-filter sharding, or is the
  factory→tx-filter path not exercised by Pendle/Euler?** The message's "cannot be safely split" refers
  to a *non-address* explosion (a single filter with a huge literal from/to list the user supplied),
  which genuinely can't be reduced. A factory-expanded from/to set is address-based and *is*
  splittable by the same algorithm — but that path is lower priority and should be scoped explicitly
  rather than silently sharded. **Decision for this fix: shard log/factory-child log filters; leave
  the tx path failing loud with its current message, and note the factory-tx path as a follow-up
  unless the committee finds it in scope for Pendle.**
- **Traces** are already server-side unfiltered (INV-5), so they carry no address list → no wall.
- **Blocks** (`includeAllBlocks`) carry no address list → no wall.

### 3.5 Regression tests the fix will need

1. **Byte-identity / no-op control (CRITICAL):** for a factory that fits one body (e.g. 872, ≤ budget)
   the sharded builder yields EXACTLY ONE shard whose body is byte-identical to today's un-sharded
   body. (Extends test (2) in the repro.) This proves the fix is a no-op below the wall — the
   dominant safety guarantee for the existing validated corpus.
2. **Shard count & budget:** N children just over the budget → 2 shards; each shard body `<
   MAX_RAW_QUERY_SIZE`; the multiplicity is `ceil` of the byte-budget, not the 1000-count.
3. **Union completeness (INV-11 extension):** a fast-check property — union of the shard plan matches
   exactly the same (address, topic0..3) logs as the un-sharded merged set (partition ⇒ same match
   set). Mutation-verified: dropping a shard fails it.
4. **Completeness gate (INV-1/INV-3, the silent-loss guard):** a `portal.ts`-level test — a chunk over
   a >budget factory streams ALL shards before the chunk promise resolves; a shard that throws
   mid-plan rejects the chunk (no partial cache-set), and a retry re-streams every shard. Mutation-
   verified: marking the chunk done after the FIRST shard (the bug this fix must not reintroduce) drops
   the later shards' rows and fails a row-count/coverage assertion.
5. **Realtime shard (if realtime is fixed this pass):** the `/stream` body for a >budget factory is
   built as ≤-cap shards; a shard that 400s is handled without dropping the others. (Realtime sharding
   changes the connection model — multiple concurrent `/stream` opens or sequential — which is a
   larger change; may be scoped to a follow-up with the current fail-loud + RPC-realtime escape
   retained. See §4.)
6. **Both-version gate:** `scripts/sync-upstream.sh 0.17.1 --test` green (and the prior version per
   release policy).

### 3.6 Where the fix lives (surgical footprint)

- `portal-filters.ts`: add `logQueryShards(): PortalQuery[]` (byte-budgeted partition of the current
  `logQuery` address union). `logQuery()` becomes the single-shard case / is expressed via shards.
- `portal.ts` `runStreams` log branch: loop over `spec.logQueryShards()` instead of one `logQuery()`.
  The completeness gate is preserved by construction (the loop is inside `runStreams`, before `p`
  resolves).
- `portal-realtime.ts` / `portal-realtime-wire.ts`: shard the `/stream` body OR retain the fail-loud +
  RPC-realtime escape and scope realtime to a follow-up (committee call).
- No seam change (HistoricalSync.syncBlockRangeData/syncBlockData untouched). No public-surface change.

---

## 4. Open questions for the committee

1. **Realtime scope this pass.** Historical has no fallback (must shard). Realtime already degrades to
   RPC realtime (`unset PORTAL_REALTIME=stream`). Do we shard realtime `/stream` now — which means
   multiple concurrent `/stream` opens or a sequential multi-open loop, a non-trivial change to the
   reorg/redelivery machine — or ship historical sharding + keep realtime's loud fail + escape hatch,
   as a scoped follow-up? Recommendation: historical this pass; realtime scoped-out with rationale +
   a tracked follow-up, unless Pendle needs Portal `/stream` realtime at >5.8k children on day one.
2. **factory-expanded tx (from/to) sharding.** `txRequestsFor` expands factory children into from/to
   sets, so a factory-driven tx filter can hit the wall too — and it IS address-splittable (contra the
   "cannot be safely split" message, which is about a huge *literal* user-supplied list). Is the
   factory→tx-filter path in scope for Pendle/Euler, or do we ship log-only sharding and leave tx
   failing loud (narrow-the-filter) as today? Recommendation: log-only now; tx follow-up if a real
   config exercises it.
3. **`PORTAL_MAX_ADDRESSES` fate.** Does the byte-budgeted shard boundary REPLACE the 1000-count batch
   cut, subsume it, or coexist (1000 = within-shard entry size, byte-budget = shard boundary)? Coexist
   is the least-invasive; folding is cleaner. Committee call on the API surface (the env var is
   documented / referenced in fail-loud messages).
4. **Safety margin sizing.** The proactive local cap must stay strictly below the server's true cap so
   we never ship a shard that 400s server-side (which would then fail loud despite sharding). What
   margin is safe (a fixed few-KiB, or a percentage)? Needs one empirical check against a real Portal
   `MAX_RAW_QUERY_SIZE` 400 boundary if the estimate proves imprecise.
5. **Shard fetch concurrency (historical).** Stream shards sequentially (simplest, preserves the
   completeness gate trivially) or concurrently (faster, but must still join ALL before `runStreams`
   returns and must respect the gate's concurrency budget)? Sequential is the safe first cut;
   concurrency is a perf follow-up under the same completeness gate.
