/**
 * portal-child-dedupe.ts — INV-17 write-side idempotence for factory children (LEAST semantics).
 *
 * Upstream's `insertChildAddresses` is a plain INSERT with no ON CONFLICT, and `ponder_sync.
 * factory_addresses` has no `UNIQUE (factory_id, chain_id, address)`, so a writer that re-flushes an
 * already-persisted child set durably DUPLICATES those rows. Before every `insertChildAddresses` the fork
 * reads the store's persisted rows (`getChildAddresses`) and keeps a child only if it is not yet persisted
 * OR is re-discovered at a STRICTLY LOWER creation block (the write-side analogue of the read side's
 * min-merge). App behavior is unaffected either way (the only consumer min-merges to a set), but this keeps
 * store-identity/digest tooling exact and stops the table growing unbounded under a re-running writer.
 *
 * This is the SHARED core for BOTH sync modes:
 *   • historical `syncBlockRangeData` (portal.ts `persistPendingChildren`) — resumed backfill re-flush;
 *   • realtime `handleRealtimeSyncEvent` finalize (runtime/realtime.ts, via `dedupeFinalizeChildAddresses`)
 *     — a resumed single-writer that re-streams the same finalized child-creation blocks.
 * Keeping ONE implementation keeps the doc⟷code⟷test correspondence exact and the per-version wiring hook
 * a minimal call-site redirect rather than logic duplicated into the upstream file.
 */

import type { Address } from 'viem';
import type { Factory } from '@/internal/types.js';

/**
 * STORE IDENTITY of a factory: the `factory` value minus `id`/`sourceId`. Both `insertChildAddresses` and
 * `getChildAddresses` strip `id`/`sourceId` (`const { id, sourceId: _sourceId, ..._factory } = factory`)
 * and upsert/select the `factories` row keyed on the remaining value (`UNIQUE (factory)` — sync-store/
 * index.ts). Two sources whose factories are identical except `id`/`sourceId` (a legal config: two
 * contracts sharing one factory) therefore map to the SAME store row. The dedupe groups by this key first
 * so two aliases can't each read absence and both insert the same children (a first-flush duplicate the
 * read-side min-merge cannot repair). Deterministic: sort the surviving keys so field order never splits an
 * alias pair. Value shapes (`address` may be an array) are compared by JSON.stringify — exact for the
 * store's stored `_factory` value.
 */
export const storeFactoryKey = (factory: Factory): string => {
  const rest: Record<string, unknown> = {};
  for (const key of Object.keys(factory).sort()) {
    if (key === 'id' || key === 'sourceId') continue;

    rest[key] = (factory as unknown as Record<string, unknown>)[key];
  }

  return JSON.stringify(rest);
};

/** A discovered child set: `[factory, address → creation-block]` pairs, as flushed to the store. */
export type ChildFlush = [Factory, Map<Address, number>][];

/** Reads the store's persisted child map for a factory (upstream `syncStore.getChildAddresses`). */
export type ChildAddressReader = (args: {
  factory: Factory;
}) => Promise<Map<Address, number>>;

/**
 * INV-17 read→dedupe (LEAST semantics), shared by both sync modes. Given a set of pending child flushes and
 * a reader over the store's persisted rows, return only the children that must actually be inserted:
 * grouped by store identity (one read + one insert per canonical factory), keeping a child only when it is
 * absent from the store OR re-discovered at a STRICTLY LOWER creation block. Case-insensitive against the
 * persisted rows (children are lowercased at discovery; `getChildAddresses` min-merges the stored text
 * case-SENSITIVELY, so a checksummed pre-existing row is normalized to lowercase before comparison).
 *
 * The result's `factory` for each group is one representative member — inserting once under any member
 * suffices for the whole alias group because `insertChildAddresses` resolves `factory_id` from the same
 * stripped value. Groups whose children are all already persisted are omitted entirely (no insert).
 *
 * The reader is a live `getChildAddresses` SELECT per canonical factory (INV-17 candor: uncached, so
 * worst-case ~O(N²) over a long accreting backfill — correctness first; a read-through min-map cache is the
 * documented follow-up). The caller runs this inside the SAME store transaction as the insert, so for a
 * single writer the read→dedupe→insert is transactional (no interleaving read/insert slips a duplicate
 * past it). Two concurrent writers are two transactions and remain the documented INV-17 TOCTOU residual.
 */
export const dedupeChildAddressesAgainstStore = async (
  flush: ChildFlush,
  getChildAddresses: ChildAddressReader,
): Promise<ChildFlush> => {
  const groups = new Map<
    string,
    { factory: Factory; children: Map<Address, number> }
  >();
  for (const [factory, children] of flush) {
    const key = storeFactoryKey(factory);
    let group = groups.get(key);
    if (group === undefined) {
      group = { factory, children: new Map() };
      groups.set(key, group);
    }
    for (const [address, block] of children) {
      const prev = group.children.get(address);
      if (prev === undefined || prev > block) {
        group.children.set(address, block);
      }
    }
  }

  const deduped: ChildFlush = [];
  for (const { factory, children } of groups.values()) {
    const persisted = await getChildAddresses({ factory });
    // getChildAddresses returns stored text verbatim and min-merges case-SENSITIVELY, so a checksummed
    // pre-existing row would not match a discovered child. Discovery lowercases every child, and upstream
    // runtime matching lowercases its lookups, so the canonical child key is lowercase — normalize the
    // persisted map to that before comparing.
    const persistedLower = new Map<Address, number>();
    for (const [address, block] of persisted) {
      const lower = address.toLowerCase() as Address;
      const prev = persistedLower.get(lower);
      if (prev === undefined || prev > block) {
        persistedLower.set(lower, block);
      }
    }

    let toInsert: Map<Address, number> | undefined;
    for (const [address, block] of children) {
      const prev = persistedLower.get(address);
      if (prev !== undefined && prev <= block) continue;

      if (toInsert === undefined) toInsert = new Map();
      toInsert.set(address, block);
    }
    if (toInsert !== undefined) deduped.push([factory, toInsert]);
  }

  return deduped;
};

/** A store handle exposing the one read the finalize dedupe needs (subset of the tx-scoped syncStore). */
export type FinalizeSyncStore = {
  getChildAddresses: (args: {
    factory: Factory;
  }) => Promise<Map<Address, number>> | Map<Address, number>;
};

/**
 * REALTIME finalize call-site hook (INV-17, extended to the realtime finalize path). Deduplicates the
 * `Map<Factory, Map<Address, number>>` that `handleRealtimeSyncEvent` builds from the newly-finalized
 * blocks against the tx-scoped store, returning a map in the SAME shape ready to spread into the existing
 * `insertChildAddresses` calls. Run INSIDE the finalize transaction (the read must see committed history
 * and share the insert's transaction). A resumed single-writer re-streams the same finalized
 * child-creation blocks and would otherwise re-INSERT the same children — this closes that.
 *
 * Kept as a thin wrapper over `dedupeChildAddressesAgainstStore` so the historical and realtime paths run
 * byte-identical dedupe logic. `getChildAddresses` on the real store returns a Promise; the wrapper awaits
 * uniformly (test doubles may return a plain Map).
 */
export const dedupeFinalizeChildAddresses = async (
  childAddresses: Map<Factory, Map<Address, number>>,
  syncStore: FinalizeSyncStore,
): Promise<Map<Factory, Map<Address, number>>> => {
  const deduped = await dedupeChildAddressesAgainstStore(
    Array.from(childAddresses.entries()),
    async ({ factory }) => syncStore.getChildAddresses({ factory }),
  );

  return new Map(deduped);
};
