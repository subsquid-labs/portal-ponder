import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { hexToBigInt } from 'viem';
import { expect, test } from 'vitest';
import { traceSafeChunkBlocks } from './portal-chunks.js';
import {
  cmpTraceAddr,
  hx,
  isFinalityGap,
  parityToCallFrame,
  toSyncReceipt,
  toSyncTransaction,
} from './portal-transform.js';

// C7: hx("0x") used to return the invalid quantity "0x" (throws downstream in BigInt); empty
// strings must normalize to "0x0". Decimal numbers/strings and existing hex pass through.
test('hx: empty quantity → 0x0 (never invalid 0x); decimal/hex normalize', () => {
  expect(hx('0x')).toBe('0x0');
  expect(hx('')).toBe('0x0');
  expect(hx('0x1a')).toBe('0x1a');
  expect(hx(26)).toBe('0x1a');
  expect(hx('26')).toBe('0x1a'); // decimal string
  expect(hx(0)).toBe('0x0');
  expect(hx(0n)).toBe('0x0');
});

/**
 * Unit tests over REAL Portal NDJSON captured at eth block 21,000,000 (+ base).
 * They pin the type mismatches the schema audit flagged — Portal's split encoding
 * (status/type decimal, gas/value hex), trace callType at action.callType,
 * staticcall value:null, CREATE init/code.
 */
const FIX = join(__dirname, '__fixtures__');
const load = (f: string): any[] =>
  readFileSync(join(FIX, f), 'utf8')
    .trim()
    .split('\n')
    .map((l) => JSON.parse(l));
const allTx = load('receipts.json').flatMap((b) =>
  (b.transactions ?? []).map((t: any) => ({ t, h: b.header })),
);
const allTrace = load('traces.json').flatMap((b) => b.traces ?? []);

// Regression for a byte-identity divergence vs the RPC path (harness/diff): Portal returns
// accessList=[] for every tx, but the RPC sync stores accessList only on TYPED txs (EIP-2930/
// 1559/4844, type ≥ 1) and null on legacy (type 0). toSyncTransaction must match that exactly.
test('accessList matches the RPC shape: legacy → none, typed → [] or list', () => {
  const h = { hash: '0xb10c', number: 100 } as any;
  const al = [{ address: '0xabc', storageKeys: ['0x1'] }];
  // legacy (type 0): Portal sends [] but RPC has no accessList → must drop to undefined (→ null col)
  expect(
    (toSyncTransaction({ type: 0, accessList: [] }, h) as any).accessList,
  ).toBeUndefined();
  // EIP-1559 with empty accessList → []
  expect(
    (toSyncTransaction({ type: 2, accessList: [] }, h) as any).accessList,
  ).toEqual([]);
  // typed with entries → passthrough
  expect(
    (toSyncTransaction({ type: 2, accessList: al }, h) as any).accessList,
  ).toEqual(al);
  // typed but Portal DROPPED the column (arbitrum/avalanche backfill) → field omitted → unknown,
  // must be undefined (SQL NULL), NEVER a fabricated [] that falsely claims "empty access list".
  expect((toSyncTransaction({ type: 1 }, h) as any).accessList).toBeUndefined();
  // typed with an explicit null (droppable-field degradation) → undefined, not []
  expect(
    (toSyncTransaction({ type: 2, accessList: null }, h) as any).accessList,
  ).toBeUndefined();
  // typed with the field omitted (undefined) → undefined, not []
  expect(
    (toSyncTransaction({ type: 2, accessList: undefined }, h) as any)
      .accessList,
  ).toBeUndefined();
});

// The RPC path (ponder `standardizeTransactions`) treats accessList as an access-list column on
// EXACTLY the four EIP-typed envelopes {1,2,3,4} and leaves every other type untouched; downstream
// `encode.ts` stores a truthy `[]` as the string "[]" (NOT null). Portal, though, FABRICATES
// `accessList: []` on EVERY tx regardless of type (verified live: eth/polygon serve `[]` on all
// legacy-0 txs). So on any non-{1,2,3,4} type (OP-stack deposit 0x7e = 126, or an unknown/system
// envelope), that fabricated `[]` must NOT be propagated — it would store "[]" where the RPC path
// stores NULL. Gating on the exact {1,2,3,4} set (not `type ≥ 1`) is what drops it. This mutation
// fails on the old `Number(tx.type) >= 1` gate, which kept the `[]` for types ≥ 5.
test('accessList: non-EIP typed tx (0x7e deposit / exotic) with Portal-fabricated [] → undefined, not []', () => {
  const h = { hash: '0xb10c', number: 100 } as any;

  // OP-stack deposit (type 0x7e = 126): carries no EIP access list; Portal's `[]` must drop.
  expect(
    (toSyncTransaction({ type: 126, accessList: [] }, h) as any).accessList,
  ).toBeUndefined();

  // Unknown/system envelope (e.g. Arbitrum-internal 0x6a = 106): same — `[]` must drop.
  expect(
    (toSyncTransaction({ type: 106, accessList: [] }, h) as any).accessList,
  ).toBeUndefined();

  // A real (non-empty) list on a non-EIP type is likewise dropped — the RPC path stores no
  // access list on these envelopes, so the Portal transform must not either.
  const al = [{ address: '0xabc', storageKeys: ['0x1'] }];
  expect(
    (toSyncTransaction({ type: 126, accessList: al }, h) as any).accessList,
  ).toBeUndefined();

  // Guard the boundary: type 4 (EIP-7702) is inside the set → a real list is preserved.
  expect(
    (toSyncTransaction({ type: 4, accessList: al }, h) as any).accessList,
  ).toEqual(al);

  // type may arrive as a hex string ("0x7e") on the wire — Number() must still classify it as
  // exotic and drop the fabricated `[]`, and a real EIP list on a hex-typed envelope must survive.
  expect(
    (toSyncTransaction({ type: '0x7e', accessList: [] }, h) as any).accessList,
  ).toBeUndefined();

  expect(
    (toSyncTransaction({ type: '0x2', accessList: al }, h) as any).accessList,
  ).toEqual(al);

  // Lower boundary: legacy (type 0) and absent type both fall outside the set → dropped, matching
  // the RPC path leaving legacy/untyped txs without an access list.
  expect(
    (toSyncTransaction({ type: 0, accessList: [] }, h) as any).accessList,
  ).toBeUndefined();

  expect(
    (toSyncTransaction({ accessList: [] }, h) as any).accessList,
  ).toBeUndefined();
});

test('receipt: decimal status/type → hex; gas fields stay hex (BigInt-able)', () => {
  const ok = allTx.find(({ t }) => t.status === 1)!;
  const r = toSyncReceipt(ok.t, ok.h) as any;
  expect(r.status).toBe('0x1'); // input was the number 1, NOT "0x1"
  expect(r.type).toMatch(/^0x[0-9a-f]+$/); // input was a number (e.g. 2)
  expect(typeof ok.t.status).toBe('number'); // sanity: confirm the fixture really is decimal
  // gas fields arrive hex; must remain hex strings that hexToBigInt accepts
  expect(() => hexToBigInt(r.gasUsed)).not.toThrow();
  expect(() => hexToBigInt(r.cumulativeGasUsed)).not.toThrow();
  expect(() => hexToBigInt(r.effectiveGasPrice)).not.toThrow();
});

test('receipt: failed tx (status 0) → 0x0', () => {
  const failed = allTx.find(({ t }) => t.status === 0);
  if (!failed) return; // fixture may not include one in this slice
  expect((toSyncReceipt(failed.t, failed.h) as any).status).toBe('0x0');
});

test('receipt: contract-creation contractAddress lowercased; non-creation null', () => {
  const creation = allTx.find(({ t }) => t.contractAddress);
  const transfer = allTx.find(({ t }) => t.contractAddress === null)!;
  expect(
    (toSyncReceipt(transfer.t, transfer.h) as any).contractAddress,
  ).toBeNull();
  if (creation) {
    const r = toSyncReceipt(creation.t, creation.h) as any;
    expect(r.contractAddress).toBe(
      (creation.t.contractAddress as string).toLowerCase(),
    );
  }
});

test('trace: delegatecall read from action.callType → DELEGATECALL', () => {
  const dc = allTrace.find((t) => t.action?.callType === 'delegatecall')!;
  const f = parityToCallFrame(dc, 0);
  expect(f.type).toBe('DELEGATECALL');
  expect(f.from).toBe(dc.action.from.toLowerCase());
  expect(f.to).toBe(dc.action.to.toLowerCase());
  expect(() => hexToBigInt(f.gas)).not.toThrow();
  expect(() => hexToBigInt(f.gasUsed)).not.toThrow();
});

test('trace: staticcall value:null → frame.value undefined (not 0x0)', () => {
  const sc = allTrace.find((t) => t.action?.callType === 'staticcall');
  if (!sc) return;
  expect(sc.action.value === null || sc.action.value === undefined).toBe(true);
  expect(parityToCallFrame(sc, 0).value).toBeUndefined();
});

test('trace: call/create mapping (CREATE uses init→input, code→output, result.address→to)', () => {
  const call = allTrace.find(
    (t) => t.type === 'call' && t.action?.callType === 'call',
  );
  if (call) expect(parityToCallFrame(call, 0).type).toBe('CALL');
  const create = allTrace.find((t) => t.type === 'create')!;
  const f = parityToCallFrame(create, 0);
  expect(f.type).toBe('CREATE');
  expect(f.to).toBe((create.result.address as string).toLowerCase());
  expect(f.input).toBe(create.action.init);
  expect(f.output).toBe(create.result.code);
});

test('trace: reverted call preserves error; reward dropped', () => {
  const rev = allTrace.find((t) => t.error);
  if (rev) expect(parityToCallFrame(rev, 0).error).toBe(rev.error);
  expect(parityToCallFrame({ type: 'reward', action: {} }, 0)).toBeUndefined();
});

test('trace: suicide → SELFDESTRUCT', () => {
  const sd = allTrace.find((t) => t.type === 'suicide');
  if (!sd) return;
  const f = parityToCallFrame(sd, 0);
  expect(f.type).toBe('SELFDESTRUCT');
  expect(f.from).toBe((sd.action.address as string).toLowerCase());
});

test('trace: traceAddress sorts in DFS pre-order', () => {
  expect([[1], [], [0, 0], [0]].sort(cmpTraceAddr)).toEqual([
    [],
    [0],
    [0, 0],
    [1],
  ]);
});

test("isFinalityGap: interval beyond Portal's finalized head triggers RPC fallback", () => {
  expect(isFinalityGap(1_000, 2_000)).toBe(false); // within Portal's head
  expect(isFinalityGap(2_001, 2_000)).toBe(true); // past it → fall back
  expect(isFinalityGap(2_000, 2_000)).toBe(false); // exactly at head is covered
  expect(isFinalityGap(2_001, undefined)).toBe(false); // head unknown → no gap yet
});

test('traceSafeChunkBlocks: caps only when traces needed and base exceeds cap', () => {
  expect(traceSafeChunkBlocks(500_000, false, 25_000)).toBe(500_000); // no traces → unchanged
  expect(traceSafeChunkBlocks(500_000, true, 25_000)).toBe(25_000); // traces → capped
  expect(traceSafeChunkBlocks(10_000, true, 25_000)).toBe(10_000); // already small → unchanged
});
