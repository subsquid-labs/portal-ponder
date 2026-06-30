import { readFileSync } from "node:fs";
import { join } from "node:path";
import { hexToBigInt } from "viem";
import { expect, test } from "vitest";
import { cmpTraceAddr, parityToCallFrame, toStateDiff, toSyncReceipt, traceSafeChunkBlocks } from "./portal-transform.js";

/**
 * Unit tests over REAL Portal NDJSON captured at eth block 21,000,000 (+ base).
 * They pin the type mismatches the schema audit flagged — Portal's split encoding
 * (status/type decimal, gas/value hex), trace callType at action.callType,
 * staticcall value:null, CREATE init/code, stateDiff prev:null⟺"+".
 */
const FIX = join(__dirname, "__fixtures__");
const load = (f: string): any[] => readFileSync(join(FIX, f), "utf8").trim().split("\n").map((l) => JSON.parse(l));
const allTx = load("receipts.json").flatMap((b) => (b.transactions ?? []).map((t: any) => ({ t, h: b.header })));
const allTrace = load("traces.json").flatMap((b) => b.traces ?? []);
const allDiff = load("statediffs.json").flatMap((b) => b.stateDiffs ?? []);

test("receipt: decimal status/type → hex; gas fields stay hex (BigInt-able)", () => {
  const ok = allTx.find(({ t }) => t.status === 1)!;
  const r = toSyncReceipt(ok.t, ok.h) as any;
  expect(r.status).toBe("0x1"); // input was the number 1, NOT "0x1"
  expect(r.type).toMatch(/^0x[0-9a-f]+$/); // input was a number (e.g. 2)
  expect(typeof ok.t.status).toBe("number"); // sanity: confirm the fixture really is decimal
  // gas fields arrive hex; must remain hex strings that hexToBigInt accepts
  expect(() => hexToBigInt(r.gasUsed)).not.toThrow();
  expect(() => hexToBigInt(r.cumulativeGasUsed)).not.toThrow();
  expect(() => hexToBigInt(r.effectiveGasPrice)).not.toThrow();
});

test("receipt: failed tx (status 0) → 0x0", () => {
  const failed = allTx.find(({ t }) => t.status === 0);
  if (!failed) return; // fixture may not include one in this slice
  expect((toSyncReceipt(failed.t, failed.h) as any).status).toBe("0x0");
});

test("receipt: contract-creation contractAddress lowercased; non-creation null", () => {
  const creation = allTx.find(({ t }) => t.contractAddress);
  const transfer = allTx.find(({ t }) => t.contractAddress === null)!;
  expect((toSyncReceipt(transfer.t, transfer.h) as any).contractAddress).toBeNull();
  if (creation) {
    const r = toSyncReceipt(creation.t, creation.h) as any;
    expect(r.contractAddress).toBe((creation.t.contractAddress as string).toLowerCase());
  }
});

test("trace: delegatecall read from action.callType → DELEGATECALL", () => {
  const dc = allTrace.find((t) => t.action?.callType === "delegatecall")!;
  const f = parityToCallFrame(dc, 0);
  expect(f.type).toBe("DELEGATECALL");
  expect(f.from).toBe(dc.action.from.toLowerCase());
  expect(f.to).toBe(dc.action.to.toLowerCase());
  expect(() => hexToBigInt(f.gas)).not.toThrow();
  expect(() => hexToBigInt(f.gasUsed)).not.toThrow();
});

test("trace: staticcall value:null → frame.value undefined (not 0x0)", () => {
  const sc = allTrace.find((t) => t.action?.callType === "staticcall");
  if (!sc) return;
  expect(sc.action.value === null || sc.action.value === undefined).toBe(true);
  expect(parityToCallFrame(sc, 0).value).toBeUndefined();
});

test("trace: call/create mapping (CREATE uses init→input, code→output, result.address→to)", () => {
  const call = allTrace.find((t) => t.type === "call" && t.action?.callType === "call");
  if (call) expect(parityToCallFrame(call, 0).type).toBe("CALL");
  const create = allTrace.find((t) => t.type === "create")!;
  const f = parityToCallFrame(create, 0);
  expect(f.type).toBe("CREATE");
  expect(f.to).toBe((create.result.address as string).toLowerCase());
  expect(f.input).toBe(create.action.init);
  expect(f.output).toBe(create.result.code);
});

test("trace: reverted call preserves error; reward dropped", () => {
  const rev = allTrace.find((t) => t.error);
  if (rev) expect(parityToCallFrame(rev, 0).error).toBe(rev.error);
  expect(parityToCallFrame({ type: "reward", action: {} }, 0)).toBeUndefined();
});

test("trace: suicide → SELFDESTRUCT", () => {
  const sd = allTrace.find((t) => t.type === "suicide");
  if (!sd) return;
  const f = parityToCallFrame(sd, 0);
  expect(f.type).toBe("SELFDESTRUCT");
  expect(f.from).toBe((sd.action.address as string).toLowerCase());
});

test("trace: traceAddress sorts in DFS pre-order", () => {
  expect([[1], [], [0, 0], [0]].sort(cmpTraceAddr)).toEqual([[], [0], [0, 0], [1]]);
});

test("traceSafeChunkBlocks: caps only when traces needed and base exceeds cap", () => {
  expect(traceSafeChunkBlocks(500_000, false, 25_000)).toBe(500_000); // no traces → unchanged
  expect(traceSafeChunkBlocks(500_000, true, 25_000)).toBe(25_000); // traces → capped
  expect(traceSafeChunkBlocks(10_000, true, 25_000)).toBe(10_000); // already small → unchanged
});

test("stateDiff: kind/key preserved; prev:null ⟺ '+'; hex prev/next", () => {
  for (const d of allDiff) {
    const sd = toStateDiff(d);
    expect(["=", "+", "*", "-"]).toContain(sd.kind);
    expect(sd.address).toBe((d.address as string).toLowerCase());
    if (sd.kind === "+") expect(sd.prev).toBeNull();
    if (sd.prev !== null) expect(String(sd.prev)).toMatch(/^0x/);
  }
  const add = allDiff.find((d) => d.kind === "+");
  if (add) expect(toStateDiff(add).prev).toBeNull();
});
