import fc from "fast-check";
import { afterEach, expect, test } from "vitest";
import { loadPortalConfig } from "./portal-config.js";
import { type GateEvent, type GateLimits, canAdmit, createGate, gateInit, gateReduce } from "./portal-gate.js";
import { setCheckMode } from "./portal-invariant.js";

setCheckMode("strict");
afterEach(() => setCheckMode("strict"));

const LIMITS: GateLimits = { min: 2, max: 8, maxRows: 1000 };

const arbEvent: fc.Arbitrary<GateEvent> = fc.oneof(
  fc.constant<GateEvent>({ type: "admit" }),
  fc.constant<GateEvent>({ type: "release" }),
  fc.constant<GateEvent>({ type: "ok" }),
  fc.constant<GateEvent>({ type: "throttle" }),
  fc.integer({ min: 0, max: 500 }).map<GateEvent>((n) => ({ type: "addRows", n })),
  fc.integer({ min: 0, max: 500 }).map<GateEvent>((n) => ({ type: "freeRows", n })),
);

test("INV-8/INV-7: bounds hold under arbitrary event sequences", () => {
  fc.assert(
    fc.property(fc.array(arbEvent, { maxLength: 200 }), (events) => {
      let s = gateInit(LIMITS, 4);
      for (const e of events) {
        // the shell only admits while capacity exists — model that precondition
        if (e.type === "admit" && !canAdmit(s)) continue;
        s = gateReduce(s, e);
        expect(s.limit).toBeGreaterThanOrEqual(LIMITS.min);
        expect(s.limit).toBeLessThanOrEqual(LIMITS.max);
        expect(s.active).toBeGreaterThanOrEqual(0);
        expect(s.rows).toBeGreaterThanOrEqual(0);
      }
    }),
  );
});

test("INV-8: AIMD ramps +2 per 8 clean, capped at MAX; throttle halves down to MIN", () => {
  let s = gateInit(LIMITS, 4); // limit 4
  for (let i = 0; i < 8; i++) s = gateReduce(s, { type: "ok" });
  expect(s.limit).toBe(6); // +2 after 8 clean
  for (let i = 0; i < 8; i++) s = gateReduce(s, { type: "ok" });
  expect(s.limit).toBe(8); // +2 → 8 (== MAX)
  for (let i = 0; i < 100; i++) s = gateReduce(s, { type: "ok" }); // stays capped
  expect(s.limit).toBe(8);
  s = gateReduce(s, { type: "throttle" });
  expect(s.limit).toBe(4); // halved
  s = gateReduce(s, { type: "throttle" });
  s = gateReduce(s, { type: "throttle" });
  expect(s.limit).toBe(2); // floored at MIN
});

test("gateInit clamps start into [min, max]", () => {
  expect(gateInit(LIMITS, 100).limit).toBe(8);
  expect(gateInit(LIMITS, 0).limit).toBe(2);
});

test("createGate: FIFO admission, no starvation while capacity exists", async () => {
  const cfg = loadPortalConfig({ PORTAL_START_CONCURRENCY: "2", PORTAL_MIN_CONCURRENCY: "1", PORTAL_MAX_CONCURRENCY: "4" });
  const gate = createGate(cfg);
  const order: number[] = [];
  const ps = Array.from({ length: 5 }, (_, i) => gate.acquire().then(() => order.push(i)));
  await Promise.resolve(); // flush microtasks: first 2 admitted (start=2)
  expect(gate.snapshot().active).toBe(2);
  gate.release(); gate.release(); gate.release(); // admit the remaining 3 in FIFO order
  await Promise.all(ps);
  expect(order).toEqual([0, 1, 2, 3, 4]); // strict FIFO — every waiter served
});

test("createGate: saturated() tracks the row budget; snapshot exposes {limit,active,rows}", () => {
  const cfg = loadPortalConfig({ PORTAL_MAX_ROWS_IN_MEM: "100" });
  const gate = createGate(cfg);
  expect(gate.saturated()).toBe(false);
  gate.addRows(150);
  expect(gate.saturated()).toBe(true);
  expect(gate.snapshot().rows).toBe(150);
  gate.freeRows(150);
  expect(gate.saturated()).toBe(false);
  gate.freeRows(999); // never negative
  expect(gate.snapshot().rows).toBe(0);
});
