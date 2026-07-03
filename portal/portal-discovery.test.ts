import fc from "fast-check";
import { expect, test } from "vitest";
import type { Factory } from "@/internal/types.js";
import type { Address } from "viem";
import { createStats } from "./portal-metrics.js";
import { createDiscovery, planDiscovery, splitWindows } from "./portal-discovery.js";
import type { ChildAddresses } from "./portal-filters.js";
import type { PortalClient } from "./portal-client.js";

const FACTORY_ADDR = "0x29a56a1b8214d9cf7c5561811750d5cbdb45cc8e";
const PROXY_CREATED = "0x04e664079117e113faa9684bc14aecb41651cbf098b14eda271248c6d0cda57c";
const factory = (): Factory => ({ id: "f", type: "log", chainId: 1, sourceId: "EVault", address: FACTORY_ADDR, eventSelector: PROXY_CREATED as any, childAddressLocation: "topic1", fromBlock: undefined, toBlock: undefined } as Factory);

const topicAddr = (addr: string) => `0x${"0".repeat(24)}${addr.replace(/^0x/, "")}`;
// a factory child "created" at block `bn`
const proxy = (child: string, bn: number) => ({ header: { number: bn }, logs: [{ address: FACTORY_ADDR, topics: [PROXY_CREATED, topicAddr(child)], data: "0x" }] });

// a fake client whose discovery stream serves the given creation events within [lo,hi]; can be made to fail.
const fakeClient = (events: { child: string; bn: number }[], failWindows: (lo: number, hi: number) => boolean = () => false): PortalClient => ({
  finalizedHead: async () => undefined,
  async *stream(_q, from, to) {
    if (failWindows(from, to)) throw new Error("scan failed");
    const batch = events.filter((e) => e.bn >= from && e.bn <= to).map((e) => proxy(e.child, e.bn));
    if (batch.length) yield batch as any;
  },
});

// ── pure window math ────────────────────────────────────────────────────────────────────────────

test("splitWindows: disjoint, cover [from,to], ≤ discoveryWindows", () => {
  fc.assert(fc.property(fc.integer({ min: 0, max: 1000 }), fc.integer({ min: 0, max: 5000 }), fc.integer({ min: 1, max: 16 }), (from, span, P) => {
    const to = from + span;
    const ws = splitWindows(from, to, 500, P);
    expect(ws.length).toBeLessThanOrEqual(P);
    expect(ws[0]![0]).toBe(from);
    expect(ws[ws.length - 1]![1]).toBe(to);
    for (let i = 1; i < ws.length; i++) expect(ws[i]![0]).toBe(ws[i - 1]![1] + 1); // disjoint + contiguous
  }));
});

test("planDiscovery: null when no floor or already covered; reaches endHint", () => {
  expect(planDiscovery({ floor: -1, through: -1, status: "idle" }, 100, { chunkBlocks: 50, endHint: 100, discoveryWindows: 8 })).toBeNull();
  expect(planDiscovery({ floor: 0, through: 100, status: "idle" }, 100, { chunkBlocks: 50, endHint: 100, discoveryWindows: 8 })).toBeNull();
  const p = planDiscovery({ floor: 0, through: -1, status: "idle" }, 100, { chunkBlocks: 50, endHint: 500, discoveryWindows: 8 });
  expect(p!.from).toBe(0);
  expect(p!.to).toBe(500); // reaches the endHint in one pass
});

// ── INV-4: earliest-creation convergence under shuffled/overlapping windows ─────────────────────────

test("INV-4: shuffled/overlapping discovery windows converge to the same earliest-creation map", async () => {
  const events = [{ child: "0xaaa", bn: 5 }, { child: "0xbbb", bn: 250 }, { child: "0xaaa", bn: 400 }]; // 0xaaa first seen at 5
  const childAddresses: ChildAddresses = new Map([["f", new Map<Address, number>()]]);
  const d = createDiscovery({ client: fakeClient(events), childAddresses, factories: [factory()], discoveryWindows: 4, stats: createStats() });
  d.setFloor(0);
  await d.ensure(500, { chunkBlocks: 100, endHint: 500 });
  const rec = childAddresses.get("f")!;
  expect(rec.get("0xaaa" as Address)).toBe(5); // earliest, not 400
  expect(rec.get("0xbbb" as Address)).toBe(250);
});

// ── INV-3: discovery-before-data + failure/recovery (fixes G2) ──────────────────────────────────────

test("INV-3/G2: a failed scan rolls the watermark back; a later ensure recovers", async () => {
  const events = [{ child: "0xaaa", bn: 42 }];
  let failing = true;
  const childAddresses: ChildAddresses = new Map([["f", new Map<Address, number>()]]);
  const d = createDiscovery({ client: fakeClient(events, () => failing), childAddresses, factories: [factory()], discoveryWindows: 2, stats: createStats() });
  d.setFloor(0);

  await expect(d.ensure(500, { chunkBlocks: 100, endHint: 500 })).rejects.toThrow(/scan failed/);
  expect(d.through()).toBe(-1); // watermark rolled back to the last good value (never advanced on failure)

  failing = false; // recover
  await d.ensure(500, { chunkBlocks: 100, endHint: 500 });
  expect(d.through()).toBe(500);
  expect(childAddresses.get("f")!.get("0xaaa" as Address)).toBe(42); // discovered on retry
});

test("INV-3: dedup — a second ensure within the watermark returns without re-scanning", async () => {
  let scans = 0;
  const client: PortalClient = { finalizedHead: async () => undefined, async *stream(_q, _from, _to) { scans++; if (false) yield []; } };
  const d = createDiscovery({ client, childAddresses: new Map([["f", new Map()]]), factories: [factory()], discoveryWindows: 1, stats: createStats() });
  d.setFloor(0);
  await d.ensure(200, { chunkBlocks: 1000, endHint: 200 }); // one window
  const after = scans;
  await d.ensure(100, { chunkBlocks: 1000, endHint: 200 }); // covered → no new scan
  expect(scans).toBe(after);
});

test("no factories → ensure is a no-op", async () => {
  const d = createDiscovery({ client: fakeClient([]), childAddresses: new Map(), factories: [], discoveryWindows: 4, stats: createStats() });
  d.setFloor(0);
  await d.ensure(1000, { chunkBlocks: 100, endHint: 1000 });
  expect(d.through()).toBe(-1);
});
