import { http } from "viem";
import { expect, test } from "vitest";
import {
  freshnessLag,
  type ProbeSample,
  probeOnce,
  rpcRealtime,
  summarize,
} from "./realtime.js";

test("rpcRealtime: composes URL strings and authed transports into a latency-ranked fallback", () => {
  // an authenticated RPC is just a viem transport — the lib is agnostic to what backs it
  const authed = http("http://b", {
    fetchOptions: { headers: { "x-api-key": "k" } },
  });
  expect(typeof rpcRealtime(["http://a", authed])).toBe("function");
  expect(typeof rpcRealtime(["http://a", "http://b"], { rank: false })).toBe(
    "function",
  );
});

test("freshnessLag: blocks behind the freshest endpoint, per chain", () => {
  const samples: ProbeSample[] = [
    { chainId: 1, name: "sqd", ok: true, latencyMs: 100, tip: 1000 },
    { chainId: 1, name: "pub", ok: true, latencyMs: 200, tip: 998 },
    { chainId: 1, name: "down", ok: false, latencyMs: 0, tip: null },
    { chainId: 42161, name: "sqd", ok: true, latencyMs: 150, tip: 5000 },
  ];
  const lag = freshnessLag(samples);
  expect(lag.get("1:sqd")).toBe(0); // freshest for chain 1
  expect(lag.get("1:pub")).toBe(2); // 2 blocks behind
  expect(lag.get("1:down")).toBe(null); // errored → no lag
  expect(lag.get("42161:sqd")).toBe(0); // freshest for chain 42161
});

test("summarize: latency percentiles, ok-rate, avg lag, last error", () => {
  const w: ProbeSample[] = [];
  for (let i = 0; i < 10; i++)
    w.push({
      chainId: 1,
      name: "sqd",
      ok: true,
      latencyMs: 100 + i * 10,
      tip: 1000 + i,
      lag: 0,
    });
  w.push({
    chainId: 1,
    name: "sqd",
    ok: false,
    latencyMs: 0,
    tip: null,
    error: "timeout",
    lag: null,
  });
  const s = summarize(w);
  const e = s["1:sqd"];
  expect(e.n).toBe(11);
  expect(e.okRate).toBeCloseTo(10 / 11, 2);
  expect(e.latMean).toBeGreaterThan(100);
  expect(e.latP95).toBeGreaterThanOrEqual(e.latP50);
  expect(e.avgLag).toBe(0);
  expect(e.lastError).toBe("timeout");
});

test("probeOnce: measures latency + tip via mock fetch, forwards headers, records errors", async () => {
  const chains = [
    {
      chainId: 1,
      endpoints: [
        { name: "sqd", url: "http://sqd", headers: { "x-api-key": "k" } },
        { name: "bad", url: "http://bad" },
      ],
    },
  ];
  const mockFetch = (async (url: string, init: any) => {
    if (url === "http://bad") throw new Error("ECONNREFUSED");
    expect(init.headers["x-api-key"]).toBe("k"); // header forwarded
    expect(JSON.parse(init.body).method).toBe("eth_blockNumber");
    return {
      ok: true,
      json: async () => ({ jsonrpc: "2.0", id: 1, result: "0x64" }),
    };
  }) as any;
  const samples = await probeOnce(chains as any, mockFetch, 5000);
  const sqd = samples.find((s) => s.name === "sqd")!;
  const bad = samples.find((s) => s.name === "bad")!;
  expect(sqd.ok).toBe(true);
  expect(sqd.tip).toBe(100); // 0x64
  expect(sqd.latencyMs).toBeGreaterThanOrEqual(0);
  expect(bad.ok).toBe(false);
  expect(bad.error).toMatch(/ECONNREFUSED/);
});

test("probeOnce: a non-200 HTTP response is unhealthy with the status", async () => {
  const chains = [{ chainId: 1, endpoints: [{ name: "e", url: "http://e" }] }];
  const mockFetch = (async () => ({
    ok: false,
    status: 529,
    json: async () => ({}),
  })) as any;
  const [s] = await probeOnce(chains as any, mockFetch);
  expect(s.ok).toBe(false);
  expect(s.error).toBe("HTTP 529");
});
