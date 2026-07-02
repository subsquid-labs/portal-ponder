#!/usr/bin/env node
// Endpoint health probe for the realtime soak — evaluates each endpoint's latency / tip-freshness /
// responsiveness. Standalone (no imports beyond node) so it survives an overnight run alongside ponder.
// Mirrors portal/realtime.ts (unit-tested). Writes PROBE_METRICS_FILE (rolling JSON) + a .log trail.
//
//   PORTAL_RPC_KEY=... [SQD_RPC_KEY=...] [PROBE_INTERVAL_MS=15000] node probe.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const dir = path.dirname(fileURLToPath(import.meta.url));
const chains = JSON.parse(fs.readFileSync(path.join(dir, "chains.json"), "utf8"));
const KEY = process.env.PORTAL_RPC_KEY;
const RPC_URL = process.env.PORTAL_RPC_URL; // client-specific base, e.g. https://euler.portal.sqd.dev/rpc/v1/evm
const PORTAL_RPC_CHAINS = new Set([1, 42161, 8453, 43114, 137, 56, 9745, 143]);
const METRICS = process.env.PROBE_METRICS_FILE || path.join(dir, "probe-metrics.json");
const INTERVAL = Number(process.env.PROBE_INTERVAL_MS || 15000);
const WINDOW = Number(process.env.PROBE_WINDOW || 40); // rolling ticks

if (!KEY || !RPC_URL) { console.error("PORTAL_RPC_KEY + PORTAL_RPC_URL required (the Portal-backed RPC base + x-api-key)"); process.exit(1); }

// per chain, probe the Portal-backed RPC (the product) alongside a keyless public RPC (freshness baseline).
// rpc.subsquid.io is a generic proxy, not the Portal-backed product — deliberately excluded.
const targets = chains.filter((c) => PORTAL_RPC_CHAINS.has(c.id)).map((c) => ({
  chainId: c.id, chain: c.name,
  endpoints: [
    { name: "portal-rpc", url: `${RPC_URL}/${c.id}`, headers: { "x-api-key": KEY } },
    ...(c.freeRpcs || []).slice(0, 1).map((u, i) => ({ name: `public${i}`, url: u })),
  ],
}));

const pct = (a, p) => { if (!a.length) return 0; const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] ?? 0; };

async function probeOne(url, headers, timeoutMs = 8000) {
  const t0 = Date.now();
  try {
    const res = await fetch(url, { method: "POST", headers: { "content-type": "application/json", ...(headers || {}) }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_blockNumber", params: [] }), signal: AbortSignal.timeout(timeoutMs) });
    const latencyMs = Date.now() - t0;
    if (!res.ok) return { ok: false, latencyMs, tip: null, error: `HTTP ${res.status}` };
    const j = await res.json();
    const tip = typeof j?.result === "string" ? Number(j.result) : null;
    return { ok: tip !== null && Number.isFinite(tip), latencyMs, tip, error: j?.error ? JSON.stringify(j.error).slice(0, 80) : undefined };
  } catch (e) { return { ok: false, latencyMs: Date.now() - t0, tip: null, error: String(e?.message ?? e).slice(0, 80) }; }
}

const ring = [];
async function tick() {
  const samples = [];
  await Promise.all(targets.flatMap((c) => c.endpoints.map(async (e) => {
    const r = await probeOne(e.url, e.headers);
    samples.push({ chainId: c.chainId, chain: c.chain, name: e.name, ...r });
  })));
  // tip-freshness: blocks behind the freshest endpoint for each chain
  const maxTip = {};
  for (const s of samples) if (s.ok && s.tip != null) maxTip[s.chainId] = Math.max(maxTip[s.chainId] ?? 0, s.tip);
  for (const s of samples) s.lag = (s.ok && s.tip != null && maxTip[s.chainId] != null) ? maxTip[s.chainId] - s.tip : null;
  ring.push(...samples);
  while (ring.length > WINDOW * samples.length) ring.shift();
  // roll into per-endpoint stats
  const byKey = {};
  for (const s of ring) (byKey[`${s.chainId}:${s.name}`] ??= []).push(s);
  const summary = {};
  for (const [k, ss] of Object.entries(byKey)) {
    const ok = ss.filter((s) => s.ok); const lats = ok.map((s) => s.latencyMs); const lags = ok.map((s) => s.lag).filter((x) => typeof x === "number");
    summary[k] = { chain: ss[0].chain, endpoint: ss[0].name, n: ss.length, okRate: +(ok.length / ss.length).toFixed(3),
      latP50: pct(lats, 50), latP95: pct(lats, 95), latMean: lats.length ? Math.round(lats.reduce((a, b) => a + b, 0) / lats.length) : 0,
      avgLagBlk: lags.length ? Math.round(lags.reduce((a, b) => a + b, 0) / lags.length) : null, lastError: [...ss].reverse().find((s) => s.error)?.error };
  }
  try { fs.writeFileSync(METRICS, JSON.stringify({ ts: new Date().toISOString(), intervalMs: INTERVAL, endpoints: summary }, null, 2)); } catch { /* best-effort */ }
  const line = Object.values(summary).filter((v) => v.endpoint === "portal-rpc").map((v) => `${v.chain} p50=${v.latP50} p95=${v.latP95} ok=${(v.okRate * 100).toFixed(0)}% lag=${v.avgLagBlk ?? "?"}`).join(" | ");
  try { fs.appendFileSync(METRICS + ".log", `${new Date().toISOString()} [portal-rpc] ${line}\n`); } catch { /* best-effort */ }
}

console.log(`probe: ${targets.length} chains × endpoints, interval ${INTERVAL}ms -> ${METRICS}`);
tick(); setInterval(tick, INTERVAL);
