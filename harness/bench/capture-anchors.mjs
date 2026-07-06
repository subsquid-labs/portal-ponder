#!/usr/bin/env node
// capture-anchors.mjs — ONE-TIME capture of the REAL chain headers the deterministic bench pins.
//
// For each of the 15 chains in harness/euler-multichain/chains.json it fetches four headers from the
// chain's own public RPC list (with fallback across the list) and records them into a committed
// anchors-<date>.json snapshot:
//   latest           — the current chain head at capture time (eth_getBlockByNumber "latest")
//   finalizedTarget  — latest.number − finalityBlockCount  (ponder's per-chain finality: 65 eth /
//                       200 polygon / 240 arbitrum / 30 default — getFinalityBlockCount, core 0.16.6)
//   deploy           — the Euler-factory deploy block (chains.json `deploy`)  = the backfill start
//   head             — the pinned head block           (chains.json `head`)   = the backfill end
// Where a second working RPC exists we RE-FETCH the header by hash from it and record whether the two
// sources AGREED on the hash (provenance/cross-check). Only the host of each source is recorded — never
// a key or a full URL with credentials.
//
// CRITICAL INVARIANT asserted per chain: finalizedTarget.number >= head  (i.e. snapshotted
//   latest − finalityBlockCount >= pinned head). This is what makes every chain present as END-CAPPED
// at startup, so a bench run stays a bounded [deploy, head] backfill and never enters realtime cutover.
// The chains advanced ~5 days past the July-1 pinned heads, so this must hold; a chain where it does
// NOT is reported loudly and (unless --allow-uncapped) makes the capture exit non-zero — we never
// silently ship a snapshot that would let a chain run unbounded.
//
//   node harness/bench/capture-anchors.mjs [--out harness/bench/anchors-YYYY-MM-DD.json]
//                                          [--only ethereum,polygon] [--allow-uncapped]
//
// This is a FREE, one-time snapshot on public RPCs — NOT the benchmark. If a chain's whole RPC list is
// dead it is reported and (unless --allow-uncapped) the run fails; we NEVER invent a header.

import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const DIR = path.dirname(fileURLToPath(import.meta.url));
const CHAINS_PATH = path.join(DIR, '..', 'euler-multichain', 'chains.json');

// Ponder per-chain finality (getFinalityBlockCount, @subsquid/ponder 0.16.6 utils/finality.ts).
// eth mainnet+testnets 65; polygon 200; arbitrum 240; everything else (OP-stack assumption) 30.
export function finalityBlockCount(chainId) {
  switch (chainId) {
    case 1:
    case 3:
    case 4:
    case 5:
    case 42:
    case 11155111:
      return 65;
    case 137:
    case 80001:
      return 200;
    case 42161:
    case 42170:
    case 421611:
    case 421613:
      return 240;
    default:
      return 30;
  }
}

// host-only provenance — never leak a full URL that could carry a key/query.
function hostOf(url) {
  try {
    return new URL(url).host;
  } catch {
    return 'invalid-url';
  }
}

const numberToHex = (n) => `0x${BigInt(n).toString(16)}`;

async function rpcCall(url, method, params, timeoutMs = 12_000) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }

  const j = await res.json();
  if (j.error) {
    throw new Error(
      `rpc ${j.error.code}: ${String(j.error.message).slice(0, 60)}`,
    );
  }

  return j.result;
}

// Keep only the header fields the shim serves (a LIGHT header, fullTransactions=false shape). We store a
// stable subset so the committed snapshot is small and deterministic and carries no per-node extras.
const HEADER_FIELDS = [
  'number',
  'hash',
  'parentHash',
  'timestamp',
  'stateRoot',
  'transactionsRoot',
  'receiptsRoot',
  'gasUsed',
  'gasLimit',
  'miner',
];
function lightHeader(block) {
  const out = {};
  for (const f of HEADER_FIELDS) {
    if (block[f] !== undefined) {
      out[f] = block[f];
    }
  }

  return out;
}

// Fetch a block header, trying each RPC in the list until one answers. Returns { header, source } where
// source is the HOST that answered (host only). Throws if the whole list is dead.
async function fetchHeader(rpcs, blockRef) {
  const errors = [];
  for (const url of rpcs) {
    try {
      const block = await rpcCall(url, 'eth_getBlockByNumber', [
        blockRef,
        false,
      ]);
      if (!block || typeof block.number !== 'string') {
        errors.push(`${hostOf(url)}: null block`);

        continue;
      }

      return { header: lightHeader(block), source: hostOf(url) };
    } catch (e) {
      errors.push(`${hostOf(url)}: ${e.message}`);
    }
  }

  throw new Error(
    `no RPC in the list served ${blockRef} — tried ${errors.length}: ${errors.join('; ')}`,
  );
}

// Cross-check a captured header's hash against a SECOND working source (a different host). Returns
// { agreed, source } or { agreed: null } if no distinct second source could answer. Uses
// eth_getBlockByNumber (by the same block number) so a lagging node that hasn't reached "latest" still
// answers for a specific historical number.
async function crossCheck(rpcs, header, primaryHost) {
  const number = header.number;
  for (const url of rpcs) {
    const host = hostOf(url);
    if (host === primaryHost) {
      continue;
    }

    try {
      const block = await rpcCall(url, 'eth_getBlockByNumber', [number, false]);
      if (!block || typeof block.hash !== 'string') {
        continue;
      }

      return { agreed: block.hash === header.hash, source: host };
    } catch {
      // try the next source
    }
  }

  return { agreed: null, source: null };
}

async function captureChain(chain, allowUncapped) {
  const finality = finalityBlockCount(chain.id);

  // latest first — the finalized target depends on it.
  const latest = await fetchHeader(chain.freeRpcs, 'latest');
  const latestNum = BigInt(latest.header.number);
  const finalizedTargetNum =
    latestNum - BigInt(finality) > 0n ? latestNum - BigInt(finality) : 0n;

  const finalizedTarget = await fetchHeader(
    chain.freeRpcs,
    numberToHex(finalizedTargetNum),
  );
  const deploy = await fetchHeader(chain.freeRpcs, numberToHex(chain.deploy));
  const head = await fetchHeader(chain.freeRpcs, numberToHex(chain.head));

  // cross-check each captured header's hash against a distinct second source.
  const checks = {};
  for (const [role, cap] of Object.entries({
    latest,
    finalizedTarget,
    deploy,
    head,
  })) {
    const cc = await crossCheck(chain.freeRpcs, cap.header, cap.source);
    checks[role] = {
      primarySource: cap.source,
      crossCheckSource: cc.source,
      hashAgreed: cc.agreed,
    };
  }

  // THE end-capped invariant: finalizedTarget >= head. Margin = finalizedTarget − head.
  const headNum = BigInt(head.header.number);
  const finalizedNum = BigInt(finalizedTarget.header.number);
  const margin = finalizedNum - headNum;
  const endCapped = margin >= 0n;

  if (!endCapped) {
    const msg =
      `✗ END-CAPPED INVARIANT FAILED for ${chain.name} (id ${chain.id}): ` +
      `finalizedTarget ${finalizedNum} < head ${headNum} (margin ${margin}). ` +
      'This chain would run UNBOUNDED at startup.';
    if (!allowUncapped) {
      throw new Error(msg);
    }

    console.error(`${msg} (continuing under --allow-uncapped)`);
  }

  return {
    entry: {
      id: chain.id,
      name: chain.name,
      finalityBlockCount: finality,
      deployBlock: chain.deploy,
      headBlock: chain.head,
      endCapped,
      endCapMargin: margin.toString(),
      headers: {
        latest: latest.header,
        finalizedTarget: finalizedTarget.header,
        deploy: deploy.header,
        head: head.header,
      },
      provenance: checks,
    },
    summary: {
      chain: chain.name,
      id: chain.id,
      latest: latestNum.toString(),
      finality,
      head: headNum.toString(),
      finalizedTarget: finalizedNum.toString(),
      margin: margin.toString(),
      endCapped,
    },
  };
}

// A stable-key JSON stringify so re-capturing the same headers yields a byte-identical manifest hash.
function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort();

    return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
  }

  return JSON.stringify(value);
}

function sha256Hex(text) {
  return createHash('sha256').update(text).digest('hex');
}

async function main() {
  const args = process.argv.slice(2);
  const outIdx = args.indexOf('--out');
  const onlyIdx = args.indexOf('--only');
  const allowUncapped = args.includes('--allow-uncapped');
  const date = new Date().toISOString().slice(0, 10);
  const outPath =
    outIdx >= 0 ? args[outIdx + 1] : path.join(DIR, `anchors-${date}.json`);
  const only =
    onlyIdx >= 0
      ? args[onlyIdx + 1]
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean)
      : null;

  const chains = JSON.parse(readFileSync(CHAINS_PATH, 'utf8'));
  const selected = only ? chains.filter((c) => only.includes(c.name)) : chains;
  if (selected.length === 0) {
    console.error('capture-anchors: no chains selected');
    process.exit(2);
  }

  const entries = [];
  const summaries = [];
  const failed = [];
  for (const chain of selected) {
    process.stderr.write(`capturing ${chain.name} (id ${chain.id})… `);
    try {
      const { entry, summary } = await captureChain(chain, allowUncapped);
      entries.push(entry);
      summaries.push(summary);
      process.stderr.write(
        `latest=${summary.latest} finalizedTarget=${summary.finalizedTarget} head=${summary.head} margin=${summary.margin} ${summary.endCapped ? 'CAPPED' : 'UNCAPPED'}\n`,
      );
    } catch (e) {
      failed.push({ chain: chain.name, id: chain.id, error: e.message });
      process.stderr.write(`FAILED: ${e.message}\n`);
      if (!allowUncapped) {
        console.error(
          `\n✗ capture aborted at ${chain.name}: ${e.message}\n` +
            '  (use --allow-uncapped to capture the rest and report failures; do NOT ship a partial snapshot as-is)',
        );
        process.exit(1);
      }
    }
  }

  // the committed snapshot: chains[] with headers+provenance+margin, plus a self-describing manifest
  // line = sha256 over the canonical (stable-key) JSON of chains[]. Recompute to verify integrity.
  const chainsCanonical = stableStringify(entries);
  const manifestSha = sha256Hex(chainsCanonical);
  const snapshot = {
    generatedAt: new Date().toISOString(),
    ponderFinalityModel: '@subsquid/ponder 0.16.6 getFinalityBlockCount',
    note:
      'Pinned real chain headers for the deterministic 15-chain flagship bench. Served by ' +
      'harness/bench/anchor-shim.mjs; the shim needs no external RPC at run time. Sources are hosts ' +
      'only (no keys). manifest.sha256 = sha256 over the stable-key JSON of chains[].',
    manifest: { sha256: manifestSha, chainCount: entries.length },
    chains: entries,
  };

  writeFileSync(outPath, `${JSON.stringify(snapshot, null, 2)}\n`);

  // human summary table on stderr (stdout stays clean for scripting).
  console.error(
    '\n── end-capped invariant (finalizedTarget − head = margin) ──',
  );
  console.error(
    'chain            id        latest  finality        head  finalizedTgt      margin  capped',
  );
  for (const s of summaries) {
    console.error(
      `${s.chain.padEnd(15)} ${String(s.id).padStart(8)} ${s.latest.padStart(12)} ${String(s.finality).padStart(8)} ${s.head.padStart(12)} ${s.finalizedTarget.padStart(12)} ${s.margin.padStart(12)}  ${s.endCapped ? 'yes' : 'NO'}`,
    );
  }
  console.error(
    `\nwrote ${outPath} (sha256 ${manifestSha}, ${entries.length} chains)`,
  );
  if (failed.length) {
    console.error(`\n✗ ${failed.length} chain(s) FAILED capture:`);
    for (const f of failed) {
      console.error(`  ${f.chain} (id ${f.id}): ${f.error}`);
    }
    process.exit(1);
  }

  const uncapped = summaries.filter((s) => !s.endCapped);
  if (uncapped.length) {
    console.error(
      `\n✗ ${uncapped.length} chain(s) are NOT end-capped: ${uncapped.map((s) => s.chain).join(', ')}`,
    );
    process.exit(1);
  }

  // stdout: the path, for scripting.
  process.stdout.write(`${outPath}\n`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error(`capture-anchors: ${e?.message ?? e}`);
    process.exit(1);
  });
}
