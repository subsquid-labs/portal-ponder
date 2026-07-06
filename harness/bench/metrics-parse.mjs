// metrics-parse.mjs — PURE parsing of a ponder /metrics Prometheus text exposition into the
// machine-readable bench result. Kept separate from the run driver (run-flagship.sh) so the parsing —
// the part with real logic (label parsing, per-chain aggregation, completion detection, wall-time
// derivation) — is unit-tested, while the driver is just orchestration (start shim, launch app, poll,
// scrape, write JSON).
//
// The metrics the driver cares about, all emitted by @subsquid/ponder 0.16.6 (internal/metrics.ts):
//   ponder_sync_is_complete{chain}                     1 when a chain has finished its bounded backfill
//   ponder_historical_start_timestamp_seconds{chain}   unix seconds the historical backfill began
//   ponder_historical_end_timestamp_seconds{chain}     unix seconds it finished (0/absent until done)
//   ponder_historical_completed_blocks{chain}          blocks indexed so far
//   ponder_historical_total_blocks{chain}              blocks the backfill must index
//   ponder_rpc_request_duration_count{chain,method}    RPC request count (histogram _count series)
//   ponder_rpc_request_error_total{chain,method}       RPC request errors
//
// A successful end-capped bench run: every chain complete, and rpc error total 0 (the shim serves every
// anchor; a non-zero error means an anchor was missing or an unexpected call was made — a red flag).

// Parse a Prometheus text body into an array of { name, labels, value } samples. Ignores # HELP/# TYPE
// comment lines and blank lines. Robust to labelless series and to label values containing commas by
// scanning the {...} block character-by-character (values are quoted).
export function parsePrometheus(text) {
  const samples = [];
  for (const rawLine of text.split('\n')) {
    const line = rawLine.trim();
    if (line === '' || line.startsWith('#')) {
      continue;
    }

    const parsed = parseSampleLine(line);
    if (parsed) {
      samples.push(parsed);
    }
  }

  return samples;
}

function parseSampleLine(line) {
  const braceStart = line.indexOf('{');
  let name;
  let labels = {};
  let rest;

  if (braceStart === -1) {
    // name value   (no labels)
    const sp = line.indexOf(' ');
    if (sp === -1) {
      return null;
    }

    name = line.slice(0, sp);
    rest = line.slice(sp + 1).trim();
  } else {
    name = line.slice(0, braceStart);
    const braceEnd = line.indexOf('}', braceStart);
    if (braceEnd === -1) {
      return null;
    }

    labels = parseLabels(line.slice(braceStart + 1, braceEnd));
    rest = line.slice(braceEnd + 1).trim();
  }

  const value = Number(rest.split(/\s+/)[0]);
  if (!Number.isFinite(value)) {
    return null;
  }

  return { name, labels, value };
}

// Parse `k1="v1",k2="v2"` into an object. Values are double-quoted; a value may contain an escaped
// quote or a comma, so we walk the string tracking whether we are inside quotes.
function parseLabels(body) {
  const labels = {};
  let i = 0;
  while (i < body.length) {
    const eq = body.indexOf('=', i);
    if (eq === -1) {
      break;
    }

    const key = body.slice(i, eq).trim();
    // value starts at the opening quote after '='
    const q1 = body.indexOf('"', eq);
    if (q1 === -1) {
      break;
    }

    let j = q1 + 1;
    let value = '';
    while (j < body.length) {
      const ch = body[j];
      if (ch === '\\' && j + 1 < body.length) {
        value += body[j + 1];
        j += 2;

        continue;
      }
      if (ch === '"') {
        break;
      }

      value += ch;
      j++;
    }

    labels[key] = value;
    // advance past the closing quote and an optional comma
    i = j + 1;
    if (body[i] === ',') {
      i++;
    }
  }

  return labels;
}

// Sum a gauge/counter's values grouped by the `chain` label. Returns Map<chain, number>.
function byChain(samples, name) {
  const out = new Map();
  for (const s of samples) {
    if (s.name !== name) {
      continue;
    }

    const chain = s.labels.chain ?? '<none>';
    out.set(chain, (out.get(chain) ?? 0) + s.value);
  }

  return out;
}

// The last value of a gauge per chain (start/end timestamps are set-once gauges, not summed).
function lastByChain(samples, name) {
  const out = new Map();
  for (const s of samples) {
    if (s.name !== name) {
      continue;
    }

    const chain = s.labels.chain ?? '<none>';
    out.set(chain, s.value);
  }

  return out;
}

// Build the bench result from parsed samples + the list of chain names we expect to complete.
//   allComplete       — every expected chain has ponder_sync_is_complete == 1
//   historicalStart/End — min start, max end across chains (unix seconds); wallSeconds = end − start
//   perChain          — [{ chain, complete, completedBlocks, totalBlocks, startTs, endTs }]
//   rpc               — { requests, errors } totalled across chains (errors MUST be 0 for a clean run)
export function summarizeMetrics(samples, expectedChains) {
  const isComplete = lastByChain(samples, 'ponder_sync_is_complete');
  const startTs = lastByChain(
    samples,
    'ponder_historical_start_timestamp_seconds',
  );
  const endTs = lastByChain(samples, 'ponder_historical_end_timestamp_seconds');
  const completed = byChain(samples, 'ponder_historical_completed_blocks');
  const total = byChain(samples, 'ponder_historical_total_blocks');
  const rpcRequests = byChain(samples, 'ponder_rpc_request_duration_count');
  const rpcErrors = byChain(samples, 'ponder_rpc_request_error_total');

  const chains = expectedChains ?? [...isComplete.keys()];
  const perChain = [];
  let allComplete = chains.length > 0;
  for (const chain of chains) {
    const complete = (isComplete.get(chain) ?? 0) === 1;
    if (!complete) {
      allComplete = false;
    }

    perChain.push({
      chain,
      complete,
      completedBlocks: completed.get(chain) ?? 0,
      totalBlocks: total.get(chain) ?? 0,
      startTs: startTs.get(chain) ?? null,
      endTs: endTs.get(chain) ?? null,
    });
  }

  const starts = [...startTs.values()].filter((v) => v > 0);
  const ends = [...endTs.values()].filter((v) => v > 0);
  const historicalStart = starts.length ? Math.min(...starts) : null;
  const historicalEnd = ends.length ? Math.max(...ends) : null;
  const wallSeconds =
    historicalStart !== null && historicalEnd !== null
      ? historicalEnd - historicalStart
      : null;

  const requests = [...rpcRequests.values()].reduce((a, b) => a + b, 0);
  const errors = [...rpcErrors.values()].reduce((a, b) => a + b, 0);

  return {
    allComplete,
    historicalStart,
    historicalEnd,
    wallSeconds,
    perChain,
    rpc: { requests, errors },
  };
}
