// deploy-helpers.mjs — pure, unit-tested cores for deploy-soak-b.sh.
//
// deploy-soak-b.sh renders a systemd unit + a chmod-600 env file for the Soak B instance. Two pieces
// of its logic are correctness-sensitive when REDEPLOYING over an EXISTING deployment (regenerating
// the env file / reconciling the indexed-chains knob), so they live here as pure functions with
// node --test coverage rather than being inlined in the shell where they cannot be asserted.
//
// The shell invokes these via `node -e`/`node <script>` and consumes stdout; see deploy-soak-b.sh.

import { readFileSync } from 'node:fs';

// ── env-carry (defect: redeploy silently dropped operative env vars) ─────────────────────────────
//
// When regenerating the env file for an EXISTING deployment we must preserve the operative variables
// the running app already relies on (Portal/RPC keys AND base URLs AND the chain list, DB schema,
// node flags, Portal tunables), not just a narrow secrets pattern. The clean mechanism is
// PRESERVE-ALL-THEN-OVERRIDE: carry every well-formed `KEY=value` assignment from the source env,
// EXCEPT the small set the deploy script authoritatively re-derives itself (so a stale carried value
// can never shadow the freshly-rendered one). This generalizes — a new tunable added to a real env
// file is carried automatically without editing this list.
//
// `OVERRIDDEN_KEYS` are the vars deploy-soak-b.sh writes itself after the carry block; carrying them
// would be dead (immediately overwritten) or actively wrong (a stale value ordered after the fresh
// one). Keep this in sync with the trailing `echo KEY=...` lines in deploy-soak-b.sh.
export const OVERRIDDEN_KEYS = new Set([
  'DATABASE_URL', // re-derived from the guarded DB name
  'DATABASE_SCHEMA', // re-derived from the single SOAK_B_SCHEMA knob (kept consistent with --schema)
  'PORTAL_REALTIME', // the soak's mode is fixed to `stream`
  'PORTAL_CHECKS', // on by design for the soak
  'EULER_CHAINS', // re-derived from the validated SOAK_CHAINS/EULER_CHAINS knob
]);

// Parse one env-file line into { key, value } or null (comment/blank/malformed). Accepts optional
// `export ` prefix and surrounding whitespace; the key must be a POSIX-shell env identifier.
export function parseEnvLine(line) {
  const raw = String(line);
  const stripped = raw.replace(/^\s*export\s+/, '').trim();
  if (stripped === '' || stripped.startsWith('#')) {
    return null;
  }
  const eq = stripped.indexOf('=');
  if (eq <= 0) {
    return null;
  }
  const key = stripped.slice(0, eq);
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
    return null;
  }

  return { key, value: stripped.slice(eq + 1), line: stripped };
}

// Filter the lines of an existing env file down to the ones a redeploy should carry over verbatim.
// Preserve-all-then-override: keep every parseable assignment whose key is NOT in `overridden`
// (default OVERRIDDEN_KEYS). De-duplicates on key (last assignment wins, mirroring shell env
// semantics) while preserving first-seen order. Returns the exact `KEY=value` strings to emit.
export function filterCarriedEnv(lines, overridden = OVERRIDDEN_KEYS) {
  const over = overridden instanceof Set ? overridden : new Set(overridden);
  const order = [];
  const byKey = new Map();
  for (const line of lines) {
    const parsed = parseEnvLine(line);
    if (!parsed) {
      continue;
    }

    if (over.has(parsed.key)) {
      continue;
    }

    if (!byKey.has(parsed.key)) {
      order.push(parsed.key);
    }
    byKey.set(parsed.key, parsed.line);
  }

  return order.map((key) => byKey.get(key));
}

// ── chain-name reconciliation (defect: SOAK_CHAINS short names never matched the app) ────────────
//
// The app config (harness/euler-multichain/ponder.config.ts) filters chains by the `name` field of
// chains.json via `EULER_CHAINS=name,name,…`. Short aliases like "eth"/"arb" would silently match
// NOTHING (the filter would produce an empty chain set). Reconcile the knob to write EULER_CHAINS
// with FULL names, mapping a set of well-known aliases for operator convenience, and FAIL LOUD on any
// value that is neither a known full name nor a known alias.
//
// Aliases are a convenience only — the canonical value written is always the chains.json `name`, and
// every alias target below IS a known chains.json name (an alias to a non-existent chain would be a
// footgun: it would pass the alias map but then fail validation). Keep them in that invariant.
export const CHAIN_ALIASES = {
  eth: 'ethereum',
  mainnet: 'ethereum',
  arb: 'arbitrum',
  bsc: 'binance',
  bnb: 'binance',
  avax: 'avalanche',
  matic: 'polygon',
};

// Resolve a comma-separated chains knob (SOAK_CHAINS / EULER_CHAINS) into the canonical
// EULER_CHAINS value the app understands. `known` is the set/list of valid chains.json names.
// Throws a loud, listing error on any unknown name so a typo fails the deploy instead of silently
// indexing zero chains. Returns { chains: string[], value: string } (value = comma-joined, ready to
// write as `EULER_CHAINS=<value>`).
export function resolveEulerChains(input, known) {
  const knownSet = known instanceof Set ? known : new Set(known);
  const requested = String(input ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  if (requested.length === 0) {
    throw new Error(
      'SOAK_CHAINS/EULER_CHAINS is empty — set it to a comma-separated list of chain names',
    );
  }

  const resolved = [];
  const seen = new Set();
  const unknown = [];
  for (const token of requested) {
    const canonical = Object.hasOwn(CHAIN_ALIASES, token)
      ? CHAIN_ALIASES[token]
      : token;
    if (!knownSet.has(canonical)) {
      unknown.push(token);
      continue;
    }

    if (!seen.has(canonical)) {
      seen.add(canonical);
      resolved.push(canonical);
    }
  }
  if (unknown.length > 0) {
    throw new Error(
      `unknown chain name(s): ${unknown.join(', ')} — valid names are: ${[...knownSet].sort().join(', ')}`,
    );
  }

  return { chains: resolved, value: resolved.join(',') };
}

// Load the canonical chain-name set from chains.json (the app's single source of truth). Kept out of
// the pure functions above so they stay filesystem-free and directly assertable; the CLI wrapper
// below wires it in.
export function loadKnownChains(chainsJsonPath) {
  const rows = JSON.parse(readFileSync(chainsJsonPath, 'utf8'));

  return new Set(rows.map((r) => r.name));
}

// ── CLI wrapper ──────────────────────────────────────────────────────────────────────────────────
//
// Invoked by deploy-soak-b.sh. Subcommands:
//   carry-env <envfile>              → prints the KEY=value lines to carry (one per line) to stdout.
//   resolve-chains <chains> <json>   → prints the canonical EULER_CHAINS value to stdout; exits
//                                      non-zero with a loud message on stderr for unknown names.
// All errors go to stderr and exit 1 so the shell's `$(…)` + `set -e` propagate the failure.
export function runCli(argv) {
  const [cmd, ...rest] = argv;
  if (cmd === 'carry-env') {
    const file = rest[0];
    if (!file) {
      throw new Error('usage: deploy-helpers.mjs carry-env <envfile>');
    }
    const lines = readFileSync(file, 'utf8').split('\n');

    return filterCarriedEnv(lines).join('\n');
  }
  if (cmd === 'resolve-chains') {
    const [chains, jsonPath] = rest;
    if (!chains || !jsonPath) {
      throw new Error(
        'usage: deploy-helpers.mjs resolve-chains <chains> <chains.json>',
      );
    }
    const known = loadKnownChains(jsonPath);

    return resolveEulerChains(chains, known).value;
  }

  throw new Error(`unknown subcommand: ${JSON.stringify(cmd)}`);
}

// Run only when executed directly (not when imported by the test suite).
if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  try {
    const out = runCli(process.argv.slice(2));
    if (out) {
      process.stdout.write(`${out}\n`);
    }
  } catch (err) {
    process.stderr.write(`${err.message}\n`);
    process.exit(1);
  }
}
