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

// The unit template renders several fixed knobs via `Environment=KEY=value` lines (PONDER_LOG_LEVEL,
// CI, SOAK_B_RESTART_LOG, …). systemd precedence is a footgun here: an `EnvironmentFile=` value
// OVERRIDES an `Environment=` value, so a STALE carried copy of any Environment=-rendered key would
// silently shadow the unit's freshly-rendered one (e.g. a stale SOAK_B_RESTART_LOG could point the
// restart-log write outside ReadWritePaths). Rather than duplicate that list by hand (which drifts
// the moment someone adds an Environment= line), DERIVE the excluded keys from the unit template
// itself and union them with OVERRIDDEN_KEYS. `parseUnitEnvironmentKeys` extracts them; the deploy
// script passes the rendered/template path to the carry-env CLI so the two can never diverge.
// Tokenize the RHS of an `Environment=` line into individual assignments, honoring systemd's
// double/single-quote wrapping (a quoted region may contain the separating whitespace) and stripping
// one surrounding quote pair from each yielded token. Not a full shell parser — just enough that a
// quoted `KEY=value with spaces` yields the whole `KEY=value with spaces` (so the KEY is recoverable)
// instead of splintering on the inner space. Unquoted runs split on whitespace as before.
function splitUnitAssignments(rhs) {
  const tokens = [];
  let current = '';
  let quote = null;
  let inToken = false;
  let escaped = false;
  for (const ch of rhs) {
    if (quote) {
      // Inside a "…" item systemd honors backslash escapes (\" does NOT close the quote); inside a
      // '…' item the backslash is literal (matches hasUnescapedClosingQuote / the systemd manual).
      if (escaped) {
        current += ch;
        escaped = false;

        continue;
      }

      if (quote === '"' && ch === '\\') {
        current += ch;
        escaped = true;

        continue;
      }

      if (ch === quote) {
        quote = null;
      } else {
        current += ch;
      }

      continue;
    }

    if (ch === '"' || ch === "'") {
      quote = ch;
      inToken = true;

      continue;
    }

    if (/\s/.test(ch)) {
      if (inToken) {
        tokens.push(current);
        current = '';
        inToken = false;
      }

      continue;
    }

    current += ch;
    inToken = true;
  }

  if (inToken) {
    tokens.push(current);
  }

  return tokens;
}

export function parseUnitEnvironmentKeys(unitText) {
  const keys = new Set();
  for (const rawLine of String(unitText).split('\n')) {
    const line = rawLine.trim();
    // Match `Environment=KEY=value` (systemd allows multiple space-separated assignments per line).
    const m = line.match(/^Environment=(.+)$/);
    if (!m) {
      continue;
    }

    // A single Environment= line can carry several assignments. systemd permits an assignment (or its
    // value) to be wrapped in "…" or '…' so it can contain spaces — tokenize RESPECTING those quotes
    // rather than a naive whitespace split, else `Environment="FOO=a b" BAR=c` would parse `"FOO` (an
    // invalid identifier, silently dropped) and the tripwire would miss FOO. Each yielded token then
    // has any surrounding quote pair stripped before the KEY is extracted.
    for (const assignment of splitUnitAssignments(m[1])) {
      const eq = assignment.indexOf('=');
      if (eq <= 0) {
        continue;
      }

      const key = assignment.slice(0, eq);
      if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
        keys.add(key);
      }
    }
  }

  return keys;
}

// The effective set of keys a redeploy must NOT carry: the vars deploy-soak-b.sh authors itself
// (OVERRIDDEN_KEYS) UNION every key the unit template renders via Environment= (derived, so it can
// never drift from the template). `unitText` is the soak-b.service source; omit it to fall back to
// the static OVERRIDDEN_KEYS alone (kept for the pure unit tests of filterCarriedEnv).
export function effectiveOverriddenKeys(unitText) {
  const keys = new Set(OVERRIDDEN_KEYS);
  if (unitText != null) {
    for (const key of parseUnitEnvironmentKeys(unitText)) {
      keys.add(key);
    }
  }

  return keys;
}

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

// Strip ONE surrounding matching quote pair (`"…"` or `'…'`) from an env value. `.env`/systemd
// EnvironmentFile syntax legally wraps a value in quotes (`DATABASE_URL="postgresql://…"`), and
// parseEnvLine returns the RAW RHS including those quotes — fine for the carry (which re-emits the
// line verbatim), but a caller that must INTERPRET the value (derive-database-url → new URL()) needs
// the bare value or a quoted-but-valid URL is falsely rejected as unparseable. Only strips a matched
// leading+trailing pair; a lone or mismatched quote is left untouched (so a genuinely malformed value
// still surfaces downstream rather than being silently "repaired").
export function unquoteEnvValue(value) {
  const v = String(value);
  if (v.length >= 2) {
    const first = v[0];
    const last = v[v.length - 1];
    if ((first === '"' || first === "'") && last === first) {
      return v.slice(1, -1);
    }
  }

  return v;
}

// Guard against multi-line / unterminated-quote values in the source env (defect: the carry splits
// the raw file on `\n`, so a quoted value spanning newlines — `FOO='a<newline>b'` — would be emitted
// truncated on the first line and its continuation misparsed as garbage lines). systemd
// EnvironmentFile semantics don't round-trip such values through this script anyway, so the correct
// behavior is to FAIL LOUD naming the offending key rather than silently emit a broken line. Scans
// the RAW file text (before the `\n` split) and throws on the first assignment whose value opens a
// quote it never closes on the same line.
export function assertNoMultilineValues(text) {
  const lines = String(text).split('\n');
  for (const line of lines) {
    const stripped = line.replace(/^\s*export\s+/, '').trim();
    if (stripped === '' || stripped.startsWith('#')) {
      continue;
    }

    const eq = stripped.indexOf('=');
    if (eq <= 0) {
      continue;
    }

    const key = stripped.slice(0, eq);
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
      continue;
    }

    // Two ways a value legally spans lines (and must be refused, not emitted truncated):
    const value = stripped.slice(eq + 1);
    const quote = value[0];

    //  (a) it OPENS with a quote that never closes on this line (the quote's content continues on the
    //      next line). Only checked for a leading quote; a properly-closed quote ends with the closing
    //      quote char, so it can't also trip the trailing-backslash check below.
    if (
      (quote === '"' || quote === "'") &&
      !hasUnescapedClosingQuote(value, quote)
    ) {
      throw new Error(
        `env var ${key} has an unterminated ${quote} quote / multi-line value — ` +
          'multi-line env values are not supported by this deploy carry; put the value on one line',
      );
    }

    //  (b) the value ends in an unescaped trailing `\` — a line continuation (systemd/shell splice the
    //      next line on, dropping the newline). Applies to unquoted values AND to a trailing backslash
    //      AFTER a closing quote (e.g. `FOO="abc"\`): a closed quoted value ends in its quote char, so
    //      this only fires when a real dangling continuation backslash follows. `endsWithOddBackslashes`
    //      is true only for a real, unescaped continuation (a trailing `\\` is an escaped literal).
    if (endsWithOddBackslashes(value)) {
      throw new Error(
        `env var ${key} ends in a line-continuation backslash / multi-line value — ` +
          'multi-line env values are not supported by this deploy carry; put the value on one line',
      );
    }
  }
}

// Does the string end with an ODD number of backslashes (so a trailing `\` is unescaped → a real line
// continuation)? `x\` → true (continuation); `x\\` → false (escaped literal backslash, no splice).
function endsWithOddBackslashes(s) {
  let count = 0;
  for (let i = s.length - 1; i >= 0 && s[i] === '\\'; i--) {
    count++;
  }

  return count % 2 === 1;
}

// Does the quoted value (opening with `quote`) close with a MATCHING, UNESCAPED quote on this line?
// A bare last-char check is wrong: `FOO="abc\"` ends in `"` but that quote is backslash-escaped, so
// the value is actually unterminated (its continuation is on the next line). Backslash escaping
// applies ONLY inside DOUBLE quotes — inside single quotes `\` is literal and `'…\'` DOES close (so
// we must not treat that `\` as an escape, or we'd false-positive on a valid single-quoted value).
// The value is closed iff we reach a matching quote that is not backslash-escaped (double-quote only).
function hasUnescapedClosingQuote(value, quote) {
  const honorsBackslash = quote === '"';
  let escaped = false;
  for (let i = 1; i < value.length; i++) {
    const ch = value[i];
    if (escaped) {
      escaped = false;

      continue;
    }

    if (honorsBackslash && ch === '\\') {
      escaped = true;

      continue;
    }

    if (ch === quote) {
      return true;
    }
  }

  return false;
}

// ── DATABASE_URL derivation (defect: redeploy clobbered a role-authenticated TCP DATABASE_URL) ─────
//
// The env regen used to author `DATABASE_URL=postgresql:///${DB_NAME}` unconditionally — a peer-auth
// form that only works where the unit's OS user maps to a DB role via peer auth on the default
// socket. If the SOURCE env's DATABASE_URL carried an explicit role+password over TCP (a perfectly
// normal setup), the redeploy silently replaced a working URL with a non-working one and the app
// then spun on DB-connection diagnostics at startup while the unit sat happily `active`.
//
// The correct move is to PRESERVE the source URL's connection identity (scheme, userinfo, host, port,
// query) and swap ONLY the database — the path segment. Shell string surgery (`${A%/*}/newdb`)
// corrupts URLs whose password (or any earlier segment) contains reserved characters like `/`, `?`,
// `@` or `#` (observed), so we parse with `new URL()`. Any URL WHATWG can parse round-trips exactly
// except the single path swap; an unparseable source URL throws (the CLI surfaces it, the shell falls
// back to the peer-auth form). Returns the derived `postgres[ql]://…/<dbName>` string.
//
// `dbName` is validated as a bare SQL identifier (the deploy guards DB_NAME=euler_rt_b before this is
// reached, but keep the function self-defending so a caller can't inject path/query syntax through it).
export function deriveDatabaseUrl(sourceUrl, dbName) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(String(dbName ?? ''))) {
    throw new Error(
      `deriveDatabaseUrl: dbName ${JSON.stringify(dbName)} is not a bare SQL identifier`,
    );
  }
  const url = new URL(sourceUrl); // throws on an unparseable source URL — caller decides the fallback
  // Guard against fail-open on a corrupt source: `new URL()` happily parses ANY WHATWG scheme, so a
  // stray `mysql://…`/`http://…` in the source env would derive cleanly and write a non-Postgres
  // DATABASE_URL for the unit (the app would then fail at connect time with no hint at deploy). Only
  // the Postgres URL schemes are valid here — refuse anything else LOUD so a corrupt env aborts.
  if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:') {
    throw new Error(
      `deriveDatabaseUrl: unsupported DATABASE_URL scheme ${JSON.stringify(url.protocol)} — ` +
        'expected postgres: or postgresql:',
    );
  }
  // Swap ONLY the database (the single path segment). `new URL()` percent-encodes the assignment, so
  // a bare identifier can never smuggle a query/fragment; everything else — scheme, userinfo (role +
  // reserved-char password), host, port, search — is preserved by the parser exactly.
  url.pathname = `/${dbName}`;

  return url.href;
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
//   carry-env <envfile> [unitfile]   → prints the KEY=value lines to carry (one per line) to stdout.
//                                      With <unitfile>, excludes every key the unit renders via
//                                      Environment= (F1) in addition to OVERRIDDEN_KEYS, and fails
//                                      loud on any multi-line/unterminated-quote value (F3).
//   resolve-chains <chains> <json>   → prints the canonical EULER_CHAINS value to stdout; exits
//                                      non-zero with a loud message on stderr for unknown names.
//   derive-database-url <envfile> <dbName>
//                                    → derive the new DATABASE_URL from the source env's own
//                                      DATABASE_URL (swap only the database, preserve everything
//                                      else) and print it. A quoted source value
//                                      (`DATABASE_URL="postgresql://…"`) is unquoted first (normal
//                                      .env syntax). When the source has NO DATABASE_URL, exit code 3
//                                      (a SILENT signal, no stderr) so the shell can fall back to the
//                                      peer-auth form. An UNPARSEABLE or non-Postgres-scheme source
//                                      DATABASE_URL is a loud exit 1 (a corrupt env should abort, not
//                                      degrade to a peer-auth form that masks the real problem).
// All errors go to stderr and exit 1 so the shell's `$(…)` + `set -e` propagate the failure.
export function runCli(argv) {
  const [cmd, ...rest] = argv;
  if (cmd === 'derive-database-url') {
    const [file, dbName] = rest;
    if (!file || !dbName) {
      throw new Error(
        'usage: deploy-helpers.mjs derive-database-url <envfile> <dbName>',
      );
    }
    const text = readFileSync(file, 'utf8');
    // Find the source DATABASE_URL via the same env-line parser used for the carry (last one wins,
    // mirroring shell env semantics) so `export DATABASE_URL=…` and stray whitespace are handled.
    let sourceUrl = null;
    for (const line of text.split('\n')) {
      const parsed = parseEnvLine(line);
      if (parsed && parsed.key === 'DATABASE_URL') {
        // Unquote here (derive path only): a quoted `DATABASE_URL="postgresql://…"` is normal .env /
        // EnvironmentFile syntax; without stripping the wrapping quotes new URL() would reject a VALID
        // env file as unparseable and abort the deploy. carry-env keeps its verbatim `parsed.line`
        // output untouched — this unquoting is confined to the value we hand to the URL parser.
        sourceUrl = unquoteEnvValue(parsed.value);
      }
    }
    // No source DATABASE_URL → exit 3 (distinct from the loud exit 1) so the shell falls back to the
    // peer-auth form. Handled in the direct-run wrapper below, not thrown, so it stays a clean signal.
    if (sourceUrl == null || sourceUrl === '') {
      return { exitCode: 3 };
    }
    // An unparseable source DATABASE_URL is corrupt — fail LOUD (exit 1) rather than silently degrade
    // to a peer-auth form that would replace an intended-but-broken URL and mask the real problem.
    return deriveDatabaseUrl(sourceUrl, dbName);
  }
  if (cmd === 'carry-env') {
    const [file, unitFile] = rest;
    if (!file) {
      throw new Error(
        'usage: deploy-helpers.mjs carry-env <envfile> [unitfile]',
      );
    }
    const text = readFileSync(file, 'utf8');
    // F3: refuse to carry a source env with a multi-line/unterminated-quote value (would be emitted
    // truncated) — fail loud naming the key instead.
    assertNoMultilineValues(text);
    // F1: exclude keys the unit template renders via Environment= (derived, never drifts) on top of
    // OVERRIDDEN_KEYS, so a stale carried copy can't shadow the unit's freshly-rendered value.
    const overridden = unitFile
      ? effectiveOverriddenKeys(readFileSync(unitFile, 'utf8'))
      : OVERRIDDEN_KEYS;

    return filterCarriedEnv(text.split('\n'), overridden).join('\n');
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
    // A subcommand may return a `{ exitCode }` sentinel (e.g. derive-database-url signalling "no
    // source DATABASE_URL → fall back") — exit with that code and print nothing.
    if (out != null && typeof out === 'object' && 'exitCode' in out) {
      process.exit(out.exitCode);
    }
    if (out) {
      process.stdout.write(`${out}\n`);
    }
  } catch (err) {
    process.stderr.write(`${err.message}\n`);
    process.exit(1);
  }
}
