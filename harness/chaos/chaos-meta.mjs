// chaos-meta.mjs — run-metadata for the chaos/resume harness. kill-loop.sh writes a <db>.meta.json
// describing exactly what produced a store (app, block range, portal, tarball identity, scenario,
// chain, factory). verify-resume.sh reuses a baseline store ONLY if its metadata matches the chaos
// run's — a stale/mismatched baseline (different app/range/portal/tarball) byte-diffed as "identical"
// would be a silent false pass, so a mismatch must REFUSE the baseline.
//
//   node chaos-meta.mjs write <metaFile>                       # fields from CHAOS_META_* env
//   node chaos-meta.mjs match <baselineMetaFile> <chaosMetaFile>   # exit 0 = compatible, 1 = mismatch
//
// The comparison core (metaMatch) is pure + exported for unit tests.

import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { basename } from 'node:path';

// The fields that MUST match for a baseline to be a valid comparison target. `scenario` is NOT here:
// a baseline is by definition the clean (no-fault, no-kill) build of the same app/range, so it will
// differ in scenario/kills by design — matching on those would wrongly reject every baseline.
// `tarballHash` (sha256 of the tarball CONTENT) is included alongside the basename: a re-packed fork
// tarball keeps the same version/filename (0.16.6-sqd.N) but can carry different bytes, so a basename
// match alone would let a baseline built from a DIFFERENT build be reused as "identical" — a false
// pass. Both stores must have been built from byte-identical @subsquid/ponder.
export const MATCH_FIELDS = [
  'app',
  'from',
  'to',
  'portal',
  'tarball',
  'tarballHash',
  'chainId',
  'factory',
];

// sha256 of a file's bytes (hex), or a stable sentinel when there is no file (the published
// @subsquid/ponder — a versioned npm artifact whose identity travels in the version, not a local
// tarball). A read error is a HARD failure, not a silent 'unknown': a metadata record that cannot
// prove which build produced a store must never validate as matching.
export function tarballHash(path) {
  if (!path) {
    return 'published';
  }

  return `sha256:${createHash('sha256').update(readFileSync(path)).digest('hex')}`;
}

// Pure compatibility check: every MATCH_FIELDS value must be equal (compared as strings). Returns
// { ok, mismatches:[{field,baseline,chaos}] }.
export function metaMatch(baseline, chaos) {
  const mismatches = [];
  for (const f of MATCH_FIELDS) {
    const b = baseline?.[f];
    const c = chaos?.[f];
    if (String(b ?? '') !== String(c ?? '')) {
      mismatches.push({ field: f, baseline: b ?? null, chaos: c ?? null });
    }
  }

  return { ok: mismatches.length === 0, mismatches };
}

// Build a metadata object from CHAOS_META_* env. The tarball is reduced to its basename — the
// meaningful identity (version) travels in the filename; an absolute path is box-specific noise.
export function metaFromEnv(env = process.env) {
  const tarball = env.CHAOS_META_TARBALL
    ? basename(env.CHAOS_META_TARBALL)
    : 'published';

  return {
    app: basename(env.CHAOS_META_APP ?? ''),
    from: Number(env.CHAOS_META_FROM ?? 0),
    to: Number(env.CHAOS_META_TO ?? 0),
    portal: env.CHAOS_META_PORTAL ?? '',
    tarball,
    // sha256 of the tarball CONTENT — proves both stores were built from the same fork bytes, not
    // merely a same-named re-pack (see MATCH_FIELDS / tarballHash).
    tarballHash: tarballHash(env.CHAOS_META_TARBALL),
    chainId: Number(env.CHAOS_META_CHAIN_ID ?? 0),
    factory: env.CHAOS_META_FACTORY ?? '',
    scenario: env.CHAOS_META_SCENARIO ?? 'none',
    kills: Number(env.CHAOS_META_KILLS ?? 0),
    writtenAt: env.CHAOS_META_NOW ?? new Date().toISOString(),
  };
}

// The chaos run's `kills` count must clear the acceptance floor — a store that "completed" without
// being killed enough proves nothing about resume. Enforced at VERIFY time too (not only in
// kill-loop), so a hand-built or under-killed chaos store cannot pass verification. A non-integer /
// negative / missing kills is treated as unproven → not satisfied.
export function killsSatisfied(kills, minKills) {
  const k = Number(kills);
  const min = Number(minKills);
  if (!Number.isInteger(k) || k < 0) {
    return { ok: false, reason: `kills is not a valid count (${kills})` };
  }
  if (k < min) {
    return {
      ok: false,
      reason: `kills=${k} < MIN_KILLS=${min} — a resume run must be killed at least MIN_KILLS times`,
    };
  }

  return { ok: true };
}

function main() {
  const [cmd, a, b] = process.argv.slice(2);

  if (cmd === 'write') {
    if (!a) {
      console.error('usage: chaos-meta.mjs write <metaFile>');
      process.exit(2);
    }

    writeFileSync(a, `${JSON.stringify(metaFromEnv(), null, 2)}\n`);
    console.log(`wrote chaos metadata ${a}`);

    return;
  }

  if (cmd === 'match') {
    if (!a || !b) {
      console.error('usage: chaos-meta.mjs match <baselineMeta> <chaosMeta>');
      process.exit(2);
    }

    let baseline;
    let chaos;
    try {
      baseline = JSON.parse(readFileSync(a, 'utf8'));
      chaos = JSON.parse(readFileSync(b, 'utf8'));
    } catch (e) {
      // a missing/corrupt metadata file must REFUSE the baseline (fail closed), never reuse blindly
      console.error(
        `chaos-meta: cannot read metadata (${e.message}) — refusing baseline`,
      );
      process.exit(1);
    }

    const { ok, mismatches } = metaMatch(baseline, chaos);
    if (!ok) {
      console.error(
        'chaos-meta: baseline MISMATCH — refusing to reuse a stale baseline:',
      );
      for (const m of mismatches) {
        console.error(`  ${m.field}: baseline=${m.baseline} chaos=${m.chaos}`);
      }
      process.exit(1);
    }

    console.log('chaos-meta: baseline metadata matches the chaos run');

    return;
  }

  if (cmd === 'kills') {
    // node chaos-meta.mjs kills <chaosMetaFile> <minKills> — exit 0 if kills >= minKills, else 1.
    if (!a || b === undefined) {
      console.error('usage: chaos-meta.mjs kills <chaosMeta> <minKills>');
      process.exit(2);
    }

    let chaos;
    try {
      chaos = JSON.parse(readFileSync(a, 'utf8'));
    } catch (e) {
      console.error(
        `chaos-meta: cannot read chaos metadata (${e.message}) — cannot verify kills`,
      );
      process.exit(1);
    }

    const verdict = killsSatisfied(chaos.kills, b);
    if (!verdict.ok) {
      console.error(`chaos-meta: ${verdict.reason}`);
      process.exit(1);
    }

    console.log(`chaos-meta: kills=${chaos.kills} ≥ MIN_KILLS=${b}`);

    return;
  }

  console.error('usage: chaos-meta.mjs write|match|kills ...');
  process.exit(2);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
