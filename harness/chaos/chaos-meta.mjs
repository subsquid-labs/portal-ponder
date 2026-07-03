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

import { readFileSync, writeFileSync } from 'node:fs';
import { basename } from 'node:path';

// The fields that MUST match for a baseline to be a valid comparison target. `scenario` is NOT here:
// a baseline is by definition the clean (no-fault, no-kill) build of the same app/range, so it will
// differ in scenario/kills by design — matching on those would wrongly reject every baseline.
export const MATCH_FIELDS = [
  'app',
  'from',
  'to',
  'portal',
  'tarball',
  'chainId',
  'factory',
];

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
    chainId: Number(env.CHAOS_META_CHAIN_ID ?? 0),
    factory: env.CHAOS_META_FACTORY ?? '',
    scenario: env.CHAOS_META_SCENARIO ?? 'none',
    kills: Number(env.CHAOS_META_KILLS ?? 0),
    writtenAt: env.CHAOS_META_NOW ?? new Date().toISOString(),
  };
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

  console.error('usage: chaos-meta.mjs write|match ...');
  process.exit(2);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
