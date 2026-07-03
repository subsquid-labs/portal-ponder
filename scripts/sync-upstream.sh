#!/usr/bin/env bash
# Build @subsquid/ponder@<ver> = ponder@<ver> + the Portal layer (portal/).
#
#   scripts/sync-upstream.sh <ponder-version> [--test | --coverage]
#
# --test runs the Portal-layer vitest suite; --coverage runs it with v8 coverage scoped to the
# Portal source (installs a matching provider, writes portal-coverage/coverage-summary.json).
#
# The fork is GENERATED, not hand-maintained: we clone ponder at the version tag (the
# monorepo carries the build tooling the npm tarball omits), drop in the 2 Portal modules,
# apply the small wiring patch for that version, rename the package, and build. Tracking a
# new ponder version = author one wiring patch + run this. See PUBLISHING.md + versions.json.
set -euo pipefail

VER="${1:?usage: sync-upstream.sh <ponder-version> [--test | --coverage]}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${SYNC_WORKDIR:-/tmp/sqd-ponder-fork}/$VER"
WIRING="$ROOT/portal/wiring/$VER.patch"
PNPM="corepack pnpm@9.10.0"

[ -f "$WIRING" ] || { echo "✗ no wiring patch for $VER at portal/wiring/$VER.patch — author it (PUBLISHING.md §'Adding a version')"; exit 1; }

if [ "${2:-}" = "--test" ]; then
  # Formatting/lint gate BEFORE anything is copied: the Portal layer must be clean under the repo's
  # own Biome (root biome.json; version pinned to the repo devDependency). `check` fails on format
  # drift and lint ERRORS (warnings pass) — so an unformatted file can never ship silently.
  echo "▶ biome check (repo config, pinned 2.5.2)"
  ( cd "$ROOT" && npx -y @biomejs/biome@2.5.2 check portal/ --diagnostic-level=error )
fi

echo "▶ cloning ponder@$VER → $WORK"
rm -rf "$WORK"; git clone --quiet --depth 1 --branch "ponder@$VER" https://github.com/ponder-sh/ponder "$WORK"

CORE="$WORK/packages/core"
SYNC="$CORE/src/sync-historical"
echo "▶ applying Portal layer"
# Copy the whole Portal layer by GLOB (source + tests together) so a newly added portal-*.ts /
# realtime-*.ts module can never be silently missed. `config.ts` (deleted) and `vite.portal.config.ts`
# are intentionally excluded by the prefix. The vite include globs (portal*.test.ts, realtime*.test.ts)
# already pick up any new test file, so they need no change.
cp "$ROOT"/portal/portal*.ts "$ROOT"/portal/realtime*.ts "$SYNC/"
mkdir -p "$SYNC/__fixtures__"; cp "$ROOT/portal/__fixtures__/"*.json "$SYNC/__fixtures__/"
cp "$ROOT/portal/vite.portal.config.ts" "$CORE/"
( cd "$WORK" && git apply --verbose "$WIRING" )

# version = <ponder-version>-sqd.<rev> so the fork can re-cut within a ponder version (npm
# permanently retires an unpublished version number, so exact-mirror can't absorb a fix).
# The ponder version stays visible; SYNC_REV bumps the fork revision (default 1).
REV="${SYNC_REV:-1}"
echo "▶ renaming → @subsquid/ponder@$VER-sqd.$REV (bin stays 'ponder' so it's drop-in)"
node -e "const f='$CORE/package.json',p=require(f);p.name='@subsquid/ponder';p.version='$VER-sqd.$REV';p.repository={type:'git',url:'git+https://github.com/subsquid-labs/portal-ponder.git'};p.bugs={url:'https://github.com/subsquid-labs/portal-ponder/issues'};p.homepage='https://sqd.dev/portal/';require('fs').writeFileSync(f,JSON.stringify(p,null,2)+'\n')"

echo "▶ building"
( cd "$WORK" && $PNPM install --silent && $PNPM --filter @ponder/utils build && $PNPM --filter @subsquid/ponder build )

# Rewrite `workspace:*` deps to real published versions. `npm publish` (unlike `pnpm publish`)
# does NOT do this, so without it the package is uninstallable standalone — e.g.
# @ponder/utils: workspace:* → @ponder/utils@0.2.18.
echo "▶ resolving workspace: deps → real versions"
node -e "
const fs=require('fs'); const pkgPath='$CORE/package.json'; const pkg=JSON.parse(fs.readFileSync(pkgPath));
const ver={}; for(const d of fs.readdirSync('$WORK/packages')){ try{ const p=JSON.parse(fs.readFileSync('$WORK/packages/'+d+'/package.json')); ver[p.name]=p.version; }catch{} }
let n=0; for(const s of ['dependencies','devDependencies','peerDependencies']){ const deps=pkg[s]||{}; for(const k of Object.keys(deps)){ if(String(deps[k]).startsWith('workspace:')){ const v=ver[k]; if(!v){console.error('no local version for workspace dep '+k);process.exit(1);} deps[k]=v; n++; console.log('  '+k+' -> '+v); } } }
fs.writeFileSync(pkgPath, JSON.stringify(pkg,null,2)+'\n'); console.log('  rewrote '+n+' workspace dep(s)');
"

if [ "${2:-}" = "--test" ]; then
  # `fast-check` (property tests) is a DEV-ONLY dep of the clone — never a runtime dep of the published
  # package. We provision it straight into the core's node_modules rather than adding it to package.json,
  # because renaming the core to @subsquid/ponder orphans sibling workspace packages (e.g. benchmark's
  # `ponder@workspace:*`), so a lockfile re-resolve (which a new package.json dep would force) fails.
  echo "▶ provisioning fast-check (dev-only, for the property tests)"
  # OUTSIDE the clone so npm doesn't walk up into the pnpm workspace root. Pinned exactly for
  # reproducible CI; the whole resolved tree is copied so transitive deps can never be missed.
  FCDIR="$(mktemp -d)"
  ( cd "$FCDIR" && npm init -y >/dev/null 2>&1 && npm install --no-audit --no-fund --silent fast-check@3.23.2 )
  cp -R "$FCDIR/node_modules/." "$CORE/node_modules/"
  rm -rf "$FCDIR"
  echo "▶ running Portal-layer tests"
  ( cd "$CORE" && pnpm exec vitest run --config vite.portal.config.ts )
fi

# --coverage: run the same suite with v8 coverage scoped to the Portal source (see the `coverage`
# block in vite.portal.config.ts). The provider version must EXACTLY match the installed vitest
# (vitest errors on a mismatch), so we resolve the concrete installed version from node_modules
# rather than the package.json spec (which may be a range like ^2.1.9 that floats to a newer patch).
# The renamed core breaks a workspace-wide `pnpm add` (benchmark still deps ponder@workspace:*), so
# install it standalone with --ignore-workspace. Writes portal-coverage/coverage-summary.json under $CORE.
if [ "${2:-}" = "--coverage" ]; then
  VITEST_VER="$(node -e "console.log(require('$CORE/node_modules/vitest/package.json').version)")"
  echo "▶ installing @vitest/coverage-v8@$VITEST_VER (matching installed vitest)"
  ( cd "$CORE" && $PNPM add -D "@vitest/coverage-v8@$VITEST_VER" --ignore-workspace )

  echo "▶ running Portal-layer tests with coverage"
  ( cd "$CORE" && pnpm exec vitest run --config vite.portal.config.ts --coverage )
  echo "✓ coverage summary → $CORE/portal-coverage/coverage-summary.json"
fi

echo "✓ built @subsquid/ponder@$VER-sqd.$REV → $CORE"
echo "  publish: cd $CORE && npm publish --access public --tag latest   # --tag latest: make this prerelease the default install"
