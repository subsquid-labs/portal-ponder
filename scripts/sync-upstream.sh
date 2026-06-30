#!/usr/bin/env bash
# Build @subsquid/ponder@<ver> = ponder@<ver> + the Portal layer (portal/).
#
#   scripts/sync-upstream.sh <ponder-version> [--test]
#
# The fork is GENERATED, not hand-maintained: we clone ponder at the version tag (the
# monorepo carries the build tooling the npm tarball omits), drop in the 2 Portal modules,
# apply the small wiring patch for that version, rename the package, and build. Tracking a
# new ponder version = author one wiring patch + run this. See PUBLISHING.md + versions.json.
set -euo pipefail

VER="${1:?usage: sync-upstream.sh <ponder-version> [--test]}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${SYNC_WORKDIR:-/tmp/sqd-ponder-fork}/$VER"
WIRING="$ROOT/portal/wiring/$VER.patch"
PNPM="corepack pnpm@9.10.0"

[ -f "$WIRING" ] || { echo "✗ no wiring patch for $VER at portal/wiring/$VER.patch — author it (PUBLISHING.md §'Adding a version')"; exit 1; }

echo "▶ cloning ponder@$VER → $WORK"
rm -rf "$WORK"; git clone --quiet --depth 1 --branch "ponder@$VER" https://github.com/ponder-sh/ponder "$WORK"

CORE="$WORK/packages/core"
SYNC="$CORE/src/sync-historical"
echo "▶ applying Portal layer"
cp "$ROOT/portal/portal.ts" "$ROOT/portal/portal-transform.ts" "$SYNC/"
cp "$ROOT/portal/portal-transform.test.ts" "$ROOT/portal/portal.test.ts" "$SYNC/" 2>/dev/null || true
mkdir -p "$SYNC/__fixtures__"; cp "$ROOT/portal/__fixtures__/"*.json "$SYNC/__fixtures__/"
cp "$ROOT/portal/vite.portal.config.ts" "$CORE/"
( cd "$WORK" && git apply --verbose "$WIRING" )

echo "▶ renaming package → @subsquid/ponder (bin stays 'ponder' so it's drop-in)"
node -e "const f='$CORE/package.json',p=require(f);p.name='@subsquid/ponder';require('fs').writeFileSync(f,JSON.stringify(p,null,2)+'\n')"

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
  echo "▶ running Portal-layer tests"
  ( cd "$CORE" && pnpm exec vitest run --config vite.portal.config.ts )
fi

echo "✓ built @subsquid/ponder@$VER → $CORE  (publish: cd $CORE && npm publish --access public)"
