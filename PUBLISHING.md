# Publishing `@subsquid/ponder` (the Portal fork)

## The model — mirror the version, keep only the layer

`@subsquid/ponder` is a **drop-in fork** of `ponder`: same `ponder` bin, plus a `portal:`
field per chain that routes the historical backfill through SQD Portal (realtime stays on `rpc`).

- **Version = exact mirror.** `@subsquid/ponder@X.Y.Z` is built from `ponder@X.Y.Z` + the Portal
  layer. The match is the number — nothing to decode, default `npm install` works, clients pin exactly.
- **We don't hand-maintain a fork.** This repo holds only the **Portal layer** (`portal/`): two modules
  (`portal.ts`, `portal-transform.ts`) + a per-version `wiring/<ver>.patch` (the 4 one-line touch-points)
  + `config.ts` (`withPortal`). That's the entire diff against upstream.
- **The fork is generated.** `scripts/sync-upstream.sh <ver>` clones `ponder@<ver>`, applies the layer,
  renames the package, and builds — producing the publishable package. Tracking a new ponder version is
  "author one small patch + run the script", not "merge a fork".
- **Version-aware by construction.** `versions.json` is the source of truth: which ponder versions we
  support, each with its wiring patch + status (`verified` / `planned` / `published`), and the
  `compat.tested` list the CI matrix proves the seam against. We publish a fork release **only for ponder
  versions a client needs** (the economy), but we *know* which past/future versions hold (the awareness).

## Releasing — automated (npm Trusted Publishing via OIDC, no token)

Releases run through [`.github/workflows/release.yml`](.github/workflows/release.yml) and
authenticate to npm with **OIDC Trusted Publishing** — no `NPM_TOKEN` (npm deprecated long-lived
tokens for CI in favour of the GitHub integration). The job applies the Portal layer to
`ponder@<version>`, builds, runs the Portal tests, then publishes.

**One-time setup** (a maintainer with publish rights on the `@subsquid` scope):

1. On npmjs.com → `@subsquid/ponder` → *Settings → Trusted Publisher → GitHub Actions*, pointing at
   repo `subsquid/portal-ponder`, workflow `release.yml`.
   - *Brand-new package:* if npm requires the package to exist before you can add a trusted publisher,
     seed it with one manual publish (below), then configure the trusted publisher — every release
     after that is tokenless.
2. *(When the repo is public)* add `--provenance` to the publish step for a signed provenance attestation.

**Cut a release:** Actions → **release** → *Run workflow* → enter the ponder version (e.g. `0.16.6`),
or push a tag `v0.16.6`. The workflow guards that the version is in `versions.json`, builds + tests, and
publishes `@subsquid/ponder@<version>`. Then flip that row's `status`→`published` in `versions.json`.

**Manual / local** (to seed the first publish, or as a fallback):

```bash
scripts/sync-upstream.sh 0.16.6 --test                       # clone + apply layer + build + test
cd "$SYNC_WORKDIR/0.16.6/packages/core" && npm publish --access public   # uses your local npm login
```

## Add a new ponder version

1. **Author the wiring patch.** The seam (`syncBlockRangeData`/`syncBlockData`) is stable, but the 4
   touch-points (`internal/types.ts`, `config/index.ts`, `build/config.ts`, `runtime/historical.ts`)
   can drift. Clone the target, hand-apply the same 4 edits (see `portal/wiring/0.16.6.patch` for the
   shape), then `git diff > portal/wiring/<ver>.patch`.
2. `scripts/sync-upstream.sh <ver> --test` — confirm it applies + builds + the Portal unit tests pass.
3. Add a `{ "ponder": "<ver>", "wiring": "wiring/<ver>.patch", "status": "verified" }` row to
   `versions.json` and add `<ver>` to `compat.tested`.
4. Commit; publish when a client needs it.

CI (`.github/workflows/ci.yml`) runs step 2 for every version in `versions.json` on each push, so a
ponder upgrade that breaks the seam is caught before release.

## Why a fork (not a thin plugin)

ponder's published `exports` only expose `.` and `./virtual`; the internals the Portal sync needs
(`runtime/filter`, `internal/types`, the SyncStore) aren't importable. A loader-hook plugin can reach
them but is fragile and version-coupled in a different way. The fork is the robust, drop-in path today;
the thin plugin remains a possible future once ponder exposes a `HistoricalSync` hook (tracked upstream).
