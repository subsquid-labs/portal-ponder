---
name: portal-ponder-release
description: Cut a new @subsquid/ponder release — the Portal fork of ponder, versioned <ponder-version>-sqd.<rev>. Bump/confirm the target ponder version in versions.json, trigger the release workflow (workflow_dispatch; a v<version> tag also works but only for rev 1), watch it build+test+publish to npm via Trusted Publishing, then set the row's published: true and write GitHub release notes for the v<version> tag. Use when the user asks to "release", "publish", "cut a version", "ship", or "re-cut a fork revision" for @subsquid/ponder / portal-ponder.
---

# @subsquid/ponder release

End-to-end release procedure for **`@subsquid/ponder`** — the drop-in Portal fork of `ponder`.
A release is `ponder@<version>` + the Portal layer (`portal/`), published to npm as
`@subsquid/ponder@<version>-sqd.<rev>`. The bin stays `ponder`, so it is a drop-in install.

The fork is **generated, not hand-maintained**: [`scripts/sync-upstream.sh <ver>`](../../../scripts/sync-upstream.sh)
clones `ponder@<ver>`, drops in the two Portal modules + the per-version `portal/wiring/<ver>.patch`,
renames the package, and builds. The publishable artifact lives in the generated tree at
`packages/core`, **not** in this repo — this repo only carries the Portal layer.

Publishing runs on **npm Trusted Publishing** (OIDC, no `NPM_TOKEN`) via
[`.github/workflows/release.yml`](../../../.github/workflows/release.yml). The workflow guards that
the requested version is declared in [`versions.json`](../../../versions.json) before it builds.

Full background: [`PUBLISHING.md`](../../../PUBLISHING.md) and `versions.json` are the sources of truth.

> **Scope:** this releases `@subsquid/ponder` only. Do not confuse the **git tag** (`v<ponder-version>`,
> e.g. `v0.16.6`) with the **npm version** (`<ponder-version>-sqd.<rev>`, e.g. `0.16.6-sqd.1`) — see
> [The two version forms](#the-two-version-forms).

## Preconditions

Confirm before starting:

- The user named a **ponder version** to release (e.g. `0.16.6`). If not, ask. It must already exist
  in `versions.json` with a `portal/wiring/<ver>.patch`. If it doesn't, this is an *add a version*
  task first — see [Adding a new ponder version](#adding-a-new-ponder-version).
- The target row's wiring patch **applies + builds + tests green** locally:
  `SYNC_REV=<rev> scripts/sync-upstream.sh <ver> --test`. CI runs this for every row on each push,
  so a green `ci.yml` on the branch is a good proxy — but re-run it if the patch or Portal modules
  changed since the last CI run.
- You know the **fork revision** (`rev`). First cut of a ponder version is `1`. Bump it (`2`, `3`, …)
  only to re-cut a fork-side fix on the **same** ponder version — npm permanently retires an
  unpublished version number, so `0.16.6-sqd.1` can never be re-published after a bad build.
- A **trusted publisher** for `@subsquid/ponder` is configured on npmjs.com pointing at
  `subsquid-labs/portal-ponder` → `release.yml`. One-time setup — see
  [First-time setup](#first-time-setup-trusted-publishing). Without it, the publish step 404s.

## The two version forms

This trips people up. There are two distinct strings:

| Form | Example | Where it lives |
|------|---------|----------------|
| **git tag** | `v0.16.6` | what you push to trigger the workflow; the bare ponder version, prefixed `v` |
| **npm version** | `0.16.6-sqd.1` | what actually publishes; ponder version + `-sqd.<rev>` |

The workflow strips `v` off the tag → `0.16.6`, guards it against `versions.json` (which stores
**bare** ponder versions), then `sync-upstream.sh` appends `-sqd.<rev>` at build time.

**Consequence:** tag `v0.16.6`, never `v0.16.6-sqd.1` — the `-sqd.1` form would fail the
`versions.json` guard (no such row). And the **tag-push path always publishes `rev` = 1**
(`SYNC_REV` defaults to `1` on a tag push). To publish `rev` ≥ 2, you **must** use the manual
`workflow_dispatch` and pass the `rev` input — a tag push can't express a revision bump.

## Steps

### 1. Pick the trigger

Two ways in, same `publish` job:

- **Manual (preferred, and required for rev ≥ 2):** Actions → **release** → *Run workflow* →
  enter `version` (e.g. `0.16.6`) and `rev` (e.g. `1`). From the CLI:

  ```sh
  gh workflow run release.yml --repo subsquid-labs/portal-ponder -f version=0.16.6 -f rev=1
  ```

- **Tag push (rev 1 only):** push a `v<version>` tag on the release branch.

  ```sh
  git tag v0.16.6
  git push origin v0.16.6
  ```

  The workflow triggers on `tags: ['v*.*.*']`. This path is `rev` = 1 by construction.

### 2. Watch the workflow

```sh
gh run watch --repo subsquid-labs/portal-ponder --exit-status
```

Or grab the latest run explicitly:

```sh
RUN_ID=$(gh run list --repo subsquid-labs/portal-ponder --workflow=release.yml --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN_ID" --repo subsquid-labs/portal-ponder --exit-status
```

The single `publish` job runs: resolve version → **guard against `versions.json`** →
`sync-upstream.sh <ver> --test` (apply layer, build, run Portal unit tests) → `npm publish`. If the
publish step fails on Trusted Publishing, the npm-side config is missing/misconfigured — surface the
error, don't retry blindly. See [Failure modes](#failure-modes).

### 3. Confirm the publish and dist-tag

The workflow tags `latest` **only** for the newest ponder version in `versions.json`; publishing an
older version (e.g. `0.15.17` after `0.16.6`) publishes it under `ponder-<version>` so it does not
clobber the default `npm install`. Verify where it landed:

```sh
npm view @subsquid/ponder dist-tags
npm view @subsquid/ponder@<version>-sqd.<rev> version   # confirm the exact build exists
```

`latest` should point at the newest ponder version's `-sqd` build; an older release should sit under
`ponder-<version>` and users pin the exact `@subsquid/ponder@<version>-sqd.<rev>`.

### 4. Flip the versions.json row to published

After a successful publish, update the row in [`versions.json`](../../../versions.json): set
`"published": true` (leave `"status"` as the verification state — `"verified"`; the schema tracks
publish state in the separate `published` boolean, not via a `status` value), bump `"rev"` if you cut
a new revision, and trim the `note` to reflect reality (drop stale "publish candidate / BROKEN seed"
caveats once they no longer apply). Commit:

```sh
git add versions.json
git commit -m "chore(release): @subsquid/ponder <version>-sqd.<rev> published"
git push
```

This is a real state change other releases and CI read — don't skip it.

### 5. Write the GitHub release notes

**Required — every release gets one.** `release.yml` publishes to npm only; it does **not** create a
GitHub Release, so this is a manual step you always do. **Order matters:** creating any `v*.*.*` tag —
including implicitly via `gh release create` — fires the publish workflow, so create the release/tag
only *after* the npm publish has succeeded (the step ordering here already does). Note this isn't
fully clean even then: after a `workflow_dispatch` publish, creating the `v<version>` tag/release
still re-triggers the workflow, which re-runs and **fails at the publish step on the already-published
version** — a guaranteed red run, harmless to npm. Expect and ignore it until a planned workflow
idempotency guard lands. Write curated, Portal-layer-scoped highlights
(what the fork changed vs plain `ponder@<version>`: the Portal backfill seam, wiring touch-points,
correctness fixes, tests) — **not** a restatement of ponder's own changelog. Use
[release-template.md](release-template.md) as the shape, and source highlights from the commits since
the previous `-sqd` cut. Apply from a file to avoid heredoc escaping around code fences:

```sh
# Tag-push release: the v<version> tag already exists — just attach the release.
gh release create v<version> --repo subsquid-labs/portal-ponder \
  --title "@subsquid/ponder <version>-sqd.<rev>" --notes-file notes.md

# If the release already exists (e.g. re-editing notes):
gh release edit v<version> --repo subsquid-labs/portal-ponder --notes-file notes.md
```

**Manual-dispatch releases have no tag** (a `workflow_dispatch` run doesn't push one). `gh release
create` will create the `v<version>` tag for you, but it defaults to the branch HEAD — pin it to the
exact commit that was released with `--target`:

```sh
gh release create v<version> --repo subsquid-labs/portal-ponder \
  --target <released-commit-sha> \
  --title "@subsquid/ponder <version>-sqd.<rev>" --notes-file notes.md
```

For a re-cut (`rev` ≥ 2), the `v<version>` tag from the earlier revision already exists — don't move
it. Append the revision's notes to the existing `v<version>` release. If you want a distinct release for
the revision, give it a tag name that **cannot** match `v*.*.*` (e.g. `sqd/<version>-sqd.<rev>`) — a
`v<version>-sqd.<rev>` tag has three dots, matches the workflow's `tags: ['v*.*.*']` trigger, and would
fire a publish run that then fails the `versions.json` guard (harmless to npm, but a guaranteed red run).

### 6. Print the summary

```sh
echo "npm:    @subsquid/ponder@<version>-sqd.<rev>"
echo "tag:    https://github.com/subsquid-labs/portal-ponder/releases/tag/v<version>"
npm view @subsquid/ponder dist-tags
```

## Re-cutting a fork revision (same ponder version)

A bad build on `<version>-sqd.1` can't be re-published under the same number. To ship a fork-side fix
on the **same** ponder version:

1. Fix the Portal layer (`portal/*.ts` or `portal/wiring/<ver>.patch`) and land it on the branch.
2. `SYNC_REV=2 scripts/sync-upstream.sh <ver> --test` — confirm green.
3. Bump the row's `"rev"` in `versions.json`.
4. Release via **manual dispatch** with `rev=2` (a tag push can't — it's always rev 1).

## Adding a new ponder version

If the requested ponder version has no `versions.json` row / wiring patch, it can't be released yet.
Per [`PUBLISHING.md`](../../../PUBLISHING.md):

1. Author `portal/wiring/<ver>.patch` — the 4 touch-points (`internal/types.ts`, `config/index.ts`,
   `build/config.ts`, `runtime/historical.ts`). See an existing patch for the shape.
2. `scripts/sync-upstream.sh <ver> --test` — applies, builds, Portal tests pass.
3. Add a `{ "ponder": "<ver>", "wiring": "wiring/<ver>.patch", "rev": 1, "status": "verified" }` row
   and add `<ver>` to `compat.tested`.
4. Commit (CI proves the seam on every push). Then release with the steps above.

## First-time setup (Trusted Publishing)

Publishing uses OIDC — there is no `NPM_TOKEN`. Once, before the first automated release, a maintainer
with publish rights on the `@subsquid` scope configures a trusted publisher on npmjs.com:

- `@subsquid/ponder` → **Settings** → **Trusted Publisher** → GitHub Actions.
- Repository: `subsquid-labs/portal-ponder`
- Workflow filename: `release.yml`

If npm requires the package to exist before a trusted publisher can be added, **seed it with one
manual publish**, then configure the publisher — every release after that is tokenless:

```sh
SYNC_REV=1 SYNC_WORKDIR=/tmp/sqd-fork scripts/sync-upstream.sh <ver> --test   # build + test locally
cd /tmp/sqd-fork/<ver>/packages/core && npm publish --access public --tag latest   # requires npm login
```

For this **manual seed publish only**, add `--provenance` when the repo is public for a signed
provenance attestation. The automated `release.yml` path needs nothing — npm attaches provenance
automatically under OIDC Trusted Publishing.

## Failure modes

- **Guard fails: "`<ver>` not in versions.json"**: the version has no row / wiring patch. Add it first
  — see [Adding a new ponder version](#adding-a-new-ponder-version). Don't hand-edit the workflow to
  skip the guard.
- **Tag pushed as `v<ver>-sqd.<rev>`**: it fails the `versions.json` guard (rows are bare ponder
  versions). Delete the tag, push `v<ver>` instead. For a revision bump, use manual dispatch with the
  `rev` input, not a tag.
- **`sync-upstream.sh` fails (patch won't apply / build breaks)**: the wiring patch drifted against
  that ponder version, or the Portal modules broke the build. Reproduce locally with
  `SYNC_REV=<rev> scripts/sync-upstream.sh <ver> --test`, fix the patch/modules, re-run. This is the
  same step CI runs, so a red `ci.yml` points at the same fix.
- **Publish fails on Trusted Publishing (404/401)**: the trusted publisher for `@subsquid/ponder` is
  missing or points at the wrong repo/workflow. See
  [First-time setup](#first-time-setup-trusted-publishing). Don't fall back to a token-based
  `npm publish` unless the user explicitly asks.
- **npm >= 11.5.1 required**: OIDC publishing needs a recent npm; node 22 ships an older one. The
  workflow already does `npm install -g npm@latest` before publishing — if you publish locally, do
  the same or the OIDC step is unavailable.
- **Version already published**: `0.16.6-sqd.1` exists on npm and can't be re-published. Bump the
  `rev` and re-cut via manual dispatch — see
  [Re-cutting a fork revision](#re-cutting-a-fork-revision-same-ponder-version).
- **`latest` clobbered by an older release**: publishing an older ponder version should route to
  `ponder-<version>`, not `latest` — the workflow computes the max from `versions.json`. If `latest`
  drifted (e.g. a manual publish), repoint it: `npm dist-tag add @subsquid/ponder@<newest-sqd> latest`
  (requires npm auth — a manual recovery outside the OIDC workflow).
