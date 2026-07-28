# Publishing `@subsquid/ponder` (the Portal fork)

## The model — `<ponder>-sqd.<rev>`, keep only the layer

`@subsquid/ponder` is a **drop-in fork** of `ponder`: same `ponder` bin, plus a `portal:`
field per chain that routes the historical backfill through SQD Portal (realtime stays on `rpc`).

- **Version = `<ponder-version>-sqd.<rev>`.** `@subsquid/ponder@0.16.6-sqd.1` is `ponder@0.16.6` +
  the Portal layer (fork revision 1). The ponder version stays visible, and `-sqd.<rev>` lets the
  fork **re-cut a fix on the same ponder version** — npm permanently retires an unpublished version
  number, so a plain mirror (`0.16.6`) can't be re-published after a bad build. The **newest** ponder
  version's release is published with `--tag latest`, so `npm i @subsquid/ponder` resolves it (npm
  doesn't auto-pick a prerelease otherwise); an **older** ponder version publishes under a
  `ponder-<version>` dist-tag rather than clobbering `latest`, and is pinned as
  `@subsquid/ponder@<version>-sqd.<rev>`.
- **We don't hand-maintain a fork.** This repo holds only the **Portal layer** (`portal/`): the
  `portal-*.ts` modules (an invariant-first functional core behind the `portal.ts` shell — see
  `portal/INVARIANTS.md`) + a per-version `wiring/<ver>.patch` (the 4 one-line touch-points). That's
  the entire diff against upstream.
- **The fork is generated.** `scripts/sync-upstream.sh <ver>` clones `ponder@<ver>`, applies the layer,
  renames the package, and builds — producing the publishable package. Tracking a new ponder version is
  "author one small patch + run the script", not "merge a fork".
- **Version-aware by construction.** `versions.json` is the source of truth: which ponder versions we
  support, each with its wiring patch, a `status` verification state (`verified` / `planned`) plus a
  separate `published` boolean, and the `compat.tested` list the CI matrix proves the seam against.
  We publish a fork release **only for the ponder versions that are needed**, while tracking which past
  and future versions still hold.

## Supported lines & the backport policy

A **supported line** is a ponder version we advertise a **moving dist-tag** for — the newest under
`latest`, plus each older line under its `ponder-<version>` tag. The set is **data, not lore**: it lives
in `versions.json` under `supportedLines.lines` (today: `0.17.2`, `0.16.10`, `0.16.6`, `0.15.17`). The
Portal layer (`portal/`) is version-agnostic, so **every supported line can carry the same layer** — and
the policy below is what keeps them in sync.

> **Current state (2026-07-28):** the catch-up wave has **landed** — every supported line now carries the
> same current Portal layer (`0.17.2-sqd.1` / `0.16.10-sqd.2` / `0.16.6-sqd.3` / `0.15.17-sqd.3`, all built
> from the same `portal/` tree and published 2026-07-28). The earlier pre-policy divergence (fixes landing
> only on the newest line) is resolved; from here the policy below keeps them in lockstep.

Because the layer is shared, a fix must not live on only one line — going forward:

- **Every fork-side fix release triggers a backport wave.** When a `-sqd.<rev>` bump lands on `latest`
  (a Portal-layer fix, not just an upstream-parity version track), rev-bump **every other supported
  line** onto the same layer and re-publish it under its `ponder-<version>` dist-tag. `latest` is never
  clobbered — the release workflow tags non-newest versions with `ponder-<version>` automatically.
- **Each line is gated independently.** `scripts/sync-upstream.sh <ver> --test` must be green (full
  suite) for every line in the wave. A per-version gate failure **surfaces to a human** — ship the green
  lines, never a red one, and never silently skip a line.
- **This is a routine self-dispatch class** (shipped fixes, full gates, no new behavior) — the same
  parity-release autonomy grant covers backport rev-bumps.
- **Docs**: after a wave, the README version matrix shows each supported line's current `-sqd` rev plus a
  one-line freshness statement ("all supported lines carry the current Portal layer as of `<release>`"),
  and release notes list the wave.
- **EOL**: dropping a line from support is an explicit `supportedLines.lines` edit **and** a README note
  (human-visible in the PR) — never silent staleness. Pin-exact historical builds (no moving dist-tag)
  are frozen and are **not** backported.

## Releasing — automated (npm Trusted Publishing via OIDC, no token)

Releases run through [`.github/workflows/release.yml`](.github/workflows/release.yml) and
authenticate to npm with **OIDC Trusted Publishing** — no `NPM_TOKEN` (npm deprecated long-lived
tokens for CI in favour of the GitHub integration). The job applies the Portal layer to
`ponder@<version>`, builds, runs the Portal tests, then publishes.

**One-time setup** (a maintainer with publish rights on the `@subsquid` scope):

1. On npmjs.com → `@subsquid/ponder` → *Settings → Trusted Publisher → GitHub Actions*, pointing at
   repo `subsquid-labs/portal-ponder`, workflow `release.yml`.
   - *Brand-new package:* if npm requires the package to exist before you can add a trusted publisher,
     seed it with one manual publish (below), then configure the trusted publisher — every release
     after that is tokenless.
2. *(When the repo is public)* add `--provenance` to the publish step for a signed provenance attestation.

**Cut a release:** Actions → **release** → *Run workflow* → enter the ponder version (e.g. `0.16.6`)
and the `rev`. The workflow guards that the version is in `versions.json`, builds + tests, and
publishes `@subsquid/ponder@<version>-sqd.<rev>`. A `v0.16.6` tag push also triggers it, but the
**tag path always publishes `rev` 1** (`SYNC_REV` defaults to `1` on a tag — a tag can't express a
revision), so a rev bump (`-sqd.2` and up) **must** go through the manual `workflow_dispatch` with the
`rev` input. After a successful publish, set that row's `"published": true` in `versions.json` (leave
`"status"` as its verification state — the schema tracks publish state in the separate `published`
boolean, not via a `status` value).

**Manual / local** (to seed the first publish, or as a fallback):

```bash
SYNC_REV=1 scripts/sync-upstream.sh 0.16.6 --test            # → @subsquid/ponder@0.16.6-sqd.1, built + tested
cd "$SYNC_WORKDIR/0.16.6/packages/core" && npm publish --access public --tag latest   # local npm login
```

## Add a new ponder version

1. **Author the wiring patch.** The seam (`syncBlockRangeData`/`syncBlockData`) is stable, but the 4
   touch-points (`internal/types.ts`, `config/index.ts`, `build/config.ts`, `runtime/historical.ts`)
   can drift. Clone the target, hand-apply the same 4 edits (see `portal/wiring/0.16.6.patch` for the
   shape), then `git diff > portal/wiring/<ver>.patch`.
2. `scripts/sync-upstream.sh <ver> --test` — confirm it applies + builds + the Portal unit tests pass.
3. Add a `{ "ponder": "<ver>", "wiring": "wiring/<ver>.patch", "status": "verified" }` row to
   `versions.json` and add `<ver>` to `compat.tested`.
4. Commit; publish when a version is needed.

CI (`.github/workflows/ci.yml`) runs step 2 for every version in `versions.json` on each push, so a
ponder upgrade that breaks the seam is caught before release.

## Why a fork (not a thin plugin)

ponder's published `exports` only expose `.` and `./virtual`; the internals the Portal sync needs
(`runtime/filter`, `internal/types`, the SyncStore) aren't importable. A loader-hook plugin can reach
them but is fragile and version-coupled in a different way. The fork is the robust, drop-in path today;
the thin plugin remains a possible future once ponder exposes a `HistoricalSync` hook (tracked upstream).
