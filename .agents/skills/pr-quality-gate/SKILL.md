---
name: pr-quality-gate
description: Runs before creating a PR. Verifies that changed Portal-layer code has co-located tests, keeps every portal/*.test.ts green by running them the repo's way (scripts/sync-upstream.sh <ver> --test), generates a Portal-layer coverage diff for the PR body (--coverage + scripts/coverage-diff.mjs), and enforces the lint/code-style gate (biome check .). Tailored to portal-ponder — the Portal fork of ponder.
---

# PR Quality Gate

Enforces quality standards before a PR is created for **portal-ponder**. Apply this checklist
**before** calling `gh pr create` — do not skip steps.

This repo is not a monorepo and has no separate test package: the whole diff against upstream is the
**Portal layer** in `portal/`. Its unit tests (`portal/*.test.ts`) can't run standalone from the repo
root — they import the patched ponder core, so they run **inside a grafted ponder checkout** produced
by `scripts/sync-upstream.sh`. Keep that in mind for Step 2.

## Step 1: Changed Portal code has tests

1. List changed files: `git diff main...HEAD --name-only` (base branch is `main`).
2. For each new or modified **source** file under `portal/` (i.e. `portal/*.ts` excluding
   `*.test.ts`, `config.ts`, and `wiring/*.patch`):
   - A co-located test file must exist: `portal/<name>.ts` → `portal/<name>.test.ts`.
   - New exported functions or non-trivial logic changes MUST be covered by a test.
3. If tests are missing, **write them** (Step 3 conventions) before proceeding, or flag it explicitly
   in the PR body under a `## Missing tests` section.

The current source ↔ test pairs are `portal.ts`, `portal-transform.ts`,
`portal-realtime.ts`, `portal-realtime-wire.ts` — each with a matching `*.test.ts`.

## Step 2: Run the Portal tests (they must be green)

Per `CLAUDE.md`, every `portal/*.test.ts` must pass before pushing. They run **inside the grafted
tree**, not from the repo root:

```bash
# Grafts the Portal layer onto a pinned ponder checkout and runs the Portal-layer vitest suite
# (config: portal/vite.portal.config.ts). Use a ponder version declared in versions.json.
scripts/sync-upstream.sh 0.16.6 --test
```

This clones `ponder@<ver>`, copies `portal/*.ts` + `portal/*.test.ts` + `portal/__fixtures__/` into
the core package, applies the wiring patch, builds, then runs
`pnpm exec vitest run --config vite.portal.config.ts`. Requirements: `pnpm` (via corepack) and network
access to clone ponder. It writes to `${SYNC_WORKDIR:-/tmp/sqd-ponder-fork}/<ver>`, leaving the repo
clean.

If a seam-sensitive change could affect other supported versions, run it for each version in
`versions.json` (`compat.tested`), e.g. also `scripts/sync-upstream.sh 0.15.17 --test`. CI
(`.github/workflows/ci.yml`) does this on every push, but run it locally when the patch or Portal
modules changed.

## Step 3: Test conventions (match the existing suite)

There is **no shared mock framework** — tests build their own HTTP/`fetchImpl` mocks and load JSON
fixtures. Match the existing style rather than inventing utilities:

- **vitest, flat style:** `import { test, expect } from "vitest"` — the suite uses top-level
  `test(...)`, not `describe`/`it`. Add `beforeEach`/`afterEach` only when a test owns a resource
  (e.g. a real `http.createServer` Portal mock) that needs cleanup — tear it down in `afterEach`.
- **Mock the Portal per test:** stand up a local `http` server or pass a `fetchImpl` stub that replays
  canned batches (see `mockPortal(...)` in `portal-realtime-wire.test.ts`). Model realistic Portal
  responses — `204`/`409`/`503`, finalized-head advances, chunk boundaries.
- **Fixtures** live in `portal/__fixtures__/*.json` (`receipts.json`, `traces.json`),
  loaded with `readFileSync(join(__dirname, "__fixtures__"), ...)`.
- **Matchers:** `toBe` / `toEqual` for values, `toMatch` for hex-shape checks, `toMatchInlineSnapshot`
  for large structured output. Assert against the **RPC shape** the Portal transform must reproduce
  (hex normalization, accessList shape, receipt/status fields) — that byte-for-byte parity vs RPC is
  the point of the suite.
- Tests live next to source: `feature.ts` → `feature.test.ts`.

## Step 4: Lint and code style

Run the repo lint gate and fix violations before the PR:

```bash
npm run lint        # biome check .
npm run lint:fix    # biome check --write . (applies safe fixes)
```

Also honor the `CLAUDE.md` conventions Biome can't express: braces on multi-line control-flow bodies
(braceless only for a one-line `if (!x) return;`/`continue;`/`break;`), one variable per declaration,
a blank line after a guard clause and before a `return`, and no assignment-as-expression.

## Step 5: Coverage guard

Coverage is scoped to the **Portal layer** (the whole diff vs upstream ponder) via the `coverage`
block in `portal/vite.portal.config.ts` — inert on a plain `--test` run, activated by `--coverage`.
Generate a base-vs-head diff and put it in the PR body; new or changed Portal source must be covered,
and overall coverage must not silently drop.

### Generate head coverage

`--coverage` does everything `--test` does, plus installs the matching v8 provider and writes
`<core>/portal-coverage/coverage-summary.json`. Run this **instead of** Step 2's `--test` to cover
both at once (it's one fresh graft — clone + build):

```bash
SYNC_WORKDIR=/tmp/cov-head scripts/sync-upstream.sh <ver> --coverage
HEAD_SUM=/tmp/cov-head/<ver>/packages/core/portal-coverage/coverage-summary.json
```

### Generate base coverage and render the diff

Measure the base branch with the **same tooling** (head's config + scripts), swapping in only the
Portal code under test, so the comparison is apples-to-apples:

```bash
git stash --include-untracked                    # park the working tree
git checkout main -- portal/portal*.ts portal/realtime*.ts   # base Portal source + its co-located tests
SYNC_WORKDIR=/tmp/cov-base scripts/sync-upstream.sh <ver> --coverage
BASE_SUM=/tmp/cov-base/<ver>/packages/core/portal-coverage/coverage-summary.json
git checkout HEAD -- portal/                      # restore head's Portal source
git stash pop

node scripts/coverage-diff.mjs "$BASE_SUM" "$HEAD_SUM"   # markdown table → paste into PR body
```

The globs match the co-located `*.test.ts` files too (e.g. `portal.test.ts` matches `portal*.ts`), so
this swaps in base *source + tests* as a self-consistent base measurement — that's intended. **Caveat:**
if this PR *adds* a Portal source file that doesn't exist on `main`, `git checkout main -- <that-glob>`
errors on the new path and leaves the tree half-reverted with the stash still pending. When the diff
adds a new `portal/*.ts`, skip the base pass and render head absolutes instead (single-arg, below).

`coverage-diff.mjs` prints the total plus only the files whose coverage changed, and appends
`⚠️ Overall statement coverage decreased.` when it drops. If the base branch predates the coverage
tooling (e.g. before this lands on `main`), skip the base pass and render head absolutes with a single
argument: `node scripts/coverage-diff.mjs "$HEAD_SUM"`.

### The guard

- New/changed Portal source with no coverage → **write tests** (Step 3) or justify the gap in the PR.
- Overall coverage dropped (the ⚠️ line) → add tests or explain why the drop is expected (e.g. dead
  code removed changing the denominator). Don't merge an unexplained decrease.

## Step 6: PR description format

Every PR created by the agent follows this template:

```markdown
## Summary
<1-3 bullets describing what changed in the Portal layer>

## Coverage (Portal layer)
<the table from `scripts/coverage-diff.mjs` — Step 5>

## Test plan
- [ ] `scripts/sync-upstream.sh <ver> --coverage` green (list the version(s) run)
- [ ] `npm run lint` clean
- <what new/changed behavior is covered, and how>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```
