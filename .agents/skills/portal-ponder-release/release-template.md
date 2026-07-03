# `@subsquid/ponder` release notes template

`release.yml` publishes to npm only, so writing these notes is a **required** step of every release
(SKILL.md step 5). **Scope: the Portal layer**, i.e. what `@subsquid/ponder@<version>-sqd.<rev>` adds
or changes vs plain `ponder@<version>`. Do **not** restate upstream ponder's own changelog. Source
the highlights from commits/PRs since the previous `-sqd` cut; keep it to user-visible changes.

---

**`@subsquid/ponder@<version>-sqd.<rev>`** — `ponder@<version>` + the SQD Portal backfill layer,
drop-in (bin stays `ponder`). Install: `npm i @subsquid/ponder`, pin exact:
`npm i @subsquid/ponder@<version>-sqd.<rev>`.

### Highlights
- <the headline Portal-side change, e.g. a correctness fix in the backfill seam>

### Portal layer
- <backfill / transform / realtime changes: `portal.ts`, `portal-transform.ts`, realtime wiring>
- <wiring: new/updated `portal/wiring/<version>.patch` touch-points>

### Fixes
- <correctness fixes vs RPC — reference the harness/diff evidence where relevant>

### Compatibility
- Built + tested against `ponder@<version>`. Seam verified against: <compat.tested list>.

---

## Prerelease / re-cut (`-sqd.<rev ≥ 2>`) notes

Leaner: drop the lead paragraph, list only what changed in this revision, and add a one-line
"what to test". Keep the install line minimal — `npm i @subsquid/ponder@<version>-sqd.<rev>`. Skip
dist-tag prose (`published to latest` etc.) — it's obvious and just noise.
