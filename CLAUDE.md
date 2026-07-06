# portal-ponder — contributor & agent rules

Fork of Ponder's historical sync that backfills from SQD Portal. The Portal layer lives in
`portal/`; `scripts/sync-upstream.sh` grafts it onto a pinned ponder checkout to build/test.

## Code style

These are enforced expectations for all TypeScript in this repo. Some are checked by Biome
(`biome check .`); the rest are conventions Biome can't express — follow them anyway.

1. **Braces on control-flow bodies.** An `if`/`for`/`while` whose body is a real statement
   uses `{ }` — never a braceless multi-line body.
   - **Exception:** a single-statement guard on one line stays braceless:
     `if (!x) continue;` / `if (!x) break;` / `if (!x) return;`.

2. **One variable per declaration.** Never `const a = 1, b = 2;` — one `const`/`let` each.
   *(Biome-enforced at `error`: `style/useSingleVarDeclarator`.)*

3. **Let guards and returns breathe.** Blank line *after* a guard clause
   (`if (...) continue;` / `break;`) when code follows, and a blank line *before* a `return`.

4. **No assignment used as an expression.** No `(m ??= new Map()).set(...)` or
   `while ((n = idx()) >= 0)` — expand into explicit statements.
   *(Biome-enforced: `suspicious/noAssignInExpressions`.)*

Match the surrounding code otherwise. `npm run lint` runs `biome check .`; `npm run lint:fix`
applies the safe fixes.

## Tests

The Portal unit tests run inside the grafted ponder tree, not from this repo root:
`scripts/sync-upstream.sh <ponder-version> --test` (config: `portal/vite.portal.config.ts`,
files `portal/*.test.ts`). Keep every `portal/*.test.ts` green before pushing.
