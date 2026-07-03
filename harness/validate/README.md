# Validation-campaign harness

Tooling for the portal-ponder production validation campaign (canonical plan:
`scratchpad/campaign-plan.md`). It runs the **cell matrix** (fork-vs-stock byte diffs across
strata), the **chaos/resume** harness, and the **Soak A/B** differ. This directory builds the
tooling; the paid matrix runs come later. Everything runs with **bash + node only** (no pnpm, no
extra npm deps) so it works on the box.

```
harness/validate/     cell runner: cells.json matrix, run-cell.sh, ctrl-cell.sh, diff-batched.mjs, rpc-meter.mjs, budget guard
harness/chaos/        fault proxy (proxy.mjs), kill-loop.sh + verify-resume.sh (resume acceptance)
harness/soak-ab/      ab-diff.mjs (finalized-overlap differ), soak-b.service + deploy-soak-b.sh
```

## Evidence-layer map (→ campaign-plan.md)

| Plan layer | What proves it | Here |
|---|---|---|
| **2** — one full-range byte diff (Euler eth [deploy→head]) | constant-memory streaming diff + app-table hash | cell `F-full` → `run-cell.sh` with `diff-batched.mjs` |
| **3** — stratified byte windows (chunk-grid, deploy-floor, format eras, empty, frontier, seeded-random; auto-shrink) | `run-cell.sh` per window via `harness/diff/run.sh` | cells `L-*`, `F-*`, `T-*`, `A-*`, `U-eth`, `E-eth`; windows expanded by `windows.mjs` |
| **4** — CTRL: genuine upstream vs fork-portal-unset | `ctrl-cell.sh` (npm `ponder@X` vs `file:` tarball, both RPC) | cell `CTRL` |
| chaos/resume — byte-identity + intervals tile after ≥200 kills | `kill-loop.sh` + `verify-resume.sh` + fault `proxy.mjs` | `harness/chaos/` |
| Soak B + A/B — finalized-overlap identity + expected tx class | `deploy-soak-b.sh` + `ab-diff.mjs` | `harness/soak-ab/` |

`R` (raw breadth, all 6 slugs × 30 spots) stays on `harness/compare/differential.ts` — `run-cell.sh`
skips `runner: differential` cells with a pointer. Layer 1 (count parity) and layer 5 (third-party)
are separate tools outside this harness.

## Cell runner

```bash
# a paid cell (pins the ONE campaign tarball; routes the paid RPC through the meter)
SQD_PONDER_TARBALL=/path/subsquid-ponder-0.16.6-sqd.2.tgz \
SQD_RPC_KEY=<rpc.subsquid.io key> \
  bash harness/validate/run-cell.sh L-eth

# the CTRL cell (genuine upstream vs fork-portal-unset)
SQD_PONDER_TARBALL=/path/tgz UPSTREAM_PONDER_VERSION=0.16.6 SQD_RPC_KEY=<key> \
  bash harness/validate/ctrl-cell.sh CTRL

# plumbing smoke — NO paid endpoints (public Portal + a free RPC)
SQD_PONDER_TARBALL=/path/tgz RPC_URL_OVERRIDE=https://ethereum-rpc.publicnode.com \
  bash harness/validate/run-cell.sh SMOKE
```

For each window `run-cell.sh`:

1. **Budget guard** — sums `requests` over every `results/*.json` and refuses to start a new window
   once the cumulative meets `budget.json` (`maxRequests`, default 4,000,000 calls ≈ $20).
2. **Meters requests** — `rpc-meter.mjs` sits in front of the paid RPC (`PONDER_RPC_URL_1` points at
   it) and tallies JSON-RPC *calls* (a batch counts as N). The meter is reset before each window, so
   `/__count` after the window is that window's exact request cost.
3. **Runs `harness/diff/run.sh`** with the campaign env pinned: `PORTAL_CHUNK_FIXED=1`
   `PORTAL_CHUNK_BLOCKS=500000` (`PORTAL_CHUNK_PINNED=1` tells run.sh to honour it verbatim),
   `PORTAL_CHECKS=strict`, `DIFF_APP` for the cell's app, `CHAIN_ID`/`EULER_FACTORY`/`ERC20_ADDRESS`
   per chain. `F-full` additionally sets `DIFF_SCRIPT=diff-batched.mjs` + `DIFF_ARGS=--app-hash`.
4. **Auto-shrink** — if a window matched > `autoShrink.threshold` rows (default 50k) it is halved and
   re-run once (`tag+shrunk`).
5. **Records** `{window, pass, requests, durationSec, matchedLogs, autoShrunk, diffTail}` to
   `results/<cellId>.json`.

### Windows (`windows.mjs`, unit-tested)

Each cell's `windows` are literal `{from,to}` or a strategy expanded deterministically:

- `seeded-random` — `count` windows of `size` blocks from a fixed `seed` (reproducible).
- `chunk-grid` — windows straddling every `chunk` (500k) edge by ±`delta` (cross-chunk-cache stress).
- `deploy-floor` — a window straddling the contract deploy block (+ the empty pre-deploy prefix).
- `format-era` — eth receipt/tx format boundaries (pre-Byzantium receipts, Merge, blob type-3, type-4/Prague).
- `frontier` — the live Portal head (`GET <portal>/finalized-head`), resolved by `run-cell.sh`.
- `full-range` — `[from → pinned Portal head]` (the `F-full` cell).

### Request-meter design

`rpc-meter.mjs` is a ~150-line `node:http` reverse proxy — **no dependencies**. It forwards each
request body verbatim to `METER_TARGET` (Node global `fetch` transparently decodes upstream gzip) and
counts JSON-RPC calls, per method. Counting the *stock-RPC* side is exactly the paid cost the campaign
budgets against (the fork's Portal side makes only setup/`readContract` RPC calls, which the meter
also captures). Control plane: `GET /__count`, `POST /__reset`. It never rewrites payloads, so the
byte diff is unaffected. Per-window counts land in `results/*.json`; the budget guard sums them.

## Chaos / resume

```bash
# repeatedly kill+resume a bounded Portal backfill into a persistent store until it completes
SQD_PONDER_TARBALL=/path/tgz CHAOS_FROM=20529207 CHAOS_TO=20579207 TRIGGER=poisson-45s \
CHAOS_DB=/tmp/chaos-store bash harness/chaos/kill-loop.sh

# then: byte-diff the chaos store vs a clean baseline + assert intervals tile the window
SQD_PONDER_TARBALL=/path/tgz bash harness/chaos/verify-resume.sh /tmp/chaos-store 20529207 20579207

# fault proxy in front of the Portal (429/5xx/reset/stall/gzip/ndjson/204 + head freeze/regress/flap)
PORTAL_UPSTREAM=https://portal.sqd.dev/datasets/ethereum-mainnet CHAOS_PORT=8700 \
CHAOS_SCENARIO=scenario.json node harness/chaos/proxy.mjs
# point the backfill's PORTAL_URL_1 at http://127.0.0.1:8700; hot-swap scenarios via POST /__scenario
```

Kill triggers (env `TRIGGER`): `poisson-45s` (default; robust, no log dependency),
`on-chunk-fetch-log`, `on-discovery-log`, `between-rangedata-blockdata`, `on-child-flush-log`. The
log triggers match `TRIGGER_REGEX` against the run log with `PONDER_LOG_LEVEL=trace` +
`PORTAL_GATE_LOG=1`; **the fork emits few distinct per-event lines today**, so the defaults target
the `[portalGate]` gate ticker / trace output — set `TRIGGER_REGEX` to your build's actual event line
for a precise kill point. `poisson-45s` and `on-chunk-fetch-log` (gate log) work out of the box and
satisfy the ≥200-kills acceptance.

`verify-resume.sh` runs `diff-batched.mjs` (chaos store vs clean baseline) **and**
`check-intervals.mjs` (every `ponder_sync.intervals` fragment must have coalesced into a single range
covering `[from,to]` — a Postgres multirange with >1 range means a gap).

## Soak B + A/B

```bash
# provision the Soak B unit (DB euler_rt_b, PORTAL_REALTIME=stream, PORTAL_CHECKS=on) — DOES NOT START it
bash harness/soak-ab/deploy-soak-b.sh /path/subsquid-ponder-0.16.6-sqd.2.tgz
sudo systemctl start soak-b.service     # operator starts deliberately

# hourly finalized-overlap diff of Soak A (RPC realtime) vs Soak B (Portal realtime)
DATABASE_URL_A=postgresql:///euler_rt DATABASE_URL_B=postgresql:///euler_rt_b \
CHAINS=1,8453,42161 CUTOVER=<block> STATUS_FILE=soak-ab-status.json \
  node harness/soak-ab/ab-diff.mjs
```

`ab-diff.mjs` uses two `psql` processes (no npm driver) and streams each side ordered, constant
memory. It asserts: **logs** strict row-set + field identity (PRIMARY, must be 0); **blocks** field
identity (total_difficulty excluded); **transactions** are EXACTLY the expected class — B may be
missing parent txs for realtime-ingested spans, each referenced by an A-side log; anything else FAILS;
per-1000-block checkpoint hashes match; `_ponder_checkpoint` is monotonic. It writes
`soak-ab-status.json` (`verdict`, `restartCount`, `lastRestartAt`, `restartsLastHour`, `alerts`,
`diffClasses`, `lagA`/`lagB`, `counters`) for the hourly monitor.

**Restart signal** — in `PORTAL_REALTIME=stream` mode, unknown-head / reorg-gap are FATAL by design
and exit **75 (EX_TEMPFAIL)**; `Restart=on-failure` makes that a designed restart-recovery, not a
soak failure. The unit's `ExecStartPre` appends a UTC timestamp to `$SOAK_B_RESTART_LOG` on every
(re)start; `ab-diff.mjs` reads it into `restartCount`/`lastRestartAt` and raises a **crash-loop**
alert when restarts exceed 3/hour. Point `ab-diff.mjs` at the same log via `RESTART_LOG=…`.

**Guardrails** (`deploy-soak-b.sh`): DB is `euler_rt_b` and nothing else; port is never `:9547`; the
`euler` prod DB is never touched; secrets are copied into a `chmod 600` env file, never printed/committed.

## Budget discipline

- `budget.json` `maxRequests` is the cumulative JSON-RPC-call ceiling (default 4M ≈ $20; CTO ceiling
  $25 / 5M). `run-cell.sh` refuses to start a new window once cumulative (`node budget-sum.mjs`) meets
  it — a run can overrun by at most the in-flight window.
- Delete run data after diffing (disk footprint < 20 GB). `results/*.json` are git-ignored.
- Every window's request count is metered and persisted — never estimate spend after the fact.

## Tests

Pure cores are unit-tested with `node --test` (wired into CI's `repo-tests` job):

```bash
node --test harness/validate/*.test.mjs harness/chaos/*.test.mjs harness/soak-ab/*.test.mjs
```

Covered: window generation (seeded determinism, chunk-grid, deploy-floor, format-era, auto-shrink,
full-range/frontier markers); `diff-batched` merge + tolerances + `hashRows` on fixture rows;
intervals tiling verdict; `ab-diff` expected-tx-class / checkpoint-monotonicity / bucket-hash logic.
