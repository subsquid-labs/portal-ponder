# Contributing

Examples are first-class regression coverage for the Portal fork. Keep them runnable from a fresh
clone, with no secrets required for the default bounded windows.

## Example e2e Gate

Run one example locally:

```sh
node scripts/examples-e2e.mjs euler-subgraph
```

Run every example:

```sh
node scripts/examples-e2e.mjs --all
```

The gate installs the example in a temporary clean copy, starts `npm run dev`, waits for Ponder's
ready endpoint, proves Portal usage from the per-chain `PORTAL_METRICS_FILE.<chainId>` metrics, runs
the manifest GraphQL assertions, and requires a clean `SIGINT` exit.

Use `E2E_RPC_URL_<chainId>` to provide private RPCs without changing example code:

```sh
E2E_RPC_URL_1=<archive-rpc> node scripts/examples-e2e.mjs euler-subgraph
```

The harness never prints RPC URLs. If the env var is unset, the example's public default RPC remains
in use.

## Definition of done

A user-facing fix is not done when it merges to `main`. The examples run the **published**
`@subsquid/ponder` package pinned in each `package.json`/`e2e.json`, not the repo working tree, so a
fix on `main` is neither reproducible by a user nor visible in example behavior until a version
carrying it is published to npm and the example pins have advanced to that version through the
release flow (see below). Merged to `main` is not user-safe; a version published and pinned, green
under the examples e2e gate, is.

## Pins

Each example pins an exact `@subsquid/ponder` version in both `package.json` and `e2e.json`.
Examples pin the newest published Portal line, and the pins move only through the release flow after
the just-published version passes the examples e2e gate.

Scheduled CI runs the gate with `--check-pins`, which compares the committed pins with
`npm view @subsquid/ponder dist-tags.latest`.

## Example READMEs

Numbers in example READMEs must come from a real fresh run of the default window. When updating a
README count, run the matching e2e gate and keep the fenced GraphQL query byte-for-byte equal to a
query in `e2e.json`. Check that with:

```sh
node scripts/examples-e2e.mjs euler-subgraph --verify-docs
```

## Review checklist

The e2e gate and `biome check` cover the mechanical checks. One behavior CI does not assert needs a
human pass on any PR that touches the Portal progress path (`portal/portal-metrics.ts`) or an
example:

- **Cold-start progress still advances.** On a fresh, dense source the first discovery scan can run
  across many blocks before the first commit lands. The per-chain progress ticker must keep moving
  through that cold window — `scanned=` climbs even while `blocks_streamed=0` — so a healthy run is
  never mistaken for a hang. `progressFingerprint` keeps this honest by including
  `discoveryScannedBlocks` (locked by a regression test in `portal-metrics.test.ts`). Confirm any
  change to the fingerprint preserves that field, and on an example PR eyeball the ticker output on
  an empty opening window to see `scanned=` advance.

## Code Style

Follow [CLAUDE.md](CLAUDE.md) for repository style. In particular: braces on multi-line control
bodies, one variable per declaration, blank lines after guards and before returns, and no assignment
used as an expression.
