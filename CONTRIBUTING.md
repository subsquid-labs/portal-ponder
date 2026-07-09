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

## Code Style

Follow [CLAUDE.md](CLAUDE.md) for repository style. In particular: braces on multi-line control
bodies, one variable per declaration, blank lines after guards and before returns, and no assignment
used as an expression.
