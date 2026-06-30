# Portal backfill benchmark base — results

_assembled full base (dedicated portal, chunk=range, read-ahead 1)_

```
indexer                            ok   wall(s)    events    ev/s  rssMB  http       MB  chunks
------------------------------------------------------------------------------------------
uniswap-portal (all-sources)       ✓       16.4     13635     834    744     8    104.7       3
uniswap-portal HEAVY (50k blk)     ✓       98.9    179245    1812   1631    12   1102.5       4
feature-call-traces (traces)       ✓        7.1        17       2    630     2      0.6       3
feature-blocks (block-interval)    ✓       85.3      2001      23    630     2     28.4       3
euler-mainnet (factory+lending)    ✓       14.4         0       0    635     -        -       -
v4-ponder (uniswap v4, base)       ✓       12.3         5       0    637     2      1.6       3
```

See BENCHMARKS.md for methodology + findings.
