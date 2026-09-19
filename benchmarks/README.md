# Local processing experiment

This experiment compares **the same current transformations and contracts** with bounded 10,000-row pages versus one full DataFrame. It is not a benchmark of the old pipeline versus the new one, and does not measure MySQL extraction, BigQuery load/merge, network or cloud billing.

Input is deterministic: sequential order IDs; user/product IDs derived by modulo; quantities cycle 2, 3, 4, 1; unit price is Decimal 19.95; UTC timestamps are fixed at 2026-01-01. There are seven source columns and two derived columns. All results are checked for exact decimal revenue and row count. There is no random seed because no randomness is used.

Each sample runs in a fresh Python subprocess. Processing time includes generation, transformation, validation and revenue reconciliation, excluding interpreter/dependency import time. Peak process RSS includes dependency imports. Limits in the recorded run are two CPUs and 2 GiB container memory; the host's visible CPU count is recorded separately and does not represent the container CPU quota. Five samples per mode and row count report variability, not a statistically credible p95.

Reproduce from repository root:

```sh
docker build --target test -t mysql-bigquery-etl-tests .
docker run --rm --cpus=2 --memory=2g \
  --user "$(id -u):$(id -g)" -v "$PWD:/app" \
  -e BENCH_CPU_LIMIT=2 -e BENCH_MEMORY_LIMIT=2GiB \
  -e BENCH_COMMIT="$(git rev-parse HEAD)" \
  mysql-bigquery-etl-tests python -m benchmarks.run
```

The CSV records base commit and SHA-256 hashes of the benchmark and contract files, so uncommitted benchmark additions are not presented as part of an older commit. Report software hashes, resource limits and the run's date with any quoted measurement. Runtime varies by machine and contention. The current experiment is sequential (bounded then full-frame); randomized ordering and more runs would improve the comparison.

Cloud evidence remains separate: run the approved sandbox workflow, capture load/merge/query job IDs and costs, and compare identical query workloads with partitioning/clustering. Do not extrapolate these local measurements into a cloud throughput, end-to-end memory or cost claim.

Summarize with `python -m benchmarks.report`. To generate the standalone SVG,
install `benchmarks/plot-requirements.txt` in a separate environment and run
`python -m benchmarks.report --plot`. Plotting dependencies are never imported
inside the timed worker. Bars show medians with min/max error bars, not confidence
intervals. The chart is derived from the recorded CSV rather than hand-entered values.

## Real MySQL experiment

`make benchmark-mysql` builds a dedicated test-container profile, seeds separate synthetic `benchmark_orders_*` tables in the disposable `commerce_fixture` database and compares 10,000-row pages with full-size pages through the actual `Source.snapshot` reader. Existing benchmark tables cause a failure rather than being overwritten. Successfully seeded tables are dropped after their samples; interrupted experiments can be reset with `make clean-fixtures`. Normal fixture users/products/orders are not modified.

The deterministic seven-column order data matches the processing-only experiment. Three fresh-process samples per mode at 100k and 1m rows validate counts and exact revenue. Samples alternate mode order. A full-size page still uses the current source reader and contracts; it is not the original pipeline implementation. Timing includes MySQL reads and DataFrame creation but excludes fixture generation, imports and destination writes. Peak RSS includes imports and excludes the separate MySQL server. Worker limits are two CPUs and 2 GiB; MySQL is uncapped, uses tmpfs, and caches are not flushed. This is a local warmed-source comparison, not a remote-network or disk-throughput claim.

Run `python3 -m benchmarks.mysql_report` to regenerate the [measured table](results/mysql-summary.md) from [raw CSV](results/mysql.csv). No cloud account is used. The Compose runtime network is internal. The existing processing-only results remain a separate experiment with a different scope.
