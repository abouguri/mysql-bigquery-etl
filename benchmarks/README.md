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
