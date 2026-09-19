# Real MySQL extraction and processing

Generated from [raw samples](mysql.csv). Each sample uses a fresh Python process. Elapsed time includes a consistent source snapshot, SQL queries, MySQL transfer, DataFrame construction, transformations, validation and exact revenue reconciliation. It excludes fixture seeding, dependency imports and destination writes.

| Rows | Page size | Samples | Median seconds (min–max) | Median peak worker RSS MiB (min–max) |
|---|---|---|---|---|
| 100,000 | 10,000 | 3 | 2.590 (2.562–2.599) | 174.54 (174.32–174.71) |
| 100,000 | 100,000 | 3 | 2.385 (2.317–2.404) | 241.30 (241.12–241.89) |
| 1,000,000 | 10,000 | 3 | 26.229 (26.215–26.363) | 177.70 (169.08–178.11) |
| 1,000,000 | 1,000,000 | 3 | 22.939 (22.932–23.368) | 872.41 (869.65–874.21) |

At 1,000,000 rows, bounded extraction used **79.6% less median peak worker RSS**.

Worker limit: 2 CPUs / 2 GiB. MySQL runs separately in the same Docker network, uses tmpfs, and has no explicit CPU/memory cap. Worker RSS excludes MySQL and does not represent total system memory. Data is synthetic and recently seeded; database/OS caches are not reset. Mode order alternates across repetitions. These samples are not a p95 estimate or a production capacity claim. No BigQuery calls or warehouse publication were timed.

Reproduce with `make benchmark-mysql`, then `python3 -m benchmarks.mysql_report`. The CSV records the base commit, source hashes, versions, measurement timestamps, row counts and reconciled revenue.
