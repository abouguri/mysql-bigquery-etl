# Local benchmark results

Recorded 2026-09-18. 5 fresh-process samples per configuration; resource limits are recorded per sample in the CSV.

| Rows | Mode | Median processing seconds | Range (s) | Median peak RSS (MiB) | RSS range (MiB) |
|---:|---|---:|---:|---:|---:|
| 100,000 | bounded | 0.800 | 0.798–0.808 | 151.67 | 150.41–151.98 |
| 100,000 | full_frame | 0.699 | 0.679–0.706 | 211.47 | 210.96–213.37 |
| 1,000,000 | bounded | 8.625 | 8.423–9.380 | 150.32 | 148.07–152.18 |
| 1,000,000 | full_frame | 8.299 | 7.700–9.457 | 772.95 | 770.76–776.93 |

At 1,000,000 rows, bounded processing used **80.6% less median peak process RSS**, with **3.9% greater median processing time**, than a full DataFrame using the same transformations/contracts.

These are local synthetic processing measurements, not end-to-end ETL throughput, original-code speedup, production capacity, BigQuery cost or a freshness SLO. Timing includes fixture generation and validation; peak RSS includes imports. Shared-host contention and sequential experiment ordering limit comparisons. See [methodology](../README.md) and [raw samples](local.csv).

The base commit predates this benchmark addition; recorded source hashes identify the exact benchmark and contract code. Every sample passed row-count and exact-revenue assertions. No p95 claim is derived from five samples.
