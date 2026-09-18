# Commerce analytics contract

The demo models one product per order, with `order_id` as the unique order key. It does not model a multi-line shopping cart. All prices are USD, dates are UTC, and tax, shipping, discounts and foreign-exchange conversion are outside this demo's contract.

Revenue equals exact decimal `quantity * unit_price`. Negative totals represent refunds; zero-value orders count as orders. Gross revenue includes nonnegative totals; refunds are reported as a positive magnitude; net revenue includes both. Average order value divides nonnegative revenue by the count of nonnegative orders, returning NULL if that count is zero. The synthetic source fixture reconciles to **309.85 USD** across three orders.

`sql/revenue_daily.sql` exposes daily revenue by the **current** product category. Categories are Type 1 dimensions: a category change can reclassify historical orders. This is a deliberate simplification, not a historical-category guarantee. Hard-deleting a referenced product/user makes quality checks fail; resolve business retention/soft-delete policy before enabling such deletes on real commerce data.

`sql/quality_checks.sql` checks uniqueness, required order fields, foreign-key relationships, exact totals and mart/base reconciliation. The ingestion protocol is atomic per table; run the cross-table checks only after the pipeline completes. A concurrent source may require repeated reconciliation or coordinated snapshots to establish a cross-table point-in-time comparison.

Render SQL without credentials or cost:

```sh
python scripts/warehouse_checks.py --project your-sandbox-project --dataset mysql_etl
```

After the cloud gate is authorized and ingestion succeeds, add `--execute`. The runner prints job IDs and processed/billed bytes and uses a maximum-bytes-billed limit for queries; this is not a total project spend cap. Source/warehouse count and revenue comparisons must use matching snapshot boundaries. The small fixture's expected totals are not a substitute for reconciling a live changing source.

## Cloud cost experiment (not yet run)

Use the same seeded data, region and representative date/user/product queries. Capture uncached query runs, scanned bytes, slot time and billable usage before and after partition/clustering changes. Include ingestion compute, merge/query, storage, logs and network costs. Publish both the overall bill and per-million-input-row cost, identifying estimated versus billed figures. Do not claim a cost reduction until measured on equivalent workloads.
