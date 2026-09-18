# Publication and recovery

The new protocol uses `etl_state_v2` and `etl_runs_v2`. The old `etl_metadata` is no longer read or mutated. Existing destination tables must exactly match the explicit commerce schema and must have unique keys. Use a new sandbox dataset for first validation; migration of existing real data requires reconciliation and a separate plan. Do not delete production tables to get past a schema error.

1. Find the table and run ID in logs. Query its row in `etl_runs_v2` and inspect BigQuery jobs named `etl_<run_id>_acquire`, `etl_<run_id>_load_<page>`, and `etl_<run_id>_publish` in the configured location.
2. A client timeout does not imply rollback. Check the publish job's terminal outcome before launching recovery. A successful publish has a SUCCEEDED audit row and a checkpoint committed in the same transaction.
3. The client resolves known job IDs with at most three attempts. It never gives the same operation a fresh ID after a timeout.
4. On failure, the lease intentionally remains until expiry (30 minutes by default). Do not manually release it while a job outcome is unknown. A stale publisher must fail the generation/ownership check.
5. After the job outcome is resolved and the lease has expired, run the CLI again. New execution reads the committed checkpoint. The timestamp lookback catches supported changes; use full reconciliation for hard deletes and delayed commits outside the lookback.
6. Failed run records may remain RUNNING; interpret them alongside job state and lease expiry, not as proof a worker is alive. Staging tables expire after one day. Fix validation errors at source; the current quality policy rejects the whole batch.

A full products snapshot intentionally clears the destination when the valid source is empty. An invalid or failed extraction must never be represented as an empty snapshot. All batch keys must be unique and all required fields valid before publication.

The ownership protocol mutates its BigQuery state row inside the same transaction as publication. It relies on BigQuery transaction conflict behavior, not an external lock. Local tests validate request construction and client failure behavior; real transaction, replay and stale-writer tests are in `tests/test_bigquery.py` and are not equivalent to mock tests.

## Explicit cloud test gate

Only run after selecting a sandbox project, authenticating and agreeing a spend cap:

```sh
RUN_BIGQUERY_TESTS=1 BQ_TEST_PROJECT=your-sandbox-project \
  python -m pytest -m cloud -v
```

Each test creates a uniquely named `etl_test_*` dataset and deletes only that dataset in cleanup. These tests incur cloud usage. Project choice, billing approval and cloud results are not inferred from `.env.example`. If the test process is killed, inspect and remove its disposable dataset after resolving jobs.

Reference: [BigQuery transactions and conflict behavior](https://docs.cloud.google.com/bigquery/docs/transactions).

## Normal runs, reconciliation and replay

```sh
python main.py
python main.py --reconcile
python main.py --table orders --replay-from 2026-01-01T00:00:00Z --replay-until 2026-01-02T00:00:00Z
```

Normal mutable-table runs scan an inclusive `updated_at` window with a 24-hour lookback. Primary-key keyset pages cover that fixed window inside one REPEATABLE READ snapshot. This handles equal update timestamps without depending on ID commit order. Successful publication advances the time watermark to the source database's captured UTC time. Per-page failures replay the whole window on the next execution.

Requirements: positive integer primary keys, source-maintained UTC update timestamps, and InnoDB snapshot semantics. Source and target consistency is per table, not a single snapshot across all tables. Arbitrarily delayed commits can still fall outside the window. Run `--reconcile` at least daily when deletes or late commits matter; it replaces selected targets from complete snapshots. Consumers must accept the chosen interval before deleted rows disappear. No near-real-time or exact change-history guarantee is made.

Replay selects **current source rows** whose update timestamps fall within the explicit range. It is not historical time travel, does not restore hard-deleted records, and leaves the live watermark unchanged. Replays use upserts, even for products, to avoid replacing a table with a partial range. Older source versions cannot overwrite newer target timestamps. Use full reconciliation to repair deletions.

Extraction holds a MySQL snapshot open while loading pages. This bounds application memory but can increase MySQL undo retention and source load. Measure that impact before scaling. The 30-minute lease is a hard safety boundary; no heartbeat is implemented. Increase workload size only after measuring duration, or redesign renewal with the same fencing guarantees.
