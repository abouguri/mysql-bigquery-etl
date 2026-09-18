# Publication and recovery

The new protocol uses `etl_state_v1` and `etl_runs_v1`. The old `etl_metadata` is no longer read or mutated. Existing destination tables must exactly match the explicit commerce schema and must have unique keys. Use a new sandbox dataset for first validation; migration of existing real data requires reconciliation and a separate plan. Do not delete production tables to get past a schema error.

1. Find the table and run ID in logs. Query its row in `etl_runs_v1` and inspect BigQuery jobs named `etl_<run_id>_acquire`, `etl_<run_id>_load`, and `etl_<run_id>_publish` in the configured location.
2. A client timeout does not imply rollback. Check the publish job's terminal outcome before launching recovery. A successful publish has a SUCCEEDED audit row and a checkpoint committed in the same transaction.
3. The client resolves known job IDs with at most three attempts. It never gives the same operation a fresh ID after a timeout.
4. On failure, the lease intentionally remains until expiry (30 minutes by default). Do not manually release it while a job outcome is unknown. A stale publisher must fail the generation/ownership check.
5. After the job outcome is resolved and the lease has expired, run the CLI again. New execution reads the committed checkpoint. The current ID cursor still requires reconciliation for changed/deleted source rows; it does not recover arbitrarily late commits.
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
