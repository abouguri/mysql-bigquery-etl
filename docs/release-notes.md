# v0.1.0-preview.1

A locally validated engineering preview. This is not a production-qualified release.

Includes bounded MySQL snapshot extraction, timestamp lookback, full reconciliation, bounded replay, explicit schema/decimal contracts, staged BigQuery publication with writer fencing, stable job recovery, CLI containers, Terraform/Scheduler/monitoring definitions, commerce SQL and reproducible local benchmarks.

Validation: 55 local tests pass with the disposable MySQL fixture; five BigQuery tests require an approved cloud sandbox. Terraform validation, static checks and the CLI image smoke test pass. Raw benchmark measurements and an honest case study are included.

Migration: state uses `etl_state_v2`/`etl_runs_v2`; existing targets must match the declared schema and have unique keys. Legacy state is not migrated automatically. The old HTTP entry point now invokes the CLI. Use a fresh sandbox for first validation; do not replace or delete real data as a migration shortcut.

Remaining gates: actual cloud replay/concurrency/quality tests; authenticated deployment and TLS connectivity; IAM-denial and alert-delivery tests; end-to-end cost/performance; the observation period for any freshness claim; and an independently reproduced demo. Schedules default to paused.
