# Five-minute presentation

This is a reproducible demo guide, not a claim that a video or live cloud demo has already been recorded.

| Time | Show | Explain |
|---|---|---|
| 0:00–0:45 | README architecture and business model | MySQL commerce data feeds BigQuery revenue reporting; correctness under failure drives the design |
| 0:45–1:30 | `make integration` and fixture revenue assertion | Known source counts and exact revenue make validation reproducible without cloud credentials |
| 1:30–2:30 | `tests/test_regressions.py`, `tests/test_publication.py`, recovery runbook | Uncertain acknowledgements reuse job IDs; partial staging does not publish; fencing and checkpoint mutation share the transaction |
| 2:30–3:30 | `tests/test_source.py` and CLI help | Snapshot pages remain consistent during source updates; reconciliation repairs the lookback's limits; replay is current-state repair, not time travel |
| 3:30–4:30 | Benchmark chart and raw samples | Bounded pages trade a little processing time for much lower memory in a controlled local experiment |
| 4:30–5:00 | Backlog cloud gates | Separate demonstrated behavior from proposed distributed guarantees and explain exactly what remains to validate |

For a real cloud demo, first complete the deployment/runbook gates. Then show a scheduled run, edit source rows, inject a source failure, inspect alerts/job IDs, resolve any uncertain outcome, wait for lease expiry, rerun and reconcile revenue. Do not replace this with fake successful logs or label mock tests as cloud recovery evidence.

## Interview follow-ups

**Why batch?** The current workload is scheduled analytics. Batch avoids introducing a CDC platform before freshness/capture requirements justify it. Polling has explicit late-commit/delete limits.

**Why keep the lock state in BigQuery?** Publication and its fencing check must commit together. An external lock with an independent check can expire between check and write. BigQuery conflict semantics are part of the design and still need the live tests.

**Why replay the whole window?** It simplifies checkpoints: progress advances only after all pages publish. It costs re-reading/re-staging after failure, while keeping memory bounded.

**Why not release a lease on every error?** The server may have committed while the client timed out. Releasing early could create overlapping publication attempts before the outcome is known.

**Why did memory fall while time increased?** Smaller DataFrames reduce peak live data but add per-batch overhead. The experiment compares identical processing logic, not different correctness levels.

**What would you improve next?** Run the real BigQuery fault/concurrency suite, deploy into the approved sandbox, verify IAM/alerts, measure end-to-end cost/source impact, and collect an honest freshness observation window. Add lease renewal or CDC only when measured requirements justify them.
