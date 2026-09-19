# Five-minute presentation

This is a reproducible demo guide, not a claim that a video or live cloud demo has already been recorded.

| Time | Show | Explain |
|---|---|---|
| 0:00–0:45 | README architecture | Explain the commerce use case, $0 constraint and shared reader/contracts with separate warehouse backends |
| 0:45–1:30 | `make demo`, `make demo-report`, then repeat | Show two users, two products, three orders and 309.85 USD without duplicates |
| 1:30–2:30 | `make recovery-demo` and recovery CSV | Workers exit abruptly around commit; inspect atomic progress, lease expiry and committed retry identity |
| 2:30–3:30 | `tests/test_source.py` and CLI help | Consistent snapshots during updates; reconciliation repairs deletes and lookback limits; replay is current-state repair |
| 3:30–4:30 | Real MySQL benchmark table and raw samples | Explain 79.6% lower worker memory, the runtime tradeoff, sample count and excluded destination/server costs |
| 4:30–5:00 | Case study and deferred cloud gates | State exactly what the local tests prove and what still requires live BigQuery validation |

For a real cloud demo, first complete the deployment/runbook gates. Then show a scheduled run, edit source rows, inject a source failure, inspect alerts/job IDs, resolve any uncertain outcome, wait for lease expiry, rerun and reconcile revenue. Do not replace this with fake successful logs or label mock tests as cloud recovery evidence.

## Interview follow-ups

**Why batch?** The current workload is scheduled analytics. Batch avoids introducing a CDC platform before freshness/capture requirements justify it. Polling has explicit late-commit/delete limits.

**Why keep the lock state in BigQuery?** Publication and its fencing check must commit together. An external lock with an independent check can expire between check and write. BigQuery conflict semantics are part of the design and still need the live tests.

**Why replay the whole window?** It simplifies checkpoints: progress advances only after all pages publish. It costs re-reading/re-staging after failure, while keeping memory bounded.

**Why different lease handling?** A cloud job may still run after a client timeout, so that backend keeps its conservative expiry rule. SQLite can resolve its local transaction state and release a matching claim on an ordinary error. An abrupt process exit bypasses cleanup, so recovery waits for lease expiry.

**Why did memory fall while time increased?** Smaller DataFrames reduce peak live data but add per-batch overhead. The experiment compares identical processing logic, not different correctness levels.

**Why SQLite?** It gives a durable, inspectable local destination with transactions and exact integer-cent money at no service cost. Its single-writer behavior limits concurrency; it does not emulate BigQuery.

**What would you improve next at $0?** Ask a peer to reproduce the demo, record the walkthrough, then measure source undo retention and local destination publication under larger workloads. Keep cloud gates deferred. Add CDC only when freshness and capture requirements justify its operational complexity.
