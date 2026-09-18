# Engineering backlog

This is the versioned issue register for the roadmap. Check validation evidence before closing a milestone.

| ID | Priority | Task | Status |
|---|---|---|---|
| ETL-01 | P0 | Reproducible fixtures, runtime, dependencies and baseline tests | Done; local evidence recorded |
| ETL-02 | P0 | Fail closed on checkpoint read failures | Done; permission/duplicate tests |
| ETL-03 | P0 | Await metadata jobs; atomic data/checkpoint publication | Implemented atomic publication; cloud gate pending |
| ETL-04 | P0 | Correct full-snapshot and empty-source semantics | Implemented; cloud replay gate pending |
| ETL-05 | P0 | Writer fencing, overlap and stale publication tests | BigQuery transaction protocol implemented; cloud gate pending |
| ETL-06 | P0 | Updates/deletes and source cursor contract | Timestamp lookback + full reconciliation implemented; MySQL tests passing |
| ETL-07 | P1 | Bootstrap order, secret/config contract, URL/SQL hygiene | Implemented; local tests, cloud bootstrap gate pending |
| ETL-08 | P1 | Bounded extraction and fixed run bounds | Implemented and tested against concurrent MySQL changes |
| ETL-09 | P1 | Schema, exact money and fail-closed transformations | Implemented; local contract tests passing |
| ETL-10 | P1 | Job runtime, deployment scripts and build isolation | Implemented; Terraform schema and CLI image checks passed |
| ETL-11 | P1 | Infrastructure, scoped identities, alerts and runbooks | Terraform prepared; apply and alert delivery require sandbox |
| ETL-12 | P1 | Opt-in real BigQuery correctness/failure tests | Written; sandbox/project/budget required to execute |
| ETL-13 | P1 | Revenue mart, reconciliation and performance/cost evidence | SQL and local 100k/1m evidence ready; cloud reconciliation/cost pending |
| ETL-14 | P2 | README, release, demo and measured CV case study | Written for local preview; live/recorded demo and independent reproduction pending |

External requirements: user-selected cloud sandbox and test spend cap; cloud credentials; a real observation window for freshness claims. Do not invent results while those are unavailable.

## External gates that remain open

1. Select the sandbox project, approved test spend and source network; provide authenticated access through the normal local tooling.
2. Execute the five opt-in BigQuery tests, then add any failure scenarios exposed by real service behavior.
3. Apply the reviewed Terraform plan; validate source TLS, unauthorized-invoker rejection, Scheduler execution and notification delivery before enabling schedules.
4. Measure end-to-end extraction/load/merge, source impact and cloud costs; compare partition/clustering layouts with an identical workload.
5. Observe the proposed freshness objective over a real 30-day window. Do not substitute a short demo for that history.
6. Record the cloud failure/recovery demo and have another engineer reproduce it.

Local implementation does not close these gates. The original six milestones remain partially verified until their external acceptance criteria are met.
