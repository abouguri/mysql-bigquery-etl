# Case study: making recurring analytics ingestion recoverable

## Problem

The original prototype appended every load, including complete product snapshots. It separated data writes from progress updates, treated metadata errors as an initial cursor, missed mutable source records through ID-only extraction, and loaded each result into memory at once.

## Implementation

The pipeline now separates source snapshots, data contracts and warehouse publication. It pages an update-time window from MySQL, validates strict schemas and exact decimal money, stages rows, and expresses publication, checkpoint advancement and owner-generation fencing in one BigQuery transaction. Full reconciliation repairs deletes/out-of-window changes; replay preserves live progress. Stable operation IDs resolve uncertain acknowledgements.

The deployment design uses scheduled Cloud Run Jobs, Terraform, scoped identities, verified source TLS and structured events. The source snapshot remains open during staging: this bounds Python memory but introduces a source undo-retention tradeoff. The BigQuery fixed lease rejects overlong/stale runs; cloud lease renewal has not been implemented. The zero-budget SQLite backend renews its lease after each staged page and shares the same MySQL reader and contracts.

## Evidence

- 68 local tests pass with a disposable real MySQL source; five cloud tests are explicitly skipped until sandbox execution is authorized.
- MySQL tests demonstrate bounded pages, timestamp ties, concurrent update/insert snapshot isolation, replay bounds, late-record reconciliation and deletion visibility.
- Fault-oriented client tests demonstrate stable job identity, no publish after failed staging, schema rejection and empty-snapshot publication construction. They do not establish live BigQuery transaction guarantees.
- Terraform schema validation, static checks and a non-root CLI container smoke test pass. Deployment and notification delivery remain unverified.
- At one million synthetic rows, bounded processing achieved 150.32 MiB median peak RSS versus 772.95 MiB full-frame, with median processing time 8.625s versus 8.299s over five samples/configuration. See the linked raw benchmark and methodology.

- The local MySQL-to-SQLite demo produces 309.85 USD and preserves counts across repeat runs. Separate-process failure checks exercise abrupt exits around commit and competing ownership; [recovery evidence](evidence/local-recovery.md) states the exact scope.

## CV wording supported today

> Built a Python/MySQL-to-BigQuery batch ingestion project with bounded snapshot extraction, explicit data contracts, replay/reconciliation and staged transactional publication; added 68 local tests, Terraform deployment definitions and recovery documentation.

> Benchmarked one million synthetic rows under a two-CPU/2-GiB limit: 10k-row batches used 80.6% less median peak process memory than a full DataFrame with the same validation logic, with 3.9% greater median processing time across five samples.

Label this a personal project. Do not claim production deployment, end-to-end exactly-once guarantees, cloud cost savings, real customer impact, an achieved freshness SLO, or staff-level organizational influence from the current evidence.

## Next proof required

Run the real BigQuery replay/concurrency/revenue tests, verify the deployed identities and alert delivery, publish cost/end-to-end benchmarks, and observe scheduled execution over the chosen measurement period. A peer reproducing the demo would provide additional independent evidence; that review has not happened yet.
