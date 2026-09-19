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

- Real MySQL extraction at one million rows used 177.70 MiB median peak worker RSS with 10k-row pages versus 872.41 MiB full-size pages (79.6% lower); median extraction/processing time was 26.229s versus 22.939s over three samples/mode. [Scope and raw evidence](../benchmarks/results/mysql-summary.md).

## CV wording supported today

> Built a Python/MySQL-to-BigQuery batch ingestion project with bounded snapshot extraction, explicit data contracts, replay/reconciliation and staged transactional publication; added 68 local tests, Terraform deployment definitions and recovery documentation.

> Measured extraction and validation of one million synthetic MySQL rows under a two-CPU/2-GiB worker limit: 10k-row pages reduced median peak worker memory by 79.6%, trading runtime from 22.939s to 26.229s across three samples per mode.

> Built a reproducible local warehouse demo and verified atomic data/checkpoint recovery with abrupt process exits before and after commit, durable retry identities and competing worker processes.

Label this a personal project. Do not claim production deployment, end-to-end exactly-once guarantees, cloud cost savings, real customer impact, an achieved freshness SLO, or staff-level organizational influence from the current evidence.

## Next proof required

With the current $0 budget, the next external proof is another engineer reproducing the local demo and recovery checks; that review has not happened yet. A short screen recording can show the actual commands, results and tradeoffs. Cloud replay/concurrency tests, deployed identities, alerts, costs and freshness observations remain deferred; local results do not close those gates.
