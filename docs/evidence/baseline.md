# Baseline experiment

Source baseline: `8a6f6a0`. Date: 2026-09-18.

The baseline tests exercise normalized emails, order totals and categories, empty transforms, configuration, and a disposable MySQL fixture. The fixture has 2 users, 2 products and 3 orders with total revenue **309.85**. It contains synthetic `.test` email addresses only.

Three strict expected-failure regressions capture current bugs: metadata permission errors reset the cursor, checkpoint writes do not await completion, and full loads append. Expected failures are visible technical debt, not passing correctness guarantees. Remove each marker when its implementation changes.

Reproduce:

```sh
make test
make integration
make clean-fixtures
```

The Compose database stores its data in a temporary filesystem. Teardown discards synthetic data only. Host access is bound to localhost on port 3307; override `MYSQL_TEST_PORT` if occupied. No cloud credentials are needed for these tests.

Cloud smoke tests and performance/cost measurements have not been performed.

The first integration attempt exposed a readiness race: `mysqladmin ping` succeeded against MySQL's temporary initialization server before TCP connections were available. The health check now executes a fixture query over TCP as the application user, verifying both connectivity and schema readiness before tests begin.

Validated on 2026-09-18 using Python 3.11.16 in Docker:

- Cloud-free suite: **4 passed, 1 skipped, 3 expected failures**.
- Fresh MySQL 8.4 integration: **5 passed, 3 expected failures**.
- Runtime and development locks share identical versions for all 43 runtime packages.
- `git diff --check`: passed.

The three expected failures remain open correctness tasks. Milestone 1 still requires bootstrap/config fixes and the opt-in cloud gate.

## Task 2 result

The mounted-source suite against the disposable MySQL fixture now reports **21 passed, 1 expected failure**. The checkpoint-read and completion-wait regressions are passing. The remaining expected failure is full-load append semantics. Shell syntax and Compose configuration checks pass. No cloud execution has been claimed.
