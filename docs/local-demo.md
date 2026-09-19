# Zero-budget local demo

No cloud account or credentials are needed. Docker Compose puts the running MySQL source and demo worker on an **internal network**, with no external route. Image/package downloads happen during build, not through cloud services. `ETL_BACKEND` defaults to `local`; BigQuery requires both explicit backend selection and `ETL_ALLOW_CLOUD=1`.

```sh
make demo
make demo-report
make demo                 # repeat safely: counts and revenue stay the same
```

Expected: two users, two products, three orders, and **309.85 USD**. The demo runs the same MySQL extraction, transformations and schema validation as the cloud design, then publishes into `local/warehouse.sqlite`. Source fixtures are disposable; the destination file remains on disk and is ignored by Git. `make clean-fixtures` removes the source containers, not the local warehouse file.

To reconcile deletions or source changes older than the polling lookback:

```sh
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) docker compose --profile demo run --rm demo --reconcile
```

For a completely new synthetic experiment, choose another local database path or explicitly remove the disposable local warehouse file after inspecting it. Do not point the demo at real data without a separate retention/migration plan.

## Local publication contract

SQLite stores required columns, primary keys and a schema signature. Monetary columns store exact **integer cents**, while the report converts them to decimal USD. Timestamps are normalized to UTC with fixed microsecond precision. Staged pages remain bounded; a snapshot replacement or version-checked upsert, progress update and success record commit together.

`BEGIN IMMEDIATE` serializes writers across the database file. A table lease/generation rejects expired owners; a successful staged page renews the local lease. A slow source page may outlive the lease and must fail closed. Expired staging is removed when a new owner acquires the table. Known local failures roll back publication, mark the attempt failed, remove staging and release only their own claim. A hard process crash cannot run cleanup, so the next owner waits for expiry (30 seconds by default).

After a commit whose acknowledgement is lost, the same run identity resolves its durable success without rereading the source. This is a local SQLite contract, not a BigQuery emulator or proof of distributed cloud behavior. SQLite has one writer per database; the backend is a portfolio/reference implementation, not an analytics warehouse at production scale.

Reference: [SQLite transaction behavior](https://www.sqlite.org/lang_transaction.html).
