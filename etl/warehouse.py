"""BigQuery publication protocol. Live concurrency guarantees require cloud tests.

The lease row is mutated inside every publication transaction. A stale owner
cannot commit against a subsequently acquired generation. Keeping fencing and
publication in BigQuery avoids an external lock/check-then-write race.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import time
import uuid

from google.api_core import exceptions
from google.cloud import bigquery

from config.config import identifier
from etl.observability import event


@dataclass(frozen=True)
class Claim:
    table: str
    run_id: str
    generation: int
    lower_id: int
    lower_time: datetime | None = None


class Warehouse:
    def __init__(self, client, project, dataset, location, lease_seconds=1800):
        self.client = client
        self.prefix = f"{project}.{identifier(dataset)}"
        self.location = location
        self.lease_seconds = lease_seconds
        self.logger = logging.getLogger(__name__)

    def query(self, sql, run_id, operation, parameters=()):
        job_id = f"etl_{run_id}_{operation}"
        config = bigquery.QueryJobConfig(query_parameters=list(parameters))
        return self._resolve(job_id, lambda: self.client.query(
            sql, job_id=job_id, location=self.location, job_config=config,
            job_retry=None,
        ))

    def _resolve(self, job_id, submit):
        """Resolve a stable job identity; never invent another ID on timeout."""
        transient = (
            exceptions.ServiceUnavailable, exceptions.InternalServerError,
            exceptions.TooManyRequests, exceptions.DeadlineExceeded,
            exceptions.GatewayTimeout, exceptions.Conflict, TimeoutError,
        )
        for attempt in range(3):
            try:
                try:
                    job = self.client.get_job(job_id, location=self.location)
                except exceptions.NotFound:
                    job = submit()
                result = job.result(timeout=60)
                event(self.logger, "warehouse_job_completed", job_id=job_id, location=self.location)
                return result
            except transient:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        raise AssertionError("Unreachable")

    @staticmethod
    def params(**values):
        types = {str: "STRING", int: "INT64", datetime: "TIMESTAMP", type(None): "TIMESTAMP"}
        return [bigquery.ScalarQueryParameter(k, types[type(v)], v) for k, v in values.items()]

    def bootstrap(self, tables):
        """Atomic CTAS seeds known table rows once; never INSERT missing locks at runtime."""
        names = [identifier(t["mysql_table"]) for t in tables]
        if not names or len(names) != len(set(names)):
            raise ValueError("Table names must be nonempty and unique")
        selections = " UNION ALL ".join(
            f"SELECT '{name}' AS table_name, 0 AS watermark_id, 0 AS generation, "
            "CAST(NULL AS STRING) AS owner, TIMESTAMP '1970-01-01' AS lease_until, "
            "CAST(NULL AS STRING) AS last_run_id, CAST(NULL AS TIMESTAMP) AS watermark_at" for name in names
        )
        run_id = uuid.uuid4().hex
        self.query(f"CREATE TABLE IF NOT EXISTS `{self.prefix}.etl_state_v2` AS {selections}", run_id, "state")
        self.query(f"""CREATE TABLE IF NOT EXISTS `{self.prefix}.etl_runs_v2` (
            run_id STRING, table_name STRING, status STRING,
            started_at TIMESTAMP, finished_at TIMESTAMP, row_count INT64,
            lower_id INT64, upper_id INT64, lower_time TIMESTAMP, upper_time TIMESTAMP)
        """, run_id, "runs")

    def acquire(self, table):
        table = identifier(table)
        run_id = uuid.uuid4().hex
        rows = list(self.query(f"""
            BEGIN TRANSACTION;
            ASSERT (SELECT COUNT(*) FROM `{self.prefix}.etl_state_v2`
                    WHERE table_name = @table) = 1 AS 'Invalid state cardinality';
            UPDATE `{self.prefix}.etl_state_v2`
            SET owner = @run_id, generation = generation + 1,
                lease_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl SECOND)
            WHERE table_name = @table AND lease_until <= CURRENT_TIMESTAMP();
            ASSERT @@row_count = 1 AS 'Table already leased';
            INSERT INTO `{self.prefix}.etl_runs_v2`
                (run_id, table_name, status, started_at, lower_id, lower_time)
            SELECT @run_id, @table, 'RUNNING', CURRENT_TIMESTAMP(), watermark_id, watermark_at
            FROM `{self.prefix}.etl_state_v2` WHERE table_name = @table;
            COMMIT TRANSACTION;
            SELECT generation, watermark_id, watermark_at FROM `{self.prefix}.etl_state_v2`
            WHERE table_name = @table AND owner = @run_id;
        """, run_id, "acquire", self.params(table=table, run_id=run_id, ttl=self.lease_seconds)))
        if len(rows) != 1:
            raise ValueError("Lease acquisition outcome is not current")
        return Claim(table, run_id, int(rows[0].generation), int(rows[0].watermark_id), getattr(rows[0], "watermark_at", None))

    def publish(self, frame, table, claim, upper_id):
        return self.publish_batches(iter([frame]), table, claim, upper_id)

    def publish_batches(self, batches, table, claim, upper_id, upper_time=None):
        """Bound memory to one validated page; publish only after all loads succeed."""
        if claim.table != table["mysql_table"]:
            raise ValueError("Claim belongs to another table")
        key = identifier(table["primary_key"])
        target = f"{self.prefix}.{identifier(table['bigquery_table'])}"
        if upper_id < claim.lower_id:
            raise ValueError("Checkpoint regression")
        stage = f"{self.prefix}.etl_stage_{claim.table}_{claim.run_id}"
        schema = table.get("schema")
        if schema is None:
            raise ValueError("Explicit target schema is required")
        columns = [identifier(field.name) for field in schema]
        stage_table = bigquery.Table(stage, schema=schema)
        stage_table.expires = datetime.now(timezone.utc) + timedelta(days=1)
        self.client.create_table(stage_table, exists_ok=True)
        self.client.create_table(bigquery.Table(target, schema=schema), exists_ok=True)
        actual = self.client.get_table(target).schema
        if [(x.name, x.field_type, x.mode) for x in actual] != [(x.name, x.field_type, x.mode) for x in schema]:
            raise ValueError("Destination schema migration required")
        row_count = 0
        for index, frame in enumerate(batches):
            if list(frame.columns) != columns:
                raise ValueError("Batch does not match its ordered schema")
            if key not in frame or frame[key].isna().any() or frame[key].duplicated().any():
                raise ValueError("Batch keys must be present, non-null and unique")
            if frame.empty:
                continue
            config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
            job_id = f"etl_{claim.run_id}_load_{index}"
            self._resolve(job_id, lambda: self.client.load_table_from_dataframe(
                frame, stage, job_config=config, job_id=job_id, location=self.location,
            ))
            row_count += len(frame)
        quoted = ", ".join(f"`{col}`" for col in columns)
        if table["incremental"]:
            updates = ", ".join(f"T.`{col}` = S.`{col}`" for col in columns if col != key)
            mutation = f"""MERGE `{target}` T USING `{stage}` S ON T.`{key}` = S.`{key}`
                WHEN MATCHED AND S.updated_at >= T.updated_at THEN UPDATE SET {updates}
                WHEN NOT MATCHED THEN INSERT ({quoted})
                VALUES ({', '.join('S.`' + col + '`' for col in columns)});"""
        else:
            mutation = f"DELETE FROM `{target}` WHERE TRUE; INSERT INTO `{target}` ({quoted}) SELECT {quoted} FROM `{stage}`;"
        # Mutate the lease row in this transaction, rather than checking it externally.
        self.query(f"""
            BEGIN TRANSACTION;
            ASSERT (SELECT COUNT(*) FROM `{self.prefix}.etl_state_v2`
                    WHERE table_name = @table) = 1 AS 'Invalid state cardinality';
            UPDATE `{self.prefix}.etl_state_v2`
            SET watermark_id = @upper_id, last_run_id = @run_id,
                watermark_at = IF(@upper_time IS NULL, watermark_at, GREATEST(COALESCE(watermark_at, @upper_time), @upper_time)),
                owner = NULL, lease_until = TIMESTAMP '1970-01-01'
            WHERE table_name = @table AND owner = @run_id AND generation = @generation
                AND watermark_id = @lower_id AND lease_until > CURRENT_TIMESTAMP();
            ASSERT @@row_count = 1 AS 'Stale owner or checkpoint';
            ASSERT (SELECT COUNT(*) FROM `{stage}`) = @rows AS 'Staging count mismatch';
            ASSERT (SELECT COUNT(*) FROM `{stage}` WHERE `{key}` IS NULL) = 0 AS 'Null key';
            ASSERT (SELECT COUNT(*) - COUNT(DISTINCT `{key}`) FROM `{stage}`) = 0 AS 'Duplicate key';
            ASSERT (SELECT COUNT(*) - COUNT(DISTINCT `{key}`) FROM `{target}`) = 0 AS 'Target key corruption';
            {mutation}
            UPDATE `{self.prefix}.etl_runs_v2`
                SET status = 'SUCCEEDED', finished_at = CURRENT_TIMESTAMP(),
                    row_count = @rows, upper_id = @upper_id, upper_time = @upper_time
                WHERE run_id = @run_id AND table_name = @table;
            ASSERT @@row_count = 1 AS 'Invalid run cardinality';
            COMMIT TRANSACTION;
        """, claim.run_id, "publish", self.params(
            table=claim.table, run_id=claim.run_id, generation=claim.generation,
            lower_id=claim.lower_id, upper_id=int(upper_id), rows=row_count, upper_time=upper_time,
        ))
        return row_count
