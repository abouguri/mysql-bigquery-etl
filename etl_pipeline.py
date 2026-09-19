"""Commerce ETL orchestration; no destination writes bypass Warehouse.publish."""
import logging
import os
import time

from google.cloud import bigquery
from sqlalchemy import URL, create_engine, text

from config.config import Config
from etl.contracts import schema, transform, validate
from etl.warehouse import Warehouse
from etl.source import Source
from etl.observability import configure_logging, event


class ETLPipeline:
    def __init__(self, backend=None):
        self.config = Config(backend=backend)
        self.config.validate()
        self.mysql_engine = None
        self.bq_client = None
        self.warehouse = None
        configure_logging()
        self.logger = logging.getLogger(__name__)

    def connect_mysql(self):
        settings = self.config.mysql_config
        url = URL.create(
            "mysql+mysqlconnector", username=settings["user"], password=settings["password"],
            host=settings["host"], port=settings["port"], database=settings["database"],
        )
        tls = {}
        if os.getenv('MYSQL_SSL_CA'):
            tls = dict(ssl_ca=os.environ['MYSQL_SSL_CA'], ssl_verify_cert=True, ssl_verify_identity=True)
        self.mysql_engine = create_engine(url, pool_pre_ping=True, connect_args=tls)
        with self.mysql_engine.connect() as connection:
            connection.execute(text("SELECT 1"))

    def connect_bigquery(self):
        self.bq_client = bigquery.Client(project=self.config.project_id)
        self.warehouse = Warehouse(
            self.bq_client, self.config.project_id,
            self.config.bigquery_config['dataset_id'], self.config.bigquery_config['location'],
        )

    def ensure_dataset(self):
        dataset = bigquery.Dataset(f"{self.config.project_id}.{self.config.bigquery_config['dataset_id']}")
        dataset.location = self.config.bigquery_config['location']
        if self.config.environment == 'production':
            actual = self.bq_client.get_dataset(dataset)
            if actual.location.lower() != dataset.location.lower():
                raise ValueError('Dataset location mismatch')
        else:
            self.bq_client.create_dataset(dataset, exists_ok=True)

    def create_metadata_table(self):
        self.warehouse.bootstrap(self.config.etl_tables)

    def transform_data(self, frame, transformations):
        return transform(frame, transformations)

    def run_pipeline(self, table_name=None, reconcile=False, replay=None):
        started = time.monotonic()
        claim = None
        try:
            self.connect_mysql()
            if self.config.backend == 'local':
                from etl.local_warehouse import LocalWarehouse
                self.warehouse = LocalWarehouse(self.config.local_path)
            else:
                self.connect_bigquery()
                self.ensure_dataset()
            self.create_metadata_table()
            source_reader = Source(self.mysql_engine, self.config.batch_size, self.config.lookback_seconds)
            selected = [t for t in self.config.etl_tables if table_name is None or t['mysql_table'] == table_name]
            if not selected:
                raise ValueError('Unknown source table')
            for original in selected:
                table = {**original, 'schema': schema(original['mysql_table'])}
                if reconcile:
                    table['incremental'] = False
                if replay:
                    table['incremental'] = True
                claim = self.warehouse.acquire(table['mysql_table'])
                event(self.logger, 'run_acquired', table=claim.table, run_id=claim.run_id)
                with source_reader.snapshot(table, claim.lower_time, full=not table['incremental'], replay=replay) as window:
                    def validated_pages():
                        for source in window.batches:
                            frame = validate(self.transform_data(source, table['transformations']), table['mysql_table'])
                            key = table['primary_key']
                            if len(frame) != len(source) or set(frame[key]) != set(source[key]):
                                raise ValueError('Transform changed source key accounting')
                            yield frame
                    upper = claim.lower_id if replay else max(claim.lower_id, window.upper_id)
                    rows = self.warehouse.publish_batches(
                        validated_pages(), table, claim, upper,
                        upper_time=None if replay else window.upper_time,
                    )
                event(self.logger, 'table_succeeded', table=claim.table, run_id=claim.run_id, rows=rows, source_watermark=window.upper_time, replay=bool(replay))
            event(self.logger, 'pipeline_succeeded', elapsed_seconds=round(time.monotonic() - started, 3))
            return True
        except Exception as error:
            # SQLite can resolve local rollback/commit; the cloud backend keeps its
            # conservative expiry rule because a remote job may still be running.
            if self.config.backend == 'local' and claim is not None:
                try:
                    self.warehouse.abort(claim)
                except Exception:
                    event(self.logger, 'local_cleanup_failed', severity=logging.ERROR)
            event(self.logger, 'pipeline_failed', severity=logging.ERROR, error_type=type(error).__name__, table=getattr(claim, 'table', None), run_id=getattr(claim, 'run_id', None))
            return False
        finally:
            if self.mysql_engine is not None:
                self.mysql_engine.dispose()
