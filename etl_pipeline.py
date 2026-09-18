"""Commerce ETL orchestration; no destination writes bypass Warehouse.publish."""
import logging
import time

from google.cloud import bigquery
from sqlalchemy import URL, create_engine, text

from config.config import Config
from etl.contracts import schema, transform, validate
from etl.warehouse import Warehouse
from etl.source import Source


class ETLPipeline:
    def __init__(self):
        self.config = Config()
        self.config.validate()
        self.mysql_engine = None
        self.bq_client = None
        self.warehouse = None
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        self.logger = logging.getLogger(__name__)

    def connect_mysql(self):
        settings = self.config.mysql_config
        url = URL.create(
            "mysql+mysqlconnector", username=settings["user"], password=settings["password"],
            host=settings["host"], port=settings["port"], database=settings["database"],
        )
        self.mysql_engine = create_engine(url, pool_pre_ping=True)
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
        self.bq_client.create_dataset(dataset, exists_ok=True)

    def create_metadata_table(self):
        self.warehouse.bootstrap(self.config.etl_tables)

    def transform_data(self, frame, transformations):
        return transform(frame, transformations)

    def run_pipeline(self, table_name=None, reconcile=False, replay=None):
        started = time.monotonic()
        try:
            self.connect_mysql()
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
                self.logger.info('Run acquired: table=%s run_id=%s', claim.table, claim.run_id)
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
                self.logger.info('Run succeeded: table=%s run_id=%s rows=%d', claim.table, claim.run_id, rows)
            self.logger.info('Pipeline succeeded: elapsed_seconds=%.3f', time.monotonic() - started)
            return True
        except Exception as error:
            # Leases deliberately expire on error. Never release an ambiguous publish.
            self.logger.error('Pipeline failed: error_type=%s', type(error).__name__)
            return False
        finally:
            if self.mysql_engine is not None:
                self.mysql_engine.dispose()
