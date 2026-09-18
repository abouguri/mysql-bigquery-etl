"""Commerce ETL orchestration; no destination writes bypass Warehouse.publish."""
import logging
import time

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import URL, create_engine, text

from config.config import Config, identifier
from etl.contracts import schema, transform, validate
from etl.warehouse import Warehouse


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

    def extract_data(self, table, last_id=0):
        name, key = identifier(table['mysql_table']), identifier(table['primary_key'])
        sql = f"SELECT * FROM `{name}`"
        parameters = {}
        if table['incremental']:
            sql += f" WHERE `{key}` > :last_id"
            parameters['last_id'] = last_id
        sql += f" ORDER BY `{key}`"
        with self.mysql_engine.connect() as connection:
            return pd.read_sql(text(sql), connection, params=parameters, coerce_float=False)

    def transform_data(self, frame, transformations):
        return transform(frame, transformations)

    def load_data(self, frame, table, claim, upper_id):
        contract = {**table, 'schema': schema(table['mysql_table'])}
        return self.warehouse.publish(frame, contract, claim, upper_id)

    def run_pipeline(self):
        started = time.monotonic()
        try:
            self.connect_mysql()
            self.connect_bigquery()
            self.ensure_dataset()
            self.create_metadata_table()
            for table in self.config.etl_tables:
                claim = self.warehouse.acquire(table['mysql_table'])
                self.logger.info('Run acquired: table=%s run_id=%s', claim.table, claim.run_id)
                source = self.extract_data(table, last_id=claim.lower_id)
                key = table['primary_key']
                upper = max(claim.lower_id, int(source[key].max())) if not source.empty else claim.lower_id
                frame = validate(self.transform_data(source, table['transformations']), table['mysql_table'])
                if len(frame) != len(source) or set(frame[key]) != set(source[key]):
                    raise ValueError('Transform changed source key accounting')
                self.load_data(frame, table, claim, upper)
                self.logger.info('Run succeeded: table=%s run_id=%s rows=%d', claim.table, claim.run_id, len(frame))
            self.logger.info('Pipeline succeeded: elapsed_seconds=%.3f', time.monotonic() - started)
            return True
        except Exception as error:
            # Leases deliberately expire on error. Never release an ambiguous publish.
            self.logger.error('Pipeline failed: error_type=%s', type(error).__name__)
            return False
        finally:
            if self.mysql_engine is not None:
                self.mysql_engine.dispose()
