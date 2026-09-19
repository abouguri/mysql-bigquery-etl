import os
import re


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def identifier(value):
    """Validate SQL identifiers; values are bound separately."""
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError("Invalid SQL identifier")
    return value


class Config:
    """Environment-only configuration; cloud secrets are injected by the runtime."""

    def __init__(self, backend=None):
        self.backend = backend or os.getenv("ETL_BACKEND", "local")
        self.local_path = os.getenv("ETL_LOCAL_PATH", "local/warehouse.sqlite")
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.environment = os.getenv("ENVIRONMENT", "development")

    def get_secret(self, secret_id, default=None):
        # The same contract applies locally and in Cloud Run. No SDK/network I/O.
        return os.getenv(secret_id, default)

    def validate(self):
        if self.backend not in {"local", "bigquery"}:
            raise ValueError("Unsupported ETL_BACKEND")
        if self.backend == "bigquery" and os.getenv("ETL_ALLOW_CLOUD") != "1":
            raise ValueError("Cloud execution disabled; ETL_ALLOW_CLOUD=1 is required")
        required = ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"]
        if self.backend == "bigquery":
            required.append("GCP_PROJECT_ID")
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise ValueError("Missing required settings: " + ", ".join(missing))
        if self.backend == "bigquery" and not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", self.project_id):
            raise ValueError("Invalid GCP_PROJECT_ID")
        if self.environment not in {"development", "test", "production"}:
            raise ValueError("Invalid ENVIRONMENT")
        if self.environment == "production" and not os.getenv("MYSQL_SSL_CA"):
            raise ValueError("Production requires MYSQL_SSL_CA for verified source TLS")
        identifier(self.bigquery_config["dataset_id"])
        if not self.bigquery_config["location"]:
            raise ValueError("BIGQUERY_LOCATION must not be empty")
        try:
            port = self.mysql_config["port"]
        except ValueError:
            raise ValueError("MYSQL_PORT must be an integer") from None
        if not 1 <= port <= 65535:
            raise ValueError("MYSQL_PORT must be between 1 and 65535")
        if self.batch_size <= 0 or self.lookback_seconds < 0:
            raise ValueError("Invalid batching settings")
        for table in self.etl_tables:
            for name in ("mysql_table", "bigquery_table", "primary_key"):
                identifier(table[name])

    @property
    def batch_size(self):
        return int(os.getenv("ETL_BATCH_SIZE", "10000"))

    @property
    def lookback_seconds(self):
        return int(os.getenv("ETL_LOOKBACK_SECONDS", "86400"))

    @property
    def mysql_config(self):
        """MySQL connection configuration"""
        return {
            'host': self.get_secret('MYSQL_HOST'),
            'port': int(self.get_secret('MYSQL_PORT', '3306')),
            'user': self.get_secret('MYSQL_USER'),
            'password': self.get_secret('MYSQL_PASSWORD'),
            'database': self.get_secret('MYSQL_DATABASE')
        }
    
    @property
    def bigquery_config(self):
        """BigQuery configuration"""
        return {
            'project_id': self.project_id,
            'dataset_id': os.getenv('BIGQUERY_DATASET', 'mysql_etl'),
            'location': os.getenv('BIGQUERY_LOCATION', 'US')
        }
    
    @property
    def etl_tables(self):
        """List of tables to extract and their transformations"""
        return [
            {
                'mysql_table': 'users',
                'bigquery_table': 'users',
                'primary_key': 'user_id',
                'incremental': True,
                'transformations': [
                    'clean_emails',
                    'standardize_dates'
                ]
            },
            {
                'mysql_table': 'orders',
                'bigquery_table': 'orders',
                'primary_key': 'order_id',
                'incremental': True,
                'transformations': [
                    'calculate_totals',
                    'categorize_orders'
                ]
            },
            {
                'mysql_table': 'products',
                'bigquery_table': 'products',
                'primary_key': 'product_id',
                'incremental': False,  # Full load for dimension table
                'transformations': [
                    'standardize_categories',
                    'format_prices'
                ]
            }
        ]