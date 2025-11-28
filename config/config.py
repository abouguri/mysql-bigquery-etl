import os
from google.cloud import secretmanager
import json

class Config:
    """Configuration management for the ETL pipeline"""
    
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.environment = os.getenv('ENVIRONMENT', 'development')
        
    def get_secret(self, secret_id, default=None):
        """Retrieve secrets from Google Secret Manager"""
        if self.environment == 'development':
            # For local development, use environment variables
            return os.getenv(secret_id, default)
        
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{self.project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(name=secret_name)
        return response.payload.data.decode('UTF-8')
    
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