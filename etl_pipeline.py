import pandas as pd
import logging
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from config.config import Config

class ETLPipeline:
    """Main ETL pipeline class for MySQL to BigQuery"""
    
    def __init__(self):
        self.config = Config()
        self.mysql_engine = None
        self.bq_client = None
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect_mysql(self):
        """Establish MySQL connection"""
        try:
            mysql_config = self.config.mysql_config
            connection_string = (
                f"mysql+mysqlconnector://{mysql_config['user']}:{mysql_config['password']}"
                f"@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
            )
            self.mysql_engine = create_engine(connection_string)
            self.logger.info("Successfully connected to MySQL")
        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            raise
    
    def connect_bigquery(self):
        """Establish BigQuery client"""
        try:
            self.bq_client = bigquery.Client(project=self.config.project_id)
            self.logger.info("Successfully connected to BigQuery")
        except Exception as e:
            self.logger.error(f"Failed to connect to BigQuery: {e}")
            raise
    
    def get_last_processed_id(self, table_name):
        """Get the last processed ID for incremental loading"""
        try:
            dataset_ref = self.bq_client.dataset(self.config.bigquery_config['dataset_id'])
            table_ref = dataset_ref.table('etl_metadata')
            
            query = f"""
                SELECT last_processed_id 
                FROM `{self.config.project_id}.{self.config.bigquery_config['dataset_id']}.etl_metadata`
                WHERE table_name = '{table_name}'
            """
            query_job = self.bq_client.query(query)
            result = query_job.result()
            
            for row in result:
                return row.last_processed_id
            return 0
        except Exception as e:
            self.logger.warning(f"Could not get last processed ID for {table_name}: {e}")
            return 0
    
    def update_last_processed_id(self, table_name, last_id):
        """Update the last processed ID in metadata table"""
        try:
            table_id = f"{self.config.project_id}.{self.config.bigquery_config['dataset_id']}.etl_metadata"
            
            query = f"""
                MERGE `{table_id}` T
                USING (SELECT '{table_name}' as table_name, {last_id} as last_processed_id) S
                ON T.table_name = S.table_name
                WHEN MATCHED THEN
                    UPDATE SET last_processed_id = S.last_processed_id, updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (table_name, last_processed_id, created_at, updated_at)
                    VALUES (S.table_name, S.last_processed_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            """
            self.bq_client.query(query)
            self.logger.info(f"Updated last processed ID for {table_name} to {last_id}")
        except Exception as e:
            self.logger.error(f"Failed to update last processed ID: {e}")
    
    def extract_data(self, table_config):
        """Extract data from MySQL"""
        try:
            table_name = table_config['mysql_table']
            primary_key = table_config['primary_key']
            incremental = table_config['incremental']
            
            if incremental:
                last_id = self.get_last_processed_id(table_name)
                query = f"SELECT * FROM {table_name} WHERE {primary_key} > {last_id} ORDER BY {primary_key}"
            else:
                query = f"SELECT * FROM {table_name}"
            
            self.logger.info(f"Extracting data from {table_name}")
            df = pd.read_sql(query, self.mysql_engine)
            self.logger.info(f"Extracted {len(df)} rows from {table_name}")
            return df
        
        except Exception as e:
            self.logger.error(f"Failed to extract data from {table_name}: {e}")
            raise
    
    def transform_data(self, df, transformations):
        """Apply transformations to the data"""
        try:
            if df.empty:
                return df
            
            # Apply each transformation
            for transformation in transformations:
                if transformation == 'clean_emails' and 'email' in df.columns:
                    df['email'] = df['email'].str.lower().str.strip()
                
                elif transformation == 'standardize_dates':
                    date_columns = [col for col in df.columns if 'date' in col.lower() or 'created' in col.lower() or 'updated' in col.lower()]
                    for col in date_columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                elif transformation == 'calculate_totals' and all(col in df.columns for col in ['quantity', 'unit_price']):
                    df['total_amount'] = df['quantity'] * df['unit_price']
                
                elif transformation == 'categorize_orders' and 'total_amount' in df.columns:
                    df['order_size'] = pd.cut(
                        df['total_amount'],
                        bins=[0, 50, 200, float('inf')],
                        labels=['Small', 'Medium', 'Large']
                    )
                
                elif transformation == 'standardize_categories' and 'category' in df.columns:
                    df['category'] = df['category'].str.title().str.strip()
                
                elif transformation == 'format_prices' and 'price' in df.columns:
                    df['price'] = pd.to_numeric(df['price'], errors='coerce').round(2)
            
            self.logger.info("Data transformations completed successfully")
            return df
        
        except Exception as e:
            self.logger.error(f"Failed to transform data: {e}")
            raise
    
    def load_data(self, df, table_config):
        """Load data to BigQuery"""
        try:
            table_name = table_config['bigquery_table']
            dataset_id = self.config.bigquery_config['dataset_id']
            table_id = f"{self.config.project_id}.{dataset_id}.{table_name}"
            
            # Create dataset if it doesn't exist
            dataset_ref = self.bq_client.dataset(dataset_id)
            try:
                self.bq_client.get_dataset(dataset_ref)
            except NotFound:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.config.bigquery_config['location']
                self.bq_client.create_dataset(dataset)
                self.logger.info(f"Created dataset {dataset_id}")
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                autodetect=True
            )
            
            # Load data
            job = self.bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for job to complete
            
            # Get the table to check row count
            table = self.bq_client.get_table(table_id)
            self.logger.info(f"Loaded {job.output_rows} rows to {table_id}. Total rows: {table.num_rows}")
            
            return job.output_rows
        
        except Exception as e:
            self.logger.error(f"Failed to load data to BigQuery: {e}")
            raise
    
    def create_metadata_table(self):
        """Create metadata table for tracking ETL progress"""
        try:
            dataset_id = self.config.bigquery_config['dataset_id']
            table_id = f"{self.config.project_id}.{dataset_id}.etl_metadata"
            
            schema = [
                bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_processed_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            self.bq_client.create_table(table, exists_ok=True)
            self.logger.info("Metadata table created/verified")
        
        except Exception as e:
            self.logger.error(f"Failed to create metadata table: {e}")
            raise
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        try:
            self.logger.info("Starting ETL pipeline")
            
            # Establish connections
            self.connect_mysql()
            self.connect_bigquery()
            
            # Create metadata table
            self.create_metadata_table()
            
            # Process each table
            for table_config in self.config.etl_tables:
                self.logger.info(f"Processing table: {table_config['mysql_table']}")
                
                # Extract
                df = self.extract_data(table_config)
                
                if df.empty:
                    self.logger.info(f"No new data for {table_config['mysql_table']}")
                    continue
                
                # Transform
                df = self.transform_data(df, table_config.get('transformations', []))
                
                # Load
                rows_loaded = self.load_data(df, table_config)
                
                # Update metadata for incremental loads
                if table_config['incremental'] and rows_loaded > 0:
                    primary_key = table_config['primary_key']
                    last_id = df[primary_key].max()
                    self.update_last_processed_id(table_config['mysql_table'], last_id)
                
                self.logger.info(f"Successfully processed {table_config['mysql_table']}")
            
            self.logger.info("ETL pipeline completed successfully")
            return True
        
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            return False
        finally:
            # Clean up connections
            if self.mysql_engine:
                self.mysql_engine.dispose()