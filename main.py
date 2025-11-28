import logging
from etl_pipeline import ETLPipeline

def main():
    """Main entry point for the ETL pipeline"""
    try:
        # Initialize and run the pipeline
        pipeline = ETLPipeline()
        success = pipeline.run_pipeline()
        
        if success:
            logging.info("ETL pipeline execution completed successfully")
            return 0
        else:
            logging.error("ETL pipeline execution failed")
            return 1
    
    except Exception as e:
        logging.error(f"Application error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())