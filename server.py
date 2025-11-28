from flask import Flask, jsonify
import logging
from etl_pipeline import ETLPipeline

app = Flask(__name__)

@app.route('/', methods=['GET'])
def run_etl():
    try:
        pipeline = ETLPipeline()
        success = pipeline.run_pipeline()
        if success:
            return jsonify({'status': 'success', 'message': 'ETL pipeline completed successfully'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'ETL pipeline failed'}), 500
    except Exception as e:
        logging.exception("ETL pipeline execution error")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
