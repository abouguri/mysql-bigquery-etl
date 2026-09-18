import os
from decimal import Decimal

import pytest
from sqlalchemy import URL, create_engine, text


@pytest.mark.mysql
@pytest.mark.skipif(os.getenv("RUN_MYSQL_TESTS") != "1", reason="opt-in local MySQL")
def test_seeded_source():
    engine = create_engine(URL.create(
        "mysql+mysqlconnector", username=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"], host=os.environ["MYSQL_HOST"],
        port=int(os.environ["MYSQL_PORT"]), database=os.environ["MYSQL_DATABASE"],
    ))
    try:
        with engine.connect() as connection:
            for table, expected in [("users", 2), ("products", 2), ("orders", 3)]:
                assert connection.scalar(text(f"SELECT COUNT(*) FROM {table}")) == expected
            assert connection.scalar(text(
                "SELECT SUM(quantity * unit_price) FROM orders"
            )) == Decimal("309.85")
    finally:
        engine.dispose()


@pytest.mark.mysql
@pytest.mark.skipif(os.getenv("RUN_MYSQL_TESTS") != "1", reason="opt-in local MySQL")
def test_source_frames_meet_commerce_contracts(monkeypatch):
    from etl_pipeline import ETLPipeline
    from etl.contracts import validate
    monkeypatch.setenv('GCP_PROJECT_ID', 'fixture-project')
    pipeline = ETLPipeline()
    pipeline.connect_mysql()
    try:
        for table in pipeline.config.etl_tables:
            source = pipeline.extract_data(table)
            output = validate(pipeline.transform_data(source, table['transformations']), table['mysql_table'])
            assert len(output) == len(source)
            if table['mysql_table'] == 'orders':
                assert sum(output.total_amount) == Decimal('309.85')
    finally:
        pipeline.mysql_engine.dispose()
