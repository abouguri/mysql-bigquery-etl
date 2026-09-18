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
