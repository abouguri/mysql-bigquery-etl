import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import URL, create_engine, text

from etl.source import Source

pytestmark = [pytest.mark.mysql, pytest.mark.skipif(
    os.getenv('RUN_MYSQL_TESTS') != '1', reason='opt-in local MySQL'
)]


@pytest.fixture
def source_db():
    engine = create_engine(URL.create(
        'mysql+mysqlconnector', username=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
        host=os.environ['MYSQL_HOST'], port=int(os.environ['MYSQL_PORT']), database=os.environ['MYSQL_DATABASE'],
    ))
    with engine.begin() as conn:
        conn.execute(text('CREATE TABLE source_fixture (id BIGINT PRIMARY KEY, label VARCHAR(20), updated_at DATETIME(6))'))
        conn.execute(text("INSERT INTO source_fixture VALUES (1, 'old', '2026-01-01'), (2, 'same', '2026-01-02'), (3, 'same', '2026-01-02')"))
    try:
        yield engine, dict(mysql_table='source_fixture', primary_key='id')
    finally:
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE source_fixture'))
        engine.dispose()


def test_bounded_pages_and_equal_timestamps(source_db):
    engine, table = source_db
    watermark = datetime(2026, 1, 2, 0, 30, tzinfo=timezone.utc)
    with Source(engine, batch_size=1, lookback_seconds=3600).snapshot(table, watermark) as window:
        pages = list(window.batches)
        assert all(len(page) <= 1 for page in pages)
        assert [int(page.id.iloc[0]) for page in pages] == [2, 3]


def test_snapshot_is_stable_during_concurrent_changes(source_db):
    engine, table = source_db
    with Source(engine, batch_size=1).snapshot(table, full=True) as window:
        first = next(window.batches)
        with engine.begin() as conn:
            conn.execute(text("UPDATE source_fixture SET label = 'changed' WHERE id = 2"))
            conn.execute(text("INSERT INTO source_fixture VALUES (4, 'new', '2026-01-02')"))
        remaining = list(window.batches)
        assert first.id.tolist() == [1]
        assert [int(page.id.iloc[0]) for page in remaining] == [2, 3]
        assert remaining[0].label.tolist() == ['same']


def test_reconciliation_includes_late_records_and_reflects_deletes(source_db):
    engine, table = source_db
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO source_fixture VALUES (4, 'late', '2026-01-01')"))
        conn.execute(text('DELETE FROM source_fixture WHERE id = 2'))
    watermark = datetime(2026, 1, 2, 0, 30, tzinfo=timezone.utc)
    reader = Source(engine, batch_size=10, lookback_seconds=3600)
    with reader.snapshot(table, watermark) as window:
        assert next(window.batches).id.tolist() == [3]
    with reader.snapshot(table, watermark, full=True) as window:
        assert next(window.batches).id.tolist() == [1, 3, 4]


def test_null_timestamp_reaches_validation_instead_of_disappearing(source_db):
    engine, table = source_db
    with engine.begin() as conn:
        conn.execute(text('UPDATE source_fixture SET updated_at = NULL WHERE id = 1'))
    with Source(engine).snapshot(table, datetime(2026, 1, 3, tzinfo=timezone.utc)) as window:
        assert 1 in next(window.batches).id.tolist()


def test_nonpositive_key_cannot_be_mistaken_for_empty_snapshot(source_db):
    engine, table = source_db
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO source_fixture VALUES (0, 'invalid', '2026-01-01')"))
    with pytest.raises(ValueError, match='positive'):
        with Source(engine).snapshot(table, full=True):
            pass


def test_replay_window_is_bounded(source_db):
    engine, table = source_db
    bounds = (datetime(2026, 1, 2, tzinfo=timezone.utc), datetime(2026, 1, 2, 1, tzinfo=timezone.utc))
    with Source(engine).snapshot(table, replay=bounds) as window:
        assert next(window.batches).id.tolist() == [2, 3]
