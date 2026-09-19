from datetime import datetime, timezone
from decimal import Decimal
import sqlite3

import pandas as pd
import pytest

from etl.contracts import schema, validate
from etl.local_warehouse import LocalWarehouse, LeaseBusy, StaleClaim


@pytest.fixture
def local_case(tmp_path):
    warehouse = LocalWarehouse(tmp_path / 'warehouse.sqlite')
    table = dict(mysql_table='products', bigquery_table='products', primary_key='product_id', incremental=False, schema=schema('products'))
    warehouse.bootstrap([table])
    frame = validate(pd.DataFrame(dict(product_id=[1], category=['Books'], price=['19.95'], updated_at=['2026-01-01'])), 'products')
    return warehouse, table, frame


def publish(case, frame=None):
    warehouse, table, original = case
    return warehouse.publish_batches([original if frame is None else frame], table, warehouse.acquire('products'), 1)


def test_snapshot_rerun_and_empty_replacement(local_case):
    warehouse, _, frame = local_case
    publish(local_case)
    publish(local_case)
    assert warehouse.report()['counts'] == {'products': 1}
    with warehouse.connection() as connection:
        assert connection.execute('SELECT price FROM products').fetchone()[0] == 1995
    publish(local_case, frame.iloc[:0])
    assert warehouse.report()['counts'] == {'products': 0}


def test_upsert_does_not_overwrite_newer_source_version(local_case):
    warehouse, table, frame = local_case
    table['incremental'] = True
    publish(local_case)
    newer = frame.copy()
    newer['updated_at'] = pd.to_datetime(['2026-02-01'], utc=True)
    newer['price'] = Decimal('29.95')
    publish(local_case, newer)
    publish(local_case, frame)
    with warehouse.connection() as connection:
        assert connection.execute('SELECT price FROM products').fetchone()[0] == 2995


def test_partial_staging_does_not_replace_destination_and_releases_lease(local_case):
    warehouse, table, frame = local_case
    publish(local_case)
    def pages():
        yield frame
        raise ValueError('source disconnected')
    claim = warehouse.acquire('products')
    with pytest.raises(ValueError):
        warehouse.publish_batches(pages(), table, claim, 2)
    assert warehouse.report()['counts']['products'] == 1
    assert warehouse.report()['states'][0]['watermark_id'] == 1
    assert warehouse.acquire('products').generation > claim.generation


def test_duplicate_key_across_pages_rolls_back_publication(local_case):
    warehouse, table, frame = local_case
    with pytest.raises(sqlite3.IntegrityError):
        warehouse.publish_batches([frame, frame], table, warehouse.acquire('products'), 1)
    assert warehouse.report()['counts']['products'] == 0
    assert warehouse.report()['states'][0]['watermark_id'] == 0


def test_lease_exclusion_and_stale_generation(local_case):
    warehouse, table, frame = local_case
    old = warehouse.acquire('products')
    with pytest.raises(LeaseBusy):
        warehouse.acquire('products')
    with warehouse.connection(write=True) as connection:
        connection.execute('UPDATE _etl_state SET lease_until=0')
    current = warehouse.acquire('products')
    with pytest.raises(StaleClaim):
        warehouse.publish_batches([frame], table, old, 1)
    warehouse.publish_batches([frame], table, current, 1)
    assert warehouse.report()['counts']['products'] == 1


def test_replay_keeps_checkpoint_time(local_case):
    warehouse, table, frame = local_case
    upper = datetime(2026, 1, 2, tzinfo=timezone.utc)
    warehouse.publish_batches([frame], table, warehouse.acquire('products'), 1, upper)
    claim = warehouse.acquire('products')
    warehouse.publish_batches([frame], table, claim, claim.lower_id)
    assert warehouse.report()['states'][0]['watermark_at'] == upper.isoformat(timespec='microseconds')


def test_retry_of_committed_identity_does_not_consume_source_again(local_case):
    warehouse, table, frame = local_case
    claim = warehouse.acquire('products')
    warehouse.publish_batches([frame], table, claim, 1)
    def forbidden():
        raise AssertionError('Must not read source after resolving committed identity')
        yield
    assert warehouse.publish_batches(forbidden(), table, claim, 1) == 1
