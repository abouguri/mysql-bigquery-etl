"""Opt-in billable integration tests. Uses a unique dataset and deletes only it."""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from etl.contracts import schema, validate
from etl.warehouse import Warehouse

pytestmark = [pytest.mark.cloud, pytest.mark.skipif(
    os.getenv('RUN_BIGQUERY_TESTS') != '1', reason='requires explicit cloud opt-in and sandbox project'
)]


@pytest.fixture
def cloud():
    project = os.environ['BQ_TEST_PROJECT']
    dataset = f'etl_test_{uuid.uuid4().hex}'
    location = os.getenv('BQ_TEST_LOCATION', 'US')
    client = bigquery.Client(project=project)
    resource = bigquery.Dataset(f'{project}.{dataset}')
    resource.location = location
    client.create_dataset(resource)
    warehouse = Warehouse(client, project, dataset, location)
    table = dict(mysql_table='products', bigquery_table='products', primary_key='product_id', incremental=False, schema=schema('products'))
    frame = validate(pd.DataFrame(dict(product_id=[1], category=['Books'], price=['19.95'], updated_at=['2026-01-01'])), 'products')
    try:
        warehouse.bootstrap([table])
        yield client, warehouse, table, frame
    finally:
        client.delete_dataset(resource, delete_contents=True, not_found_ok=True)
        client.close()


def count(client, warehouse):
    return list(client.query(f'SELECT COUNT(*) n FROM `{warehouse.prefix}.products`', location=warehouse.location).result())[0].n


def test_real_snapshot_replay_and_empty_source(cloud):
    client, warehouse, table, frame = cloud
    for _ in range(2):
        claim = warehouse.acquire('products')
        warehouse.publish(frame, table, claim, 1)
        assert count(client, warehouse) == 1
    claim = warehouse.acquire('products')
    warehouse.publish(frame.iloc[:0], table, claim, 1)
    assert count(client, warehouse) == 0


def test_real_expired_owner_cannot_publish(cloud):
    client, warehouse, table, frame = cloud
    old = warehouse.acquire('products')
    client.query(f"UPDATE `{warehouse.prefix}.etl_state_v2` SET lease_until = TIMESTAMP '1970-01-01' WHERE table_name = 'products'", location=warehouse.location).result()
    current = warehouse.acquire('products')
    with pytest.raises(BadRequest):
        warehouse.publish(frame, table, old, 1)
    assert count(client, warehouse) == 0
    warehouse.publish(frame, table, current, 1)
    assert count(client, warehouse) == 1


def test_real_overlapping_acquisitions_have_one_owner(cloud):
    _, warehouse, _, _ = cloud
    def acquire():
        try:
            return warehouse.acquire('products')
        except BadRequest:
            return None
    with ThreadPoolExecutor(max_workers=2) as workers:
        claims = list(workers.map(lambda _: acquire(), range(2)))
    assert sum(claim is not None for claim in claims) == 1


def test_real_upsert_replay(cloud):
    client, warehouse, table, frame = cloud
    table = {**table, 'incremental': True}
    for _ in range(2):
        warehouse.publish(frame, table, warehouse.acquire('products'), 1)
    assert count(client, warehouse) == 1
    frame.loc[0, 'category'] = 'Updated'
    warehouse.publish(frame, table, warehouse.acquire('products'), 1)
    row = list(client.query(f'SELECT category FROM `{warehouse.prefix}.products`', location=warehouse.location).result())[0]
    assert row.category == 'Updated'
