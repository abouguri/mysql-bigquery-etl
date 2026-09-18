from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound

from etl.contracts import schema, validate
from etl.warehouse import Claim, Warehouse


@pytest.fixture
def publication():
    client = Mock()
    client.get_job.side_effect = NotFound('new job')
    client.get_table.return_value.schema = schema('products')
    warehouse = Warehouse(client, 'fixture-project', 'fixture', 'US')
    table = dict(mysql_table='products', bigquery_table='products', primary_key='product_id', incremental=False, schema=schema('products'))
    frame = validate(pd.DataFrame(dict(product_id=[1], category=['Books'], price=['19.95'], updated_at=['2026-01-01'])), 'products')
    return client, warehouse, table, frame


def test_snapshot_is_published_with_fence_and_checkpoint(publication):
    client, warehouse, table, frame = publication
    claim = Claim('products', 'a'*32, 1, 0)
    assert warehouse.publish(frame, table, claim, 1) == 1
    sql = client.query.call_args.args[0]
    assert sql.index('BEGIN TRANSACTION') < sql.index('UPDATE `fixture-project.fixture.etl_state_v2`')
    assert sql.index('DELETE FROM') < sql.index('COMMIT TRANSACTION')
    assert 'generation = @generation' in sql
    assert 'watermark_id = @lower_id' in sql
    assert 'lease_until > CURRENT_TIMESTAMP()' in sql
    assert 'ASSERT @@row_count = 1' in sql
    assert 'MERGE' not in sql
    assert client.load_table_from_dataframe.call_args.kwargs['job_config'].write_disposition == 'WRITE_APPEND'
    client.load_table_from_dataframe.return_value.result.assert_called_once()


def test_empty_snapshot_still_publishes(publication):
    client, warehouse, table, frame = publication
    assert warehouse.publish(frame.iloc[:0], table, Claim('products', 'b'*32, 2, 10), 10) == 0
    client.load_table_from_dataframe.assert_not_called()
    assert 'DELETE FROM' in client.query.call_args.args[0]


def test_duplicate_keys_do_not_reach_bigquery(publication):
    client, warehouse, table, frame = publication
    with pytest.raises(ValueError, match='unique'):
        warehouse.publish(pd.concat([frame, frame]), table, Claim('products', 'c'*32, 1, 0), 1)
    client.query.assert_not_called()
    client.load_table_from_dataframe.assert_not_called()


def test_failed_stage_never_publishes(publication):
    client, warehouse, table, frame = publication
    client.load_table_from_dataframe.return_value.result.side_effect = ValueError('failed load')
    with pytest.raises(ValueError, match='failed load'):
        warehouse.publish(frame, table, Claim('products', 'd'*32, 1, 0), 1)
    client.query.assert_not_called()


def test_schema_change_fails_before_load(publication):
    client, warehouse, table, frame = publication
    client.get_table.return_value.schema = []
    with pytest.raises(ValueError, match='migration'):
        warehouse.publish(frame, table, Claim('products', 'e'*32, 1, 0), 1)
    client.load_table_from_dataframe.assert_not_called()


def test_acquire_validates_state_cardinality():
    client = Mock()
    client.get_job.side_effect = NotFound('new')
    client.query.return_value.result.return_value = []
    warehouse = Warehouse(client, 'fixture-project', 'fixture', 'US')
    with pytest.raises(ValueError, match='outcome'):
        warehouse.acquire('products')


def test_acquire_reads_committed_generation():
    client = Mock()
    client.get_job.side_effect = NotFound('new')
    client.query.return_value.result.return_value = [SimpleNamespace(generation=2, watermark_id=10)]
    claim = Warehouse(client, 'fixture-project', 'fixture', 'US').acquire('users')
    assert (claim.generation, claim.lower_id) == (2, 10)


def test_pages_load_independently_with_stable_job_ids(publication):
    client, warehouse, table, frame = publication
    other = frame.copy()
    other['product_id'] = 2
    assert warehouse.publish_batches(iter([frame, other]), table, Claim('products', 'f'*32, 1, 0), 2) == 2
    ids = [call.kwargs['job_id'] for call in client.load_table_from_dataframe.call_args_list]
    assert ids == [f"etl_{'f'*32}_load_0", f"etl_{'f'*32}_load_1"]
    params = client.query.call_args.kwargs['job_config'].query_parameters
    assert next(p.value for p in params if p.name == 'rows') == 2


def test_later_page_failure_does_not_publish_partial_snapshot(publication):
    client, warehouse, table, frame = publication
    def pages():
        yield frame
        raise ValueError('page extraction failed')
    with pytest.raises(ValueError, match='extraction failed'):
        warehouse.publish_batches(pages(), table, Claim('products', 'f'*32, 1, 0), 2)
    client.load_table_from_dataframe.assert_called_once()
    client.query.assert_not_called()
