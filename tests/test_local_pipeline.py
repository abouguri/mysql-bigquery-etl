import os
from unittest.mock import Mock

import pytest

from config.config import Config
from etl.local_warehouse import LocalWarehouse
from etl_pipeline import ETLPipeline


def test_cloud_backend_requires_explicit_opt_in(monkeypatch):
    monkeypatch.setenv('ETL_ALLOW_CLOUD', '0')
    with pytest.raises(ValueError, match='Cloud execution disabled'):
        Config(backend='bigquery').validate()


@pytest.mark.mysql
@pytest.mark.skipif(os.getenv('RUN_MYSQL_TESTS') != '1', reason='opt-in local MySQL')
def test_complete_mysql_to_local_pipeline_without_cloud(tmp_path, monkeypatch):
    path = tmp_path / 'warehouse.sqlite'
    monkeypatch.setenv('ETL_LOCAL_PATH', str(path))
    monkeypatch.setenv('ETL_ALLOW_CLOUD', '0')
    monkeypatch.delenv('GCP_PROJECT_ID', raising=False)
    forbidden = Mock(side_effect=AssertionError('Cloud must never be initialized'))
    monkeypatch.setattr('etl_pipeline.bigquery.Client', forbidden)
    assert ETLPipeline(backend='local').run_pipeline()
    assert ETLPipeline(backend='local').run_pipeline(reconcile=True)
    report = LocalWarehouse(path).report()
    assert report['counts'] == {'orders': 3, 'products': 2, 'users': 2}
    assert report['net_revenue_usd'] == '309.85'
    forbidden.assert_not_called()
