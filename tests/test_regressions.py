"""Failure-path checks for stable BigQuery job identity."""
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import Forbidden, NotFound

from etl.warehouse import Warehouse


def test_permission_error_fails_closed():
    client = Mock()
    client.get_job.side_effect = Forbidden('denied')
    warehouse = Warehouse(client, 'fixture-project', 'fixture', 'US')
    with pytest.raises(Forbidden):
        warehouse.acquire('users')
    client.query.assert_not_called()


def test_mutation_waits_for_completion():
    client = Mock()
    client.get_job.side_effect = NotFound('new job')
    warehouse = Warehouse(client, 'fixture-project', 'fixture', 'US')
    warehouse.query('SELECT 1', 'abc', 'check')
    client.query.return_value.result.assert_called_once_with(timeout=60)


def test_unknown_job_outcome_reuses_identity(monkeypatch):
    client = Mock()
    existing = Mock()
    existing.result.return_value = ['committed']
    client.get_job.side_effect = [NotFound('new job'), existing]
    client.query.return_value.result.side_effect = TimeoutError()
    monkeypatch.setattr('etl.warehouse.time.sleep', lambda _: None)
    warehouse = Warehouse(client, 'fixture-project', 'fixture', 'US')
    assert warehouse.query('mutation', 'abc', 'publish') == ['committed']
    client.query.assert_called_once()
    assert [call.args[0] for call in client.get_job.call_args_list] == ['etl_abc_publish'] * 2
