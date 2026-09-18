import json
import logging
from unittest.mock import Mock

import pytest

from etl.observability import JsonFormatter, event


def test_production_requires_verified_tls(pipeline, monkeypatch):
    monkeypatch.setenv('ENVIRONMENT', 'production')
    pipeline.config.environment = 'production'
    monkeypatch.delenv('MYSQL_SSL_CA', raising=False)
    with pytest.raises(ValueError, match='MYSQL_SSL_CA'):
        pipeline.config.validate()


def test_production_does_not_need_dataset_creation_permission(pipeline):
    pipeline.config.environment = 'production'
    pipeline.bq_client = Mock()
    pipeline.bq_client.get_dataset.return_value.location = 'US'
    pipeline.ensure_dataset()
    pipeline.bq_client.create_dataset.assert_not_called()


def test_tls_verifies_certificate_and_hostname(pipeline, monkeypatch):
    monkeypatch.setenv('MYSQL_SSL_CA', '/fixture/ca.pem')
    engine = Mock()
    engine.connect.return_value.__enter__ = Mock(return_value=Mock())
    engine.connect.return_value.__exit__ = Mock(return_value=False)
    factory = Mock(return_value=engine)
    monkeypatch.setattr('etl_pipeline.create_engine', factory)
    pipeline.connect_mysql()
    assert factory.call_args.kwargs['connect_args'] == dict(
        ssl_ca='/fixture/ca.pem', ssl_verify_cert=True, ssl_verify_identity=True
    )


def test_structured_failure_does_not_include_exception_payload():
    logger = Mock()
    error = ValueError('sensitive-record-payload')
    event(logger, 'pipeline_failed', severity=logging.ERROR, error_type=type(error).__name__)
    args, kwargs = logger.log.call_args
    record = logging.LogRecord('etl', args[0], '', 0, args[1], (), None)
    record.event = kwargs['extra']['event']
    record.fields = kwargs['extra']['fields']
    output = JsonFormatter().format(record)
    assert json.loads(output)['event'] == 'pipeline_failed'
    assert 'sensitive-record-payload' not in output
