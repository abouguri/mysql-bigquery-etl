import os
import pytest
from config.config import Config


def test_config_defaults(monkeypatch):
    monkeypatch.delenv('GCP_PROJECT_ID', raising=False)
    monkeypatch.delenv('BIGQUERY_DATASET', raising=False)
    monkeypatch.delenv('BIGQUERY_LOCATION', raising=False)
    monkeypatch.delenv('MYSQL_HOST', raising=False)
    monkeypatch.delenv('MYSQL_PORT', raising=False)
    monkeypatch.delenv('MYSQL_USER', raising=False)
    monkeypatch.delenv('MYSQL_PASSWORD', raising=False)
    monkeypatch.delenv('MYSQL_DATABASE', raising=False)

    config = Config()
    assert config.environment == 'development'
    assert config.bigquery_config['dataset_id'] == 'mysql_etl'
    assert config.bigquery_config['location'] == 'US'
    assert config.mysql_config['port'] == 3306
    assert config.mysql_config['host'] is None
    assert config.mysql_config['user'] is None
    assert config.mysql_config['password'] is None
    assert config.mysql_config['database'] is None


def test_etl_tables_structure():
    config = Config()
    tables = config.etl_tables
    assert isinstance(tables, list)
    for table in tables:
        assert 'mysql_table' in table
        assert 'bigquery_table' in table
        assert 'primary_key' in table
        assert 'incremental' in table
        assert 'transformations' in table
        assert isinstance(table['transformations'], list)
