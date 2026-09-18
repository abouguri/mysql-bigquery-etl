from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from config.config import Config, identifier


def test_runtime_injected_secrets_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("MYSQL_PASSWORD", "a@b:/?#")
    assert Config().mysql_config["password"] == "a@b:/?#"


@pytest.mark.parametrize("name", ["users; DROP TABLE users", "users`", "a.b", "", None])
def test_untrusted_identifier_rejected(name):
    with pytest.raises(ValueError, match="identifier"):
        identifier(name)


def test_validation_does_not_expose_values(pipeline, monkeypatch):
    monkeypatch.delenv("MYSQL_HOST")
    with pytest.raises(ValueError, match="MYSQL_HOST") as error:
        pipeline.config.validate()
    assert "fixture-only" not in str(error.value)


@pytest.mark.parametrize("value", ["0", "65536", "secret-value"])
def test_invalid_port(pipeline, monkeypatch, value):
    monkeypatch.setenv("MYSQL_PORT", value)
    with pytest.raises(ValueError, match="MYSQL_PORT") as error:
        pipeline.config.validate()
    assert "secret-value" not in str(error.value)


def test_metadata_bootstrap_order(pipeline):
    calls = []
    pipeline.connect_mysql = Mock()
    pipeline.connect_bigquery = Mock()
    pipeline.ensure_dataset = lambda: calls.append("dataset")
    pipeline.create_metadata_table = lambda: calls.append("metadata")
    assert pipeline.run_pipeline() is False
    assert calls == ["dataset", "metadata"]


def test_password_is_encoded_by_url_builder(pipeline, monkeypatch):
    monkeypatch.setenv("MYSQL_PASSWORD", "a@b:/?#")
    engine = Mock()
    engine.connect.return_value.__enter__ = Mock(return_value=Mock())
    engine.connect.return_value.__exit__ = Mock(return_value=False)
    factory = Mock(return_value=engine)
    monkeypatch.setattr("etl_pipeline.create_engine", factory)
    pipeline.connect_mysql()
    url = factory.call_args.args[0]
    assert url.password == "a@b:/?#"
    assert url.host == "localhost"
