import pytest


@pytest.fixture
def pipeline(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "fixture-project")
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_USER", "fixture")
    monkeypatch.setenv("MYSQL_PASSWORD", "fixture-only")
    monkeypatch.setenv("MYSQL_DATABASE", "commerce_fixture")
    from etl_pipeline import ETLPipeline
    return ETLPipeline()
