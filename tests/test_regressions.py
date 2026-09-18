"""Document correctness gaps until their roadmap tasks are implemented."""
from unittest.mock import Mock

import pandas as pd
import pytest
from google.api_core.exceptions import Forbidden
from google.cloud import bigquery


def test_checkpoint_permission_error_fails_closed(pipeline):
    pipeline.bq_client = Mock()
    pipeline.bq_client.query.side_effect = Forbidden("denied")
    with pytest.raises(Forbidden):
        pipeline.get_last_processed_id("users")


def test_checkpoint_waits_for_completion(pipeline):
    pipeline.bq_client = Mock()
    pipeline.update_last_processed_id("users", 1)
    pipeline.bq_client.query.return_value.result.assert_called_once()


@pytest.mark.xfail(strict=True, reason="ETL-04: snapshots currently append")
def test_full_load_replaces_existing_rows(pipeline):
    pipeline.bq_client = Mock()
    pipeline.load_data(pd.DataFrame({"product_id": [1]}), pipeline.config.etl_tables[2])
    args = pipeline.bq_client.load_table_from_dataframe.call_args.kwargs
    assert args["job_config"].write_disposition != bigquery.WriteDisposition.WRITE_APPEND
