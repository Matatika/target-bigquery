"""Tests standard target features using the built-in SDK tests library."""

import os

import pytest
from singer_sdk.testing import get_target_test_class

from target_bigquery.target import TargetBigQuery

StandardTargetTests = get_target_test_class(
    target_class=TargetBigQuery,
    config={
        "credentials_json": os.environ["BQ_CREDS"],
        "project": os.environ["BQ_PROJECT"],
        "dataset": os.environ["BQ_DATASET"],
    },
    # Default True re-reads .env on every Target construction, clobbering the
    # per-session BQ_DATASET override in conftest.py.
    parse_env_config=False,
)


class TestTargetBigQuery(StandardTargetTests):
    """Standard Target Tests."""

    @pytest.mark.skip(
        reason="FIXED strategy repacks records into an opaque `data` blob, so key "
        "properties are never top-level keys -- see BaseBigQuerySink._singer_validate_message"
    )
    def test_target_record_missing_key_property(self, *args, **kwargs):
        pass
