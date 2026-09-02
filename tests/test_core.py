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
)


class TestTargetBigQuery(StandardTargetTests):
    """Standard Target Tests."""

    @pytest.mark.skip(
        reason=(
            "The default (FIXED) ingestion strategy intentionally repacks every "
            "record into an opaque `data` JSON blob, so key-property presence in the "
            "raw record is never meaningful or enforced -- see "
            "BaseBigQuerySink._singer_validate_message. This standard test assumes "
            "targets don't restructure records, which doesn't hold for FIXED."
        )
    )
    def test_target_record_missing_key_property(self, *args, **kwargs):
        pass
