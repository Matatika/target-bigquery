import os
import uuid

import dotenv

dotenv.load_dotenv()

_DATASET_MARKER = "_pytest_"
"""Required substring before pytest_sessionfinish will delete a dataset."""

# Give this session its own throwaway dataset so concurrent sessions can't race on
# shared table names. Must happen at module level (not a fixture): test_core.py reads
# BQ_DATASET at its own module-import time, which happens right after this runs.
if os.environ.get("BQ_PROJECT") and os.environ.get("BQ_DATASET"):
    os.environ["BQ_DATASET"] = f"{os.environ['BQ_DATASET']}{_DATASET_MARKER}{uuid.uuid4().hex[:8]}"


def pytest_sessionfinish(session, exitstatus) -> None:
    """Drop the session's throwaway dataset, if BigQuery was touched at all."""
    dataset = os.environ.get("BQ_DATASET", "")
    if _DATASET_MARKER not in dataset:
        # Safety backstop: never delete a dataset we didn't generate ourselves above.
        return
    if not (os.environ.get("BQ_PROJECT") and os.environ.get("BQ_CREDS")):
        return

    from google.cloud import bigquery

    from target_bigquery.core import BigQueryCredentials, bigquery_client_factory

    credentials = BigQueryCredentials(json=os.environ["BQ_CREDS"], project=os.environ["BQ_PROJECT"])
    client = bigquery_client_factory(credentials)
    client.delete_dataset(
        bigquery.DatasetReference(os.environ["BQ_PROJECT"], dataset),
        delete_contents=True,
        not_found_ok=True,
    )
