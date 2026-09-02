# Copyright (c) 2023 Alex Butler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
"""ADBC connectivity for Arrow BATCH ingestion (encoding.format == "arrow").

``pyarrow`` and ``adbc-driver-manager`` are regular (required) dependencies of this
package -- both are pure-wheel and pip-installable. The native BigQuery ADBC driver itself
is *not* pip-installable, though, and must be installed separately via ``dbc install
bigquery`` (the CLI that ships with ``adbc-driver-manager`` - see
https://docs.adbc-drivers.org/drivers/bigquery/). Arrow BATCH mode fails fast with an
actionable error (see ``require_arrow_support``) if that native driver isn't present.

Nothing in this module is imported eagerly by the rest of the target; it is only touched
when a BATCH message with ``encoding.format == "arrow"`` is actually received.
"""

import json
from functools import lru_cache
from typing import TYPE_CHECKING

import pyarrow as pa
from adbc_driver_manager import AdbcDatabase, dbapi

if TYPE_CHECKING:
    from target_bigquery.core import BigQueryCredentials


class ArrowSupportError(RuntimeError):
    """Raised when Arrow BATCH mode is requested but Arrow/ADBC support is unavailable."""


def require_arrow_support() -> None:
    """Eagerly validate Arrow/ADBC support is usable.

    Raises ArrowSupportError with an actionable message if the native BigQuery ADBC driver
    can't be loaded. Called before any Arrow BATCH file is processed, so failures surface
    immediately rather than mid-batch.
    """
    try:
        db = AdbcDatabase(driver="bigquery")
    except Exception as exc:  # pylint: disable=broad-except
        raise ArrowSupportError(
            "Arrow BATCH mode is configured but the native BigQuery ADBC driver could not "
            "be loaded. Install it with `dbc install bigquery` (see "
            "https://docs.adbc-drivers.org/drivers/bigquery/). "
            f"Underlying error: {exc}"
        ) from exc
    else:
        db.close()


def _db_kwargs(credentials: "BigQueryCredentials", dataset: str, location: str | None) -> dict:
    """Map BigQueryCredentials + target config to the driver's `bigquery.*` db_kwargs.

    Reuses the same config keys as bigquery_client_factory (credentials_path/
    credentials_json/project) -- no new config keys introduced for ADBC connectivity.
    """
    kwargs: dict = {"bigquery.project_id": credentials.project, "bigquery.dataset_id": dataset}
    if location:
        kwargs["bigquery.location"] = location
    if credentials.path:
        kwargs["bigquery.auth_type"] = "json_credential_file"
        kwargs["bigquery.auth.credentials"] = str(credentials.path)
    elif credentials.json:
        kwargs["bigquery.auth_type"] = "json_credential_string"
        kwargs["bigquery.auth.credentials"] = (
            credentials.json if isinstance(credentials.json, str) else json.dumps(credentials.json)
        )
    else:
        kwargs["bigquery.auth_type"] = "app_default_credentials"
    return kwargs


@lru_cache
def connect(
    credentials: "BigQueryCredentials", dataset: str, location: str | None = None
) -> "dbapi.Connection":
    """Return a cached ADBC DBAPI connection for the given credentials/dataset/location."""
    return dbapi.connect(
        driver="bigquery",
        db_kwargs=_db_kwargs(credentials, dataset, location),
    )


def ingest(
    conn: "dbapi.Connection",
    table_name: str,
    dataset: str,
    table: pa.Table,
) -> int:
    """Bulk-append an Arrow table into an existing BigQuery table via ADBC.

    Always ``mode="append"``: table creation/DDL (schema, partitioning, clustering) stays
    owned by ``BigQueryTable.create_table``, never by ADBC.
    """
    with conn.cursor() as cur:
        return cur.adbc_ingest(table_name, table, mode="append", db_schema_name=dataset)
