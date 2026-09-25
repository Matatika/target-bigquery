"""Reproduces the Test-85100 SIT failure: a MERGE fails with

    google.api_core.exceptions.BadRequest: 400 ... Query error:
    Name properties__hs_date_entered_customer not found inside source at [1:6753]

Root cause: BaseBigQuerySink.merge_table() (core.py) builds its UPDATE/INSERT column
list from `self.merge_target`'s schema -- the *already existing*, physical BigQuery
table, which only ever grows columns (see Denormalized.update_schema()). The `source`
side of the MERGE is `self.table`, a brand-new temp table created fresh every run from
*this run's* schema only (BaseBigQuerySink._create_overwrite_table()). If a column that
exists on the target (e.g. a HubSpot property that appeared in a past sync) is absent
from the current run's schema (e.g. tap-hubspot no longer returns that property), the
temp table never gets that column, and the generated MERGE statement references
`source.<that column>`, which BigQuery rejects.

This test reproduces the defect without touching real BigQuery: it fakes just enough
of a sink to call the real `merge_table()`, and its mock `bigquery_client.query()`
enforces BigQuery's actual invariant (every referenced `source.<col>` must exist on the
source table) so the same class of error surfaces deterministically and fast.
"""

import re

import pytest
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from target_bigquery.core import (
    BaseBigQuerySink,
    BigQueryTable,
    IngestionStrategy,
    SchemaResolverVersion,
)


class FakeQueryJob:
    def result(self):
        return None


class FakeBigQueryClient:
    """Stands in for google.cloud.bigquery.Client, enforcing that every
    `source.`col`` referenced in the submitted SQL exists in `source_columns`,
    the way real BigQuery validates a MERGE statement's USING clause."""

    def __init__(self, source_columns: set[str]):
        self.source_columns = source_columns
        self.queries: list[str] = []

    def query(self, query: str) -> FakeQueryJob:
        self.queries.append(query)
        for match in re.finditer(r"source\.`([^`]+)`", query):
            column = match.group(1)
            if column not in self.source_columns:
                raise BadRequest(
                    f"400 GET https://bigquery.googleapis.com/bigquery/v2/projects/p/queries/fake"
                    f"?maxResults=0&location=US&prettyPrint=false: Query error: "
                    f"Name {column} not found inside source at [1:1]"
                )
        return FakeQueryJob()


def _table(name: str, schema: list[bigquery.SchemaField]) -> BigQueryTable:
    table = BigQueryTable(
        name=name,
        dataset="import_runner__tap_hubspot",
        project="staging-www-271511",
        jsonschema={},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        schema_resolver_version=SchemaResolverVersion.V1,
    )
    # Simulate an already-materialized BigQuery table (skip the network call
    # `as_table()` would otherwise need).
    table._table = bigquery.Table(table.as_ref(), schema=schema)
    return table


class _ConcreteSink(BaseBigQuerySink):
    """BaseBigQuerySink is abstract; merge_table() itself doesn't need the
    parts that make it so, so a no-op concrete subclass is enough to
    instantiate one without running __init__."""

    def process_batch(self, context):
        del context
        raise NotImplementedError

    @staticmethod
    def worker_cls_factory(worker_executor_cls, config):
        del worker_executor_cls, config
        raise NotImplementedError


def _sink_for_merge(merge_target: BigQueryTable, source: BigQueryTable) -> BaseBigQuerySink:
    """A bare sink instance with just what merge_table() touches, built without
    running __init__ (which would need real BigQuery credentials)."""
    sink = object.__new__(_ConcreteSink)
    sink.merge_target = merge_target
    sink.table = source
    sink._key_properties = ["id"]  # `key_properties` is a read-only property
    sink._config = {}  # `config` is a read-only property; `_is_dedupe_before_upsert_candidate` reads config.get(...)
    return sink


def test_merge_fails_when_target_has_column_absent_from_this_runs_source():
    """Two syncs of `companies`: the first run sees `hs_date_entered_customer`,
    growing the target table's schema. The second run's schema no longer includes
    it (e.g. HubSpot stopped returning that lifecycle-stage property), so this
    run's freshly created temp table lacks the column -- but the merge still
    tries to reference it because it iterates the *target's* (wider) schema."""

    # The physical `companies` table, as left behind by a prior run that still
    # had `properties__hs_date_entered_customer` in its schema.
    merge_target = _table(
        "companies",
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("properties__name", "STRING"),
            bigquery.SchemaField("properties__hs_date_entered_customer", "TIMESTAMP"),
        ],
    )

    # This run's freshly created temp table: built from *this run's* schema only,
    # which no longer has that property.
    source = _table(
        "companies__20260922044811__0a62e027",
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("properties__name", "STRING"),
        ],
    )

    sink = _sink_for_merge(merge_target, source)
    client = FakeBigQueryClient(source_columns={"id", "properties__name"})

    with pytest.raises(
        BadRequest,
        match="properties__hs_date_entered_customer not found inside source",
    ):
        sink.merge_table(bigquery_client=client)


def test_merge_succeeds_when_source_and_target_columns_match():
    """Control case: no schema drift, so the generated MERGE only references
    columns that exist on both sides and succeeds."""
    merge_target = _table(
        "companies",
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("properties__name", "STRING"),
        ],
    )
    source = _table(
        "companies__20260922050000__deadbeef",
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("properties__name", "STRING"),
        ],
    )

    sink = _sink_for_merge(merge_target, source)
    client = FakeBigQueryClient(source_columns={"id", "properties__name"})

    sink.merge_table(bigquery_client=client)  # should not raise

    assert client.queries  # sanity: the MERGE was actually issued


# --- Fix verification ------------------------------------------------------
#
# The fix: BigQueryTable.get_resolved_schema()/as_table()/create_table() now accept
# `extra_fields`/`extra_schema`, unioned into the created schema by field name, and
# BaseBigQuerySink._create_overwrite_table() passes the merge target's already-known
# columns as that extra schema. So a freshly created temp table is always a superset
# of the merge target's columns, even when this run's own schema shrank.


def test_get_resolved_schema_unions_extra_fields_by_name():
    table = BigQueryTable(
        name="companies",
        dataset="d",
        project="p",
        jsonschema={
            "type": "object",
            "properties": {
                "id": {"type": ["string", "null"]},
                "properties__name": {"type": ["string", "null"]},
            },
        },
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        schema_resolver_version=SchemaResolverVersion.V1,
    )
    extra_fields = [
        bigquery.SchemaField("properties__name", "STRING"),  # already present -> not duplicated
        bigquery.SchemaField(
            "properties__hs_date_entered_customer", "TIMESTAMP"
        ),  # missing -> appended
    ]

    resolved = table.get_resolved_schema(extra_fields=extra_fields)
    names = [field.name for field in resolved]

    assert names.count("properties__name") == 1
    assert "properties__hs_date_entered_customer" in names


class FakeCreateTableClient:
    """Stands in for google.cloud.bigquery.Client for BigQueryTable.create_table():
    the dataset already exists, the (uniquely named) table never does."""

    def __init__(self):
        self.created: bigquery.Table | None = None

    def get_dataset(self, ref):
        return bigquery.Dataset(ref)

    def get_table(self, ref):
        raise NotFound("no such table")

    def create_table(self, table: bigquery.Table) -> bigquery.Table:
        self.created = table
        return table


def test_create_overwrite_table_widens_temp_table_and_merge_then_succeeds():
    """End-to-end (offline) proof: the temp table created for a run whose schema
    shrank still ends up with the merge target's columns, so the MERGE that
    previously failed in test_merge_fails_when_target_has_column_absent_from_this_runs_source
    now succeeds."""
    merge_target = _table(
        "companies",
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("properties__name", "STRING"),
            bigquery.SchemaField("properties__hs_date_entered_customer", "TIMESTAMP"),
        ],
    )

    # This run's schema no longer mentions `hs_date_entered_customer`.
    temp_table = BigQueryTable(
        name="companies__20260922050000__deadbeef",
        dataset="import_runner__tap_hubspot",
        project="staging-www-271511",
        jsonschema={
            "type": "object",
            "properties": {
                "id": {"type": ["string", "null"]},
                "properties__name": {"type": ["string", "null"]},
            },
        },
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        schema_resolver_version=SchemaResolverVersion.V1,
    )

    create_client = FakeCreateTableClient()
    extra_schema = merge_target.as_table().schema  # what _create_overwrite_table() now passes
    temp_table.create_table(
        create_client,
        extra_schema=extra_schema,
        table={},
        dataset={},
    )

    assert create_client.created is not None
    created_names = {field.name for field in create_client.created.schema}
    assert created_names == {"id", "properties__name", "properties__hs_date_entered_customer"}

    sink = _sink_for_merge(merge_target, temp_table)
    merge_client = FakeBigQueryClient(source_columns=created_names)

    sink.merge_table(bigquery_client=merge_client)  # should not raise anymore
