"""DataFrame writeTo (DataFrameWriterV2) tests for HMS-backed catalog tables.

Regression tests for https://github.com/lakehq/sail/issues/2055, where
string literals in the write input were misresolved as column identifiers
for tables in a Hive Metastore catalog.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.mark.parametrize("format_", ["iceberg", "delta"])
def test_write_to_create(hms_spark: SparkSession, hms_database: str, format_: str) -> None:
    table_fqn = f"{hms_database}.write_to_create"

    df = hms_spark.sql("SELECT 1 AS id, 'hello' AS text")
    df.writeTo(table_fqn).using(format_).create()
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]


@pytest.mark.parametrize("format_", ["iceberg", "delta"])
def test_write_to_append(hms_spark: SparkSession, hms_database: str, format_: str) -> None:
    table_fqn = f"{hms_database}.write_to_append"

    df = hms_spark.sql("SELECT 1 AS id, 'hello' AS text")
    df.writeTo(table_fqn).using(format_).create()

    df = hms_spark.sql("SELECT 2 AS id, 'world' AS text")
    df.writeTo(table_fqn).append()
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row.id, row.text) for row in rows] == [(1, "hello"), (2, "world")]


@pytest.mark.xfail(
    reason="Hive Metastore catalog does not support REPLACE yet",
    strict=True,
)
def test_write_to_create_or_replace_iceberg(hms_spark: SparkSession, hms_database: str) -> None:
    table_fqn = f"{hms_database}.write_to_replace"

    df = hms_spark.sql("SELECT 1 AS id, 'hello' AS text")
    df.writeTo(table_fqn).using("iceberg").createOrReplace()
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]

    # Replace the now-existing table to cover the replace path.
    df = hms_spark.sql("SELECT 2 AS id, 'bye' AS text")
    df.writeTo(table_fqn).using("iceberg").createOrReplace()
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row.id, row.text) for row in rows] == [(2, "bye")]
