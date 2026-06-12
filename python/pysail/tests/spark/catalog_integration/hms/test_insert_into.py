# ruff: noqa: S608
"""INSERT INTO tests for HMS-backed catalog tables.

Regression tests for https://github.com/lakehq/sail/issues/2055, where
string literals in INSERT INTO ... VALUES were misresolved as column
identifiers for tables in a Hive Metastore catalog.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.mark.parametrize("format_", ["iceberg", "delta"])
def test_insert_into_values_lakehouse(hms_spark: SparkSession, hms_database: str, format_: str) -> None:
    table_fqn = f"{hms_database}.insert_{format_}"
    # Plain `CREATE TABLE ... USING <lakehouse format>` is not supported by
    # the HMS catalog yet, so the table is created via CTAS, which goes
    # through the write path where the table format writer produces the
    # table metadata.
    hms_spark.sql(f"CREATE TABLE {table_fqn} USING {format_} AS SELECT 0 AS id, 'init' AS text")

    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'hello')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(0, "init"), (1, "hello")]

    # Append to the now-existing table to cover the use-existing write path.
    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'world')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(0, "init"), (1, "hello"), (2, "world")]


@pytest.mark.xfail(
    reason="Hive Metastore catalog does not support plain CREATE TABLE USING ICEBERG yet",
    strict=True,
)
def test_plain_create_table_insert_iceberg(hms_spark: SparkSession, hms_database: str) -> None:
    table_fqn = f"{hms_database}.plain_create_iceberg"
    hms_spark.sql(f"CREATE TABLE {table_fqn} (id INT, text STRING) USING iceberg")

    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'hello')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]


def test_insert_into_values_parquet(hms_spark: SparkSession, hms_database: str) -> None:
    table_fqn = f"{hms_database}.insert_parquet"
    hms_spark.sql(f"CREATE TABLE {table_fqn} (id INT, text STRING) USING parquet")

    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'hello')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]

    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'world')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello"), (2, "world")]
