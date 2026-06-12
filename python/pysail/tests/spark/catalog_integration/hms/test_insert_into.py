# ruff: noqa: S608
"""INSERT INTO tests for HMS-backed catalog tables.

Regression tests for https://github.com/lakehq/sail/issues/2055, where
string literals in INSERT INTO ... VALUES were misresolved as column
identifiers for tables in a Hive Metastore catalog.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_insert_into_values_iceberg(hms_spark: SparkSession, hms_database: str) -> None:
    table_fqn = f"{hms_database}.insert_iceberg"
    hms_spark.sql(f"CREATE TABLE {table_fqn} (id INT, text STRING) USING iceberg")

    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'hello')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]

    # Append to the now-existing table to cover the use-existing write path.
    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'world')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello"), (2, "world")]


def test_insert_into_values_parquet(hms_spark: SparkSession, hms_database: str) -> None:
    table_fqn = f"{hms_database}.insert_parquet"
    hms_spark.sql(f"CREATE TABLE {table_fqn} (id INT, text STRING) USING parquet")

    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'hello')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]

    hms_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'world')")
    rows = hms_spark.sql(f"SELECT id, text FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello"), (2, "world")]
