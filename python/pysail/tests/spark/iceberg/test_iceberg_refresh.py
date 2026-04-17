"""Tests for the refresh catalog operations against the Iceberg catalog provider."""

import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from pysail.testing.spark.utils.sql import escape_sql_string_literal


def test_iceberg_refresh_table_via_sql(spark, sql_catalog):
    """`REFRESH TABLE` succeeds against an Iceberg-backed table."""
    table_name = "iceberg_refresh_sql"

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=StringType(), required=False),
    )

    table = sql_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=schema,
    )

    try:
        table.append(pa.Table.from_pandas(pd.DataFrame([{"id": 1, "value": "a"}])))
        table_path = table.location()
        spark.sql(f"CREATE TABLE iceberg_refresh USING iceberg LOCATION '{escape_sql_string_literal(table_path)}'")
        try:
            # REFRESH TABLE should be a successful no-op for Iceberg tables.
            spark.sql("REFRESH TABLE iceberg_refresh")
            # After refresh the data must remain readable.
            rows = spark.sql("SELECT id, value FROM iceberg_refresh ORDER BY id").collect()
            assert [(r.id, r.value) for r in rows] == [(1, "a")]
        finally:
            spark.sql("DROP TABLE IF EXISTS iceberg_refresh")
    finally:
        sql_catalog.drop_table(f"default.{table_name}")


def test_iceberg_catalog_refresh_table_api(spark, sql_catalog):
    """`spark.catalog.refreshTable` returns None for an Iceberg-backed table."""
    table_name = "iceberg_refresh_api"

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    )

    table = sql_catalog.create_table(
        identifier=f"default.{table_name}",
        schema=schema,
    )

    try:
        table.append(pa.Table.from_pandas(pd.DataFrame([{"id": 10}])))
        table_path = table.location()
        spark.sql(f"CREATE TABLE iceberg_refresh_api USING iceberg LOCATION '{escape_sql_string_literal(table_path)}'")
        try:
            assert spark.catalog.refreshTable("iceberg_refresh_api") is None
            assert spark.catalog.refreshByPath(table_path) is None
        finally:
            spark.sql("DROP TABLE IF EXISTS iceberg_refresh_api")
    finally:
        sql_catalog.drop_table(f"default.{table_name}")
