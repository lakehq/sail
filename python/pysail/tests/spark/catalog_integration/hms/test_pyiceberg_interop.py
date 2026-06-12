# ruff: noqa: S608
"""Sail writes to HMS Iceberg tables created by a foreign engine (pyiceberg).

The reporter of https://github.com/lakehq/sail/issues/2055 writes to tables
that were created in HMS by external pyiceberg-backed tooling, so these
tests create the tables with pyiceberg's Hive catalog and then write to
them with Sail.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_HMS_S3_BUCKET = "hms-warehouse"


def _create_pyiceberg_table(catalog, database: str, table: str, **kwargs) -> None:
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, NestedField, StringType
    from thrift.Thrift import TApplicationException

    schema = kwargs.pop(
        "schema",
        Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
            NestedField(field_id=2, name="text", field_type=StringType(), required=False),
        ),
    )
    try:
        catalog.create_table(
            f"{database}.{table}",
            schema=schema,
            location=f"s3://{_HMS_S3_BUCKET}/{database}/{table}",
            **kwargs,
        )
    except TApplicationException:
        # The thrift bindings bundled with pyiceberg predate HMS 4.x, which
        # removed legacy methods such as `get_table`. Table creation itself
        # succeeds; only the trailing table load fails, so the error is
        # ignored here. Reads through pyiceberg are not possible against
        # HMS 4.x, so the tests verify the written data through Sail.
        pass


def test_insert_into_pyiceberg_created_table(
    hms_spark: SparkSession,
    pyiceberg_hive_catalog,
    hms_database: str,
) -> None:
    table = "foreign_insert"
    _create_pyiceberg_table(pyiceberg_hive_catalog, hms_database, table)

    hms_spark.sql(f"INSERT INTO {hms_database}.{table} VALUES (1, 'hello')")

    rows = hms_spark.sql(f"SELECT id, text FROM {hms_database}.{table} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]


def test_write_to_append_pyiceberg_created_table(
    hms_spark: SparkSession,
    pyiceberg_hive_catalog,
    hms_database: str,
) -> None:
    table = "foreign_append"
    _create_pyiceberg_table(pyiceberg_hive_catalog, hms_database, table)

    df = hms_spark.sql("SELECT 1 AS id, 'hello' AS text")
    df.writeTo(f"{hms_database}.{table}").append()

    rows = hms_spark.sql(f"SELECT id, text FROM {hms_database}.{table} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hello")]


def test_insert_into_pyiceberg_table_with_string_default(
    hms_spark: SparkSession,
    pyiceberg_hive_catalog,
    hms_database: str,
) -> None:
    """An Iceberg v3 string column default must not be misread as a column
    reference when Sail resolves the write (the JSON-encoded default value
    `"hello"` must not turn into the identifier `hello`)."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, NestedField, StringType

    table = "foreign_default"
    try:
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
            NestedField(
                field_id=2,
                name="text",
                field_type=StringType(),
                required=False,
                initial_default="hello",
                write_default="hello",
            ),
        )
        _create_pyiceberg_table(
            pyiceberg_hive_catalog,
            hms_database,
            table,
            schema=schema,
            properties={"format-version": "3"},
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"pyiceberg cannot create a v3 table with column defaults: {exc}")

    hms_spark.sql(f"INSERT INTO {hms_database}.{table} VALUES (1, 'hi')")

    rows = hms_spark.sql(f"SELECT id, text FROM {hms_database}.{table} ORDER BY id").collect()
    assert [(row.id, row.text) for row in rows] == [(1, "hi")]
