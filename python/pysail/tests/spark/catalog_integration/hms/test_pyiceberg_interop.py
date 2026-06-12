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


def _create_pyiceberg_table(catalog, database: str, table: str, **kwargs):
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, NestedField, StringType

    schema = kwargs.pop(
        "schema",
        Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
            NestedField(field_id=2, name="text", field_type=StringType(), required=False),
        ),
    )
    return catalog.create_table(
        f"{database}.{table}",
        schema=schema,
        location=f"s3://{_HMS_S3_BUCKET}/{database}/{table}",
        **kwargs,
    )


def _pyiceberg_rows(catalog, database: str, table: str) -> list[tuple[int, str]]:
    data = catalog.load_table(f"{database}.{table}").scan().to_arrow()
    rows = sorted(zip(data["id"].to_pylist(), data["text"].to_pylist(), strict=False))
    return [(int(i), t) for i, t in rows]


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
    # Verify through pyiceberg as well so we know the commit is visible to
    # the foreign engine, not just to Sail.
    assert _pyiceberg_rows(pyiceberg_hive_catalog, hms_database, table) == [(1, "hello")]


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
    assert _pyiceberg_rows(pyiceberg_hive_catalog, hms_database, table) == [(1, "hello")]


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
