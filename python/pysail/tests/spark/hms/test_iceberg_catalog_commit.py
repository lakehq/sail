# ruff: noqa: S608
"""Iceberg commit tests for HMS-backed catalog tables."""

from __future__ import annotations

import urllib.parse
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.hms.conftest import _reference_catalog_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


pytestmark = pytest.mark.catalog_integration


def _catalog_properties(reference_spark: SparkSession, database: str, table: str) -> dict[str, str]:
    scala_properties = _reference_catalog_table(reference_spark, database, table).properties()
    iterator = scala_properties.iterator()
    properties: dict[str, str] = {}
    while iterator.hasNext():
        entry = iterator.next()
        properties[str(entry.productElement(0))] = str(entry.productElement(1))
    return properties


def _metadata_location(reference_spark: SparkSession, database: str, table: str) -> str:
    properties = _catalog_properties(reference_spark, database, table)
    location = properties.get("metadata-location") or properties.get("metadata_location")
    assert location
    return location


def _metadata_filename(location: str) -> str:
    return PurePosixPath(urllib.parse.urlparse(location).path).name


def test_sail_insert_advances_hms_iceberg_metadata_location(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "iceberg_commit"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING ICEBERG
        """
    )

    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a'), (2, 'b')")
    first_location = _metadata_location(reference_spark_s3, hms_s3_database, table)
    assert not _metadata_filename(first_location).startswith("v")

    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (3, 'c')")
    second_location = _metadata_location(reference_spark_s3, hms_s3_database, table)
    assert second_location != first_location
    assert not _metadata_filename(second_location).startswith("v")

    rows = hms_s3_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a"), (2, "b"), (3, "c")]
