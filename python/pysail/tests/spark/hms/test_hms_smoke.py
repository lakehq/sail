# ruff: noqa: RUF002, S608
"""Minimal HMS smoke tests – Sail can connect to HMS, list databases, and
handle file://-specific behaviours (URI authority form)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

pytestmark = pytest.mark.catalog_integration


def test_hms_list_default_database(hms_s3_spark):
    """Sail connected to HMS should see at least the ``default`` database.

    This is the smallest meaningful smoke assertion: verify that the Sail
    server can talk to the HMS container and that ``SHOW DATABASES``
    returns the built-in ``default`` database that every Hive Metastore
    creates on first startup.
    """
    databases = [row.name for row in hms_s3_spark.sql("SHOW DATABASES").collect()]
    assert "default" in databases, f"HMS 'default' database not found; got: {databases}"


def _reference_catalog_table(reference_spark: SparkSession, database: str, table: str):
    return (
        reference_spark._jsparkSession.sessionState()  # noqa: SLF001
        .catalog()
        .externalCatalog()
        .getTable(database, table)
    )


def _scala_option_to_string(option) -> str | None:
    if option.isDefined():
        value = option.get()
        return value.toString() if hasattr(value, "toString") else str(value)
    return None


def test_hms_sail_creates_partitioned_parquet_with_file_uri_authority(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
    hms_warehouse_dir: Path,
) -> None:
    """Sail recovers partitions for local file URIs that include an authority."""
    table = "roundtrip_partitioned_file_uri_authority"
    table_fqn = f"{hms_s3_database}.{table}"
    location_path = hms_warehouse_dir / hms_s3_database / table
    location = f"file://localhost{location_path}"

    hms_s3_spark.sql(
        f"CREATE TABLE {table_fqn} (id INT, region STRING) USING PARQUET PARTITIONED BY (region) LOCATION '{location}'"
    )
    hms_s3_spark.sql(
        f"""
        INSERT INTO {table_fqn} VALUES
          (1, 'north'),
          (2, 'a/b')
        """
    )

    spark_table = _reference_catalog_table(reference_spark_s3, hms_s3_database, table)
    assert spark_table.tableType().name() == "EXTERNAL"
    assert _scala_option_to_string(spark_table.provider()) == "parquet"
    ref_partitions = {row.partition for row in reference_spark_s3.sql(f"SHOW PARTITIONS {table_fqn}").collect()}
    assert ref_partitions == {
        "region=a%2Fb",
        "region=north",
    }
    ref_rows = reference_spark_s3.sql(f"SELECT id, region FROM {table_fqn} ORDER BY id").collect()
    assert [(r.id, r.region) for r in ref_rows] == [(1, "north"), (2, "a/b")]
