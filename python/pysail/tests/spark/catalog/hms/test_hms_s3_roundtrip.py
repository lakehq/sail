# ruff: noqa: S608, TC002
"""HMS interop tests for tables under S3 database locations."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pysail.tests.spark.catalog.hms.conftest import (
    _describe_extended_properties,
    _reference_catalog_table,
    _scala_option_to_string,
)

_S3_WAREHOUSE_PREFIX = "s3://hms-warehouse"


def _assert_sail_describes_s3_table(
    spark: SparkSession,
    table_fqn: str,
    location_prefix: str,
) -> None:
    props = _describe_extended_properties(spark, table_fqn)
    assert props["Type"] == "EXTERNAL"
    assert props["Provider"].lower() == "parquet"
    assert props["Location"].startswith(location_prefix)


def _assert_reference_describes_s3_table(
    jvm_spark: SparkSession,
    database: str,
    table: str,
    location_prefix: str,
    table_type: str,
) -> None:
    spark_table = _reference_catalog_table(jvm_spark, database, table)
    storage = spark_table.storage()
    assert spark_table.tableType().name() == table_type
    assert _scala_option_to_string(spark_table.provider()) == "parquet"
    location = _scala_option_to_string(storage.locationUri())
    if location is not None:
        assert location.startswith(location_prefix)


def test_s3_spark_creates_sail_reads_parquet(
    jvm_spark: SparkSession,
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "spark_parquet"
    table_fqn = f"{hms_s3_database}.{table}"
    location_prefix = f"{_S3_WAREHOUSE_PREFIX}/{hms_s3_database}"

    jvm_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    jvm_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'spark'), (2, 's3')")

    _assert_sail_describes_s3_table(spark, table_fqn, location_prefix)
    rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "spark"), (2, "s3")]


def test_s3_sail_creates_spark_reads_external_parquet(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "sail_external_parquet"
    table_fqn = f"{hms_s3_database}.{table}"
    location_prefix = f"{_S3_WAREHOUSE_PREFIX}/{hms_s3_database}"

    spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    spark.sql(f"INSERT INTO {table_fqn} VALUES (10, 'sail'), (11, 'spark')")

    _assert_reference_describes_s3_table(
        jvm_spark,
        hms_s3_database,
        table,
        location_prefix,
        "EXTERNAL",
    )
    rows = jvm_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(10, "sail"), (11, "spark")]
