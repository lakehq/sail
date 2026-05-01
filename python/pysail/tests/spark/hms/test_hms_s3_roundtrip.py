"""HMS interop tests for managed tables under S3 database locations."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

pytestmark = pytest.mark.hms_interop

_S3_WAREHOUSE_PREFIX = "s3://hms-warehouse"


def _describe_extended_properties(spark: SparkSession, table_fqn: str) -> dict[str, str]:
    rows = spark.sql(f"DESCRIBE EXTENDED {table_fqn}").collect()
    props: dict[str, str] = {}
    for row in rows:
        key = (row.col_name or "").strip()
        value = (row.data_type or "").strip()
        if key:
            props[key] = value
    return props


def _assert_sail_describes_s3_managed_table(
    hms_s3_spark: SparkSession,
    table_fqn: str,
    location_prefix: str,
) -> None:
    props = _describe_extended_properties(hms_s3_spark, table_fqn)
    assert props["Type"] == "MANAGED"
    assert props["Provider"].lower() == "parquet"
    assert props["Location"].startswith(location_prefix)


def _reference_catalog_table(reference_spark_s3: SparkSession, database: str, table: str):
    return (
        reference_spark_s3._jsparkSession.sessionState()
        .catalog()
        .externalCatalog()
        .getTable(database, table)
    )


def _scala_option_to_string(option) -> str | None:
    if option.isDefined():
        value = option.get()
        return value.toString() if hasattr(value, "toString") else str(value)
    return None


def _assert_reference_describes_s3_managed_table(
    reference_spark_s3: SparkSession,
    database: str,
    table: str,
    location_prefix: str,
) -> None:
    spark_table = _reference_catalog_table(reference_spark_s3, database, table)
    storage = spark_table.storage()
    assert spark_table.tableType().name() == "MANAGED"
    assert _scala_option_to_string(spark_table.provider()) == "parquet"
    location = _scala_option_to_string(storage.locationUri())
    assert location is not None
    assert location.startswith(location_prefix)


def test_s3_spark_creates_sail_reads_managed_parquet(
    reference_spark_s3: SparkSession,
    hms_s3_spark: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "spark_managed_parquet"
    table_fqn = f"{hms_s3_database}.{table}"
    location_prefix = f"{_S3_WAREHOUSE_PREFIX}/{hms_s3_database}"

    reference_spark_s3.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    reference_spark_s3.sql(f"INSERT INTO {table_fqn} VALUES (1, 'spark'), (2, 's3')")

    _assert_sail_describes_s3_managed_table(hms_s3_spark, table_fqn, location_prefix)
    rows = hms_s3_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "spark"), (2, "s3")]


def test_s3_sail_creates_spark_reads_managed_parquet(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "sail_managed_parquet"
    table_fqn = f"{hms_s3_database}.{table}"
    location_prefix = f"{_S3_WAREHOUSE_PREFIX}/{hms_s3_database}"

    hms_s3_spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING PARQUET")
    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (10, 'sail'), (11, 'spark')")

    _assert_reference_describes_s3_managed_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        location_prefix,
    )
    rows = reference_spark_s3.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(10, "sail"), (11, "spark")]


def test_s3_sail_partitioned_managed_table_recovers_hms_partitions(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "sail_partitioned_parquet"
    table_fqn = f"{hms_s3_database}.{table}"
    location_prefix = f"{_S3_WAREHOUSE_PREFIX}/{hms_s3_database}"

    hms_s3_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (id INT, name STRING, region STRING)
        USING PARQUET
        PARTITIONED BY (region)
        """
    )
    hms_s3_spark.sql(
        f"""
        INSERT INTO {table_fqn}
        VALUES (1, 'north row', 'north'), (2, 'slash row', 'a/b')
        """
    )

    _assert_reference_describes_s3_managed_table(
        reference_spark_s3,
        hms_s3_database,
        table,
        location_prefix,
    )
    partitions = {
        row.partition for row in reference_spark_s3.sql(f"SHOW PARTITIONS {table_fqn}").collect()
    }
    assert partitions == {"region=a%2Fb", "region=north"}
    rows = reference_spark_s3.sql(
        f"SELECT id, region FROM {table_fqn} ORDER BY id"
    ).collect()
    assert [(row.id, row.region) for row in rows] == [(1, "north"), (2, "a/b")]
