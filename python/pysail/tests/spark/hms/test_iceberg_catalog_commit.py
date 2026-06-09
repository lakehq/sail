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


def _set_catalog_properties(
    reference_spark: SparkSession, database: str, table: str, properties: dict[str, str]
) -> None:
    jvm = reference_spark._jvm  # noqa: SLF001
    hadoop_conf = reference_spark._jsc.hadoopConfiguration()  # noqa: SLF001
    hive_conf = jvm.org.apache.hadoop.hive.conf.HiveConf()
    hive_conf.set("hive.metastore.uris", hadoop_conf.get("hive.metastore.uris"))
    client = jvm.org.apache.hadoop.hive.metastore.HiveMetaStoreClient(hive_conf)
    try:
        hms_table = client.getTable(database, table)
        parameters = hms_table.getParameters()
        parameters.clear()
        for key, value in properties.items():
            parameters.put(key, value)
        client.alter_table(database, table, hms_table)
    finally:
        client.close()


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
        CREATE TABLE {table_fqn}
        USING ICEBERG
        AS SELECT 1 AS id, 'a' AS name
        """
    )
    first_location = _metadata_location(reference_spark_s3, hms_s3_database, table)
    assert not _metadata_filename(first_location).startswith("v")

    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'b'), (3, 'c')")
    second_location = _metadata_location(reference_spark_s3, hms_s3_database, table)
    assert second_location != first_location
    assert not _metadata_filename(second_location).startswith("v")

    rows = hms_s3_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a"), (2, "b"), (3, "c")]


def test_sail_reads_and_appends_hms_iceberg_table_with_jvm_style_marker(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "iceberg_jvm_marker"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(
        f"""
        CREATE TABLE {table_fqn}
        USING ICEBERG
        AS SELECT 1 AS id, 'a' AS name
        """
    )
    first_location = _metadata_location(reference_spark_s3, hms_s3_database, table)

    properties = _catalog_properties(reference_spark_s3, hms_s3_database, table)
    properties.pop("spark.sql.sources.provider", None)
    properties.pop("table_type", None)
    properties["TABLE_TYPE"] = "ICEBERG"
    _set_catalog_properties(reference_spark_s3, hms_s3_database, table, properties)
    properties = _catalog_properties(reference_spark_s3, hms_s3_database, table)
    assert properties.get("TABLE_TYPE") == "ICEBERG"
    assert "spark.sql.sources.provider" not in properties
    assert "table_type" not in properties

    rows = hms_s3_spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a")]

    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'b')")
    second_location = _metadata_location(reference_spark_s3, hms_s3_database, table)
    assert second_location != first_location
    assert not _metadata_filename(second_location).startswith("v")

    rows = hms_s3_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a"), (2, "b")]


def test_hms_rejects_stale_iceberg_metadata_location_update(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "iceberg_stale_commit"
    table_fqn = f"{hms_s3_database}.{table}"

    hms_s3_spark.sql(
        f"""
        CREATE TABLE {table_fqn}
        USING ICEBERG
        AS SELECT 1 AS id, 'a' AS name
        """
    )
    current_location = _metadata_location(reference_spark_s3, hms_s3_database, table)

    with pytest.raises(Exception, match="base metadata location"):
        hms_s3_spark.sql(
            f"""
            ALTER TABLE {table_fqn}
            SET TBLPROPERTIES (
              'metadata-location' = '{current_location}.new',
              'previous_metadata_location' = '{current_location}.stale'
            )
            """
        )

    assert _metadata_location(reference_spark_s3, hms_s3_database, table) == current_location


def test_hms_plain_iceberg_create_records_metadata_location(
    hms_s3_spark: SparkSession,
    reference_spark_s3: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "iceberg_plain_create"
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
    first_location = _metadata_location(reference_spark_s3, hms_s3_database, table)
    assert not _metadata_filename(first_location).startswith("v")

    rows = hms_s3_spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
    assert rows == []
    rows = reference_spark_s3.sql(f"SELECT id, name FROM {table_fqn}").collect()
    assert rows == []

    hms_s3_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")
    second_location = _metadata_location(reference_spark_s3, hms_s3_database, table)
    assert second_location != first_location
    assert not _metadata_filename(second_location).startswith("v")

    rows = reference_spark_s3.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a")]
