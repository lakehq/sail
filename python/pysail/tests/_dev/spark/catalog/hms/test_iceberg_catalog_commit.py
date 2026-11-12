# ruff: noqa: S608
"""Iceberg commit tests for HMS-backed catalog tables."""

from __future__ import annotations

import re
import urllib.parse
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import pytest

from pysail.tests._dev.spark.catalog.hms.conftest import _reference_catalog_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


UUID_METADATA_FILE_PATTERN = re.compile(
    r"^\d{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json$"
)


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


def _assert_uuid_metadata_location(location: str, expected_version: int | None = None) -> None:
    filename = _metadata_filename(location)
    assert UUID_METADATA_FILE_PATTERN.match(filename), filename
    if expected_version is not None:
        assert filename.startswith(f"{expected_version:05}-"), filename


def test_expiration_rejects_hms_without_mutation(spark, jvm_spark, hms_s3_database, hms_s3_env):
    from pysail.tests._dev.spark.catalog.hms.test_lakehouse_ddl import _iceberg_metadata, _s3_client

    table = "unsupported_expiration"
    name = f"{hms_s3_database}.{table}"
    spark.sql(f"CREATE TABLE {name} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    spark.sql(f"INSERT OVERWRITE {name} VALUES (2)")
    pointer, metadata = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    before = _reference_catalog_table(jvm_spark, hms_s3_database, table)
    location = urllib.parse.urlparse(metadata["location"])
    client = _s3_client(hms_s3_env)

    def inventory():
        return {
            item["Key"]: client.get_object(Bucket=location.netloc, Key=item["Key"])["Body"].read()
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=location.netloc, Prefix=location.path.lstrip("/").rstrip("/") + "/"
            )
            for item in page.get("Contents", [])
        }

    files = inventory()
    with pytest.raises(Exception, match="pointer-authoritative registrations"):
        spark.sql(f"CALL system.expire_snapshots('{name}', older_than => TIMESTAMP '2099-01-01')").collect()
    assert _reference_catalog_table(jvm_spark, hms_s3_database, table).equals(before)
    assert _metadata_location(jvm_spark, hms_s3_database, table) == pointer
    assert inventory() == files
    assert [row.id for row in spark.table(name).collect()] == [2]
    assert [row.id for row in jvm_spark.table(f"iceberg.{name}").collect()] == [2]


def test_sail_insert_advances_hms_iceberg_metadata_location(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "iceberg_commit"
    table_fqn = f"{hms_s3_database}.{table}"

    spark.sql(
        f"""
        CREATE TABLE {table_fqn}
        USING ICEBERG
        AS SELECT 1 AS id, 'a' AS name
        """
    )
    first_location = _metadata_location(jvm_spark, hms_s3_database, table)
    _assert_uuid_metadata_location(first_location, 0)

    spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'b'), (3, 'c')")
    second_location = _metadata_location(jvm_spark, hms_s3_database, table)
    assert second_location != first_location
    _assert_uuid_metadata_location(second_location, 1)

    rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a"), (2, "b"), (3, "c")]


def test_sail_reads_and_appends_hms_iceberg_table_with_jvm_style_marker(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "iceberg_jvm_marker"
    table_fqn = f"{hms_s3_database}.{table}"

    spark.sql(
        f"""
        CREATE TABLE {table_fqn}
        USING ICEBERG
        AS SELECT 1 AS id, 'a' AS name
        """
    )
    first_location = _metadata_location(jvm_spark, hms_s3_database, table)

    properties = _catalog_properties(jvm_spark, hms_s3_database, table)
    properties.pop("spark.sql.sources.provider", None)
    properties.pop("table_type", None)
    properties["TABLE_TYPE"] = "ICEBERG"
    _set_catalog_properties(jvm_spark, hms_s3_database, table, properties)
    properties = _catalog_properties(jvm_spark, hms_s3_database, table)
    assert properties.get("TABLE_TYPE") == "ICEBERG"
    assert "spark.sql.sources.provider" not in properties
    assert "table_type" not in properties

    rows = spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a")]

    spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'b')")
    second_location = _metadata_location(jvm_spark, hms_s3_database, table)
    assert second_location != first_location
    _assert_uuid_metadata_location(second_location)

    rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a"), (2, "b")]


def test_hms_rejects_stale_iceberg_metadata_location_update(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "iceberg_stale_commit"
    table_fqn = f"{hms_s3_database}.{table}"

    spark.sql(
        f"""
        CREATE TABLE {table_fqn}
        USING ICEBERG
        AS SELECT 1 AS id, 'a' AS name
        """
    )
    current_location = _metadata_location(jvm_spark, hms_s3_database, table)

    with pytest.raises(Exception, match="catalog-managed Iceberg tables"):
        spark.sql(
            f"""
            ALTER TABLE {table_fqn}
            SET TBLPROPERTIES (
              'metadata-location' = '{current_location}.new',
              'previous_metadata_location' = '{current_location}.stale'
            )
            """
        )

    assert _metadata_location(jvm_spark, hms_s3_database, table) == current_location


def test_hms_plain_iceberg_create_records_metadata_location(
    spark: SparkSession,
    jvm_spark: SparkSession,
    hms_s3_database: str,
) -> None:
    table = "iceberg_plain_create"
    table_fqn = f"{hms_s3_database}.{table}"

    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING ICEBERG
        """
    )
    first_location = _metadata_location(jvm_spark, hms_s3_database, table)
    _assert_uuid_metadata_location(first_location, 0)

    rows = spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
    assert rows == []

    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")
    second_location = _metadata_location(jvm_spark, hms_s3_database, table)
    assert second_location != first_location
    _assert_uuid_metadata_location(second_location, 1)

    rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.name) for row in rows] == [(1, "a")]
