# ruff: noqa: S608, PLR2004
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


_POLICY_WRITE_SQL = {
    "delete": "DELETE FROM {target} WHERE id=1",
    "update": "UPDATE {target} SET name='updated' WHERE id=1",
    "merge_matched": ("MERGE INTO {target} t USING {source} s ON t.id=s.id WHEN MATCHED THEN UPDATE SET name=s.name"),
    "merge_insert": (
        "MERGE INTO {target} t USING {source} s ON t.id=s.id "
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
    ),
    "metadata_delete": "DELETE FROM {target}",
    "truncate": "TRUNCATE TABLE {target}",
    "noop_delete": "DELETE FROM {target} WHERE false",
    "insert": "INSERT INTO {target} VALUES (3, 'new')",
    "overwrite": "INSERT OVERWRITE TABLE {target} SELECT 3 AS id, 'new' AS name",
}


@pytest.mark.parametrize(
    ("operation", "mode", "version", "policy"),
    [
        pytest.param(operation, mode, 2, "false", id=f"disabled-v2-{mode}-{operation}")
        for mode in ["copy-on-write", "merge-on-read"]
        for operation in ["delete", "update", "merge_matched", "merge_insert", "metadata_delete", "truncate"]
    ]
    + [
        pytest.param("update", "merge-on-read", 3, "false", id="disabled-v3-dv-update"),
        pytest.param("noop_delete", "copy-on-write", 2, "false", id="disabled-noop-delete"),
        pytest.param("insert", "copy-on-write", 2, "false", id="disabled-insert-control"),
        pytest.param("overwrite", "copy-on-write", 2, "false", id="disabled-overwrite-control"),
        pytest.param("update", "copy-on-write", 2, None, id="absent-cow-update"),
        pytest.param("update", "merge-on-read", 2, "true", id="enabled-v2-position-update"),
        pytest.param("update", "merge-on-read", 3, "true", id="enabled-v3-dv-update"),
        pytest.param("merge_insert", "merge-on-read", 2, None, id="absent-insert-only-merge"),
        pytest.param("metadata_delete", "copy-on-write", 2, None, id="absent-metadata-delete"),
        pytest.param("truncate", "merge-on-read", 2, "true", id="enabled-truncate"),
    ],
)
def test_iceberg_writes_validate_metadata_lock_policy_before_staging(
    spark, jvm_spark, hms_s3_database, hms_s3_env, operation, mode, version, policy
):
    # Import lazily: the DDL tests reuse this module's HMS pointer helpers.
    from pysail.tests._dev.spark.catalog.hms.test_lakehouse_ddl import _iceberg_metadata, _s3_client

    table = "write_lock_policy"
    name = f"{hms_s3_database}.{table}"
    reference = f"iceberg.{name}"
    jvm_spark.sql(
        f"CREATE TABLE {reference} (id INT, name STRING) USING iceberg TBLPROPERTIES ("
        f"'format-version'='{version}', 'write.delete.mode'='{mode}', "
        f"'write.update.mode'='{mode}', 'write.merge.mode'='{mode}')"
    )
    jvm_spark.sql("SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'").coalesce(1).writeTo(reference).append()
    # Both rows share a file, so selective mutations cannot become file-level metadata deletes.
    files = jvm_spark.sql(f"SELECT record_count FROM {reference}.files WHERE content=0").collect()
    assert [row.record_count for row in files] == [2]
    if policy is not None:
        jvm_spark.sql(f"ALTER TABLE {reference} SET TBLPROPERTIES ('engine.hive.lock-enabled'='{policy}')")
    # Official Iceberg supports false via conditional no-lock publication; Sail rejects that protocol.
    # Remove only the HMS mirror, never rewrite immutable Iceberg metadata.
    properties = _catalog_properties(jvm_spark, hms_s3_database, table)
    properties.pop("engine.hive.lock-enabled", None)
    _set_catalog_properties(jvm_spark, hms_s3_database, table, properties)
    assert "engine.hive.lock-enabled" not in _catalog_properties(jvm_spark, hms_s3_database, table)
    pointer, metadata = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    assert metadata["properties"].get("engine.hive.lock-enabled") == policy
    assert metadata["format-version"] == version
    source = f"{hms_s3_database}.lock_policy_source"
    source_reference = f"iceberg.{source}"
    source_before = None
    locations = [urllib.parse.urlparse(metadata["location"])]
    if operation.startswith("merge"):
        jvm_spark.sql(f"CREATE TABLE {source_reference} (id INT, name STRING) USING iceberg")
        jvm_spark.sql(f"INSERT INTO {source_reference} VALUES (1, 'updated'), (3, 'new')")
        _, source_metadata = _iceberg_metadata(jvm_spark, hms_s3_database, "lock_policy_source", hms_s3_env)
        locations.append(urllib.parse.urlparse(source_metadata["location"]))
        source_before = _reference_catalog_table(jvm_spark, hms_s3_database, "lock_policy_source")
    client = _s3_client(hms_s3_env)

    def inventory():
        return {
            (location.netloc, item["Key"]): client.get_object(Bucket=location.netloc, Key=item["Key"])["Body"].read()
            for location in locations
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=location.netloc, Prefix=location.path.lstrip("/").rstrip("/") + "/"
            )
            for item in page.get("Contents", [])
        }

    before = inventory()
    catalog_before = _reference_catalog_table(jvm_spark, hms_s3_database, table)
    statement = _POLICY_WRITE_SQL[operation].format(target=name, source=source)
    if policy == "false":
        with pytest.raises(Exception, match=r"engine\.hive\.lock-enabled"):
            spark.sql(statement).collect()
        assert _metadata_location(jvm_spark, hms_s3_database, table) == pointer
        assert _reference_catalog_table(jvm_spark, hms_s3_database, table).equals(catalog_before)
        assert inventory() == before
        assert _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env) == (pointer, metadata)
        expected = [(1, "a"), (2, "b")]
    else:
        spark.sql(statement).collect()
        assert _metadata_location(jvm_spark, hms_s3_database, table) != pointer
        expected = {
            "update": [(1, "updated"), (2, "b")],
            "merge_insert": [(1, "a"), (2, "b"), (3, "new")],
            "metadata_delete": [],
            "truncate": [],
        }[operation]
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
    assert [tuple(row) for row in jvm_spark.sql(f"SELECT * FROM {reference} ORDER BY id").collect()] == expected
    if policy != "false" and operation == "update" and mode == "merge-on-read":
        delete_files = jvm_spark.sql(f"SELECT file_format FROM {reference}.files WHERE content=1").collect()
        assert [row.file_format for row in delete_files] == ["PUFFIN" if version == 3 else "PARQUET"]
    if source_before is not None:
        assert _reference_catalog_table(jvm_spark, hms_s3_database, "lock_policy_source").equals(source_before)
        jvm_spark.sql(f"REFRESH TABLE {source_reference}")
        assert [tuple(row) for row in spark.table(source).orderBy("id").collect()] == [(1, "updated"), (3, "new")]
        assert [tuple(row) for row in jvm_spark.sql(f"SELECT * FROM {source_reference} ORDER BY id").collect()] == [
            (1, "updated"),
            (3, "new"),
        ]
        source_location = locations[1]
        source_prefix = source_location.path.lstrip("/").rstrip("/") + "/"
        source_bytes = {
            key: value
            for key, value in before.items()
            if key[0] == source_location.netloc and key[1].startswith(source_prefix)
        }
        after = inventory()
        assert {
            key: value
            for key, value in after.items()
            if key[0] == source_location.netloc and key[1].startswith(source_prefix)
        } == source_bytes


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
