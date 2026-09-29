# ruff: noqa: S608
"""DDL on managed Delta and external lakehouse registrations in Unity."""

import json

import pytest

from pysail.testing.spark.ddl import exercise_lakehouse_alter
from pysail.tests.spark.catalog.unity.conftest import (
    _location_to_path,
    _table_location,
    _unity_delta_commit_info,
    _unity_table_info,
)


def test_unity_managed_delta_ddl(spark, unity_rest_url):
    database = "managed_ddl"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING, part STRING) USING delta PARTITIONED BY (part) "
            "TBLPROPERTIES ('delta.enableTypeWidening'='true')"
        )
        exercise_lakehouse_alter(spark, table)
        info = _unity_table_info(unity_rest_url, table)
        assert next(c["type_name"] for c in info["columns"] if c["name"] == "id") == "LONG"
        assert "custom" not in info["properties"]
        # Hide the last published commit: ALTER must read the ratified catalog state.
        location = _location_to_path(_table_location(spark, table))
        published = sorted((location / "_delta_log").glob("[0-9]*.json"))[-1]
        version = int(published.stem)
        assert _unity_delta_commit_info(unity_rest_url, table, version)
        published.unlink()
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('after-unpublished'='true')")
        assert _unity_delta_commit_info(unity_rest_url, table, version + 1)
        info = _unity_table_info(unity_rest_url, table)
        assert info["properties"]["after-unpublished"] == "true"
        assert spark.table(table).count() == 3  # noqa: PLR2004
        with pytest.raises(Exception, match="coordination properties"):
            spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('io.unitycatalog.tableId')")
        assert json.loads(next(c["type_json"] for c in info["columns"] if c["name"] == "id"))["type"] == "long"
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


@pytest.mark.parametrize("fmt", ["delta"])
def test_unity_external_lakehouse_ddl(spark, unity_storage_root, fmt):
    database = f"external_ddl_{fmt}"
    table = f"{database}.t"
    location = (unity_storage_root / database).as_uri()
    properties = "'format-version'='3'" if fmt == "iceberg" else "'delta.enableTypeWidening'='true'"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING, part STRING) USING {fmt} "
            f"PARTITIONED BY (part) LOCATION '{location}' TBLPROPERTIES ({properties})"
        )
        exercise_lakehouse_alter(spark, table)
        spark.sql(f"DROP TABLE {table}")
        spark.sql(f"CREATE TABLE {table} USING {fmt} LOCATION '{location}'")
        assert spark.table(table).count() == 3  # noqa: PLR2004
        assert spark.table(table).schema["id"].dataType.simpleString() == "bigint"
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def test_unity_native_iceberg_create_rejected_before_storage(spark, unity_storage_root):
    database = "unsupported_iceberg_ddl"
    location = unity_storage_root / database
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        with pytest.raises(Exception, match="Iceberg REST"):
            spark.sql(f"CREATE TABLE {database}.t (id INT) USING iceberg LOCATION '{location.as_uri()}'")
        assert not location.exists()
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")
