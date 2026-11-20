# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608
"""Table replacement (`REPLACE TABLE`, `saveAsTable` overwrite, `writeTo` replace) for Delta tables in Unity."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from pysail.tests._dev.spark.catalog.unity.conftest import (
    _location_to_path,
    _table_location,
    _unity_table_info,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def _latest_commit(location: Path) -> tuple[int, list[dict]]:
    """Returns the version and the actions of the latest published commit."""
    published = sorted((location / "_delta_log").glob("[0-9]*.json"))
    assert published, f"no published Delta commit in {location}"
    with published[-1].open(encoding="utf-8") as f:
        return int(published[-1].stem), [json.loads(line) for line in f if line.strip()]


def _actions(actions: list[dict], kind: str) -> list[dict]:
    return [action[kind] for action in actions if kind in action]


def _columns(unity_rest_url: str, table: str) -> list[tuple[str, str]]:
    columns = sorted(_unity_table_info(unity_rest_url, table).get("columns") or [], key=lambda c: c["position"])
    return [(column["name"], column["type_name"]) for column in columns]


def _rows(spark: SparkSession, table: str, order_by: str) -> list[tuple]:
    return [tuple(row) for row in spark.sql(f"SELECT * FROM {table} ORDER BY {order_by}").collect()]


def test_save_as_table_overwrite_replaces_managed_delta_table(spark: SparkSession, unity_rest_url: str) -> None:
    database = "unity_replace_save_as_table"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        # The table does not exist, so it is created.
        first = spark.createDataFrame([(1, "one"), (2, "two")], schema="id INT, name STRING")
        first.write.format("delta").mode("overwrite").saveAsTable(table)
        created = _unity_table_info(unity_rest_url, table)
        assert created["table_type"] == "MANAGED"
        location = _location_to_path(_table_location(spark, table))
        assert _rows(spark, table, "id") == [(1, "one"), (2, "two")]
        version, _ = _latest_commit(location)

        # The existing table is replaced with one commit.
        second = spark.createDataFrame([(3, "three")], schema="id INT, name STRING")
        second.write.format("delta").mode("overwrite").saveAsTable(table)
        assert _rows(spark, table, "id") == [(3, "three")]
        replaced_version, actions = _latest_commit(location)
        assert replaced_version == version + 1
        assert _actions(actions, "remove")
        assert _actions(actions, "add")

        # The schema is replaced in the Delta log and in the catalog with the same commit.
        third = spark.createDataFrame([("a", 1.5, 7)], schema="key STRING, value DOUBLE, part INT")
        (
            third.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("part")
            .saveAsTable(table)
        )
        assert _rows(spark, table, "key") == [("a", 1.5, 7)]
        schema_version, actions = _latest_commit(location)
        assert schema_version == version + 2
        (metadata,) = _actions(actions, "metaData")
        assert metadata["partitionColumns"] == ["part"]
        assert _columns(unity_rest_url, table) == [("key", "STRING"), ("value", "DOUBLE"), ("part", "INT")]

        replaced = _unity_table_info(unity_rest_url, table)
        assert replaced["table_id"] == created["table_id"]
        assert replaced["storage_location"] == created["storage_location"]
        assert replaced["table_type"] == "MANAGED"

        # The replaced table accepts further writes.
        spark.sql(f"INSERT INTO {table} VALUES ('b', 2.5, 8)")
        assert _rows(spark, table, "key") == [("a", 1.5, 7), ("b", 2.5, 8)]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def test_write_to_replace_managed_delta_table(spark: SparkSession, unity_rest_url: str) -> None:
    database = "unity_replace_write_to"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        frame = spark.createDataFrame([(1, "one")], schema="id INT, name STRING")
        with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
            frame.writeTo(table).using("delta").replace()
        with pytest.raises(Exception, match=r"(?i)not found|does not exist|cannot be found"):
            spark.table(table).collect()

        frame.writeTo(table).using("delta").createOrReplace()
        created = _unity_table_info(unity_rest_url, table)
        location = _location_to_path(_table_location(spark, table))
        assert _rows(spark, table, "id") == [(1, "one")]
        version, _ = _latest_commit(location)

        spark.createDataFrame([(2, "two")], schema="id INT, name STRING").writeTo(table).using(
            "delta"
        ).createOrReplace()
        assert _rows(spark, table, "id") == [(2, "two")]
        spark.createDataFrame([(3, "three")], schema="id INT, name STRING").writeTo(table).using("delta").replace()
        assert _rows(spark, table, "id") == [(3, "three")]

        replaced_version, _ = _latest_commit(location)
        assert replaced_version == version + 2
        replaced = _unity_table_info(unity_rest_url, table)
        assert replaced["table_id"] == created["table_id"]
        assert replaced["storage_location"] == created["storage_location"]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def test_sql_replace_managed_delta_table(spark: SparkSession, unity_rest_url: str, unity_uses_delta_api: bool) -> None:  # noqa: FBT001
    database = "unity_replace_sql"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        with pytest.raises(Exception, match=r"(?i)not found|cannot be found"):
            spark.sql(f"REPLACE TABLE {table} (id INT) USING delta")
        with pytest.raises(Exception, match=r"(?i)not found|cannot be found"):
            spark.sql(f"REPLACE TABLE {table} USING delta AS SELECT 1 AS id")

        spark.sql(f"CREATE OR REPLACE TABLE {table} (id INT, name STRING) USING delta TBLPROPERTIES ('team'='a')")
        created = _unity_table_info(unity_rest_url, table)
        assert created["table_type"] == "MANAGED"
        location = _location_to_path(_table_location(spark, table))
        spark.sql(f"INSERT INTO {table} VALUES (1, 'one'), (2, 'two')")
        version, actions = _latest_commit(location)
        (data_file,) = _actions(actions, "add")

        # The statement without a query replaces the metadata and removes the data with one commit.
        spark.sql(
            f"CREATE OR REPLACE TABLE {table} (key STRING, value DOUBLE, part INT) USING delta "
            "PARTITIONED BY (part) COMMENT 'replaced' TBLPROPERTIES ('owner-team'='b')"
        )
        assert _rows(spark, table, "key") == []
        replaced_version, actions = _latest_commit(location)
        assert replaced_version == version + 1
        (metadata,) = _actions(actions, "metaData")
        assert [field["name"] for field in json.loads(metadata["schemaString"])["fields"]] == ["key", "value", "part"]
        assert metadata["partitionColumns"] == ["part"]
        assert metadata["description"] == "replaced"
        assert [action["path"] for action in _actions(actions, "remove")] == [data_file["path"]]
        # The table keeps its identity and its protocol properties, and loses its other properties.
        configuration = metadata["configuration"]
        assert configuration["io.unitycatalog.tableId"] == created["table_id"]
        assert configuration["delta.enableInCommitTimestamps"] == "true"
        assert configuration["owner-team"] == "b"
        assert "team" not in configuration

        replaced = _unity_table_info(unity_rest_url, table)
        assert replaced["table_id"] == created["table_id"]
        assert replaced["storage_location"] == created["storage_location"]
        assert _columns(unity_rest_url, table) == [("key", "STRING"), ("value", "DOUBLE"), ("part", "INT")]
        partition_columns = [c["name"] for c in replaced["columns"] if c.get("partition_index") is not None]
        assert partition_columns == ["part"]
        if unity_uses_delta_api:
            assert replaced["properties"]["owner-team"] == "b"
            assert "team" not in replaced["properties"]
            assert replaced["comment"] == "replaced"

        spark.sql(f"INSERT INTO {table} VALUES ('a', 1.5, 7)")
        assert _rows(spark, table, "key") == [("a", 1.5, 7)]

        spark.sql(f"REPLACE TABLE {table} (id BIGINT) USING delta")
        assert _rows(spark, table, "id") == []
        assert _columns(unity_rest_url, table) == [("id", "LONG")]
        _, actions = _latest_commit(location)
        (metadata,) = _actions(actions, "metaData")
        assert metadata["partitionColumns"] == []
        assert "owner-team" not in metadata["configuration"]
        assert len(_actions(actions, "remove")) == 1

        # The statements with a query replace the data.
        spark.sql(f"CREATE OR REPLACE TABLE {table} USING delta AS SELECT CAST(1 AS BIGINT) AS id")
        assert _rows(spark, table, "id") == [(1,)]
        spark.sql(f"REPLACE TABLE {table} USING delta AS SELECT CAST(id AS BIGINT) AS id FROM range(2, 4)")
        assert _rows(spark, table, "id") == [(2,), (3,)]
        final_version, _ = _latest_commit(location)
        assert final_version == replaced_version + 4
        assert _unity_table_info(unity_rest_url, table)["table_id"] == created["table_id"]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def test_replace_external_delta_table(spark: SparkSession, unity_rest_url: str, unity_storage_root: Path) -> None:
    database = "unity_replace_external"
    table = f"{database}.t"
    path = unity_storage_root / database
    location = path.as_uri()
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, name STRING) USING delta LOCATION '{location}'")
        spark.sql(f"INSERT INTO {table} VALUES (1, 'one')")
        created = _unity_table_info(unity_rest_url, table)
        assert created["table_type"] == "EXTERNAL"
        version, _ = _latest_commit(path)

        frame = spark.createDataFrame([(2, "two"), (3, "three")], schema="id INT, name STRING")
        frame.write.format("delta").mode("overwrite").saveAsTable(table)
        assert _rows(spark, table, "id") == [(2, "two"), (3, "three")]
        frame.limit(1).writeTo(table).using("delta").createOrReplace()
        assert spark.table(table).count() == 1
        replaced_version, _ = _latest_commit(path)
        assert replaced_version == version + 2

        spark.sql(f"CREATE OR REPLACE TABLE {table} (id INT, name STRING) USING delta LOCATION '{location}'")
        assert _rows(spark, table, "id") == []
        metadata_version, actions = _latest_commit(path)
        assert metadata_version == version + 3
        assert len(_actions(actions, "metaData")) == 1
        assert len(_actions(actions, "remove")) == 1

        with pytest.raises(Exception, match="different table location"):
            spark.sql(f"CREATE OR REPLACE TABLE {table} (id INT) USING delta LOCATION '{location}_other'")

        replaced = _unity_table_info(unity_rest_url, table)
        assert replaced["table_id"] == created["table_id"]
        assert replaced["table_type"] == "EXTERNAL"
        assert replaced["storage_location"] == created["storage_location"]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")
