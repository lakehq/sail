# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608
"""Table replacement (`REPLACE TABLE`, `saveAsTable` overwrite, `writeTo` replace) for Delta tables in Unity."""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING
from urllib.parse import quote, unquote

import pyarrow.parquet as pq
import pytest
import requests

from pysail.tests._dev.spark.catalog.unity.conftest import (
    _location_to_path,
    _qualified_table_name,
    _table_location,
    _unique_database,
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
    database = _unique_database("unity_replace_save_as_table")
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
    database = _unique_database("unity_replace_write_to")
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
    database = _unique_database("unity_replace_sql")
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
    database = _unique_database("unity_replace_external")
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


def test_sql_replace_drops_the_delta_properties_of_the_replaced_table(spark: SparkSession) -> None:
    database = _unique_database("unity_replace_properties")
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT) USING delta "
            "TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableChangeDataFeed' = 'true')"
        )
        location = _location_to_path(_table_location(spark, table))
        _, actions = _latest_commit(location)
        (metadata,) = _actions(actions, "metaData")
        assert metadata["configuration"]["delta.appendOnly"] == "true"
        assert metadata["configuration"]["delta.enableChangeDataFeed"] == "true"

        # The properties that the statement does not set are not kept, as in Delta Spark.
        # The table has no data, since an append-only table does not allow the removal.
        spark.sql(f"CREATE OR REPLACE TABLE {table} (id INT) USING delta")
        _, actions = _latest_commit(location)
        (metadata,) = _actions(actions, "metaData")
        assert "delta.appendOnly" not in metadata["configuration"]
        assert "delta.enableChangeDataFeed" not in metadata["configuration"]

        spark.sql(f"INSERT INTO {table} VALUES (3), (4)")
        spark.sql(f"DELETE FROM {table} WHERE id = 3")
        assert _rows(spark, table, "id") == [(4,)]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


# The properties with fixed values that Unity Catalog requires for managed Delta tables.
_UNITY_REQUIRED_PROPERTIES = {
    "delta.enableDeletionVectors": "true",
    "delta.checkpointPolicy": "v2",
    "delta.enableInCommitTimestamps": "true",
    "delta.checkpoint.writeStatsAsStruct": "true",
    "delta.checkpoint.writeStatsAsJson": "true",
}


def _delta_api_table_properties(unity_rest_url: str, table: str) -> dict:
    """Loads the properties of the table with the Unity Catalog Delta REST API."""
    catalog_name, schema_name, name = _qualified_table_name(table).split(".", 2)
    response = requests.get(
        f"{unity_rest_url}/delta/v1/catalogs/{quote(catalog_name, safe='')}"
        f"/schemas/{quote(schema_name, safe='')}/tables/{quote(name, safe='')}",
        timeout=10,
    )
    response.raise_for_status()
    return response.json()["metadata"]["properties"]


def test_sql_replace_keeps_the_unity_required_properties(
    spark: SparkSession,
    unity_rest_url: str,
    unity_uses_delta_api: bool,  # noqa: FBT001
) -> None:
    if not unity_uses_delta_api:
        pytest.skip("the managed table properties are required by the Unity Catalog Delta REST API")
    database = f"unity_replace_required_{uuid.uuid4().hex[:8]}"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING delta TBLPROPERTIES ('team' = 'a')")
        location = _location_to_path(_table_location(spark, table))
        _, actions = _latest_commit(location)
        (metadata,) = _actions(actions, "metaData")
        assert _UNITY_REQUIRED_PROPERTIES.items() <= metadata["configuration"].items()

        # The statement keeps the required properties without changing the protocol.
        spark.sql(f"CREATE OR REPLACE TABLE {table} (id BIGINT) USING delta")
        _, actions = _latest_commit(location)
        assert _actions(actions, "protocol") == []
        (metadata,) = _actions(actions, "metaData")
        assert _UNITY_REQUIRED_PROPERTIES.items() <= metadata["configuration"].items()
        assert "team" not in metadata["configuration"]
        properties = _delta_api_table_properties(unity_rest_url, table)
        assert _UNITY_REQUIRED_PROPERTIES.items() <= properties.items()
        assert "team" not in properties

        # The statement keeps the required properties when it changes the protocol,
        # which the catalog validates against the managed table requirements.
        spark.sql(
            f"CREATE OR REPLACE TABLE {table} (id BIGINT) USING delta "
            "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        )
        _, actions = _latest_commit(location)
        (protocol,) = _actions(actions, "protocol")
        assert "changeDataFeed" in protocol["writerFeatures"]
        (metadata,) = _actions(actions, "metaData")
        assert _UNITY_REQUIRED_PROPERTIES.items() <= metadata["configuration"].items()
        properties = _delta_api_table_properties(unity_rest_url, table)
        assert _UNITY_REQUIRED_PROPERTIES.items() <= properties.items()
        assert properties["delta.enableChangeDataFeed"] == "true"

        # The required properties cannot be changed or removed, since the catalog
        # does not check them again unless the protocol changes.
        required = "requires the table property delta.checkpointPolicy=v2"
        with pytest.raises(Exception, match=required):
            spark.sql(
                f"CREATE OR REPLACE TABLE {table} (id BIGINT) USING delta "
                "TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')"
            )
        with pytest.raises(Exception, match=required):
            spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.checkpointPolicy' = 'classic')")
        with pytest.raises(Exception, match="requires the table property delta.enableDeletionVectors=true"):
            spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('delta.enableDeletionVectors')")
        spark.sql(
            f"CREATE OR REPLACE TABLE {table} (id BIGINT) USING delta TBLPROPERTIES ('delta.checkpointPolicy' = 'v2')"
        )
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        properties = _delta_api_table_properties(unity_rest_url, table)
        assert _UNITY_REQUIRED_PROPERTIES.items() <= properties.items()

        spark.sql(f"INSERT INTO {table} VALUES (1), (2)")
        spark.sql(f"DELETE FROM {table} WHERE id = 1")
        assert _rows(spark, table, "id") == [(2,)]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def _published_commits(location: Path) -> dict[int, list[dict]]:
    commits = {}
    for path in (location / "_delta_log").glob("[0-9]*.json"):
        with path.open(encoding="utf-8") as f:
            commits[int(path.stem)] = [json.loads(line) for line in f if line.strip()]
    return commits


def _row_ids(adds: list[dict]) -> list[int]:
    return sorted(add["baseRowId"] + offset for add in adds for offset in range(json.loads(add["stats"])["numRecords"]))


def test_sql_replace_enables_and_keeps_row_tracking(spark: SparkSession) -> None:
    database = _unique_database("unity_replace_row_tracking")
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING delta")
        location = _location_to_path(_table_location(spark, table))
        spark.sql(f"INSERT INTO {table} VALUES (1), (2), (3)")
        before, _ = _latest_commit(location)
        commits = _published_commits(location)
        configuration = [m for _, a in sorted(commits.items()) for m in _actions(a, "metaData")][-1]["configuration"]
        assert configuration.get("delta.enableRowTracking") != "true"
        # The rows of a table that supports row tracking already have row IDs.
        used = _row_ids([add for actions in commits.values() for add in _actions(actions, "add") if "baseRowId" in add])
        high_water_mark = max(used, default=-1)

        # A REPLACE enables row tracking, as in Delta Spark.
        spark.sql(f"REPLACE TABLE {table} (id BIGINT) USING delta TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
        spark.sql(f"INSERT INTO {table} VALUES (10), (11), (12)")
        assert _rows(spark, table, "id") == [(10,), (11,), (12,)]
        commits = _published_commits(location)
        protocols = [p for _, a in sorted(commits.items()) for p in _actions(a, "protocol")]
        assert {"rowTracking", "domainMetadata"} <= set(protocols[-1]["writerFeatures"])
        configuration = [m for _, a in sorted(commits.items()) for m in _actions(a, "metaData")][-1]["configuration"]
        assert configuration["delta.enableRowTracking"] == "true"
        row_id_column = configuration["delta.rowTracking.materializedRowIdColumnName"]
        assert row_id_column.startswith("_row-id-col-")
        new = {version: actions for version, actions in commits.items() if version > before}
        adds = [(version, add) for version, actions in new.items() for add in _actions(actions, "add")]
        assert _row_ids([add for _, add in adds]) == [high_water_mark + 1, high_water_mark + 2, high_water_mark + 3]
        assert all(add["defaultRowCommitVersion"] == version for version, add in adds)

        # A REPLACE of the table keeps row tracking, the names of the materialized columns,
        # and the row ID high-water mark.
        spark.sql(f"CREATE OR REPLACE TABLE {table} (id BIGINT) USING delta")
        spark.sql(f"INSERT INTO {table} VALUES (20)")
        version, actions = _latest_commit(location)
        (add,) = _actions(actions, "add")
        assert add["baseRowId"] == high_water_mark + 4
        assert add["defaultRowCommitVersion"] == version
        replaced = _published_commits(location)[version - 1]
        (metadata,) = _actions(replaced, "metaData")
        assert metadata["configuration"]["delta.enableRowTracking"] == "true"
        assert metadata["configuration"]["delta.rowTracking.materializedRowIdColumnName"] == row_id_column
        assert _rows(spark, table, "id") == [(20,)]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def test_replace_as_select_replaces_the_properties_in_the_data_commit(
    spark: SparkSession,
    unity_rest_url: str,
    unity_uses_delta_api: bool,  # noqa: FBT001
) -> None:
    database = _unique_database("unity_replace_as_select_properties")
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING delta COMMENT 'created' TBLPROPERTIES ('team' = 'a')")
        created = _unity_table_info(unity_rest_url, table)
        location = _location_to_path(_table_location(spark, table))
        spark.sql(f"INSERT INTO {table} VALUES (1), (2), (3)")
        version, _ = _latest_commit(location)

        # An overwrite with saveAsTable keeps the table definition, as the Delta V1 writer does.
        spark.createDataFrame([(4,)], schema="id BIGINT").write.format("delta").mode("overwrite").saveAsTable(table)
        overwritten_version, actions = _latest_commit(location)
        assert overwritten_version == version + 1
        assert _actions(actions, "metaData") == []
        assert _rows(spark, table, "id") == [(4,)]
        # The rows of a table that supports row tracking already have row IDs.
        commits = _published_commits(location)
        used = _row_ids([add for actions in commits.values() for add in _actions(actions, "add") if "baseRowId" in add])
        high_water_mark = max(used, default=-1)

        # The statement replaces the properties and the comment of the table, and enables
        # row tracking, in the commit that replaces the data, as Delta Spark does.
        spark.sql(
            f"REPLACE TABLE {table} USING delta COMMENT 'replaced' "
            "TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'owner-team' = 'b') "
            "AS SELECT id FROM range(10, 13)"
        )
        assert _rows(spark, table, "id") == [(10,), (11,), (12,)]
        replaced_version, actions = _latest_commit(location)
        assert replaced_version == overwritten_version + 1
        protocols = [p for _, a in sorted(_published_commits(location).items()) for p in _actions(a, "protocol")]
        assert {"rowTracking", "domainMetadata"} <= set(protocols[-1]["writerFeatures"])
        (metadata,) = _actions(actions, "metaData")
        assert metadata["description"] == "replaced"
        configuration = metadata["configuration"]
        assert configuration["delta.enableRowTracking"] == "true"
        assert configuration["delta.rowTracking.materializedRowIdColumnName"].startswith("_row-id-col-")
        assert configuration["owner-team"] == "b"
        assert "team" not in configuration
        # The table keeps its identity in the catalog and its protocol properties.
        assert configuration["io.unitycatalog.tableId"] == created["table_id"]
        assert configuration["delta.enableInCommitTimestamps"] == "true"
        assert _actions(actions, "remove")
        adds = _actions(actions, "add")
        assert _row_ids(adds) == [high_water_mark + 1, high_water_mark + 2, high_water_mark + 3]
        assert all(add["defaultRowCommitVersion"] == replaced_version for add in adds)
        if unity_uses_delta_api:
            properties = _delta_api_table_properties(unity_rest_url, table)
            assert properties["owner-team"] == "b"
            assert properties["delta.enableRowTracking"] == "true"
            assert "team" not in properties
            assert _UNITY_REQUIRED_PROPERTIES.items() <= properties.items()
            assert _unity_table_info(unity_rest_url, table)["comment"] == "replaced"

        # A replacement without properties keeps row tracking and drops the other properties.
        spark.createDataFrame([(20,)], schema="id BIGINT").writeTo(table).using("delta").createOrReplace()
        assert _rows(spark, table, "id") == [(20,)]
        _, actions = _latest_commit(location)
        (metadata,) = _actions(actions, "metaData")
        assert metadata.get("description") is None
        assert metadata["configuration"]["delta.enableRowTracking"] == "true"
        assert "owner-team" not in metadata["configuration"]
        (add,) = _actions(actions, "add")
        assert add["baseRowId"] == high_water_mark + 4

        if unity_uses_delta_api:
            with pytest.raises(Exception, match="requires the table property delta.checkpointPolicy=v2"):
                spark.sql(
                    f"CREATE OR REPLACE TABLE {table} USING delta "
                    "TBLPROPERTIES ('delta.checkpointPolicy' = 'classic') AS SELECT CAST(30 AS BIGINT) AS id"
                )
            assert _rows(spark, table, "id") == [(20,)]

        replaced = _unity_table_info(unity_rest_url, table)
        assert replaced["table_id"] == created["table_id"]
        assert replaced["storage_location"] == created["storage_location"]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def _column_mapping(fields: list[dict]) -> dict[str, tuple[str, int, dict]]:
    """Returns the physical name, the column ID and the nested mapping of each column."""
    mapping = {}
    for field in fields:
        metadata = field.get("metadata") or {}
        nested = field["type"].get("fields", []) if isinstance(field["type"], dict) else []
        mapping[field["name"]] = (
            metadata.get("delta.columnMapping.physicalName"),
            metadata.get("delta.columnMapping.id"),
            _column_mapping(nested),
        )
    return mapping


def test_replace_as_select_applies_the_column_mapping_mode(spark: SparkSession) -> None:
    database = _unique_database("unity_replace_column_mapping")
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")

    def values(start: int) -> str:
        return (
            f"SELECT * FROM VALUES (CAST({start} AS INT), named_struct('x', {start * 10}, 'y', 'p'), 'a'), "
            f"(CAST({start + 1} AS INT), named_struct('x', {(start + 1) * 10}, 'y', 'q'), 'b') AS t(id, point, grp)"
        )

    try:
        spark.sql(f"CREATE TABLE {table} USING delta PARTITIONED BY (grp) AS {values(1)}")
        location = _location_to_path(_table_location(spark, table))
        used_ids: set[int] = set()
        used_names: set[str] = set()
        # The data of each replacement is written with the column mapping of the statement,
        # with new physical names and column IDs, also when the mode stays the same.
        for start, mode in [(3, "name"), (5, "name"), (7, None)]:
            properties = f"TBLPROPERTIES ('delta.columnMapping.mode' = '{mode}')" if mode else ""
            spark.sql(
                f"CREATE OR REPLACE TABLE {table} USING delta PARTITIONED BY (grp) {properties} AS {values(start)}"
            )
            rows = spark.sql(f"SELECT id, point.x, point.y, grp FROM {table} ORDER BY id").collect()
            assert [tuple(row) for row in rows] == [
                (start, start * 10, "p", "a"),
                (start + 1, (start + 1) * 10, "q", "b"),
            ]
            _, actions = _latest_commit(location)
            (metadata,) = _actions(actions, "metaData")
            mapping = _column_mapping(json.loads(metadata["schemaString"])["fields"])
            adds = _actions(actions, "add")
            assert len(adds) == 2  # noqa: PLR2004
            if mode is None:
                assert not any(key.startswith("delta.columnMapping.") for key in metadata["configuration"])
                assert all(physical is None for physical, _, _ in mapping.values())
                data_columns = {"id": "id", "point": "point"}
                nested_columns = ["x", "y"]
            else:
                assert metadata["configuration"]["delta.columnMapping.mode"] == mode
                columns = [*mapping.values(), *mapping["point"][2].values()]
                names = {physical for physical, _, _ in columns}
                ids = {column_id for _, column_id, _ in columns}
                assert all(isinstance(column_id, int) for column_id in ids)
                assert names.isdisjoint(used_names | set(mapping) | {"x", "y"}), (names, used_names)
                assert min(ids) > max(used_ids, default=0), (ids, used_ids)
                assert int(metadata["configuration"]["delta.columnMapping.maxColumnId"]) == max(ids)
                used_ids |= ids
                used_names |= names
                data_columns = {"id": mapping["id"][0], "point": mapping["point"][0]}
                nested_columns = [mapping["point"][2][name][0] for name in ["x", "y"]]
                assert all(set(add["partitionValues"]) == {mapping["grp"][0]} for add in adds)
            for add in adds:
                schema = pq.read_schema(location / unquote(add["path"]))
                assert schema.names == list(data_columns.values()), add["path"]
                assert [field.name for field in schema.field(data_columns["point"]).type] == nested_columns
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")
