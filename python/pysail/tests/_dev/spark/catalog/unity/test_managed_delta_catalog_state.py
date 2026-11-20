# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608
"""Catalog state, time travel, and partial overwrites of Unity Catalog managed Delta tables."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from urllib.parse import quote

import pytest
import requests

from pysail.tests._dev.spark.catalog.unity.conftest import (
    _location_to_path,
    _qualified_table_name,
    _table_location,
    _unity_table_info,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def _published_versions(location: Path) -> list[int]:
    return sorted(int(path.stem) for path in (location / "_delta_log").glob("[0-9]*.json"))


def _commit_actions(location: Path, version: int) -> list[dict]:
    with (location / "_delta_log" / f"{version:020}.json").open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _in_commit_timestamp(location: Path, version: int) -> int:
    (commit_info,) = [action["commitInfo"] for action in _commit_actions(location, version) if "commitInfo" in action]
    return commit_info["inCommitTimestamp"]


def _columns(unity_rest_url: str, table: str) -> list[tuple[str, str]]:
    columns = sorted(_unity_table_info(unity_rest_url, table).get("columns") or [], key=lambda c: c["position"])
    return [(column["name"], column["type_name"]) for column in columns]


def _rows(spark: SparkSession, query: str) -> list[tuple]:
    return [tuple(row) for row in spark.sql(query).collect()]


def _delta_api_table(unity_rest_url: str, table: str) -> dict:
    """Loads the table with the Unity Catalog Delta REST API."""
    catalog_name, schema_name, name = _qualified_table_name(table).split(".", 2)
    response = requests.get(
        f"{unity_rest_url}/delta/v1/catalogs/{quote(catalog_name, safe='')}"
        f"/schemas/{quote(schema_name, safe='')}/tables/{quote(name, safe='')}",
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def test_metadata_changes_are_in_unity_catalog(
    spark: SparkSession,
    unity_rest_url: str,
    unity_uses_delta_api: bool,  # noqa: FBT001
) -> None:
    database = "unity_catalog_state_metadata"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, name STRING) USING delta TBLPROPERTIES ('team'='a')")
        spark.sql(f"INSERT INTO {table} VALUES (1, 'one')")
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('owner-team'='b')")
        spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('team')")
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS (score DOUBLE)")
        frame = spark.createDataFrame(
            [(2, "two", 2.5, ["x"])],
            schema="id INT, name STRING, score DOUBLE, tags ARRAY<STRING>",
        )
        frame.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
        assert _rows(spark, f"SELECT * FROM {table} ORDER BY id") == [
            (1, "one", None, None),
            (2, "two", 2.5, ["x"]),
        ]

        location = _location_to_path(_table_location(spark, table))
        latest = _published_versions(location)[-1]
        # Each statement is one commit.
        assert latest == 5
        info = _unity_table_info(unity_rest_url, table)
        assert _columns(unity_rest_url, table) == [
            ("id", "INT"),
            ("name", "STRING"),
            ("score", "DOUBLE"),
            ("tags", "ARRAY"),
        ]
        assert info["properties"]["owner-team"] == "b"
        assert "team" not in info["properties"]
        if unity_uses_delta_api:
            # The commits are made with the Delta REST API, which has the latest version of the
            # table and its metadata.
            delta_table = _delta_api_table(unity_rest_url, table)
            assert delta_table["latest-table-version"] == latest, delta_table
            metadata = delta_table["metadata"]
            fields = metadata["columns"]["fields"]
            assert [field["name"] for field in fields] == ["id", "name", "score", "tags"]
            assert fields[3]["type"] == {"type": "array", "element-type": "string", "contains-null": True}
            assert metadata["properties"]["owner-team"] == "b"
            assert "team" not in metadata["properties"]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def test_managed_delta_table_time_travel(spark: SparkSession) -> None:
    database = "unity_catalog_state_time_travel"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING delta")
        for value in [1, 2, 3]:
            spark.sql(f"INSERT INTO {table} VALUES ({value})")
        location = _location_to_path(_table_location(spark, table))
        assert _published_versions(location) == [0, 1, 2, 3]

        for version, expected in [(0, []), (1, [(1,)]), (2, [(1,), (2,)]), (3, [(1,), (2,), (3,)])]:
            assert _rows(spark, f"SELECT id FROM {table} VERSION AS OF {version} ORDER BY id") == expected
            timestamp = _in_commit_timestamp(location, version)
            # The timestamp of a commit reads the version of the commit.
            text = (
                datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
            assert _rows(spark, f"SELECT id FROM {table} TIMESTAMP AS OF '{text}' ORDER BY id") == expected
            assert (
                spark.read.option("versionAsOf", str(version)).table(table).orderBy("id").collect()
                == spark.sql(f"SELECT id FROM {table} VERSION AS OF {version} ORDER BY id").collect()
            )

        with pytest.raises(Exception, match="latest ratified version is 3, but version 4 was requested"):
            spark.sql(f"SELECT id FROM {table} VERSION AS OF 4").collect()
        with pytest.raises(Exception, match=r"(?i)no version of the delta table exists at or before"):
            spark.sql(f"SELECT id FROM {table} TIMESTAMP AS OF '2000-01-01T00:00:00Z'").collect()
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def test_managed_delta_table_is_checkpointed_at_the_checkpoint_interval(spark: SparkSession) -> None:
    """Sail checkpoints a catalog-managed table at a published version, so reads do not replay every commit."""
    database = "unity_catalog_state_commits"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING delta")
        # The default checkpoint interval is 10 commits.
        for value in range(12):
            spark.sql(f"INSERT INTO {table} VALUES ({value})")
        location = _location_to_path(_table_location(spark, table))
        assert _published_versions(location) == list(range(13))
        log = location / "_delta_log"
        checkpoints = {int(path.name.split(".", 1)[0]) for path in log.glob("*.checkpoint*")}
        assert checkpoints == {10}, sorted(path.name for path in log.iterdir())
        with (log / "_last_checkpoint").open(encoding="utf-8") as f:
            assert json.load(f)["version"] == 10
        assert _rows(spark, f"SELECT count(*), sum(id) FROM {table}") == [(12, sum(range(12)))]
        assert _rows(spark, f"SELECT count(*) FROM {table} VERSION AS OF 11") == [(11,)]
        assert _rows(spark, f"SELECT count(*) FROM {table} VERSION AS OF 5") == [(5,)]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")


def test_partial_overwrites_of_managed_delta_table_are_not_table_replacements(
    spark: SparkSession,
    unity_rest_url: str,
) -> None:
    database = "unity_catalog_state_overwrite"
    table = f"{database}.t"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part INT) USING delta PARTITIONED BY (part) TBLPROPERTIES ('team'='a')"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, 1), (2, 2)")
        created = _unity_table_info(unity_rest_url, table)
        location = _location_to_path(_table_location(spark, table))

        # The rows that match the condition are replaced, and the table is kept.
        frame = spark.createDataFrame([(10, 1)], schema="id INT, part INT")
        frame.write.format("delta").mode("overwrite").option("replaceWhere", "part = 1").saveAsTable(table)
        assert _rows(spark, f"SELECT id, part FROM {table} ORDER BY id") == [(2, 2), (10, 1)]

        # The partitions that have rows are replaced, and the other partitions are kept.
        frame = spark.createDataFrame([(20, 2), (30, 3)], schema="id INT, part INT")
        frame.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").saveAsTable(table)
        assert _rows(spark, f"SELECT id, part FROM {table} ORDER BY id") == [(10, 1), (20, 2), (30, 3)]

        versions = _published_versions(location)
        assert versions[-2:] == [2, 3]
        for version in versions[-2:]:
            actions = _commit_actions(location, version)
            # Neither commit changes the metadata of the table.
            assert not [action for action in actions if "metaData" in action], actions
        info = _unity_table_info(unity_rest_url, table)
        assert info["table_id"] == created["table_id"]
        assert info["properties"]["team"] == "a"
        assert _columns(unity_rest_url, table) == [("id", "INT"), ("part", "INT")]
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")
