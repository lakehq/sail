"""Metadata-layer assertions for Iceberg REST CREATE OR REPLACE / REPLACE TABLE.

These tests load the raw REST catalog metadata (``GET /v1/namespaces/{ns}/tables/{t}``)
to assert properties that are not observable through Spark SQL alone: the table UUID is
preserved across a replace (atomic replace, not drop+create), the current snapshot is
reset, stale user properties are removed, and the metadata pointer advances.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_replace_meta_test"


@pytest.fixture(scope="module", autouse=True)
def namespace(spark: SparkSession) -> Generator[None, None, None]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    yield
    spark.sql(f"DROP DATABASE IF EXISTS {NAMESPACE} CASCADE")


def _load_table(iceberg_rest_endpoint: str, table_name: str) -> dict:
    namespace = urllib.parse.quote(NAMESPACE, safe="")
    table = urllib.parse.quote(table_name, safe="")
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/tables/{table}"
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def test_replace_preserves_table_uuid(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "uuid_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING iceberg")

    before = _load_table(iceberg_rest_endpoint, table_name)
    uuid_before = before["metadata"]["table-uuid"]
    assert uuid_before

    spark.sql(f"CREATE OR REPLACE TABLE {table_fqn} (id BIGINT, score DOUBLE) USING iceberg")

    after = _load_table(iceberg_rest_endpoint, table_name)
    # Atomic replace preserves identity: the table UUID must be unchanged (no drop+create).
    assert after["metadata"]["table-uuid"] == uuid_before


def test_replace_advances_metadata_location(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "loc_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(f"CREATE TABLE {table_fqn} (id INT) USING iceberg")

    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]
    assert before_location

    spark.sql(f"CREATE OR REPLACE TABLE {table_fqn} (id BIGINT, name STRING) USING iceberg")

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_location = after["metadata-location"]
    # The metadata pointer must advance to a new file, and the old one is recorded in the log.
    assert after_location != before_location
    assert after["metadata"]["metadata-log"][-1]["metadata-file"] == before_location


def test_replace_removes_stale_user_property(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "props_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (id INT)
        USING iceberg
        TBLPROPERTIES ('owner' = 'alice', 'team' = 'data-eng')
        """
    )

    before = _load_table(iceberg_rest_endpoint, table_name)
    before_props = before["metadata"]["properties"]
    assert before_props.get("owner") == "alice"
    assert before_props.get("team") == "data-eng"

    # Replace supplying only "team" (not "owner") — the stale "owner" property must be removed.
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {table_fqn} (id INT)
        USING iceberg
        TBLPROPERTIES ('team' = 'platform')
        """
    )

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_props = after["metadata"]["properties"]
    assert "owner" not in after_props, "stale 'owner' property must be removed on replace"
    assert after_props.get("team") == "platform"


def test_replace_resets_current_snapshot(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "snap_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(f"CREATE TABLE {table_fqn} (id INT, name STRING) USING iceberg")

    created = _load_table(iceberg_rest_endpoint, table_name)
    # A fresh table has no live current snapshot and no snapshots.
    assert created["metadata"]["current-snapshot-id"] in (-1, None)
    assert created["metadata"]["snapshots"] == []

    # A real INSERT produces a committed data snapshot in the fixture.
    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a'), (2, 'b')")
    with_data = _load_table(iceberg_rest_endpoint, table_name)
    live_snapshot_id = with_data["metadata"]["current-snapshot-id"]
    assert live_snapshot_id not in (-1, None), "INSERT must produce a live current snapshot"
    assert len(with_data["metadata"]["snapshots"]) == 1

    spark.sql(f"CREATE OR REPLACE TABLE {table_fqn} (id INT, name STRING) USING iceberg")

    replaced = _load_table(iceberg_rest_endpoint, table_name)
    # The current snapshot is reset (the replaced table reads empty) — the `main` ref is
    # removed, so current-snapshot-id is the -1 sentinel (or null), never the old snapshot.
    reset = replaced["metadata"]["current-snapshot-id"]
    assert reset != live_snapshot_id, "replace must not carry the old data snapshot forward"
    assert reset in (-1, None), f"current-snapshot-id must be reset, got {reset!r}"

    # The replaced table reads as empty through SQL.
    rows = spark.sql(f"SELECT id, name FROM {table_fqn}").collect()  # noqa: S608
    assert rows == []


def test_replace_creates_missing_table(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    # CREATE OR REPLACE on a non-existent table just creates it.
    table_name = "cor_new_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    spark.sql(f"CREATE OR REPLACE TABLE {table_fqn} (id INT) USING iceberg")

    table = _load_table(iceberg_rest_endpoint, table_name)
    assert table["metadata-location"]
    assert table["metadata"]["table-uuid"]
