# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

"""Reading an Iceberg REST table loads the table once per statement."""

from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.tests._dev.spark.catalog.iceberg.conftest import recording_proxy

if TYPE_CHECKING:
    from collections.abc import Generator

NAMESPACE = "load_table_requests"


def _requests(records, start):
    """Returns the table and view loads recorded since `start`."""
    return [
        (urllib.parse.unquote(path).removeprefix("/v1/namespaces/"), status, access_delegation)
        for method, path, _, status, access_delegation in records[start:]
        if method == "GET" and ("/tables/" in path or "/views/" in path)
    ]


def _server(recording_catalog, seaweedfs_host_endpoint, options=""):
    endpoint, _ = recording_catalog
    return spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": f'[{{name="sail", type="iceberg-rest", uri="{endpoint}"{options}}}]',
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": seaweedfs_host_endpoint,
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
            "AWS_ALLOW_HTTP": "true",
        }
    )


@pytest.fixture(scope="module")
def remote(recording_catalog, seaweedfs_host_endpoint) -> Generator[str, None, None]:
    with _server(recording_catalog, seaweedfs_host_endpoint) as server:
        yield server.remote


def test_read_loads_each_table_once_per_statement(spark, recording_catalog):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    first = f"{NAMESPACE}.first"
    second = f"{NAMESPACE}.second"
    try:
        spark.sql(f"CREATE TABLE {first} (id BIGINT) USING iceberg")
        spark.sql(f"CREATE TABLE {second} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {first} VALUES (1), (2)")
        spark.sql(f"INSERT INTO {second} VALUES (2), (3)")

        start = len(records)
        assert [row.id for row in spark.read.table(first).orderBy("id").collect()] == [1, 2]
        assert _requests(records, start) == [(f"{NAMESPACE}/tables/first", 200, None)]

        start = len(records)
        # Repeated references to a table also load it once.
        rows = (
            spark.read.table(first)
            .alias("a")
            .join(spark.read.table(first).alias("b"), "id")
            .join(spark.read.table(first).alias("c"), "id")
            .join(spark.read.table(second), "id")
            .collect()
        )
        assert [row.id for row in rows] == [2]
        assert sorted(_requests(records, start)) == [
            (f"{NAMESPACE}/tables/first", 200, None),
            (f"{NAMESPACE}/tables/second", 200, None),
        ]

        # Each statement loads the table again and sees its latest version.
        spark.sql(f"INSERT INTO {first} VALUES (3)")
        start = len(records)
        assert [row.id for row in spark.read.table(first).orderBy("id").collect()] == [1, 2, 3]
        assert _requests(records, start) == [(f"{NAMESPACE}/tables/first", 200, None)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {first}")
        spark.sql(f"DROP TABLE IF EXISTS {second}")


def test_read_sends_configured_access_delegation_once(spark, recording_catalog, seaweedfs_host_endpoint):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    name = f"{NAMESPACE}.delegated"
    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        options = ', access_delegation="vended-credentials"'
        with (
            _server(recording_catalog, seaweedfs_host_endpoint, options) as server,
            spark_session_factory(server.remote) as sessions,
        ):
            start = len(records)
            assert [row.id for row in sessions.create().table(name).collect()] == [1]
            assert _requests(records, start) == [(f"{NAMESPACE}/tables/delegated", 200, "vended-credentials")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_metadata_table_read_loads_the_table_once(spark, recording_catalog):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    name = f"{NAMESPACE}.metadata"
    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        start = len(records)
        rows = spark.sql(f"SELECT operation FROM {name}.snapshots").collect()
        assert [row.operation for row in rows] == ["append"]
        # The metadata table name is not a table or a view, so the base table is loaded.
        assert _requests(records, start) == [
            (f"{NAMESPACE}\x1fmetadata/tables/snapshots", 404, None),
            (f"{NAMESPACE}\x1fmetadata/views/snapshots", 404, None),
            (f"{NAMESPACE}/tables/metadata", 200, None),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_view_read_loads_the_view_and_its_table_once(spark, recording_catalog):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    table = f"{NAMESPACE}.base"
    view = f"{NAMESPACE}.filtered"
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {table} VALUES (1), (2)")
        spark.sql(f"CREATE VIEW {view} AS SELECT id FROM {table} WHERE id > 1")
        start = len(records)
        assert [row.id for row in spark.table(view).collect()] == [2]
        assert _requests(records, start) == [
            (f"{NAMESPACE}/tables/filtered", 404, None),
            (f"{NAMESPACE}/views/filtered", 200, None),
            (f"{NAMESPACE}/tables/base", 200, None),
        ]
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}")
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_invalid_scan_planning_mode_is_reported_after_read_validation(
    spark, iceberg_rest_endpoint, seaweedfs_host_endpoint
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    name = f"{NAMESPACE}.planned"
    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        with (
            recording_proxy(iceberg_rest_endpoint, {"scan-planning-mode": "unknown"}) as catalog,
            _server(catalog, seaweedfs_host_endpoint) as server,
            spark_session_factory(server.remote) as sessions,
        ):
            session = sessions.create()
            with pytest.raises(Exception, match="Invalid Iceberg REST scan-planning-mode: unknown"):
                session.table(name).collect()
            # Missing tables and invalid time travel are reported first.
            with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
                session.sql(f"SELECT * FROM {name}.unknown").collect()
            with pytest.raises(Exception, match="expression must be deterministic"):
                session.sql(f"SELECT * FROM {name} VERSION AS OF rand()").collect()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_rejected_access_delegation_is_reported_when_table_access_begins(
    spark, iceberg_rest_endpoint, seaweedfs_host_endpoint
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    view = f"{NAMESPACE}.constant"
    table = f"{NAMESPACE}.rejected"
    try:
        spark.sql(f"CREATE VIEW {view} AS SELECT 1 AS id")
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        with (
            recording_proxy(iceberg_rest_endpoint, reject_access_delegation=True) as catalog,
            _server(catalog, seaweedfs_host_endpoint, ', access_delegation="vended-credentials"') as server,
            spark_session_factory(server.remote) as sessions,
        ):
            session = sessions.create()
            # Views and missing tables are resolved as without the header.
            assert [row.id for row in session.sql(f"SELECT * FROM {view}").collect()] == [1]
            with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
                session.table(f"{NAMESPACE}.missing").collect()
            # The rejection is reported when table access begins, after read validation.
            with pytest.raises(Exception, match="expression must be deterministic"):
                session.sql(f"SELECT * FROM {table} VERSION AS OF rand()").collect()
            with pytest.raises(Exception, match="403 Forbidden"):
                session.table(table).collect()
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}")
        spark.sql(f"DROP TABLE IF EXISTS {table}")
