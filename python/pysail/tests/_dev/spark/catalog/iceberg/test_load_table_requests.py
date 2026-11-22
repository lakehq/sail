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


def test_temporary_view_read_loads_its_table_once_per_statement(spark, recording_catalog):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    table = f"{NAMESPACE}.viewed"
    views = ["viewed_sql", "viewed_data_frame"]
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        spark.sql(f"CREATE TEMPORARY VIEW viewed_sql AS SELECT id FROM {table}")
        spark.table(table).createOrReplaceTempView("viewed_data_frame")
        spark.sql(f"INSERT INTO {table} VALUES (2)")
        for view in views:
            # Each read of a temporary view loads the table and sees its latest version.
            start = len(records)
            assert [row.id for row in spark.table(view).orderBy("id").collect()] == [1, 2]
            assert _requests(records, start) == [(f"{NAMESPACE}/tables/viewed", 200, None)]
            # The view and the other references to the table share one load.
            start = len(records)
            rows = spark.sql(
                f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN {table} t ON v.id = t.id "
                f"WHERE v.id IN (SELECT id FROM viewed_sql) AND t.id IN (SELECT id FROM viewed_data_frame)"
            ).collect()
            assert sorted(tuple(row) for row in rows) == [(1, 1), (2, 2)]
            assert _requests(records, start) == [(f"{NAMESPACE}/tables/viewed", 200, None)]
    finally:
        for view in views:
            spark.catalog.dropTempView(view)
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
            _, records = catalog
            session = sessions.create()
            # Views and missing tables are resolved as without the header.
            start = len(records)
            assert [row.id for row in session.sql(f"SELECT * FROM {view}").collect()] == [1]
            assert _requests(records, start) == [
                (f"{NAMESPACE}/tables/constant", 403, "vended-credentials"),
                (f"{NAMESPACE}/tables/constant", 404, None),
                (f"{NAMESPACE}/views/constant", 200, None),
            ]
            start = len(records)
            with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
                session.table(f"{NAMESPACE}.missing").collect()
            # The namespace name is also looked up as the base table of a metadata table.
            assert _requests(records, start) == [
                (f"{NAMESPACE}/tables/missing", 403, "vended-credentials"),
                (f"{NAMESPACE}/tables/missing", 404, None),
                (f"{NAMESPACE}/views/missing", 404, None),
                (f"default/tables/{NAMESPACE}", 403, "vended-credentials"),
                (f"default/tables/{NAMESPACE}", 404, None),
                (f"default/views/{NAMESPACE}", 404, None),
            ]
            # The rejection is reported when table access begins, after read validation.
            with pytest.raises(Exception, match="expression must be deterministic"):
                session.sql(f"SELECT * FROM {table} VERSION AS OF rand()").collect()
            start = len(records)
            with pytest.raises(Exception, match="403 Forbidden"):
                session.table(table).collect()
            assert _requests(records, start) == [
                (f"{NAMESPACE}/tables/rejected", 403, "vended-credentials"),
                (f"{NAMESPACE}/tables/rejected", 200, None),
                (f"{NAMESPACE}/tables/rejected", 403, "vended-credentials"),
            ]
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}")
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_read_loads_a_table_once_however_its_name_is_written(spark, recording_catalog):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    name = f"{NAMESPACE}.spelled"
    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name} VALUES (1), (2)")
        start = len(records)
        rows = spark.sql(
            f"SELECT a.id FROM {name} a JOIN sail.{name} b ON a.id = b.id JOIN `sail`.{name} c ON a.id = c.id"
        ).collect()
        assert sorted(row.id for row in rows) == [1, 2]
        assert _requests(records, start) == [(f"{NAMESPACE}/tables/spelled", 200, None)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _writes(name):
    """Returns statements that write to the empty table `name`, each with the rows that it leaves."""
    return [
        (f"INSERT INTO {name} VALUES (1, 10), (2, 20)", [(1, 10), (2, 20)]),
        # The target and the source are the same table under two names.
        (f"INSERT INTO {name} SELECT id + 2, v FROM sail.{name}", [(1, 10), (2, 20), (3, 10), (4, 20)]),
        (f"UPDATE {name} SET v = 0 WHERE id = 1", [(1, 0), (2, 20), (3, 10), (4, 20)]),
        (f"DELETE FROM {name} WHERE id = 2", [(1, 0), (3, 10), (4, 20)]),
        (
            f"MERGE INTO {name} t USING sail.{name} s ON t.id = s.id AND s.id = 3 "
            "WHEN MATCHED THEN UPDATE SET v = s.v + 1",
            [(1, 0), (3, 11), (4, 20)],
        ),
        (f"TRUNCATE TABLE {name}", []),
    ]


def test_write_loads_the_target_table_once_to_plan_the_statement(spark, recording_catalog):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    name = f"{NAMESPACE}.written"
    load = (f"{NAMESPACE}/tables/written", 200, None)
    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT, v BIGINT) USING iceberg")
        for sql, expected in _writes(name):
            start = len(records)
            spark.sql(sql).collect()
            # The table is loaded once to plan the statement, and once more when the
            # statement commits, to detect a concurrent change to the table.
            assert _requests(records, start) == [load, load], sql
            assert [(row.id, row.v) for row in spark.table(name).orderBy("id").collect()] == expected, sql
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_write_asks_for_access_delegation_only_to_plan_the_statement(spark, recording_catalog, seaweedfs_host_endpoint):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    name = f"{NAMESPACE}.delegated_write"
    path = f"{NAMESPACE}/tables/delegated_write"
    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT, v BIGINT) USING iceberg")
        options = ', access_delegation="vended-credentials"'
        with (
            _server(recording_catalog, seaweedfs_host_endpoint, options) as server,
            spark_session_factory(server.remote) as sessions,
        ):
            session = sessions.create()
            for sql, expected in _writes(name):
                start = len(records)
                session.sql(sql).collect()
                # Only the load that plans the statement asks for access delegation. The
                # load without it is the one of the commit, so the statement is planned
                # with one load.
                assert _requests(records, start) == [(path, 200, "vended-credentials"), (path, 200, None)], sql
                assert [(row.id, row.v) for row in spark.table(name).orderBy("id").collect()] == expected, sql
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_write_keeps_a_change_made_to_the_table_after_the_statement_is_planned(
    spark, iceberg_rest_endpoint, seaweedfs_host_endpoint
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    name = f"{NAMESPACE}.raced"
    pending = []

    def change_table(method, path):
        # Another session adds a row right after the table is loaded to plan a statement.
        if method == "GET" and path.endswith("/tables/raced") and pending:
            spark.sql(pending.pop()).collect()

    def rows():
        return [(row.id, row.v) for row in spark.table(name).orderBy("id").collect()]

    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT, v BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name} VALUES (1, 10), (2, 20)")
        with (
            recording_proxy(iceberg_rest_endpoint, before_response=change_table) as catalog,
            _server(catalog, seaweedfs_host_endpoint) as server,
            spark_session_factory(server.remote) as sessions,
        ):
            session = sessions.create()
            # The target of each statement shares the table load of the statement, which
            # is older than the added row. The commit loads the table again and sees the row.
            for added, sql in enumerate(
                [
                    f"UPDATE {name} SET v = 0 WHERE id = 1",
                    f"DELETE FROM {name} WHERE id = 2",
                    f"INSERT INTO {name} SELECT id + 10, v FROM sail.{name} WHERE id < 100",
                ],
                start=100,
            ):
                before = rows()
                pending.append(f"INSERT INTO {name} VALUES ({added}, 0)")
                try:
                    session.sql(sql).collect()
                    failed = False
                except Exception:  # noqa: BLE001
                    failed = True
                assert not pending, sql
                # The statement fails, or it is applied to the table with the added row.
                assert (added, 0) in rows(), sql
                if failed:
                    assert rows() == sorted([*before, (added, 0)]), sql
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_metadata_table_shares_the_load_of_its_table(spark, recording_catalog):
    _, records = recording_catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    name = f"{NAMESPACE}.inspected"
    load = (f"{NAMESPACE}/tables/inspected", 200, None)
    # The name of a metadata table is looked up as a table and as a view first.
    missing = [
        (f"{NAMESPACE}\x1finspected/tables/snapshots", 404, None),
        (f"{NAMESPACE}\x1finspected/views/snapshots", 404, None),
    ]
    try:
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name} VALUES (1), (2)")
        # The metadata table and the table, under another name, share one load of the table.
        for sql, expected in [
            (f"SELECT count(*) AS n FROM {name}.snapshots s CROSS JOIN sail.{name} t", [*missing, load]),
            (f"SELECT count(*) AS n FROM sail.{name} t CROSS JOIN {name}.snapshots s", [load, *missing]),
            (
                f"SELECT count(*) AS n FROM {name}.snapshots s CROSS JOIN {name}.snapshots r CROSS JOIN {name} t",
                [*missing, load, *missing],
            ),
        ]:
            start = len(records)
            assert [row.n for row in spark.sql(sql).collect()] == [2], sql
            assert _requests(records, start) == expected, sql
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
