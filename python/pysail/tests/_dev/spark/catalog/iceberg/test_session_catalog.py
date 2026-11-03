from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession

CATALOG = "org_1"
NAMESPACE = "session_catalog_test"
TOKEN = "session-catalog-secret-token"  # noqa: S105


@pytest.fixture(scope="module")
def remote(seaweedfs_host_endpoint: str) -> Generator[str, None, None]:
    """Start Sail without an Iceberg REST catalog in its configuration."""
    with spark_connect_server(
        envs={
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": seaweedfs_host_endpoint,
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
            "AWS_ALLOW_HTTP": "true",
        },
    ) as server:
        yield server.remote


@pytest.fixture(scope="module")
def sessions(remote: str):
    with spark_session_factory(remote) as sessions:
        yield sessions


@pytest.fixture(scope="module")
def uri(lakekeeper_endpoint: str, lakekeeper_warehouse_id: str) -> str:
    del lakekeeper_warehouse_id
    return f"{lakekeeper_endpoint}/catalog"


def _register(session: SparkSession, uri: str, name: str = CATALOG) -> None:
    session.conf.set(f"spark.sql.catalog.{name}", "iceberg-rest")
    session.conf.set(f"spark.sql.catalog.{name}.uri", uri)
    session.conf.set(f"spark.sql.catalog.{name}.warehouse", "demo")
    session.conf.set(f"spark.sql.catalog.{name}.bearer_access_token", TOKEN)


@pytest.fixture(scope="module")
def table(sessions, uri: str) -> Generator[str, None, None]:
    """Create a table through a session catalog and drop it after the tests."""
    spark = sessions.create()
    _register(spark, uri)
    name = f"{CATALOG}.{NAMESPACE}.events"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).writeTo(name).using("iceberg").create()
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(f"DROP NAMESPACE IF EXISTS {CATALOG}.{NAMESPACE} CASCADE")


def _ids(spark: SparkSession, table: str) -> list[int]:
    return [row["id"] for row in spark.table(table).orderBy("id").collect()]


def test_session_catalog_reads_and_writes_a_table(sessions, uri: str, table: str) -> None:
    spark = sessions.create()
    _register(spark, uri)
    assert _ids(spark, table) == [1, 2]
    spark.sql(f"INSERT INTO {table} VALUES (3, 'c')")
    assert _ids(spark, table) == [1, 2, 3]
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    assert _ids(spark, table) == [1, 2]


def test_session_catalog_is_invisible_to_other_sessions(sessions, uri: str, table: str) -> None:
    first, second = sessions.create(), sessions.create()
    _register(first, uri)
    assert _ids(first, table) == [1, 2]
    with pytest.raises(Exception, match="events"):
        _ids(second, table)


def test_session_catalog_is_dropped_when_its_type_is_unset(sessions, uri: str, table: str) -> None:
    spark = sessions.create()
    _register(spark, uri)
    assert _ids(spark, table) == [1, 2]
    spark.conf.unset(f"spark.sql.catalog.{CATALOG}")
    with pytest.raises(Exception, match="events"):
        _ids(spark, table)


def test_session_catalog_follows_its_options(sessions, uri: str, table: str) -> None:
    spark = sessions.create()
    _register(spark, uri)
    assert _ids(spark, table) == [1, 2]
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.uri", "http://127.0.0.1:1/catalog")
    with pytest.raises(Exception):  # noqa: B017, PT011
        _ids(spark, table)
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.uri", uri)
    assert _ids(spark, table) == [1, 2]


def test_session_catalog_cannot_replace_a_server_catalog(sessions, uri: str) -> None:
    spark = sessions.create()
    with pytest.raises(Exception, match="sail"):
        spark.conf.set("spark.sql.catalog.sail", "iceberg-rest")
        spark.conf.set("spark.sql.catalog.sail.uri", uri)
    assert spark.conf.get("spark.sql.catalog.sail", None) is None
    spark.sql("CREATE NAMESPACE IF NOT EXISTS sail.session_catalog_server_test")
    spark.sql("DROP NAMESPACE sail.session_catalog_server_test")


def test_session_catalog_does_not_take_a_token_file(sessions, uri: str) -> None:
    spark = sessions.create()
    spark.conf.set(f"spark.sql.catalog.{CATALOG}", "iceberg-rest")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.uri", uri)
    with pytest.raises(Exception, match="bearer_access_token_file"):
        spark.conf.set(f"spark.sql.catalog.{CATALOG}.bearer_access_token_file", "/etc/hostname")
    assert spark.conf.get(f"spark.sql.catalog.{CATALOG}.bearer_access_token_file", None) is None


def test_session_catalog_token_is_redacted(sessions, uri: str) -> None:
    spark = sessions.create()
    _register(spark, uri)
    listed = spark.conf.getAll
    assert TOKEN not in str(listed)
    assert listed[f"spark.sql.catalog.{CATALOG}.warehouse"] == "demo"
    assert listed[f"spark.sql.catalog.{CATALOG}.bearer_access_token"] != TOKEN
    key = f"spark.sql.catalog.{CATALOG}.bearer_access_token"
    echoed = spark.sql(f"SET {key}={TOKEN}").collect()
    assert TOKEN not in str(echoed)
