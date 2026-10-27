from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING

import pytest
import requests

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession

NAMESPACE = "lakekeeper_access_session_test"
TABLE = f"sail.{NAMESPACE}.remote_signing_t"


@pytest.fixture(scope="module")
def remote(
    lakekeeper_endpoint: str,
    lakekeeper_warehouse_id: str,
    seaweedfs_host_endpoint: str,
) -> Generator[str, None, None]:
    """Start Sail with the Lakekeeper-backed Iceberg REST catalog."""
    del lakekeeper_warehouse_id
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{lakekeeper_endpoint}/catalog", warehouse="demo"}}]'
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": catalog_config,
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT": seaweedfs_host_endpoint,
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
            "AWS_ALLOW_HTTP": "true",
        },
    ) as server:
        yield server.remote


@pytest.fixture(scope="module", autouse=True)
def namespace(spark: SparkSession) -> Generator[None, None, None]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS sail.{NAMESPACE}")
    yield
    spark.sql(f"DROP NAMESPACE IF EXISTS sail.{NAMESPACE} CASCADE")


def _load_lakekeeper_table(
    lakekeeper_endpoint: str,
    lakekeeper_warehouse_id: str,
) -> dict:
    namespace = urllib.parse.quote(NAMESPACE, safe="")
    table = urllib.parse.quote(TABLE.rsplit(".", 1)[1], safe="")
    response = requests.get(
        f"{lakekeeper_endpoint}/catalog/v1/{lakekeeper_warehouse_id}/namespaces/{namespace}/tables/{table}",
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def test_create_write_uses_configured_credentials_with_lakekeeper_session_hints(
    spark: SparkSession,
    lakekeeper_endpoint: str,
    lakekeeper_warehouse_id: str,
) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    source = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    source.writeTo(TABLE).using("iceberg").create()

    rows = spark.table(TABLE).orderBy("id").collect()
    assert [(row["id"], row["name"]) for row in rows] == [(1, "a"), (2, "b")]

    table = _load_lakekeeper_table(lakekeeper_endpoint, lakekeeper_warehouse_id)
    assert table["config"]["s3.remote-signing-enabled"] == "true"
    assert table["storage-credentials"]


def test_lakekeeper_lakehouse_ddl(spark: SparkSession, lakekeeper_endpoint: str) -> None:
    from pyiceberg.catalog.rest import RestCatalog

    from pysail.testing.spark.ddl import exercise_lakehouse_alter

    table = f"{NAMESPACE}.ddl"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING, part STRING) USING iceberg "
            "PARTITIONED BY (part) TBLPROPERTIES ('format-version'='3', 'retained'='yes')"
        )
        exercise_lakehouse_alter(spark, table)
        reference = RestCatalog("reference", uri=f"{lakekeeper_endpoint}/catalog", warehouse="demo").load_table(table)
        assert str(reference.schema().find_field("id").field_type) == "long"
        assert "custom" not in reference.properties
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("client", ["sail", "pyiceberg"])
@pytest.mark.xfail(
    strict=True, reason="Lakekeeper 0.12.1 retains the last removed property on load-table, including with PyIceberg"
)
def test_lakekeeper_remove_last_property(spark: SparkSession, lakekeeper_endpoint: str, client: str) -> None:
    from pyiceberg.catalog.rest import RestCatalog

    table = f"{NAMESPACE}.remove_last_property"
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg TBLPROPERTIES ('custom'='v')")
        reference = RestCatalog("reference", uri=f"{lakekeeper_endpoint}/catalog", warehouse="demo").load_table(table)
        if client == "sail":
            spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('custom')")
        else:
            with reference.transaction() as transaction:
                transaction.remove_properties("custom")
        assert "custom" not in reference.refresh().properties
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
