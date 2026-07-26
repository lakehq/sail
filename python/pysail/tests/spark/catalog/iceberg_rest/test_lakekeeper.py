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
