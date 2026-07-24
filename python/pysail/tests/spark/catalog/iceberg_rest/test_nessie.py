"""Nessie-backed Iceberg REST catalog integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.tests.spark.catalog.iceberg_rest.test_commit import _s3_object_keys

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def remote(
    nessie_iceberg_rest_endpoint: str,
    seaweedfs_host_endpoint: str,
) -> Generator[str, None, None]:
    """Start Sail server with Nessie as the Iceberg REST catalog."""
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{nessie_iceberg_rest_endpoint}"}}]'
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


def test_create_table_with_nessie_catalog(spark: SparkSession) -> None:
    """Create a table through Nessie's Iceberg REST endpoint."""
    namespace = "nessie_create_table"
    table = f"{namespace}.t1"

    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}").collect()
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg").collect()

        rows = spark.sql(f"SHOW TABLES IN {namespace} LIKE 't1'").collect()

        assert len(rows) == 1
        assert rows[0].database == namespace
        assert rows[0].tableName == "t1"
        assert rows[0].isTemporary is False
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {namespace} CASCADE").collect()


def test_nessie_write_honors_absolute_data_path(
    spark: SparkSession,
    seaweedfs_host_endpoint: str,
) -> None:
    namespace = "nessie_absolute_data_path"
    table = f"{namespace}.t1"
    data_location = f"s3://icebergdata/managed-data/{namespace}/t1"
    warehouse_namespace = f"s3://icebergdata/nessie/{namespace}"

    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}").collect()
        spark.sql(
            f"""
            CREATE TABLE {table} (id INT, name STRING)
            USING iceberg
            TBLPROPERTIES ('write.data.path' = '{data_location}')
            """
        ).collect()
        spark.sql(f"INSERT INTO {table} VALUES (1, 'one')")  # noqa: S608

        data_keys = _s3_object_keys(seaweedfs_host_endpoint, data_location)
        assert any(key.endswith(".parquet") for key in data_keys)
        warehouse_keys = _s3_object_keys(seaweedfs_host_endpoint, warehouse_namespace)
        assert not any(key.endswith(".parquet") for key in warehouse_keys)

        rows = spark.sql(f"SELECT id, name FROM {table}").collect()  # noqa: S608
        assert [(row.id, row.name) for row in rows] == [(1, "one")]
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {namespace} CASCADE").collect()
