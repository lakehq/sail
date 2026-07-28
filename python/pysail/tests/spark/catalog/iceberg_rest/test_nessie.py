"""Nessie-backed Iceberg REST catalog integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def remote(nessie_iceberg_rest_endpoint: str) -> Generator[str, None, None]:
    """Start Sail server with Nessie as the Iceberg REST catalog."""
    catalog_config = f'[{{name="sail", type="iceberg-rest", uri="{nessie_iceberg_rest_endpoint}"}}]'
    with spark_connect_server(envs={"SAIL_CATALOG__LIST": catalog_config}) as server:
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
