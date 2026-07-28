from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.tests.spark.catalog.iceberg_rest.conftest import NESSIE_NAMESPACE_SEPARATOR

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def remote(nessie_iceberg_rest_endpoint: str) -> Generator[str, None, None]:
    """Start Sail with default and custom-separator catalogs against a default-separator Nessie server."""
    catalogs = [
        f'{{name="sail", type="iceberg-rest", uri="{nessie_iceberg_rest_endpoint}"}}',
        f'{{name="sail_custom_separator", type="iceberg-rest", uri="{nessie_iceberg_rest_endpoint}", '
        f'namespace_separator="{NESSIE_NAMESPACE_SEPARATOR}"}}',
    ]
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": f"[{', '.join(catalogs)}]",
            "SAIL_CATALOG__DEFAULT_CATALOG": "sail",
        },
    ) as server:
        yield server.remote


def _assert_table_listed(spark: SparkSession, namespace: str, table_name: str) -> None:
    tables = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    table_names = [row.tableName for row in tables]
    assert table_name in table_names


def _assert_no_tables_listed(spark: SparkSession, namespace: str) -> None:
    tables = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    table_names = [row.tableName for row in tables]
    assert table_names == []


@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_custom_config_cannot_resolve_default_separator_namespace(
    spark: SparkSession,
) -> None:
    catalog_name = "sail_custom_separator"
    root_namespace = f"namespace_separator_default_created_{catalog_name}"
    nested_namespace = f"{root_namespace}.child"
    table_name = "t1"
    table = f"{nested_namespace}.{table_name}"

    spark.catalog.setCurrentCatalog("sail")
    spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
    spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
    spark.catalog.setCurrentCatalog(catalog_name)

    try:
        spark.catalog.setCurrentCatalog("sail")
        spark.sql(f"CREATE DATABASE {root_namespace}").collect()
        spark.sql(f"CREATE DATABASE {nested_namespace}").collect()
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg").collect()
        _assert_table_listed(spark, nested_namespace, table_name)

        spark.catalog.setCurrentCatalog(catalog_name)
        _assert_no_tables_listed(spark, nested_namespace)
    finally:
        spark.catalog.setCurrentCatalog("sail")
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
