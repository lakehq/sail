from __future__ import annotations

import contextlib
import json
import urllib.request
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.catalog_integration.iceberg_rest.conftest import (
    NESSIE_NAMESPACE_SEPARATOR,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def spark_doctest() -> None:
    """Override the module-autouse doctest fixture.

    These tests create their own Sail server with a custom Nessie REST catalog
    configuration. The shared doctest fixture would otherwise start the default
    iceberg_spark fixture for this module too.
    """


def _load_config(iceberg_rest_endpoint: str) -> dict[str, object]:
    url = f"{iceberg_rest_endpoint}/v1/config"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def _assert_rest_config_namespace_separator(config: dict[str, object], separator: str) -> None:
    defaults = config.get("defaults", {})
    overrides = config.get("overrides", {})
    assert isinstance(defaults, dict), config
    assert isinstance(overrides, dict), config
    actual = overrides.get("namespace-separator", defaults.get("namespace-separator"))
    assert actual == separator, config


def _exercise_namespace_separator(
    spark: SparkSession,
    *,
    namespace_id: str,
) -> None:
    root_namespace = f"namespace_separator_{namespace_id}"
    nested_namespace = f"{root_namespace}.child"

    spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
    try:
        spark.sql(f"CREATE DATABASE {root_namespace}").collect()
        spark.sql(f"CREATE DATABASE {nested_namespace}").collect()
        databases = spark.sql(f"SHOW DATABASES IN {root_namespace}").collect()
        database_names = [row.name for row in databases]
        assert "child" in database_names or nested_namespace in database_names
    finally:
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()


def _assert_table_listed(spark: SparkSession, namespace: str, table_name: str) -> None:
    tables = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    table_names = [row.tableName for row in tables]
    assert table_name in table_names


def _assert_no_tables_listed(spark: SparkSession, namespace: str) -> None:
    tables = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    table_names = [row.tableName for row in tables]
    assert table_names == []


@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_custom_server_catalog_work_with_config(
    nessie_spark_custom_separator: SparkSession,
    nessie_custom_separator_iceberg_rest_endpoint: str,
) -> None:
    catalog_name = "sail_custom_separator"
    namespace_id = "custom_separator"
    config = _load_config(nessie_custom_separator_iceberg_rest_endpoint)
    _assert_rest_config_namespace_separator(config, NESSIE_NAMESPACE_SEPARATOR)

    nessie_spark_custom_separator.catalog.setCurrentCatalog(catalog_name)
    _exercise_namespace_separator(
        nessie_spark_custom_separator,
        namespace_id=namespace_id,
    )


@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_custom_config_cannot_resolve_default_separator_namespace(
    nessie_spark_incorrect_custom_separator: SparkSession,
) -> None:
    catalog_name = "sail_custom_separator"
    root_namespace = f"namespace_separator_default_created_{catalog_name}"
    nested_namespace = f"{root_namespace}.child"
    table_name = "t1"
    table = f"{nested_namespace}.{table_name}"

    nessie_spark_incorrect_custom_separator.catalog.setCurrentCatalog("sail")
    nessie_spark_incorrect_custom_separator.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
    nessie_spark_incorrect_custom_separator.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
    nessie_spark_incorrect_custom_separator.catalog.setCurrentCatalog(catalog_name)

    try:
        nessie_spark_incorrect_custom_separator.catalog.setCurrentCatalog("sail")
        nessie_spark_incorrect_custom_separator.sql(f"CREATE DATABASE {root_namespace}").collect()
        nessie_spark_incorrect_custom_separator.sql(f"CREATE DATABASE {nested_namespace}").collect()
        nessie_spark_incorrect_custom_separator.sql(f"CREATE TABLE {table} (id INT) USING iceberg").collect()
        _assert_table_listed(nessie_spark_incorrect_custom_separator, nested_namespace, table_name)

        nessie_spark_incorrect_custom_separator.catalog.setCurrentCatalog(catalog_name)
        _assert_no_tables_listed(nessie_spark_incorrect_custom_separator, nested_namespace)
    finally:
        nessie_spark_incorrect_custom_separator.catalog.setCurrentCatalog("sail")
        with contextlib.suppress(Exception):
            nessie_spark_incorrect_custom_separator.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            nessie_spark_incorrect_custom_separator.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
