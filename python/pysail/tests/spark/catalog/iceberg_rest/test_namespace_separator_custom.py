from __future__ import annotations

import contextlib
import json
import urllib.request
from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.tests.spark.catalog.iceberg_rest.conftest import NESSIE_NAMESPACE_SEPARATOR

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def remote(nessie_custom_separator_iceberg_rest_endpoint: str) -> Generator[str, None, None]:
    """Start Sail with Nessie catalog for namespace separator config."""
    catalogs = [
        f'{{name="sail_custom_separator", type="iceberg-rest", '
        f'uri="{nessie_custom_separator_iceberg_rest_endpoint}", '
        f'namespace_separator="{NESSIE_NAMESPACE_SEPARATOR}"}}'
    ]
    with spark_connect_server(
        envs={
            "SAIL_CATALOG__LIST": f"[{', '.join(catalogs)}]",
            "SAIL_CATALOG__DEFAULT_CATALOG": "sail_custom_separator",
        },
    ) as server:
        yield server.remote


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


@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_custom_server_catalog_work_with_config(
    spark: SparkSession,
    nessie_custom_separator_iceberg_rest_endpoint: str,
) -> None:
    catalog_name = "sail_custom_separator"
    config = _load_config(nessie_custom_separator_iceberg_rest_endpoint)
    _assert_rest_config_namespace_separator(config, NESSIE_NAMESPACE_SEPARATOR)

    spark.catalog.setCurrentCatalog(catalog_name)
    _exercise_namespace_separator(
        spark,
        namespace_id="custom_separator",
    )
