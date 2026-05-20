from __future__ import annotations

import contextlib
import json
import urllib.request
from typing import TYPE_CHECKING

import pytest

from pysail.tests.spark.catalog_integration.iceberg_rest.conftest import (
    NESSIE_NAMESPACE_SEPARATOR,
    NESSIE_NAMESPACE_SEPARATOR_CATALOGS,
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
    deeply_nested_namespace = f"{nested_namespace}.grandchild"

    spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
    try:
        spark.sql(f"CREATE DATABASE {root_namespace}").collect()
        spark.sql(f"CREATE DATABASE {nested_namespace}").collect()
        spark.sql(f"CREATE DATABASE {deeply_nested_namespace}").collect()
        databases = spark.sql(f"SHOW DATABASES IN {root_namespace}").collect()
        database_names = [row.name for row in databases]
        assert "child" in database_names or nested_namespace in database_names
    finally:
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {deeply_nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()


@pytest.mark.parametrize(
    ("catalog_name", "namespace_id"),
    [
        ("sail_snake_case", "snake_case"),
        ("sail_kebab_case", "kebab_case"),
        ("sail_camel_case", "camel_case"),
        ("sail_flat_case", "flat_case"),
    ],
)
@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_config_aliases(
    nessie_spark_custom_separator: SparkSession,
    nessie_custom_separator_iceberg_rest_endpoint: str,
    catalog_name: str,
    namespace_id: str,
) -> None:
    config = _load_config(nessie_custom_separator_iceberg_rest_endpoint)
    _assert_rest_config_namespace_separator(config, NESSIE_NAMESPACE_SEPARATOR)

    nessie_spark_custom_separator.catalog.setCurrentCatalog(catalog_name)
    _exercise_namespace_separator(
        nessie_spark_custom_separator,
        namespace_id=namespace_id,
    )


@pytest.mark.parametrize(
    "catalog_name",
    [name for name, _ in NESSIE_NAMESPACE_SEPARATOR_CATALOGS],
)
@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_default_config_cannot_list_custom_separator_namespace(
    nessie_spark_incorrect_default_separator: SparkSession,
    nessie_spark_custom_separator: SparkSession,
    catalog_name: str,
) -> None:
    root_namespace = f"namespace_separator_custom_created_{catalog_name}"
    nested_namespace = f"{root_namespace}.child"
    deeply_nested_namespace = f"{nested_namespace}.grandchild"

    nessie_spark_custom_separator.catalog.setCurrentCatalog(catalog_name)
    nessie_spark_incorrect_default_separator.catalog.setCurrentCatalog(catalog_name)
    nessie_spark_custom_separator.sql(f"DROP DATABASE IF EXISTS {deeply_nested_namespace} CASCADE").collect()
    nessie_spark_custom_separator.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
    nessie_spark_custom_separator.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()

    try:
        nessie_spark_custom_separator.sql(f"CREATE DATABASE {root_namespace}").collect()
        nessie_spark_custom_separator.sql(f"CREATE DATABASE {nested_namespace}").collect()
        nessie_spark_custom_separator.sql(f"CREATE DATABASE {deeply_nested_namespace}").collect()

        assert nessie_spark_incorrect_default_separator.sql(f"SHOW DATABASES IN {nested_namespace}").collect() == []
    finally:
        with contextlib.suppress(Exception):
            nessie_spark_custom_separator.sql(f"DROP DATABASE IF EXISTS {deeply_nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            nessie_spark_custom_separator.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            nessie_spark_custom_separator.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()


@pytest.mark.parametrize(
    "catalog_name",
    [name for name, _ in NESSIE_NAMESPACE_SEPARATOR_CATALOGS],
)
@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_custom_config_cannot_list_default_separator_namespace(
    nessie_spark: SparkSession,
    nessie_spark_incorrect_custom_separator: SparkSession,
    catalog_name: str,
) -> None:
    root_namespace = f"namespace_separator_default_created_{catalog_name}"
    nested_namespace = f"{root_namespace}.child"
    deeply_nested_namespace = f"{nested_namespace}.grandchild"

    nessie_spark_incorrect_custom_separator.catalog.setCurrentCatalog(catalog_name)
    nessie_spark.sql(f"DROP DATABASE IF EXISTS {deeply_nested_namespace} CASCADE").collect()
    nessie_spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
    nessie_spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()

    try:
        nessie_spark.sql(f"CREATE DATABASE {root_namespace}").collect()
        nessie_spark.sql(f"CREATE DATABASE {nested_namespace}").collect()
        nessie_spark.sql(f"CREATE DATABASE {deeply_nested_namespace}").collect()

        assert nessie_spark_incorrect_custom_separator.sql(f"SHOW DATABASES IN {nested_namespace}").collect() == []
    finally:
        with contextlib.suppress(Exception):
            nessie_spark.sql(f"DROP DATABASE IF EXISTS {deeply_nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            nessie_spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            nessie_spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
