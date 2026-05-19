from __future__ import annotations

import contextlib
import json
import urllib.parse
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


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _load_config(iceberg_rest_endpoint: str) -> dict[str, object]:
    url = f"{iceberg_rest_endpoint}/v1/config"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def _list_namespaces(
    iceberg_rest_endpoint: str,
    separator: str,
    *,
    prefix: str | None = None,
    parent: list[str] | None = None,
) -> dict[str, object]:
    return _list_namespaces_with_parent_separator(
        iceberg_rest_endpoint,
        prefix=prefix,
        parent=parent,
        parent_separator=separator,
    )


def _list_namespaces_with_parent_separator(
    iceberg_rest_endpoint: str,
    *,
    prefix: str | None = None,
    parent: list[str] | None = None,
    parent_separator: str,
) -> dict[str, object]:
    prefix_path = f"/{_quote(prefix)}" if prefix is not None else ""
    query = ""
    if parent is not None:
        parent_namespace = parent_separator.join(parent)
        query = f"?parent={_quote(parent_namespace)}"
    url = f"{iceberg_rest_endpoint}/v1{prefix_path}/namespaces{query}"
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


def _rest_config_prefix(config: dict[str, object]) -> str | None:
    defaults = config.get("defaults", {})
    overrides = config.get("overrides", {})
    assert isinstance(defaults, dict), config
    assert isinstance(overrides, dict), config
    prefix = overrides.get("prefix", defaults.get("prefix"))
    assert prefix is None or isinstance(prefix, str), config
    return prefix


def _assert_namespace_listed(
    iceberg_rest_endpoint: str,
    namespace_parts: list[str],
    separator: str,
    *,
    prefix: str | None = None,
) -> None:
    root_namespaces = _list_namespaces(iceberg_rest_endpoint, separator, prefix=prefix)
    assert namespace_parts[:1] in root_namespaces["namespaces"]

    for depth in range(1, len(namespace_parts)):
        parent = namespace_parts[:depth]
        child = namespace_parts[: depth + 1]
        child_namespaces = _list_namespaces(
            iceberg_rest_endpoint,
            separator,
            prefix=prefix,
            parent=parent,
        )
        if child not in child_namespaces["namespaces"] and len(parent) > 1:
            child_namespaces = _list_namespaces_with_parent_separator(
                iceberg_rest_endpoint,
                prefix=prefix,
                parent=parent,
                parent_separator="\x1f",
            )
        assert child in child_namespaces["namespaces"]


def _exercise_namespace_separator(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    *,
    namespace_id: str,
    separator: str,
    prefix: str | None = None,
) -> None:
    namespace_parts = [f"namespace_separator_{namespace_id}", "child", "grandchild"]
    root_namespace = namespace_parts[0]
    nested_namespace = ".".join(namespace_parts[:2])
    deeply_nested_namespace = ".".join(namespace_parts)

    spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE")
    try:
        spark.sql(f"CREATE DATABASE {root_namespace}")
        spark.sql(f"CREATE DATABASE {nested_namespace}")
        spark.sql(f"CREATE DATABASE {deeply_nested_namespace}")
        _assert_namespace_listed(
            iceberg_rest_endpoint,
            namespace_parts,
            separator,
            prefix=prefix,
        )
    finally:
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {deeply_nested_namespace} CASCADE")
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE")
        with contextlib.suppress(Exception):
            spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE")


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
        nessie_custom_separator_iceberg_rest_endpoint,
        namespace_id=namespace_id,
        separator=NESSIE_NAMESPACE_SEPARATOR,
        prefix=_rest_config_prefix(config),
    )


@pytest.mark.parametrize(
    "catalog_name",
    [name for name, _ in NESSIE_NAMESPACE_SEPARATOR_CATALOGS],
)
@pytest.mark.usefixtures("_create_s3_bucket")
def test_namespace_separator_default_config_cannot_list_custom_separator_namespace(
    nessie_spark_incorrect_default_separator: SparkSession,
    nessie_spark_custom_separator: SparkSession,
    nessie_custom_separator_iceberg_rest_endpoint: str,
    catalog_name: str,
) -> None:
    root_namespace = f"namespace_separator_custom_created_{catalog_name}"
    nested_namespace = f"{root_namespace}.child"
    namespace_parts = [root_namespace, "child"]

    nessie_spark_custom_separator.catalog.setCurrentCatalog(catalog_name)
    nessie_spark_custom_separator.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
    nessie_spark_custom_separator.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()

    try:
        nessie_spark_custom_separator.sql(f"CREATE DATABASE {root_namespace}").collect()
        nessie_spark_custom_separator.sql(f"CREATE DATABASE {nested_namespace}").collect()

        _assert_namespace_listed(
            nessie_custom_separator_iceberg_rest_endpoint,
            namespace_parts,
            NESSIE_NAMESPACE_SEPARATOR,
        )

        default_separator_namespaces = _list_namespaces(
            nessie_custom_separator_iceberg_rest_endpoint,
            "\x1f",
            parent=[root_namespace],
        )
        assert namespace_parts not in default_separator_namespaces["namespaces"]

        sail_default_separator_namespaces = [
            row.name
            for row in nessie_spark_incorrect_default_separator.sql(f"SHOW DATABASES IN {root_namespace}").collect()
        ]
        assert "child" not in sail_default_separator_namespaces
        assert nested_namespace not in sail_default_separator_namespaces
    finally:
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
    nessie_iceberg_rest_endpoint: str,
    catalog_name: str,
) -> None:
    root_namespace = f"namespace_separator_default_created_{catalog_name}"
    nested_namespace = f"{root_namespace}.child"
    namespace_parts = [root_namespace, "child"]

    nessie_spark_incorrect_custom_separator.catalog.setCurrentCatalog(catalog_name)
    nessie_spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
    nessie_spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()

    try:
        nessie_spark.sql(f"CREATE DATABASE {root_namespace}").collect()
        nessie_spark.sql(f"CREATE DATABASE {nested_namespace}").collect()

        _assert_namespace_listed(
            nessie_iceberg_rest_endpoint,
            namespace_parts,
            "\x1f",
        )

        custom_separator_namespaces = _list_namespaces(
            nessie_iceberg_rest_endpoint,
            NESSIE_NAMESPACE_SEPARATOR,
            parent=[root_namespace],
        )
        assert namespace_parts not in custom_separator_namespaces["namespaces"]

        sail_custom_separator_namespaces = [
            row.name
            for row in nessie_spark_incorrect_custom_separator.sql(f"SHOW DATABASES IN {root_namespace}").collect()
        ]
        assert "child" not in sail_custom_separator_namespaces
        assert nested_namespace not in sail_custom_separator_namespaces
    finally:
        with contextlib.suppress(Exception):
            nessie_spark.sql(f"DROP DATABASE IF EXISTS {nested_namespace} CASCADE").collect()
        with contextlib.suppress(Exception):
            nessie_spark.sql(f"DROP DATABASE IF EXISTS {root_namespace} CASCADE").collect()
