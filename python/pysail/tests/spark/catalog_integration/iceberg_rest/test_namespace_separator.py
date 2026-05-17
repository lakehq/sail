from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

    from pyspark.sql import SparkSession


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def _load_namespace(
    iceberg_rest_endpoint: str,
    namespace_parts: list[str],
    separator: str,
) -> dict[str, object]:
    namespace = _quote(separator.join(namespace_parts))
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def _sail_namespace_rows(spark: SparkSession, namespace: str) -> dict[str, str]:
    return {row.info_name: row.info_value for row in spark.sql(f"DESCRIBE DATABASE {namespace}").collect()}


def _assert_namespace_visible(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    namespace_parts: list[str],
    separator: str,
) -> None:
    _assert_sail_namespace_visible(spark, namespace_parts)
    assert _load_namespace(iceberg_rest_endpoint, namespace_parts, separator)["namespace"] == namespace_parts


def _assert_sail_namespace_visible(
    spark: SparkSession,
    namespace_parts: list[str],
) -> None:
    namespace = ".".join(namespace_parts)

    rows = _sail_namespace_rows(spark, namespace)
    assert rows["Namespace Name"] == namespace

    listed_namespaces = {row.name for row in spark.sql(f"SHOW DATABASES LIKE '{namespace}'").collect()}
    assert namespace in listed_namespaces


def _exercise_namespace_separator(
    spark: SparkSession,
    iceberg_rest_endpoint: str,
    *,
    namespace_id: str,
    separator: str,
) -> None:
    parent = f"namespace_separator_{namespace_id}"
    child = "child"
    namespace = f"{parent}.{child}"

    spark.sql(f"DROP DATABASE IF EXISTS {parent} CASCADE")
    try:
        spark.sql(f"CREATE DATABASE {parent}")
        spark.sql(f"CREATE DATABASE {namespace}")

        _assert_namespace_visible(
            spark,
            iceberg_rest_endpoint,
            [parent, child],
            separator,
        )
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {parent} CASCADE")


@pytest.mark.parametrize(
    ("config_key", "namespace_id"),
    [
        ("namespace_separator", "snake_case"),
        ("namespace-separator", "kebab_case"),
        ("namespaceSeparator", "camel_case"),
        ("namespaceseparator", "flat_case"),
    ],
)
def test_namespace_separator_config_aliases(
    iceberg_spark_factory: Callable[[str, str], AbstractContextManager[SparkSession]],
    iceberg_rest_endpoint: str,
    config_key: str,
    namespace_id: str,
) -> None:
    separator = "::"

    with iceberg_spark_factory(
        f'{config_key}="{separator}"',
        f"iceberg_rest_namespace_separator_{namespace_id}",
    ) as spark:
        _exercise_namespace_separator(
            spark,
            iceberg_rest_endpoint,
            namespace_id=namespace_id,
            separator=separator,
        )


@pytest.mark.parametrize(
    "separator",
    [
        "::",
        "/",
    ],
)
def test_namespace_separator_custom_values(
    iceberg_spark_factory: Callable[[str, str], AbstractContextManager[SparkSession]],
    iceberg_rest_endpoint: str,
    separator: str,
) -> None:
    namespace_id = "slash" if separator == "/" else "colon_colon"

    with iceberg_spark_factory(
        f'namespace_separator="{separator}"',
        f"iceberg_rest_namespace_separator_{namespace_id}",
    ) as spark:
        _exercise_namespace_separator(
            spark,
            iceberg_rest_endpoint,
            namespace_id=namespace_id,
            separator=separator,
        )
