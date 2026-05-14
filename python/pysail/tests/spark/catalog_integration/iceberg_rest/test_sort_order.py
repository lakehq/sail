from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_sort_order_test"


@pytest.fixture(scope="module", autouse=True)
def namespace(iceberg_spark: SparkSession) -> Generator[None, None, None]:
    iceberg_spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    yield
    iceberg_spark.sql(f"DROP DATABASE IF EXISTS {NAMESPACE} CASCADE")


def _load_table_metadata(iceberg_rest_endpoint: str, table_name: str) -> dict:
    namespace = urllib.parse.quote(NAMESPACE, safe="")
    table = urllib.parse.quote(table_name, safe="")
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/tables/{table}"
    # Safe: the URL is built from the controlled Iceberg REST fixture endpoint.
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)["metadata"]


def _current_schema_fields(metadata: dict) -> dict[str, int]:
    current_schema_id = metadata["current-schema-id"]
    schema = next(schema for schema in metadata["schemas"] if schema["schema-id"] == current_schema_id)
    return {field["name"]: field["id"] for field in schema["fields"]}


def _default_sort_fields(metadata: dict) -> list[dict]:
    default_sort_order_id = metadata["default-sort-order-id"]
    sort_order = next(order for order in metadata["sort-orders"] if order["order-id"] == default_sort_order_id)
    return sort_order["fields"]


def _create_sorted_table(spark: SparkSession, table_name: str, sort_clause: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{table_name}")
    spark.sql(
        f"""
        CREATE TABLE {NAMESPACE}.{table_name} (
          id INT,
          ts TIMESTAMP,
          d DATE,
          name STRING,
          category STRING
        )
        USING iceberg
        CLUSTERED BY (id) SORTED BY ({sort_clause}) INTO 8 BUCKETS
        """
    )


@pytest.mark.parametrize(
    ("table_name", "sort_clause", "expected"),
    [
        (
            "sort_identity",
            "id ASC, name DESC, category",
            [
                ("id", "identity", "asc"),
                ("name", "identity", "desc"),
                ("category", "identity", "asc"),
            ],
        ),
        (
            "sort_temporal_plural",
            "years(ts) ASC, months(ts) DESC, days(d) ASC, hours(ts) DESC",
            [
                ("ts", "year", "asc"),
                ("ts", "month", "desc"),
                ("d", "day", "asc"),
                ("ts", "hour", "desc"),
            ],
        ),
        (
            "sort_temporal_singular",
            "year(ts) DESC, month(ts) ASC, day(d) DESC, hour(ts) ASC",
            [
                ("ts", "year", "desc"),
                ("ts", "month", "asc"),
                ("d", "day", "desc"),
                ("ts", "hour", "asc"),
            ],
        ),
        (
            "sort_bucket_truncate",
            "bucket(16, id) ASC, truncate(3, name) DESC, truncate(category, 4) ASC",
            [
                ("id", "bucket[16]", "asc"),
                ("name", "truncate[3]", "desc"),
                ("category", "truncate[4]", "asc"),
            ],
        ),
    ],
)
def test_create_table_sort_order_transforms(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
    table_name: str,
    sort_clause: str,
    expected: list[tuple[str, str, str]],
) -> None:
    _create_sorted_table(iceberg_spark, table_name, sort_clause)

    metadata = _load_table_metadata(iceberg_rest_endpoint, table_name)
    field_ids = _current_schema_fields(metadata)
    sort_fields = _default_sort_fields(metadata)

    assert len(sort_fields) == len(expected)
    for actual, (column, transform, direction) in zip(sort_fields, expected, strict=True):
        assert actual["source-id"] == field_ids[column]
        assert actual["transform"] == transform
        assert actual["direction"] == direction
        assert actual["null-order"] == "nulls-last"


@pytest.mark.parametrize(
    ("table_name", "sort_clause", "error"),
    [
        ("sort_unknown_transform", "unknown(ts)", "unsupported sort transform function"),
        ("sort_year_extra_arg", "years(ts, id)", "expects a single column"),
        ("sort_bucket_missing_column", "bucket(16)", "requires argument at index 1"),
        ("sort_bucket_bad_count", "bucket(name, id)", "bucket count must be an integer literal"),
        ("sort_truncate_missing_width", "truncate(name)", "truncate sort transform expects"),
        ("sort_truncate_bad_width", "truncate(name, category)", "truncate sort transform expects"),
    ],
)
def test_create_table_sort_order_transform_errors(
    iceberg_spark: SparkSession,
    table_name: str,
    sort_clause: str,
    error: str,
) -> None:
    with pytest.raises(Exception, match=error):
        _create_sorted_table(iceberg_spark, table_name, sort_clause)
