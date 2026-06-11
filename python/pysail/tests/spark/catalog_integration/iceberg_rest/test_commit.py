from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_commit_test"


@pytest.fixture(scope="module", autouse=True)
def namespace(iceberg_spark: SparkSession) -> Generator[None, None, None]:
    iceberg_spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    yield
    iceberg_spark.sql(f"DROP DATABASE IF EXISTS {NAMESPACE} CASCADE")


def _load_table(iceberg_rest_endpoint: str, table_name: str) -> dict:
    namespace = urllib.parse.quote(NAMESPACE, safe="")
    table = urllib.parse.quote(table_name, safe="")
    url = f"{iceberg_rest_endpoint}/v1/namespaces/{namespace}/tables/{table}"
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return json.load(response)


def test_ctas_records_rest_catalog_metadata_location(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "ctas_t"
    iceberg_spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{table_name}")
    iceberg_spark.sql(
        f"""
        CREATE TABLE {NAMESPACE}.{table_name}
        USING iceberg
        AS SELECT 1 AS id, 'a' AS name
        """
    )

    table = _load_table(iceberg_rest_endpoint, table_name)
    metadata_location = table["metadata-location"]
    assert metadata_location
    assert not PurePosixPath(metadata_location).name.startswith("v")
    assert table["metadata"]["current-snapshot-id"] is not None

    rows = iceberg_spark.sql(f"SELECT id, name FROM {NAMESPACE}.{table_name}").collect()  # noqa: S608
    assert [(row["id"], row["name"]) for row in rows] == [(1, "a")]


def test_insert_advances_rest_catalog_metadata_location(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "commit_t"
    iceberg_spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{table_name}")
    iceberg_spark.sql(
        f"""
        CREATE TABLE {NAMESPACE}.{table_name} (
          id INT,
          name STRING
        )
        USING iceberg
        """
    )

    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]
    assert before["metadata"].get("current-snapshot-id") in (None, -1)
    rows = iceberg_spark.sql(f"SELECT id, name FROM {NAMESPACE}.{table_name}").collect()  # noqa: S608
    assert rows == []

    iceberg_spark.sql(f"INSERT INTO {NAMESPACE}.{table_name} VALUES (1, 'a'), (2, 'b')")  # noqa: S608

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_location = after["metadata-location"]

    assert after_location != before_location
    assert not PurePosixPath(after_location).name.startswith("v")
    assert after["metadata"]["current-snapshot-id"] is not None

    iceberg_spark.sql(f"INSERT INTO {NAMESPACE}.{table_name} VALUES (3, 'c')")  # noqa: S608
    appended = _load_table(iceberg_rest_endpoint, table_name)
    assert appended["metadata-location"] != after_location
    assert not PurePosixPath(appended["metadata-location"]).name.startswith("v")

    rows = iceberg_spark.sql(f"SELECT id, name FROM {NAMESPACE}.{table_name} ORDER BY id").collect()  # noqa: S608
    assert [(row["id"], row["name"]) for row in rows] == [(1, "a"), (2, "b"), (3, "c")]


def test_rest_catalog_rejects_non_iceberg_create_format(
    iceberg_spark: SparkSession,
) -> None:
    table_name = "delta_bad_t"
    iceberg_spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{table_name}")

    with pytest.raises(Exception, match=r"(?i)Iceberg REST catalog cannot create 'delta' tables"):
        iceberg_spark.sql(
            f"""
            CREATE TABLE {NAMESPACE}.{table_name} (
              id INT
            )
            USING DELTA
            """
        )
