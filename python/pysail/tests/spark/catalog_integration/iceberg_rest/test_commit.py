from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator

    from pyspark.sql import SparkSession


NAMESPACE = "iceberg_commit_test"
UUID_METADATA_FILE_PATTERN = re.compile(
    r"^\d{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json$"
)


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


def _assert_uuid_metadata_location(metadata_location: str, expected_version: int | None = None) -> None:
    filename = PurePosixPath(metadata_location).name
    assert UUID_METADATA_FILE_PATTERN.match(filename), filename
    if expected_version is not None:
        assert filename.startswith(f"{expected_version:05}-"), filename


def _current_schema_field_names(metadata: dict) -> list[str]:
    current_schema_id = metadata["current-schema-id"]
    current_schema = next(schema for schema in metadata["schemas"] if schema["schema-id"] == current_schema_id)
    return [field["name"] for field in current_schema["fields"]]


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
    _assert_uuid_metadata_location(metadata_location)
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
    _assert_uuid_metadata_location(before_location, 0)
    assert before["metadata"]["current-snapshot-id"] == -1
    assert before["metadata"]["snapshots"] == []
    rows = iceberg_spark.sql(f"SELECT id, name FROM {NAMESPACE}.{table_name}").collect()  # noqa: S608
    assert rows == []

    iceberg_spark.sql(f"INSERT INTO {NAMESPACE}.{table_name} VALUES (1, 'a'), (2, 'b')")  # noqa: S608

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_location = after["metadata-location"]

    assert after_location != before_location
    _assert_uuid_metadata_location(after_location, 1)
    assert after["metadata"]["current-snapshot-id"] is not None
    assert len(after["metadata"]["metadata-log"]) == 1
    assert after["metadata"]["metadata-log"][0]["metadata-file"] == before_location

    iceberg_spark.sql(f"INSERT INTO {NAMESPACE}.{table_name} VALUES (3, 'c')")  # noqa: S608
    appended = _load_table(iceberg_rest_endpoint, table_name)
    assert appended["metadata-location"] != after_location
    _assert_uuid_metadata_location(appended["metadata-location"], 2)
    assert [entry["metadata-file"] for entry in appended["metadata"]["metadata-log"]] == [
        before_location,
        after_location,
    ]

    rows = iceberg_spark.sql(f"SELECT id, name FROM {NAMESPACE}.{table_name} ORDER BY id").collect()  # noqa: S608
    assert [(row["id"], row["name"]) for row in rows] == [(1, "a"), (2, "b"), (3, "c")]


def test_merge_schema_append_advances_rest_catalog_metadata_location(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "merge_schema_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    iceberg_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    iceberg_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING iceberg
        """
    )
    iceberg_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")  # noqa: S608
    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]
    _assert_uuid_metadata_location(before_location, 1)

    evolved = iceberg_spark.createDataFrame([(2, "b", 20)], schema="id INT, name STRING, age INT")
    (evolved.write.format("iceberg").mode("append").option("mergeSchema", "true").saveAsTable(table_fqn))

    after = _load_table(iceberg_rest_endpoint, table_name)
    after_location = after["metadata-location"]
    assert after_location != before_location
    _assert_uuid_metadata_location(after_location, 2)
    assert after["metadata"]["metadata-log"][-1]["metadata-file"] == before_location
    assert _current_schema_field_names(after["metadata"]) == ["id", "name", "age"]

    rows = iceberg_spark.sql(f"SELECT id, name, age FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row["id"], row["name"], row["age"]) for row in rows] == [(1, "a", None), (2, "b", 20)]


def test_insert_overwrite_advances_rest_catalog_metadata_location(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "overwrite_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    iceberg_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    iceberg_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT,
          name STRING
        )
        USING iceberg
        """
    )
    created = _load_table(iceberg_rest_endpoint, table_name)
    created_location = created["metadata-location"]
    _assert_uuid_metadata_location(created_location, 0)

    iceberg_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'old'), (2, 'old')")  # noqa: S608
    before_overwrite = _load_table(iceberg_rest_endpoint, table_name)
    before_overwrite_location = before_overwrite["metadata-location"]
    _assert_uuid_metadata_location(before_overwrite_location, 1)

    iceberg_spark.sql(f"INSERT OVERWRITE TABLE {table_fqn} VALUES (3, 'new'), (4, 'new')")  # noqa: S608
    after_overwrite = _load_table(iceberg_rest_endpoint, table_name)
    after_overwrite_location = after_overwrite["metadata-location"]
    assert after_overwrite_location != before_overwrite_location
    _assert_uuid_metadata_location(after_overwrite_location, 2)
    assert [entry["metadata-file"] for entry in after_overwrite["metadata"]["metadata-log"]] == [
        created_location,
        before_overwrite_location,
    ]
    assert after_overwrite["metadata"]["snapshots"][-1]["summary"]["operation"] == "overwrite"

    rows = iceberg_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()  # noqa: S608
    assert [(row["id"], row["name"]) for row in rows] == [(3, "new"), (4, "new")]


def test_rest_catalog_rejects_catalog_managed_iceberg_alter(
    iceberg_spark: SparkSession,
    iceberg_rest_endpoint: str,
) -> None:
    table_name = "alter_reject_t"
    table_fqn = f"{NAMESPACE}.{table_name}"
    iceberg_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
    iceberg_spark.sql(
        f"""
        CREATE TABLE {table_fqn} (
          id INT
        )
        USING iceberg
        """
    )
    before = _load_table(iceberg_rest_endpoint, table_name)
    before_location = before["metadata-location"]

    with pytest.raises(Exception, match="catalog-managed Iceberg tables"):
        iceberg_spark.sql(
            f"""
            ALTER TABLE {table_fqn}
            SET TBLPROPERTIES ('owner' = 'alice')
            """
        )

    after = _load_table(iceberg_rest_endpoint, table_name)
    assert after["metadata-location"] == before_location


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
