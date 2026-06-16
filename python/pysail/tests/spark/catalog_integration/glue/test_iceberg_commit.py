# ruff: noqa: S608
"""Iceberg commit tests for Glue catalog tables."""

from __future__ import annotations

import json
import re
import urllib.parse
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

import boto3
import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


pytestmark = pytest.mark.catalog_integration
UUID_METADATA_FILE_PATTERN = re.compile(
    r"^\d{5}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json$"
)


def _glue_parameters(moto_endpoint: str, database: str, table: str) -> dict[str, str]:
    client = _glue_client(moto_endpoint)
    return client.get_table(DatabaseName=database, Name=table)["Table"].get("Parameters", {})


def _glue_client(moto_endpoint: str):
    return boto3.client(
        "glue",
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
    )


def _update_glue_parameters(moto_endpoint: str, database: str, table: str, parameters: dict[str, str]) -> None:
    client = _glue_client(moto_endpoint)
    table_value = client.get_table(DatabaseName=database, Name=table)["Table"]
    table_input = {
        "Name": table_value["Name"],
        "Parameters": parameters,
        "StorageDescriptor": table_value["StorageDescriptor"],
        "PartitionKeys": table_value.get("PartitionKeys", []),
        "TableType": table_value.get("TableType"),
    }
    client.update_table(DatabaseName=database, TableInput={k: v for k, v in table_input.items() if v is not None})


def _metadata_location(moto_endpoint: str, database: str, table: str) -> str:
    properties = _glue_parameters(moto_endpoint, database, table)
    for key in ("metadata-location", "metadata_location"):
        if key in properties:
            return properties[key]
    raise AssertionError(properties)


def _metadata_filename(location: str) -> str:
    return PurePosixPath(urllib.parse.urlparse(location).path).name


def _assert_uuid_metadata_location(location: str, expected_version: int | None = None) -> None:
    filename = _metadata_filename(location)
    assert UUID_METADATA_FILE_PATTERN.match(filename), filename
    if expected_version is not None:
        assert filename.startswith(f"{expected_version:05}-"), filename


def _load_metadata_json(location: str) -> dict:
    parsed = urllib.parse.urlparse(location)
    assert parsed.scheme == "file", location
    return json.loads(Path(urllib.parse.unquote(parsed.path)).read_text(encoding="utf-8"))


def _current_schema_field_names(metadata: dict) -> list[str]:
    current_schema_id = metadata["current-schema-id"]
    current_schema = next(schema for schema in metadata["schemas"] if schema["schema-id"] == current_schema_id)
    return [field["name"] for field in current_schema["fields"]]


def test_ctas_records_glue_iceberg_metadata_location(
    glue_spark: SparkSession,
    moto_endpoint: str,
    tmp_path: Path,
) -> None:
    database = "glue_iceberg_ctas_db"
    table = "ctas_t"
    table_fqn = f"{database}.{table}"
    location = (tmp_path / "ctas_t").as_uri()

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(
            f"""
            CREATE TABLE {table_fqn}
            USING ICEBERG
            LOCATION '{location}'
            AS SELECT 1 AS id, 'a' AS name
            """
        )

        metadata_location = _metadata_location(moto_endpoint, database, table)
        assert metadata_location.startswith(location)
        _assert_uuid_metadata_location(metadata_location)

        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
        assert [(row.id, row.name) for row in rows] == [(1, "a")]
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


def test_insert_advances_glue_iceberg_metadata_location(
    glue_spark: SparkSession,
    moto_endpoint: str,
    tmp_path: Path,
) -> None:
    database = "glue_iceberg_commit_db"
    table = "commit_t"
    table_fqn = f"{database}.{table}"
    location = (tmp_path / "commit_t").as_uri()

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{location}'
            """
        )
        created_location = _metadata_location(moto_endpoint, database, table)
        assert created_location.startswith(location)
        _assert_uuid_metadata_location(created_location, 0)
        created_metadata = _load_metadata_json(created_location)
        assert created_metadata["current-snapshot-id"] == -1
        assert created_metadata["snapshots"] == []
        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
        assert rows == []

        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a'), (2, 'b')")
        first_location = _metadata_location(moto_endpoint, database, table)
        assert first_location != created_location
        assert first_location.startswith(location)
        _assert_uuid_metadata_location(first_location, 1)
        first_metadata = _load_metadata_json(first_location)
        assert len(first_metadata["metadata-log"]) == 1
        assert first_metadata["metadata-log"][0]["metadata-file"] == created_location

        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (3, 'c')")
        second_location = _metadata_location(moto_endpoint, database, table)
        assert second_location != first_location
        assert second_location.startswith(location)
        _assert_uuid_metadata_location(second_location, 2)
        second_metadata = _load_metadata_json(second_location)
        assert [entry["metadata-file"] for entry in second_metadata["metadata-log"]] == [
            created_location,
            first_location,
        ]

        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name) for row in rows] == [(1, "a"), (2, "b"), (3, "c")]
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


def test_merge_schema_append_advances_glue_iceberg_metadata_location(
    glue_spark: SparkSession,
    moto_endpoint: str,
    tmp_path: Path,
) -> None:
    database = "glue_iceberg_merge_schema_db"
    table = "merge_schema_t"
    table_fqn = f"{database}.{table}"
    location = (tmp_path / "merge_schema_t").as_uri()

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{location}'
            """
        )
        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")
        before_location = _metadata_location(moto_endpoint, database, table)
        _assert_uuid_metadata_location(before_location, 1)

        evolved = glue_spark.createDataFrame([(2, "b", 20)], schema="id INT, name STRING, age INT")
        (evolved.write.format("iceberg").mode("append").option("mergeSchema", "true").saveAsTable(table_fqn))

        after_location = _metadata_location(moto_endpoint, database, table)
        assert after_location != before_location
        assert after_location.startswith(location)
        _assert_uuid_metadata_location(after_location, 2)
        after_metadata = _load_metadata_json(after_location)
        assert after_metadata["metadata-log"][-1]["metadata-file"] == before_location
        assert _current_schema_field_names(after_metadata) == ["id", "name", "age"]

        rows = glue_spark.sql(f"SELECT id, name, age FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name, row.age) for row in rows] == [(1, "a", None), (2, "b", 20)]
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


def test_create_table_validation_runs_before_glue_iceberg_metadata_materialization(
    glue_spark: SparkSession,
    tmp_path: Path,
) -> None:
    database = "glue_iceberg_rejected_create_db"
    table = "bucket_t"
    table_fqn = f"{database}.{table}"
    table_path = tmp_path / table

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        with pytest.raises(Exception, match="BUCKET BY"):
            glue_spark.sql(
                f"""
                CREATE TABLE {table_fqn} (
                  id INT,
                  name STRING
                )
                USING ICEBERG
                LOCATION '{table_path.as_uri()}'
                CLUSTERED BY (id) INTO 4 BUCKETS
                """
            )

        assert not (table_path / "metadata").exists()
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


def test_sail_reads_and_appends_glue_iceberg_table_with_jvm_style_marker(
    glue_spark: SparkSession,
    moto_endpoint: str,
    tmp_path: Path,
) -> None:
    database = "glue_iceberg_jvm_marker_db"
    table = "commit_t"
    table_fqn = f"{database}.{table}"
    location = (tmp_path / "jvm_marker").as_uri()

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{location}'
            """
        )
        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")
        first_location = _metadata_location(moto_endpoint, database, table)

        parameters = _glue_parameters(moto_endpoint, database, table)
        parameters.pop("table_type", None)
        parameters.pop("classification", None)
        parameters["TABLE_TYPE"] = "ICEBERG"
        _update_glue_parameters(moto_endpoint, database, table, parameters)
        parameters = _glue_parameters(moto_endpoint, database, table)
        assert parameters.get("TABLE_TYPE") == "ICEBERG"
        assert "table_type" not in parameters

        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
        assert [(row.id, row.name) for row in rows] == [(1, "a")]

        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (2, 'b')")
        second_location = _metadata_location(moto_endpoint, database, table)
        assert second_location != first_location
        _assert_uuid_metadata_location(second_location)
        second_metadata = _load_metadata_json(second_location)
        assert second_metadata["metadata-log"][-1]["metadata-file"] == first_location

        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name) for row in rows] == [(1, "a"), (2, "b")]
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


def test_glue_rejects_stale_iceberg_metadata_location_update(
    glue_spark: SparkSession,
    moto_endpoint: str,
    tmp_path: Path,
) -> None:
    database = "glue_iceberg_stale_commit_db"
    table = "commit_t"
    table_fqn = f"{database}.{table}"
    location = (tmp_path / "stale_commit").as_uri()

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING ICEBERG
            LOCATION '{location}'
            """
        )
        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")
        current_location = _metadata_location(moto_endpoint, database, table)

        with pytest.raises(Exception, match="base metadata location"):
            glue_spark.sql(
                f"""
                ALTER TABLE {table_fqn}
                SET TBLPROPERTIES (
                  'metadata-location' = '{current_location}.new',
                  'previous_metadata_location' = '{current_location}.stale'
                )
                """
            )

        assert _metadata_location(moto_endpoint, database, table) == current_location
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
