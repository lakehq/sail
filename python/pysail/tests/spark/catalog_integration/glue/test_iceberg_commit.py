# ruff: noqa: S608
"""Iceberg commit tests for Glue catalog tables."""

from __future__ import annotations

import urllib.parse
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

import boto3
import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


pytestmark = pytest.mark.catalog_integration


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

        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a'), (2, 'b')")
        first_location = _metadata_location(moto_endpoint, database, table)
        assert first_location.startswith(location)
        assert not _metadata_filename(first_location).startswith("v")

        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (3, 'c')")
        second_location = _metadata_location(moto_endpoint, database, table)
        assert second_location != first_location
        assert second_location.startswith(location)
        assert not _metadata_filename(second_location).startswith("v")

        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name) for row in rows] == [(1, "a"), (2, "b"), (3, "c")]
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
        assert not _metadata_filename(second_location).startswith("v")

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
