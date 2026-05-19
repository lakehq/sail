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
    client = boto3.client(
        "glue",
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
    )
    return client.get_table(DatabaseName=database, Name=table)["Table"].get("Parameters", {})


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
