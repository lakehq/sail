# ruff: noqa: S608
"""Delta commit tests for Glue catalog tables."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import boto3

from pysail.testing.spark.utils.sql import escape_sql_string_literal

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def _glue_client(moto_endpoint: str):
    return boto3.client(
        "glue",
        region_name="us-east-1",
        endpoint_url=moto_endpoint,
    )


def _glue_parameters(moto_endpoint: str, database: str, table: str) -> dict[str, str]:
    return _glue_client(moto_endpoint).get_table(DatabaseName=database, Name=table)["Table"].get("Parameters", {})


def _delta_actions(table_path: Path, version: int) -> list[dict]:
    commit = table_path / "_delta_log" / f"{version:020}.json"
    assert commit.exists(), f"missing Delta commit: {commit}"
    return [json.loads(line) for line in commit.read_text(encoding="utf-8").splitlines() if line]


def test_create_table_materializes_delta_log_and_marks_glue_provider(
    glue_spark: SparkSession,
    moto_endpoint: str,
    tmp_path: Path,
) -> None:
    database = "glue_delta_create_db"
    table = "delta_t"
    table_fqn = f"{database}.{table}"
    location_path = tmp_path / "delta_t"
    location = location_path.as_uri()

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(location)}'
            """
        )

        parameters = _glue_parameters(moto_endpoint, database, table)
        assert parameters["spark.sql.sources.provider"] == "delta"

        actions = _delta_actions(location_path, 0)
        assert any("protocol" in action for action in actions)
        assert any("metaData" in action for action in actions)
        assert not any("add" in action for action in actions)

        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
        assert rows == []

        glue_spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")
        _delta_actions(location_path, 1)

        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name) for row in rows] == [(1, "a")]
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


def test_create_table_if_not_exists_does_not_materialize_new_delta_location(
    glue_spark: SparkSession,
    moto_endpoint: str,
    tmp_path: Path,
) -> None:
    database = "glue_delta_if_not_exists_db"
    table = "delta_t"
    table_fqn = f"{database}.{table}"
    location_path = tmp_path / "delta_t"
    alternate_path = tmp_path / "delta_t_alternate"

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(location_path.as_uri())}'
            """
        )
        assert (location_path / "_delta_log" / "00000000000000000000.json").exists()

        glue_spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_fqn} (
              id INT,
              name STRING
            )
            USING DELTA
            LOCATION '{escape_sql_string_literal(alternate_path.as_uri())}'
            """
        )

        assert not (alternate_path / "_delta_log").exists()
        parameters = _glue_parameters(moto_endpoint, database, table)
        assert parameters["spark.sql.sources.provider"] == "delta"
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


def test_ctas_materializes_delta_log_and_marks_glue_provider(
    glue_spark: SparkSession,
    moto_endpoint: str,
    tmp_path: Path,
) -> None:
    database = "glue_delta_ctas_db"
    table = "delta_ctas_t"
    table_fqn = f"{database}.{table}"
    location_path = tmp_path / "delta_ctas_t"
    location = location_path.as_uri()

    glue_spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    try:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(
            f"""
            CREATE TABLE {table_fqn}
            USING DELTA
            LOCATION '{escape_sql_string_literal(location)}'
            AS SELECT 2 AS id, 'b' AS name
            """
        )

        parameters = _glue_parameters(moto_endpoint, database, table)
        assert parameters["spark.sql.sources.provider"] == "delta"

        actions = _delta_actions(location_path, 0)
        assert any("protocol" in action for action in actions)
        assert any("metaData" in action for action in actions)
        assert any("add" in action for action in actions)

        rows = glue_spark.sql(f"SELECT id, name FROM {table_fqn}").collect()
        assert [(row.id, row.name) for row in rows] == [(2, "b")]
    finally:
        glue_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        glue_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
