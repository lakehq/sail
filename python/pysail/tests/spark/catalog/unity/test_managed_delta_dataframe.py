# ruff: noqa: S608
"""DataFrame API tests for Unity Catalog managed Delta tables."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from pysail.testing.spark.utils.common import pyspark_version
from pysail.tests.spark.catalog.unity.conftest import _unity_delta_commit_info, _unity_table_info

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _expected_struct_field(name: str, data_type: str) -> dict:
    return {
        "metadata": {},
        "name": name,
        "nullable": True,
        "type": data_type,
    }


def _struct_field_type_json_by_name(unity_rest_url: str, table_name: str) -> dict[str, dict]:
    table_info = _unity_table_info(unity_rest_url, table_name)
    columns = table_info.get("columns") or []
    actual: dict[str, dict] = {}
    for column in columns:
        name = column.get("name")
        assert isinstance(name, str), f"Unity Catalog column has invalid name: {column!r}"
        raw_type_json = column.get("type_json")
        assert isinstance(raw_type_json, str), f"Unity Catalog column has invalid type_json: {column!r}"
        type_json = json.loads(raw_type_json)
        assert isinstance(type_json, dict), f"Unity Catalog type_json is not a StructField JSON object: {type_json!r}"
        actual[name] = type_json
    return actual


def test_dataframe_append_and_merge_schema_are_unity_managed(
    spark: SparkSession,
    unity_rest_url: str,
) -> None:
    database = "unity_dataframe_test"
    table = "managed_delta_df_t"
    table_fqn = f"{database}.{table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING delta
            """
        )

        (
            spark.createDataFrame([(1, "one")], schema="id INT, name STRING")
            .write.format("delta")
            .mode("append")
            .saveAsTable(table_fqn)
        )

        first_commit = _unity_delta_commit_info(unity_rest_url, table_fqn, 1)
        assert first_commit

        evolved = spark.createDataFrame([(2, "two", 20)], schema="id INT, name STRING, age INT")
        (evolved.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_fqn))

        second_commit = _unity_delta_commit_info(unity_rest_url, table_fqn, 2)
        assert second_commit

        rows = spark.sql(f"SELECT id, name, age FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name, row.age) for row in rows] == [(1, "one", None), (2, "two", 20)]

        table_info = _unity_table_info(unity_rest_url, table_fqn)
        columns = table_info.get("columns") or []
        assert [(column.get("name"), column.get("type_name")) for column in columns] == [
            ("id", "INT"),
            ("name", "STRING"),
            ("age", "INT"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(f"DROP SCHEMA IF EXISTS {database} CASCADE")


def test_managed_delta_create_and_schema_merge_write_spark_struct_field_type_json(
    spark: SparkSession,
    unity_rest_url: str,
) -> None:
    database = "unity_dataframe_type_json_test"
    table = "managed_delta_type_json_t"
    table_fqn = f"{database}.{table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING delta
            """
        )

        assert _struct_field_type_json_by_name(unity_rest_url, table_fqn) == {
            "id": _expected_struct_field("id", "integer"),
            "name": _expected_struct_field("name", "string"),
        }

        evolved = spark.createDataFrame([(1, "one", "new")], schema="id INT, name STRING, extra STRING")
        (evolved.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_fqn))

        assert _struct_field_type_json_by_name(unity_rest_url, table_fqn) == {
            "id": _expected_struct_field("id", "integer"),
            "name": _expected_struct_field("name", "string"),
            "extra": _expected_struct_field("extra", "string"),
        }
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(f"DROP SCHEMA IF EXISTS {database} CASCADE")


@pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="DataFrame arguments in spark.sql() require Spark 4+",
)
def test_spark_sql_merge_dataframe_source_is_unity_managed(
    spark: SparkSession,
    unity_rest_url: str,
) -> None:
    database = "unity_dataframe_sql_args_test"
    table = "managed_delta_sql_args_merge_t"
    table_fqn = f"{database}.{table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(
            f"""
            CREATE TABLE {table_fqn}
            USING delta
            AS SELECT * FROM VALUES
              (1, 'old'),
              (2, 'keep')
            AS t(id, name)
            """
        )

        source_df = spark.createDataFrame([(1, "new"), (3, "insert")], schema="id INT, name STRING")
        spark.sql(
            f"""
            MERGE INTO {table_fqn} AS tgt
            USING {{src_df}} AS src
            ON tgt.id = src.id
            WHEN MATCHED THEN UPDATE SET name = src.name
            WHEN NOT MATCHED THEN INSERT *
            """,
            src_df=source_df,
        )

        rows = spark.sql(f"SELECT id, name FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name) for row in rows] == [(1, "new"), (2, "keep"), (3, "insert")]

        assert _unity_delta_commit_info(unity_rest_url, table_fqn, 2)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(f"DROP SCHEMA IF EXISTS {database} CASCADE")
