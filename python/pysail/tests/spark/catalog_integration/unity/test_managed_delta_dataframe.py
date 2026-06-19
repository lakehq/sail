# ruff: noqa: S608
"""DataFrame API tests for Unity Catalog managed Delta tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pysail.tests.spark.catalog_integration.unity.conftest import _unity_delta_commit_info, _unity_table_info

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_dataframe_append_and_merge_schema_are_unity_managed(
    unity_spark: SparkSession,
    unity_rest_url: str,
) -> None:
    database = "unity_dataframe_test"
    table = "managed_delta_df_t"
    table_fqn = f"{database}.{table}"

    unity_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
    try:
        unity_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        unity_spark.sql(
            f"""
            CREATE TABLE {table_fqn} (
              id INT,
              name STRING
            )
            USING delta
            """
        )

        (
            unity_spark.createDataFrame([(1, "one")], schema="id INT, name STRING")
            .write.format("delta")
            .mode("append")
            .saveAsTable(table_fqn)
        )

        first_commit = _unity_delta_commit_info(unity_rest_url, table_fqn, 1)
        assert first_commit

        evolved = unity_spark.createDataFrame([(2, "two", 20)], schema="id INT, name STRING, age INT")
        (evolved.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_fqn))

        second_commit = _unity_delta_commit_info(unity_rest_url, table_fqn, 2)
        assert second_commit

        rows = unity_spark.sql(f"SELECT id, name, age FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name, row.age) for row in rows] == [(1, "one", None), (2, "two", 20)]

        table_info = _unity_table_info(unity_rest_url, table_fqn)
        columns = table_info.get("columns") or []
        assert [(column.get("name"), column.get("type_name")) for column in columns] == [
            ("id", "INT"),
            ("name", "STRING"),
            ("age", "INT"),
        ]
    finally:
        unity_spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        unity_spark.sql(f"DROP SCHEMA IF EXISTS {database} CASCADE")
