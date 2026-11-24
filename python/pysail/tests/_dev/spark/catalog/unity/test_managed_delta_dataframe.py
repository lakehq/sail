# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608
"""DataFrame API tests for Unity Catalog managed Delta tables."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from pysail.tests._dev.spark.catalog.unity.conftest import (
    _unique_database,
    _unity_delta_commit_info,
    _unity_table_info,
)

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
    database = _unique_database("unity_dataframe_test")
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
    database = _unique_database("unity_dataframe_type_json_test")
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


@pytest.mark.spark(version=">=4", reason="Uses DataFrame arguments in spark.sql()")
def test_spark_sql_merge_dataframe_source_is_unity_managed(
    spark: SparkSession,
    unity_rest_url: str,
    unity_uses_delta_api: bool,  # noqa: FBT001
) -> None:
    database = _unique_database("unity_dataframe_sql_args_test")
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

        # With the Delta REST API, version 0 has the data of CREATE TABLE AS SELECT, while
        # the table is created before the data is written otherwise.
        assert _unity_delta_commit_info(unity_rest_url, table_fqn, 1 if unity_uses_delta_api else 2)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(f"DROP SCHEMA IF EXISTS {database} CASCADE")


@pytest.mark.parametrize("mode", ["name", "id"])
def test_write_to_creates_column_mapped_managed_table(spark: SparkSession, mode: str) -> None:
    database = _unique_database("unity_dataframe_column_mapping_test")
    table_fqn = f"{database}.managed_delta_mapped_{mode}_t"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        df = spark.createDataFrame([(1, "one", "a"), (2, "two", "b")], schema="id INT, name STRING, grp STRING")
        df.writeTo(table_fqn).using("delta").tableProperty("delta.columnMapping.mode", mode).partitionedBy(
            "grp"
        ).create()

        rows = spark.sql(f"SELECT id, name, grp FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.name, row.grp) for row in rows] == [(1, "one", "a"), (2, "two", "b")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(f"DROP SCHEMA IF EXISTS {database} CASCADE")


def test_save_as_table_with_delta_option_writes_data_with_the_option(spark: SparkSession) -> None:
    """The table that the write creates has the Delta write options as table properties,
    whether the catalog creates the table before or after the data is written, so the data
    is written with the metadata of the table."""
    database = _unique_database("unity_dataframe_delta_option_test")
    table_fqn = f"{database}.managed_delta_option_t"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        df = spark.createDataFrame([(1, "one")], schema="id INT, name STRING")
        df.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable(table_fqn)
        assert [tuple(row) for row in spark.table(table_fqn).collect()] == [(1, "one")]
        properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table_fqn}").collect()}
        assert properties.get("delta.columnMapping.mode") == "name"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(f"DROP SCHEMA IF EXISTS {database} CASCADE")


def test_save_as_table_with_delta_option_sets_the_table_property(spark: SparkSession) -> None:
    """A write option that names a Delta table property is a property of the table that the
    write creates, as in Delta Spark."""
    database = _unique_database("unity_dataframe_delta_property_test")
    table_fqn = f"{database}.managed_delta_property_t"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        df = spark.createDataFrame([(1, "one")], schema="id INT, name STRING")
        df.write.format("delta").option("delta.appendOnly", "true").saveAsTable(table_fqn)

        assert [tuple(row) for row in spark.table(table_fqn).collect()] == [(1, "one")]
        properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table_fqn}").collect()}
        assert properties.get("delta.appendOnly") == "true"
        with pytest.raises(Exception, match="(?i)append.only"):
            spark.sql(f"DELETE FROM {table_fqn} WHERE id = 1")
        assert [tuple(row) for row in spark.table(table_fqn).collect()] == [(1, "one")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_fqn}")
        spark.sql(f"DROP SCHEMA IF EXISTS {database} CASCADE")


def test_idempotent_append_to_managed_delta_table(spark: SparkSession, unity_rest_url: str) -> None:
    database = "unity_idempotent_append"
    table = f"{database}.events"
    spark.sql(f"CREATE SCHEMA {database}")
    try:
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING delta")
        for value, version in enumerate([1, 1, 0, 2]):
            spark.range(value, value + 1).write.format("delta").mode("append").options(
                txnAppId="job", txnVersion=version
            ).saveAsTable(table)
        assert sorted(row.id for row in spark.table(table).collect()) == [0, 3]
        assert _unity_delta_commit_info(unity_rest_url, table, 2)
    finally:
        spark.sql(f"DROP SCHEMA {database} CASCADE")
