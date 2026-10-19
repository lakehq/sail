# ruff: noqa: S608, PLR2004
import json

import pytest

from pysail.testing.spark.ddl import exercise_lakehouse_replace_columns
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests.spark.catalog.test_lakehouse_column_ddl import identity, metadata_schema

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Requires lakehouse extensions")


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_replace_columns_identity_and_writes(spark, tmp_path, fmt):
    table = f"replace_{fmt}"
    properties = "TBLPROPERTIES ('delta.columnMapping.mode'='name')" if fmt == "delta" else ""
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING) USING {fmt} LOCATION '{tmp_path.as_uri()}' {properties}"
        )
        before = metadata_schema(tmp_path, fmt)
        exercise_lakehouse_replace_columns(spark, table, fmt)
        after = metadata_schema(tmp_path, fmt)
        if fmt == "delta":
            assert identity(before["fields"][0], fmt) == identity(after["fields"][1], fmt)
            assert identity(before["fields"][1], fmt) == identity(after["fields"][0], fmt)
            assert after["fields"][0]["metadata"]["comment"] == "123"
            commits = [
                json.loads(line).get("commitInfo", {})
                for log in (tmp_path / "_delta_log").glob("*.json")
                for line in log.read_text().splitlines()
            ]
            replace = next(commit for commit in commits if commit.get("operation") == "REPLACE COLUMNS")
            assert json.loads(replace["operationParameters"]["columns"])[0]["metadata"]["comment"] == "123"
        else:
            assert min(f["id"] for f in after["fields"]) > max(f["id"] for f in before["fields"])
            assert after["fields"][0]["doc"] == "123"
        spark.sql(f"ALTER TABLE {table} REPLACE COLUMNS (id INT)")
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == (
            [1, 2] if fmt == "delta" else [None, None]
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    ("expression", "generated_type", "id_type", "valid"),
    [
        ("CASE WHEN id > 0 THEN id ELSE 0L END", "BIGINT", "INT", True),
        ("IF(id > 0, id, 0L)", "BIGINT", "INT", True),
        ("COALESCE(id, 0L)", "BIGINT", "INT", True),
        ("CASE WHEN id > 0 THEN 1 ELSE id END", "INT", "BIGINT", False),
    ],
)
def test_delta_replace_columns_analyzes_generated_expression(
    spark, tmp_path, expression, generated_type, id_type, valid
):
    table = "replace_generated_coercion"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, generated {generated_type} GENERATED ALWAYS AS ({expression})) "
            f"USING delta LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('delta.enableTypeWidening'='true')"
        )
        spark.sql("INSERT INTO replace_generated_coercion (id) VALUES (1)")
        before = metadata_schema(tmp_path, "delta")
        statement = f"ALTER TABLE {table} REPLACE COLUMNS (id {id_type}, generated {generated_type})"
        if valid:
            spark.sql(statement)
        else:
            with pytest.raises(Exception, match="DELTA_GENERATED_COLUMNS_EXPR_TYPE_MISMATCH"):
                spark.sql(statement)
            assert metadata_schema(tmp_path, "delta") == before
        assert tuple(spark.table(table).first()) == (1, 1)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_delta_replace_nested_columns_and_widen(spark, tmp_path):
    table = "replace_nested"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT NOT NULL COMMENT 'kept', nested STRUCT<a: INT, b: STRING>, "
            f"items ARRAY<STRUCT<a: INT, b: STRING>>) USING delta LOCATION '{tmp_path.as_uri()}' "
            "TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.enableTypeWidening'='true')"
        )
        spark.sql(
            f"INSERT INTO {table} SELECT 1, named_struct('a', 10, 'b', 'b'), array(named_struct('a', 20, 'b', 'c'))"
        )
        before = metadata_schema(tmp_path, "delta")
        spark.sql(
            f"ALTER TABLE {table} REPLACE COLUMNS (items ARRAY<STRUCT<b: STRING, a: BIGINT>>, nested STRUCT<b: STRING>, id BIGINT)"
        )
        row = spark.table(table).first().asDict(recursive=True)
        assert row == {"items": [{"b": "c", "a": 20}], "nested": {"b": "b"}, "id": 1}
        after = metadata_schema(tmp_path, "delta")
        assert identity(after["fields"][2], "delta") == identity(before["fields"][0], "delta")
        assert after["fields"][2]["metadata"]["comment"] == "kept"
        assert after["fields"][2]["nullable"]
        assert "delta.typeChanges" in after["fields"][2]["metadata"]
        spark.sql(f"INSERT INTO {table} SELECT array(named_struct('b', 'd', 'a', 30L)), named_struct('b', 'e'), 2L")
        assert spark.table(table).count() == 2
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_replace_partition_dependency_is_atomic(spark, tmp_path, fmt):
    table = f"replace_partition_{fmt}"
    properties = "TBLPROPERTIES ('delta.columnMapping.mode'='name')" if fmt == "delta" else ""
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part STRING) USING {fmt} PARTITIONED BY (part) LOCATION '{tmp_path.as_uri()}' {properties}"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, 'a')")
        before = metadata_schema(tmp_path, fmt)
        with pytest.raises(Exception, match="partition"):
            spark.sql(f"ALTER TABLE {table} REPLACE COLUMNS (id INT)")
        assert metadata_schema(tmp_path, fmt) == before
        assert tuple(spark.table(table).first()) == (1, "a")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    ("columns", "error"),
    [
        ("id INT, extra STRING", "Ambiguous"),
        ("id INT NOT NULL, value STRING", "nullability"),
        ("id STRING, value STRING", "type widening"),
        ("id INT, value STRING, extra INT NOT NULL", "required"),
        ("id INT, value STRING, id BIGINT", "already exists"),
    ],
)
def test_invalid_delta_replacement_is_atomic(spark, tmp_path, columns, error):
    table = "replace_invalid"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING) USING delta LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('delta.columnMapping.mode'='name')"
        )
        before = metadata_schema(tmp_path, "delta")
        with pytest.raises(Exception, match=error):
            spark.sql(f"ALTER TABLE {table} REPLACE COLUMNS ({columns})")
        assert metadata_schema(tmp_path, "delta") == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_replace_column_defaults_and_comments(spark, tmp_path, fmt):
    table = f"replace_defaults_{fmt}"
    properties = "'delta.feature.allowColumnDefaults'='supported'" if fmt == "delta" else "'format-version'='3'"
    default = "DEFAULT 'old'" if fmt == "delta" else ""
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING {default} COMMENT 'kept') USING {fmt} LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ({properties})"
        )
        spark.sql(f"ALTER TABLE {table} REPLACE COLUMNS (value STRING, id INT)")
        column = next(c for c in spark.catalog.listColumns(table) if c.name == "value")
        assert column.description == ("kept" if fmt == "delta" else None)
        spark.sql(f"INSERT INTO {table} (id) VALUES (1)")
        assert spark.table(table).first().value == ("old" if fmt == "delta" else None)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("generated_type", ["INT", "BIGINT"])
def test_delta_replace_columns_validates_generated_expression_type(spark, tmp_path, generated_type):
    table = "replace_generated_type"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, generated INT GENERATED ALWAYS AS (id + 1)) "
            f"USING delta LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('delta.enableTypeWidening'='true')"
        )
        spark.sql("INSERT INTO replace_generated_type (id) VALUES (1)")
        before = metadata_schema(tmp_path, "delta")

        statement = f"ALTER TABLE {table} REPLACE COLUMNS (id BIGINT, generated {generated_type})"
        if generated_type == "INT":
            with pytest.raises(Exception, match=r"(?i)generated.*type|type.*generated"):
                spark.sql(statement)
            assert metadata_schema(tmp_path, "delta") == before
        else:
            spark.sql(statement)
            spark.sql("INSERT INTO replace_generated_type (id) VALUES (2147483648)")
            assert tuple(spark.table(table).orderBy("id").collect()[1]) == (2147483648, 2147483649)
        assert tuple(spark.table(table).orderBy("id").first()) == (1, 2)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("case_sensitive", [True, False])
@pytest.mark.parametrize(
    ("definition", "replacement", "value"),
    [
        ("Value INT", "value INT", "1"),
        ("payload STRUCT<Value: INT>", "payload STRUCT<value: INT>", "named_struct('Value', 1)"),
        ("payload ARRAY<STRUCT<Value: INT>>", "payload ARRAY<STRUCT<value: INT>>", "array(named_struct('Value', 1))"),
        (
            "payload MAP<STRING, STRUCT<Value: INT>>",
            "payload MAP<STRING, STRUCT<value: INT>>",
            "map('key', named_struct('Value', 1))",
        ),
    ],
    ids=["top-level", "struct", "array", "map-value"],
)
def test_delta_replace_columns_respects_case_sensitive_resolution(
    spark, tmp_path, definition, replacement, value, case_sensitive
):
    table = "replace_case_sensitive_columns"
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        spark.sql(f"CREATE TABLE {table} ({definition}) USING delta LOCATION '{tmp_path.as_uri()}'")
        spark.sql(f"INSERT INTO {table} SELECT {value}")
        before = {log.name: log.read_bytes() for log in (tmp_path / "_delta_log").glob("*.json")}
        rows = spark.table(table).collect()
        schema = spark.table(table).schema
        spark.conf.set(key, str(case_sensitive).lower())

        if case_sensitive:
            with pytest.raises(Exception, match=r"(?i)ambiguous|cannot.*drop|not.*supported"):
                spark.sql(f"ALTER TABLE {table} REPLACE COLUMNS ({replacement})")
            assert {log.name: log.read_bytes() for log in (tmp_path / "_delta_log").glob("*.json")} == before
        else:
            spark.sql(f"ALTER TABLE {table} REPLACE COLUMNS ({replacement})")

        assert spark.table(table).schema == schema
        assert spark.table(table).collect() == rows
        spark.sql(f"INSERT INTO {table} SELECT {value}")
        assert spark.table(table).collect() == rows * 2
    finally:
        spark.conf.set(key, previous)
        spark.sql(f"DROP TABLE IF EXISTS {table}")
