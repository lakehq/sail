# ruff: noqa: S608
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.mark.parametrize("fmt", ["parquet", "json", "csv"])
def test_file_column_rename_rejects_without_losing_values(spark, tmp_path, fmt):
    table = f"file_column_rename_{fmt}"
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, value STRING) USING {fmt} LOCATION '{location}'")
        spark.sql(f"INSERT INTO {table} VALUES (1, 'old')")
        with pytest.raises(Exception, match=r"(?i)not support"):
            spark.sql(f"ALTER TABLE {table} RENAME COLUMN value TO renamed")
        assert spark.table(table).columns == ["id", "value"]
        assert [tuple(row) for row in spark.table(table).collect()] == [(1, "old")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    "operation",
    [
        "ADD COLUMN c INT FIRST",
        "ALTER COLUMN b FIRST",
        "DROP COLUMN a",
        "REPLACE COLUMNS (b INT, a INT)",
    ],
)
def test_csv_column_changes_reject_without_reinterpreting_existing_data(spark, tmp_path, operation):
    table = "csv_column_change"
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.sql(f"CREATE TABLE {table} (a INT, b INT) USING csv LOCATION '{location}'")
        spark.sql(f"INSERT INTO {table} VALUES (1, 100)")
        with pytest.raises(Exception, match=r"(?i)not support"):
            spark.sql(f"ALTER TABLE {table} {operation}")
        assert spark.table(table).columns == ["a", "b"]
        assert spark.table(table).first().asDict() == {"a": 1, "b": 100}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.skipif(is_jvm_spark(), reason="Requires Iceberg extension")
@pytest.mark.parametrize(
    ("operation", "expected_names"),
    [
        ("ALTER COLUMN id COMMENT 'updated'", ["id", "a", "A"]),
        ("RENAME COLUMN id TO renamed", ["renamed", "a", "A"]),
    ],
)
def test_iceberg_column_change_preserves_case_distinct_siblings(spark, tmp_path, operation, expected_names):
    table = "case_distinct_column_change"
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    location = escape_sql_string_literal(str(tmp_path))
    try:
        spark.conf.set(key, "true")
        spark.sql(f"CREATE TABLE {table} (id INT, a INT, A INT) USING iceberg LOCATION '{location}'")
        spark.sql(f"INSERT INTO {table} VALUES (1, 2, 3)")
        spark.sql(f"ALTER TABLE {table} {operation}")
        assert spark.table(table).columns == expected_names
        assert [tuple(row) for row in spark.table(table).collect()] == [(1, 2, 3)]
    finally:
        spark.conf.set(key, previous)
        spark.sql(f"DROP TABLE IF EXISTS {table}")
