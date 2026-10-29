# ruff: noqa: S608
"""Iceberg DML returns empty SQL results without repeating writes on collection."""

import pytest
from pyspark.sql.types import StructType

from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture(
    params=[
        (1, "copy-on-write"),
        (2, "copy-on-write"),
        (3, "copy-on-write"),
        (2, "merge-on-read"),
        (3, "merge-on-read"),
    ],
    ids=["v1-cow", "v2-cow", "v3-cow", "v2-mor", "v3-mor"],
)
def dml_table(spark, tmp_path, request):
    version, mode = request.param
    table = "iceberg_dml_results"
    spark.sql(f"""
        CREATE TABLE {table} (id INT, value INT, p INT) USING iceberg PARTITIONED BY (p)
        LOCATION '{escape_sql_string_literal(str(tmp_path))}' TBLPROPERTIES (
          'format-version'='{version}', 'write.delete.mode'='{mode}',
          'write.update.mode'='{mode}', 'write.merge.mode'='{mode}')
    """)
    try:
        yield table
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def _empty_result(spark, sql, path):
    frame = spark.sql(sql)
    assert frame.schema == StructType([])
    assert frame.collect() == []
    metadata = sorted((path / "metadata").glob("*.metadata.json"))
    assert frame.collect() == []
    assert sorted((path / "metadata").glob("*.metadata.json")) == metadata


def _rows(spark, table):
    return spark.table(table).orderBy("id").collect()


def test_iceberg_insert_update_delete_results(spark, tmp_path, dml_table):
    table = dml_table
    _empty_result(spark, f"INSERT INTO {table} SELECT 1, 10, 0 WHERE false", tmp_path)
    assert _rows(spark, table) == []
    _empty_result(spark, f"INSERT INTO {table} VALUES (1, 10, 0), (2, NULL, 0), (3, 30, 1)", tmp_path)
    assert _rows(spark, table) == [(1, 10, 0), (2, None, 0), (3, 30, 1)]
    _empty_result(spark, f"UPDATE {table} SET value = 0 WHERE id = 99", tmp_path)
    _empty_result(spark, f"UPDATE {table} SET value = value WHERE id = 1", tmp_path)
    _empty_result(spark, f"UPDATE {table} SET value = value + 1 WHERE id = 3", tmp_path)
    assert _rows(spark, table) == [(1, 10, 0), (2, None, 0), (3, 31, 1)]
    _empty_result(spark, f"DELETE FROM {table} WHERE value < 20", tmp_path)
    assert _rows(spark, table) == [(2, None, 0), (3, 31, 1)]
    _empty_result(spark, f"DELETE FROM {table} WHERE id = 99", tmp_path)
    _empty_result(spark, f"UPDATE {table} SET value = id", tmp_path)
    assert _rows(spark, table) == [(2, 2, 0), (3, 3, 1)]
    _empty_result(spark, f"DELETE FROM {table} WHERE p = 0", tmp_path)
    assert _rows(spark, table) == [(3, 3, 1)]
    _empty_result(spark, f"DELETE FROM {table}", tmp_path)
    assert _rows(spark, table) == []
    _empty_result(spark, f"DELETE FROM {table}", tmp_path)
    _empty_result(spark, f"UPDATE {table} SET value = 0", tmp_path)
    _empty_result(spark, f"INSERT INTO {table} VALUES (4, 40, 0)", tmp_path)
    _empty_result(spark, f"INSERT OVERWRITE {table} VALUES (5, 50, 0)", tmp_path)
    assert _rows(spark, table) == [(5, 50, 0)]
    _empty_result(spark, f"INSERT OVERWRITE {table} SELECT 1, 10, 0 WHERE false", tmp_path)
    assert _rows(spark, table) == []


def test_iceberg_merge_delete_preserves_physical_duplicates(spark, tmp_path, dml_table):
    table = dml_table
    _empty_result(spark, f"INSERT INTO {table} VALUES (1, 10, 0), (1, 10, 0), (9, 90, 1)", tmp_path)
    _empty_result(
        spark,
        f"""
        MERGE INTO {table} t
        USING (SELECT * FROM VALUES (1, 100, 0), (1, 100, 0), (3, 30, 1), (3, 30, 1) AS s(id, value, p)) s
        ON t.id = s.id WHEN MATCHED THEN DELETE
        WHEN NOT MATCHED AND s.value = 30 THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.id = 9 THEN UPDATE SET value = t.value + 1
    """,
        tmp_path,
    )
    assert _rows(spark, table) == [(3, 30, 1), (3, 30, 1), (9, 91, 1)]


def test_iceberg_merge_results(spark, tmp_path, dml_table):
    table = dml_table
    _empty_result(spark, f"INSERT INTO {table} VALUES (1, 10, 0), (2, 20, 0), (3, 30, 1), (9, 90, 1)", tmp_path)
    _empty_result(
        spark,
        f"""
        MERGE INTO {table} t
        USING (SELECT * FROM VALUES (1, 11, 0), (2, 20, 0), (4, 40, 1) AS s(id, value, p)) s
        ON t.id = s.id WHEN MATCHED AND s.id = 2 THEN DELETE
        WHEN MATCHED THEN UPDATE SET value = s.value
        WHEN NOT MATCHED THEN INSERT *
    """,
        tmp_path,
    )
    assert _rows(spark, table) == [(1, 11, 0), (3, 30, 1), (4, 40, 1), (9, 90, 1)]
    _empty_result(
        spark,
        f"""
        MERGE INTO {table} t USING (SELECT 0 AS id WHERE false) s ON t.id = s.id
        WHEN NOT MATCHED BY SOURCE AND t.id = 3 THEN UPDATE SET value = value + 1
        WHEN NOT MATCHED BY SOURCE AND t.id = 9 THEN DELETE
    """,
        tmp_path,
    )
    assert _rows(spark, table) == [(1, 11, 0), (3, 31, 1), (4, 40, 1)]
    _empty_result(
        spark,
        f"""
        MERGE INTO {table} t USING (SELECT 5 AS id, 50 AS value, 1 AS p) s ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT *
    """,
        tmp_path,
    )
    _empty_result(
        spark,
        f"""
        MERGE INTO {table} t USING (SELECT 99 AS id) s ON t.id = s.id
        WHEN MATCHED THEN DELETE
    """,
        tmp_path,
    )
    assert _rows(spark, table) == [(1, 11, 0), (3, 31, 1), (4, 40, 1), (5, 50, 1)]
    _empty_result(spark, f"DELETE FROM {table}", tmp_path)
    _empty_result(
        spark,
        f"""
        MERGE INTO {table} t USING (SELECT 6 AS id, 60 AS value, 1 AS p) s ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT *
    """,
        tmp_path,
    )
    assert _rows(spark, table) == [(6, 60, 1)]
