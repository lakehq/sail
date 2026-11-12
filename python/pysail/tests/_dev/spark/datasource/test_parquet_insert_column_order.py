# ruff: noqa: S608
from collections import Counter
from urllib.parse import unquote
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal

OVERWRITE_MODE = "spark.sql.sources.partitionOverwriteMode"


@pytest.fixture
def table(spark):
    name = f"insert_column_order_{uuid4().hex}"
    previous = spark.conf.get(OVERWRITE_MODE, None)
    spark.conf.set(OVERWRITE_MODE, "dynamic")
    try:
        yield name
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        if previous is None:
            spark.conf.unset(OVERWRITE_MODE)
        else:
            spark.conf.set(OVERWRITE_MODE, previous)


def create_table(spark, table, path, schema, partitions="p"):
    spark.sql(
        f"CREATE TABLE {table} ({schema}) USING parquet PARTITIONED BY ({partitions}) "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    )


def assert_rows(spark, table, path, columns, partition_columns, expected):
    assert Counter(tuple(row) for row in spark.table(table).select(*columns).collect()) == Counter(expected)
    # Decode the physical payload and directory keys independently of Sail's reader.
    actual = []
    for file in path.rglob("*.parquet"):
        keys = dict(unquote(segment).split("=", 1) for segment in file.relative_to(path).parts[:-1])
        parquet = pq.ParquetFile(file)
        assert not set(partition_columns).intersection(parquet.schema_arrow.names)
        for row in parquet.read().to_pylist():
            row.update(keys)
            actual.append(tuple(row[column] for column in columns))
    expected_files = [
        tuple(str(value) if column in partition_columns else value for column, value in zip(columns, row, strict=True))
        for row in expected
    ]
    assert Counter(actual) == Counter(expected_files)


@pytest.mark.parametrize("entry", ["sql", "sql-partition", "insertInto"])
@pytest.mark.parametrize("mode", ["append", "overwrite"])
@pytest.mark.parametrize(
    ("column_type", "default", "old", "keep", "new"),
    [
        pytest.param("STRING", "", "old", "keep", "new", id="heterogeneous"),
        pytest.param("INT", "", 100, 200, 9, id="same-type"),
        pytest.param("STRING", " DEFAULT 'fallback'", "old", "keep", "new", id="defaults"),
    ],
)
def test_positional_insert_partition_first(spark, tmp_path, table, entry, mode, column_type, default, old, keep, new):
    path = tmp_path / "table"
    create_table(spark, table, path, f"p INT, msg {column_type}{default}")
    initial = spark.createDataFrame([(1, old), (2, keep)], f"p INT, msg {column_type}")
    initial.write.format("parquet").partitionBy("p").mode("append").saveAsTable(table)
    # Names intentionally differ: insertInto and SQL positional writes ignore them.
    source = spark.createDataFrame([(new, 1)], f"value {column_type}, key INT")
    if entry == "insertInto":
        source.write.mode(mode).insertInto(table)
    else:
        view = f"source_{uuid4().hex}"
        source.createOrReplaceTempView(view)
        try:
            operation = "INSERT INTO" if mode == "append" else "INSERT OVERWRITE"
            clause = "PARTITION (p)" if entry == "sql-partition" else ""
            spark.sql(f"{operation} {table} {clause} SELECT value, key FROM {view}")
        finally:
            spark.catalog.dropTempView(view)
    expected = [(1, new), (2, keep)]
    if mode == "append":
        expected.append((1, old))
    assert_rows(spark, table, path, ["p", "msg"], ["p"], expected)


def test_positional_insert_preserves_case_distinct_columns(spark, tmp_path, table):
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "true")
        path = tmp_path / "table"
        create_table(spark, table, path, "p INT, P INT, msg STRING")
        described = spark.sql(f"DESCRIBE TABLE {table}").collect()
        assert [row.col_name for row in described[:3]] == ["P", "msg", "p"]
        spark.sql(f"INSERT INTO {table} PARTITION (p) SELECT 101, 'new', 1")
        # Inspect physical fields directly to distinguish data column P from partition p.
        actual = []
        for file in path.rglob("*.parquet"):
            assert file.relative_to(path).parts[:-1] == ("p=1",)
            parquet = pq.ParquetFile(file)
            assert parquet.schema_arrow.names == ["P", "msg"]
            actual.extend(parquet.read().to_pylist())
        assert actual == [{"P": 101, "msg": "new"}]
    finally:
        spark.conf.set(key, previous)


def test_positional_default_preserves_column_features(spark, tmp_path, table):
    path = tmp_path / "table"
    create_table(spark, table, path, "p INT, msg STRING DEFAULT 'fallback' COMMENT 'message'")
    spark.sql(f"INSERT INTO {table} PARTITION (p) VALUES (DEFAULT, 1)")
    assert_rows(spark, table, path, ["p", "msg"], ["p"], [(1, "fallback")])
    columns = spark.catalog.listColumns(table)
    assert [(column.name, column.description) for column in columns] == [("msg", "message"), ("p", None)]


def test_positional_multiple_partition_order(spark, tmp_path, table):
    path = tmp_path / "table"
    create_table(spark, table, path, "q INT, a INT, p INT, b INT", partitions="p, q")
    spark.sql(f"INSERT INTO {table} (q, a, p, b) VALUES (10, 100, 1, 101), (20, 200, 2, 201)")
    # Neither declaration order nor INSERT clause order determines the binding.
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (q, p) SELECT 9, 8, 1, 10")
    assert_rows(spark, table, path, ["a", "b", "p", "q"], ["p", "q"], [(9, 8, 1, 10), (200, 201, 2, 20)])
    assert {file.relative_to(path).parts[:2] for file in path.rglob("*.parquet")} == {
        ("p=1", "q=10"),
        ("p=2", "q=20"),
    }


@pytest.mark.parametrize("entry", ["by-name", "target-columns", "saveAsTable"])
def test_name_based_insert_ignores_column_order(spark, tmp_path, table, entry):
    path = tmp_path / "table"
    create_table(spark, table, path, "p INT, msg STRING")
    spark.sql(f"INSERT INTO {table} (p, msg) VALUES (1, 'old'), (2, 'keep')")
    if entry == "saveAsTable":
        spark.createDataFrame([(3, "new")], "p INT, msg STRING").write.format("parquet").partitionBy("p").mode(
            "append"
        ).saveAsTable(table)
        expected = [(1, "old"), (2, "keep"), (3, "new")]
    else:
        clause = "BY NAME" if entry == "by-name" else "(p, msg)"
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (p) {clause} SELECT 1 AS p, 'new' AS msg")
        expected = [(1, "new"), (2, "keep")]
    assert_rows(spark, table, path, ["p", "msg"], ["p"], expected)


# [CREDIT]: https://github.com/apache/spark/blob/v4.3.0/sql/core/src/test/scala/org/apache/spark/sql/execution/command/DDLSuite.scala#L753-L763
@pytest.mark.parametrize("entry", ["create", "ctas", "saveAsTable"])
def test_catalog_column_order_matches_scan_and_positional_insert(spark, tmp_path, table, entry):
    path = tmp_path / "table"
    query = "SELECT 10 AS q, 'old-a' AS a, 1 AS p, 'old-b' AS b"
    if entry == "create":
        create_table(spark, table, path, "q INT, a STRING, p INT, b STRING", partitions="p, q")
        spark.sql(f"INSERT INTO {table} (q, a, p, b) {query}")
    elif entry == "ctas":
        spark.sql(
            f"CREATE TABLE {table} USING parquet PARTITIONED BY (p, q) "
            f"LOCATION '{escape_sql_string_literal(str(path))}' AS {query}"
        )
    else:
        spark.sql(query).write.format("parquet").partitionBy("p", "q").option("path", str(path)).saveAsTable(table)

    described = []
    for row in spark.sql(f"DESCRIBE TABLE {table}").collect():
        if not row.col_name or row.col_name.startswith("#"):
            break
        described.append(row.col_name)
    expected_columns = ["a", "b", "p", "q"]
    assert described == expected_columns
    assert [(column.name, column.isPartition) for column in spark.catalog.listColumns(table)] == [
        ("a", False),
        ("b", False),
        ("p", True),
        ("q", True),
    ]
    assert spark.table(table).columns == expected_columns
    assert_rows(spark, table, path, expected_columns, ["p", "q"], [("old-a", "old-b", 1, 10)])

    view = f"source_{uuid4().hex}"
    spark.createDataFrame([(10, "new-a", 1, "new-b")], "q INT, a STRING, p INT, b STRING").createOrReplaceTempView(view)
    try:
        # Adapters build their positional SELECT from DESCRIBE/listColumns metadata.
        projection = ", ".join(f"`{column}`" for column in described)
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (q, p) SELECT {projection} FROM {view}")
    finally:
        spark.catalog.dropTempView(view)
    assert_rows(spark, table, path, expected_columns, ["p", "q"], [("new-a", "new-b", 1, 10)])
