# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

"""A temporary view reads the current version of the catalog tables that it was created over."""

from __future__ import annotations

import uuid

import pytest
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.testing.spark.session import patch_spark_connect_session, spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests._dev.spark.catalog.temp_view_utils import (
    COLUMNS_CHANGED,
    FORMATS,
    KINDS,
    LAKE_FORMATS,
    NOT_FOUND,
    PLAN_KINDS,
    Table,
    error,
    rows,
)

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only: Delta Lake and Iceberg tables in the memory catalog")


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server() as server:
        yield server.remote


@pytest.fixture(scope="module")
def observer(spark, remote):
    session = SparkSession.builder.remote(remote).create()
    patch_spark_connect_session(session)
    try:
        assert session.session_id != spark.session_id
        yield session
    finally:
        session.stop()


@pytest.fixture
def table(spark, tmp_path, request):
    table = Table(spark, tmp_path, request.param)
    yield table
    table.drop()


@pytest.mark.parametrize("table", FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_the_rows_committed_after_its_creation(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    table.insert("(2)")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
    # The view and the table are read at one version of the table.
    joined = f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN {table.name} t ON v.id = t.id"
    assert rows(spark, joined) == [(1, 1), (2, 2)]
    assert rows(spark, f"SELECT (SELECT count(*) FROM {view}), (SELECT count(*) FROM {table.name})") == [(2, 2)]
    table.insert("(3)")
    assert rows(spark, f"SELECT count(*) FROM {view} a JOIN {view} b ON a.id = b.id") == [(3,)]


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_write_reads_the_current_rows_of_a_view_over_its_target(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    table.insert("(2)")
    spark.sql(f"INSERT INTO {table.name} SELECT id + 10 FROM {view}")
    assert rows(spark, f"SELECT * FROM {table.name}") == [(1,), (2,), (11,), (12,)]
    assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,), (11,), (12,)]
    source = f"(SELECT id + 10 AS id FROM {view})"
    spark.sql(f"MERGE INTO {table.name} t USING {source} s ON t.id = s.id WHEN MATCHED THEN DELETE")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
    if table.fmt == "iceberg":
        # A Delta Lake table has no `DELETE` with a subquery yet.
        spark.sql(f"DELETE FROM {table.name} WHERE id IN (SELECT id + 1 FROM {view})")
        assert rows(spark, f"SELECT * FROM {view}") == [(1,)]


@pytest.mark.parametrize("table", FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_view_keeps_its_columns_when_the_table_gains_a_column(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    spark.sql(f"ALTER TABLE {table.name} ADD COLUMNS (extra STRING)")
    table.insert("(2, 'x')")
    assert spark.sql(f"SELECT * FROM {view}").columns == ["id"]
    assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
    joined = f"SELECT v.id, t.id, t.extra FROM {view} v JOIN {table.name} t ON v.id = t.id"
    assert rows(spark, joined) == [(1, 1, None), (2, 2, "x")]
    assert rows(spark, f"SELECT id, count(*) FROM {view} GROUP BY id") == [(1, 1), (2, 1)]


@pytest.mark.parametrize("table", FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_view_of_a_dropped_table_is_not_found(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    spark.sql(f"DROP TABLE {table.name}")
    assert NOT_FOUND in error(spark, f"SELECT * FROM {view}")
    # The table is created again with one more column and another column order.
    table.create("extra STRING, id BIGINT")
    table.insert("('x', 5)")
    assert rows(spark, f"SELECT * FROM {view}") == [(5,)]


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_table_that_is_created_again_in_another_format(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    spark.sql(f"DROP TABLE {table.name}")
    table.create("id BIGINT", fmt="iceberg" if table.fmt == "delta" else "delta")
    table.insert("(7)")
    assert rows(spark, f"SELECT * FROM {view}") == [(7,)]
    joined = f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN {table.name} t ON v.id = t.id"
    assert rows(spark, joined) == [(7, 7)]


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_view_rejects_a_table_whose_column_type_changed(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    spark.sql(f"DROP TABLE {table.name}")
    table.create("id INT")
    message = error(spark, f"SELECT * FROM {view}")
    assert COLUMNS_CHANGED in message
    assert "- `id` type has changed from BIGINT to INT" in message


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_table_whose_unread_columns_were_dropped_or_renamed(spark, tmp_path, kind):
    table = Table(spark, tmp_path, "iceberg", "id BIGINT, extra STRING, other STRING")
    try:
        table.insert("(1, 'x', 'y')")
        view = table.create_view(kind, f"SELECT id FROM {table.name}")
        spark.sql(f"ALTER TABLE {table.name} DROP COLUMN extra")
        spark.sql(f"ALTER TABLE {table.name} RENAME COLUMN other TO another")
        table.insert("(2, 'z')")
        assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
        spark.sql(f"ALTER TABLE {table.name} RENAME COLUMN id TO key")
        message = error(spark, f"SELECT * FROM {view}")
        assert COLUMNS_CHANGED in message
        assert "- `id` BIGINT has been removed" in message
    finally:
        table.drop()


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_column_renamed_only_in_letter_case(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    spark.sql(f"DROP TABLE {table.name}")
    table.create("ID BIGINT")
    table.insert("(2)")
    assert spark.sql(f"SELECT * FROM {view}").columns == ["id"]
    assert rows(spark, f"SELECT * FROM {view}") == [(2,)]
    assert rows(spark, f"SELECT id, count(*) FROM {view} WHERE id > 0 GROUP BY id") == [(2, 1)]


@pytest.mark.parametrize("kind", PLAN_KINDS)
def test_view_keeps_the_column_metadata_that_it_was_created_with(spark, tmp_path, kind):
    table = Table(spark, tmp_path, "delta")
    try:
        write = spark.sql("SELECT 1L AS id")
        write.select(col("id").alias("id", metadata={"tag": "old"})).write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table.name)
        view = table.create_view(kind)
        assert spark.sql(f"SELECT * FROM {view}").schema["id"].metadata == {"tag": "old"}
        write.select(col("id").alias("id", metadata={"tag": "new"})).write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table.name)
        assert spark.table(table.name).schema["id"].metadata == {"tag": "new"}
        assert spark.sql(f"SELECT * FROM {view}").schema["id"].metadata == {"tag": "old"}
        assert rows(spark, f"SELECT id, count(*) FROM {view} GROUP BY id") == [(1, 1)]
        joined = f"SELECT v.id, t.id FROM {view} v JOIN {table.name} t ON v.id = t.id"
        assert rows(spark, joined) == [(1, 1)]
    finally:
        table.drop()


@pytest.mark.parametrize("fmt", LAKE_FORMATS)
@pytest.mark.parametrize("kind", KINDS)
def test_view_of_a_whole_struct_rejects_a_table_whose_struct_gains_a_field(spark, tmp_path, fmt, kind):
    table = Table(spark, tmp_path, fmt, "id BIGINT, s STRUCT<a: INT>")
    try:
        table.insert("(1, named_struct('a', 1))")
        view = table.create_view(kind)
        fields = table.create_view(
            "sv" if kind.startswith("d") else "dv", f"SELECT id, s.a AS a FROM {table.name} WHERE s.a > 0"
        )
        spark.sql(f"ALTER TABLE {table.name} ADD COLUMNS (s.b INT)")
        table.insert("(2, named_struct('a', 2, 'b', 3))")
        message = error(spark, f"SELECT * FROM {view}")
        assert COLUMNS_CHANGED in message
        assert "- `s`.`b` INT has been added" in message
        # A view that reads only some fields of the struct reads the table.
        assert rows(spark, f"SELECT * FROM {fields}") == [(1, 1), (2, 2)]
    finally:
        table.drop()


# The cases of a table that is created again with other columns: its columns and rows,
# the query of a view over it, the columns and rows that it is created with again, and
# the rows that the view reads then, or `None` if the view must be created again.
COLUMN_CHANGES = {
    "unread column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM {t}",
        "a INT, x INT",
        "(5, 500)",
        [(5,)],
    ),
    "unread column retyped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM {t}",
        "a INT, b STRING, x INT",
        "(5, 'q', 500)",
        [(5,)],
    ),
    "unread column made not null": (
        "a INT, b INT",
        "(1, 10)",
        "SELECT a FROM {t}",
        "a INT, b INT NOT NULL",
        "(5, 50)",
        [(5,)],
    ),
    "read column made not null": ("a INT", "(1)", "SELECT * FROM {t}", "a INT NOT NULL", "(5)", [(5,)]),
    "read column retyped to a string": ("a INT", "(1)", "SELECT * FROM {t}", "a STRING", "('x')", None),
    # The type name is computed when the view is created, and the view still reads the column.
    "type name of a retyped column": (
        "a INT, b INT",
        "(1, 10)",
        "SELECT a, typeof(b) AS tb FROM {t}",
        "a INT, b STRING",
        "(5, 'x')",
        None,
    ),
    "read column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT * FROM {t}",
        "a INT, x INT",
        "(5, 500)",
        None,
    ),
    "read column retyped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a, b FROM {t}",
        "a INT, b BIGINT, x INT",
        "(5, 50, 500)",
        None,
    ),
    "filter column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM {t} WHERE b > 0",
        "a INT, x INT",
        "(5, 500)",
        None,
    ),
    "count of rows": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT count(*) AS n FROM {t}",
        "a INT, x INT",
        "(5, 500)",
        [(1,)],
    ),
    "aggregate, unread column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a, sum(b) AS s FROM {t} GROUP BY a",
        "a INT, b INT",
        "(5, 50)",
        [(5, 50)],
    ),
    "aggregate column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a, sum(b) AS s FROM {t} GROUP BY a",
        "a INT, x INT",
        "(5, 500)",
        None,
    ),
    "window, unread column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a, row_number() OVER (ORDER BY b) AS r FROM {t}",
        "a INT, b INT",
        "(5, 50)",
        [(5, 1)],
    ),
    "sort column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM {t} ORDER BY b",
        "a INT, x INT",
        "(5, 500)",
        None,
    ),
    "scalar subquery, unread column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a, (SELECT max(x) FROM {t}) AS m FROM {t}",
        "a INT, x INT",
        "(5, 500)",
        [(5, 500)],
    ),
    "volatile CTE, unread column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "WITH c AS (SELECT a, x, rand() AS r FROM {t}) SELECT a FROM c",
        "a INT, x INT",
        "(5, 500)",
        [(5,)],
    ),
    "correlated subquery, unread column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM {t} WHERE EXISTS (SELECT 1 FROM {t} AS t2 WHERE t2.b = {t}.b)",
        "a INT, b INT",
        "(5, 50)",
        [(5,)],
    ),
    "subquery of all columns, unread column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM (SELECT * FROM {t}) AS q",
        "a INT, b INT",
        "(5, 50)",
        [(5,)],
    ),
    "subquery of all columns, filter column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM (SELECT * FROM {t}) AS q WHERE q.b > 0",
        "a INT, x INT",
        "(5, 500)",
        None,
    ),
    "lateral subquery, read column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT q.a, l.z FROM (SELECT a, b AS c FROM {t} WHERE a > 0) AS q, LATERAL (SELECT q.c + 1 AS z) AS l",
        "a INT, x INT",
        "(5, 500)",
        None,
    ),
    "lateral subquery, unread column dropped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT q.a, l.z FROM (SELECT a, b AS c FROM {t} WHERE a > 0) AS q, LATERAL (SELECT q.c + 1 AS z) AS l",
        "a INT, b INT",
        "(5, 50)",
        [(5, 51)],
    ),
    "struct field added": (
        "id INT, s STRUCT<a: INT, b: INT>",
        "(1, named_struct('a', 10, 'b', 20))",
        "SELECT s.a AS a FROM {t}",
        "id INT, s STRUCT<a: INT, b: INT, c: INT>",
        "(2, named_struct('a', 11, 'b', 21, 'c', 31))",
        [(11,)],
    ),
    "unread struct field retyped": (
        "id INT, s STRUCT<a: INT, b: INT>",
        "(1, named_struct('a', 10, 'b', 20))",
        "SELECT id, s.a AS a FROM {t} WHERE s.a > 0",
        "id INT, s STRUCT<a: INT, b: STRING>",
        "(3, named_struct('a', 12, 'b', 'x'))",
        [(3, 12)],
    ),
    "read struct field dropped": (
        "id INT, s STRUCT<a: INT, b: INT>",
        "(1, named_struct('a', 10, 'b', 20))",
        "SELECT s.a AS a FROM {t}",
        "id INT, s STRUCT<b: INT>",
        "(4, named_struct('b', 22))",
        None,
    ),
    "distinct row column retyped": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        "SELECT a FROM (SELECT DISTINCT * FROM {t})",
        "a INT, b STRING",
        "(1, 'z'), (1, 'y'), (2, 'y')",
        [(1,), (1,), (2,)],
    ),
    "distinct row column retyped with fewer distinct rows": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        "SELECT a FROM (SELECT DISTINCT a, b FROM {t})",
        "a INT, b STRING",
        "(1, 'z'), (1, 'z'), (2, 'y')",
        [(1,), (2,)],
    ),
    "distinct row column dropped": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        "SELECT a FROM (SELECT DISTINCT a, b FROM {t})",
        "a INT",
        "(1), (1), (2)",
        None,
    ),
    "distinct column of all columns dropped": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        "SELECT a FROM (SELECT DISTINCT * FROM {t})",
        "a INT",
        "(1), (1), (2)",
        None,
    ),
    "grouping column dropped": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        "SELECT count(*) AS n FROM {t} GROUP BY b",
        "a INT",
        "(1), (1), (2)",
        None,
    ),
    "grouping column retyped": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        "SELECT a FROM {t} GROUP BY a, b",
        "a INT, b STRING",
        "(1, 'z'), (1, 'y'), (2, 'y')",
        [(1,), (1,), (2,)],
    ),
    "distinct row struct gains a field": (
        "a INT, b STRUCT<x: INT>",
        "(1, named_struct('x', 1)), (1, named_struct('x', 1)), (2, named_struct('x', 3))",
        "SELECT a FROM (SELECT DISTINCT * FROM {t})",
        "a INT, b STRUCT<x: INT, y: INT>",
        "(1, named_struct('x', 1, 'y', 5)), (1, named_struct('x', 1, 'y', 6)), (2, named_struct('x', 3, 'y', 7))",
        [(1,), (1,), (2,)],
    ),
    "read struct field retyped": (
        "id INT, s STRUCT<a: INT, b: INT>",
        "(1, named_struct('a', 10, 'b', 20))",
        "SELECT s.a AS a FROM {t}",
        "id INT, s STRUCT<a: STRING, b: INT>",
        "(5, named_struct('a', 'q', 'b', 23))",
        None,
    ),
}


# The cases that run for each table format and each way to store a view. The column rules
# do not depend on either, so the other cases run for one of each.
MATRIX_CASES = {
    "unread column dropped",
    "read column retyped",
    "struct field added",
    "unread struct field retyped",
    "distinct row column retyped",
    "column dropped on one side",
    "type name of a retyped column",
}


def change_params(cases):
    return [
        pytest.param(case, fmt, kind, id=f"{case}-{fmt}-{kind}")
        for case in cases
        for fmt in (["delta", "parquet"] if case in MATRIX_CASES else ["delta"])
        for kind in (["sv", "gv", "dv"] if case in MATRIX_CASES else ["sv"])
    ]


@pytest.mark.parametrize(("case", "fmt", "kind"), change_params(COLUMN_CHANGES))
def test_view_reads_a_table_whose_columns_changed(spark, tmp_path, fmt, kind, case):
    columns, values, query, new_columns, new_values, expected = COLUMN_CHANGES[case]
    table = Table(spark, tmp_path, fmt, columns)
    try:
        table.insert(values)
        view = table.create_view(kind, query.format(t=table.name))
        spark.sql(f"DROP TABLE {table.name}")
        table.create(new_columns)
        table.insert(new_values)
        if expected is None:
            assert COLUMNS_CHANGED in error(spark, f"SELECT * FROM {view}")
        else:
            assert rows(spark, f"SELECT * FROM {view}") == expected
    finally:
        table.drop()


# The cases of a view over the union of two tables `t` and `w (a INT, b INT)`, with the rows
# (1, 10), (1, 20), (2, 30) and (1, 10), (3, 40), when one of them is created again with
# other columns: the query of the view, the table, its columns and rows, and the rows
# that the view reads then, or `None` if the view must be created again.
UNION_CHANGES = {
    "column dropped on one side": (
        "SELECT a FROM (SELECT a, b FROM {t} UNION ALL SELECT a, b FROM {w})",
        "w",
        "a INT",
        "(1), (3)",
        [(1,), (1,), (1,), (2,), (3,)],
    ),
    "column retyped on one side": (
        "SELECT a FROM (SELECT a, b FROM {t} UNION ALL SELECT a, b FROM {w})",
        "t",
        "a INT, b STRING",
        "(1, 'z'), (1, 'y'), (2, 'y')",
        [(1,), (1,), (1,), (2,), (3,)],
    ),
    # The inputs of a union keep the types of the columns that tell its rows apart.
    "distinct row column widened on one side": (
        "SELECT a FROM (SELECT a, b FROM {t} UNION SELECT a, b FROM {w})",
        "t",
        "a INT, b BIGINT",
        "(1, 10), (1, 20), (2, 30)",
        None,
    ),
}


@pytest.mark.parametrize(("case", "fmt", "kind"), change_params(UNION_CHANGES))
def test_view_of_a_union_reads_a_table_whose_columns_changed(spark, tmp_path, fmt, kind, case):
    query, changed, columns, values, expected = UNION_CHANGES[case]
    tables = {"t": Table(spark, tmp_path, fmt, "a INT, b INT"), "w": Table(spark, tmp_path, fmt, "a INT, b INT")}
    try:
        tables["t"].insert("(1, 10), (1, 20), (2, 30)")
        tables["w"].insert("(1, 10), (3, 40)")
        view = tables["t"].create_view(kind, query.format(t=tables["t"].name, w=tables["w"].name))
        spark.sql(f"DROP TABLE {tables[changed].name}")
        tables[changed].create(columns)
        tables[changed].insert(values)
        if expected is None:
            assert COLUMNS_CHANGED in error(spark, f"SELECT * FROM {view}")
        else:
            assert rows(spark, f"SELECT * FROM {view}") == expected
    finally:
        for table in tables.values():
            table.drop()


# Queries over `t (a INT, u INT)` whose column `a` had no nulls when a view of the query
# was created, and has nulls when the view is read.
NULLABLE_QUERIES = {
    "null tests": "SELECT a, a IS NULL AS n, coalesce(a, -1) AS c, a + 1 AS p FROM {t}",
    "count": "SELECT count(a) AS ca, count(*) AS n, min(a) AS m FROM {t}",
    "distinct count": "SELECT count(DISTINCT a) AS cd, count(a, u) AS c2 FROM {t}",
    "filter": "SELECT u FROM {t} WHERE a IS NOT NULL",
    "group": "SELECT a, count(*) AS k FROM {t} GROUP BY a",
    "join": "SELECT t1.u, t2.u AS u2 FROM {t} AS t1 JOIN {t} AS t2 ON t1.a = t2.a",
    "null-safe join": "SELECT t1.u, t2.u AS u2 FROM {t} AS t1 JOIN {t} AS t2 ON t1.a <=> t2.a",
    "sort": "SELECT a FROM {t} ORDER BY a NULLS FIRST LIMIT 1",
    "concatenation": "SELECT concat_ws('-', cast(a AS STRING), 'x') AS c FROM {t}",
    "window count": "SELECT u, count(a) OVER () AS c FROM {t}",
}


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("query", NULLABLE_QUERIES)
def test_view_reads_a_column_that_has_nulls_now(spark, tmp_path, fmt, kind, query):
    table = Table(spark, tmp_path, fmt, "a INT NOT NULL, u INT")
    try:
        table.insert("(1, 10)")
        sql = NULLABLE_QUERIES[query].format(t=table.name)
        view = table.create_view(kind, sql)
        spark.sql(f"DROP TABLE {table.name}")
        table.create("a INT, u INT")
        table.insert("(NULL, 20), (2, 30)")
        assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)
    finally:
        table.drop()


@pytest.mark.parametrize("kind", KINDS)
def test_view_reports_the_nullability_of_its_columns(spark, tmp_path, kind):
    table = Table(spark, tmp_path, "delta", "a INT NOT NULL, u INT")
    try:
        view = table.create_view(kind, f"SELECT a, u FROM {table.name}")
        expected = [("a", False), ("u", True)]
        assert [(x.name, x.nullable) for x in spark.table(table.name).schema] == expected
        assert [(x.name, x.nullable) for x in spark.table(view).schema] == expected
        name = view.removeprefix("global_temp.")
        database = "global_temp" if name != view else None
        columns = spark.catalog.listColumns(name, database)
        assert [(x.name, x.nullable) for x in columns] == expected
    finally:
        table.drop()


def struct_schema(fields, *, nullable=False, comment=None):
    """The schema `a INT, s STRUCT<...>` with the fields of `s`, whose columns and fields have no nulls by default."""
    metadata = {"comment": comment} if comment else {}
    struct = StructType([StructField(name, data_type, nullable) for name, data_type in fields])
    return StructType([StructField("a", IntegerType(), nullable), StructField("s", struct, nullable, metadata)])


def write_struct_table(spark, table, schema, values):
    """Creates the table again with a schema that SQL cannot insert into, such as fields without nulls."""
    table.locations += 1
    location = table.tmp_path / f"{table.name}_{table.locations}"
    spark.sql(f"DROP TABLE IF EXISTS {table.name}")
    spark.createDataFrame(values, schema).write.format(table.fmt).option("path", str(location)).saveAsTable(table.name)


INT_FIELDS = [("f", IntegerType()), ("g", IntegerType())]

# Queries over `t (a INT NOT NULL, s STRUCT<f: INT NOT NULL, g: INT NOT NULL> NOT NULL)` and how the
# table changes: its new schema and rows. The view reads the rows that the query reads then.
STRUCT_CHANGES = {
    "struct gains a field": (
        "SELECT a, s.f AS f FROM {t}",
        struct_schema([*INT_FIELDS, ("h", IntegerType())]),
        [(1, (1, 2, 9))],
    ),
    "unread struct field retyped": (
        "SELECT a, s.f AS f FROM {t}",
        struct_schema([("f", IntegerType()), ("g", StringType())]),
        [(1, (1, "x"))],
    ),
    "read struct field has nulls": (
        "SELECT a, s.f AS f, s.f IS NULL AS n FROM {t}",
        struct_schema(INT_FIELDS, nullable=True),
        [(None, (None, 2)), (2, None), (3, (5, 6))],
    ),
    "read struct field counted": (
        "SELECT a, count(s.f) AS n, count(*) AS m FROM {t} GROUP BY a",
        struct_schema(INT_FIELDS, nullable=True),
        [(None, (None, 2)), (2, None), (3, (5, 6))],
    ),
    "whole struct with nulls and a comment": (
        "SELECT a, s FROM {t}",
        struct_schema(INT_FIELDS, nullable=True, comment="the struct"),
        [(None, (None, 2)), (2, None), (3, (5, 6))],
    ),
}


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("case", STRUCT_CHANGES)
def test_view_reads_a_struct_whose_fields_had_no_nulls(spark, tmp_path, kind, case):
    query, schema, values = STRUCT_CHANGES[case]
    table = Table(spark, tmp_path, "delta")
    try:
        write_struct_table(spark, table, struct_schema(INT_FIELDS), [(1, (1, 2)), (2, (3, 4))])
        sql = query.format(t=table.name)
        view = table.create_view(kind, sql)
        assert spark.table(view).schema == spark.sql(sql).schema
        write_struct_table(spark, table, schema, values)
        assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)
    finally:
        table.drop()


@pytest.mark.parametrize("kind", ["sv", "dv"])
def test_view_over_a_view_reads_a_struct_whose_fields_had_no_nulls(spark, tmp_path, kind):
    table = Table(spark, tmp_path, "delta")
    try:
        write_struct_table(spark, table, struct_schema(INT_FIELDS), [(1, (1, 2)), (2, (3, 4))])
        inner = table.create_view("sv", f"SELECT a, s FROM {table.name}")
        outer = table.create_view(
            kind if kind != "sv" else "svc", f"SELECT count(a) AS n, count(s.f) AS m FROM {inner}"
        )
        write_struct_table(spark, table, struct_schema(INT_FIELDS, nullable=True), [(None, (None, 2)), (2, None)])
        assert rows(spark, f"SELECT * FROM {outer}") == [(1, 0)]
    finally:
        table.drop()


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("query", ["SELECT a, b FROM {t}", "SELECT a, typeof(b) AS tb FROM {t}"])
def test_view_over_a_view_reads_the_columns_that_the_inner_view_reads(spark, tmp_path, kind, query):
    table = Table(spark, tmp_path, "delta", "a INT, b INT")
    try:
        table.insert("(1, 10)")
        inner = table.create_view("sv", query.format(t=table.name))
        outer = table.create_view(kind if kind != "sv" else "svc", f"SELECT a FROM {inner}")
        spark.sql(f"DROP TABLE {table.name}")
        table.create("a INT, b STRING")
        table.insert("(5, 'x')")
        assert COLUMNS_CHANGED in error(spark, f"SELECT * FROM {inner}")
        assert COLUMNS_CHANGED in error(spark, f"SELECT * FROM {outer}")
    finally:
        table.drop()


# Views over views of a table: the columns and rows of the table, the query of each view
# (over the table `{t}` and the view before it `{v}`), the new columns and rows of the
# table, and the rows of the last view then, which reads only what its inner views read.
VIEW_OVER_VIEW_CHANGES = {
    "unread struct field retyped": (
        "a INT, s STRUCT<f: INT, g: INT>",
        "(1, named_struct('f', 1, 'g', 2))",
        ["SELECT a, s.f AS f FROM {t}", "SELECT a, f FROM {v}"],
        "a INT, s STRUCT<f: INT, g: STRING>",
        "(2, named_struct('f', 7, 'g', 'x'))",
        [(2, 7)],
    ),
    "struct gains a field": (
        "a INT, s STRUCT<f: INT, g: INT>",
        "(1, named_struct('f', 1, 'g', 2))",
        ["SELECT a, s.f AS f FROM {t}", "SELECT a, f FROM {v}"],
        "a INT, s STRUCT<f: INT, g: INT, h: INT>",
        "(2, named_struct('f', 7, 'g', 8, 'h', 9))",
        [(2, 7)],
    ),
    "grouping column retyped": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        ["SELECT a FROM {t} GROUP BY a, b", "SELECT a FROM {v}"],
        "a INT, b STRING",
        "(1, 'z'), (1, 'z'), (2, 'y')",
        [(1,), (2,)],
    ),
    "column unread in a subquery retyped": (
        "a INT, b INT",
        "(1, 10)",
        ["SELECT a FROM (SELECT a, b FROM {t})", "SELECT a FROM {v}"],
        "a INT, b STRING",
        "(5, 'x')",
        [(5,)],
    ),
    "column unread by all views retyped": (
        "a INT, b INT, c INT",
        "(1, 10, 100)",
        ["SELECT a, c FROM {t}", "SELECT a FROM {v}", "SELECT a FROM {v}"],
        "a INT, b STRING, c INT",
        "(5, 'x', 500)",
        [(5,)],
    ),
}


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("case", VIEW_OVER_VIEW_CHANGES)
def test_view_over_views_reads_a_table_whose_unread_columns_changed(spark, tmp_path, kind, case):
    columns, values, queries, new_columns, new_values, expected = VIEW_OVER_VIEW_CHANGES[case]
    table = Table(spark, tmp_path, "delta", columns)
    views = []
    try:
        table.insert(values)
        view = None
        for i, query in enumerate(queries):
            view_kind = kind if i == len(queries) - 1 else "sv"
            view = f"{view_kind}_{uuid.uuid4().hex}"
            sql = query.format(t=table.name, v=views[-1] if views else None)
            if view_kind == "sv":
                spark.sql(f"CREATE TEMPORARY VIEW {view} AS {sql}")
            else:
                spark.sql(sql).createOrReplaceTempView(view)
            views.append(view)
        spark.sql(f"DROP TABLE {table.name}")
        table.create(new_columns)
        table.insert(new_values)
        assert rows(spark, f"SELECT * FROM {view}") == expected
    finally:
        for view in views:
            spark.sql(f"DROP VIEW IF EXISTS {view}")
        table.drop()


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_table_whose_unread_column_was_retyped_by_a_write(spark, tmp_path, kind):
    table = Table(spark, tmp_path, "delta", "id INT, extra INT")
    try:
        table.insert("(1, 2)")
        view = table.create_view(kind, f"SELECT id FROM {table.name}")
        write = spark.sql("SELECT 5 AS id, 'x' AS extra").write.mode("overwrite")
        write.option("overwriteSchema", "true").saveAsTable(table.name)
        assert rows(spark, f"SELECT * FROM {view}") == [(5,)]
    finally:
        table.drop()


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize(
    "query",
    [
        "SELECT a FROM {t} WHERE EXISTS (SELECT 1 FROM {u} WHERE {u}.k = {t}.b)",
        "SELECT a, (SELECT max(k) FROM {u} WHERE {u}.k = {t}.b) AS m FROM {t}",
    ],
)
def test_view_rejects_a_table_without_a_column_of_an_outer_reference(spark, tmp_path, fmt, kind, query):
    table = Table(spark, tmp_path, fmt, "a INT, b INT, x INT")
    other = Table(spark, tmp_path, fmt, "k INT")
    try:
        table.insert("(1, 10, 100)")
        other.insert("(10)")
        view = table.create_view(kind, query.format(t=table.name, u=other.name))
        spark.sql(f"DROP TABLE {table.name}")
        table.create("a INT, x INT")
        table.insert("(1, 100)")
        assert COLUMNS_CHANGED in error(spark, f"SELECT * FROM {view}")
    finally:
        table.drop()
        other.drop()


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_table_whose_column_comment_changed(spark, tmp_path, kind):
    table = Table(spark, tmp_path, "delta")
    try:
        table.insert("(1)")
        view = table.create_view(kind)
        spark.sql(f"ALTER TABLE {table.name} ALTER COLUMN id COMMENT 'the id'")
        table.insert("(2)")
        assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
        assert rows(spark, f"SELECT id, count(*) FROM {view} GROUP BY id") == [(1, 1), (2, 1)]
        joined = f"SELECT v.id, count(*) FROM {view} v JOIN {table.name} t ON v.id = t.id GROUP BY v.id"
        assert rows(spark, joined) == [(1, 1), (2, 1)]
    finally:
        table.drop()


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", PLAN_KINDS)
def test_plan_view_does_not_read_a_view_that_takes_the_name_of_its_table(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    spark.sql(f"DROP TABLE {table.name}")
    spark.sql(f"CREATE VIEW {table.name} AS SELECT 5L AS id")
    message = error(spark, f"SELECT * FROM {view}")
    assert NOT_FOUND in message
    assert "is now a view" in message


@pytest.mark.parametrize("kind", KINDS)
def test_view_with_time_travel_reads_its_version(spark, tmp_path, kind):
    table = Table(spark, tmp_path, "delta")
    try:
        table.insert("(1)")
        view = table.create_view(kind, f"SELECT * FROM {table.name} VERSION AS OF 1")
        table.insert("(2)")
        for _ in range(2):
            assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
        joined = f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN {table.name} t ON v.id = t.id"
        assert rows(spark, joined) == [(1, 1), (None, 2)]
        spark.sql(f"DROP TABLE {table.name}")
        assert NOT_FOUND in error(spark, f"SELECT * FROM {view}")
        # The version of a Delta Lake table is not a version of an Iceberg table.
        table.create("id BIGINT", fmt="iceberg")
        assert "created again with another format" in error(spark, f"SELECT * FROM {view}")
    finally:
        table.drop()


@pytest.mark.parametrize("kind", KINDS)
def test_view_over_an_iceberg_metadata_table_reads_the_current_snapshots(spark, tmp_path, kind):
    table = Table(spark, tmp_path, "iceberg")
    try:
        table.insert("(1)")
        view = table.create_view(kind, f"SELECT snapshot_id FROM {table.name}.snapshots")
        assert len(rows(spark, f"SELECT * FROM {view}")) == 1
        table.insert("(2)")
        assert len(rows(spark, f"SELECT * FROM {view}")) == 2
    finally:
        table.drop()


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
def test_views_over_views_read_the_current_rows(spark, table):
    table.insert("(1)")
    inner = table.create_view("sv")
    outer = table.create_view("dv", f"SELECT id FROM {inner} WHERE id > 0")
    both = table.create_view("gv", f"SELECT a.id FROM {outer} a JOIN {inner} b ON a.id = b.id")
    table.insert("(2)")
    assert rows(spark, f"SELECT * FROM {outer}") == [(1,), (2,)]
    assert rows(spark, f"SELECT * FROM {both}") == [(1,), (2,)]
    joined = f"SELECT v.id, t.id FROM {both} v FULL OUTER JOIN {table.name} t ON v.id = t.id"
    assert rows(spark, joined) == [(1, 1), (2, 2)]


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
def test_data_frame_view_that_replaces_itself_reads_the_current_rows(spark, table):
    view = f"self_{uuid.uuid4().hex}"
    try:
        spark.table(table.name).createOrReplaceTempView(view)
        for i in range(3):
            table.insert(f"({i})")
            spark.table(view).filter("id >= 0").createOrReplaceTempView(view)
            assert rows(spark, f"SELECT * FROM {view}") == [(x,) for x in range(i + 1)]
        table.insert("(-1)")
        assert rows(spark, f"SELECT * FROM {view}") == [(0,), (1,), (2,)]
    finally:
        spark.catalog.dropTempView(view)


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", PLAN_KINDS)
def test_plan_view_over_a_persistent_view_reads_the_current_rows(spark, table, kind):
    persistent = f"pv_{uuid.uuid4().hex}"
    try:
        table.insert("(1)")
        spark.sql(f"CREATE VIEW {persistent} AS SELECT id + 1000 AS id FROM {table.name}")
        view = table.create_view(kind, f"SELECT * FROM {persistent}")
        table.insert("(2)")
        assert rows(spark, f"SELECT * FROM {view}") == [(1001,), (1002,)]
        # The view keeps the definition of the persistent view that it was created with.
        spark.sql(f"CREATE OR REPLACE VIEW {persistent} AS SELECT 55L AS id")
        assert rows(spark, f"SELECT * FROM {view}") == [(1001,), (1002,)]
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {persistent}")


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_the_table_of_the_database_that_it_was_created_in(spark, tmp_path, kind):
    suffix = uuid.uuid4().hex
    first, second = f"dba_{suffix}", f"dbb_{suffix}"
    views = []
    try:
        for database, value in [(first, 100), (second, 200)]:
            spark.sql(f"CREATE DATABASE {database}")
            spark.sql(f"CREATE TABLE {database}.c (id BIGINT) USING delta LOCATION '{tmp_path / database}'")
            spark.sql(f"INSERT INTO {database}.c VALUES ({value})")
        spark.catalog.setCurrentDatabase(first)
        table = Table.__new__(Table)
        table.spark, table.views, table.name = spark, {}, "c"
        view = table.create_view(kind)
        views.append(table)
        spark.catalog.setCurrentDatabase(second)
        spark.sql(f"INSERT INTO {first}.c VALUES (101)")
        assert rows(spark, f"SELECT * FROM {view}") == [(100,), (101,)]
        assert rows(spark, f"SELECT v.id, c.id FROM {view} v CROSS JOIN c") == [(100, 200), (101, 200)]
        # A temporary view and a CTE that take the name of the table later.
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW c AS SELECT 5L AS id")
        assert rows(spark, f"SELECT * FROM {view}") == [(100,), (101,)]
        spark.catalog.dropTempView("c")
        assert rows(spark, f"WITH c AS (SELECT 5L AS id) SELECT * FROM {view}") == [(100,), (101,)]
    finally:
        spark.catalog.setCurrentDatabase("default")
        for table in views:
            for kind_, name in table.views.items():
                if kind_ in ("gv", "dgv"):
                    spark.catalog.dropGlobalTempView(name.removeprefix("global_temp."))
                else:
                    spark.catalog.dropTempView(name)
        spark.sql(f"DROP DATABASE {first} CASCADE")
        spark.sql(f"DROP DATABASE {second} CASCADE")


@pytest.mark.parametrize("table", FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_after_the_session_configuration_changed(spark, table, kind):
    options = {
        "spark.sql.caseSensitive": "true",
        "spark.sql.session.timeZone": "Asia/Tokyo",
        "spark.sql.ansi.enabled": "false",
    }
    spark.sql(f"DROP TABLE {table.name}")
    table.create("id BIGINT, Ts TIMESTAMP, n TIMESTAMP_NTZ, s STRUCT<A: INT, b: ARRAY<STRING>>, d DECIMAL(10, 2)")
    values = "(1, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00', named_struct('A', 1, 'b', array('x')), 1.5)"
    table.insert(values)
    view = table.create_view(kind)
    expected = rows(spark, f"SELECT id, unix_timestamp(Ts), CAST(n AS STRING), s.A, s.b[0], d FROM {table.name}")
    previous = {key: spark.conf.get(key) for key in options}
    try:
        for key, value in options.items():
            spark.conf.set(key, value)
            assert (
                rows(spark, f"SELECT id, unix_timestamp(Ts), CAST(n AS STRING), s.A, s.b[0], d FROM {view}") == expected
            )
            joined = f"SELECT count(*) FROM {view} v JOIN {table.name} t ON v.id = t.id"
            assert rows(spark, joined) == [(1,)]
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


def test_global_view_reads_the_table_of_the_session_that_created_it(spark, observer, tmp_path):
    table = Table(spark, tmp_path, "delta")
    local = f"loc_{uuid.uuid4().hex}"
    function = f"fn_{uuid.uuid4().hex}"
    try:
        table.insert("(1)")
        spark.sql(f"CREATE TEMPORARY VIEW {local} AS SELECT * FROM {table.name}")
        spark.udf.register(function, udf(lambda x: f"G{x}"))
        direct = table.create_view("dgv")
        nested = table.create_view("gv", f"SELECT id, {function}(id) AS f FROM {local}")
        table.insert("(2)")
        # The table is in the memory catalog of the first session, which the other session does not see.
        assert NOT_FOUND in error(observer, f"SELECT * FROM {table.name}")
        assert rows(observer, f"SELECT * FROM {direct}") == [(1,), (2,)]
        assert rows(observer, f"SELECT * FROM {nested}") == [(1, "G1"), (2, "G2")]
        assert rows(observer, f"SELECT a.id, b.id FROM {direct} a FULL OUTER JOIN {nested} b ON a.id = b.id") == [
            (1, 1),
            (2, 2),
        ]
        # A table of the same name in the other session is another table.
        observer.sql(f"CREATE TABLE {table.name} (id BIGINT) USING delta LOCATION '{tmp_path / 'observer'}'")
        observer.sql(f"INSERT INTO {table.name} VALUES (9)")
        assert rows(observer, f"SELECT * FROM {direct}") == [(1,), (2,)]
        assert rows(observer, f"SELECT a.id, b.id FROM {direct} a CROSS JOIN {table.name} b") == [(1, 9), (2, 9)]
        spark.sql(f"DROP TABLE {table.name}")
        assert NOT_FOUND in error(observer, f"SELECT * FROM {direct}")
    finally:
        observer.sql(f"DROP TABLE IF EXISTS {table.name}")
        spark.catalog.dropTempView(local)
        table.drop()


def test_views_without_catalog_tables_are_unchanged(spark, tmp_path):
    names = [f"plain_{i}_{uuid.uuid4().hex}" for i in range(6)]
    local, ranged, function, parquet, csv, random = names
    try:
        spark.createDataFrame([(1,), (2,)], "a int").createOrReplaceTempView(local)
        assert rows(spark, f"SELECT * FROM {local}") == [(1,), (2,)]
        spark.range(3).createOrReplaceTempView(ranged)
        assert rows(spark, f"SELECT * FROM {ranged}") == [(0,), (1,), (2,)]
        spark.udf.register(function, udf(lambda x: f"U{x}"))
        spark.sql(f"CREATE TEMPORARY VIEW {function} AS SELECT {function}(id) AS u FROM range(2)")
        assert rows(spark, f"SELECT * FROM {function}") == [("U0",), ("U1",)]
        # The rows of a view over files are the files of each read, and its columns are those of its creation.
        spark.range(2).write.parquet(str(tmp_path / "parquet"))
        spark.read.parquet(str(tmp_path / "parquet")).createOrReplaceTempView(parquet)
        spark.range(2, 4).write.mode("append").parquet(str(tmp_path / "parquet"))
        assert rows(spark, f"SELECT * FROM {parquet}") == [(0,), (1,), (2,), (3,)]
        spark.range(2).write.option("header", "true").csv(str(tmp_path / "csv"))
        spark.sql(f"CREATE TEMPORARY VIEW {csv} USING csv OPTIONS (path '{tmp_path / 'csv'}', header 'true')")
        assert rows(spark, f"SELECT * FROM {csv}") == [("0",), ("1",)]
        spark.sql(f"CREATE TEMPORARY VIEW {random} AS SELECT rand() AS r")
        first = rows(spark, f"SELECT * FROM {random}")
        assert first != rows(spark, f"SELECT * FROM {random}")
    finally:
        for name in names:
            spark.catalog.dropTempView(name)


def test_view_over_a_streaming_source_is_read_by_a_streaming_query(spark):
    view = f"stream_{uuid.uuid4().hex}"
    spark.readStream.format("rate").load().createOrReplaceTempView(view)
    query = spark.table(view).writeStream.format("console").start()
    try:
        assert query.isActive
        assert query.exception() is None
        query.processAllAvailable()
    finally:
        query.stop()
        spark.catalog.dropTempView(view)


@pytest.mark.spark(version=">=4", reason="Uses the Python DataSource API")
@pytest.mark.parametrize("kind", KINDS)
def test_view_over_a_python_data_source_table_reads_the_table_of_each_read(spark, kind):
    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import IntegerType, StructField, StructType

    name = f"py_{uuid.uuid4().hex}"

    class Source(DataSource):
        @classmethod
        def name(cls):
            return name

        def schema(self):
            return StructType([StructField("id", IntegerType())])

        def reader(self, schema):  # noqa: ARG002
            return Reader()

    class Reader(DataSourceReader):
        def read(self, partition):  # noqa: ARG002
            yield (7,)

    spark.dataSource.register(Source)
    table = Table.__new__(Table)
    table.spark, table.views, table.name = spark, {}, name
    try:
        spark.sql(f"CREATE TABLE {name} USING {name}")
        view = table.create_view(kind)
        assert rows(spark, f"SELECT * FROM {view}") == [(7,)]
        assert rows(spark, f"SELECT a.id, b.id FROM {view} a JOIN {name} b ON a.id = b.id") == [(7, 7)]
        spark.sql(f"DROP TABLE {name}")
        assert NOT_FOUND in error(spark, f"SELECT * FROM {view}")
    finally:
        table.drop()


PATH_KINDS = ["sql", "sql_columns", "global_sql", "data_frame", "global_data_frame", "using"]
# A view created with `USING iceberg` and a path cannot be created.
PATH_VIEWS = [(fmt, kind) for fmt in LAKE_FORMATS for kind in PATH_KINDS if (fmt, kind) != ("iceberg", "using")]


def create_path_view(spark, kind, fmt, path):
    """Creates a temporary view of the kind over the Delta Lake or Iceberg table at the path."""
    view = f"{kind}_{uuid.uuid4().hex}"
    if kind == "sql":
        spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT * FROM {fmt}.`{path}`")
    elif kind == "sql_columns":
        spark.sql(f"CREATE TEMPORARY VIEW {view} (id) AS SELECT id FROM {fmt}.`{path}`")
    elif kind == "global_sql":
        spark.sql(f"CREATE GLOBAL TEMPORARY VIEW {view} AS SELECT * FROM {fmt}.`{path}`")
    elif kind == "data_frame":
        spark.read.format(fmt).load(str(path)).createOrReplaceTempView(view)
    elif kind == "global_data_frame":
        spark.read.format(fmt).load(str(path)).createOrReplaceGlobalTempView(view)
    elif kind == "using":
        spark.sql(f"CREATE TEMPORARY VIEW {view} USING {fmt} OPTIONS (path '{path}')")
    else:
        raise ValueError(kind)
    return f"global_temp.{view}" if kind.startswith("global") else view


def drop_path_view(spark, view):
    if view.startswith("global_temp."):
        spark.catalog.dropGlobalTempView(view.removeprefix("global_temp."))
    else:
        spark.catalog.dropTempView(view)


@pytest.mark.parametrize(("fmt", "kind"), PATH_VIEWS)
def test_view_over_a_lake_table_path_reads_the_rows_committed_after_its_creation(spark, tmp_path, fmt, kind):
    path = tmp_path / "t"
    spark.sql("SELECT 1L AS id").write.format(fmt).save(str(path))
    view = create_path_view(spark, kind, fmt, path)
    try:
        assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
        spark.sql("SELECT 2L AS id").write.format(fmt).mode("append").save(str(path))
        assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
        # The view and the path are read at one version of the table.
        joined = f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN {fmt}.`{path}` t ON v.id = t.id"
        assert rows(spark, joined) == [(1, 1), (2, 2)]
        spark.sql("SELECT 3L AS id").write.format(fmt).mode("append").save(str(path))
        assert rows(spark, f"SELECT count(*) FROM {view} a JOIN {view} b ON a.id = b.id") == [(3,)]
    finally:
        drop_path_view(spark, view)


def test_view_created_with_using_reads_the_rows_inserted_through_it(spark, tmp_path):
    fmt = "delta"
    path = tmp_path / "t"
    spark.sql("SELECT 1L AS id").write.format(fmt).save(str(path))
    view = create_path_view(spark, "using", fmt, path)
    try:
        spark.sql(f"INSERT INTO {view} VALUES (2)")
        assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
        spark.sql(f"INSERT INTO {view} SELECT id + 10 FROM {view}")
        assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,), (11,), (12,)]
    finally:
        drop_path_view(spark, view)


@pytest.mark.parametrize("kind", PATH_KINDS)
def test_view_over_a_lake_table_path_with_time_travel_reads_its_version(spark, tmp_path, kind):
    path = tmp_path / "t"
    spark.sql("SELECT 1L AS id").write.format("delta").save(str(path))
    view = f"{kind}_{uuid.uuid4().hex}"
    if kind in ("sql", "sql_columns", "global_sql"):
        scope = "GLOBAL TEMPORARY" if kind == "global_sql" else "TEMPORARY"
        spark.sql(f"CREATE {scope} VIEW {view} AS SELECT * FROM delta.`{path}` VERSION AS OF 0")
    elif kind == "using":
        spark.sql(f"CREATE TEMPORARY VIEW {view} USING delta OPTIONS (path '{path}', versionAsOf '0')")
    else:
        df = spark.read.format("delta").option("versionAsOf", "0").load(str(path))
        if kind == "global_data_frame":
            df.createOrReplaceGlobalTempView(view)
        else:
            df.createOrReplaceTempView(view)
    view = f"global_temp.{view}" if kind.startswith("global") else view
    try:
        spark.sql("SELECT 2L AS id").write.format("delta").mode("append").save(str(path))
        for _ in range(2):
            assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
        joined = f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN delta.`{path}` t ON v.id = t.id"
        assert rows(spark, joined) == [(1, 1), (None, 2)]
    finally:
        drop_path_view(spark, view)
