# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

"""A temporary view reads the current version of the catalog tables that it was created over."""

from __future__ import annotations

import shutil
import time
import uuid

import pytest
from pyspark.sql.functions import array, col, count, explode, pandas_udf, rand, udf, xxhash64
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests._dev.spark.catalog.temp_view_utils import (
    COLUMNS_CHANGED,
    DEFINITIONS,
    FORMATS,
    KINDS,
    LAKE_FORMATS,
    NOT_FOUND,
    PLAN_KINDS,
    create_view,
    drop_view,
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
    """Returns another session of the server of `spark`."""
    with spark_session_factory(remote) as sessions:
        session = sessions.create()
        assert session.session_id != spark.session_id
        yield session


@pytest.fixture
def table(tables, request):
    return tables(request.param)


def assert_view_reads(spark, view, expected):
    """Asserts that the view reads the rows, or, if `expected` is `None`, what its definition reads now: the
    view then resolves its definition again and reads its columns of what the query or the DataFrame reads
    now, or it fails as that fails."""
    if expected is None:
        definition, columns = DEFINITIONS[view]
        try:
            expected = rows(spark, definition.select(*(col(f"`{x}`") for x in columns)))
        except Exception:  # noqa: BLE001
            error(spark, f"SELECT * FROM {view}")
            return
    assert rows(spark, f"SELECT * FROM {view}") == expected


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
def test_sql_view_over_a_data_frame_argument_reads_the_rows_committed_after_its_creation(spark, table):
    table.insert("(1)")
    view = f"sv_{uuid.uuid4().hex}"
    spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT * FROM {{df}}", df=spark.table(table.name))
    table.views.append(view)
    table.insert("(2)")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
    joined = f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN {table.name} t ON v.id = t.id"
    assert rows(spark, joined) == [(1, 1), (2, 2)]


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
    table.recreate("id BIGINT", "(7)", fmt="iceberg" if table.fmt == "delta" else "delta")
    assert rows(spark, f"SELECT * FROM {view}") == [(7,)]
    joined = f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN {table.name} t ON v.id = t.id"
    assert rows(spark, joined) == [(7, 7)]


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_table_whose_unread_columns_were_dropped_or_renamed(spark, tables, kind):
    table = tables("iceberg", "id BIGINT, extra STRING, other STRING")
    table.insert("(1, 'x', 'y')")
    view = table.create_view(kind, f"SELECT id FROM {table.name}")
    spark.sql(f"ALTER TABLE {table.name} DROP COLUMN extra")
    spark.sql(f"ALTER TABLE {table.name} RENAME COLUMN other TO another")
    table.insert("(2, 'z')")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
    spark.sql(f"ALTER TABLE {table.name} RENAME COLUMN id TO key")
    message = error(spark, f"SELECT * FROM {view}")
    # The view fails to resolve its definition again.
    assert COLUMNS_CHANGED in message
    assert "- `id` BIGINT has been removed" in message


@pytest.mark.parametrize("kind", ["sv", "dv"])
def test_views_over_views_read_a_table_whose_unread_column_changed_its_type(spark, tables, kind):
    table = tables("delta", "a INT, b INT, u INT")
    table.insert("(1, 10, 100), (2, 20, 200)")
    view = table.create_view(kind, f"SELECT a, b FROM {table.name}")
    for _ in range(2):
        frame = spark.table(view).selectExpr("a + 1 AS a", "b") if kind == "dv" else None
        view = create_view(spark, kind, f"SELECT a + 1 AS a, b FROM {view}", frame)
        table.views.append(view)
    table.recreate("a INT, b INT, u STRING", "(3, 30, 'x')")
    assert rows(spark, f"SELECT * FROM {view}") == [(5, 30)]


# Queries over `t (id INT, k INT, b INT)` that compute a value of `b` that they do not read, and the
# query without that value.
UNUSED_VALUE_QUERIES = {
    "aggregate": (
        "SELECT id FROM (SELECT id, max(b) AS mb FROM {t} GROUP BY id)",
        "SELECT id FROM {t} GROUP BY id",
    ),
    "window": ("SELECT id FROM (SELECT id, sum(b) OVER (PARTITION BY id) AS s FROM {t})", "SELECT id FROM {t}"),
    "union": (
        "SELECT id FROM (SELECT id, b FROM {t} UNION ALL SELECT id, b FROM {t})",
        "SELECT id FROM {t} UNION ALL SELECT id FROM {t}",
    ),
    "column": ("SELECT id, k FROM (SELECT id, k, b AS e FROM {t})", "SELECT id, k FROM {t}"),
    "expression": ("SELECT id, k FROM (SELECT id, k, b + 1 AS e FROM {t})", "SELECT id, k FROM {t}"),
    "scalar subquery": (
        "SELECT id, k FROM (SELECT id, k, (SELECT max(b) FROM {t}) AS e FROM {t})",
        "SELECT id, k FROM {t}",
    ),
    "exists": (
        "SELECT id, k FROM (SELECT x.id, x.k, EXISTS (SELECT 1 FROM {t} u WHERE u.id = x.b) AS e FROM {t} x)",
        "SELECT id, k FROM {t}",
    ),
    "lateral": (
        "SELECT id, k FROM (SELECT x.id, x.k, l.e FROM {t} x, LATERAL (SELECT x.b + 1 AS e) l)",
        "SELECT id, k FROM {t}",
    ),
    "cte": ("WITH c AS (SELECT id, k, b + 1 AS e FROM {t}) SELECT id, k FROM c", "SELECT id, k FROM {t}"),
    "passed on": ("SELECT id, k FROM (SELECT id, k, b FROM {t})", "SELECT id, k FROM {t}"),
    "passed on cte": ("WITH s AS (SELECT id, k, b FROM {t}) SELECT id, k FROM s", "SELECT id, k FROM {t}"),
}

# The changes of `b`: the format of the table, and whether the table is created again with or without
# `b` under another name, or `b` is dropped or renamed in place.
UNUSED_VALUE_CHANGES = [
    ("delta", "drop"),
    ("delta", "rename"),
    ("parquet", "drop"),
    ("parquet", "rename"),
    ("delta_cm", "alter drop"),
    ("delta_cm", "alter rename"),
]


def change_unused_value(spark, table, change):
    if change == "drop":
        table.recreate("id INT, k INT", "(3, 30)")
    elif change == "rename":
        table.recreate("id INT, k INT, c INT", "(3, 30, 300)")
    elif change == "alter drop":
        spark.sql(f"ALTER TABLE {table.name} DROP COLUMN b")
        table.insert("(3, 30)")
    else:
        spark.sql(f"ALTER TABLE {table.name} RENAME COLUMN b TO c")
        table.insert("(3, 30, 300)")


@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize("query", UNUSED_VALUE_QUERIES)
@pytest.mark.parametrize(("fmt", "change"), UNUSED_VALUE_CHANGES)
def test_view_reads_a_table_whose_column_of_a_value_that_it_does_not_read_changed(
    spark, tables, kind, query, fmt, change
):
    sql, unused_removed = UNUSED_VALUE_QUERIES[query]
    using, properties = METADATA_FORMATS[fmt]
    table = tables(using, "id INT, k INT, b INT", properties)
    table.insert("(1, 10, 100), (2, 20, 200)")
    view = table.create_view(kind, sql.format(t=table.name))
    change_unused_value(spark, table, change)
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, unused_removed.format(t=table.name))


@pytest.mark.parametrize(("fmt", "change"), UNUSED_VALUE_CHANGES)
def test_data_frame_view_reads_a_table_whose_column_of_a_value_that_it_does_not_read_changed(
    spark, tables, fmt, change
):
    using, properties = METADATA_FORMATS[fmt]
    table = tables(using, "id INT, k INT, b INT", properties)
    table.insert("(1, 10, 100), (2, 20, 200)")
    frame = spark.table(table.name).withColumn("e", col("b")).select("id", "k")
    view = table.create_view("dv", frame=frame)
    change_unused_value(spark, table, change)
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, f"SELECT id, k FROM {table.name}")


@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
def test_view_of_a_window_over_a_sorted_path_reads_files_whose_unread_column_was_retyped(spark, tmp_path, kind):
    path = str(tmp_path / f"p_{uuid.uuid4().hex}")
    spark.sql("SELECT 7 AS a, 1 AS b").write.parquet(path)
    sql = (
        "SELECT q.a, dense_rank() OVER (PARTITION BY q.a ORDER BY q.a) AS r "
        f"FROM (SELECT * FROM parquet.`{path}` ORDER BY a) q"
    )
    view = create_view(spark, kind, sql)
    try:
        spark.sql("SELECT 7 AS a, CAST(1 AS BIGINT) AS b").write.mode("overwrite").parquet(path)
        assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql) == [(7, 1)]
    finally:
        drop_view(spark, view)


# Queries over `t (a INT, b INT)` and `w (a INT, c INT)` whose star or natural join reads the column
# that `t` or `w` gains: the query, the table, its columns and rows then, and the columns of the view.
GAINED_COLUMN_QUERIES = {
    "distinct star": (
        "SELECT a FROM (SELECT DISTINCT * FROM {t})",
        "t",
        "a INT, b INT, c INT",
        "(1, 1, 1), (1, 1, 2), (2, 2, 2)",
        "a",
    ),
    "union of stars": (
        "SELECT * FROM {t} UNION SELECT * FROM {t}",
        "t",
        "a INT, b INT, c INT",
        "(1, 1, 1), (1, 1, 2), (2, 2, 2)",
        "a, b",
    ),
    "natural join": (
        "SELECT a, b, c FROM {t} NATURAL JOIN {w}",
        "w",
        "a INT, c INT, b INT",
        "(1, 10, 1), (2, 20, 9)",
        "a, b, c",
    ),
    "star": ("SELECT * FROM {t}", "t", "a INT, b INT, c INT", "(1, 1, 1), (1, 1, 2), (2, 2, 2)", "a, b"),
}


@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("case", GAINED_COLUMN_QUERIES)
def test_view_reads_the_column_that_its_star_or_natural_join_gains(spark, tables, kind, fmt, case):
    query, changed, columns, values, outputs = GAINED_COLUMN_QUERIES[case]
    inputs = {"t": tables(fmt, "a INT, b INT"), "w": tables(fmt, "a INT, c INT")}
    inputs["t"].insert("(1, 1), (1, 1), (2, 2)")
    inputs["w"].insert("(1, 10), (2, 20)")
    sql = query.format(t=inputs["t"].name, w=inputs["w"].name)
    view = inputs["t"].create_view(kind, sql)
    inputs[changed].recreate(columns, values)
    # The view keeps its columns, and reads what its query reads now.
    assert spark.table(view).columns == outputs.split(", ")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, f"SELECT {outputs} FROM ({sql})")


@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize("query", ["SELECT DISTINCT * FROM {t} VERSION AS OF 1", "SELECT * FROM {t} VERSION AS OF 1"])
@pytest.mark.parametrize(
    ("fmt", "change"),
    [
        ("delta", "ADD COLUMNS (z INT)"),
        ("delta_cm", "ADD COLUMNS (z INT)"),
        ("delta_cm", "RENAME COLUMN c TO c_r"),
    ],
)
def test_view_of_a_version_reads_the_version_after_the_table_gained_or_renamed_a_column(
    spark, tables, kind, query, fmt, change
):
    using, properties = METADATA_FORMATS[fmt]
    table = tables(using, "k INT, c INT", properties)
    table.insert("(1, 1), (1, 1), (2, 2)")
    view = table.create_view(kind, query.format(t=table.name))
    expected = rows(spark, f"SELECT * FROM {view}")
    spark.sql(f"ALTER TABLE {table.name} {change}")
    table.insert("(3, 3, 3)" if change.startswith("ADD") else "(3, 3)")
    # The columns of the version do not change, and the view reads the rows of the version.
    assert rows(spark, f"SELECT * FROM {view}") == expected


@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
def test_view_reads_the_current_database_of_its_creation(spark, tmp_path, kind):
    suffix = uuid.uuid4().hex
    a, b = f"a_{suffix}", f"b_{suffix}"
    catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    table = f"{a}.t"
    views = []
    spark.sql(f"CREATE DATABASE {a}")
    spark.sql(f"CREATE DATABASE {b}")
    try:
        spark.catalog.setCurrentDatabase(a)
        spark.sql(f"CREATE TABLE {table} (x INT, y INT) USING delta LOCATION '{tmp_path / suffix}_0'")
        spark.sql(f"INSERT INTO {table} VALUES (1, 10), (2, 20)")
        views.append(create_view(spark, kind, "SELECT current_database() AS d, current_catalog() AS c, x FROM t"))
        views.append(create_view(spark, kind, f"SELECT x FROM t WHERE current_database() = '{a}'"))
        spark.catalog.setCurrentDatabase(b)
        # The views read the table as it is, after a change of a column that they read, which makes
        # them resolve their queries again, and after the change is undone.
        for i, columns in enumerate([None, "x BIGINT, y INT", "x INT, y INT"]):
            if columns is not None:
                spark.sql(f"DROP TABLE {table}")
                spark.sql(f"CREATE TABLE {table} ({columns}) USING delta LOCATION '{tmp_path / suffix}_{i}'")
                spark.sql(f"INSERT INTO {table} VALUES (1, 10), (2, 20)")
            assert rows(spark, f"SELECT * FROM {views[0]}") == [(a, catalog, 1), (a, catalog, 2)]
            assert rows(spark, f"SELECT * FROM {views[1]}") == [(1,), (2,)]
    finally:
        spark.catalog.setCurrentDatabase("default")
        for view in views:
            drop_view(spark, view)
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"DROP DATABASE IF EXISTS {a} CASCADE")
        spark.sql(f"DROP DATABASE IF EXISTS {b} CASCADE")


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_column_renamed_only_in_letter_case(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    table.recreate("ID BIGINT", "(2)")
    assert spark.sql(f"SELECT * FROM {view}").columns == ["id"]
    assert rows(spark, f"SELECT * FROM {view}") == [(2,)]
    assert rows(spark, f"SELECT id, count(*) FROM {view} WHERE id > 0 GROUP BY id") == [(2, 1)]


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("created", ["false", "true"])
def test_view_matches_columns_as_the_session_did_when_it_was_created(spark, table, kind, created):
    previous = spark.conf.get("spark.sql.caseSensitive")
    try:
        table.insert("(1)")
        spark.conf.set("spark.sql.caseSensitive", created)
        view = table.create_view(kind)
        table.recreate("ID BIGINT", "(2)")
        spark.conf.set("spark.sql.caseSensitive", "true" if created == "false" else "false")
        if created == "false":
            assert rows(spark, f"SELECT * FROM {view}") == [(2,)]
        else:
            # A definition that was resolved case-sensitively can tell `id` from `ID`, also
            # when the view resolves its definition again.
            assert COLUMNS_CHANGED in error(spark, f"SELECT * FROM {view}")
    finally:
        spark.conf.set("spark.sql.caseSensitive", previous)


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("query", "created", "current", "value"),
    [
        ("SELECT hash(b) AS v, xxhash64(b) AS w FROM {t}", "b BIGINT", "b INT", "7"),
        ("SELECT CAST(b AS STRING) AS v, concat('x', b) AS w FROM {t}", "b DOUBLE", "b FLOAT", "1.1"),
        ("SELECT CAST(b AS STRING) AS v FROM {t}", "b DECIMAL(10, 2)", "b DECIMAL(5, 1)", "1.1"),
    ],
)
def test_view_computes_on_the_types_of_the_columns_that_its_definition_resolves_to(
    spark, tables, fmt, kind, query, created, current, value
):
    table = tables(fmt, f"id INT, {created}")
    table.insert(f"(1, {value})")
    view = table.create_view(kind, query.format(t=table.name))
    table.recreate(f"id INT, {current}", f"(2, {value})")
    # The values of the column convert exactly to the type that the view was created with. The view
    # resolves its definition again and computes on the current type, as the query does.
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, query.format(t=table.name))


@pytest.mark.parametrize("kind", KINDS)
def test_view_computes_as_its_query_on_the_values_of_a_narrowed_column(spark, tables, kind):
    table = tables("delta", "id INT, b BIGINT")
    table.insert("(1, 7)")
    sql = f"SELECT b * 2 AS v FROM {table.name}"
    view = table.create_view(kind, sql)
    table.recreate("id INT, b INT", "(2, 2000000000)")
    # The product is computed on the current type, as in the query.
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("current", "value"),
    [
        ("s STRUCT<y: INT, x: INT>", "named_struct('y', 3, 'x', 4)"),
        ("s STRUCT<X: INT, y: INT>", "named_struct('X', 3, 'y', 4)"),
        ("s STRUCT<x: INT, y: INT, z: INT>", "named_struct('x', 3, 'y', 4, 'z', 5)"),
    ],
)
@pytest.mark.parametrize(
    "query",
    ["SELECT to_json(s) AS v FROM {t}", "SELECT xxhash64(s) AS v FROM {t}", "SELECT CAST(s AS STRING) AS v FROM {t}"],
)
def test_view_computes_on_a_struct_whose_fields_changed(spark, tables, kind, current, value, query):
    table = tables("delta", "id INT, s STRUCT<x: INT, y: INT>")
    table.insert("(1, named_struct('x', 1, 'y', 2))")
    sql = query.format(t=table.name)
    view = table.create_view(kind, sql)
    table.recreate(f"id INT, {current}", f"(2, {value})")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("where", ["", " WHERE ts > TIMESTAMP '1970-01-01 12:00:00'"])
def test_view_reads_a_timestamp_whose_time_zone_changed(spark, tmp_path, kind, where):
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Parquet files of the same instants, labeled with different time zones.
    for zone in ["UTC", "America/New_York"]:
        directory = tmp_path / zone.replace("/", "_")
        directory.mkdir()
        values = pa.array([0, 86_400_000_000], pa.timestamp("us", tz=zone))
        pq.write_table(pa.table({"ts": values}), directory / "part-0.parquet")
    name = f"t_{uuid.uuid4().hex}"
    view = None
    try:
        spark.sql(f"CREATE TABLE {name} USING parquet LOCATION '{tmp_path / 'UTC'}'")
        sql = f"SELECT ts FROM {name}{where}"
        view = create_view(spark, kind, sql)
        spark.sql(f"DROP TABLE {name}")
        spark.sql(f"CREATE TABLE {name} USING parquet LOCATION '{tmp_path / 'America_New_York'}'")
        # The view resolves its definition again, which reads the instants of the query.
        assert spark.sql(f"SELECT * FROM {view}").collect() == spark.sql(sql).collect()
    finally:
        if view is not None:
            drop_view(spark, view)
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("kind", PLAN_KINDS)
def test_view_keeps_the_column_metadata_that_it_was_created_with(spark, tables, kind):
    table = tables("delta")
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


# The cases of a table that is created again with other columns: its columns and rows,
# the query of a view over it, the columns and rows that it is created with again, and
# the rows that the view reads then, or `None` if the view resolves its query again and
# reads what the query reads now, or fails as the query fails.
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
    # A field of a struct that the query builds reads only the value of the field.
    "field of a named struct, other value retyped": (
        "a INT, b INT",
        "(1, 10)",
        "SELECT named_struct('p', a, 'q', b).p AS p FROM {t}",
        "a INT, b STRING",
        "(5, 'x')",
        [(5,)],
    ),
    "field of a struct, other value retyped": (
        "a INT, b INT",
        "(1, 10)",
        "SELECT struct(a, b).a AS p FROM {t}",
        "a INT, b STRING",
        "(5, 'x')",
        [(5,)],
    ),
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
    # The current values of the column convert exactly to the type that the view reads.
    "read column narrowed": (
        "a INT, b BIGINT, x INT",
        "(1, 10, 100)",
        "SELECT a, b FROM {t}",
        "a INT, b INT, x INT",
        "(5, 50, 500)",
        [(5, 50)],
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
    "exists subquery of all columns, unread column retyped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM {t} WHERE EXISTS (SELECT * FROM {t} AS t2 WHERE t2.a = {t}.a)",
        "a INT, b STRING, x INT",
        "(5, 'q', 500)",
        [(5,)],
    ),
    "scalar subquery column retyped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a, (SELECT max(b) FROM {t}) AS m FROM {t}",
        "a INT, b BIGINT, x INT",
        "(5, 50, 500)",
        None,
    ),
    "in subquery column retyped": (
        "a INT, b INT, x INT",
        "(1, 1, 100)",
        "SELECT a FROM {t} WHERE a IN (SELECT b FROM {t})",
        "a INT, b BIGINT, x INT",
        "(5, 5, 500)",
        None,
    ),
    "unread aggregate, its column retyped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM (SELECT a, sum(b) AS s FROM {t} GROUP BY a)",
        "a INT, b STRING, x INT",
        "(5, 'q', 500)",
        [(5,)],
    ),
    "unread window value, its keys retyped": (
        "a INT, b INT, x INT",
        "(1, 10, 100)",
        "SELECT a FROM (SELECT a, row_number() OVER (PARTITION BY b ORDER BY x) AS r FROM {t})",
        "a INT, b STRING, x STRING",
        "(5, 'q', 'r')",
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
    "aliased distinct row column retyped": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        "SELECT h0 FROM (SELECT DISTINCT a AS h0, b AS h1 FROM {t}) dh",
        "a INT, b BIGINT",
        "(1, 10), (1, 10), (5, 30)",
        [(1,), (5,)],
    ),
    "aliased grouping column retyped": (
        "a INT, b INT",
        "(1, 10), (1, 20), (2, 30)",
        "SELECT h0 FROM (SELECT a AS h0, b AS h1 FROM {t} GROUP BY a, b) dh",
        "a INT, b BIGINT",
        "(1, 10), (1, 10), (5, 30)",
        [(1,), (5,)],
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
    # A column that is only compared, or counted, summed or averaged, can change to another
    # signed integer type, as Spark analyzes the query of the view again.
    "filter column widened": (
        "a INT, b INT",
        "(1, 10), (2, -5)",
        "SELECT a FROM {t} WHERE b > 0",
        "a INT, b BIGINT",
        "(5, 3000000000), (6, -3000000000)",
        [(5,)],
    ),
    "filter column narrowed": (
        "a INT, b BIGINT",
        "(1, 3000000000), (2, -5)",
        "SELECT a FROM {t} WHERE b IN (7, 3000000000)",
        "a INT, b TINYINT",
        "(5, 7), (6, -1)",
        [(5,)],
    ),
    "sum column widened": (
        "a INT, b INT",
        "(1, 10)",
        "SELECT a, sum(b) AS s FROM {t} GROUP BY a",
        "a INT, b BIGINT",
        "(5, 3000000000), (5, 3000000000)",
        [(5, 6000000000)],
    ),
    "having column widened": (
        "a INT, b INT",
        "(1, 10), (2, -5)",
        "SELECT a FROM {t} GROUP BY a HAVING count(DISTINCT b) > 1 OR avg(b) > 0",
        "a INT, b BIGINT",
        "(5, 3000000000), (6, -3000000000), (6, 3000000000), (7, -1)",
        [(5,), (6,)],
    ),
    "join key widened": (
        "a INT, b INT",
        "(1, 10)",
        "SELECT x.a FROM {t} x JOIN {t} y ON x.b = y.b",
        "a INT, b BIGINT",
        "(5, 3000000000), (6, 7)",
        [(5,), (6,)],
    ),
    # A sort column can change to any type, and the rows are in the order of its values now.
    "sort column retyped to a string": (
        "a INT, b INT",
        "(1, 10), (2, 9)",
        "SELECT a FROM {t} ORDER BY b LIMIT 1",
        "a INT, b STRING",
        "(5, '10'), (6, '9')",
        [(5,)],
    ),
    # Columns compared with a FLOAT, or retyped to a DOUBLE.
    "filter column compared with a float widened": (
        "a INT, b INT, f FLOAT",
        "(1, 10, 1.5)",
        "SELECT a FROM {t} WHERE b > f",
        "a INT, b BIGINT, f FLOAT",
        "(5, 3000000000, 1.5)",
        None,
    ),
    "filter column compared with a float narrowed": (
        "a INT, b BIGINT, f FLOAT",
        "(1, 10, 1.5)",
        "SELECT a FROM {t} WHERE b > f",
        "a INT, b INT, f FLOAT",
        "(5, 7, 1.5)",
        [(5,)],
    ),
    "filter column retyped to a double": (
        "a INT, b INT",
        "(1, 10)",
        "SELECT a FROM {t} WHERE b > 0",
        "a INT, b DOUBLE",
        "(5, 0.5)",
        None,
    ),
    "count distinct column retyped to a double": (
        "a INT, b INT",
        "(1, 10)",
        "SELECT count(DISTINCT b) AS n FROM {t}",
        "a INT, b DOUBLE",
        "(5, 0.0), (6, -0.0)",
        None,
    ),
}


# The cases that run for each table format and each way to store a view. The column rules
# do not depend on either, so the other cases run for one of each.
MATRIX_CASES = {
    "unread column dropped",
    "read column retyped",
    "read column narrowed",
    "struct field added",
    "unread struct field retyped",
    "distinct row column retyped",
    "filter column widened",
    "column dropped on one side",
    "column narrowed on one side",
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
def test_view_reads_a_table_whose_columns_changed(spark, tables, fmt, kind, case):
    columns, values, query, new_columns, new_values, expected = COLUMN_CHANGES[case]
    table = tables(fmt, columns)
    table.insert(values)
    view = table.create_view(kind, query.format(t=table.name))
    table.recreate(new_columns, new_values)
    assert_view_reads(spark, view, expected)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (None, NOT_FOUND),
        (("b INT", "(20)"), COLUMNS_CHANGED),
        (("a STRING, b INT", "('x', 20)"), COLUMNS_CHANGED),
    ],
    ids=["table dropped", "read column dropped", "read column retyped"],
)
@pytest.mark.parametrize("kind", KINDS)
def test_view_reports_its_columns_after_a_change_that_fails_its_reads(spark, tables, kind, change, message):
    table = tables("delta", "a INT, b INT")
    table.insert("(1, 10)")
    query = f"SELECT a, b + 1 AS c FROM {table.name}"
    view = table.create_view(kind, query)
    schema = spark.table(view).schema
    if change is None:
        spark.sql(f"DROP TABLE {table.name}")
    else:
        table.recreate(*change)
    if change is not None and change[0].startswith("a "):
        # The view resolves its definition again, and reports the columns that it reads.
        assert spark.table(view).schema == spark.sql(query).schema
        assert rows(spark, f"SELECT * FROM {view}") == [("x", 21)]
    else:
        # The schema of the view is the schema of its stored plan, and a read fails.
        assert spark.table(view).schema == schema
        assert message in error(spark, f"SELECT * FROM {view}")


@pytest.mark.parametrize("change", ["retyped", "dropped"])
@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
def test_view_of_the_type_of_a_scalar_subquery_reads_the_current_type(spark, tables, kind, change):
    table = tables("delta", "b INT")
    table.insert("(1)")
    view = table.create_view(kind, f"SELECT typeof((SELECT b FROM {table.name} LIMIT 1)) AS ty")
    outer = table.create_view("sv", f"SELECT ty FROM {view}")
    if change == "retyped":
        table.recreate("b STRING", "('x')")
    else:
        spark.sql(f"DROP TABLE {table.name}")
    for name in (view, outer):
        if change == "retyped":
            # The view resolves its definition again, as Spark analyzes the query of a SQL view at
            # each read.
            assert rows(spark, f"SELECT * FROM {name}") == [("string",)]
        else:
            assert NOT_FOUND in error(spark, f"SELECT * FROM {name}")


@pytest.mark.parametrize("fmt", ["parquet", "delta"])
@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
def test_view_groups_a_cast_to_an_interval_of_a_column_without_nulls(spark, tables, fmt, kind):
    table = tables(fmt, "s INT NOT NULL")
    table.insert("(1), (2), (2)")
    query = f"SELECT CAST(s AS INTERVAL YEAR) AS x FROM {table.name}"
    view = table.create_view(kind, query)
    # Spark Connect cannot collect year-month intervals, so the groups are counted.
    groups = "SELECT count(*) FROM (SELECT x, count(*) FROM {} GROUP BY x)"
    assert rows(spark, groups.format(view)) == rows(spark, groups.format(f"({query})")) == [(2,)]


@pytest.mark.parametrize(
    ("data_type", "value"), [("TIMESTAMP", "TIMESTAMP '2020-01-01 00:00:00'"), ("DECIMAL(10,2)", "1.5")]
)
def test_view_over_a_data_frame_view_groups_a_column_that_has_no_nulls_now(spark, tables, data_type, value):
    table = tables("parquet", f"k INT, c {data_type}")
    table.insert(f"(1, {value}), (2, {value})")
    inner = table.create_view("dv", f"SELECT k, c FROM {table.name}")
    table.recreate(f"k INT, c {data_type} NOT NULL", f"(1, {value}), (2, {value})")
    outer = table.create_view("sv", f"SELECT c, count(*) AS n FROM {inner} GROUP BY c")
    direct = f"SELECT c, count(*) AS n FROM {table.name} GROUP BY c"
    assert rows(spark, f"SELECT * FROM {outer}") == rows(spark, direct)


# DataFrames of the distinct rows of a table `(a, b)` by both columns, which read only `a`.
DISTINCT_FRAMES = {
    "distinct": lambda frame: frame.distinct().select("a"),
    "dropDuplicates": lambda frame: frame.dropDuplicates(["a", "b"]).select("a"),
}


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("case", DISTINCT_FRAMES)
def test_data_frame_view_of_distinct_rows_reads_a_table_whose_key_changed(spark, tables, fmt, case):
    table = tables(fmt, "a INT, b INT")
    table.insert("(1, 10), (1, 20), (2, 30)")
    view = table.create_view("dv", frame=DISTINCT_FRAMES[case](spark.table(table.name)))
    # The current values of a column that tells rows apart tell them apart, of whatever type.
    table.recreate("a INT, b STRING", "(1, 'z'), (1, 'y'), (2, 'y')")
    assert_view_reads(spark, view, [(1,), (1,), (2,)])
    table.recreate("a INT", "(1), (2)")
    assert_view_reads(spark, view, None)


# DataFrames of the distinct rows of a table `(k, a, b)`, which read only `a`, and whether `k`
# tells their rows apart.
DUPLICATE_FRAMES = {
    "dropDuplicates(a)": (lambda frame: frame.dropDuplicates(["a"]).select("a"), False),
    "dropDuplicates(a, b)": (lambda frame: frame.dropDuplicates(["a", "b"]).select("a"), False),
    "dropDuplicates()": (lambda frame: frame.dropDuplicates().select("a"), True),
}


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("change", ["retyped", "dropped"])
@pytest.mark.parametrize("case", DUPLICATE_FRAMES)
def test_data_frame_view_of_distinct_rows_reads_a_table_whose_other_column_changed(spark, tables, case, change, nested):
    query, key = DUPLICATE_FRAMES[case]
    table = tables("parquet", "k INT, a INT, b INT")
    table.insert("(1, 1, 10), (2, 1, 20), (3, 2, 30)")
    view = table.create_view("dv", frame=query(spark.table(table.name)))
    if nested:
        view = table.create_view("sv", f"SELECT a FROM {view}")
    if change == "retyped":
        table.recreate("k BIGINT, a INT, b INT", "(1, 1, 10), (9, 5, 50)")
    else:
        table.recreate("a INT, b INT", "(1, 10), (5, 50)")
    assert_view_reads(spark, view, None if key and change == "dropped" else [(1,), (5,)])


# The cases of a view over the union of two tables `t` and `w (a INT, b INT)`, with the rows
# (1, 10), (1, 20), (2, 30) and (1, 10), (3, 40), when one of them is created again with
# other columns: the query of the view, the table, its columns and rows, and the rows
# that the view reads then, a query of the rows that it reads then, or `None` if the view
# resolves its query again and reads what the query reads now, or fails as the query fails.
UNION_CHANGES = {
    # The union reads `b` of neither input, so the view reads the current rows of `a`,
    # while the query no longer resolves (main reads the rows of `w` before the change).
    "column dropped on one side": (
        "SELECT a FROM (SELECT a, b FROM {t} UNION ALL SELECT a, b FROM {w})",
        "w",
        "a INT",
        "(1), (3)",
        "SELECT a FROM {t} UNION ALL SELECT a FROM {w}",
    ),
    "column retyped on one side": (
        "SELECT a FROM (SELECT a, b FROM {t} UNION ALL SELECT a, b FROM {w})",
        "t",
        "a INT, b STRING",
        "(1, 'z'), (1, 'y'), (2, 'y')",
        [(1,), (1,), (1,), (2,), (3,)],
    ),
    "column narrowed on one side": (
        "SELECT b FROM (SELECT a, b FROM {t} UNION ALL SELECT a, b FROM {w})",
        "t",
        "a INT, b SMALLINT",
        "(1, 7)",
        [(7,), (10,), (40,)],
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
def test_view_of_a_union_reads_a_table_whose_columns_changed(spark, tables, fmt, kind, case):
    query, changed, columns, values, expected = UNION_CHANGES[case]
    union = {"t": tables(fmt, "a INT, b INT"), "w": tables(fmt, "a INT, b INT")}
    union["t"].insert("(1, 10), (1, 20), (2, 30)")
    union["w"].insert("(1, 10), (3, 40)")
    view = union["t"].create_view(kind, query.format(t=union["t"].name, w=union["w"].name))
    union[changed].recreate(columns, values)
    if isinstance(expected, str):
        expected = rows(spark, expected.format(t=union["t"].name, w=union["w"].name))
    assert_view_reads(spark, view, expected)


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
def test_data_frame_view_of_a_union_reads_a_column_of_its_second_input_by_its_position(spark, tables, fmt):
    # The union reads the column `a` of `w` only by its position, under the name of `t`.
    t = tables(fmt, "a INT, b INT")
    t.insert("(1, 10)")
    w = tables(fmt, "a INT, b INT")
    w.insert("(2, 20)")

    def frame():
        return spark.table(t.name).union(spark.table(w.name)).select("a")

    view = t.create_view("dv", frame=frame())
    w.recreate("a BIGINT, b INT", "(3000000000, 30)")
    assert sorted(spark.table(view).collect()) == sorted(frame().collect()) == [(1,), (3000000000,)]


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
def test_view_reads_a_column_that_has_nulls_now(spark, tables, fmt, kind, query):
    table = tables(fmt, "a INT NOT NULL, u INT")
    table.insert("(1, 10)")
    sql = NULLABLE_QUERIES[query].format(t=table.name)
    view = table.create_view(kind, sql)
    table.recreate("a INT, u INT", "(NULL, 20), (2, 30)")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


@pytest.mark.parametrize("kind", KINDS)
def test_view_reports_the_nullability_of_its_columns(spark, tables, kind):
    table = tables("delta", "a INT NOT NULL, u INT")
    view = table.create_view(kind, f"SELECT a, u FROM {table.name}")
    expected = [("a", False), ("u", True)]
    assert [(x.name, x.nullable) for x in spark.table(table.name).schema] == expected
    assert [(x.name, x.nullable) for x in spark.table(view).schema] == expected
    name = view.removeprefix("global_temp.")
    database = "global_temp" if name != view else None
    columns = spark.catalog.listColumns(name, database)
    assert [(x.name, x.nullable) for x in columns] == expected


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
def test_view_reads_a_struct_whose_fields_had_no_nulls(spark, tables, kind, case):
    query, schema, values = STRUCT_CHANGES[case]
    table = tables("delta")
    write_struct_table(spark, table, struct_schema(INT_FIELDS), [(1, (1, 2)), (2, (3, 4))])
    sql = query.format(t=table.name)
    view = table.create_view(kind, sql)
    assert spark.table(view).schema == spark.sql(sql).schema
    write_struct_table(spark, table, schema, values)
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


@pytest.mark.parametrize("kind", ["sv", "dv"])
def test_view_over_a_view_reads_a_struct_whose_fields_had_no_nulls(spark, tables, kind):
    table = tables("delta")
    write_struct_table(spark, table, struct_schema(INT_FIELDS), [(1, (1, 2)), (2, (3, 4))])
    inner = table.create_view("sv", f"SELECT a, s FROM {table.name}")
    outer = table.create_view(kind, f"SELECT count(a) AS n, count(s.f) AS m FROM {inner}")
    write_struct_table(spark, table, struct_schema(INT_FIELDS, nullable=True), [(None, (None, 2)), (2, None)])
    assert rows(spark, f"SELECT * FROM {outer}") == [(1, 0)]


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize(
    ("before", "after"), [("iceberg", "parquet"), ("parquet", "iceberg"), ("iceberg", "delta"), ("delta", "iceberg")]
)
def test_view_reads_a_table_created_again_with_the_same_columns_in_another_format(spark, tables, kind, before, after):
    # The formats store strings, binary values and lists in Arrow types of other encodings.
    columns = "k INT, b BINARY, c STRING, s STRUCT<x: STRING, y: BINARY>, a ARRAY<STRING>, m MAP<STRING, BINARY>"
    table = tables(before, columns)
    table.insert("(1, X'0A', 'p', named_struct('x', 'q', 'y', X'0B'), array('r'), map('s', X'0C'))")
    view = table.create_view(kind)
    table.recreate(columns, "(2, X'FF', 'u', named_struct('x', 'v', 'y', X'EE'), array('w'), map('z', X'DD'))", after)
    assert spark.table(view).collect() == spark.table(table.name).collect()


# Queries over `t (k, s STRUCT<x, b>)` and `u (c0)` that read the field `x` of `s` only.
CORRELATED_FIELD_QUERIES = {
    "exists": "SELECT k FROM {t} WHERE EXISTS (SELECT 1 FROM {u} WHERE {u}.c0 = {t}.s.x)",
    "scalar subquery": "SELECT k, (SELECT count(*) FROM {u} WHERE {u}.c0 = {t}.s.x) AS n FROM {t}",
}


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("change", ["DROP COLUMN s.b", "ADD COLUMNS (s.g INT)"])
@pytest.mark.parametrize("query", CORRELATED_FIELD_QUERIES)
def test_view_reads_a_struct_whose_unread_field_changed_through_a_correlated_subquery(
    spark, tables, kind, change, query
):
    table = tables("iceberg", "k INT, s STRUCT<x: BIGINT, b: STRING>")
    other = tables("parquet", "c0 BIGINT")
    table.insert("(1, named_struct('x', 1, 'b', 'p')), (2, named_struct('x', 5, 'b', 'q'))")
    other.insert("(1), (2)")
    sql = CORRELATED_FIELD_QUERIES[query].format(t=table.name, u=other.name)
    view = table.create_view(kind, sql)
    spark.sql(f"ALTER TABLE {table.name} {change}")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


# Queries over `t (k, s STRUCT<a, b>)` that read the struct `s` whole and its field `a`.
STRUCT_AND_FIELD_QUERIES = [
    "SELECT s, s.a AS y FROM {t}",
    "SELECT s.b AS x, s, s.a AS y FROM {t}",
    "SELECT s FROM {t} WHERE s.a > 0",
    "SELECT s.a AS y FROM {t} WHERE s IS NOT NULL",
]


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("fmt", ["iceberg", "parquet"])
@pytest.mark.parametrize("query", STRUCT_AND_FIELD_QUERIES)
def test_view_reads_a_struct_whose_field_was_renamed_in_letter_case(spark, tables, kind, fmt, query):
    table = tables(fmt, "k INT, s STRUCT<a: INT, b: INT>")
    table.insert("(1, named_struct('a', 1, 'b', 2))")
    sql = query.format(t=table.name)
    view = table.create_view(kind, sql)
    if fmt == "iceberg":
        spark.sql(f"ALTER TABLE {table.name} RENAME COLUMN s.a TO A")
    else:
        table.recreate("k INT, s STRUCT<A: INT, b: INT>")
    table.insert("(2, named_struct('A', 3, 'b', 4))")
    assert sorted(spark.table(view).collect()) == sorted(spark.sql(sql).collect())
    # The view resolves its definition again, which reads the struct with the names that it has now.
    assert spark.table(view).schema.simpleString() == spark.sql(sql).schema.simpleString()


# Queries that read a field of a struct inside an array or a map, over a table `(k, c)` or a
# table `(k, s STRUCT<c>)`, the column of the table, and its type and value with the field `a`.
NESTED_STRUCT_QUERIES = {
    "array": (
        "SELECT c[0].a AS y FROM {t}",
        "c",
        "ARRAY<STRUCT<{a}: INT, b: INT>>",
        "array(named_struct('{a}', 1, 'b', 2))",
    ),
    "map": (
        "SELECT c['x'].a AS y FROM {t}",
        "c",
        "MAP<STRING, STRUCT<{a}: INT, b: INT>>",
        "map('x', named_struct('{a}', 1, 'b', 2))",
    ),
    "array in a struct": (
        "SELECT c[0].a AS y FROM (SELECT s.c AS c FROM {t})",
        "s",
        "STRUCT<c: ARRAY<STRUCT<{a}: INT, b: INT>>>",
        "named_struct('c', array(named_struct('{a}', 1, 'b', 2)))",
    ),
    "map in a struct": (
        "SELECT c['x'].a AS y FROM (SELECT s.c AS c FROM {t})",
        "s",
        "STRUCT<c: MAP<STRING, STRUCT<{a}: INT, b: INT>>>",
        "named_struct('c', map('x', named_struct('{a}', 1, 'b', 2)))",
    ),
}


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("case", NESTED_STRUCT_QUERIES)
def test_view_reads_a_struct_in_an_array_or_a_map_whose_field_was_renamed_in_letter_case(spark, tables, kind, case):
    query, column, data_type, value = NESTED_STRUCT_QUERIES[case]
    table = tables("parquet", f"k INT, {column} {data_type.format(a='a')}")
    table.insert(f"(1, {value.format(a='a')})")
    sql = query.format(t=table.name)
    view = table.create_view(kind, sql)
    schema = spark.table(view).schema
    table.recreate(f"k INT, {column} {data_type.format(a='A')}", f"(2, {value.format(a='A')})")
    # The view gives the field its stored name, whatever the plan of the view computes from it.
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql) == [(1,)]
    assert spark.table(view).schema.simpleString() == schema.simpleString()


# Queries over `t (c ARRAY<STRUCT<x INT, y INT>>)` that compute with the whole array.
ARRAY_OF_STRUCT_QUERIES = {
    "read": "SELECT c FROM {t}",
    "union": "SELECT c FROM {t} UNION ALL SELECT array(named_struct('x', 10L, 'y', 20L))",
    "equality": "SELECT c = array(named_struct('x', 7, 'y', 8)) AS eq FROM {t}",
    "cast": "SELECT CAST(c AS ARRAY<STRUCT<x: BIGINT, y: BIGINT>>) AS c FROM {t}",
}


def plain(value):
    """Returns the value with lists in the place of rows, whose field names are not compared."""
    return [plain(x) for x in value] if isinstance(value, (list, tuple)) else value


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("case", ARRAY_OF_STRUCT_QUERIES)
def test_view_computes_with_an_array_of_structs_whose_field_was_renamed_in_letter_case(spark, tables, kind, case):
    table = tables("delta", "c ARRAY<STRUCT<x: INT, y: INT>>")
    table.insert("(array(named_struct('x', 1, 'y', 2)))")
    sql = ARRAY_OF_STRUCT_QUERIES[case].format(t=table.name)
    view = table.create_view(kind, sql)
    table.recreate("c ARRAY<STRUCT<X: INT, y: INT>>", "(array(named_struct('X', 7, 'y', 8)))")
    # The view resolves its definition again and computes as the query does.
    expected = plain(spark.sql(sql).collect())
    assert sorted(plain(spark.table(view).collect()), key=str) == sorted(expected, key=str)


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("fmt", ["delta", "parquet"])
def test_view_reads_a_struct_whose_fields_changed_order_in_the_order_of_the_view(spark, tables, kind, fmt):
    table = tables(fmt, "s STRUCT<x: INT, y: STRING>")
    table.insert("(named_struct('x', 1, 'y', 'a'))")
    view = table.create_view(kind, f"SELECT s FROM {table.name}")
    table.recreate("s STRUCT<y: STRING, x: INT>", "(named_struct('y', 'b', 'x', 2))")
    # The view resolves its definition again, which reads the struct in the order that it has now.
    assert spark.table(view).schema.simpleString() == spark.table(table.name).schema.simpleString()
    assert rows(spark, f"SELECT s.x, s.y FROM {view}") == [(2, "b")]


# Queries over `t (a, b, s STRUCT<x, y>)` whose key `s.x` or `b` only tells rows apart, and the
# column whose type changes.
KEY_QUERIES = {
    "group by struct field": ("SELECT a, count(*) AS n FROM {t} GROUP BY a, s.x", "s.x"),
    "distinct struct field": ("SELECT a FROM (SELECT DISTINCT a, s.x FROM {t})", "s.x"),
    "group by column": ("SELECT a, count(*) AS n FROM {t} GROUP BY a, b", "b"),
}


@pytest.mark.parametrize("case", KEY_QUERIES)
def test_view_reads_a_key_whose_type_changed(spark, tables, case):
    query, changed = KEY_QUERIES[case]
    table = tables("iceberg", "a INT, b INT, s STRUCT<x: INT, y: INT>")
    table.insert("(1, 1, named_struct('x', 1, 'y', 1)), (1, 2, named_struct('x', 2, 'y', 1))")
    sql = query.format(t=table.name)
    view = table.create_view("sv", sql)
    spark.sql(f"ALTER TABLE {table.name} ALTER COLUMN {changed} TYPE BIGINT")
    table.insert("(5, 5, named_struct('x', 9, 'y', 1))")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


# Queries over `t (a INT, b INT)` that only compare `b`, sort by it, or count, sum or average it.
OPERAND_QUERIES = {
    "filter": "SELECT a FROM {t} WHERE b > 0",
    "between": "SELECT a FROM {t} WHERE b BETWEEN 0 AND 3000000000",
    "sort": "SELECT a FROM {t} ORDER BY b DESC LIMIT 1",
    "count distinct": "SELECT count(DISTINCT b) AS n FROM {t}",
    "sum": "SELECT sum(b) AS s FROM {t}",
    "avg": "SELECT avg(b) AS m FROM {t}",
    "join key": "SELECT x.a FROM {t} x JOIN {t} y ON x.b = y.b",
}


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize(
    ("fmt", "properties"), [("delta", " TBLPROPERTIES ('delta.enableTypeWidening' = 'true')"), ("iceberg", "")]
)
@pytest.mark.parametrize("case", OPERAND_QUERIES)
def test_view_reads_a_column_widened_in_place_that_it_only_uses_as_an_operand(
    spark, tables, kind, fmt, properties, case
):
    table = tables(fmt, "a INT, b INT", properties)
    table.insert("(1, 1), (2, -5), (3, 7)")
    query = OPERAND_QUERIES[case].format(t=table.name)
    view = table.create_view(kind, query)
    spark.sql(f"ALTER TABLE {table.name} ALTER COLUMN b TYPE BIGINT")
    table.insert("(5, 3000000000), (6, -3000000000)")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, query)


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("fmt", ["delta", "parquet"])
def test_view_joins_on_a_key_widened_on_one_side(spark, tables, kind, fmt):
    # The analyzer coerces the keys of the join to a common type again.
    left = tables(fmt, "a INT, b INT")
    left.insert("(1, 10), (2, 20)")
    right = tables(fmt, "b INT, c INT")
    right.insert("(10, 100), (30, 300)")
    query = f"SELECT l.a, r.c FROM {left.name} l JOIN {right.name} r ON l.b = r.b"
    view = left.create_view(kind, query)
    left.recreate("a INT, b BIGINT", "(5, 30), (6, 3000000000)")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, query) == [(5, 300)]


def test_view_sorts_by_a_column_retyped_to_a_map(spark, tables):
    # Spark fails to analyze the query again, and Sail sorts by a map as the query does.
    table = tables("delta", "a INT, b INT")
    table.insert("(1, 10)")
    sql = f"SELECT a FROM {table.name} ORDER BY b"
    view = table.create_view("sv", sql)
    table.recreate("a INT, b MAP<STRING, INT>", "(5, map('x', 1)), (6, map('y', 2))")
    assert spark.sql(f"SELECT * FROM {view}").collect() == spark.sql(sql).collect()


# Table formats whose columns carry different field metadata, such as Iceberg field IDs and
# Delta Lake column mapping, as the clause after the columns and the table properties.
METADATA_FORMATS = {
    "iceberg": ("iceberg", ""),
    "delta": ("delta", ""),
    "delta_cm": ("delta", " TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"),
    "parquet": ("parquet", ""),
}


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize(
    ("before", "after"),
    [("iceberg", "delta"), ("delta_cm", "iceberg"), ("delta_cm", "parquet"), ("delta", "delta_cm"), ("delta", None)],
)
def test_view_of_a_union_reads_a_table_whose_column_metadata_changed(spark, tables, kind, before, after):
    using, properties = METADATA_FORMATS[before]
    table = tables(using, "k INT, c STRING", properties)
    table.insert("(1, 'a'), (2, 'b')")
    sql = f"SELECT c FROM {table.name} UNION SELECT 'z'"
    view = table.create_view(kind, sql)
    if after is None:
        spark.sql(f"ALTER TABLE {table.name} ALTER COLUMN c COMMENT 'the column'")
    else:
        table.recreate("k INT, c STRING", "(1, 'a'), (2, 'b')", *METADATA_FORMATS[after])
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


# Queries over `t (a INT NOT NULL, u INT NOT NULL)` whose outputs have no nulls.
NOT_NULL_QUERIES = {
    "union": "SELECT a FROM {t} UNION SELECT u FROM {t}",
    "union all": "SELECT a FROM {t} UNION ALL SELECT u FROM {t}",
    "struct": "SELECT struct(a, u) AS s FROM {t}",
    "array": "SELECT array(a, u) AS r FROM {t}",
    "map": "SELECT map(a, u) AS m FROM {t}",
    "map from arrays": "SELECT map_from_arrays(array(a), array(u)) AS m FROM {t}",
    "map from entries": "SELECT map_from_entries(array(struct(a, u))) AS m FROM {t}",
    "case": "SELECT CASE WHEN a > 0 THEN a ELSE u END AS c FROM {t}",
    "coalesce": "SELECT coalesce(a, u) AS c FROM {t}",
}


@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize("query", NOT_NULL_QUERIES)
def test_view_reports_the_nullability_of_the_columns_that_it_computes(spark, tables, kind, query):
    table = tables("delta", "a INT NOT NULL, u INT NOT NULL")
    table.insert("(1, 2)")
    sql = NOT_NULL_QUERIES[query].format(t=table.name)
    view = table.create_view(kind, sql)
    assert spark.table(view).schema == spark.sql(sql).schema
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize(("before", "after"), [("a INT NOT NULL", "a INT"), ("a INT", "a INT NOT NULL")])
def test_view_reads_a_column_whose_nullability_changed(spark, tables, kind, before, after):
    table = tables("delta", before)
    table.insert("(1)")
    sql = f"SELECT a, array(a) AS r, struct(a) AS s FROM {table.name}"
    view = table.create_view(kind, sql)
    table.recreate(after, "(2)")
    assert spark.table(view).schema == spark.sql(sql).schema
    assert spark.table(view).collect() == spark.sql(sql).collect()


# Structs that a view reads whole and that gain a field: the columns and a row before, and
# after the change.
STRUCT_GAINS = {
    "struct": (
        "s STRUCT<a: INT, b: INT>",
        "named_struct('a', 10, 'b', 20)",
        "s STRUCT<b: INT, c: INT, a: INT>",
        "named_struct('b', 21, 'c', 31, 'a', 11)",
    ),
    "struct in a list": (
        "s ARRAY<STRUCT<a: INT, b: INT>>",
        "array(named_struct('a', 10, 'b', 20))",
        "s ARRAY<STRUCT<a: INT, b: INT, c: INT>>",
        "array(named_struct('a', 11, 'b', 21, 'c', 31))",
    ),
    "struct in a map": (
        "s MAP<STRING, STRUCT<a: INT, b: INT>>",
        "map('k', named_struct('a', 10, 'b', 20))",
        "s MAP<STRING, STRUCT<a: INT, b: INT, c: INT>>",
        "map('k', named_struct('a', 11, 'b', 21, 'c', 31))",
    ),
    "struct renamed in letter case": (
        "s STRUCT<a: INT, b: INT>",
        "named_struct('a', 10, 'b', 20)",
        "s STRUCT<A: INT, b: INT, c: INT>",
        "named_struct('A', 11, 'b', 21, 'c', 31)",
    ),
}


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize("case", STRUCT_GAINS)
def test_view_reads_a_struct_that_gained_fields(spark, tables, fmt, kind, case):
    columns, value, new_columns, new_value = STRUCT_GAINS[case]
    table = tables(fmt, f"id INT, {columns}")
    table.insert(f"(1, {value})")
    view = table.create_view(kind)
    table.recreate(f"id INT, {new_columns}", f"(2, {new_value})")
    # The view resolves its definition again, which reads the struct with the fields that it has now.
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, f"SELECT * FROM {table.name}")


@pytest.mark.parametrize("kind", ["sv", "dv"])
def test_view_of_a_map_whose_key_structs_gained_a_field(spark, tables, kind):
    table = tables("parquet", "id INT, m MAP<STRUCT<a: INT>, INT>")
    table.insert("(1, map(named_struct('a', 10), 1))")
    view = table.create_view(kind)
    table.recreate("id INT, m MAP<STRUCT<a: INT, c: INT>, INT>", "(2, map(named_struct('a', 11, 'c', 31), 2))")
    # The view resolves its definition again, which reads the map as it is now.
    assert spark.table(view).collect() == spark.table(table.name).collect()


# Queries over `t (k INT, c STRUCT<y: TIMESTAMP, z: INT> NOT NULL)` that count the struct.
STRUCT_COUNT_QUERIES = {
    "count": "SELECT count(c) AS n FROM {t}",
    "group": "SELECT k, count(c) AS n FROM {t} GROUP BY k",
    "filter": "SELECT count(c) FILTER (WHERE k >= 0) AS n FROM {t}",
}


@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize("query", STRUCT_COUNT_QUERIES)
def test_view_counts_a_parquet_struct_of_timestamps_without_nulls(spark, tables, kind, query):
    table = tables("parquet", "k INT, c STRUCT<y: TIMESTAMP, z: INT> NOT NULL")
    table.insert("(0, named_struct('y', TIMESTAMP '2020-01-01 00:00:00', 'z', 0))")
    table.insert("(1, named_struct('y', TIMESTAMP '2020-01-02 00:00:00', 'z', 1))")
    sql = STRUCT_COUNT_QUERIES[query].format(t=table.name)
    view = table.create_view(kind, sql)
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)
    structs = spark.sql(f"SELECT k, c FROM {table.name} ORDER BY k").collect()
    assert [x.c.z for x in structs] == [0, 1]


@pytest.mark.parametrize("kind", ["sv", "dv"])
@pytest.mark.parametrize("query", ["SELECT a, b FROM {t}", "SELECT a, typeof(b) AS tb FROM {t}"])
def test_view_over_a_view_reads_the_columns_that_the_inner_view_reads(spark, tables, kind, query):
    table = tables("delta", "a INT, b INT")
    table.insert("(1, 10)")
    inner = table.create_view("sv", query.format(t=table.name))
    outer = table.create_view(kind, f"SELECT a FROM {inner}")
    table.recreate("a INT, b STRING", "(5, 'x')")
    assert_view_reads(spark, inner, None)
    assert_view_reads(spark, outer, None)


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
    "intersect of the inner view": (
        "a BIGINT, b DOUBLE",
        "(1, 1.0), (2, 2.0)",
        ["SELECT b FROM {t}", "SELECT b FROM {v} INTERSECT SELECT b FROM {v}"],
        "b DOUBLE, a_r BIGINT",
        "(3.0, 1), (3.0, 2)",
        [(3.0,)],
    ),
    "intersect all of the inner view": (
        "a BIGINT, b DOUBLE",
        "(1, 1.0), (2, 2.0)",
        ["SELECT b FROM {t}", "SELECT b FROM {v} INTERSECT ALL SELECT b FROM {v}"],
        "b DOUBLE, a_r BIGINT",
        "(3.0, 1), (3.0, 2)",
        [(3.0,), (3.0,)],
    ),
    "except of the inner view": (
        "a BIGINT, b DOUBLE",
        "(1, 1.0), (2, 2.0)",
        ["SELECT b FROM {t}", "SELECT b FROM {v} EXCEPT SELECT b FROM {v} WHERE b < 0"],
        "b DOUBLE, a_r BIGINT",
        "(3.0, 1), (3.0, 2)",
        [(3.0,)],
    ),
    "except all of the inner view": (
        "a BIGINT, b DOUBLE",
        "(1, 1.0), (2, 2.0)",
        ["SELECT b FROM {t}", "SELECT b FROM {v} EXCEPT ALL SELECT b FROM {v} WHERE b < 0"],
        "b DOUBLE, a_r BIGINT",
        "(3.0, 1), (3.0, 2)",
        [(3.0,), (3.0,)],
    ),
    "no column read by the inner view": (
        "a INT, b INT",
        "(1, 10), (2, 20)",
        ["SELECT count(*) AS n FROM {t}", "SELECT n FROM {v}"],
        "a STRING, b STRING",
        "('x', 'y')",
        [(1,)],
    ),
    "constant read by the inner view": (
        "a INT, b INT",
        "(1, 10)",
        ["SELECT 1 AS one FROM {t}", "SELECT one FROM {v}"],
        "a STRING",
        "('x'), ('y')",
        [(1,), (1,)],
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
def test_view_over_views_reads_a_table_whose_unread_columns_changed(spark, tables, kind, case):
    columns, values, queries, new_columns, new_values, expected = VIEW_OVER_VIEW_CHANGES[case]
    table = tables("delta", columns)
    table.insert(values)
    view = None
    for i, query in enumerate(queries):
        view = table.create_view(kind if i == len(queries) - 1 else "sv", query.format(t=table.name, v=view))
    table.recreate(new_columns, new_values)
    assert rows(spark, f"SELECT * FROM {view}") == expected


# Queries over `t (id INT, k INT, extra INT)` that read `id` and `k` through an operator that
# passes the columns of its input on, as SQL or as a function of the DataFrame of the table.
PASSING_QUERIES = {
    "repartition": lambda frame: frame.repartition(3).select("id", "k"),
    "repartition by a column": lambda frame: frame.repartition(3, "k").select("id", "k"),
    "coalesce": lambda frame: frame.coalesce(1).select("id", "k"),
    "sort within partitions": lambda frame: frame.sortWithinPartitions("k").select("id", "k"),
    "repartition by range": lambda frame: frame.repartitionByRange(3, "k").select("id", "k"),
    "sample": lambda frame: frame.sample(fraction=1.0, seed=3).select("id", "k"),
    "explode": lambda frame: frame.select("id", explode(array("id", "k")).alias("e")),
    "unpivot": lambda frame: frame.unpivot("id", ["k"], "n", "v"),
    "table sample": "SELECT id, k FROM {t} TABLESAMPLE (100 PERCENT)",
    "lateral view": "SELECT id, e FROM {t} LATERAL VIEW explode(array(id, k)) x AS e",
    "explode in the select list": "SELECT id, explode(array(id, k)) AS e FROM {t}",
    "posexplode": "SELECT id, posexplode(array(k)) FROM {t}",
    "inline": "SELECT inline(array(named_struct('a', k))) FROM {t}",
    "unpivot in SQL": "SELECT id, n, v FROM {t} UNPIVOT (v FOR n IN (k))",
    "window over a sorted query": (
        "SELECT q.id, dense_rank() OVER (PARTITION BY q.k ORDER BY q.id) AS r FROM (SELECT * FROM {t} ORDER BY k) q"
    ),
    "window over a sorted query with a limit": (
        "SELECT q.id, count(*) OVER (PARTITION BY q.k) AS n FROM (SELECT * FROM {t} ORDER BY k LIMIT 100) q"
    ),
    "partition ID": "SELECT id, k, spark_partition_id() * 0 AS p FROM {t}",
    "monotonically increasing ID": "SELECT id, k, monotonically_increasing_id() * 0 AS m FROM {t}",
}


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("change", ["retyped", "dropped"])
@pytest.mark.parametrize("query", PASSING_QUERIES)
def test_view_over_an_operator_that_passes_columns_on_reads_a_table_whose_unread_column_changed(
    spark, tables, fmt, change, query
):
    table = tables(fmt, "id INT, k INT, extra INT")
    table.insert("(1, 10, 100), (2, 20, 200)")
    definition = PASSING_QUERIES[query]
    if isinstance(definition, str):
        sql = definition.format(t=table.name)
        view = table.create_view("sv", sql)

        def direct():
            return spark.sql(sql)
    else:
        view = table.create_view("dv", frame=definition(spark.table(table.name)))

        def direct():
            return definition(spark.table(table.name))

    if change == "retyped":
        table.recreate("id INT, k INT, extra STRING", "(1, 10, 'a'), (4, 40, 'b')")
    else:
        table.recreate("id INT, k INT", "(1, 10), (4, 40)")
    assert sorted(spark.table(view).collect()) == sorted(direct().collect())


# Queries over `t (id INT, s ARRAY<STRUCT<a: INT, b: INT>>, extra INT)` that read the field `a`
# of the structs in `s`.
UNNESTED_STRUCT_QUERIES = {
    "explode": "SELECT id, x.a AS a FROM {t} LATERAL VIEW explode(s) e AS x",
    "inline": "SELECT a FROM (SELECT inline(s) FROM {t})",
}


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("query", UNNESTED_STRUCT_QUERIES)
def test_view_that_unnests_structs_reads_a_table_whose_columns_changed(spark, tables, fmt, query):
    table = tables(fmt, "id INT, s ARRAY<STRUCT<a: INT, b: INT>>, extra INT")
    table.insert("(1, array(named_struct('a', 1, 'b', 2)), 100)")
    sql = UNNESTED_STRUCT_QUERIES[query].format(t=table.name)
    view = table.create_view("sv", sql)
    table.recreate(
        "id INT, s ARRAY<STRUCT<a: INT, b: INT>>, extra STRING", "(2, array(named_struct('a', 3, 'b', 4)), 'x')"
    )
    assert sorted(spark.table(view).collect()) == sorted(spark.sql(sql).collect())
    # The field that the view reads changes its type.
    table.recreate("id INT, s ARRAY<STRUCT<a: BIGINT, b: INT>>", "(3, array(named_struct('a', 5, 'b', 6)))")
    assert_view_reads(spark, view, None)


# DataFrames whose definitions run a query to find their columns, over a table `(id, s)` with the
# rows (1, 'a'), (2, 'b'): the DataFrame, and its rows before and after the row (1, 'b') is added.
QUERYING_FRAMES = {
    "pivot without values": (
        lambda frame: frame.groupBy("s").pivot("id").count(),
        [("a", 1, None), ("b", None, 1)],
        [("a", 1, None), ("b", 1, 1)],
    ),
    "crosstab": (
        lambda frame: frame.crosstab("s", "id"),
        [("a", 1, 0), ("b", 0, 1)],
        [("a", 1, 0), ("b", 1, 1)],
    ),
}


@pytest.mark.parametrize("kind", ["dv", "dgv"])
@pytest.mark.parametrize("case", QUERYING_FRAMES)
def test_data_frame_view_whose_definition_runs_a_query_reads_the_current_rows(spark, tables, kind, case):
    query, before, after = QUERYING_FRAMES[case]
    table = tables("delta", "id BIGINT, s STRING")
    table.insert("(1, 'a'), (2, 'b')")
    frame = query(spark.table(table.name))
    view = table.create_view(kind, frame=frame)
    # The columns of the values are in no particular order.
    sql = f"SELECT `{frame.columns[0]}`, `1`, `2` FROM {view}"
    assert rows(spark, sql) == before
    table.insert("(1, 'b')")
    assert rows(spark, sql) == after


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_table_whose_unread_column_was_retyped_by_a_write(spark, tables, kind):
    table = tables("delta", "id INT, extra INT")
    table.insert("(1, 2)")
    view = table.create_view(kind, f"SELECT id FROM {table.name}")
    write = spark.sql("SELECT 5 AS id, 'x' AS extra").write.mode("overwrite")
    write.option("overwriteSchema", "true").saveAsTable(table.name)
    assert rows(spark, f"SELECT * FROM {view}") == [(5,)]


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("kind", ["sv", "gv", "dv"])
@pytest.mark.parametrize(
    "query",
    [
        "SELECT a FROM {t} WHERE EXISTS (SELECT 1 FROM {u} WHERE {u}.k = {t}.b)",
        "SELECT a, (SELECT max(k) FROM {u} WHERE {u}.k = {t}.b) AS m FROM {t}",
    ],
)
def test_view_rejects_a_table_without_a_column_of_an_outer_reference(spark, tables, fmt, kind, query):
    table = tables(fmt, "a INT, b INT, x INT")
    other = tables(fmt, "k INT")
    table.insert("(1, 10, 100)")
    other.insert("(10)")
    view = table.create_view(kind, query.format(t=table.name, u=other.name))
    table.recreate("a INT, x INT", "(1, 100)")
    assert_view_reads(spark, view, None)


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_table_whose_column_comment_changed(spark, tables, kind):
    table = tables("delta")
    table.insert("(1)")
    view = table.create_view(kind)
    spark.sql(f"ALTER TABLE {table.name} ALTER COLUMN id COMMENT 'the id'")
    table.insert("(2)")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
    assert rows(spark, f"SELECT id, count(*) FROM {view} GROUP BY id") == [(1, 1), (2, 1)]
    joined = f"SELECT v.id, count(*) FROM {view} v JOIN {table.name} t ON v.id = t.id GROUP BY v.id"
    assert rows(spark, joined) == [(1, 1), (2, 1)]


@pytest.mark.parametrize("table", LAKE_FORMATS, indirect=True)
@pytest.mark.parametrize("kind", ["sv", "svc", "gv", "dv", "dgv"])
def test_view_of_a_table_whose_name_is_now_the_name_of_a_view(spark, table, kind):
    table.insert("(1)")
    view = table.create_view(kind)
    spark.sql(f"DROP TABLE {table.name}")
    spark.sql(f"CREATE VIEW {table.name} AS SELECT 5L AS id")
    try:
        # The view resolves its definition again, which reads the view.
        assert rows(spark, f"SELECT * FROM {view}") == [(5,)]
    finally:
        spark.sql(f"DROP VIEW {table.name}")


@pytest.mark.parametrize("kind", KINDS)
def test_view_with_time_travel_reads_its_version(spark, tables, kind):
    table = tables("delta")
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
    assert spark.table(view).columns == ["id"]


def test_data_frame_view_that_travels_in_time_by_a_read_option_reads_its_version(spark, tables):
    table = tables("delta")
    table.insert("(1)")
    view = table.create_view("dv", frame=spark.read.option("versionAsOf", 1).table(table.name))
    table.insert("(2)")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    # The version of a Delta Lake table is not a version of a Parquet table.
    table.recreate("id BIGINT", "(7), (8)", fmt="parquet")
    assert "created again with another format" in error(spark, f"SELECT * FROM {view}")
    assert spark.table(view).columns == ["id"]


@pytest.mark.parametrize(
    ("fmt", "key"),
    [
        ("iceberg", "snapshot-id"),
        ("iceberg", "snapshot_id"),
        ("iceberg", "version_as_of"),
        ("iceberg", "versionAsOf"),
        ("delta", "version_as_of"),
        ("delta", "timestamp_as_of"),
    ],
)
def test_data_frame_view_of_a_version_by_any_selector_fails_after_another_format(spark, tables, fmt, key):
    table = tables(fmt)
    table.insert("(1)")
    if fmt == "iceberg":
        version = spark.sql(f"SELECT snapshot_id FROM {table.name}.snapshots").collect()[0][0]
    elif key == "version_as_of":
        version = 1
    else:
        # A time between the commits of the two inserts.
        time.sleep(1)
        version = spark.sql("SELECT CAST(current_timestamp() AS STRING)").collect()[0][0]
        time.sleep(1)
    table.insert("(2)")
    view = table.create_view("dv", frame=spark.read.option(key, version).table(table.name))
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    table.recreate("id BIGINT", "(5)", fmt="parquet")
    assert "created again with another format" in error(spark, f"SELECT * FROM {view}")


def test_data_frame_view_of_a_branch_follows_the_table(spark, tables):
    table = tables("iceberg")
    table.insert("(1)")
    view = table.create_view("dv", frame=spark.read.option("branch", "main").table(table.name))
    table.recreate("id BIGINT", "(5)", fmt="parquet")
    assert rows(spark, f"SELECT * FROM {view}") == [(5,)]


def test_data_frame_view_of_a_read_in_a_required_format_fails_after_another_format(spark, tables):
    table = tables("iceberg")
    table.insert("(1)")
    view = table.create_view("dv", frame=spark.read.format("iceberg").load(table.name))
    current = table.create_view("dv", frame=spark.table(table.name))
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    table.recreate("id BIGINT", "(5)", fmt="parquet")
    # The view fails as its data frame does, and a view of a read in any format follows the table.
    assert "is not an iceberg table" in error(spark, f"SELECT * FROM {view}")
    assert rows(spark, f"SELECT * FROM {current}") == [(5,)]


def test_data_frame_view_of_a_version_of_a_removed_path_reports_its_columns(spark, tables):
    table = tables("delta")
    table.insert("(1)")
    location = table.tmp_path / f"{table.name}_{table.locations}"
    view = table.create_view("dv", frame=spark.read.format("delta").option("versionAsOf", 1).load(str(location)))
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    shutil.rmtree(location)
    error(spark, f"SELECT * FROM {view}")
    assert spark.table(view).columns == ["id"]


@pytest.mark.parametrize("kind", KINDS)
def test_view_over_an_iceberg_metadata_table_reads_the_current_snapshots(spark, tables, kind):
    table = tables("iceberg")
    table.insert("(1)")
    view = table.create_view(kind, f"SELECT snapshot_id FROM {table.name}.snapshots")
    assert len(rows(spark, f"SELECT * FROM {view}")) == 1
    table.insert("(2)")
    assert len(rows(spark, f"SELECT * FROM {view}")) == 2


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
@pytest.mark.parametrize("kind", PLAN_KINDS)
def test_plan_view_over_a_persistent_view_reads_the_current_rows(spark, table, kind):
    persistent = f"pv_{uuid.uuid4().hex}"
    try:
        table.insert("(1)")
        spark.sql(f"CREATE VIEW {persistent} AS SELECT id + 1000 AS id FROM {table.name}")
        view = table.create_view(kind, f"SELECT * FROM {persistent}")
        table.insert("(2)")
        assert rows(spark, f"SELECT * FROM {view}") == [(1001,), (1002,)]
        # The view reads the current definition of the persistent view.
        spark.sql(f"CREATE OR REPLACE VIEW {persistent} AS SELECT 55L AS id")
        assert rows(spark, f"SELECT * FROM {view}") == [(55,)]
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
        view = create_view(spark, kind, "SELECT * FROM c")
        views.append(view)
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
        for view in views:
            drop_view(spark, view)
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
    table.recreate(
        "id BIGINT, Ts TIMESTAMP, n TIMESTAMP_NTZ, s STRUCT<A: INT, b: ARRAY<STRING>>, d DECIMAL(10, 2)",
        "(1, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:00:00', named_struct('A', 1, 'b', array('x')), 1.5)",
    )
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


@pytest.mark.parametrize("table", ["delta", "parquet"], indirect=True)
def test_global_view_reads_the_table_of_the_session_that_created_it(spark, observer, table, tmp_path):
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
        # The creator replaces its view and its function, and then drops its view.
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {local} AS SELECT 9L AS id")
        spark.udf.register(function, udf(lambda x: f"B{x}"))
        assert rows(observer, f"SELECT * FROM {nested}") == [(1, "G1"), (2, "G2")]
        spark.catalog.dropTempView(local)
        table.insert("(3)")
        assert rows(observer, f"SELECT * FROM {nested}") == [(1, "G1"), (2, "G2"), (3, "G3")]
        # A table of the same name in the other session is another table.
        observer.sql(f"CREATE TABLE {table.name} (id BIGINT) USING delta LOCATION '{tmp_path / 'observer'}'")
        observer.sql(f"INSERT INTO {table.name} VALUES (9)")
        assert rows(observer, f"SELECT * FROM {direct}") == [(1,), (2,), (3,)]
        assert rows(observer, f"SELECT a.id, b.id FROM {direct} a CROSS JOIN {table.name} b") == [
            (1, 9),
            (2, 9),
            (3, 9),
        ]
        spark.sql(f"DROP TABLE {table.name}")
        assert NOT_FOUND in error(observer, f"SELECT * FROM {direct}")
    finally:
        observer.sql(f"DROP TABLE IF EXISTS {table.name}")
        spark.sql(f"DROP VIEW IF EXISTS {local}")


def test_global_view_resolves_its_query_again_in_the_catalog_of_the_session_that_created_it(
    spark, observer, tables, tmp_path
):
    table = tables("delta", "id BIGINT, b INT, c STRUCT<x: INT>")
    table.insert("(1, 1, named_struct('x', 1))")
    queries = [
        f"SELECT id, CAST(b AS STRING) AS b, c FROM {table.name}",
        f"SELECT typeof((SELECT b FROM {table.name} LIMIT 1)) AS ty",
        f"SELECT to_json(struct(*)) AS j FROM {table.name}",
    ]
    views = [table.create_view("gv", query) for query in queries]
    try:
        # A table of the same name in the other session is another table.
        observer.sql(f"CREATE TABLE {table.name} (id BIGINT) USING delta LOCATION '{tmp_path / 'observer'}'")
        observer.sql(f"INSERT INTO {table.name} VALUES (9)")
        both = f"SELECT id FROM {table.name} UNION ALL SELECT id FROM {views[0]}"
        assert rows(observer, both) == [(1,), (9,)]
        table.recreate("id BIGINT, b BIGINT, c STRUCT<x: INT, y: INT>", "(2, 3000000000, named_struct('x', 1, 'y', 2))")
        # The other session resolves the queries in the catalog of the session that created the views.
        for view, query in zip(views, queries, strict=True):
            assert rows(observer, f"SELECT * FROM {view}") == rows(spark, query)
        assert rows(observer, both) == [(2,), (9,)]
        spark.sql(f"DROP TABLE {table.name}")
        for view in views:
            assert NOT_FOUND in error(observer, f"SELECT * FROM {view}")
    finally:
        observer.sql(f"DROP TABLE IF EXISTS {table.name}")


def test_global_view_resolves_its_query_again_after_the_session_that_created_it_stopped(remote, observer, tmp_path):
    name = f"t_{uuid.uuid4().hex}"
    view = f"gv_{uuid.uuid4().hex}"
    with spark_session_factory(remote) as sessions:
        creator = sessions.create()
        creator.sql(f"CREATE TABLE {name} (id BIGINT, b INT) USING delta LOCATION '{tmp_path / name}_1'")
        creator.sql(f"INSERT INTO {name} VALUES (1, 1)")
        creator.sql(f"CREATE GLOBAL TEMPORARY VIEW {view} AS SELECT id, CAST(b AS STRING) AS b FROM {name}")
        creator.sql(f"DROP TABLE {name}")
        creator.sql(f"CREATE TABLE {name} (id BIGINT, b BIGINT) USING delta LOCATION '{tmp_path / name}_2'")
        creator.sql(f"INSERT INTO {name} VALUES (2, 3000000000)")
    try:
        assert rows(observer, f"SELECT * FROM global_temp.{view}") == [(2, "3000000000")]
    finally:
        observer.catalog.dropGlobalTempView(view)


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
    views = []
    try:
        spark.sql(f"CREATE TABLE {name} USING {name}")
        view = create_view(spark, kind, f"SELECT * FROM {name}")
        views.append(view)
        assert rows(spark, f"SELECT * FROM {view}") == [(7,)]
        assert rows(spark, f"SELECT a.id, b.id FROM {view} a JOIN {name} b ON a.id = b.id") == [(7, 7)]
        spark.sql(f"DROP TABLE {name}")
        assert NOT_FOUND in error(spark, f"SELECT * FROM {view}")
    finally:
        for view in views:
            drop_view(spark, view)
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.spark(version=">=4", reason="Uses the Python DataSource API")
@pytest.mark.parametrize("kind", ["gv", "dgv"])
def test_global_view_over_a_python_data_source_table_reads_by_the_source_of_its_session(spark, observer, kind):
    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import IntegerType, StructField, StructType

    name = f"py_{uuid.uuid4().hex}"

    def source(value):
        class Reader(DataSourceReader):
            def read(self, partition):  # noqa: ARG002
                yield (value,)

        class Source(DataSource):
            @classmethod
            def name(cls):
                return name

            def schema(self):
                return StructType([StructField("id", IntegerType())])

            def reader(self, schema):  # noqa: ARG002
                return Reader()

        return Source

    spark.dataSource.register(source(7))
    views = []
    try:
        spark.sql(f"CREATE TABLE {name} USING {name}")
        view = create_view(spark, kind, f"SELECT * FROM {name}")
        views.append(view)
        # The view reads by the source of the session that created it, which another session
        # does not have, or has under another implementation.
        assert rows(observer, f"SELECT * FROM {view}") == [(7,)]
        observer.dataSource.register(source(99))
        assert rows(observer, f"SELECT * FROM {view}") == [(7,)]
        assert rows(spark, f"SELECT * FROM {view}") == [(7,)]
    finally:
        for view in views:
            drop_view(spark, view)
        spark.sql(f"DROP TABLE IF EXISTS {name}")


PATH_KINDS = [*KINDS, "using"]
# A view created with `USING iceberg` and a path cannot be created.
PATH_VIEWS = [(fmt, kind) for fmt in LAKE_FORMATS for kind in PATH_KINDS if (fmt, kind) != ("iceberg", "using")]


def create_path_view(spark, kind, fmt, path, version=None):
    """Creates a temporary view of the kind over the Delta Lake or Iceberg table at the path, at the version
    if given. The DataFrame views read the path with the DataFrame reader."""
    if kind == "using":
        view = f"using_{uuid.uuid4().hex}"
        options = f"path '{path}'" + (f", versionAsOf '{version}'" if version is not None else "")
        spark.sql(f"CREATE TEMPORARY VIEW {view} USING {fmt} OPTIONS ({options})")
        return view
    query = f"SELECT * FROM {fmt}.`{path}`" + (f" VERSION AS OF {version}" if version is not None else "")
    reader = spark.read.format(fmt)
    if version is not None:
        reader = reader.option("versionAsOf", str(version))
    return create_view(spark, kind, query, reader.load(str(path)))


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
        drop_view(spark, view)


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
        drop_view(spark, view)


@pytest.mark.parametrize("kind", PATH_KINDS)
def test_view_over_a_lake_table_path_with_time_travel_reads_its_version(spark, tmp_path, kind):
    path = tmp_path / "t"
    spark.sql("SELECT 1L AS id").write.format("delta").save(str(path))
    view = create_path_view(spark, kind, "delta", path, version=0)
    try:
        spark.sql("SELECT 2L AS id").write.format("delta").mode("append").save(str(path))
        for _ in range(2):
            assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
        joined = f"SELECT v.id, t.id FROM {view} v FULL OUTER JOIN delta.`{path}` t ON v.id = t.id"
        assert rows(spark, joined) == [(1, 1), (None, 2)]
    finally:
        drop_view(spark, view)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize(
    ("query", "columns", "values", "expected"),
    [
        ("SELECT u FROM {t} WHERE a > 0", "a DOUBLE, u INT", "(0.5, 1), (-0.5, 2)", [(1,)]),
        ("SELECT u FROM {t} WHERE a = '2'", "a STRING, u INT", "('2', 1), ('02', 2)", [(1,)]),
        ("SELECT count(DISTINCT a) AS n FROM {t}", "a DOUBLE, u INT", "(0.0, 1), (-0.0, 2)", [(1,)]),
        (
            "SELECT u, CASE WHEN u > 0 THEN a ELSE 0 END AS c FROM {t}",
            "a BIGINT, u INT",
            "(3000000000, 1)",
            [(1, 3000000000)],
        ),
        ("SELECT typeof(a) AS t FROM {t}", "a STRING, u INT", "('q', 1)", [("string",)]),
        # The outputs have the types of the query now, and the outputs that the view did not have are not read.
        ("SELECT a FROM {t}", "a BIGINT, u INT", "(3000000000, 1)", [(3000000000,)]),
        ("SELECT * FROM {t}", "a STRING, u INT, x INT", "('q', 1, 7)", [("q", 1)]),
    ],
)
def test_view_resolves_its_definition_again_for_a_column_that_changed(
    spark, tables, kind, fmt, query, columns, values, expected
):
    table = tables(fmt, "a INT, u INT")
    table.insert("(1, 1), (2, 2)")
    view = table.create_view(kind, query.format(t=table.name))
    table.recreate(columns, values)
    assert rows(spark, f"SELECT * FROM {view}") == expected


def test_sql_view_resolves_its_query_again_with_its_context(spark, tables, tmp_path):
    table = tables("delta", "a INT, u INT, ts TIMESTAMP")
    other = f"db_{uuid.uuid4().hex}"
    previous = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        values = "(1, 10, TIMESTAMP '2020-01-01 01:00:00'), (1, 20, TIMESTAMP '2020-01-01 03:00:00')"
        table.insert(values)
        query = f"SELECT u FROM {table.name} WHERE a > 0 AND ts > '2020-01-01 02:00:00'"
        view = table.create_view("sv", query)
        expected = rows(spark, query)
        assert expected == [(20,)]
        table.recreate("a DOUBLE, u INT, ts TIMESTAMP", values)
        # The time zone, the database, the CTEs and the temporary views of the reader.
        spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
        spark.sql(f"CREATE DATABASE {other}")
        spark.catalog.setCurrentDatabase(other)
        spark.sql(
            f"CREATE TABLE {table.name} (a BIGINT, u INT, ts TIMESTAMP) USING delta LOCATION '{tmp_path / other}'"
        )
        spark.sql(f"INSERT INTO {table.name} VALUES (1, 99, TIMESTAMP '2030-01-01 00:00:00')")
        assert rows(spark, f"SELECT * FROM {view}") == expected
        spark.catalog.setCurrentDatabase("default")
        cte = f"WITH {table.name} AS (SELECT 1D AS a, 99 AS u, TIMESTAMP '2030-01-01 00:00:00' AS ts)"
        assert rows(spark, f"{cte} SELECT * FROM {view}") == expected
        spark.sql(f"CREATE TEMPORARY VIEW {table.name} AS SELECT 1D AS a, 99 AS u, TIMESTAMP '2030-01-01' AS ts")
        assert rows(spark, f"SELECT * FROM {view}") == expected
    finally:
        spark.catalog.setCurrentDatabase("default")
        spark.catalog.dropTempView(table.name)
        spark.conf.set("spark.sql.session.timeZone", previous)
        spark.sql(f"DROP DATABASE IF EXISTS {other} CASCADE")


def test_sql_view_resolves_its_query_again_with_the_temporary_views_that_it_read(spark, tables):
    table = tables("delta", "a INT, u INT")
    table.insert("(1, 1), (-1, 2)")
    inner = table.create_view("sv", f"SELECT * FROM {table.name}")
    outer = table.create_view("sv", f"SELECT u FROM {inner} WHERE a > 0")
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {inner} AS SELECT 5 AS a, 50 AS u")
    # A view that reads itself reads the view that it replaced.
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {outer} AS SELECT u FROM {outer} WHERE u > 0")
    table.recreate("a DOUBLE, u INT", "(0.5, 3), (-1.0, 4)")
    assert rows(spark, f"SELECT * FROM {outer}") == [(3,)]


def test_view_created_over_a_sql_view_that_resolves_its_query_again(spark, tables):
    table = tables("delta", "a INT, u INT")
    table.insert("(1, 1)")
    inner = table.create_view("sv", f"SELECT u FROM {table.name} WHERE a > 0")
    table.recreate("a DOUBLE, u INT", "(0.5, 2), (-0.5, 3)")
    outer = table.create_view("sv", f"SELECT u FROM {inner}")
    assert rows(spark, f"SELECT * FROM {outer}") == [(2,)]
    table.recreate("a STRING, u INT", "('1', 4), ('-1', 5)")
    assert rows(spark, f"SELECT * FROM {outer}") == [(4,)]


def test_sql_view_fails_when_its_query_no_longer_has_its_columns(spark, tables):
    table = tables("delta", "a INT, u INT")
    view = table.create_view("sv", f"SELECT * FROM {table.name}")
    table.recreate("a BIGINT", "(1)")
    assert COLUMNS_CHANGED in error(spark, f"SELECT * FROM {view}")


@pytest.mark.parametrize("kind", ["gv", "dgv"])
def test_global_view_over_a_table_of_its_session_catalog(spark, observer, tables, kind):
    table = tables("delta")
    table.insert("(1)")
    view = table.create_view(kind, f"SELECT 1 AS n FROM {table.name} WHERE id > 0")
    table.recreate("id DOUBLE", "(2.0)")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    # The other session resolves the query in the catalog of this session.
    assert rows(observer, f"SELECT * FROM {view}") == [(1,)]


@pytest.mark.parametrize("query", ["SELECT c FROM {t}", "SELECT c.x AS x FROM {t}"])
def test_sql_view_resolves_its_query_again_for_a_struct_field_renamed_in_letter_case(spark, tables, query):
    table = tables("delta", "id INT, c STRUCT<x: INT>")
    table.insert("(1, named_struct('x', 1))")
    previous = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", "true")
        view = table.create_view("sv", query.format(t=table.name))
        table.recreate("id INT, c STRUCT<X: INT>", "(2, named_struct('X', 2))")
        if query.startswith("SELECT c "):
            # The struct has the field names of the query now.
            result = spark.sql(f"SELECT * FROM {view}").collect()
            assert [row.c.asDict() for row in result] == [{"X": 2}]
        else:
            assert "x" in error(spark, f"SELECT * FROM {view}").lower()
    finally:
        spark.conf.set("spark.sql.caseSensitive", previous)


# Queries over `t (id, arr ARRAY<STRUCT<x, y>>, m MAP<STRING, STRUCT<x, y>>)` that read the field `x`
# through a container, which reads the whole column, so that a change of `y` makes a view resolve
# its query again.
CONTAINER_FIELD_QUERIES = [
    "SELECT arr[0].x AS v FROM {t}",
    "SELECT m['k'].x AS v FROM {t}",
    "SELECT element_at(arr, 1).x AS v FROM {t}",
    "SELECT transform(arr, e -> e.x) AS v FROM {t}",
]
CONTAINER_COLUMNS = "id INT, arr ARRAY<STRUCT<x: INT, {f}>>, m MAP<STRING, STRUCT<x: INT, {f}>>"


def container_row(i, f):
    return f"({i}, array(named_struct('x', {i}, {f})), map('k', named_struct('x', {i}, {f})))"


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("query", CONTAINER_FIELD_QUERIES)
@pytest.mark.parametrize(
    ("field", "value"), [("y: STRING", "'y', 'q'"), ("y: INT, z: INT", "'y', 2, 'z', 3"), (None, None)]
)
def test_sql_view_reads_a_field_through_a_container_whose_other_field_changed(spark, tables, fmt, query, field, value):
    table = tables(fmt, CONTAINER_COLUMNS.format(f="y: INT"))
    table.insert(container_row(1, "'y', 1"))
    sql = query.format(t=table.name)
    view = table.create_view("sv", sql)
    if field is None:
        # The field `y` is dropped.
        table.recreate("id INT, arr ARRAY<STRUCT<x: INT>>, m MAP<STRING, STRUCT<x: INT>>")
        table.insert("(2, array(named_struct('x', 2)), map('k', named_struct('x', 2)))")
    else:
        table.recreate(CONTAINER_COLUMNS.format(f=field), container_row(2, value))
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


@pytest.mark.parametrize("query", CONTAINER_FIELD_QUERIES)
@pytest.mark.parametrize(
    "changes",
    [
        ["ALTER COLUMN arr.element.y TYPE BIGINT", "ALTER COLUMN m.value.y TYPE BIGINT"],
        ["ADD COLUMN arr.element.z INT", "ADD COLUMN m.value.z INT"],
        ["DROP COLUMN arr.element.y", "DROP COLUMN m.value.y"],
    ],
)
def test_sql_view_reads_a_field_through_a_container_whose_other_field_changed_in_place(spark, tables, query, changes):
    table = tables("iceberg", CONTAINER_COLUMNS.format(f="y: INT"))
    table.insert(container_row(1, "'y', 1"))
    sql = query.format(t=table.name)
    view = table.create_view("sv", sql)
    for change in changes:
        spark.sql(f"ALTER TABLE {table.name} {change}")
    table.insert("(2, NULL, NULL)")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)


# Queries over `t (a INT, s STRUCT<x: INT>)` that expand a star inside the arguments of a function.
STAR_IN_FUNCTION_QUERIES = [
    "SELECT to_json(struct(*)) AS j FROM {t}",
    "SELECT to_json(struct(s.*)) AS j FROM {t}",
    "SELECT count(DISTINCT *) AS c FROM {t}",
]


@pytest.mark.parametrize("fmt", ["delta", "parquet"])
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("query", STAR_IN_FUNCTION_QUERIES)
def test_view_expands_a_star_inside_a_function_again_when_the_table_gained_columns(spark, tables, fmt, kind, query):
    table = tables(fmt, "a INT, s STRUCT<x: INT>")
    table.insert("(1, named_struct('x', 1)), (1, named_struct('x', 1))")
    sql = query.format(t=table.name)
    view = table.create_view(kind, sql)
    table.recreate(
        "a INT, s STRUCT<x: INT, y: INT>, b INT",
        "(1, named_struct('x', 1, 'y', 2), 3), (1, named_struct('x', 1, 'y', 4), 3)",
    )
    outer = table.create_view("sv", f"SELECT * FROM {view}")
    table.recreate(
        "a INT, s STRUCT<x: INT, y: INT, z: INT>, b INT, c INT",
        "(1, named_struct('x', 1, 'y', 2, 'z', 5), 3, 6), (1, named_struct('x', 1, 'y', 2, 'z', 7), 3, 6)",
    )
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, sql)
    assert rows(spark, f"SELECT * FROM {outer}") == rows(spark, sql)


def test_sql_view_keeps_its_plan_when_its_query_no_longer_has_its_columns_after_a_star_expanded(spark, tables):
    table = tables("delta", "a INT")
    table.insert("(1)")
    view = table.create_view("sv", f"SELECT *, to_json(struct(*)) AS j FROM {table.name}")
    # The query now has two columns named `j`, so the view reads its stored plan.
    table.recreate("a INT, j STRING", "(1, 'x')")
    assert rows(spark, f"SELECT * FROM {view}") == [(1, '{"a":1}')]


def columns_of_dataframes(spark, table):
    t = spark.table(table.name)
    return t.join(t.select(t["id"].alias("k")), t["id"] == col("k")).select(t["id"], t["b"])


def self_join(spark, table):
    t = spark.table(table.name)
    return t.alias("x").join(t.alias("y"), col("x.id") == col("y.id")).select(col("x.id"), col("y.b"))


def to_strings(values):
    """Returns the values of a pandas Series, or the rows of a pandas DataFrame for a struct, as strings."""
    return values.astype(str) if values.ndim == 1 else values.apply(lambda row: str(tuple(row)), axis=1)


# DataFrames over a table `t (id INT, b ...)` that read the column `b` alike for each type of `b` in
# `DATA_FRAME_CHANGES`, by the session and the table.
DATA_FRAMES = {
    "filter": lambda spark, table: spark.table(table.name).where(col("b").isNotNull()),
    "columns of DataFrames": columns_of_dataframes,
    "self join": self_join,
    "hash": lambda spark, table: spark.table(table.name).select("id", xxhash64("b").alias("h")),
    "cast to string": lambda spark, table: spark.table(table.name).select("id", col("b").cast("string").alias("s")),
    "sort": lambda spark, table: spark.table(table.name).orderBy("b", "id").limit(10),
    "aggregate": lambda spark, table: spark.table(table.name).groupBy("id").agg(count("b").alias("n")),
    "Python UDF": lambda spark, table: spark.table(table.name).select("id", udf(str, StringType())("b").alias("s")),
    "pandas UDF": lambda spark, table: spark.table(table.name).select(
        "id", pandas_udf(to_strings, "string")("b").alias("s")
    ),
    "map in pandas": lambda spark, table: spark.table(table.name).mapInPandas(
        lambda batches: (batch.assign(b=batch["b"].astype(str)) for batch in batches), "id INT, b STRING"
    ),
    "local relation": lambda spark, table: spark.table(table.name).join(
        spark.createDataFrame([(1, "one"), (2, "two")], "id INT, name STRING"), "id"
    ),
    "random with a seed": lambda spark, table: spark.table(table.name).select("id", "b", rand(7).alias("r")),
    "parameters": lambda spark, table: spark.sql(f"SELECT * FROM {table.name} WHERE id > :x", args={"x": 0}),
    "temporary view": lambda spark, table: spark.table(table.create_view("dv")).where(col("id") > 0),
    "cached": lambda spark, table: spark.table(table.name).where(col("id") > 0).cache(),
}
# The types of `b` that a table is created with and then created again with, and their rows.
DATA_FRAME_CHANGES = {
    "INT to BIGINT": ("b INT", "(1, 1)", "b BIGINT", "(1, 3000000000), (2, 2)"),
    "BIGINT to INT": ("b BIGINT", "(1, 1)", "b INT", "(1, 3), (2, 2)"),
    "INT to STRING": ("b INT", "(1, 1)", "b STRING", "(1, 'x'), (2, '2')"),
    "struct fields reordered": (
        "b STRUCT<x: INT, y: INT>",
        "(1, named_struct('x', 1, 'y', 2))",
        "b STRUCT<y: INT, x: INT>",
        "(1, named_struct('y', 3, 'x', 4)), (2, named_struct('y', 5, 'x', 6))",
    ),
    "struct field added": (
        "b STRUCT<x: INT, y: INT>",
        "(1, named_struct('x', 1, 'y', 2))",
        "b STRUCT<x: INT, y: INT, z: STRING>",
        "(1, named_struct('x', 3, 'y', 4, 'z', 'q'))",
    ),
}


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("kind", ["dv", "dgv"])
@pytest.mark.parametrize("frame", DATA_FRAMES)
@pytest.mark.parametrize("change", DATA_FRAME_CHANGES)
def test_dataframe_view_reads_what_its_dataframe_reads_after_its_table_changed(spark, tables, fmt, kind, frame, change):
    created, before, current, after = DATA_FRAME_CHANGES[change]
    table = tables(fmt, f"id INT, {created}")
    table.insert(before)
    definition = DATA_FRAMES[frame](spark, table)
    columns = definition.columns
    view = table.create_view(kind, frame=definition)
    table.recreate(f"id INT, {current}", after)
    # The view reads what the DataFrame that it was created from reads now, under the names of its
    # columns, with the types that they have now. The client keeps the schema of a DataFrame once it
    # has it, so the types are those of a new DataFrame over the DataFrame.
    result = spark.table(view)
    assert result.columns == columns
    assert [x.dataType for x in result.schema] == [x.dataType for x in definition.select("*").schema]
    assert sorted(result.collect()) == sorted(definition.collect())


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("kind", ["dv", "dgv"])
@pytest.mark.parametrize("frame", DATA_FRAMES)
def test_dataframe_view_fails_when_a_column_that_it_reads_was_dropped(spark, tables, fmt, kind, frame):
    table = tables(fmt, "id INT, b INT")
    table.insert("(1, 1)")
    view = table.create_view(kind, frame=DATA_FRAMES[frame](spark, table))
    table.recreate("id INT", "(2)")
    message = error(spark, f"SELECT * FROM {view}")
    # The Python function of `mapInPandas` declares its columns, and fails to find the column.
    if frame != "map in pandas":
        assert COLUMNS_CHANGED in message


@pytest.mark.parametrize("kind", KINDS)
def test_view_over_a_persistent_view_reads_its_current_definition(spark, tables, kind):
    table = tables("delta", "id INT, b INT")
    table.insert("(1, 10), (2, 20)")
    persistent = f"pv_{uuid.uuid4().hex}"
    try:
        spark.sql(f"CREATE VIEW {persistent} AS SELECT id FROM {table.name} WHERE b > 10")
        view = table.create_view(kind, f"SELECT * FROM {persistent}")
        outer = table.create_view("sv", f"SELECT id FROM {view}")
        assert rows(spark, f"SELECT * FROM {outer}") == [(2,)]
        # The persistent view is defined again over the same table, which did not change.
        for definition, expected in [
            (f"SELECT id FROM {table.name}", [(1,), (2,)]),
            (f"SELECT b, id * 10 AS id FROM {table.name}", [(10,), (20,)]),
        ]:
            spark.sql(f"CREATE OR REPLACE VIEW {persistent} AS {definition}")
            assert rows(spark, f"SELECT id FROM {persistent}") == expected
            assert rows(spark, f"SELECT * FROM {view}") == expected
            assert rows(spark, f"SELECT * FROM {outer}") == expected
            assert spark.table(view).schema == spark.sql(f"SELECT id FROM {persistent}").schema
        # A definition that no longer has the column, and a dropped view. A request for the schema
        # gets the columns that the view was created with.
        spark.sql(f"CREATE OR REPLACE VIEW {persistent} AS SELECT 1 AS other")
        for name in (view, outer):
            assert "INCOMPATIBLE_VIEW_SCHEMA_CHANGE" in error(spark, f"SELECT * FROM {name}")
        spark.sql(f"DROP VIEW {persistent}")
        for name in (view, outer):
            assert NOT_FOUND in error(spark, f"SELECT * FROM {name}")
            assert spark.table(name).columns == ["id"]
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {persistent}")


@pytest.mark.parametrize("inner", ["dv", "dgv", "sv"])
@pytest.mark.parametrize("outer", ["sv", "gv"])
@pytest.mark.parametrize("change", ["added in place", "created again"])
def test_view_over_a_view_reads_a_struct_that_gained_a_field(spark, tables, inner, outer, change):
    table = tables("delta", "k INT, c STRUCT<n: INT>")
    table.insert("(1, named_struct('n', 1))")
    inner_view = table.create_view(inner, f"SELECT k, c FROM {table.name}")
    query = "SELECT k, c, to_json(c) AS j FROM {v}"
    view = table.create_view(outer, query.format(v=inner_view))
    if change == "added in place":
        spark.sql(f"ALTER TABLE {table.name} ADD COLUMNS (c.g INT)")
        table.insert("(2, named_struct('n', 2, 'g', 3))")
    else:
        table.recreate("k INT, c STRUCT<n: INT, g: INT>", "(2, named_struct('n', 2, 'g', 3))")
    # Both views resolve their definitions again, so that the outer view reads the struct with the field.
    direct = query.format(v=f"(SELECT k, c FROM {table.name}) q")
    assert rows(spark, f"SELECT * FROM {view}") == rows(spark, direct)
