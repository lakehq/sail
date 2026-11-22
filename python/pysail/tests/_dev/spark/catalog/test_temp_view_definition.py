# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

"""A temporary view created with a SQL query resolves the query at each read, as a Spark SQL view does."""

from __future__ import annotations

import uuid

import pytest
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import lit, udf, udtf

from pysail.testing.spark.session import patch_spark_connect_session, spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only: Delta Lake tables in the memory catalog")

# The kinds of SQL views: session, session with a column list, and global.
KINDS = ["sv", "svc", "gv"]

CHANGED = "INCOMPATIBLE_VIEW_SCHEMA_CHANGE"
UP_CAST = "CANNOT_UP_CAST_DATATYPE"
NOT_FOUND = "TABLE_OR_VIEW_NOT_FOUND"
TOO_DEEP = "VIEW_EXCEED_MAX_NESTED_DEPTH"


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server() as server:
        yield server.remote


def new_session(remote):
    session = SparkSession.builder.remote(remote).create()
    patch_spark_connect_session(session)
    return session


@pytest.fixture(scope="module")
def creator(remote):
    session = new_session(remote)
    yield session
    session.stop()


@pytest.fixture(scope="module")
def reader(remote):
    session = new_session(remote)
    yield session
    session.stop()


def name(prefix="n"):
    return f"{prefix}_{uuid.uuid4().hex}"


def rows(session, sql):
    return sorted(tuple(row) for row in session.sql(sql).collect())


def error(session, sql):
    with pytest.raises(Exception) as info:  # noqa: PT011
        session.sql(sql).collect()
    return str(info.value)


def create_table(session, tmp_path, table, columns, values=None):
    location = tmp_path / name("location")
    session.sql(f"CREATE TABLE {table} ({columns}) USING delta LOCATION '{location}'")
    if values:
        session.sql(f"INSERT INTO {table} VALUES {values}")


def replace_table(session, tmp_path, table, columns, values=None):
    session.sql(f"DROP TABLE {table}")
    create_table(session, tmp_path, table, columns, values)


def create_view(session, kind, query, columns=None):
    view = name(kind)
    if kind == "sv":
        session.sql(f"CREATE TEMPORARY VIEW {view} AS {query}")
        return view
    if kind == "svc":
        columns = columns or ", ".join(session.sql(query).columns)
        session.sql(f"CREATE TEMPORARY VIEW {view} ({columns}) AS {query}")
        return view
    session.sql(f"CREATE GLOBAL TEMPORARY VIEW {view} AS {query}")
    return f"global_temp.{view}"


@pytest.fixture
def table(creator):
    table = name("t")
    yield table
    creator.sql(f"DROP VIEW IF EXISTS {table}")
    creator.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("kind", KINDS)
def test_view_ignores_changes_of_a_column_that_it_does_not_read(creator, tmp_path, table, kind):
    create_table(creator, tmp_path, table, "id INT, extra INT", "(1, 2)")
    view = create_view(creator, kind, f"SELECT id FROM {table}")
    replace_table(creator, tmp_path, table, "id INT", "(3)")
    assert rows(creator, f"SELECT * FROM {view}") == [(3,)]
    replace_table(creator, tmp_path, table, "id INT, other STRING", "(4, 'x')")
    assert rows(creator, f"SELECT * FROM {view}") == [(4,)]
    replace_table(creator, tmp_path, table, "extra STRING, id INT", "('x', 5)")
    assert rows(creator, f"SELECT * FROM {view}") == [(5,)]


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_column_of_a_narrower_type_as_its_type(creator, tmp_path, table, kind):
    create_table(creator, tmp_path, table, "id INT", "(1)")
    view = create_view(creator, kind, f"SELECT * FROM {table}")
    replace_table(creator, tmp_path, table, "id SMALLINT", "(2)")
    assert rows(creator, f"SELECT * FROM {view}") == [(2,)]
    assert creator.sql(f"SELECT * FROM {view}").schema["id"].dataType.simpleString() == "int"


@pytest.mark.parametrize("kind", KINDS)
def test_view_follows_the_nullability_of_a_column(creator, tmp_path, table, kind):
    create_table(creator, tmp_path, table, "id INT NOT NULL", "(1)")
    view = create_view(creator, kind, f"SELECT * FROM {table}")
    replace_table(creator, tmp_path, table, "id INT", "(NULL)")
    assert rows(creator, f"SELECT * FROM {view}") == [(None,)]
    assert creator.sql(f"SELECT * FROM {view}").schema["id"].nullable


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "columns",
    ["id BIGINT", "id STRING", "id DOUBLE", "id DECIMAL(19, 0)"],
)
def test_view_rejects_a_column_type_that_is_not_an_up_cast(creator, tmp_path, table, kind, columns):
    create_table(creator, tmp_path, table, "id INT", "(1)")
    view = create_view(creator, kind, f"SELECT * FROM {table}")
    replace_table(creator, tmp_path, table, columns)
    assert UP_CAST in error(creator, f"SELECT * FROM {view}")


def test_view_of_a_bigint_rejects_a_decimal_that_can_overflow_it(creator, tmp_path, table):
    create_table(creator, tmp_path, table, "id BIGINT", "(1)")
    view = create_view(creator, "sv", f"SELECT * FROM {table}")
    replace_table(creator, tmp_path, table, "id DECIMAL(19, 0)")
    assert UP_CAST in error(creator, f"SELECT * FROM {view}")
    replace_table(creator, tmp_path, table, "id DECIMAL(18, 0)", "(2)")
    assert rows(creator, f"SELECT * FROM {view}") == [(2,)]


@pytest.mark.parametrize("kind", KINDS)
def test_view_over_all_columns_rejects_a_dropped_column(creator, tmp_path, table, kind):
    create_table(creator, tmp_path, table, "id INT, extra INT", "(1, 2)")
    view = create_view(creator, kind, f"SELECT * FROM {table}")
    replace_table(creator, tmp_path, table, "id INT", "(1)")
    assert CHANGED in error(creator, f"SELECT * FROM {view}")


@pytest.mark.parametrize("kind", KINDS)
def test_view_of_a_named_column_rejects_a_dropped_column(creator, tmp_path, table, kind):
    create_table(creator, tmp_path, table, "id INT, extra INT", "(1, 2)")
    view = create_view(creator, kind, f"SELECT id FROM {table}")
    replace_table(creator, tmp_path, table, "extra INT", "(2)")
    assert "id" in error(creator, f"SELECT * FROM {view}")


def test_view_with_a_column_list_matches_query_columns_by_name_and_position(creator):
    view = name("sv")
    creator.sql(f"CREATE TEMPORARY VIEW {view} (x, y) AS SELECT 1 AS col, 2 AS col")
    assert rows(creator, f"SELECT x, y FROM {view}") == [(1, 2)]


@pytest.mark.parametrize("kind", KINDS)
def test_view_reads_a_persistent_view_that_replaced_its_table(creator, tmp_path, table, kind):
    create_table(creator, tmp_path, table, "id INT", "(1)")
    view = create_view(creator, kind, f"SELECT * FROM {table}")
    creator.sql(f"DROP TABLE {table}")
    creator.sql(f"CREATE VIEW {table} AS SELECT 7 AS id")
    assert rows(creator, f"SELECT * FROM {view}") == [(7,)]


@pytest.mark.parametrize("kind", ["sv", "svc"])
def test_view_follows_an_inner_view(creator, kind):
    inner = name("inner")
    creator.sql(f"CREATE TEMPORARY VIEW {inner} AS SELECT 1 AS id")
    view = create_view(creator, kind, f"SELECT * FROM {inner}")
    creator.sql(f"CREATE OR REPLACE TEMPORARY VIEW {inner} AS SELECT 2 AS id")
    assert rows(creator, f"SELECT * FROM {view}") == [(2,)]
    creator.sql(f"DROP VIEW {inner}")
    assert NOT_FOUND in error(creator, f"SELECT * FROM {view}")


def test_view_follows_a_session_function(creator):
    function = name("f")
    creator.udf.register(function, udf(lambda x: x + 1, "int"))
    view = create_view(creator, "sv", f"SELECT {function}(1) AS v")
    assert rows(creator, f"SELECT * FROM {view}") == [(2,)]
    creator.udf.register(function, udf(lambda x: x + 100, "int"))
    assert rows(creator, f"SELECT * FROM {view}") == [(101,)]


def test_function_registered_later_does_not_replace_a_built_in_function(remote):
    session = new_session(remote)
    try:
        view = create_view(session, "sv", "SELECT upper('a') AS v")
        session.udf.register("upper", udf(lambda _: "z", "string"))
        assert rows(session, "SELECT upper('a')") == [("z",)]
        assert rows(session, f"SELECT * FROM {view}") == [("A",)]
    finally:
        session.stop()


def test_view_keeps_its_creation_database(creator, tmp_path):
    first, second, table = name("db"), name("db"), name("t")
    creator.sql(f"CREATE DATABASE {first}")
    creator.sql(f"CREATE DATABASE {second}")
    try:
        create_table(creator, tmp_path, f"{first}.{table}", "id INT", "(1)")
        create_table(creator, tmp_path, f"{second}.{table}", "id INT", "(2)")
        creator.sql(f"USE DATABASE {first}")
        view = create_view(creator, "sv", f"SELECT * FROM {table}")
        creator.sql(f"USE DATABASE {second}")
        assert rows(creator, f"SELECT * FROM {view}") == [(1,)]
    finally:
        creator.sql("USE DATABASE default")
        creator.sql(f"DROP DATABASE {first} CASCADE")
        creator.sql(f"DROP DATABASE {second} CASCADE")


def test_view_keeps_its_creation_configuration(remote):
    session = new_session(remote)
    try:
        session.conf.set("spark.sql.ansi.enabled", "false")
        view = create_view(session, "sv", "SELECT CAST('x' AS INT) AS v")
        session.conf.set("spark.sql.ansi.enabled", "true")
        assert rows(session, f"SELECT * FROM {view}") == [(None,)]
    finally:
        session.stop()


def test_view_does_not_see_a_later_view_with_the_name_of_its_table(creator, tmp_path, table):
    create_table(creator, tmp_path, table, "id INT", "(1)")
    view = create_view(creator, "sv", f"SELECT * FROM {table}")
    creator.sql(f"CREATE TEMPORARY VIEW {table} AS SELECT 9 AS id")
    try:
        assert rows(creator, f"SELECT * FROM {table}") == [(9,)]
        assert rows(creator, f"SELECT * FROM {view}") == [(1,)]
    finally:
        creator.sql(f"DROP VIEW {table}")


@pytest.mark.parametrize("persistent", [False, True])
def test_view_does_not_see_a_cte_of_the_reader(creator, tmp_path, table, persistent):
    create_table(creator, tmp_path, table, "id INT", "(1)")
    view = name("v")
    kind = "VIEW" if persistent else "TEMPORARY VIEW"
    creator.sql(f"CREATE {kind} {view} AS SELECT * FROM {table}")
    try:
        assert rows(creator, f"WITH {table} AS (SELECT 99 AS id) SELECT * FROM {view}") == [(1,)]
    finally:
        creator.sql(f"DROP VIEW {view}")


@pytest.mark.parametrize("kind", ["sv", "gv"])
def test_view_that_reads_its_own_name_keeps_working(creator, tmp_path, table, kind):
    create_table(creator, tmp_path, table, "id INT, extra INT", "(1, 2)")
    view = create_view(creator, kind, f"SELECT id FROM {table}")
    create = "CREATE OR REPLACE GLOBAL TEMPORARY VIEW" if kind == "gv" else "CREATE OR REPLACE TEMPORARY VIEW"
    bare = view.removeprefix("global_temp.")
    creator.sql(f"{create} {bare} AS SELECT id + 10 AS id FROM {view}")
    assert rows(creator, f"SELECT * FROM {view}") == [(11,)]
    creator.sql(f"INSERT INTO {table} VALUES (2, 3)")
    assert rows(creator, f"SELECT * FROM {view}") == [(11,), (12,)]
    # A column that neither version of the view reads can change.
    replace_table(creator, tmp_path, table, "id INT", "(5)")
    assert rows(creator, f"SELECT * FROM {view}") == [(15,)]


def test_view_replaced_many_times_by_a_query_over_itself(creator):
    view = name("sv")
    creator.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT 0 AS id")
    for _ in range(150):
        creator.sql(f"CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT id + 1 AS id FROM {view}")
    assert rows(creator, f"SELECT * FROM {view}") == [(150,)]


def test_views_that_read_each_other_keep_working(creator):
    a, b = name("a"), name("b")
    creator.sql(f"CREATE TEMPORARY VIEW {b} AS SELECT 1 AS id")
    creator.sql(f"CREATE TEMPORARY VIEW {a} AS SELECT id + 10 AS id FROM {b}")
    creator.sql(f"CREATE OR REPLACE TEMPORARY VIEW {b} AS SELECT id + 100 AS id FROM {a}")
    assert rows(creator, f"SELECT * FROM {b}") == [(111,)]
    assert rows(creator, f"SELECT * FROM {a}") == [(121,)]


def test_view_over_a_data_frame_argument_outlives_the_statement(creator):
    view = name("sv")
    creator.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT id + 1 AS id FROM {{df}}", df=creator.range(2))
    assert rows(creator, f"SELECT * FROM {view}") == [(1,), (2,)]


def test_view_nesting_limit(creator):
    views = [name("d")]
    creator.sql(f"CREATE TEMPORARY VIEW {views[0]} AS SELECT 1 AS a")
    for _ in range(99):
        view = name("d")
        creator.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT a FROM {views[-1]}")
        views.append(view)
    assert rows(creator, f"SELECT * FROM {views[-1]}") == [(1,)]
    assert TOO_DEEP in error(creator, f"CREATE TEMPORARY VIEW {name('d')} AS SELECT a FROM {views[-1]}")


def test_global_view_reads_the_session_names_of_its_creator(remote, reader, tmp_path):
    session = new_session(remote)
    inner, table, function = name("inner"), name("t"), name("f")
    create_table(session, tmp_path, table, "id INT, extra INT", "(1, 2)")
    session.sql(f"CREATE TEMPORARY VIEW {inner} AS SELECT id FROM {table}")
    session.udf.register(function, udf(lambda x: x + 1, "int"))
    view = create_view(session, "gv", f"SELECT {function}(id) AS id FROM {inner}")
    try:
        assert rows(reader, f"SELECT * FROM {view}") == [(2,)]
        session.sql(f"INSERT INTO {table} VALUES (2, 3)")
        assert rows(reader, f"SELECT * FROM {view}") == [(2,), (3,)]
        replace_table(session, tmp_path, table, "id INT", "(5)")
        assert rows(reader, f"SELECT * FROM {view}") == [(6,)]
        session.sql(f"INSERT INTO {table} VALUES (6)")
    finally:
        session.stop()
    # The names of the session remain after the session ends.
    assert rows(reader, f"SELECT * FROM {view}") == [(6,), (7,)]
    reader.sql(f"DROP VIEW {view}")


def test_view_over_a_global_view_can_have_the_name_of_an_inner_view_of_its_creator(creator, reader):
    inner = name("inner")
    creator.sql(f"CREATE TEMPORARY VIEW {inner} AS SELECT 1 AS id")
    view = create_view(creator, "gv", f"SELECT id + 1 AS id FROM {inner}")
    try:
        reader.sql(f"CREATE TEMPORARY VIEW {inner} AS SELECT id + 10 AS id FROM {view}")
        assert rows(reader, f"SELECT * FROM {inner}") == [(12,)]
    finally:
        reader.sql(f"DROP VIEW IF EXISTS {inner}")
        creator.sql(f"DROP VIEW {inner}")
        creator.sql(f"DROP VIEW {view}")


def test_global_view_reads_a_persistent_view_of_its_creator(creator, reader, tmp_path, table):
    persistent = name("pv")
    create_table(creator, tmp_path, table, "id INT", "(1)")
    creator.sql(f"CREATE VIEW {persistent} AS SELECT id + 1 AS id FROM {table}")
    view = create_view(creator, "gv", f"SELECT * FROM {persistent}")
    try:
        assert rows(reader, f"SELECT * FROM {view}") == [(2,)]
        creator.sql(f"INSERT INTO {table} VALUES (2)")
        assert rows(reader, f"SELECT * FROM {view}") == [(2,), (3,)]
    finally:
        creator.sql(f"DROP VIEW {view}")
        creator.sql(f"DROP VIEW {persistent}")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT * FROM range(2)", [(0,), (1,)]),
        ("SELECT * FROM VALUES (1, 'a') AS t(x, y)", [(1, "a")]),
        ("WITH c AS (SELECT 1 AS x) SELECT a.x FROM c a JOIN c b ON a.x = b.x", [(1,)]),
        ("SELECT explode(array(1, 2)) AS x", [(1,), (2,)]),
    ],
)
def test_view_without_a_table(creator, reader, kind, query, expected):
    view = create_view(creator, kind, query)
    assert rows(creator, f"SELECT * FROM {view}") == expected
    if kind == "gv":
        assert rows(reader, f"SELECT * FROM {view}") == expected


def test_view_over_a_file_path_reads_the_current_files(creator, tmp_path):
    path = tmp_path / name("files")
    creator.range(1).write.parquet(str(path))
    view = create_view(creator, "sv", f"SELECT * FROM parquet.`{path}`")
    assert rows(creator, f"SELECT * FROM {view}") == [(0,)]
    creator.range(1, 2).write.mode("append").parquet(str(path))
    assert rows(creator, f"SELECT * FROM {view}") == [(0,), (1,)]


def test_view_over_a_table_function(creator, reader):
    @udtf(returnType="x: int")
    class Numbers:
        def eval(self, n):
            for i in range(n):
                yield (i,)

    function = name("udtf")
    creator.udtf.register(function, Numbers)
    session_view = create_view(creator, "sv", f"SELECT * FROM {function}(2)")
    global_view = create_view(creator, "gv", f"SELECT * FROM {function}(2)")
    assert rows(creator, f"SELECT * FROM {session_view}") == [(0,), (1,)]
    assert rows(reader, f"SELECT * FROM {global_view}") == [(0,), (1,)]


def test_view_over_a_streaming_view_is_read_by_a_streaming_query(creator):
    source, view = name("stream"), name("sv")
    creator.readStream.format("rate").load().withColumn("tag", lit(1)).createOrReplaceTempView(source)
    creator.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT value, tag FROM {source}")
    query = creator.table(view).writeStream.format("console").start()
    try:
        assert query.isActive
        assert query.exception() is None
        query.processAllAvailable()
    finally:
        query.stop()


UP_CASTS = [
    ("CAST(1 AS TINYINT)", "SMALLINT"),
    ("CAST(1 AS SMALLINT)", "INT"),
    ("1", "BIGINT"),
    ("1", "FLOAT"),
    ("CAST(1 AS BIGINT)", "DOUBLE"),
    ("1", "DECIMAL(10, 0)"),
    ("CAST(1 AS BIGINT)", "DECIMAL(20, 0)"),
    ("CAST(1.5 AS DECIMAL(10, 2))", "DECIMAL(12, 4)"),
    ("CAST(7 AS DECIMAL(9, 0))", "INT"),
    ("CAST(7 AS DECIMAL(18, 0))", "BIGINT"),
    ("DATE'2024-01-02'", "TIMESTAMP"),
    ("DATE'2024-01-02'", "TIMESTAMP_NTZ"),
    ("TIMESTAMP'2024-01-02 03:04:05'", "TIMESTAMP_NTZ"),
    ("TIMESTAMP_NTZ'2024-01-02 03:04:05'", "TIMESTAMP"),
    ("TIMESTAMP'2024-01-02 03:04:05'", "BIGINT"),
    ("CAST(1704164645 AS BIGINT)", "TIMESTAMP"),
    ("1", "STRING"),
    ("1.5D", "STRING"),
    ("CAST(1.5 AS DECIMAL(10, 2))", "STRING"),
    ("true", "STRING"),
    ("X'41'", "STRING"),
    ("DATE'2024-01-02'", "STRING"),
    ("TIMESTAMP'2024-01-02 03:04:05'", "STRING"),
    ("INTERVAL '1' YEAR", "STRING"),
    ("INTERVAL '1' DAY", "STRING"),
    ("INTERVAL '1' DAY", "INTERVAL DAY TO SECOND"),
    ("INTERVAL '1' YEAR", "INTERVAL YEAR TO MONTH"),
    ("NULL", "INT"),
    ("array(CAST(1 AS SMALLINT))", "ARRAY<INT>"),
    ("map(1, CAST(2 AS SMALLINT))", "MAP<BIGINT, INT>"),
    ("named_struct('a', CAST(1 AS SMALLINT))", "STRUCT<x: INT>"),
]


@pytest.mark.parametrize(("value", "stored"), UP_CASTS)
def test_view_reads_each_up_cast_as_a_cast(creator, value, stored):
    inner = name("inner")
    creator.sql(f"CREATE TEMPORARY VIEW {inner} AS SELECT CAST(NULL AS {stored}) AS c")
    view = create_view(creator, "sv", f"SELECT c FROM {inner}")
    creator.sql(f"CREATE OR REPLACE TEMPORARY VIEW {inner} AS SELECT {value} AS c")
    actual = creator.sql(f"SELECT * FROM {view}")
    assert actual.schema["c"].dataType == creator.sql(f"SELECT CAST(NULL AS {stored}) AS c").schema["c"].dataType
    expected = creator.sql(f"SELECT CAST({value} AS {stored}) AS c")
    # The client cannot collect every type, such as a year-month interval.
    assert actual.selectExpr("CAST(c AS STRING)").collect() == expected.selectExpr("CAST(c AS STRING)").collect()


def test_global_view_keeps_its_creation_database_in_another_session(creator, reader, tmp_path):
    database, table = name("db"), name("t")
    creator.sql(f"CREATE DATABASE {database}")
    try:
        create_table(creator, tmp_path, f"{database}.{table}", "id INT", "(1)")
        creator.sql(f"USE DATABASE {database}")
        view = create_view(creator, "gv", f"SELECT * FROM {table}")
        assert rows(reader, f"SELECT * FROM {view}") == [(1,)]
        reader.sql(f"DROP VIEW {view}")
    finally:
        creator.sql("USE DATABASE default")
        creator.sql(f"DROP DATABASE {database} CASCADE")
