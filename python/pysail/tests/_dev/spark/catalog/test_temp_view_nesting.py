# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

"""Temporary views over other temporary views keep the definitions that they were created with,
however deep the views are nested, and even when the views read their own names."""

from __future__ import annotations

import uuid

import pytest
from pyspark.sql.functions import udf

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests._dev.spark.catalog.temp_view_utils import Table, rows

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only: Spark rejects views that read their own names")


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server() as server:
        yield server.remote


@pytest.fixture(scope="module")
def reader(spark, remote):
    with spark_session_factory(remote) as sessions:
        session = sessions.create()
        assert session.session_id != spark.session_id
        yield session


@pytest.fixture
def names(spark):
    """Returns new names of views, which are dropped after the test."""
    created = []

    def new(prefix="v"):
        created.append(f"{prefix}_{uuid.uuid4().hex}")
        return created[-1]

    yield new
    for name in created:
        spark.sql(f"DROP VIEW IF EXISTS {name}")


@pytest.mark.parametrize("depth", [101, 300])
def test_chain_of_views(spark, names, depth):
    views = [names()]
    spark.sql(f"CREATE TEMPORARY VIEW {views[0]} AS SELECT 0L AS id")
    for _ in range(depth):
        views.append(names())
        spark.sql(f"CREATE TEMPORARY VIEW {views[-1]} AS SELECT id + 1 AS id FROM {views[-2]}")
    assert rows(spark, f"SELECT * FROM {views[-1]}") == [(depth,)]


def replace_with_query_over_itself(spark, kind, view):
    if kind == "sv":
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT id + 1 AS id FROM {view}")
    else:
        spark.table(view).selectExpr("id + 1 AS id").createOrReplaceTempView(view)


@pytest.mark.parametrize("kind", ["sv", "dv"])
def test_view_replaced_many_times_by_a_query_over_itself(spark, names, kind):
    view = names()
    spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT 0L AS id")
    for _ in range(150):
        replace_with_query_over_itself(spark, kind, view)
    assert rows(spark, f"SELECT * FROM {view}") == [(150,)]


@pytest.mark.parametrize("kind", ["sv", "dv"])
def test_view_replaced_many_times_by_a_query_over_itself_reads_the_current_rows(spark, tmp_path, names, kind):
    table = Table(spark, tmp_path, "delta")
    try:
        table.insert("(0)")
        view = names()
        spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT * FROM {table.name}")
        for _ in range(150):
            replace_with_query_over_itself(spark, kind, view)
        table.insert("(1000)")
        assert rows(spark, f"SELECT * FROM {view}") == [(150,), (1150,)]
    finally:
        table.drop()


def test_views_that_read_each_other(spark, names):
    a, b = names("a"), names("b")
    spark.sql(f"CREATE TEMPORARY VIEW {b} AS SELECT 1 AS id")
    spark.sql(f"CREATE TEMPORARY VIEW {a} AS SELECT id + 10 AS id FROM {b}")
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {b} AS SELECT id + 100 AS id FROM {a}")
    assert rows(spark, f"SELECT * FROM {b}") == [(111,)]
    assert rows(spark, f"SELECT * FROM {a}") == [(11,)]
    # Each view is replaced in turn by a query over the other one.
    values = {a: 11, b: 111}
    for i in range(150):
        view, other = (a, b) if i % 2 == 0 else (b, a)
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT id + 1 AS id FROM {other}")
        values[view] = values[other] + 1
    assert rows(spark, f"SELECT * FROM {a}") == [(values[a],)]
    assert rows(spark, f"SELECT * FROM {b}") == [(values[b],)]


@pytest.mark.parametrize("kind", ["sv", "dv"])
def test_view_keeps_the_inner_view_that_it_was_created_with(spark, names, kind):
    inner, view = names("inner"), names()
    spark.sql(f"CREATE TEMPORARY VIEW {inner} AS SELECT 1 AS id")
    if kind == "sv":
        spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT * FROM {inner}")
    else:
        spark.table(inner).createOrReplaceTempView(view)
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {inner} AS SELECT 2 AS id")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    spark.sql(f"DROP VIEW {inner}")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]


def test_view_keeps_the_session_function_that_it_was_created_with(spark, names):
    function, view = names("fn"), names()
    spark.udf.register(function, udf(lambda x: f"A{x}"))
    spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT {function}(1) AS f")
    spark.udf.register(function, udf(lambda x: f"B{x}"))
    assert rows(spark, f"SELECT * FROM {view}") == [("A1",)]


def test_view_keeps_the_persistent_view_that_it_was_created_with(spark, names):
    persistent, view = names("pv"), names()
    spark.sql(f"CREATE VIEW {persistent} AS SELECT 1 AS id")
    spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT * FROM {persistent}")
    spark.sql(f"CREATE OR REPLACE VIEW {persistent} AS SELECT 2 AS id")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    spark.sql(f"DROP VIEW {persistent}")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]


def test_global_view_keeps_the_names_of_the_session_that_created_it(spark, reader, tmp_path, names):
    local, function, view = names("local"), names("fn"), f"gv_{uuid.uuid4().hex}"
    table = Table(spark, tmp_path, "parquet")
    try:
        table.insert("(1)")
        spark.sql(f"CREATE TEMPORARY VIEW {local} AS SELECT * FROM {table.name}")
        spark.udf.register(function, udf(lambda x: f"A{x}"))
        spark.sql(f"CREATE GLOBAL TEMPORARY VIEW {view} AS SELECT id, {function}(id) AS f FROM {local}")
        table.insert("(2)")
        assert rows(reader, f"SELECT * FROM global_temp.{view}") == [(1, "A1"), (2, "A2")]
        # The creator replaces its view and its function, and then drops its view.
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {local} AS SELECT 9L AS id")
        spark.udf.register(function, udf(lambda x: f"B{x}"))
        assert rows(reader, f"SELECT * FROM global_temp.{view}") == [(1, "A1"), (2, "A2")]
        spark.catalog.dropTempView(local)
        table.insert("(3)")
        assert rows(reader, f"SELECT * FROM global_temp.{view}") == [(1, "A1"), (2, "A2"), (3, "A3")]
    finally:
        spark.catalog.dropGlobalTempView(view)
        table.drop()


@pytest.mark.spark(version=">=4", reason="Uses DataFrame arguments of `spark.sql`")
def test_view_over_a_data_frame_argument_outlives_the_statement(spark, names):
    view = names()
    spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT id + 1 AS id FROM {{df}}", df=spark.range(2))
    assert rows(spark, f"SELECT * FROM {view}") == [(1,), (2,)]
