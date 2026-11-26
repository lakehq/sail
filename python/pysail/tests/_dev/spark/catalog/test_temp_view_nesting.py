# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

"""Temporary views over other temporary views keep the definitions that they were created with,
however deep the views are nested, and even when the views read their own names."""

from __future__ import annotations

import uuid

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests._dev.spark.catalog.temp_view_utils import LAKE_FORMATS, rows

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only: Spark rejects views that read their own names")


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


@pytest.mark.parametrize("fmt", LAKE_FORMATS)
@pytest.mark.parametrize("kind", ["sv", "dv"])
def test_view_replaced_many_times_by_a_query_over_itself_reads_the_current_rows(spark, tables, names, kind, fmt):
    table = tables(fmt)
    values = [0]
    table.insert("(0)")
    view = names()
    spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT * FROM {table.name}")
    for i in range(1, 151):
        replace_with_query_over_itself(spark, kind, view)
        if i % 50 == 0:
            # Each definition of the view reads the rows committed after it was replaced.
            values.append(1000 * i)
            table.insert(f"({values[-1]})")
            assert rows(spark, f"SELECT * FROM {view}") == [(x + i,) for x in values]


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


def test_view_keeps_the_persistent_view_that_it_was_created_with(spark, names):
    persistent, view = names("pv"), names()
    spark.sql(f"CREATE VIEW {persistent} AS SELECT 1 AS id")
    spark.sql(f"CREATE TEMPORARY VIEW {view} AS SELECT * FROM {persistent}")
    spark.sql(f"CREATE OR REPLACE VIEW {persistent} AS SELECT 2 AS id")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]
    spark.sql(f"DROP VIEW {persistent}")
    assert rows(spark, f"SELECT * FROM {view}") == [(1,)]


# The first view of a chain of views over `t (id BIGINT, b INT)` that reads the rows `(1, 1)`, the
# change of the table, and the row of the first view after the change. Each view over it adds 1 to `id`.
CHANGED_CHAINS = {
    "retyped": ("SELECT id, b FROM {t}", ("id INT, b STRING", "(1, 'x')"), (1, "x")),
    "gained": (
        "SELECT id, to_json(struct(*)) AS b FROM {t}",
        ("id BIGINT, b INT, c INT", "(1, 1, 2)"),
        (1, '{"id":1,"b":1,"c":2}'),
    ),
    "folded": ("SELECT 1L AS id, typeof((SELECT b FROM {t} LIMIT 1)) AS b", ("b STRING", "('x')"), (1, "string")),
}


@pytest.mark.parametrize("change", CHANGED_CHAINS)
@pytest.mark.parametrize("shape", ["chain", "replaced", "replaced by a DataFrame"])
@pytest.mark.parametrize("depth", [30, 100, 101])
def test_views_nested_deeply_resolve_their_queries_again_after_their_table_changed(
    spark, tables, names, change, shape, depth
):
    first, columns, (i, value) = CHANGED_CHAINS[change]
    table = tables("delta", "id BIGINT, b INT")
    table.insert("(1, 1)")
    view = names()
    spark.sql(f"CREATE TEMPORARY VIEW {view} AS {first.format(t=table.name)}")
    for _ in range(depth - 1):
        inner = view
        if shape == "chain":
            view = names()
        if shape == "replaced by a DataFrame":
            spark.table(inner).selectExpr("id + 1 AS id", "b").createOrReplaceTempView(view)
        else:
            spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {view} AS SELECT id + 1 AS id, b FROM {inner}")
    table.recreate(*columns)
    if depth <= 100:
        assert rows(spark, f"SELECT * FROM {view}") == [(i + depth - 1, value)]
        assert spark.table(view).columns == ["id", "b"]
    elif change == "gained":
        # The plans of the views fit their table, so that the views nested more deeply read them.
        assert len(rows(spark, f"SELECT * FROM {view}")) == 1
    else:
        # Spark fails to analyze views nested more deeply as well.
        with pytest.raises(Exception, match="VIEW_EXCEED_MAX_NESTED_DEPTH"):
            spark.sql(f"SELECT * FROM {view}").collect()
