import pytest
from pyspark.sql import functions as F  # noqa: N812

# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_function.py,
# SparkConnectFunctionTests.test_lambda_functions, which compares each result with
# JVM Spark. The expected rows below are what JVM Spark returns for the same calls with
# ANSI mode on, as in the test session.
QUERY = """
SELECT * FROM VALUES
(ARRAY('a', 'ab'), ARRAY(1, 2, 3), ARRAY(1, NULL, 3), 1, 2, 'a', NULL, MAP(0, 0)),
(ARRAY('x', NULL), NULL, ARRAY(1, 3), 3, 4, 'x', MAP(2, 0), MAP(-1, 1)),
(NULL, ARRAY(-1, -2, -3), Array(), 5, 6, NULL, MAP(-1, 2, -3, -4), NULL)
AS tab(a, b, c, d, e, f, g, h)
"""


@pytest.fixture(scope="module")
def df(spark):
    return spark.sql(QUERY)


def _by_length_desc(x, y):
    return F.when(x.isNull() | y.isNull(), F.lit(0)).otherwise(F.length(y) - F.length(x))


CASES = [
    ("exists", lambda df: df.select(F.exists(df.b, lambda x: x < 0)), [(False,), (None,), (True,)]),
    ("exists_isnull", lambda df: df.select(F.exists("a", lambda x: F.isnull(x))), [(False,), (True,), (None,)]),
    ("aggregate", lambda df: df.select(F.aggregate(df.b, "d", lambda acc, x: acc + x)), [(7,), (None,), (-1,)]),
    ("aggregate_name", lambda df: df.select(F.aggregate("b", df.d, lambda acc, x: acc + x)), [(7,), (None,), (-1,)]),
    (
        "aggregate_finish",
        lambda df: df.select(F.aggregate(df.b, "d", lambda acc, x: acc + x, lambda acc: acc + 100)),
        [(107,), (None,), (99,)],
    ),
    (
        "aggregate_finish_name",
        lambda df: df.select(F.aggregate("b", df.d, lambda acc, x: acc + x, lambda acc: acc + 100)),
        [(107,), (None,), (99,)],
    ),
    (
        "array_sort",
        lambda df: df.select(F.array_sort(df.b, lambda x, y: F.abs(x) - F.abs(y))),
        [([1, 2, 3],), (None,), ([-1, -2, -3],)],
    ),
    (
        "array_sort_with_nulls",
        lambda df: df.select(F.array_sort("a", _by_length_desc)),
        [(["ab", "a"],), (["x", None],), (None,)],
    ),
    ("filter", lambda df: df.select(F.filter(df.b, lambda x: x < 0)), [([],), (None,), ([-1, -2, -3],)]),
    (
        "filter_not_null",
        lambda df: df.select(F.filter("a", lambda x: ~F.isnull(x))),
        [(["a", "ab"],), (["x"],), (None,)],
    ),
    ("forall", lambda df: df.select(F.forall(df.b, lambda x: x != 0)), [(True,), (None,), (True,)]),
    ("forall_not_null", lambda df: df.select(F.forall("a", lambda x: ~F.isnull(x))), [(True,), (False,), (None,)]),
    ("transform", lambda df: df.select(F.transform(df.b, lambda x: x + 1)), [([2, 3, 4],), (None,), ([0, -1, -2],)]),
    (
        "transform_name",
        lambda df: df.select(F.transform("b", lambda x: x + 1)),
        [([2, 3, 4],), (None,), ([0, -1, -2],)],
    ),
    (
        "transform_index",
        lambda df: df.select(F.transform(df.b, lambda x, i: x + 1 - i)),
        [([2, 2, 2],), (None,), ([0, -2, -4],)],
    ),
    (
        "transform_index_name",
        lambda df: df.select(F.transform("b", lambda x, i: x + 1 - i)),
        [([2, 2, 2],), (None,), ([0, -2, -4],)],
    ),
    (
        "zip_with",
        lambda df: df.select(F.zip_with(df.b, "c", lambda v1, v2: v1 - F.abs(v2))),
        [([0, None, 0],), (None,), ([None, None, None],)],
    ),
    (
        "zip_with_name",
        lambda df: df.select(F.zip_with("b", df.c, lambda v1, v2: v1 - F.abs(v2))),
        [([0, None, 0],), (None,), ([None, None, None],)],
    ),
    ("map_filter", lambda df: df.select(F.map_filter(df.g, lambda k, v: k > v)), [(None,), ({2: 0},), ({-3: -4},)]),
    ("map_filter_name", lambda df: df.select(F.map_filter("g", lambda k, v: k > v)), [(None,), ({2: 0},), ({-3: -4},)]),
    (
        "map_zip_with",
        lambda df: df.select(F.map_zip_with(df.g, "h", lambda _k, v1, v2: v1 + v2)),
        [(None,), ({-1: None, 2: None},), (None,)],
    ),
    (
        "map_zip_with_name",
        lambda df: df.select(F.map_zip_with("g", df.h, lambda _k, v1, v2: v1 + v2)),
        [(None,), ({-1: None, 2: None},), (None,)],
    ),
    (
        "transform_keys",
        lambda df: df.select(F.transform_keys(df.g, lambda k, _v: k - 1)),
        [(None,), ({1: 0},), ({-2: 2, -4: -4},)],
    ),
    (
        "transform_keys_name",
        lambda df: df.select(F.transform_keys("g", lambda k, _v: k - 1)),
        [(None,), ({1: 0},), ({-2: 2, -4: -4},)],
    ),
    (
        "transform_values",
        lambda df: df.select(F.transform_values(df.g, lambda _k, v: F.abs(v) + 1)),
        [(None,), ({2: 1},), ({-1: 3, -3: 5},)],
    ),
    (
        "transform_values_name",
        lambda df: df.select(F.transform_values("g", lambda _k, v: F.abs(v) + 1)),
        [(None,), ({2: 1},), ({-1: 3, -3: 5},)],
    ),
]


@pytest.mark.parametrize(("build", "expected"), [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_lambda_function(df, build, expected):
    assert [tuple(row) for row in build(df).collect()] == expected
