import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import Row

from pysail.testing.spark.utils.common import pyspark_version

# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_function.py,
# SparkConnectFunctionTests.test_collection_functions, which compares each result with
# JVM Spark. The expected rows below are what JVM Spark returns for the same calls with
# ANSI mode on, as in the test session.
QUERY = """
SELECT * FROM VALUES
(ARRAY('a', 'ab'), ARRAY(1, 2, 3), ARRAY(1, NULL, 3), 1, 2, 'a'),
(ARRAY('x', NULL), NULL, ARRAY(1, 3), 3, 4, 'x'),
(NULL, ARRAY(-1, -2, -3), Array(), 5, 6, NULL)
AS tab(a, b, c, d, e, f)
"""


@pytest.fixture(scope="module")
def df(spark):
    return spark.sql(QUERY)


CASES = [
    (
        "array_distinct",
        lambda df: df.select(F.array_distinct("a"), F.array_distinct(df.b)),
        [(["a", "ab"], [1, 2, 3]), (["x", None], None), (None, [-1, -2, -3])],
    ),
    (
        "array_compact",
        lambda df: df.select(F.array_compact("a"), F.array_compact(df.b)),
        [(["a", "ab"], [1, 2, 3]), (["x"], None), (None, [-1, -2, -3])],
    ),
    ("array_max", lambda df: df.select(F.array_max("a"), F.array_max(df.b)), [("ab", 3), ("x", None), (None, -1)]),
    ("array_min", lambda df: df.select(F.array_min("a"), F.array_min(df.b)), [("a", 1), ("x", None), (None, -3)]),
    (
        "reverse",
        lambda df: df.select(F.reverse("a"), F.reverse(df.b)),
        [(["ab", "a"], [3, 2, 1]), ([None, "x"], None), (None, [-3, -2, -1])],
    ),
    ("size", lambda df: df.select(F.size("a"), F.size(df.b)), [(2, 3), (2, None), (None, 3)]),
    ("array_except", lambda df: df.select(F.array_except("b", df.c)), [([2],), (None,), ([-1, -2, -3],)]),
    ("array_intersect", lambda df: df.select(F.array_intersect("b", df.c)), [([1, 3],), (None,), ([],)]),
    ("array_union", lambda df: df.select(F.array_union("b", df.c)), [([1, 2, 3, None],), (None,), ([-1, -2, -3],)]),
    ("arrays_overlap", lambda df: df.select(F.arrays_overlap("b", df.c)), [(True,), (None,), (False,)]),
    ("array_position", lambda df: df.select(F.array_position(df.a, "ab")), [(2,), (0,), (None,)]),
    ("array_remove", lambda df: df.select(F.array_remove(df.a, "ab")), [(["a"],), (["x", None],), (None,)]),
    ("array", lambda df: df.select(F.array(df.d, "e")), [([1, 2],), ([3, 4],), ([5, 6],)]),
    (
        "array_with_literal",
        lambda df: df.select(F.array(df.d, "e", F.lit(99))),
        [([1, 2, 99],), ([3, 4, 99],), ([5, 6, 99],)],
    ),
    ("array_contains_literal", lambda df: df.select(F.array_contains(df.a, "ab")), [(True,), (None,), (None,)]),
    ("array_contains_column", lambda df: df.select(F.array_contains(df.a, df.f)), [(True,), (True,), (None,)]),
    (
        "array_append_literal",
        lambda df: df.select(F.array_append(df.a, "xyz")),
        [(["a", "ab", "xyz"],), (["x", None, "xyz"],), (None,)],
    ),
    (
        "array_append_lit",
        lambda df: df.select(F.array_append(df.a, F.lit("ab"))),
        [(["a", "ab", "ab"],), (["x", None, "ab"],), (None,)],
    ),
    (
        "array_append_column",
        lambda df: df.select(F.array_append(df.a, df.f)),
        [(["a", "ab", "a"],), (["x", None, "x"],), (None,)],
    ),
    (
        "array_prepend_literal",
        lambda df: df.select(F.array_prepend(df.a, "xyz")),
        [(["xyz", "a", "ab"],), (["xyz", "x", None],), (None,)],
    ),
    (
        "array_prepend_lit",
        lambda df: df.select(F.array_prepend(df.a, F.lit("ab"))),
        [(["ab", "a", "ab"],), (["ab", "x", None],), (None,)],
    ),
    (
        "array_prepend_column",
        lambda df: df.select(F.array_prepend(df.a, df.f)),
        [(["a", "a", "ab"],), (["x", "x", None],), (None,)],
    ),
    (
        "array_insert_negative",
        lambda df: df.select(F.array_insert(df.a, -5, "ab")),
        [(["ab", None, None, "a", "ab"],), (["ab", None, None, "x", None],), (None,)],
    ),
    (
        "array_insert_column",
        lambda df: df.select(F.array_insert(df.a, 3, df.f)),
        [(["a", "ab", "a"],), (["x", None, "x"],), (None,)],
    ),
    (
        "array_join",
        lambda df: df.select(F.array_join(df.a, ","), F.array_join("b", ":"), F.array_join("c", "~")),
        [("a,ab", "1:2:3", "1~3"), ("x", None, "1~3"), (None, "-1:-2:-3", "")],
    ),
    (
        "array_join_null_replacement",
        lambda df: df.select(
            F.array_join(df.a, ",", "_null_"),
            F.array_join("b", ":", ".null."),
            F.array_join("c", "~", "NULL"),
        ),
        [("a,ab", "1:2:3", "1~NULL~3"), ("x,_null_", None, "1~3"), (None, "-1:-2:-3", "")],
    ),
    (
        "array_repeat_column_count",
        lambda df: df.select(F.array_repeat(df.f, "d")),
        [(["a"],), (["x", "x", "x"],), ([None, None, None, None, None],)],
    ),
    (
        "array_repeat_name_count",
        lambda df: df.select(F.array_repeat("f", df.d)),
        [(["a"],), (["x", "x", "x"],), ([None, None, None, None, None],)],
    ),
    (
        "array_repeat_int_count",
        lambda df: df.select(F.array_repeat("f", 3)),
        [(["a", "a", "a"],), (["x", "x", "x"],), ([None, None, None],)],
    ),
    (
        "arrays_zip",
        lambda df: df.select(F.arrays_zip(df.b, "c")),
        [
            ([Row(b=1, c=1), Row(b=2, c=None), Row(b=3, c=3)],),
            (None,),
            ([Row(b=-1, c=None), Row(b=-2, c=None), Row(b=-3, c=None)],),
        ],
    ),
    ("concat", lambda df: df.select(F.concat("d", df.e, F.lit(-1))), [("12-1",), ("34-1",), ("56-1",)]),
    ("create_map", lambda df: df.select(F.create_map(df.d, df.e)), [({1: 2},), ({3: 4},), ({5: 6},)]),
    (
        "create_map_two_entries",
        lambda df: df.select(F.create_map(df.d, "e", "e", F.lit(1))),
        [({1: 2, 2: 1},), ({4: 1, 3: 4},), ({5: 6, 6: 1},)],
    ),
    ("element_at_name", lambda df: df.select(F.element_at("a", 1)), [("a",), ("x",), (None,)]),
    ("element_at_column", lambda df: df.select(F.element_at(df.a, 1)), [("a",), ("x",), (None,)]),
    ("get_name", lambda df: df.select(F.get("a", 1)), [("ab",), (None,), (None,)]),
    ("get_column", lambda df: df.select(F.get(df.a, 1)), [("ab",), (None,), (None,)]),
    (
        "slice",
        lambda df: df.select(F.slice(df.a, 1, 2), F.slice("c", 2, 3)),
        [(["a", "ab"], [None, 3]), (["x", None], [3]), (None, [])],
    ),
    (
        "sort_array",
        lambda df: df.select(F.sort_array(df.a, True), F.sort_array("c", False)),
        [(["a", "ab"], [3, 1, None]), ([None, "x"], [3, 1]), (None, [])],
    ),
    (
        "struct",
        lambda df: df.select(F.struct(df.a, "d", "e", df.f)),
        [
            (Row(a=["a", "ab"], d=1, e=2, f="a"),),
            (Row(a=["x", None], d=3, e=4, f="x"),),
            (Row(a=None, d=5, e=6, f=None),),
        ],
    ),
    (
        "sequence",
        lambda df: df.select(F.sequence(F.lit(1), F.lit(5))),
        [([1, 2, 3, 4, 5],), ([1, 2, 3, 4, 5],), ([1, 2, 3, 4, 5],)],
    ),
    (
        "sequence_with_step",
        lambda df: df.select(F.sequence(F.lit(1), F.lit(5), F.lit(1))),
        [([1, 2, 3, 4, 5],), ([1, 2, 3, 4, 5],), ([1, 2, 3, 4, 5],)],
    ),
    ("sequence_columns", lambda df: df.select(F.sequence(df.d, "e")), [([1, 2],), ([3, 4],), ([5, 6],)]),
    (
        "sequence_columns_with_step",
        lambda df: df.select(F.sequence(df.d, "e", F.lit(1))),
        [([1, 2],), ([3, 4],), ([5, 6],)],
    ),
]


@pytest.mark.parametrize(("build", "expected"), [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_collection_function(df, build, expected):
    assert [tuple(row) for row in build(df).collect()] == expected


def test_size_of_null_without_ansi(spark, df):
    # With ANSI mode off, spark.sql.legacy.sizeOfNull makes size(NULL) return -1.
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        assert [tuple(row) for row in df.select(F.size("a"), F.size(df.b)).collect()] == [(2, 3), (2, -1), (-1, 3)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


def test_shuffle(df):
    # The permutation is random, so only the row count is compared.
    assert df.select(F.shuffle(df.a), F.shuffle("b")).count() == 3  # noqa: PLR2004


@pytest.mark.skipif(pyspark_version() < (4,), reason="the shuffle seed argument requires PySpark 4+")
def test_shuffle_with_seed(df):
    def shuffled(seed):
        return [row[0] for row in df.select(F.shuffle("b", seed)).collect()]

    first = shuffled(7)
    # The same seed gives the same permutation, and each array keeps its elements.
    assert shuffled(7) == first
    assert [sorted(v) if v is not None else None for v in first] == [[1, 2, 3], None, [-3, -2, -1]]
