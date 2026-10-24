import math

import pytest
from pyspark.sql import functions as F  # noqa: N812

# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_function.py,
# SparkConnectFunctionTests.test_generator_functions, which compares each result with
# JVM Spark. The expected rows below are what JVM Spark returns for the same calls with
# ANSI mode on, as in the test session.
QUERY = """
SELECT * FROM VALUES
(ARRAY('a', 'ab'), ARRAY(1, 2, 3), ARRAY(1, NULL, 3), MAP(1, 2, 3, 4), 1, FLOAT(2.0), 3),
(ARRAY('x', NULL), NULL, ARRAY(1, 3), NULL, 3, FLOAT(4.0), 5),
(NULL, ARRAY(-1, -2, -3), Array(), MAP(-1, NULL, -3, -4), 7, FLOAT('NAN'), 9)
AS tab(a, b, c, d, e, f, g)
"""

STRUCTS = "ARRAY(STRUCT(e, f), STRUCT(g AS e, f))"


@pytest.fixture(scope="module")
def df(spark):
    return spark.sql(QUERY)


def _normalize(value):
    # NaN never equals itself, so it is compared as a marker.
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value


CASES = [
    (
        "explode_array",
        lambda df: df.select(F.explode(df.a), F.col("b")),
        [("a", [1, 2, 3]), ("ab", [1, 2, 3]), ("x", None), (None, None)],
    ),
    (
        "explode_array_name",
        lambda df: df.select(F.explode("a"), "b"),
        [("a", [1, 2, 3]), ("ab", [1, 2, 3]), ("x", None), (None, None)],
    ),
    (
        "explode_map",
        lambda df: df.select(F.explode(df.d), F.col("c")),
        [(1, 2, [1, None, 3]), (3, 4, [1, None, 3]), (-1, None, []), (-3, -4, [])],
    ),
    (
        "explode_map_name",
        lambda df: df.select(F.explode("d"), "c"),
        [(1, 2, [1, None, 3]), (3, 4, [1, None, 3]), (-1, None, []), (-3, -4, [])],
    ),
    (
        "explode_outer_array",
        lambda df: df.select(F.explode_outer(df.a), F.col("b")),
        [("a", [1, 2, 3]), ("ab", [1, 2, 3]), ("x", None), (None, None), (None, [-1, -2, -3])],
    ),
    (
        "explode_outer_array_name",
        lambda df: df.select(F.explode_outer("a"), "b"),
        [("a", [1, 2, 3]), ("ab", [1, 2, 3]), ("x", None), (None, None), (None, [-1, -2, -3])],
    ),
    (
        "explode_outer_map",
        lambda df: df.select(F.explode_outer(df.d), F.col("c")),
        [(1, 2, [1, None, 3]), (3, 4, [1, None, 3]), (None, None, [1, 3]), (-1, None, []), (-3, -4, [])],
    ),
    (
        "explode_outer_map_name",
        lambda df: df.select(F.explode_outer("d"), "c"),
        [(1, 2, [1, None, 3]), (3, 4, [1, None, 3]), (None, None, [1, 3]), (-1, None, []), (-3, -4, [])],
    ),
    (
        "flatten",
        lambda df: df.select(F.flatten(F.array("b", df.c)), F.col("b")),
        [([1, 2, 3, 1, None, 3], [1, 2, 3]), (None, None), ([-1, -2, -3], [-1, -2, -3])],
    ),
    (
        "inline",
        lambda df: df.select(F.expr(STRUCTS).alias("X")).select(F.inline("X")),
        [(1, 2.0), (3, 2.0), (3, 4.0), (5, 4.0), (7, "NaN"), (9, "NaN")],
    ),
    (
        "inline_outer",
        lambda df: df.select(F.expr(STRUCTS).alias("X")).select(F.inline_outer("X")),
        [(1, 2.0), (3, 2.0), (3, 4.0), (5, 4.0), (7, "NaN"), (9, "NaN")],
    ),
    (
        "posexplode_array",
        lambda df: df.select(F.posexplode(df.a), F.col("b")),
        [(0, "a", [1, 2, 3]), (1, "ab", [1, 2, 3]), (0, "x", None), (1, None, None)],
    ),
    (
        "posexplode_array_name",
        lambda df: df.select(F.posexplode("a"), "b"),
        [(0, "a", [1, 2, 3]), (1, "ab", [1, 2, 3]), (0, "x", None), (1, None, None)],
    ),
    (
        "posexplode_map",
        lambda df: df.select(F.posexplode(df.d), F.col("c")),
        [(0, 1, 2, [1, None, 3]), (1, 3, 4, [1, None, 3]), (0, -1, None, []), (1, -3, -4, [])],
    ),
    (
        "posexplode_map_name",
        lambda df: df.select(F.posexplode("d"), "c"),
        [(0, 1, 2, [1, None, 3]), (1, 3, 4, [1, None, 3]), (0, -1, None, []), (1, -3, -4, [])],
    ),
    (
        "posexplode_outer_array",
        lambda df: df.select(F.posexplode_outer(df.a), F.col("b")),
        [(0, "a", [1, 2, 3]), (1, "ab", [1, 2, 3]), (0, "x", None), (1, None, None), (None, None, [-1, -2, -3])],
    ),
    (
        "posexplode_outer_array_name",
        lambda df: df.select(F.posexplode_outer("a"), "b"),
        [(0, "a", [1, 2, 3]), (1, "ab", [1, 2, 3]), (0, "x", None), (1, None, None), (None, None, [-1, -2, -3])],
    ),
    (
        "posexplode_outer_map",
        lambda df: df.select(F.posexplode_outer(df.d), F.col("c")),
        [
            (0, 1, 2, [1, None, 3]),
            (1, 3, 4, [1, None, 3]),
            (None, None, None, [1, 3]),
            (0, -1, None, []),
            (1, -3, -4, []),
        ],
    ),
    (
        "posexplode_outer_map_name",
        lambda df: df.select(F.posexplode_outer("d"), "c"),
        [
            (0, 1, 2, [1, None, 3]),
            (1, 3, 4, [1, None, 3]),
            (None, None, None, [1, 3]),
            (0, -1, None, []),
            (1, -3, -4, []),
        ],
    ),
]


@pytest.mark.parametrize(("build", "expected"), [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_generator_function(df, build, expected):
    rows = [tuple(_normalize(v) for v in row) for row in build(df).collect()]
    assert rows == expected
