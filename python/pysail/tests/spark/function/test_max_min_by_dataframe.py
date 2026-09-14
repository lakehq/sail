"""Tests for `max_by` and `min_by` through the DataFrame API."""

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import pyspark_version

ROWS = [
    (1, "a", 10, "g1"),
    (2, "b", 50, "g1"),
    (3, "c", 20, "g2"),
    (4, "d", None, "g2"),
    (5, None, 40, "g1"),
    (6, "f", 30, "g2"),
]
SCHEMA = "i int, x string, y int, g string"

requires_top_k = pytest.mark.skipif(pyspark_version() < (4, 2), reason="the top-k form requires Spark 4.2+")


@pytest.fixture
def df(spark):
    # `createDataFrame` columns carry Spark field metadata, unlike SQL `VALUES` columns, so these
    # tests also cover a value argument whose Arrow field has metadata.
    return spark.createDataFrame(ROWS, SCHEMA)


def test_group_by_agg(df):
    rows = df.groupBy("g").agg(F.max_by("x", "y").alias("mx"), F.min_by("x", "y").alias("mn")).orderBy("g").collect()
    assert [tuple(r) for r in rows] == [("g1", "b", "a"), ("g2", "f", "c")]


def test_rollup(df):
    rows = df.rollup("g").agg(F.max_by("x", "y").alias("mx")).orderBy(F.col("g").asc_nulls_first()).collect()
    assert [tuple(r) for r in rows] == [(None, "b"), ("g1", "b"), ("g2", "f")]


def test_window_rows_between(df):
    w = Window.partitionBy("g").orderBy("i").rowsBetween(-1, 0)
    rows = (
        df.select("i", F.max_by("x", "y").over(w).alias("mx"), F.min_by("x", "y").over(w).alias("mn"))
        .orderBy("i")
        .collect()
    )
    assert [tuple(r) for r in rows] == [
        (1, "a", "a"),
        (2, "b", "a"),
        (3, "c", "c"),
        (4, "c", "c"),
        (5, "b", None),
        (6, "f", "f"),
    ]


@requires_top_k
def test_top_k_agg(df):
    [row] = df.agg(F.max_by("x", "y", 2).alias("mx"), F.min_by("x", "y", 2).alias("mn")).collect()
    assert (row["mx"], row["mn"]) == (["b", None], ["a", "c"])


@requires_top_k
def test_top_k_window_range_between(df):
    w = Window.orderBy("i").rangeBetween(-2, 0)
    rows = (
        df.select("i", F.max_by("x", "y", 2).over(w).alias("mx"), F.min_by("x", "y", 2).over(w).alias("mn"))
        .orderBy("i")
        .collect()
    )
    assert [tuple(r) for r in rows] == [
        (1, ["a"], ["a"]),
        (2, ["b", "a"], ["a", "b"]),
        (3, ["b", "c"], ["a", "c"]),
        (4, ["b", "c"], ["c", "b"]),
        (5, [None, "c"], ["c", None]),
        (6, [None, "f"], ["f", None]),
    ]


@requires_top_k
def test_default_column_names_and_schema(df):
    result = df.agg(F.max_by("x", "y"), F.min_by("x", "y", 2))
    assert result.columns == ["max_by(x, y)", "min_by(x, y, 2)"]
    assert result.schema.simpleString() == "struct<max_by(x, y):string,min_by(x, y, 2):array<string>>"
    assert [f.nullable for f in result.schema.fields] == [True, True]
