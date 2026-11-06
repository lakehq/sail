"""Column metadata of expression results.

Like Spark, the result of an expression that does not refer to a column directly (an aggregate
or `coalesce`) has no column metadata, while a grouping key keeps the metadata of its column.
"""

import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.fixture
def df(spark):
    return spark.createDataFrame([(1, 1), (5, None)], "a long, b long").withMetadata("a", {"comment": "c"})


@pytest.mark.parametrize(
    "column",
    [
        lambda: F.min("a"),
        lambda: F.max("a"),
        lambda: F.count("a"),
        lambda: F.coalesce("a", "b"),
        lambda: F.nvl("a", "b"),
        lambda: F.ifnull("a", "b"),
        lambda: F.nvl2("a", "a", "b"),
        lambda: F.when(F.col("a") > 0, F.col("a")).otherwise(F.col("b")),
    ],
    ids=["min", "max", "count", "coalesce", "nvl", "ifnull", "nvl2", "case_when"],
)
def test_expression_result_has_no_column_metadata(df, column):
    out = df.select(column().alias("x"))
    assert out.schema["x"].metadata == {}


def test_aggregate_values_are_unchanged(df):
    assert tuple(df.select(F.min("a"), F.max("a")).first()) == (1, 5)
    assert [row[0] for row in df.select(F.coalesce("b", "a")).orderBy("a").collect()] == [1, 5]


def test_grouping_key_keeps_column_metadata(df):
    out = df.groupBy("a").agg(F.max("b").alias("m"))
    assert out.schema["a"].metadata == {"comment": "c"}
    assert out.schema["m"].metadata == {}


def test_column_reference_keeps_column_metadata(df):
    assert df.select(F.col("a").alias("x")).schema["x"].metadata == {"comment": "c"}


@pytest.mark.parametrize(
    ("literal", "expected"),
    [
        ("INTERVAL '1' HOUR", "interval hour"),
        ("INTERVAL '1' DAY", "interval day"),
        ("INTERVAL '1' YEAR", "interval year"),
        ("INTERVAL '1-2' YEAR TO MONTH", "interval year to month"),
    ],
)
def test_result_keeps_the_interval_type(spark, literal, expected):
    values = f"VALUES ({literal}) AS t(i)"
    aggregates = spark.sql(f"SELECT max(i) AS m, min(i) AS n FROM {values}")  # noqa: S608
    conditional = spark.sql(f"SELECT coalesce(i, i) AS c FROM {values}")  # noqa: S608
    fields = aggregates.schema.fields + conditional.schema.fields
    assert [field.dataType.simpleString() for field in fields] == [expected] * 3


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ("INTERVAL '1' YEAR", "INTERVAL '2' MONTH", "interval year to month"),
        ("INTERVAL '2' MONTH", "INTERVAL '1' YEAR", "interval year to month"),
        ("INTERVAL '1' HOUR", "INTERVAL '2' MINUTE", "interval hour to minute"),
        ("INTERVAL '1' DAY", "INTERVAL '2' SECOND", "interval day to second"),
    ],
)
def test_coalesce_widens_the_interval_type(spark, first, second, expected):
    out = spark.sql(f"SELECT coalesce(a, b) AS c FROM VALUES ({first}, {second}) AS t(a, b)")  # noqa: S608
    assert out.schema["c"].dataType.simpleString() == expected
