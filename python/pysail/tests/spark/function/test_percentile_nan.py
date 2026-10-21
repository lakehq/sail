import pyspark.sql.functions as F  # noqa: N812
import pytest

NAN = float("nan")


def is_nan(value):
    return value is not None and value != value  # noqa: PLR0124


def test_median_treats_nan_as_the_largest_value(spark):
    df = spark.createDataFrame([(NAN,), (None,), (2.1,), (0.5,)], "v double")
    [row] = df.select(F.median("v").alias("median")).collect()
    assert row.median == pytest.approx(2.1)


def test_percentile_treats_nan_as_the_largest_value(spark):
    df = spark.createDataFrame([(0.5,), (NAN,), (2.1,), (3.0,)], "v double")
    [row] = df.select(
        F.percentile("v", 0.5).alias("p50"),
        F.percentile("v", 1.0).alias("p100"),
        F.percentile("v", 0.0).alias("p0"),
    ).collect()
    assert row.p50 == pytest.approx(2.55)
    assert is_nan(row.p100)
    assert row.p0 == pytest.approx(0.5)


def test_percentiles_of_only_nan(spark):
    df = spark.createDataFrame([(NAN,), (NAN,)], "v double")
    [row] = df.select(F.median("v").alias("median")).collect()
    assert is_nan(row.median)


def test_median_by_group_with_nan(spark):
    df = spark.createDataFrame([(1, NAN), (1, 1.0), (1, 5.0), (2, 4.0), (2, NAN)], "k int, v double")
    rows = {row.k: row.median for row in df.groupBy("k").agg(F.median("v").alias("median")).collect()}
    assert rows[1] == pytest.approx(5.0)
    assert is_nan(rows[2])
