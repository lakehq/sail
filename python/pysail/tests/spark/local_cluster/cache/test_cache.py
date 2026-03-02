import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.tests.spark.utils import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def large_range(spark):
    # Large enough to create meaningful parallel work, but cheap to generate.
    n = 200_000
    partitions = 32
    return spark.range(0, n).repartition(partitions)


def test_cache_persists_data(spark):
    # Force a shuffle boundary and cache the shuffled output.
    df = (
        spark.range(0, 200_000)
        .repartition(32)
        .select(
            F.col("id").cast("long").alias("id"),
            F.concat(F.lit("v"), F.col("id").cast("string")).alias("value"),
        )
        .cache()
    )

    # Avoid collecting large data to the driver; validate via stable aggregates.
    result1 = df.agg(F.sum("id").alias("s"), F.count("*").alias("c")).collect()[0]
    result2 = df.agg(F.sum("id").alias("s"), F.count("*").alias("c")).collect()[0]

    assert result1 == result2
    assert result1["c"] == 200_000  # noqa: PLR2004


def test_cache_with_transformation(large_range):
    lower_bound = 123
    cached = large_range.filter(F.col("id") >= lower_bound).select(F.col("id").alias("n")).cache()

    # Two actions to ensure reuse.
    result1 = cached.agg(
        F.min("n").alias("min"),
        F.max("n").alias("max"),
        F.count("*").alias("c"),
    ).collect()[0]
    result2 = cached.agg(
        F.min("n").alias("min"),
        F.max("n").alias("max"),
        F.count("*").alias("c"),
    ).collect()[0]

    assert result1 == result2
    assert result1["min"] == lower_bound


def test_cache_then_aggregate(large_range):
    # groupBy/agg forces a shuffle exchange after reading from cache.
    cached = large_range.select(
        (F.col("id") % F.lit(10)).cast("int").alias("group"),
        F.col("id").cast("long").alias("n"),
    ).cache()

    agg = cached.groupBy("group").agg(F.sum("n").alias("sum_n"))
    # Keep results small (10 rows) while still exercising shuffle.
    result = agg.orderBy("group").collect()

    assert len(result) == 10  # noqa: PLR2004
    total = agg.agg(F.sum("sum_n").alias("total")).collect()[0]["total"]
    expected_total = cached.agg(F.sum("n").alias("total")).collect()[0]["total"]
    assert total == expected_total
