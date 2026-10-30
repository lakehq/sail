import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import Row


def test_percentile_approx_with_an_array_of_percentages(spark):
    df = spark.range(0, 100, 1, 1)
    actual = df.select(F.percentile_approx("id", [0.25, 0.5, 0.75], 1000000).alias("q"))
    assert actual.schema.simpleString() == "struct<q:array<bigint>>"
    assert actual.schema["q"].dataType.containsNull is False
    assert actual.collect() == [Row(q=[24, 49, 74])]


def test_percentile_approx_with_a_percentage(spark):
    df = spark.range(0, 100, 1, 1).select((F.col("id") % 3).alias("key"), (F.col("id") * 1.5).alias("value"))
    actual = df.groupBy("key").agg(F.percentile_approx("value", F.lit(0.5), F.lit(1000000)).alias("median"))
    assert actual.schema.simpleString() == "struct<key:bigint,median:double>"
    assert sorted(actual.collect()) == [Row(key=0, median=72.0), Row(key=1, median=73.5), Row(key=2, median=75.0)]


def test_approx_count_distinct_on_floating_point_values(spark):
    df = spark.createDataFrame([(float("nan"),), (None,), (2.1,), (0.5,), (0.0,), (-0.0,)], "b double")
    assert df.select(F.approx_count_distinct("b").alias("n")).collect() == [Row(n=4)]
    df = spark.createDataFrame([(1.5,), (1.5,), (2.5,)], "b float")
    assert df.select(F.approx_count_distinct("b", 0.1).alias("n")).collect() == [Row(n=2)]
