import pytest
from pandas.testing import assert_frame_equal

from pysail.tests.spark.utils import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


def test_cache_persists_data(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
    df.cache()

    result1 = df.toPandas()
    result2 = df.toPandas()

    assert_frame_equal(result1, result2)


def test_cache_with_transformation(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["n"])
    cached = df.filter("n > 1").cache()

    result1 = cached.sort("n").toPandas()
    result2 = cached.sort("n").toPandas()

    assert_frame_equal(result1, result2)
    assert len(result1) == 2  # noqa: PLR2004


def test_cache_then_aggregate(spark):
    # groupBy/agg requires a shuffle exchange after reading from cache.
    df = spark.createDataFrame([(1, "x"), (2, "x"), (3, "y"), (4, "y")], ["n", "group"])
    cached = df.filter("n > 0").cache()

    result = cached.groupBy("group").sum("n").sort("group").toPandas()

    assert list(result["group"]) == ["x", "y"]
    assert list(result["sum(n)"]) == [3, 7]


def test_cache_then_join(spark):
    # join forces a shuffle exchange on the cached left side.
    left = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
    left.cache()

    right = spark.createDataFrame([(1, 10), (2, 20)], ["id", "score"])
    result = left.join(right, on="id").sort("id").toPandas()

    assert list(result["id"]) == [1, 2]
    assert list(result["val"]) == ["a", "b"]
    assert list(result["score"]) == [10, 20]

