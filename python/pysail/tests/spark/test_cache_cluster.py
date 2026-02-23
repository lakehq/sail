import os

import pytest
from pandas.testing import assert_frame_equal

from pysail.spark import SparkConnectServer
from pysail.tests.spark.utils import is_jvm_spark


@pytest.fixture(scope="session")
def remote():
    """Override the remote fixture to run in local-cluster mode with multiple workers."""
    original_mode = os.environ.get("SAIL_MODE")
    os.environ["SAIL_MODE"] = "local-cluster"

    try:
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        yield f"sc://localhost:{port}"
        server.stop()
    finally:
        if original_mode is None:
            os.environ.pop("SAIL_MODE", None)
        else:
            os.environ["SAIL_MODE"] = original_mode


@pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")
class TestCacheCluster:
    def test_cache_persists_data(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
        df.cache()

        result1 = df.sort("id").toPandas()
        result2 = df.sort("id").toPandas()

        assert_frame_equal(result1, result2)

    def test_cache_with_transformation(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["n"])
        cached = df.filter("n > 1").cache()

        result1 = cached.sort("n").toPandas()
        result2 = cached.sort("n").toPandas()

        assert_frame_equal(result1, result2)
        assert len(result1) == 2  # noqa: PLR2004

    def test_cache_then_aggregate(self, spark):
        # groupBy/agg requires a shuffle exchange after reading from cache.
        df = spark.createDataFrame([(1, "x"), (2, "x"), (3, "y"), (4, "y")], ["n", "group"])
        cached = df.filter("n > 0").cache()

        result = cached.groupBy("group").sum("n").sort("group").toPandas()

        assert list(result["group"]) == ["x", "y"]
        assert list(result["sum(n)"]) == [3, 7]

    def test_cache_then_join(self, spark):
        # join forces a shuffle exchange on the cached left side.
        left = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
        left.cache()

        right = spark.createDataFrame([(1, 10), (2, 20)], ["id", "score"])
        result = left.join(right, on="id").sort("id").toPandas()

        assert list(result["id"]) == [1, 2]
        assert list(result["val"]) == ["a", "b"]
        assert list(result["score"]) == [10, 20]
