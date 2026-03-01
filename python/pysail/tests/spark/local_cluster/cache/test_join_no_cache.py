import pytest
import pyspark.sql.functions as F  # noqa: N812

from pysail.tests.spark.utils import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


def test_join_with_limit_without_cache(spark):
    """Checks whether CollectLeft join with LIMIT is deterministic without caching."""
    large_range = spark.range(0, 200_000).repartition(32)
    left = large_range.select(
        F.col("id").cast("long").alias("id"),
        F.concat(F.lit("v"), F.col("id").cast("string")).alias("val"),
    )
    right = large_range.limit(1_000).select(
        F.col("id").cast("long").alias("id"),
        (F.col("id") * 10).cast("long").alias("score"),
    )
    joined = left.join(right, on="id")

    c1 = joined.count()
    c2 = joined.count()
    assert c1 == c2 == 1_000  # noqa: PLR2004
