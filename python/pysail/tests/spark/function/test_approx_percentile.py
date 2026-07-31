"""DataFrame-API tests for `percentile_approx` / `approx_percentile`.

These cover the parts of the Greenwald-Khanna sketch that SQL alone cannot
reach, because they depend on how the input is *partitioned* — which is what
drives `Accumulator::merge_batch`. The BDD scenarios in
`features/approx_percentile.feature` cover everything else.
"""

import math

import pyspark.sql.functions as F  # noqa: N812
import pytest

pytestmark = pytest.mark.approx_percentile

# `accuracy` sets the sketch's relative error to `1 / accuracy`, so a quantile
# may be off by at most `n / accuracy` ranks.
ACCURACY = 100
ROWS = 100_000

# Rows surviving the filter in the sparse case, small enough that the sketch is
# lossless and the nearest rank is exact.
SPARSE_ROWS = 50
SPARSE_PARTITIONS = 16


def _median(df):
    return df.agg(F.expr(f"percentile_approx(id, 0.5, {ACCURACY})").alias("r")).collect()[0]["r"]


def test_merge_across_partitions_stays_within_the_error_bound(spark):
    """Every partitioning must respect the Greenwald-Khanna guarantee.

    The sketch is partition-dependent by design — Spark itself returns a
    different quantile for the same query at a different partitioning — so this
    asserts the error bound rather than an exact value. A mis-adjusted `delta`
    in `merge` shows up here as drift outside the bound.
    """
    df = spark.range(0, ROWS)
    bound = ROWS / ACCURACY
    true_median = ROWS // 2

    single = _median(df.coalesce(1))
    spread = _median(df.repartition(8))

    assert abs(single - true_median) <= bound, f"single-partition median {single} outside +-{bound}"
    assert abs(spread - true_median) <= bound, f"8-partition median {spread} outside +-{bound}"


def test_merge_skips_empty_partitions(spark):
    """Partitions that contribute no rows must not perturb the result.

    Filtering after a repartition leaves most partitions empty, so the final
    aggregate merges a mix of populated and empty sketches.
    """
    sparse = spark.range(0, 1000).repartition(SPARSE_PARTITIONS).filter(F.col("id") < SPARSE_ROWS)

    # Nearest rank over 0..SPARSE_ROWS-1: ceil(0.5 * 50) = 25 -> the 25th smallest -> 24.
    expected = math.ceil(0.5 * SPARSE_ROWS) - 1
    assert sparse.agg(F.expr("percentile_approx(id, 0.5)").alias("r")).collect()[0]["r"] == expected


def test_all_partitions_empty_returns_null(spark):
    empty = spark.range(0, 1000).repartition(8).filter(F.col("id") < 0)

    assert empty.agg(F.expr("percentile_approx(id, 0.5)").alias("r")).collect()[0]["r"] is None
