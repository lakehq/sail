import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


def _sample_with_bounds(spark, lower_bound, upper_bound, *, with_replacement=True):
    sampled = spark.range(10, numPartitions=1).sample(with_replacement, 0.0, 1)
    sampled._plan.lower_bound = lower_bound  # noqa: SLF001
    sampled._plan.upper_bound = upper_bound  # noqa: SLF001
    return sampled


def test_dataframe_sample_replazement_seed(spark):
    df = spark.createDataFrame([(0), (1), (2), (3), (4), (5), (6), (7), (8), (9)], ["id"])
    df2 = df.sample(True, 0.5, 1)

    assert_frame_equal(
        df2.toPandas(),
        pd.DataFrame({"id": [0, 0, 3, 4, 8]}),
    )


def test_dataframe_sample_with_replacement_zero_fraction(spark):
    sampled = spark.range(10, numPartitions=1).sample(True, 1e-12, 1)
    assert sampled.collect() == []

    sampled = spark.range(10, numPartitions=1).sample(True, 0.0, 1)
    assert sampled.collect() == []


def test_dataframe_sample_with_replacement_uses_bound_difference(spark):
    sampled = _sample_with_bounds(spark, 2.0, 2.0)
    assert sampled.collect() == []


def test_dataframe_sample_accepts_spark_rounding_tolerance(spark):
    sampled = _sample_with_bounds(spark, 0.5000005, 0.5)
    assert sampled.collect() == []

    sampled = _sample_with_bounds(spark, -0.0000005, 0.0, with_replacement=False)
    assert sampled.collect() == []

    sampled = _sample_with_bounds(
        spark,
        0.0,
        1.0000005,
        with_replacement=False,
    )
    assert sampled.count() == 10  # noqa: PLR2004


def test_dataframe_sample_rejects_fraction_beyond_spark_rounding_tolerance(spark):
    with pytest.raises(
        Exception,
        match=r"Sampling fraction .* must be nonnegative with replacement",
    ):
        _sample_with_bounds(spark, 0.500002, 0.5).collect()

    with pytest.raises(
        Exception,
        match=r"Sampling fraction .* must be on interval \[0, 1\] without replacement",
    ):
        _sample_with_bounds(
            spark,
            0.0,
            1.000002,
            with_replacement=False,
        ).collect()


def test_dataframe_sample_rejects_individual_bounds(spark):
    with pytest.raises(
        Exception,
        match=r"Lower bound .* must be <= upper bound .*",
    ):
        # The fraction rounds to exactly -epsilon, but Spark still rejects the bounds.
        _sample_with_bounds(
            spark,
            5e-324,
            -0.000001,
            with_replacement=False,
        ).collect()

    with pytest.raises(
        Exception,
        match=r"Lower bound .* must be >= 0\.0",
    ):
        _sample_with_bounds(spark, -0.000002, -0.000002, with_replacement=False).collect()

    with pytest.raises(
        Exception,
        match=r"Upper bound .* must be <= 1\.0",
    ):
        _sample_with_bounds(spark, 1.000002, 1.000002, with_replacement=False).collect()


def test_dataframe_sample_error_formats_doubles_like_scala(spark):
    with pytest.raises(
        Exception,
        match=r"Sampling fraction \(2\.0\) must be on interval \[0, 1\] without replacement",
    ):
        _sample_with_bounds(spark, 0.0, 2.0, with_replacement=False).collect()

    with pytest.raises(
        Exception,
        match=r"Sampling fraction \(-1\.0\) must be nonnegative with replacement",
    ):
        _sample_with_bounds(spark, 1.0, 0.0).collect()

    with pytest.raises(
        Exception,
        match=r"Lower bound \(-2\.0E-6\) must be >= 0\.0",
    ):
        _sample_with_bounds(spark, -0.000002, -0.000002, with_replacement=False).collect()


@pytest.mark.xfail(reason="Known issue: seeded sampling repeats its pattern every batch", strict=True)
def test_seeded_sample_pattern_differs_across_batches(spark):
    sampled = spark.range(16384, numPartitions=1).sample(True, 0.5, 42)
    picked = [row.id for row in sampled.collect()]
    first_batch = {i for i in picked if i < 8192}  # noqa: PLR2004
    second_batch = {i - 8192 for i in picked if i >= 8192}  # noqa: PLR2004
    assert first_batch != second_batch
