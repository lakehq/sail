from collections import Counter

import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

jvm_spark_only = pytest.mark.skipif(
    not is_jvm_spark(),
    reason="Seeded sampling RNG semantics differ from Spark",
)


def _sample_with_bounds(spark, lower_bound, upper_bound, *, with_replacement=True):
    sampled = spark.range(10, numPartitions=1).sample(with_replacement, 0.0, 1)
    sampled._plan.lower_bound = lower_bound  # noqa: SLF001
    sampled._plan.upper_bound = upper_bound  # noqa: SLF001
    return sampled


@pytest.fixture
def single_partition_spark(spark):
    if is_jvm_spark():
        yield spark
        return

    envs = {
        "SAIL_EXECUTION__BATCH_SIZE": "1024",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "1",
    }
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        yield sessions.create()


@jvm_spark_only
def test_dataframe_sample_with_replacement_matches_spark_seed(spark):
    sampled = spark.range(10, numPartitions=1).sample(True, 0.5, 1)
    assert [row.id for row in sampled.collect()] == [0, 2, 3, 4, 7, 9]


@pytest.mark.parametrize(
    ("with_replacement", "expected"),
    [
        (False, [[2, 3], [6, 7, 8]]),
        pytest.param(True, [[0, 2, 3, 4], []], marks=jvm_spark_only),
    ],
)
def test_seeded_sample_matches_spark_partition_seeds(
    spark,
    with_replacement,
    expected,
):
    sampled = spark.range(10, numPartitions=2).sample(with_replacement, 0.5, 1)
    rows = sampled.select(F.spark_partition_id().alias("partition"), "id").collect()
    actual = [[], []]
    for row in rows:
        actual[row.partition].append(row.id)
    assert actual == expected


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


@pytest.mark.parametrize("with_replacement", [False, pytest.param(True, marks=jvm_spark_only)])
def test_seeded_sample_pattern_differs_across_batches(
    single_partition_spark,
    with_replacement,
):
    sampled = single_partition_spark.range(2048, numPartitions=1).sample(
        with_replacement,
        0.5,
        42,
    )
    picked = [row.id for row in sampled.collect()]
    first_batch = Counter(i for i in picked if i < 1024)  # noqa: PLR2004
    second_batch = Counter(i - 1024 for i in picked if i >= 1024)  # noqa: PLR2004
    assert first_batch != second_batch


# The expected values are from Spark 3.5.9.
def test_random_split_matches_spark_partition_seeds(spark):
    df = spark.range(0, 20, 1, 2)
    splits = [sorted(row.id for row in split.collect()) for split in df.randomSplit([1.0, 2.0, 3.0], 2)]
    assert splits == [
        [9, 14],
        [1, 2, 3, 5, 6, 7, 10, 15, 18],
        [0, 4, 8, 11, 12, 13, 16, 17, 19],
    ]


def test_sample_by_matches_spark_partition_seeds(spark):
    df = spark.range(0, 100, 1, 4).selectExpr("id", "id % 3 AS key")
    sampled = df.sampleBy("key", fractions={0: 0.1, 1: 0.2}, seed=0)
    assert sorted(row.id for row in sampled.collect()) == [9, 16, 22, 28, 43, 45, 49, 72, 79]
