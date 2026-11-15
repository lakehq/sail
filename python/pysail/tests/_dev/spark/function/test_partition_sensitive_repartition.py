"""Tests for the partitions of operators whose output depends on the partition of each row.

Spark computes monotonically increasing IDs, partition IDs and random functions with a seed over
the partitions of their input, so a filter or projection between the input and these operators
must not repartition the rows. The inputs are large enough for the filter to be parallelized.
The expected values were verified on JVM Spark 4.2.0.
"""

import re

import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param(
            "SELECT * FROM (SELECT id, rand(42) AS r, randn(7) AS n, monotonically_increasing_id() AS m, "
            "spark_partition_id() AS p FROM range(0, 200000, 1, 2) WHERE id % 3 = 0) "
            "WHERE id IN (0, 3, 99999, 100002, 100005, 199998) ORDER BY id",
            [
                (0, 0.619189370225301, -1.773755556110447, 0, 0),
                (3, 0.5096018842446481, -0.32537852999017897, 1, 0),
                (99999, 0.25663271151027145, 0.13352556333393895, 33333, 0),
                (100002, 0.8017532427858894, -0.12108592338857035, 8589934592, 1),
                (100005, 0.6565552949992319, 1.7346662488921807, 8589934593, 1),
                (199998, 0.2960652389884153, 1.1881358062111675, 8589967924, 1),
            ],
            id="filter",
        ),
        pytest.param(
            "SELECT id, r FROM (SELECT id + 1 AS id, rand(42) AS r FROM range(0, 200000, 1, 2) WHERE id % 2 = 0) "
            "WHERE id IN (1, 3, 99999, 100001, 100003, 199999) ORDER BY id",
            [
                (1, 0.619189370225301),
                (3, 0.5096018842446481),
                (99999, 0.23965525928543419),
                (100001, 0.8017532427858894),
                (100003, 0.6565552949992319),
                (199999, 0.5318277829094208),
            ],
            id="filter-and-projection",
        ),
        pytest.param(
            "SELECT id, spark_partition_id() AS p FROM "
            "(SELECT id FROM range(0, 12, 1, 3) WHERE id % 2 = 1 AND rand(7) < 0.8) ORDER BY id",
            [(1, 0), (3, 0), (5, 1), (7, 1), (9, 2), (11, 2)],
            id="sorted-partition-id-over-seeded-filter",
        ),
        pytest.param(
            "SELECT id, monotonically_increasing_id() AS m FROM "
            "(SELECT id FROM range(0, 12, 1, 3) WHERE id % 2 = 1 AND rand(7) < 0.8) ORDER BY id",
            [(1, 0), (3, 1), (5, 8589934592), (7, 8589934593), (9, 17179869184), (11, 17179869185)],
            id="sorted-monotonic-id-over-seeded-filter",
        ),
    ],
)
def test_partition_sensitive_functions_over_filters(spark, query, expected):
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.spark(skip_if_jvm=True, reason="Inspects the Sail plan")
def test_no_round_robin_repartition_below_partition_sensitive_operators(spark):
    df = spark.sql(
        "SELECT id, rand(42) AS r, monotonically_increasing_id() AS m, spark_partition_id() AS p "
        "FROM range(0, 200000, 1, 2) WHERE id % 3 = 0"
    )
    lines = df._explain_string().splitlines()  # noqa: SLF001
    sensitive = [
        i for i, line in enumerate(lines) if re.search(r"(SeededRandomExec|MonotonicIdExec|SparkPartitionIdExec)", line)
    ]
    source = next(i for i, line in enumerate(lines) if "RangeExec" in line)
    assert sensitive, lines
    assert not any("RoundRobinBatch" in line for line in lines[min(sensitive) : source]), lines


def test_monotonically_increasing_id_over_filter_is_deterministic(spark):
    # Spark assigns the IDs within each of the 2 partitions of the range in order, as in
    # `(0, 0), (3, 1), ...` and `(1000002, 8589934592), (1000005, 8589934593), ...`.
    partition_size = 1_000_000
    first_id_in_second_partition = 1_000_002
    expected_rows = 666_667
    df = spark.range(0, 2_000_000, 1, 2).filter("id % 3 = 0").withColumn("m", F.monotonically_increasing_id())
    expected = F.when(F.col("id") < partition_size, F.col("id") / 3).otherwise(
        F.lit(1 << 33) + (F.col("id") - first_id_in_second_partition) / 3
    )
    for _ in range(3):
        assert df.where(F.col("m") != expected).count() == 0
        assert df.count() == expected_rows
