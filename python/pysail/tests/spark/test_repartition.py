import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark


def partition_count(df):
    def counter(_iterator):
        yield pd.DataFrame({"n": [1]})

    return df.mapInPandas(counter, schema="n: long").count()


def input_partition_groups(df):
    def counter(iterator):
        input_pids = set()
        for pdf in iterator:
            input_pids.update(pdf["input_pid"].tolist())
        yield pd.DataFrame({"input_pids": [",".join(str(pid) for pid in sorted(input_pids))]})

    rows = df.mapInPandas(counter, schema="input_pids: string").collect()
    return [set() if row["input_pids"] == "" else {int(pid) for pid in row["input_pids"].split(",")} for row in rows]


def normalized_plan(df):
    return normalize_plan_text(df._explain_string())  # noqa: SLF001


def test_explicit_repartition(spark):
    assert partition_count(spark.range(0, 10, 1, 2)) == 2  # noqa: PLR2004
    assert (
        partition_count(spark.range(0, 10, 1, 3).select("id", F.lit("foo").alias("a")).repartition("a")) == 3  # noqa: PLR2004
    )
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition(5)) == 5  # noqa: PLR2004
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition(6, "b", "a")) == 6  # noqa: PLR2004
    assert partition_count(spark.sql("SELECT 1 AS a, 'foo' as b").repartition("a")) == 1


def test_explicit_repartition_spreads_identical_rows(spark):
    partition_ids = {
        row["pid"]
        for row in spark.range(0, 64, 1, 1)
        .select(F.lit("same").alias("value"))
        .repartition(4)
        .selectExpr("spark_partition_id() AS pid")
        .distinct()
        .collect()
    }

    assert partition_ids == set(range(4))


@pytest.mark.skipif(is_jvm_spark(), reason="Sail only")
def test_repartition_assigns_rows_round_robin(spark):
    df = spark.sql("SELECT id FROM range(0, 6, 1, 1)")
    repartitioned = df.repartition(2)
    repartitioned.createOrReplaceTempView("repartitioned_rows")

    result = spark.sql(
        """
        WITH repartitioned AS (
          SELECT id, spark_partition_id() AS pid
          FROM repartitioned_rows
        )
        SELECT id, DENSE_RANK() OVER (ORDER BY pid) - 1 AS pid
        FROM repartitioned
        ORDER BY id
    """
    ).collect()

    assert [(row["id"], row["pid"]) for row in result] == [
        (0, 0),
        (1, 1),
        (2, 0),
        (3, 1),
        (4, 0),
        (5, 1),
    ]


def test_explicit_repartition_plan_shape_uses_expected_physical_nodes(spark):
    round_robin_plan = normalized_plan(spark.sql("SELECT 1 AS a, 'foo' AS b").repartition(5))
    repartition_one_plan = normalized_plan(spark.range(0, 8, 1, 2).repartition(1))
    hash_plan = normalized_plan(
        spark.range(0, 8, 1, 2).select("id", (F.col("id") % 2).alias("group")).repartition(1, "group")
    )

    assert "ExplicitRepartitionExec" in round_robin_plan
    assert "ExplicitRepartitionExec" in repartition_one_plan
    assert "RepartitionExec: partitioning=Hash([" in hash_plan


def test_explicit_coalesce(spark):
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(1)) == 1
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(2)) == 2  # noqa: PLR2004
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(3)) == 2  # noqa: PLR2004
    assert partition_count(spark.range(0, 10, 1, 4).coalesce(2)) == 2  # noqa: PLR2004


def test_explicit_coalesce_preserves_rows(spark):
    row_count = 12
    partition_count_before = 4

    df = spark.range(0, row_count, 1, partition_count_before).select(
        "id",
        (F.col("id") % 3).alias("group"),
        (F.col("id") % 2 == 0).alias("is_even"),
    )

    actual = df.coalesce(2).orderBy("id").toPandas()
    expected = df.orderBy("id").toPandas()

    assert_frame_equal(actual, expected)


def test_explicit_coalesce_keeps_input_partitions_intact(spark):
    row_count = 48
    input_partition_count = 4
    output_partition_count = 2

    groups = input_partition_groups(
        spark.range(0, row_count, 1, input_partition_count)
        .selectExpr("spark_partition_id() AS input_pid")
        .coalesce(output_partition_count)
    )

    assert len(groups) == output_partition_count
    assert all(group for group in groups)
    assert {pid for group in groups for pid in group} == set(range(input_partition_count))
    assert sum(len(group) for group in groups) == input_partition_count


def test_explicit_coalesce_after_filter_and_projection(spark):
    row_count = 12
    partition_count_before = 4
    start_value = 4

    df = (
        spark.range(0, row_count, 1, partition_count_before)
        .filter(F.col("id") >= start_value)
        .select(F.col("id").alias("value"), (F.col("id") * 2).alias("double_value"))
    )

    actual = df.coalesce(1).orderBy("value").toPandas()
    expected = pd.DataFrame(
        {
            "value": list(range(start_value, row_count)),
            "double_value": [value * 2 for value in range(start_value, row_count)],
        }
    )

    assert partition_count(df.coalesce(1)) == 1
    assert_frame_equal(actual, expected)
