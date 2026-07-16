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

    assert "RepartitionExec: partitioning=RoundRobinBatch(5)" in round_robin_plan
    assert "RepartitionExec: partitioning=RoundRobinBatch(1)" in repartition_one_plan
    assert "RepartitionExec: partitioning=Hash([" in hash_plan


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_explicit_repartition_pushes_column_projection_down_plan(spark, snapshot):
    df1 = spark.sql("SELECT id AS id1 FROM range(6)")
    df2 = spark.sql("SELECT id AS id2 FROM range(6)")
    df3 = spark.sql("SELECT id AS id3 FROM range(6)")
    df = df1.join(df2, df1.id1 == df2.id2).join(df3, df1.id1 == df3.id3).repartition(3).select("id1", "id2")
    plan = normalized_plan(df)

    assert plan == snapshot


def test_explicit_repartition_pushes_column_projection_down_result(spark):
    df1 = spark.sql("SELECT id AS id1 FROM range(6)")
    df2 = spark.sql("SELECT id AS id2 FROM range(6)")
    df3 = spark.sql("SELECT id AS id3 FROM range(6)")
    df = (
        df1.join(df2, df1.id1 == df2.id2)
        .join(df3, df1.id1 == df3.id3)
        .repartition(3)
        .select("id1", "id2")
        .orderBy("id1", "id2")
    )
    result = df.collect()

    assert [(row["id1"], row["id2"]) for row in result] == [
        (0, 0),
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5),
    ]


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_explicit_repartition_hash_partitioning_remaps_after_projection_pushdown_plan(spark, snapshot):
    df1 = spark.sql("SELECT id AS id1 FROM range(6)")
    df2 = spark.sql("SELECT id AS id2 FROM range(6)")
    df3 = spark.sql("SELECT id AS id3 FROM range(6)")
    df = (
        df1.join(df2, df1.id1 == df2.id2)
        .join(df3, df1.id1 == df3.id3)
        .repartition(3, "id1", "id2")
        .select("id1", "id2")
    )
    plan = normalized_plan(df)

    assert plan == snapshot


@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_explicit_repartition_does_not_push_filter_down_plan(spark, snapshot):
    df = spark.range(6).repartition(3).filter(F.col("id") % 2 == 0)
    plan = normalized_plan(df)

    assert plan == snapshot


def test_explicit_coalesce(spark):
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(1)) == 1
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(2)) == 2  # noqa: PLR2004
    assert partition_count(spark.range(0, 10, 1, 2).coalesce(3)) == 2  # noqa: PLR2004
    assert partition_count(spark.range(0, 10, 1, 4).coalesce(2)) == 2  # noqa: PLR2004


def test_coalesce_hint(spark):
    df = spark.range(0, 12, 1, 4).select("id", (F.col("id") % 3).alias("group"))

    actual = df.hint("COALESCE", 2).orderBy("id").toPandas()
    expected = df.orderBy("id").toPandas()

    assert partition_count(df.hint("COALESCE", 2)) == 2  # noqa: PLR2004
    assert partition_count(df.hint("COALESCE", 6)) == 4  # noqa: PLR2004
    assert_frame_equal(actual, expected)


def test_coalesce_hint_rejects_zero_partitions(spark):
    with pytest.raises(Exception, match="COALESCE hint requires at least one partition"):
        partition_count(spark.range(0, 10, 1, 2).hint("COALESCE", 0))


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
