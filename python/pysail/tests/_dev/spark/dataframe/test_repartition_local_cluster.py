# SPDX-License-Identifier: Apache-2.0

"""`repartitionByRange` on cluster workers.

The split points come from sample stages of the same job as the shuffle, which run
on the workers, and the range channels cross the distributed codec. The expected partitions match JVM Spark,
where the sample covers every row.
"""

from collections import Counter

import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.tests._dev.spark.dataframe.test_repartition import NESTED_KEYS, spark_order

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local-cluster mode")


@pytest.fixture(scope="module")
def local_cluster_spark():
    with (
        spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        yield sessions.create()


@pytest.mark.parametrize("descending", [False, True])
def test_range_repartition_assigns_key_ranges(local_cluster_spark, descending):
    source = local_cluster_spark.range(0, 36, 1, 4).withColumn("key", F.col("id") % 12)
    key = F.col("key").desc() if descending else F.col("key").asc()
    rows = source.repartitionByRange(3, key).select("key", F.spark_partition_id().alias("pid")).collect()
    for row in rows:
        expected = (11 - row.key) // 4 if descending else row.key // 4
        assert row.pid == expected, row
    assert len(rows) == 36


def partition_rows(df):
    """The number of rows of each partition, including the empty ones, in ascending order."""

    def counter(iterator):
        yield pd.DataFrame({"n": [sum(len(pdf) for pdf in iterator)]})

    return sorted(row.n for row in df.mapInPandas(counter, schema="n: long").collect())


@pytest.mark.parametrize(("rows", "expected"), [(0, [0]), (24, [0, 24])], ids=["empty", "constant"])
def test_range_repartition_has_a_partition_per_spark_bound_and_one_more(local_cluster_spark, rows, expected):
    # The driver reads the split points before it plans the tasks that read the shuffle.
    source = local_cluster_spark.range(0, rows, 1, 1).selectExpr("1 AS k")
    assert partition_rows(source.repartitionByRange(8, "k")) == expected


def test_coalesce_does_not_add_partitions_to_a_range_repartition(local_cluster_spark):
    source = local_cluster_spark.range(0, 24, 1, 1).selectExpr("1 AS k")
    assert partition_rows(source.repartitionByRange(8, "k").coalesce(4)) == [0, 24]


def test_nested_range_repartition_has_a_partition_per_spark_bound(local_cluster_spark):
    # The outer split points read the output of the inner range shuffle.
    source = local_cluster_spark.range(0, 24, 1, 1).selectExpr("id", "1 AS k")
    assert partition_rows(source.repartitionByRange(8, "id").repartitionByRange(6, "k")) == [0, 24]


def test_range_repartition_of_a_shared_cte_has_a_partition_per_spark_bound(local_cluster_spark):
    # The CTE runs before the split points and keeps its output for the shuffle.
    df = local_cluster_spark.sql(
        "WITH t AS (SELECT 1 AS k FROM range(24)) "
        "SELECT /*+ REPARTITION_BY_RANGE(8, k) */ k FROM t WHERE k IN (SELECT k FROM t)"
    )
    assert partition_rows(df) == [0, 24]


def failing_key(_value):
    msg = "range key computed"
    raise ValueError(msg)


def test_range_repartition_samples_only_when_the_query_runs(local_cluster_spark):
    key = F.udf(failing_key, "long")("id")
    df = local_cluster_spark.range(12).withColumn("key", key).repartitionByRange(3, "key")
    df._explain_string(extended=True)  # noqa: SLF001
    with pytest.raises(Exception, match="range key computed"):
        df.collect()


@pytest.mark.parametrize("key", NESTED_KEYS.values(), ids=NESTED_KEYS.keys())
@pytest.mark.parametrize("descending", [False, True], ids=["asc", "desc"])
def test_range_repartition_orders_nested_keys_like_spark(local_cluster_spark, key, descending):
    # The workers encode the nested keys for the sample and the shuffle.
    sql, value = key
    order = F.col("k").desc() if descending else F.col("k").asc()
    rows = (
        local_cluster_spark.range(12)
        .selectExpr("id", f"{sql} AS k")
        .repartitionByRange(3, order)
        .select("id", F.spark_partition_id().alias("pid"))
        .collect()
    )
    ranked = sorted(range(12), key=lambda i: spark_order(value(i)), reverse=descending)
    assert {row.id: row.pid for row in rows} == {i: rank // 4 for rank, i in enumerate(ranked)}


def test_nested_range_repartitions_assign_key_ranges(local_cluster_spark):
    source = local_cluster_spark.range(0, 36, 1, 4).withColumn("key", F.col("id") % 12)
    # The projection keeps the two repartitions from collapsing into one.
    inner = source.repartitionByRange(3, "key").withColumn("inner_pid", F.spark_partition_id())
    rows = inner.repartitionByRange(2, F.col("key").desc()).withColumn("pid", F.spark_partition_id()).collect()
    for row in rows:
        assert row.inner_pid == row.key // 4, row
        assert row.pid == (11 - row.key) // 6, row
    assert len(rows) == 36


def test_three_nested_range_repartitions_assign_key_ranges(local_cluster_spark):
    # Each sample has copies of the range repartitionings inside it, which read the
    # split points of one sample.
    source = local_cluster_spark.range(0, 36, 1, 4).withColumn("key", F.col("id") % 12)
    first = source.repartitionByRange(3, "key").withColumn("first_pid", F.spark_partition_id())
    second = first.repartitionByRange(2, F.col("key").desc()).withColumn("second_pid", F.spark_partition_id())
    rows = second.repartitionByRange(4, "key").withColumn("pid", F.spark_partition_id()).collect()
    for row in rows:
        assert row.first_pid == row.key // 4, row
        assert row.second_pid == (11 - row.key) // 6, row
        assert row.pid == row.key // 3, row
    assert len(rows) == 36


def test_range_repartition_samples_input_with_scalar_subquery(local_cluster_spark):
    rows = local_cluster_spark.sql(
        "SELECT id, spark_partition_id() AS pid FROM ("
        "SELECT /*+ REPARTITION_BY_RANGE(3, id) */ id FROM range(12) "
        "WHERE id < (SELECT max(id) + 1 FROM range(12)))"
    ).collect()
    assert sorted((row.id, row.pid) for row in rows) == [(i, i // 4) for i in range(12)]


def test_range_repartition_explains_the_distributed_plan(local_cluster_spark):
    # Sail prints the distributed plan in the codegen mode.
    plan = local_cluster_spark.range(12).repartitionByRange(3, "id")._explain_string(mode="codegen")  # noqa: SLF001
    assert "Distributed plan error" not in plan, plan
    assert "RangeSplitPointsExec" in plan, plan
    assert "distribution=Channel(channels=3)" in plan, plan


def test_range_repartition_inside_scalar_subquery(local_cluster_spark):
    rows = local_cluster_spark.sql(
        "SELECT id, spark_partition_id() AS pid FROM ("
        "SELECT /*+ REPARTITION_BY_RANGE(3, id) */ id FROM range(12) "
        "WHERE id < (SELECT max(id) + 1 FROM (SELECT /*+ REPARTITION_BY_RANGE(2, id) */ id FROM range(12))))"
    ).collect()
    assert sorted((row.id, row.pid) for row in rows) == [(i, i // 4) for i in range(12)]


def test_range_repartition_samples_imbalanced_input_again(local_cluster_spark):
    # Of 201 input partitions only one has rows, so the first sample keeps one key
    # of it. The partition is sampled again in proportion to its rows.
    source = local_cluster_spark.range(0, 100_000, 1, 1).union(local_cluster_spark.range(0, 0, 1, 200))
    rows = source.repartitionByRange(2, "id").groupBy(F.spark_partition_id().alias("pid")).count().collect()
    counts = {row.pid: row["count"] for row in rows}
    assert sum(counts.values()) == 100_000
    assert len(counts) == 2, counts
    assert all(count > 30_000 for count in counts.values()), counts


def test_range_repartition_keeps_each_key_in_one_partition(local_cluster_spark):
    source = local_cluster_spark.range(0, 12, 1, 2).withColumn("key", F.col("id") % 3)
    rows = source.repartitionByRange(10, "key").select("key", F.spark_partition_id().alias("pid")).collect()
    assert {(row.key, row.pid) for row in rows} == {(0, 0), (1, 1), (2, 2)}


def test_range_repartition_samples_the_shared_cte_that_the_shuffle_reads(local_cluster_spark):
    # The sample reads the same evaluation of the CTE as the shuffle, as in Spark.
    rows = local_cluster_spark.sql(
        "WITH c AS (SELECT id, rand() AS v FROM range(12)) "
        "SELECT /*+ REPARTITION_BY_RANGE(3, id) */ id, v "
        "FROM (SELECT id, v FROM c UNION ALL SELECT id, v FROM c)"
    ).collect()
    pairs = Counter((row.id, row.v) for row in rows)
    assert len(rows) == 24
    assert sorted(id_ for id_, _ in pairs) == list(range(12))
    assert set(pairs.values()) == {2}, pairs


def test_range_stage_history_matches_executed_tasks(local_cluster_spark):
    spark = local_cluster_spark
    spark.range(24, numPartitions=1).selectExpr("1 AS k").repartitionByRange(8, "k").collect()
    rows = spark.sql(
        """
        WITH stages AS (
            SELECT s.* FROM system.execution.stages s
            JOIN system.session.sessions ss ON ss.session_id = s.session_id
            WHERE ss.client_session_id = :session
        ),
        last_range AS (
            SELECT max(job_id) AS job_id FROM stages WHERE distribution LIKE 'Channel(%'
        )
        SELECT CAST(s.stage AS BIGINT) AS stage, CAST(s.partitions AS BIGINT) AS planned, s.distribution,
               count(DISTINCT t.partition) AS executed
        FROM stages s JOIN last_range r ON s.job_id = r.job_id
        JOIN system.execution.tasks t
          ON (s.session_id, s.job_id, s.stage) = (t.session_id, t.job_id, t.stage)
        GROUP BY s.stage, s.partitions, s.distribution
        """,
        args={"session": spark.session_id},
    ).collect()
    assert [r.distribution for r in rows if r.distribution.startswith("Channel(")] == ["Channel(channels=2)"]
    assert [(r.stage, r.planned, r.executed) for r in rows if r.planned != r.executed] == []
