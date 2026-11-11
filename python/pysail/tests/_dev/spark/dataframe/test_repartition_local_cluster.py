"""`repartitionByRange` on cluster workers.

The split points come from a sample job that runs on the workers, and the range
partitioning crosses the distributed codec. The expected partitions match JVM Spark,
where the sample covers every row.
"""

import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

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
    assert len(rows) == 36  # noqa: PLR2004


def failing_key(_value):
    msg = "range key computed"
    raise ValueError(msg)


def test_range_repartition_samples_only_when_the_query_runs(local_cluster_spark):
    key = F.udf(failing_key, "long")("id")
    df = local_cluster_spark.range(12).withColumn("key", key).repartitionByRange(3, "key")
    df._explain_string(extended=True)  # noqa: SLF001
    with pytest.raises(Exception, match="range key computed"):
        df.collect()


def test_nested_range_repartitions_assign_key_ranges(local_cluster_spark):
    source = local_cluster_spark.range(0, 36, 1, 4).withColumn("key", F.col("id") % 12)
    # The projection keeps the two repartitions from collapsing into one.
    inner = source.repartitionByRange(3, "key").withColumn("inner_pid", F.spark_partition_id())
    rows = inner.repartitionByRange(2, F.col("key").desc()).withColumn("pid", F.spark_partition_id()).collect()
    for row in rows:
        assert row.inner_pid == row.key // 4, row
        assert row.pid == (11 - row.key) // 6, row
    assert len(rows) == 36  # noqa: PLR2004


def test_range_repartition_samples_input_with_scalar_subquery(local_cluster_spark):
    rows = local_cluster_spark.sql(
        "SELECT id, spark_partition_id() AS pid FROM ("
        "SELECT /*+ REPARTITION_BY_RANGE(3, id) */ id FROM range(12) "
        "WHERE id < (SELECT max(id) + 1 FROM range(12)))"
    ).collect()
    assert sorted((row.id, row.pid) for row in rows) == [(i, i // 4) for i in range(12)]


def test_range_repartition_samples_imbalanced_input_again(local_cluster_spark):
    # Of 201 input partitions only one has rows, so the first sample keeps one key
    # of it. The partition is sampled again in proportion to its rows.
    source = local_cluster_spark.range(0, 100_000, 1, 1).union(local_cluster_spark.range(0, 0, 1, 200))
    rows = source.repartitionByRange(2, "id").groupBy(F.spark_partition_id().alias("pid")).count().collect()
    counts = {row.pid: row["count"] for row in rows}
    assert sum(counts.values()) == 100_000  # noqa: PLR2004
    assert len(counts) == 2, counts  # noqa: PLR2004
    assert all(count > 30_000 for count in counts.values()), counts  # noqa: PLR2004


def test_range_repartition_keeps_each_key_in_one_partition(local_cluster_spark):
    source = local_cluster_spark.range(0, 12, 1, 2).withColumn("key", F.col("id") % 3)
    rows = source.repartitionByRange(10, "key").select("key", F.spark_partition_id().alias("pid")).collect()
    assert {(row.key, row.pid) for row in rows} == {(0, 0), (1, 1), (2, 2)}
