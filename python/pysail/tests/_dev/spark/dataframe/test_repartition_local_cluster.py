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


def test_range_repartition_keeps_each_key_in_one_partition(local_cluster_spark):
    source = local_cluster_spark.range(0, 12, 1, 2).withColumn("key", F.col("id") % 3)
    rows = source.repartitionByRange(10, "key").select("key", F.spark_partition_id().alias("pid")).collect()
    assert {(row.key, row.pid) for row in rows} == {(0, 0), (1, 1), (2, 2)}
