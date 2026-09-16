import pytest
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(envs={"SAIL_MODE": "local-cluster", "SAIL_CLUSTER__TASK_STREAM_BUFFER": "1"}) as server:
        yield server.remote


@pytest.mark.timeout(15)
@pytest.mark.parametrize("partitions", [2, 4])
@pytest.mark.parametrize("join_type", ["left_semi", "left_anti"])
def test_shuffle_finishes_when_join_leaves_probe_partitions_unread(spark, partitions, join_type):
    # Only one build partition contains a row. The other join partitions finish
    # without opening their probe inputs. Multiple range batches overflow the
    # one-batch shuffle buffers, including those belonging to unread channels.
    build = spark.range(1, numPartitions=1).repartition(partitions, "id")
    probe = spark.range(10_000, numPartitions=1).repartition(partitions, "id")

    rows = build.join(probe, "id", join_type).collect()

    assert rows == ([Row(id=0)] if join_type == "left_semi" else [])
